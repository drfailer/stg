package stg

import vmem "core:mem/virtual"
import "core:thread"
import "core:sync"
import "base:intrinsics"
import "core:container/queue"
import "core:fmt"

import prof "profiler"

// TODO: I would like to try a version without balancing (the workers continue to process a task untill the queue is empty)
// TODO: rename the struct here

CACHE_LINE :: 64

MULTI_CONSUMER_SELF_BALANCE_CHECK_ITERATION_COUNT :: 8

// runner //////////////////////////////////////////////////////////////////////

TaskInfoAndGroup :: struct {
    group: ^WorkerGroup,
    task_info: ^TaskInfo,
}

Runner :: struct {
    worker_groups: [dynamic]^WorkerGroup,
    tasks: map[TaskProc]TaskInfoAndGroup,
    arena: vmem.Arena,
}

runner_init :: proc(runner: ^Runner) {
    prof.init()
    prof.register() // register the main thread
    arena_error := vmem.arena_init_growing(&runner.arena)
    ensure(arena_error == nil)
    runner.worker_groups = make([dynamic]^WorkerGroup, vmem.arena_allocator(&runner.arena))
    runner.tasks = make(map[TaskProc]TaskInfoAndGroup, vmem.arena_allocator(&runner.arena))
}

runner_fini :: proc(runner: ^Runner) {
    runner_stop(runner)
    vmem.arena_destroy(&runner.arena)
    prof.fini()
}

runner_start :: proc(runner: ^Runner) {
    ttl_thread_count := 0
    for group in runner.worker_groups {
        ttl_thread_count += len(group.workers)
    }
    start_job := job_tracker(ttl_thread_count)
    for group in runner.worker_groups {
        worker_group_start(group, &start_job)
    }
    job_wait(&start_job)
    prof.start() // we start the profiling when all the threads are registered
}

runner_stop :: proc(runner: ^Runner) {
    for group in runner.worker_groups {
        worker_group_fini(group)
    }
    clear(&runner.worker_groups)
    prof.stop()
}

add_thread_group :: proc(runner: ^Runner, helper_thread_count: uint) -> ^WorkerGroup {
    allocator := vmem.arena_allocator(&runner.arena)
    group := new(WorkerGroup, allocator)
    worker_group_init(Worker, group, runner, len(runner.worker_groups), helper_thread_count)
    append(&runner.worker_groups, group)
    return group
}

push_job_runner :: proc(runner: ^Runner, task_proc: TaskProc, data := Data{}, tracker: ^JobTracker = nil) {
    data := data
    data.job_tracker = tracker
    task, task_exists := runner.tasks[task_proc]
    ensure(task_exists, "tasks must be registered with add_task to be used")
    queue_push(&task.task_info.queue, data)
    worker_group_account_jobs(task.group, task.task_info, 1)
}

// task info ///////////////////////////////////////////////////////////////////

TaskInfo :: struct {
    kind: TaskKind,
    procedure: TaskProc,
    thread_count: uint,
    data: rawptr,
    shared_space: SharedSpace,
    queue: MPMCQueue(Data, 1024),
    // --- the queue is aligned so it adds some padding for the locks atomics
    active_worker_count: uint,
    is_ready: bool,
}

TaskKind :: enum {
    Standard,
    Shared,
}

SharedSpace :: struct {
    barrier: Barrier,
    data: [2]Data,
    has_data: [2]bool,
}

task_info_create :: proc(kind: TaskKind, procedure: TaskProc, thread_count: uint, data: rawptr) -> ^TaskInfo {
    task_info := new(TaskInfo)
    task_info.kind = kind
    task_info.procedure = procedure
    task_info.thread_count = thread_count
    task_info.data = data
    queue_init(&task_info.queue)
    return task_info
}

task_info_destroy :: proc(task_info: ^TaskInfo) {
    queue_destroy(&task_info.queue)
    free(task_info)
}

// worker group ////////////////////////////////////////////////////////////////

WorkerGroup :: struct {
    id: int,
    runner: ^Runner,
    // worker data
    workers: [dynamic]^Worker,
    run_mutex: sync.Mutex,
    run_cond: sync.Cond,
    // tasks
    standard_tasks_infos: [dynamic]^TaskInfo,
    shared_tasks_infos: [dynamic]^TaskInfo,
    // scheduler data
    standard_tasks_workload_info: WorkloadInfo,
    shared_tasks_workload_info: WorkloadInfo,
    committed_shared_worker_count: uint,
    curr_shared_task_index: uint,
}

WorkloadInfo :: struct #align(CACHE_LINE) {
    required_worker_count: uint,            // sum(task[i].thread_count)
    _pad0: [CACHE_LINE - size_of(uint)]u8,
    pending_jobs_count: uint,               // number of data in all the queues
    _pad1: [CACHE_LINE - size_of(uint)]u8,
    worker_count: uint,                     // number of workers in the branch
}

worker_group_init :: proc($W: typeid, group: ^WorkerGroup, runner: ^Runner, id: int, thread_count: uint)
        where W == Worker || intrinsics.type_is_subtype_of(W, Worker) {
    allocator := vmem.arena_allocator(&runner.arena)
    group.id = id
    group.runner = runner
    group.workers = make([dynamic]^Worker, thread_count, allocator)
    for i in 0..<thread_count {
        group.workers[i] = new(W, allocator)
        group.workers[i].group = group
        group.workers[i].id = int(i)
    }
    group.standard_tasks_infos = make([dynamic]^TaskInfo)
    group.shared_tasks_infos = make([dynamic]^TaskInfo)
    // TODO: we could start the group here technically
}

worker_group_fini :: proc(group: ^WorkerGroup) {
    worker_group_stop(group)
    for task_info in group.standard_tasks_infos do task_info_destroy(task_info)
    for task_info in group.shared_tasks_infos do task_info_destroy(task_info)
    delete(group.standard_tasks_infos)
    delete(group.shared_tasks_infos)
}

worker_group_start :: proc(group: ^WorkerGroup, start_job: ^JobTracker) {
    for worker in group.workers {
        worker_start(worker, start_job)
    }
}

worker_group_stop :: proc(group: ^WorkerGroup) {
    sync.mutex_lock(&group.run_mutex)
    for worker in group.workers {
        sync.atomic_store(&worker.can_terminate, true)
    }
    sync.mutex_unlock(&group.run_mutex)
    sync.cond_broadcast(&group.run_cond)
    for worker in group.workers {
        worker_stop(worker)
    }
}

add_task :: proc(group: ^WorkerGroup, procedure: TaskProc, thread_count: uint = 1, data : rawptr = nil) {
    ensure(thread_count <= uint(len(group.workers)), "the task requires too many threads")
    ensure(procedure not_in group.runner.tasks, "cannot add the same task multiple times")
    task_info := task_info_create(.Standard, procedure, thread_count, data)
    append(&group.standard_tasks_infos, task_info)
    group.runner.tasks[procedure] = TaskInfoAndGroup{group, task_info}
}

add_shared_task :: proc(group: ^WorkerGroup, procedure: TaskProc, thread_count: uint = 1, data : rawptr = nil) {
    ensure(thread_count <= uint(len(group.workers)), "the task requires too many threads")
    ensure(procedure not_in group.runner.tasks, "cannot add the same task multiple times")
    task_info := task_info_create(.Shared, procedure, thread_count, data)
    append(&group.shared_tasks_infos, task_info)
    group.runner.tasks[procedure] = TaskInfoAndGroup{group, task_info}
}

worker_group_notify_workers :: proc(group: ^WorkerGroup, count: uint) {
    if count == 1 {
        sync.cond_signal(&group.run_cond)
    } else {
        sync.cond_broadcast(&group.run_cond)
    }
}

@(private="file")
worker_group_account_jobs :: proc(group: ^WorkerGroup, task_info: ^TaskInfo, jobs_count: uint) {
    if jobs_count == 0 do return
    wi := &group.standard_tasks_workload_info if task_info.kind == .Standard else &group.shared_tasks_workload_info

    sync.atomic_add(&wi.pending_jobs_count, jobs_count)
    if !sync.atomic_exchange(&task_info.is_ready, true) {
        sync.atomic_add(&wi.required_worker_count, task_info.thread_count)
        if task_info.kind == .Shared {
            sync.atomic_store(&group.curr_shared_task_index, 0)
        }
        worker_group_notify_workers(group, jobs_count)
    } else {
        // make sure we have enough workers
        if sync.atomic_load(&task_info.active_worker_count) < task_info.thread_count {
            worker_group_notify_workers(group, jobs_count)
        }
    }
}

// worker //////////////////////////////////////////////////////////////////////

Worker :: struct #align(CACHE_LINE) {
    thread: ^thread.Thread,
    group: ^WorkerGroup,
    id: int,
    local_index: uint,
    process_count: uint,
    _pad0: [CACHE_LINE]u8,
    can_terminate: bool,
}

worker_start :: proc(worker: ^Worker, start_job: ^JobTracker) {
    sync.atomic_store(&worker.can_terminate, false)
    // TODO: we may want to experiment with the priority
    worker.thread = thread.create_and_start_with_poly_data2(worker, start_job, worker_run, init_context = context)
}

worker_stop :: proc(worker: ^Worker) {
    if worker.thread != nil {
        thread.destroy(worker.thread)
        worker.thread = nil
    }
}

worker_run :: proc(worker: ^Worker, start_job: ^JobTracker) {
    prof.register()
    job_done(start_job) // notify that this worker is started
    job_wait(start_job) // wait for all the rest of the threads to start

    prof.procedure()
    for {
        sync.mutex_lock(&worker.group.run_mutex)
        for {
            prof.region("worker_sleep")
            if sync.atomic_load(&worker.can_terminate) do break
            if sync.atomic_load(&worker.group.standard_tasks_workload_info.pending_jobs_count) > 0 do break
            if sync.atomic_load(&worker.group.shared_tasks_workload_info.pending_jobs_count) > 0 do break
            if sync.atomic_load(&worker.group.committed_shared_worker_count) > 0 do break
            // intrinsics.cpu_relax()
            sync.cond_wait(&worker.group.run_cond, &worker.group.run_mutex) // FIXME: this can deadlock too
        }
        sync.mutex_unlock(&worker.group.run_mutex)
        if sync.atomic_load(&worker.can_terminate) do break
        prof.region_begin("worker_process")
        process_tasks(worker)
        prof.region_end("worker_process")
    }
}

@(private="file")
process_tasks :: proc(worker: ^Worker) {
    group := worker.group
    standard_task_required_worker_count := sync.atomic_load(&group.standard_tasks_workload_info.required_worker_count)
    shared_task_required_worker_count := sync.atomic_load(&group.shared_tasks_workload_info.required_worker_count)

    if standard_task_required_worker_count == 0 && shared_task_required_worker_count == 0 do return

    if sync.atomic_load(&group.committed_shared_worker_count) > 0 {
        process_shared_tasks(worker)
        return
    }

    standard_task_worker_count := sync.atomic_load(&group.standard_tasks_workload_info.worker_count)
    standard_task_pending_jobs_count := sync.atomic_load(&group.standard_tasks_workload_info.pending_jobs_count)
    has_standard_work := standard_task_worker_count < standard_task_required_worker_count &&
                         standard_task_worker_count < standard_task_pending_jobs_count

    if has_standard_work {
        process_standard_tasks(worker)
    } else if shared_task_required_worker_count > 0 {
        standard_task_reserved_worker_count := min(standard_task_required_worker_count, standard_task_pending_jobs_count)
        shared_available := cast(uint)max(0, int(len(group.workers)) - int(standard_task_reserved_worker_count))
        // FIXME: we may want a more fine grained check per task
        if shared_available >= shared_task_required_worker_count {
            process_shared_tasks(worker)
        }
    }
}

process_standard_tasks :: proc(worker: ^Worker) {
    sync.atomic_add(&worker.group.standard_tasks_workload_info.worker_count, 1)
    defer sync.atomic_sub(&worker.group.standard_tasks_workload_info.worker_count, 1)

    group := worker.group
    loop_count: uint = 0
    task_count := len(group.standard_tasks_infos)
    worker_count := len(group.workers)

    worker.process_count = 0
    for task_index := worker.id % task_count; ; task_index += 1 {
        if task_index >= task_count {
            if worker.process_count == 0 && loop_count > 0 {
                intrinsics.cpu_relax()
                break
            }
            loop_count += 1
            worker.process_count = 0
            task_index = 0
        }

        task_info := group.standard_tasks_infos[task_index]
        if !sync.atomic_load(&task_info.is_ready) do continue

        expected_worker_count := compute_standard_task_expected_worker_count(worker, task_info)

        // this happens when the current task just got marked unready, and all
        // the other task are unready, in this case we leave
        if expected_worker_count == 0 do return

        task_active_worker_count := sync.atomic_load(&task_info.active_worker_count)

        // conditions:
        // 1. too many workers with respect to the task configuration (max thread count threshold)
        // 2. too many workers with respect to the current workload (balance the workers across active tasks)
        // TODO: this condition should also take the queue size into account
        if task_active_worker_count >= expected_worker_count || task_active_worker_count >= task_info.thread_count do continue

        // we task ownership of the task and we try to dequeue
        if sync.atomic_add(&task_info.active_worker_count, 1) < task_info.thread_count {
            old_process_count := worker.process_count
            process_standard_task(worker, task_info)
            if worker.process_count == old_process_count {
                // security to make the task unready when no processing was done
                make_task_unready(worker.group, task_info)
            }
        } else {
            // ownership race lost with another worker
            intrinsics.cpu_relax()
        }
        sync.atomic_sub(&task_info.active_worker_count, 1)
    }
}

@(private="file")
process_standard_task :: proc(worker: ^Worker, task_info: ^TaskInfo) {
    group := worker.group
    ctx := TaskContext{worker, task_info, {}}
    wi := &group.standard_tasks_workload_info

    if task_info.thread_count == 1 {
        //
        // In single consumer tasks, we process all the queue and we don't
        // leave the task until its done. This is made to decrease the latency
        // of synchronization node which are single threaded.
        //
        queues_size := queue_size(&task_info.queue)
        sync.atomic_sub(&wi.pending_jobs_count, queues_size) // greedy update to avoid atomic_sub in the loop
        process_count: uint = 0
        for {
            data := queue_pop(&task_info.queue) or_break
            task_info.procedure.(TaskProcStandard)(ctx, data)
            process_count += 1
        }
        // if we processed more elements, we update the pending jobs count
        if process_count > queues_size {
            sync.atomic_sub(&wi.pending_jobs_count, process_count - queues_size)
        }
        worker.process_count += process_count
    } else {
        //
        // Multi-consumer tasks have a self-balance condition that recomputes
        // the expected worker count for the current task with respect to the
        // current workload and leave if there are too many workers on the task.
        // This allows workers to spread over the ready tasks and maintain an
        // active worker count proportional to the consumer count of each task
        // and the numbers of available workers.
        //
        // TODO: we need a more sofisticated condition for graphs that have
        //       more tasks than workers to avoid rebalancing after dequeue.
        //
        for iteration := 0; ; iteration += 1 {
            if queue_size(&task_info.queue) == 0 do break
            sync.atomic_sub(&wi.pending_jobs_count, 1) // greedy update to reduce the chance of waking up a new worker for nothing
            data, ok := queue_pop(&task_info.queue)
            if !ok {
                sync.atomic_add(&wi.pending_jobs_count, 1) // revert update
                intrinsics.cpu_relax()
                return
            }
            task_info.procedure.(TaskProcStandard)(ctx, data)
            worker.process_count += 1
            // rebalance strategy, if a new task has been marked ready, the some of
            // the workers have to leave to rebalance the workload
            if iteration > MULTI_CONSUMER_SELF_BALANCE_CHECK_ITERATION_COUNT {
                iteration = 0
                if sync.atomic_load(&task_info.active_worker_count) > compute_standard_task_expected_worker_count(worker, task_info) {
                    break
                }
            }
        }
    }
}

@(private="file")
process_shared_tasks :: proc(worker: ^Worker) {
    // TODO: we need a condition to prevent workers from entering a shared task
    //       when we know not enough workers will be available in time (could be
    //       based on the number of workers already processing a shared task)

    task_count := uint(len(worker.group.shared_tasks_infos))
    loop_count: uint = 0
    for {
        task_index := sync.atomic_load(&worker.group.curr_shared_task_index)
        if task_index >= task_count {
            if loop_count > 0 do break
            loop_count += 1
            continue
        }

        task_info := worker.group.shared_tasks_infos[task_index]

        if !sync.atomic_load(&task_info.is_ready) {
            sync.atomic_compare_exchange_weak(&worker.group.curr_shared_task_index, task_index, task_index + 1)
            continue
        }

        if sync.atomic_load(&task_info.active_worker_count) >= task_info.thread_count {
            sync.atomic_compare_exchange_weak(&worker.group.curr_shared_task_index, task_index, task_index + 1)
            continue
        }

        worker.local_index = sync.atomic_add(&task_info.active_worker_count, 1)
        if worker.local_index < task_info.thread_count {
            if worker.local_index == 0 {
                sync.atomic_store(&worker.group.committed_shared_worker_count, task_info.thread_count - 1)
                worker_group_notify_workers(worker.group, task_info.thread_count - 1)
            } else {
                sync.atomic_sub(&worker.group.committed_shared_worker_count, 1)
            }
            process_shared_task(worker, task_info)
            if sync.atomic_sub(&task_info.active_worker_count, 1) == 1 {
                sync.atomic_store(&task_info.shared_space.barrier.state[0], 0)
                sync.atomic_store(&task_info.shared_space.barrier.state[1], 0)
                make_task_unready(worker.group, task_info)
            }
        } else {
            sync.atomic_sub(&task_info.active_worker_count, 1)
            sync.atomic_compare_exchange_weak(&worker.group.curr_shared_task_index, task_index, task_index + 1)
            continue
        }
        break
    }
}

@(private="file")
process_shared_task :: proc(worker: ^Worker, task_info: ^TaskInfo) {
    assert(queue_size(&task_info.queue) > 0)
    ctx := TaskContext{worker, task_info, &task_info.shared_space}
    barrier_state_index: u8
    buf_index: uint = 0

    if worker.local_index == 0 {
        ctx.shared_space.data[0], ctx.shared_space.has_data[0] = queue_pop(&task_info.queue)
        sync.atomic_sub(&worker.group.shared_tasks_workload_info.pending_jobs_count, 1)
    }

    for {
        barrier_wait(&task_info.shared_space.barrier, &barrier_state_index, u32(task_info.thread_count))
        if !ctx.shared_space.has_data[buf_index] do break

        if worker.local_index == 0 {
            ctx.shared_space.data[1 - buf_index], ctx.shared_space.has_data[1 - buf_index] = queue_pop(&task_info.queue)
            if ctx.shared_space.has_data[1 - buf_index] {
                sync.atomic_sub(&worker.group.shared_tasks_workload_info.pending_jobs_count, 1)
            }
        }

        task_info.procedure.(TaskProcShared)(ctx, ctx.shared_space.data[buf_index], worker.local_index, task_info.thread_count)
        buf_index = 1 - buf_index
    }
}

@(private="file")
make_task_unready :: proc(group: ^WorkerGroup, task_info: ^TaskInfo) {
    wi := &group.standard_tasks_workload_info if task_info.kind == .Standard else &group.shared_tasks_workload_info

    // this avoids updating the readyness when an element just got pushed.
    // NOTE: there is no need to wake up another worker here since the it has
    //       already been done by the producer.
    if queue_size(&task_info.queue) > 0 do return

    if sync.atomic_exchange(&task_info.is_ready, false) {
        sync.atomic_sub(&wi.required_worker_count, task_info.thread_count)
    }
    // post check for race with concurrent push
    if queue_size(&task_info.queue) > 0 {
        if !sync.atomic_exchange(&task_info.is_ready, true) {
            sync.atomic_add(&wi.required_worker_count, task_info.thread_count)
        }
        sync.cond_signal(&group.run_cond)
    }
}

@(private="file")
compute_standard_task_expected_worker_count :: proc(worker: ^Worker, task_info: ^TaskInfo) -> uint {
    group := worker.group
    group_size := uint(len(group.workers))
    ttl := sync.atomic_load(&group.standard_tasks_workload_info.required_worker_count)
    if ttl == 0 { return 0 }
    return max((group_size * task_info.thread_count) / ttl, 1)
}
