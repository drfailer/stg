package stg

import vmem "core:mem/virtual"
import "core:thread"
import "core:sync"
import "base:intrinsics"
import "core:container/queue"
import "core:fmt"

CACHE_LINE :: 64

// TODO: test this
MULTI_CONSUMER_SELF_BALANCE_CHECK_ITERATION_COUNT :: 1

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

runner_init :: proc(runner: ^Runner)
{
    arena_error := vmem.arena_init_growing(&runner.arena)
    ensure(arena_error == nil)
    runner.worker_groups = make([dynamic]^WorkerGroup, vmem.arena_allocator(&runner.arena))
    runner.tasks = make(map[TaskProc]TaskInfoAndGroup, vmem.arena_allocator(&runner.arena))
}

runner_fini :: proc(runner: ^Runner)
{
    runner_stop(runner)
    vmem.arena_destroy(&runner.arena)
}

runner_start :: proc(runner: ^Runner)
{
    for group in runner.worker_groups {
        worker_group_start(group)
    }
}

runner_stop :: proc(runner: ^Runner)
{
    for group in runner.worker_groups {
        worker_group_fini(group)
    }
}

add_thread_group :: proc(runner: ^Runner, helper_thread_count: uint) -> ^WorkerGroup
{
    allocator := vmem.arena_allocator(&runner.arena)
    group := new(WorkerGroup, allocator)
    worker_group_init(Worker, group, runner, len(runner.worker_groups), helper_thread_count)
    append(&runner.worker_groups, group)
    return group
}

push_job_runner :: proc(runner: ^Runner, task_proc: TaskProc, data := Data{}, tracker: ^JobTracker = nil)
{
    data := data
    data.job_tracker = tracker
    task, task_exists := runner.tasks[task_proc]
    ensure(task_exists, "tasks must be registered with add_task to be used")
    queue_push(&task.task_info.queue, data)
    worker_group_account_jobs(task.group, task.task_info, 1)
}

// task info ///////////////////////////////////////////////////////////////////

TaskInfo :: struct {
    procedure: TaskProc,
    consumer_count: uint, // number of workers allowed to dequeue
    thread_count: uint,   // number of threads per consumer (shared tasks)
    data: rawptr,
    shared_task_data: SharedTaskData,
    queue: MPMCQueue(Data, 1024),
    // --- the queue is aligned so it adds some padding for the locks atomics
    active_consumer_count: uint,
    is_ready: bool,
}

SharedTaskData :: struct {
    shared_spaces: [dynamic]SharedSpace, // shared per instance
}

SharedSpace :: struct {
    barrier: sync.Barrier, // TODO: we will eventually need a more flexible barrier
    data: Data,
    has_data: bool,
}

task_info_create :: proc(procedure: TaskProc, consumer_count, thread_count: uint, data: rawptr) -> ^TaskInfo
{
    task_info := new(TaskInfo)
    task_info.procedure = procedure
    task_info.consumer_count = consumer_count
    task_info.thread_count = thread_count
    task_info.data = data
    queue_init(&task_info.queue)
    if thread_count > 1 {
        task_info.shared_task_data = SharedTaskData{
            shared_spaces = make([dynamic]SharedSpace, consumer_count),
        }
        for &sp in task_info.shared_task_data.shared_spaces {
            sync.barrier_init(&sp.barrier, int(thread_count))
        }
    }
    return task_info
}

task_info_destroy :: proc(task_info: ^TaskInfo)
{
    queue_destroy(&task_info.queue)
    if task_info.thread_count > 1 {
        delete(task_info.shared_task_data.shared_spaces)
    }
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
    parked_worker_count: uint,
}

// TODO: figure out if we need some padding bettween the atomics
WorkloadInfo :: struct #align(CACHE_LINE) {
    required_worker_count: uint, // sum(task[i].max_instance_count)
    pending_jobs_count: uint,    // number of data in all the queues
    worker_count: uint,          // number of workers in the branch
}

worker_group_init :: proc($W: typeid, group: ^WorkerGroup, runner: ^Runner, id: int, helper_thread_count: uint)
    where W == Worker || intrinsics.type_is_subtype_of(W, Worker)
{
    allocator := vmem.arena_allocator(&runner.arena)
    group.id = id
    group.runner = runner
    group.workers = make([dynamic]^Worker, helper_thread_count, allocator)
    for i in 0..<helper_thread_count {
        group.workers[i] = new(W, allocator)
        group.workers[i].group = group
        group.workers[i].id = int(i)
    }
    group.standard_tasks_infos = make([dynamic]^TaskInfo)
    group.shared_tasks_infos = make([dynamic]^TaskInfo)
    // TODO: we could start the group here technically
}

worker_group_fini :: proc(group: ^WorkerGroup)
{
    worker_group_stop(group)
    for task_info in group.standard_tasks_infos do task_info_destroy(task_info)
    for task_info in group.shared_tasks_infos do task_info_destroy(task_info)
    delete(group.standard_tasks_infos)
    delete(group.shared_tasks_infos)
}

worker_group_start :: proc(group: ^WorkerGroup)
{
    for worker in group.workers {
        worker_start(worker)
    }
}

worker_group_stop :: proc(group: ^WorkerGroup)
{
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

add_task :: proc(group: ^WorkerGroup, procedure: TaskProc, consumer_count: uint, data : rawptr = nil)
{
    ensure(consumer_count <= uint(len(group.workers)), "the task requires too many threads")
    task_info := task_info_create(procedure, consumer_count, 1, data)
    append(&group.standard_tasks_infos, task_info)
    group.runner.tasks[procedure] = TaskInfoAndGroup{group, task_info}
}

add_shared_task :: proc(group: ^WorkerGroup, procedure: TaskProc, consumer_count, thread_count: uint, data : rawptr = nil)
{
    ensure(consumer_count * thread_count <= uint(len(group.workers)), "the task requires too many threads")
    task_info := task_info_create(procedure, consumer_count, thread_count, data)
    append(&group.shared_tasks_infos, task_info)
    group.runner.tasks[procedure] = TaskInfoAndGroup{group, task_info}
}

worker_group_notify_workers :: proc(group: ^WorkerGroup, count: uint)
{
    if count == 1 {
        sync.cond_signal(&group.run_cond)
    } else {
        sync.cond_broadcast(&group.run_cond)
    }
}

@(private="file")
worker_group_account_jobs :: proc(group: ^WorkerGroup, task_info: ^TaskInfo, jobs_count: uint)
{
    if jobs_count == 0 do return

    switch _ in task_info.procedure {
    case (TaskProcStandard):
        wi := &group.standard_tasks_workload_info
        sync.atomic_add(&wi.pending_jobs_count, jobs_count)
        if !sync.atomic_exchange(&task_info.is_ready, true) {
            sync.atomic_add(&wi.required_worker_count, task_info.consumer_count)
            worker_group_notify_workers(group, jobs_count)
        } else {
            // make sure we have enough workers
            if sync.atomic_load(&task_info.active_consumer_count) < task_info.consumer_count {
                worker_group_notify_workers(group, jobs_count)
            }
        }
    case (TaskProcShared):
        wi := &group.shared_tasks_workload_info
        sync.atomic_add(&wi.pending_jobs_count, jobs_count)
        required_worker_count := task_info.consumer_count * task_info.thread_count
        if !sync.atomic_exchange(&task_info.is_ready, true) {
            sync.atomic_add(&wi.required_worker_count, required_worker_count)
            worker_group_notify_workers(group, jobs_count * task_info.thread_count)
        } else {
            // TODO: this condition is unclear v
            if sync.atomic_load(&task_info.active_consumer_count) < task_info.consumer_count {
                worker_group_notify_workers(group, jobs_count * task_info.thread_count)
            }
        }
    }
}

// worker //////////////////////////////////////////////////////////////////////

Worker :: struct #align(CACHE_LINE) {
    thread: ^thread.Thread,
    group: ^WorkerGroup,
    id: int,
    local_index: uint,
    instance_index: uint,
    process_count: uint,
    _pad0: [CACHE_LINE]u8,
    parked: bool,
    _pad1: [CACHE_LINE - size_of(bool)]u8,
    can_terminate: bool,
}

worker_start :: proc(worker: ^Worker)
{
    sync.atomic_store(&worker.parked, true)
    sync.atomic_store(&worker.can_terminate, false)
    // TODO: we may want to experiment with the priority
    worker.thread = thread.create_and_start_with_poly_data(worker, worker_run)
}

worker_stop :: proc(worker: ^Worker)
{
    if worker.thread != nil {
        thread.destroy(worker.thread)
        worker.thread = nil
    }
}

worker_run :: proc(worker: ^Worker)
{
    for {
        sync.atomic_store(&worker.parked, true)
        sync.atomic_add(&worker.group.parked_worker_count, 1)
        sync.mutex_lock(&worker.group.run_mutex)
        for {
            if sync.atomic_load(&worker.can_terminate) do break
            if sync.atomic_load(&worker.group.standard_tasks_workload_info.pending_jobs_count) > 0 do break
            if sync.atomic_load(&worker.group.shared_tasks_workload_info.pending_jobs_count) > 0 do break
            sync.cond_wait(&worker.group.run_cond, &worker.group.run_mutex)
        }
        sync.mutex_unlock(&worker.group.run_mutex)
        if sync.atomic_load(&worker.can_terminate) do break
        sync.atomic_store(&worker.parked, false)
        sync.atomic_sub(&worker.group.parked_worker_count, 1)
        process_tasks(worker)
    }
}

@(private="file")
process_tasks :: proc(worker: ^Worker)
{
    group := worker.group
    for {
        standard_task_required_worker_count := sync.atomic_load(&group.standard_tasks_workload_info.required_worker_count)
        shared_task_required_worker_count := sync.atomic_load(&group.shared_tasks_workload_info.required_worker_count)

        if standard_task_required_worker_count == 0 && shared_task_required_worker_count == 0 do break

        standard_task_worker_count := sync.atomic_load(&group.standard_tasks_workload_info.worker_count)
        standard_task_pending_jobs_count := sync.atomic_load(&group.standard_tasks_workload_info.pending_jobs_count)

        if standard_task_worker_count < standard_task_required_worker_count &&
           standard_task_worker_count < standard_task_pending_jobs_count {
            process_standard_tasks(worker)
        } else {
            // fmt.printfln("worker {}: worker count = {}, required = {}, job count = {}",
            //     worker.id, standard_task_worker_count, standard_task_required_worker_count, standard_task_pending_jobs_count)
            // process_shared_tasks(worker)
        }
    }
}

process_standard_tasks :: proc(worker: ^Worker)
{
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

        expected_worker_count := compute_task_expected_worker_count(worker, task_info)

        // this happens when the current task just got marked unready, and all
        // the other task are unready, in this case we leave
        if expected_worker_count == 0 do return

        task_active_worker_count := sync.atomic_load(&task_info.active_consumer_count)

        // conditions:
        // 1. too many workers with respect to the task configuration (max consumer count threshold)
        // 2. too many workers with respect to the current workload (balance the workers across active tasks)
        // TODO: this condition should also take the queue size into account
        if task_active_worker_count >= expected_worker_count || task_active_worker_count >= task_info.consumer_count do continue

        // we task ownership of the task and we try to dequeue
        if sync.atomic_add(&task_info.active_consumer_count, 1) < task_info.consumer_count {
            old_process_count := worker.process_count
            process_standard_task(worker, task_info)
            if worker.process_count == old_process_count {
                // security to make the task unready when no processing was done
                make_standard_task_unready(worker.group, task_info)
            }
        } else {
            // ownership race lost with another worker
            intrinsics.cpu_relax()
        }
        sync.atomic_sub(&task_info.active_consumer_count, 1)
    }
}

@(private="file")
process_standard_task :: proc(worker: ^Worker, task_info: ^TaskInfo)
{
    group := worker.group
    ctx := TaskContext{worker, task_info, {}}

    if task_info.consumer_count == 1 {
        //
        // In single consumer tasks, we process all the queue and we don't
        // leave the task until its done. This is made to decrease the latency
        // of synchronization node which are single threaded.
        //
        queues_size := queue_size(&task_info.queue)
        sync.atomic_sub(&group.standard_tasks_workload_info.pending_jobs_count, queues_size) // greedy update to avoid atomic_sub in the loop
        process_count: uint = 0
        for {
            data := queue_pop(&task_info.queue) or_break
            task_info.procedure.(TaskProcStandard)(ctx, data)
            process_count += 1
        }
        // if we processed more elements, we update the pending jobs count
        if process_count > queues_size {
            sync.atomic_sub(&group.standard_tasks_workload_info.pending_jobs_count, process_count - queues_size)
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
            sync.atomic_sub(&group.standard_tasks_workload_info.pending_jobs_count, 1) // greedy update to reduce the chance of waking up a new worker for nothing
            data, ok := queue_pop(&task_info.queue)
            if !ok {
                sync.atomic_add(&group.standard_tasks_workload_info.pending_jobs_count, 1) // revert update
                intrinsics.cpu_relax()
                return
            }
            task_info.procedure.(TaskProcStandard)(ctx, data)
            worker.process_count += 1
            // rebalance strategy, if a new task has been marked ready, the some of
            // the workers have to leave to rebalance the workload
            if iteration > MULTI_CONSUMER_SELF_BALANCE_CHECK_ITERATION_COUNT {
                iteration = 0
                if sync.atomic_load(&task_info.active_consumer_count) > compute_task_expected_worker_count(worker, task_info) {
                    break
                }
            }
        }
    }
}

@(private="file")
process_shared_tasks :: proc(worker: ^Worker)
{
    panic("shared task need to be rewriten")
    // sync.atomic_add(&worker.group.shared_task_worker_count, 1) // count the threads waiting before the mutex
    // defer sync.atomic_sub(&worker.group.shared_task_worker_count, 1)
    // // TODO: add an early leave condition when the condition are not met
    // for sync.atomic_load(&worker.group.shared_task_count) > 0 {
    //     sync.mutex_lock(&worker.group.shared_task_mutex)
    //
    //     if queue.len(worker.group.shared_task_queue) == 0 do return
    //
    //     ticket := queue.front(&worker.group.shared_task_queue)
    //     required_worker_count := ticket.task_info.helper_thread_count - ticket.thread_counters[ticket.instance_counter]
    //     available_workers_count := sync.atomic_load(&worker.group.shared_task_worker_count) +
    //                                sync.atomic_load(&worker.group.parked_worker_count) + 1
    //     if available_workers_count >= required_worker_count {
    //         worker.instance_index = ticket.instance_counter
    //         worker.local_index = ticket.thread_counters[ticket.instance_counter]
    //         ticket.thread_counters[ticket.instance_counter] += 1
    //         if ticket.thread_counters[ticket.instance_counter] == ticket.task_info.helper_thread_count {
    //             ticket.instance_counter += 1
    //             if ticket.instance_counter == ticket.task_info.max_thread_count {
    //                 queue.dequeue(&worker.group.shared_task_queue)
    //                 sync.atomic_sub(&worker.group.shared_task_count, 1)
    //             }
    //         }
    //         sync.mutex_unlock(&worker.group.shared_task_mutex)
    //         process_shared_task(worker, ticket.task_info)
    //     } else {
    //         sync.mutex_unlock(&worker.group.shared_task_mutex)
    //         return // leav early?
    //     }
    // }
}

@(private="file")
process_shared_task :: proc(worker: ^Worker, task_info: ^TaskInfo)
{
    panic("shared task need to be rewriten")
    // shared_space := &task_info.shared_tasks_infos.shared[worker.instance_index]
    // ctx := TaskContext{worker, task_info, shared_space}
    //
    // for {
    //     if worker.local_index == 0 {
    //         ctx.shared_space.data, ctx.shared_space.has_data = queue_pop(&task_info.queue)
    //     }
    //     sync.barrier_wait(&shared_space.barrier)
    //     if !ctx.shared_space.has_data do break
    //     task_info.procedure.(TaskProcShared)(ctx, ctx.shared_space.data, worker.local_index, task_info.helper_thread_count)
    //     if worker.local_index == 0 {
    //         sync.atomic_sub(&worker.group.work_count, 1)
    //     }
    //     sync.barrier_wait(&shared_space.barrier)
    // }
}

@(private="file")
make_standard_task_unready :: proc(group: ^WorkerGroup, task_info: ^TaskInfo) {
    // try to update the ready flag and the required worker count on success
    if sync.atomic_exchange(&task_info.is_ready, false) {
        sync.atomic_sub(&group.standard_tasks_workload_info.required_worker_count, task_info.consumer_count)
    }
    // post check of the queue size to handle update edge cases (lock free updates)
    if queue_size(&task_info.queue) > 0 {
        // we help the thread that pushed the new job by trying to make the
        // task ready again for him
        if !sync.atomic_exchange(&task_info.is_ready, true) {
            sync.atomic_add(&group.standard_tasks_workload_info.required_worker_count, task_info.consumer_count)
        }
        // this brings extra deadlock safety and make sure at least one worker
        // will process the data. We only signal one because this only occurs
        // when on job is pushed (if more jobs come after this, the thread
        // which pushes the jobs will wake up more workers).
        sync.cond_signal(&group.run_cond)
    }
}

@(private="file")
compute_task_expected_worker_count :: proc(worker: ^Worker, task_info: ^TaskInfo) -> uint {
    group := worker.group
    group_size := uint(len(group.workers))

    ttl: uint

    if task_info.thread_count == 1 {
        ttl = sync.atomic_load(&group.standard_tasks_workload_info.required_worker_count)
    } else {
        ttl = sync.atomic_load(&group.shared_tasks_workload_info.required_worker_count)
    }

    if ttl == 0 { return 0 }
    return max((group_size * task_info.consumer_count * task_info.thread_count) / ttl, 1)
}
