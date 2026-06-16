package stg

import vmem "core:mem/virtual"
import "core:thread"
import "core:sync"
import "base:intrinsics"

CACHE_LINE :: 64

// runner //////////////////////////////////////////////////////////////////////

TaskIndex :: struct {
    group: ^WorkerGroup,
    index: int,
}

Runner :: struct {
    worker_groups: [dynamic]^WorkerGroup,
    tasks_indices: map[TaskProc]TaskIndex,
    arena: vmem.Arena,
}

runner_init :: proc(runner: ^Runner)
{
    arena_error := vmem.arena_init_growing(&runner.arena)
    ensure(arena_error == nil)
    runner.worker_groups = make([dynamic]^WorkerGroup, vmem.arena_allocator(&runner.arena))
    runner.tasks_indices = make(map[TaskProc]TaskIndex, vmem.arena_allocator(&runner.arena))
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
        worker_group_stop(group)
    }
}

add_thread_group :: proc(runner: ^Runner, thread_count: int) -> ^WorkerGroup
{
    allocator := vmem.arena_allocator(&runner.arena)
    group := new(WorkerGroup, allocator)
    worker_group_init(Worker, group, runner, len(runner.worker_groups), thread_count)
    append(&runner.worker_groups, group)
    return group
}

push_job_runner :: proc(runner: ^Runner, task_proc: TaskProc, data := Data{}, tracker: ^JobTracker = nil)
{
    data := data
    data.job_tracker = tracker
    task_group_index, task_exists := runner.tasks_indices[task_proc]
    ensure(task_exists, "tasks must be registered with add_task to be used")
    group := task_group_index.group
    task_index := task_group_index.index
    task_info := &group.tasks_infos[task_index]
    queue_push(&task_info.queue, data)
    worker_group_add_work(group, task_info, 1)
}

// task info ///////////////////////////////////////////////////////////////////

TaskInfo :: struct {
    procedure: TaskProc,
    max_instances_count: int, // maximum number of instances for this task (if 1, only one worker will execute this task at a time)
    thread_count: int,        // number of helper threads requested by the task
    data: rawptr,             // the tasks can have an associated data
    barrier: sync.Barrier,    // used for parallel tasks
    queue: MPMCQueue(Data, 1024),
    // --- the queue is aligned so it adds some padding for the locks atomics
    active_worker_count: uint,
    ready_flag: bool,
}

// worker group ////////////////////////////////////////////////////////////////

WorkerGroup :: struct {
    id: int,
    runner: ^Runner,
    tasks_infos: [dynamic]TaskInfo,
    workers: [dynamic]^Worker,
    worker_run_mutex: sync.Mutex,
    worker_run_cond: sync.Cond,
    process_count: int,
    // TODO: we may also want a job queue here for temporary work (non registered tasks)
    _pad0: [CACHE_LINE]u8,
    work_count: uint,
    _pad1: [CACHE_LINE - size_of(uint)]u8,
    ttl_required_worker_count: uint,
}

worker_group_init :: proc($W: typeid, group: ^WorkerGroup, runner: ^Runner, id: int, thread_count: int)
    where W == Worker || intrinsics.type_is_subtype_of(W, Worker)
{
    allocator := vmem.arena_allocator(&runner.arena)
    group.id = id
    group.runner = runner
    group.workers = make([dynamic]^Worker, thread_count, allocator)
    for i in 0..<thread_count {
        group.workers[i] = new(W, allocator)
        group.workers[i].group = group
        group.workers[i].id = i
    }
    group.tasks_infos = make([dynamic]TaskInfo)
}

worker_group_start :: proc(group: ^WorkerGroup)
{
    for worker in group.workers {
        worker_start(worker)
    }
}

worker_group_stop :: proc(group: ^WorkerGroup)
{
    sync.mutex_lock(&group.worker_run_mutex)
    for worker in group.workers {
        sync.atomic_store(&worker.can_terminate, true)
    }
    sync.mutex_unlock(&group.worker_run_mutex)
    sync.cond_broadcast(&group.worker_run_cond)
    for worker in group.workers {
        worker_stop(worker)
    }
    delete(group.tasks_infos)
}

add_task :: proc(group: ^WorkerGroup, task_proc: TaskProc, max_instances_count: int, data : rawptr = nil)
{
    task_info := TaskInfo{
        procedure = task_proc,
        max_instances_count = max_instances_count,
        thread_count = 1,
        data = data,
    }
    queue_init(&task_info.queue)
    append(&group.tasks_infos, task_info)
    task_index := len(group.tasks_infos) - 1
    group.runner.tasks_indices[task_proc] = TaskIndex{group, task_index}
}

add_parallel_task :: proc(group: ^WorkerGroup, task_proc: TaskProc, max_instances_count, thread_count: int, data : rawptr = nil)
{
    add_task(group, task_proc, max_instances_count, data)
    task_info := &group.tasks_infos[len(group.tasks_infos) - 1]
    task_info.thread_count = min(thread_count, len(group.workers))
    sync.barrier_init(&task_info.barrier, task_info.thread_count)
}

@(private="file")
worker_group_add_work :: proc(group: ^WorkerGroup, task_info: ^TaskInfo, work_count: uint)
{
    if work_count == 0 { return }
    sync.atomic_add(&group.work_count, work_count)

    if !sync.atomic_exchange(&task_info.ready_flag, true) {
        sync.atomic_add(&group.ttl_required_worker_count, uint(task_info.max_instances_count * task_info.thread_count))
        sync.cond_signal(&group.worker_run_cond)
    } else {
        if sync.atomic_load(&task_info.active_worker_count) < uint(task_info.max_instances_count * task_info.thread_count) {
            sync.cond_signal(&group.worker_run_cond)
        }
    }
}

// worker //////////////////////////////////////////////////////////////////////

Worker :: struct #align(CACHE_LINE) {
    thread: ^thread.Thread,
    group: ^WorkerGroup,
    id: int,
    process_count: int,
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
        sync.mutex_lock(&worker.group.worker_run_mutex)
        for {
            if sync.atomic_load(&worker.can_terminate) || sync.atomic_load(&worker.group.work_count) > 0 {
                break
            }
            sync.cond_wait(&worker.group.worker_run_cond, &worker.group.worker_run_mutex)
        }
        sync.mutex_unlock(&worker.group.worker_run_mutex)
        if sync.atomic_load(&worker.can_terminate) do break
        sync.atomic_store(&worker.parked, false)
        process_tasks(worker)
    }
}

process_tasks :: proc(worker: ^Worker) {
    group := worker.group
    loop_count: uint = 0
    task_count := len(group.tasks_infos)

    worker.process_count = 0
    task_index := worker.id % task_count
    for {
        if task_index >= task_count {
            if worker.process_count == 0 && loop_count > 0 {
                intrinsics.cpu_relax()
                return
            }
            loop_count += 1
            worker.process_count = 0
            task_index = 0
        }

        task_info := &group.tasks_infos[task_index]
        task_max_worker_count := uint(task_info.max_instances_count * task_info.max_instances_count)

        if !sync.atomic_load(&task_info.ready_flag) {
            task_index += 1
            continue
        }

        expected_worker_count := compute_task_expected_worker_count(worker, task_info)
        if expected_worker_count == 0 {
            return
        }
        if sync.atomic_load(&task_info.active_worker_count) >= expected_worker_count {
            task_index += 1
            continue
        }
        // we load first to make sure before trying atomic_add
        if sync.atomic_load(&task_info.active_worker_count) >= task_max_worker_count {
            task_index += 1
            continue
        }

        if sync.atomic_add(&task_info.active_worker_count, 1) < task_max_worker_count {
            old_process_count := worker.process_count
            process_task(worker, task_info)
            if worker.process_count == old_process_count {
                make_task_unready(group, task_info)
            }
        } else {
            intrinsics.cpu_relax()
        }
        sync.atomic_sub(&task_info.active_worker_count, 1)
        task_index += 1
    }
}

@(private="file")
process_task :: proc(worker: ^Worker, task_info: ^TaskInfo)
{
    switch procedure in task_info.procedure {
    case TaskProcStandard: process_standard_task(worker, task_info)
    case TaskProcParallel: process_parallel_task(worker, task_info)
    }
}

@(private="file")
process_standard_task :: proc(worker: ^Worker, task_info: ^TaskInfo)
{
    group := worker.group
    ctx := TaskContext{worker, task_info}

    if task_info.max_instances_count == 1 {
        queues_size := queue_size(&task_info.queue)
        sync.atomic_sub(&group.work_count, queues_size) // we will process all the queue
        process_count: uint = 0
        for {
            data := queue_pop(&task_info.queue) or_break
            task_info.procedure.(TaskProcStandard)(ctx, data)
            process_count += 1
        }
        // if we processed more elements, we update the work_count
        if process_count > queues_size {
            sync.atomic_sub(&group.work_count, process_count - queues_size)
        }
        worker.process_count += int(process_count)
    } else {
        for {
            sync.atomic_sub(&group.work_count, 1)
            data, ok := queue_pop(&task_info.queue)
            if !ok {
                sync.atomic_add(&group.work_count, 1)
                intrinsics.cpu_relax()
                return
            }
            task_info.procedure.(TaskProcStandard)(ctx, data)
            worker.process_count += 1
            // rebalance strategy, if a new task has been marked ready, the some of
            // the workers have to leave to rebalance the workload
            if sync.atomic_load(&task_info.active_worker_count) > compute_task_expected_worker_count(worker, task_info) {
                break
            }
        }
    }
}

@(private="file")
process_parallel_task :: proc(worker: ^Worker, task_info: ^TaskInfo)
{
    panic("unimplemented")
}

@(private="file")
make_task_unready :: proc(group: ^WorkerGroup, task_info: ^TaskInfo) {
    if sync.atomic_exchange(&task_info.ready_flag, false) {
        sync.atomic_sub(&group.ttl_required_worker_count, uint(task_info.max_instances_count))
    }
    if queue_size(&task_info.queue) > 0 {
        if !sync.atomic_exchange(&task_info.ready_flag, true) {
            sync.atomic_add(&group.ttl_required_worker_count, uint(task_info.max_instances_count))
        }
        sync.cond_signal(&group.worker_run_cond)
    }
}

@(private="file")
compute_task_expected_worker_count :: proc(worker: ^Worker, task_info: ^TaskInfo) -> uint {
    group := worker.group
    group_size := uint(len(group.workers))
    ttl := sync.atomic_load(&group.ttl_required_worker_count)
    if ttl == 0 { return 0 }
    return max(group_size * uint(task_info.max_instances_count) * uint(task_info.thread_count) / ttl, 1)
}
