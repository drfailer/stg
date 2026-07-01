package lock_runner

import vmem "core:mem/virtual"
import "core:thread"
import "core:sync"
import "core:log"
import stg "../"
import prof "../profiler"

TaskInfo :: struct {
    using task_info: stg.TaskInfo,
    cur_thread_count: int,
    mutex: sync.Mutex,
    queue: stg.MPMCQueue(stg.Data, 1024),
    ready_list_index: int,
}

Runner :: struct {
    using runner: stg.Runner,
    arena: vmem.Arena,
}

WorkerGroup :: struct {
    using group: stg.WorkerGroup,
    ready_lists: [stg.TaskKind]ReadyList,
    needed_shared_worker_count: int,
}

Worker :: struct {
    using worker: stg.Worker,
}

runner_init :: proc(runner: ^Runner, nb_threads := 0) {
    err := vmem.arena_init_growing(&runner.arena)
    ensure(err == nil)
    allocator := vmem.arena_allocator(&runner.arena)
    runner.groups = make([dynamic]^stg.WorkerGroup)
    runner.tasks = make(map[stg.TaskProc]^stg.TaskInfo)
    runner.add_group = add_group
    runner.add_task = add_task
    runner.add_job = add_job
    if nb_threads > 0 do add_group(runner, nb_threads)
    return
}

runner_fini :: proc(runner: ^Runner) {
    runner_stop(runner)
    for &group_ in runner.groups {
        group := cast(^WorkerGroup)group_
        for &tasks in group.tasks {
            for &task_ in tasks {
                task := cast(^TaskInfo)task_
                stg.queue_destroy(&task.queue)
            }
            delete(tasks)
        }
    }
    delete(runner.tasks)
    delete(runner.groups)
    vmem.arena_destroy(&runner.arena)
}

runner_start :: proc(runner: ^Runner) {
    allocator := vmem.arena_allocator(&runner.arena)
    for &group in runner.groups {
        group := cast(^WorkerGroup)group
        for &list, task_kind in group.ready_lists do ready_list_init(&list, len(group.tasks[task_kind]), allocator)
        for &worker in group.workers {
            worker.thread = thread.create_and_start_with_poly_data(cast(^Worker)worker, worker_run, init_context = context)
        }
    }
}

runner_stop :: proc(runner: ^Runner) {
    for &group in runner.groups {
        sync.lock(&group.mutex)
        for &worker in group.workers {
            sync.atomic_store(&worker.can_terminate, true)
        }
        sync.unlock(&group.mutex)
        sync.cond_broadcast(&group.cond)
        for &worker in group.workers {
            thread.join(worker.thread)
            thread.destroy(worker.thread)
        }
    }
}

add_group :: proc(runner: ^Runner, nb_threads: int) -> int {
    allocator := vmem.arena_allocator(&runner.arena)
    group := new(WorkerGroup, allocator)
    group.id = len(runner.groups)
    group.runner = runner
    group.workers = make([dynamic]^stg.Worker, nb_threads, allocator = allocator)
    for idx in 0..<len(group.workers) {
        worker := new(Worker, allocator)
        worker.id = idx
        worker.group = group
        worker.can_terminate = false
        group.workers[idx] = worker
    }
    group.tasks[.Standard] = make([dynamic]^stg.TaskInfo)
    group.tasks[.Shared] = make([dynamic]^stg.TaskInfo)
    append(&runner.groups, group)
    return group.id
}

add_task :: proc(runner: ^Runner, group_id: int, task: stg.TaskProc, nb_threads: int, data: rawptr) {
    ensure(group_id < len(runner.groups), "unknown group id")
    allocator := vmem.arena_allocator(&runner.arena)
    group := cast(^WorkerGroup)runner.groups[group_id]

    ensure(nb_threads <= len(group.workers))

    task_info := new(TaskInfo, allocator)
    task_info.procedure = task
    task_info.data = data
    task_info.max_thread_count = nb_threads
    task_info.group = group
    stg.queue_init(&task_info.queue)
    task_info.ready_list_index = -1
    switch _ in task {
    case (stg.TaskProcStandard): task_info.kind = .Standard
    case (stg.TaskProcShared):
        task_info.kind = .Shared
        sync.barrier_init(&task_info.shared_space.barrier, nb_threads)
    }
    append(&group.tasks[task_info.kind], task_info)
    runner.tasks[task] = task_info
}

add_job :: proc(runner: ^Runner, task: stg.TaskProc, data: stg.Data) {
    assert(task in runner.tasks)
    task_info := cast(^TaskInfo)runner.tasks[task]
    group := cast(^WorkerGroup)task_info.group
    stg.queue_push(&task_info.queue, data)
    if sync.guard(&group.ready_lists[task_info.kind].mutex) {
        if sync.guard(&task_info.mutex) {
            add_task_info_to_ready_list(task_info)
            if task_info.cur_thread_count < task_info.max_thread_count &&
               task_info.cur_thread_count < int(stg.queue_size(&task_info.queue)) {
                sync.cond_signal(&group.cond) // wakeup a worker for the processing
            }
        }
    }
}

@(private)
worker_run :: proc(worker: ^Worker) {
    prof.register_thread()
    group := cast(^WorkerGroup)worker.group
    for {
        if sync.guard(&group.mutex) { // sleep
            for {
                if sync.atomic_load(&worker.can_terminate) do return
                if sync.atomic_load(&group.needed_shared_worker_count) > 0 do break
                if ready_list_size(&group.ready_lists[.Standard]) > 0 do break
                if ready_list_size(&group.ready_lists[.Shared]) > 0 do break
                sync.cond_wait(&group.cond, &group.mutex)
            }
        }
        process_tasks(worker)
    }
}

@(private)
process_tasks :: proc(worker: ^Worker) {
    group := cast(^WorkerGroup)worker.group
    if sync.atomic_load(&group.needed_shared_worker_count) > 0 {
        if sync.atomic_sub(&group.needed_shared_worker_count, 1) > 1 {
            process_shared_tasks(worker, true)
        } else {
            sync.atomic_add(&group.needed_shared_worker_count, 1)
        }
    }
    for ready_list_size(&group.ready_lists[.Standard]) > 0 {
        process_standard_tasks(worker) or_break
    }
    for ready_list_size(&group.ready_lists[.Shared]) > 0 {
        process_shared_tasks(worker, false) or_break
    }
}

@(private)
process_standard_tasks :: proc(worker: ^Worker) -> bool {
    group := cast(^WorkerGroup)worker.group
    task_info: ^TaskInfo
    task_found := false

    if sync.guard(&group.ready_lists[.Standard].mutex) {
        // TODO: for the standard tasks, we should not always start at 0
        for idx := 0; idx < len(group.ready_lists[.Standard].datas); idx += 1 {
            task_info = group.ready_lists[.Standard].datas[idx]
            if sync.guard(&task_info.mutex) {
                if task_info.cur_thread_count < task_info.max_thread_count && stg.queue_size(&task_info.queue) > 0 {
                    worker.local_id = task_info.cur_thread_count
                    task_info.cur_thread_count += 1
                    task_found = true
                    break
                }
            }
        }
    }

    // execute task
    if !task_found {
        return false
    }
    run_standard_task(worker, task_info)

    //reset
    if sync.guard(&group.ready_lists[.Standard].mutex) {
        if sync.guard(&task_info.mutex) {
            remove_task_info_from_ready_list(task_info)
            task_info.cur_thread_count -= 1
        }
    }
    return true
}

@(private)
run_standard_task :: proc(worker: ^Worker, task_info: ^TaskInfo) {
    ctx := stg.TaskContext{worker, task_info, &task_info.shared_space}
    for {
        data := stg.queue_pop(&task_info.queue) or_break
        task_info.procedure.(stg.TaskProcStandard)(ctx, data)
    }
}

@(private)
process_shared_tasks :: proc(worker: ^Worker, is_helper: bool) -> bool {
    group := cast(^WorkerGroup)worker.group
    task_info: ^TaskInfo
    task_found := false

    if sync.guard(&group.ready_lists[.Shared].mutex) {
        for idx := 0; idx < len(&group.ready_lists[.Shared].datas); idx += 1 {
            task_info = group.ready_lists[.Shared].datas[idx]
            if sync.guard(&task_info.mutex) {
                if is_helper && task_info.cur_thread_count == 0 do break // security to avoid helper threads to start a new task
                if task_info.cur_thread_count < task_info.max_thread_count && stg.queue_size(&task_info.queue) > 0 {
                    worker.local_id = task_info.cur_thread_count
                    task_info.cur_thread_count += 1
                    if worker.local_id == 0 {
                        sync.atomic_add(&group.needed_shared_worker_count, task_info.max_thread_count - 1)
                        sync.cond_broadcast(&group.cond)
                    } else {
                        // helper threads update this counter early to avoid to many
                        // threads to enter this function before processing the
                        // standard tasks
                        if !is_helper do sync.atomic_sub(&group.needed_shared_worker_count, 1)
                    }
                    task_found = true
                    break
                }
            }
        }
    }

    // execute task
    if !task_found {
        return false
    }
    run_shared_task(worker, task_info)
    if worker.local_id > 0 do return true

    // reset
    if sync.guard(&group.ready_lists[.Shared].mutex) {
        if sync.guard(&task_info.mutex) {
            remove_task_info_from_ready_list(task_info)
            task_info.cur_thread_count = 0
        }
    }
    return true
}

@(private)
run_shared_task :: proc(worker: ^Worker, task_info: ^TaskInfo) {
    ctx := stg.TaskContext{worker, task_info, &task_info.shared_space}
    for {
        sync.barrier_wait(&task_info.shared_space.barrier)
        if worker.local_id == 0 {
            task_info.shared_space.data = nil
            if data, ok := stg.queue_pop(&task_info.queue); ok {
                task_info.shared_space.data = data
            }
        }
        sync.barrier_wait(&task_info.shared_space.barrier)
        data := task_info.shared_space.data.? or_break
        task_info.procedure.(stg.TaskProcShared)(ctx, data, worker.local_id, task_info.max_thread_count)
    }
}

// ready list //////////////////////////////////////////////////////////////////

ReadyList :: struct {
    mutex: sync.Mutex,
    datas: [dynamic]^TaskInfo,
}

ready_list_init :: proc(list: ^ReadyList, size: int, allocator := context.allocator) {
    list.datas = make([dynamic]^TaskInfo, 0, size, allocator = allocator)
}

ready_list_destroy :: proc(list: ^ReadyList) {
    delete(list.datas)
}

ready_list_size :: proc(list: ^ReadyList) -> int {
    sync.lock(&list.mutex)
    defer sync.unlock(&list.mutex)
    return len(list.datas)
}

//
// note: those functions expect the ready list and the task to be locked
//

@(private)
add_task_info_to_ready_list :: proc(task_info: ^TaskInfo) {
    if task_info == nil || task_info.ready_list_index >= 0 do return
    group := cast(^WorkerGroup)task_info.group
    list := &group.ready_lists[task_info.kind]
    append(&list.datas, task_info)
    task_info.ready_list_index = len(list.datas) - 1
}

@(private)
remove_task_info_from_ready_list :: proc(task_info: ^TaskInfo) {
    if task_info == nil || task_info.ready_list_index < 0 || stg.queue_size(&task_info.queue) > 0 do return
    group := cast(^WorkerGroup)task_info.group
    idx := task_info.ready_list_index
    task_kind := task_info.kind
    ready_count := len(group.ready_lists[task_kind].datas)

    if ready_count > 1 {
        group.ready_lists[task_kind].datas[idx] = group.ready_lists[task_kind].datas[ready_count - 1]
        group.ready_lists[task_kind].datas[idx].ready_list_index = idx
    }
    pop(&group.ready_lists[task_kind].datas)
    task_info.ready_list_index = -1
}
