//
// Simple Task Graph (STG)
//

package stg

import "core:sync"
import "core:thread"
import "core:log"

// job /////////////////////////////////////////////////////////////////////////

Job :: struct {
    mutex: sync.Mutex,
    cond: sync.Cond,
    steps: int,
}

job :: proc(step_count := 1) -> Job {
    return Job{steps = step_count}
}

job_done_from_job :: proc(job: ^Job) {
    if sync.atomic_sub(&job.steps, 1) <= 1 {
        sync.cond_broadcast(&job.cond)
    }
}

job_done_from_data :: proc(data: Data) {
    ensure(data.job != nil, "called `job_done` with nil data job is nil")
    job_done_from_job(data.job)
}

job_done :: proc { job_done_from_job, job_done_from_data }

job_wait :: proc(job: ^Job) {
    sync.mutex_lock(&job.mutex)
    for {
        if sync.atomic_load(&job.steps) <= 0 do break
        sync.cond_wait(&job.cond, &job.mutex)
    }
    sync.mutex_unlock(&job.mutex)
}

// data ////////////////////////////////////////////////////////////////////////

Data :: struct {
    type: typeid,
    ptr: rawptr,
    job: ^Job,
    // pool: ^DataPool,
}

data_ptr :: proc(data: Data, $T: typeid) -> ^T {
    return cast(^T)data.ptr
}

data_type :: proc(data: Data) -> typeid {
    return data.type
}

make_data_data :: proc(ptr: ^$T, job: ^Job = nil) -> Data
    where T != Job {
    return Data{T, ptr, job}
}

make_data_job :: proc(job: ^Job) -> Data {
    return Data{Job, nil, job}
}

make_data :: proc { make_data_data, make_data_job }

// tasks ///////////////////////////////////////////////////////////////////////

TaskKind :: enum {
    Standard,
    Shared,
}

TaskInfo :: struct {
    kind: TaskKind,
    procedure: TaskProc,
    data: rawptr,
    shared_space: SharedSpace,
    max_thread_count: int,
}

SharedSpace :: struct {
    barrier: sync.Barrier,
    user_barriers: [8]SpinBarrier,
    data: Maybe(Data),
    impl: rawptr,
}

TaskContext :: struct {
    runner: ^Runner,
    worker: ^Worker,
    task_info: ^TaskInfo,
    shared_space: ^SharedSpace,
}

TaskProcStandard :: proc(ctx: TaskContext, data: Data)
TaskProcShared :: proc(ctx: TaskContext, data: Data, thread_index, thread_count: int)

TaskProc :: union {
    TaskProcStandard,
    TaskProcShared,
}

add_job_data :: proc(ctx: TaskContext, task: TaskProc, data: Data) {
    runner_add_job_data(ctx.runner, task, data)
}

add_job_job :: proc(ctx: TaskContext, task: TaskProc, job: ^Job) {
    runner_add_job_job(ctx.runner, task, job)
}

add_job :: proc { add_job_data, add_job_job, runner_add_job_data, runner_add_job_job }

thread_id :: proc(ctx: TaskContext) -> int {
    return ctx.worker.id
}

task_data :: proc(ctx: TaskContext, $T: typeid) -> ^T {
    return cast(^T)ctx.task_info.data
}

sync :: proc(ctx: TaskContext, count: int = 0, branch_id := 0) {
    ensure(branch_id < len(ctx.shared_space.user_barriers))
    count := count if count > 0 else ctx.task_info.max_thread_count
    spin_barrier_wait(&ctx.shared_space.user_barriers[branch_id], count)
}

// TODO: we need a `last` function that return true when count is reached but that do not block the other threads

// runner //////////////////////////////////////////////////////////////////////

Runner :: struct {
    // TODO: should the runner have a multi-pool?
    add_group: proc(runner: ^Runner, nb_threads: int) -> int,
    add_task: proc(runner: ^Runner, group: int, task: TaskProc, nb_threads: int, data: rawptr),
    add_job: proc(runner: ^Runner, task: TaskProc, data: Data),
}

Worker :: struct {
    id: int,
    local_id: int,
}

add_group :: proc(runner: ^Runner, nb_threads: int) -> int {
    return runner.add_group(runner, nb_threads)
}

add_task :: proc(runner: ^Runner, task: TaskProc, nb_threads: int = 1, data: rawptr = nil, group := 0) {
    runner.add_task(runner, group, task, nb_threads, data)
}

runner_add_job_data :: proc(runner: ^Runner, task: TaskProc, data: Data) {
    runner.add_job(runner, task, data)
}

runner_add_job_job :: proc(runner: ^Runner, task: TaskProc, job: ^Job) {
    runner.add_job(runner, task, make_data(job))
}
