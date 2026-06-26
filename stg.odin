//
// Simple Task Graph (STG)
//

package stg

import "core:sync"
import "core:log"

// - init
// - create thread groups
// - declare tasks:
// - run: push_task(ctx, task_proc, data) / push_task(ctx, task_proc, data, ticket)

// job tracker /////////////////////////////////////////////////////////////////

JobTracker :: struct {
    mutex: sync.Mutex,
    cond: sync.Cond,
    steps: int,
}

job_tracker :: proc(step_count := 1) -> JobTracker {
    return JobTracker{steps = step_count}
}

job_done_tracker :: proc(tracker: ^JobTracker) {
    if sync.atomic_sub(&tracker.steps, 1) <= 1 {
        sync.cond_broadcast(&tracker.cond)
    }
}

job_done_data :: proc(data: Data) {
    ensure(data.job_tracker != nil, "called `job_done` with nil data job tracker is nil")
    job_done_tracker(data.job_tracker)
}

job_done :: proc { job_done_tracker, job_done_data }

job_wait :: proc(tracker: ^JobTracker) {
    sync.mutex_lock(&tracker.mutex)
    for {
        if sync.atomic_load(&tracker.steps) <= 0 do break
        sync.cond_wait(&tracker.cond, &tracker.mutex)
    }
    sync.mutex_unlock(&tracker.mutex)
}

// data ////////////////////////////////////////////////////////////////////////

Data :: struct {
    type: typeid,
    ptr: rawptr,
    job_tracker: ^JobTracker,
    // pool: ^DataPool,
}

data_ptr :: proc(data: Data, $T: typeid) -> ^T {
    return cast(^T)data.ptr
}

data_type :: proc(data: Data) -> typeid {
    return data.type
}

make_data :: proc(ptr: ^$T, job_tracker: ^JobTracker = nil) -> Data {
    return Data{T, ptr, job_tracker}
}

// tasks ///////////////////////////////////////////////////////////////////////

TaskContext :: struct {
    worker: ^Worker,
    task_info: ^TaskInfo,
    shared_space: ^SharedSpace,
}

SharedSpace :: struct {
    user_barriers: [8]SpinBarrier,
}

TaskProcStandard :: proc(ctx: TaskContext, data: Data)
TaskProcShared :: proc(ctx: TaskContext, data: Data, thread_index, thread_count: uint)

TaskProc :: union {
    TaskProcStandard,
    TaskProcShared,
}

push_job_task :: proc(ctx: TaskContext, task_proc: TaskProc, data := Data{}, tracker: ^JobTracker = nil) {
    push_job_runner(ctx.worker.group.runner, task_proc, data, tracker)
}

push_job :: proc {
    push_job_runner,
    push_job_task,
}

thread_id :: proc(ctx: TaskContext) -> int {
    return ctx.worker.id
}

task_data :: proc(ctx: TaskContext, $T: typeid) -> ^T {
    return cast(^T)ctx.task_info.data
}

sync :: proc(ctx: TaskContext, count: uint = 0, branch_id := 0) {
    ensure(branch_id < len(ctx.shared_space.user_barriers))
    count := count if count > 0 else ctx.task_info.thread_count
    spin_barrier_wait(&ctx.shared_space.user_barriers[branch_id], count)
}
