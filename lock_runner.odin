package stg

// TODO: this runner should use a single queue for the shared and standar task
// - here we will not utilize the lock free queue because we need batch operations for the shared tasks (batch enqueue)
// - shared tasks: we will probably enqueue N jobs (N is the number of shared workers), but we will do it only once (active task status makes sure the tasks is being processed)
// - not using lock free structures here will make things easier, we can still change later
// - we also need a special dequeue system for the state (the worker could have an internal queue and do a batch dequeue in some circomptsences, but nor work stealing yet)

// block the shared data
// if another_thread_needs_helpers() {
//   do not dequeue and go help;
// } else {
//   dequeue and request help for the queue configuration
// }
//
// Problem: processing multiple elements?
// - we can still organize the queues per task, but a single thread can access all the infos at a time
// - we maintain a ready list as well

// On the standard task side, the dequeue can be optimized by locking only the
// ready list and go to a random ready task. The ready list should be an non
// sorted array of tasks (just put a task in it when it is ready): this will
// allow random selection of the ready task.
//
// We also need one queue per task here because we still want to be able to
// limit the number of threads processing the queue (we cannot just have a
// queue of tasks).
