package stg

// TODO: this runner should use a single queue for the shared and standar task
// - here we will not utilize the lock free queue because we need batch operations for the shared tasks (batch enqueue)
// - shared tasks: we will probably enqueue N jobs (N is the number of shared workers), but we will do it only once (active task status makes sure the tasks is being processed)
// - not using lock free structures here will make things easier, we can still change later
// - we also need a special dequeue system for the state (the worker could have an internal queue and do a batch dequeue in some circomptsences, but nor work stealing yet)
