package stg

import "core:sync"
import "base:intrinsics"

// spin barrier ////////////////////////////////////////////////////////////////

SpinBarrier :: struct {
    counter: int,
}

spin_barrier_wait :: proc(barrier: ^SpinBarrier, count: int) {
    if sync.atomic_add(&barrier.counter, 1) == count - 1 {
        sync.atomic_store(&barrier.counter, 0)
        return
    }
    for {
        if sync.atomic_load(&barrier.counter) == 0 do break
        intrinsics.cpu_relax()
    }
}
