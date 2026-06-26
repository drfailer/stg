package stg

import "core:sync"
import "base:intrinsics"

// cancelable barrier //////////////////////////////////////////////////////////

DONE_BIT_MASK :: u32(1 << 31)
CANCELED_BIT_MASK :: u32(1 << 30)
THREAD_COUNTER_MASK :: ~(DONE_BIT_MASK | CANCELED_BIT_MASK)

CancelBarrier :: struct {
    state: [2]u32,
    mutex: sync.Atomic_Mutex,
}

unpack_state :: proc(state: u32) -> (thread_counter: u32, done, canceled: bool) {
    thread_counter = state & THREAD_COUNTER_MASK
    done = (state & DONE_BIT_MASK) != 0
    canceled = (state & CANCELED_BIT_MASK) != 0
    return
}

// should return false when canceled
cancel_barrier_wait :: proc(barrier: ^CancelBarrier, state_index: ^u8, count: u32) -> (success: bool) {
    assert(count <= THREAD_COUNTER_MASK)
    defer {
        // INVARIANT: the next state is ready for use
        state_index^ = (state_index^ + 1) & 1
    }

    state := sync.atomic_load(&barrier.state[state_index^])
    thread_counter, done, canceled := unpack_state(state)
    if canceled do return false

    thread_counter = sync.atomic_add(&barrier.state[state_index^], 1) + 1
    if thread_counter >= count {
        // INVARIANT: if we arrive here, we haven't been caneled
        sync.atomic_store(&barrier.state[1 - state_index^], 0)
        sync.atomic_store(&barrier.state[state_index^], DONE_BIT_MASK)
        return true

    }
    // INVARIANT: if we arrive here, we need to wait for anothe thread to signal success or cancel
    for {
        state = sync.atomic_load(&barrier.state[state_index^])
        thread_counter, canceled, done = unpack_state(state)
        if canceled do return false
        if done do return true
        intrinsics.cpu_relax()
    }
    return true
}

cancel_barrier_cancel :: proc(barrier: ^CancelBarrier, state_index: ^u8) {
    defer {
        // INVARIANT: the next state is ready for use
        state_index^ = (state_index^ + 1) & 1
    }
    // we use the lock to force late threads to wait for the reset before going
    // to the next barrier (assuming they all go through this function before
    // going to the next barrier)
    if sync.guard(&barrier.mutex) {
        state := sync.atomic_load(&barrier.state[state_index^])
        thread_counter, done, canceled := unpack_state(state)
        if canceled do return
        sync.atomic_store(&barrier.state[1 - state_index^], 0)
        sync.atomic_store(&barrier.state[state_index^], CANCELED_BIT_MASK)
    }
}

// spin barrier ////////////////////////////////////////////////////////////////

SpinBarrier :: struct {
    counter: uint,
}

spin_barrier_wait :: proc(barrier: ^SpinBarrier, count: uint) {
    if sync.atomic_add(&barrier.counter, 1) == count - 1 {
        sync.atomic_store(&barrier.counter, 0)
        return
    }
    for {
        if sync.atomic_load(&barrier.counter) == 0 do break
        intrinsics.cpu_relax()
    }
}
