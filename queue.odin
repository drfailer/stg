package stg

import "core:sync"
import "core:time"
import "core:fmt"
import "core:strings"
import "base:intrinsics"


// lock free queue /////////////////////////////////////////////////////////////

/*
 * This is the implementation of the lock free queue described by Maged M.
 * Michael and Michael L. Scott in "Simple, Fast, and Practical Non-Blocking
 * and Blocking Concurrent Queue Algorithms".
 * paper link: https://www.cs.rochester.edu/u/scott/papers/1996_PODC_queues.pdf
 */

LockFreeQueueNodePtr :: struct($T: typeid) #align(16) {
    ptr: ^LockFreeQueueNode(T),
    count: uint,
}

LockFreeQueueNode :: struct($T: typeid) #align(CACHE_LINE) {
    data: T,
    next: LockFreeQueueNodePtr(T),
}

LockFreeQueue :: struct($T: typeid) {
    head: LockFreeQueueNodePtr(T),
    tail: LockFreeQueueNodePtr(T),
    free: LockFreeQueueNodePtr(T),
}

lock_free_queue_init :: proc(queue: ^LockFreeQueue($T)) {
    node := lock_free_queue_allocate_node(queue)
    queue.head = {node, 0}
    queue.tail = {node, 0}
}

// NOTE(atomic): this function is supposed to be executed by a single thread,
//               therefore we don't use atomics here.
lock_free_queue_destroy :: proc(queue: ^LockFreeQueue($T)) {
    // used nodes
    node := queue.head
    for node.ptr != nil {
        next := node.ptr.next
        free(node.ptr)
        node = next
    }
    // free nodes
    fnode := queue.free
    for fnode.ptr != nil {
        next := fnode.ptr.next
        free(fnode.ptr)
        fnode = next
    }
}

lock_free_queue_push :: proc(queue: ^LockFreeQueue($T), value: T) {
    tail, next: LockFreeQueueNodePtr(T)
    node := lock_free_queue_allocate_node(queue)

    node.data = value
    atomic_store16(&node.next, LockFreeQueueNodePtr(T){nil, 0}, .Relaxed)
    for {
        tail = atomic_load16(&queue.tail, .Acquire)
        next = atomic_load16(&tail.ptr.next, .Acquire)

        if tail == atomic_load16(&queue.tail, .Acquire) {
            if next.ptr == nil {
                new_next := LockFreeQueueNodePtr(T){node, next.count + 1}
                if _, ok := atomic_compare_exchange_weak16(&tail.ptr.next, next, new_next, .Release); ok {
                    break
                } else {
                    intrinsics.cpu_relax()
                }
            } else {
                new_tail := LockFreeQueueNodePtr(T){next.ptr, tail.count + 1}
                atomic_compare_exchange_weak16(&queue.tail, tail, new_tail, .Release)
                intrinsics.cpu_relax()
            }
        }
    }
    new_tail := LockFreeQueueNodePtr(T){node, tail.count + 1}
    atomic_compare_exchange_weak16(&queue.tail, tail, new_tail, .Release)
}

lock_free_queue_pop :: proc(queue: ^LockFreeQueue($T)) -> (result: T, popped: bool) {
    head, tail, next: LockFreeQueueNodePtr(T)

    for {
        head = atomic_load16(&queue.head, .Acquire)
        tail = atomic_load16(&queue.tail, .Acquire)
        next = atomic_load16(&head.ptr.next, .Acquire)

        if head == atomic_load16(&queue.head, .Acquire) {
            if head == tail {
                if next.ptr == nil {
                    return result, false
                }
                new_tail := LockFreeQueueNodePtr(T){next.ptr, tail.count + 1}
                atomic_compare_exchange_weak16(&queue.tail, tail, new_tail, .Release)
                intrinsics.cpu_relax()
            } else {
                result = next.ptr.data
                new_head := LockFreeQueueNodePtr(T){next.ptr, head.count + 1}
                if _, ok := atomic_compare_exchange_weak16(&queue.head, head, new_head, .Release); ok {
                    break
                } else {
                    intrinsics.cpu_relax()
                }
            }
        }
    }
    lock_free_queue_release_node(queue, head.ptr)
    return result, true
}

lock_free_queue_allocate_node :: proc(queue: ^LockFreeQueue($T)) -> ^LockFreeQueueNode(T) {
    free: LockFreeQueueNodePtr(T)

    for {
        free = atomic_load16(&queue.free, .Acquire)
        if free.ptr == nil {
            return new(LockFreeQueueNode(T))
        }
        next := atomic_load16(&free.ptr.next, .Acquire)
        new_free := LockFreeQueueNodePtr(T){next.ptr, next.count + 1}
        if val, ok := atomic_compare_exchange_weak16(&queue.free, free, new_free, .Release); ok {
            return val.ptr
        }
    }
}


lock_free_queue_release_node :: proc(queue: ^LockFreeQueue($T), node: ^LockFreeQueueNode(T)) {
    assert(node != nil)
    free: LockFreeQueueNodePtr(T)

    // TODO: add a counter and a max pool size so we don't keep increasing the
    //       pool size infinitely

    for {
        free = atomic_load16(&queue.free, .Acquire)
        atomic_store16(&node.next, LockFreeQueueNodePtr(T){free.ptr, free.count + 1}, .Relaxed)
        new_free := LockFreeQueueNodePtr(T){node, free.count + 1}
        if _, ok := atomic_compare_exchange_weak16(&queue.free, free, new_free, .Release); ok {
            break
        }
    }
}

// finite mpmc queue ///////////////////////////////////////////////////////////

/*
 * Implementation of Dmitry Vyukov MPMC queue.
 */

FiniteMPMCQueue :: struct($T: typeid, $SIZE: uint) #align(CACHE_LINE) {
    datas: [SIZE]T,
    indices: [SIZE]uint,
    head: uint,
    tail: uint,
}

finite_mpmc_queue_init :: proc(queue: ^FiniteMPMCQueue($T, $SIZE)) {
    for i: uint = 0; i < SIZE; i += 1 {
        queue.indices[i] = i
    }
}

finite_mpmc_queue_destroy :: proc(queue: ^FiniteMPMCQueue($T, $SIZE)) {}

finite_mpmc_queue_push :: proc(queue: ^FiniteMPMCQueue($T, $SIZE), value: T) -> bool {
    t := sync.atomic_load(&queue.tail)
    mask := SIZE - 1
    ok: bool

    for {
        seq := sync.atomic_load_explicit(&queue.indices[t & mask], .Acquire)
        diff := int(seq) - int(t)
        if diff == 0 {
            if t, ok = sync.atomic_compare_exchange_weak(&queue.tail, t, t + 1); ok {
                break
            }
        } else if diff < 0 {
            return false
        } else {
            intrinsics.cpu_relax()
            t = sync.atomic_load(&queue.tail)
        }
    }
    queue.datas[t & mask] = value
    sync.atomic_store_explicit(&queue.indices[t & mask], t + 1, .Release)
    return true
}

finite_mpmc_queue_pop :: proc(queue: ^FiniteMPMCQueue($T, $SIZE)) -> (result: T, popped: bool) {
    h := sync.atomic_load(&queue.head)
    mask := SIZE - 1
    ok: bool

    for {
        seq := sync.atomic_load_explicit(&queue.indices[h & mask], .Acquire)
        diff := int(seq) - int(h + 1)

        if diff == 0 {
            if h, ok = sync.atomic_compare_exchange_weak( &queue.head, h, h + 1); ok {
                break
            }
        } else if diff < 0 {
            return result, false
        } else {
            intrinsics.cpu_relax()
            h = sync.atomic_load(&queue.head)
        }
    }
    result = queue.datas[h & mask]
    sync.atomic_store_explicit(&queue.indices[h & mask], h + SIZE, .Release)
    return result, true
}

// mpmc queue //////////////////////////////////////////////////////////////////

// This is a hack to be able to use the finite lock free queue (which is much
// faster than the linked list implementation), and have a backup queue to
// mitigate the overflow. There are more complicated versions of the Dmitry
// Vyukov queue that can grow, but those are not as fast and too complex to
// justify their usage here. On top of that, when the size of the data that
// flows within the graph is well tuned, we should not overflow in a finite
// queue.
//
// NOTE: when there is an overflow, this queue does not keep the order, but
//       this is not important in our case.

MPMCQueue :: struct($T: typeid, $SIZE: uint) #align(CACHE_LINE) {
    // TODO(atomics16): we should use atomic16_is_supported to determin wether we need a lock queue or a lock free queue
    overflow_queue: LockFreeQueue(T),
    finite_queue: FiniteMPMCQueue(T, SIZE),
    size: uint,
}

mpmc_queue_init :: proc(queue: ^MPMCQueue($T, $SIZE)) {
    lock_free_queue_init(&queue.overflow_queue)
    finite_mpmc_queue_init(&queue.finite_queue)
}

mpmc_queue_destroy :: proc(queue: ^MPMCQueue($T, $SIZE)) {
    lock_free_queue_destroy(&queue.overflow_queue)
    finite_mpmc_queue_destroy(&queue.finite_queue)
}

mpmc_queue_push :: proc(queue: ^MPMCQueue($T, $SIZE), value: T) {
    if !finite_mpmc_queue_push(&queue.finite_queue, value) {
        lock_free_queue_push(&queue.overflow_queue, value)
    }
    sync.atomic_add(&queue.size, 1)
}

mpmc_queue_pop :: proc(queue: ^MPMCQueue($T, $SIZE)) -> (result: T, popped: bool) {
    result, popped = finite_mpmc_queue_pop(&queue.finite_queue)
    if popped {
        sync.atomic_sub(&queue.size, 1)
        return result, true
    }
    result, popped = lock_free_queue_pop(&queue.overflow_queue)
    if popped {
        sync.atomic_sub(&queue.size, 1)
        return result, true
    }
    return result, false
}

mpmc_queue_size :: proc(queue: ^MPMCQueue($T, $SIZE)) -> uint {
    return sync.atomic_load(&queue.size)
}

// procedure groups ////////////////////////////////////////////////////////////

queue_init :: proc {
    lock_free_queue_init,
    finite_mpmc_queue_init,
    mpmc_queue_init,
}

queue_destroy :: proc {
    lock_free_queue_destroy,
    finite_mpmc_queue_destroy,
    mpmc_queue_destroy,
}

queue_push :: proc {
    lock_free_queue_push,
    finite_mpmc_queue_push,
    mpmc_queue_push,
}

queue_pop :: proc {
    lock_free_queue_pop,
    finite_mpmc_queue_pop,
    mpmc_queue_pop,
}

queue_size :: proc {
    mpmc_queue_size,
}
