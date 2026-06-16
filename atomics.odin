#+private
package stg

import "core:sync"

atomic_load16 :: proc(val: ^$T, $mode: sync.Atomic_Memory_Order) -> T {
    return transmute(T)sync.atomic_load_explicit(transmute(^u128)val, mode)
}

atomic_store16 :: proc(dst: ^$T, val: T, $mode: sync.Atomic_Memory_Order) {
    sync.atomic_store_explicit(transmute(^u128)dst, transmute(u128)val, mode)
}

atomic_compare_exchange_weak16 :: proc(dst: ^$T, old, new: T, $success: sync.Atomic_Memory_Order) -> (T, bool) {
    result, ok := sync.atomic_compare_exchange_weak_explicit(transmute(^u128)dst, transmute(u128)old, transmute(u128)new, success, .Relaxed)
    return transmute(T)result, ok
}

