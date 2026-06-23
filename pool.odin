package stg

import "base:runtime"
import "core:sync"
import "core:mem"
import vmem "core:mem/virtual"

MultiPool :: struct {
    pools: map[typeid]Pool,
}

// call this once to destroy all the pools (do not work if the types requrire
// complex deinitialization)
multi_pool_destroy :: proc(multi_pool: ^MultiPool) {
    for _, &pool in multi_pool.pools {
        vmem.arena_destroy(&pool.arena)
    }
    delete(multi_pool.pools)
}

multi_pool_init_type_default :: proc(multi_pool: ^MultiPool, $T: typeid, count: uint) -> runtime.Allocator_Error {
    return multi_pool_init_type_callback(multi_pool, T, count, 0, nil)
}

multi_pool_init_type_callback :: proc(multi_pool: ^MultiPool, $T: typeid, count: uint, init_data: $E,
                             init_elem: proc(data: ^T, init_data: E, pool_allocator: mem.Allocator),
                             loc := #caller_location) -> runtime.Allocator_Error {
    ensure(T not_in multi_pool.pools, "cannot init the same type twice in the multi-pool", loc = loc)
    multi_pool.pools[T] = Pool{}
    return pool_init(&multi_pool.pools[T], T, count, init_data, init_elem)
}

multi_pool_init_type :: proc{ multi_pool_init_type_default, multi_pool_init_type_callback }

multi_pool_fini_type :: proc(multi_pool: ^MultiPool, $T: typeid, count: int, fini_data: $E,
                             fini_elem: proc(data: ^T, fini_data: E) = nil,
                             loc := #caller_location) {
    ensure(T in multi_pool.pools, "the pool for the given type doesn not exsit in the multipool", loc = loc)
    pool_fini(&multi_pool.pools[T], T, fini_data, fini_elem)
    delete_key(&multi_pool.pools, T)
}

multi_pool_alloc :: proc(multi_pool: ^MultiPool, $T: typeid, wait := false, loc := #caller_location) -> ^T {
    ensure(T in multi_pool.pools, "the pool for the given type doesn not exsit in the multipool", loc = loc)
    return pool_alloc(&multi_pool.pools[T], T, wait)
}

multi_pool_alloc_dynamic :: proc(multi_pool: ^MultiPool, $T: typeid, init_data: $E = int(0),
                                 init_elem: proc(data: ^T, init_data: E, pool_allocator: mem.Allocator) = nil,
                                 loc := #caller_location) -> (^T, runtime.Allocator_Error) {
    ensure(T in multi_pool.pools, "the pool for the given type doesn not exsit in the multipool", loc = loc)
    return pool_alloc_dynamic(&multi_pool.pools[T], T, init_data, init_elem)
}

multi_pool_release :: proc(multi_pool: ^MultiPool, data: ^$T, loc := #caller_location) {
    ensure(T in multi_pool.pools, "the pool for the given type doesn not exsit in the multipool", loc = loc)
    pool_release(&multi_pool.pools[T], data)
}

// TODO: those should be private

Pool :: struct {
    element_type: typeid,
    element_size: uint,
    arena: vmem.Arena,
    mutex: sync.Mutex,
    cond: sync.Cond,
    free_list: ^PoolNodeHeader,
}

PoolNodeHeader :: struct {
    next: ^PoolNodeHeader,
    guard: uintptr, // used to make sure the released elements are valid pool nodes
}

@(private="file")
data_from_header :: proc(header: ^PoolNodeHeader, $T: typeid) -> ^T {
    data_ptr := uintptr(header) + size_of(PoolNodeHeader)
    return cast(^T)rawptr(data_ptr)
}

@(private="file")
header_from_data :: proc(data: ^$T) -> ^PoolNodeHeader {
    header_ptr := uintptr(data) - size_of(PoolNodeHeader)
    return cast(^PoolNodeHeader)header_ptr
}

@(private="file")
alloc_node :: proc(pool: ^Pool, $T: typeid, init_data: $E,
                  init_elem: proc(data: ^T, init_data: E, pool_allocator: mem.Allocator)) -> (node: ^PoolNodeHeader, err: runtime.Allocator_Error) {
    allocator := vmem.arena_allocator(&pool.arena)
    alloc_size := size_of(PoolNodeHeader) + pool.element_size
    data := vmem.arena_alloc(&pool.arena, alloc_size, align_of(T)) or_return
    node = cast(^PoolNodeHeader)raw_data(data)
    node.guard = uintptr(node)
    if init_elem != nil do init_elem(data_from_header(node, T), init_data, allocator)
    return node, nil
}


pool_init :: proc(pool: ^Pool, $T: typeid, count: uint, init_data: $E,
                  init_elem: proc(data: ^T, init_data: E, pool_allocator: mem.Allocator)) -> runtime.Allocator_Error {
    pool.element_type = T
    pool.element_size = size_of(T)
    vmem.arena_init_growing(&pool.arena) or_return

    for _ in 0..<count {
        node := alloc_node(pool, T, init_data, init_elem) or_return
        node.next = pool.free_list
        pool.free_list = node
    }
    return nil
}

// NOTE: we are not tracking the allocated elements, which means that if
//       fini_elem is not nil, the non released element will not be freed properly.
pool_fini :: proc(pool: ^Pool, $T: typeid, count: int, fini_data: $E = int(0),
                  fini_elem: proc(data: ^T, fini_data: E) = nil) {
    if fini_elem != nil {
        for node := pool.free_list; node != nil; {
            next := node.next
            fini_elem(data_from_header(node), fini_data)
            node = next
        }
    }
    vmem.arena_destroy(pool.arena)
}

pool_alloc :: proc(pool: ^Pool, $T: typeid, wait := false) -> ^T {
    sync.lock(&pool.mutex)
    defer sync.unlock(&pool.mutex)

    if pool.free_list == nil {
        if !wait do return nil
        for pool.free_list != nil do sync.cond_wait(&pool.cond, &pool.mutex)
    }
    node := pool.free_list
    pool.free_list = node.next
    node.next = nil
    return data_from_header(node, T)
}

pool_alloc_dynamic :: proc(pool: ^Pool, $T: typeid, init_data: $E = int(0),
                           init_elem: proc(data: ^T, init_data: E, pool_allocator: mem.Allocator) = nil) -> (data: ^T, err: runtime.Allocator_Error) {
    sync.lock(&pool.mutex)
    defer sync.unlock(&pool.mutex)

    if pool.free_list == nil {
        pool.free_list = alloc_node(pool, T, init_data, init_elem) or_return
    }
    node := pool.free_list
    pool.free_list = node.next
    node.next = nil
    return data_from_header(node, T), nil
}

pool_release :: proc(pool: ^Pool, data: ^$T) {
    sync.lock(&pool.mutex)
    defer sync.unlock(&pool.mutex)

    node := header_from_data(data)
    if node.guard != uintptr(node) do panic("tried to release data that does not belong to the pool")
    node.next = pool.free_list
    pool.free_list = node
}
