package dgemM

import "cblas"
import "core:testing"
import "core:fmt"
import "core:log"
import "core:sync"
import "core:time"
import "core:container/queue"
import "core:mem"
import vmem "core:mem/virtual"
import "../../"
import "../common"
import lr "../../lock_runner"
import prof "../../profiler"

ftype :: f64

Matrix :: struct {
    rows, cols: uint,
    data: [dynamic]ftype,
}

MatrixA :: distinct Matrix
MatrixB :: distinct Matrix
MatrixC :: distinct Matrix

MatrixTile :: struct {
    mat: ^Matrix,
    row, col: uint,
    rows, cols: uint,
    data: []ftype,
    ld: uint,
}

MatrixTileA :: distinct MatrixTile
MatrixTileB :: distinct MatrixTile
MatrixTileC :: distinct MatrixTile
MatrixTileP :: distinct MatrixTile

dgemm :: proc(A, B, C: Matrix) {
    M := C.rows
    N := C.cols
    K := A.cols
    cblas.dgemm(.NoTrans, .NoTrans, M, N, K, 1.0, A.data[:], K, B.data[:], N, 0, C.data[:], N)
}

SplitMatrixTaskData :: struct {
    tile_size: uint,
    data_pool: ^stg.MultiPool,
}

alloc_matrix_tile :: proc(pool: ^stg.MultiPool, type: typeid) -> ^MatrixTile {
    switch type {
    case MatrixA: return cast(^MatrixTile)stg.multi_pool_alloc(pool, MatrixTileA)
    case MatrixB: return cast(^MatrixTile)stg.multi_pool_alloc(pool, MatrixTileB)
    case MatrixC: return cast(^MatrixTile)stg.multi_pool_alloc(pool, MatrixTileC)
    }
    panic("invalid use of alloc_matrix_tile")
}

split_matrix_task :: proc(ctx: stg.TaskContext, data: stg.Data) {
    prof.procedure()
    self := stg.task_data(ctx, SplitMatrixTaskData)

    m := stg.data_ptr(data, Matrix)
    tid := stg.data_type(data)
    tile_rows := m.rows / self.tile_size + (m.rows % self.tile_size == 0 ? 0 : 1)
    tile_cols := m.cols / self.tile_size + (m.cols % self.tile_size == 0 ? 0 : 1)

    log.debug("split matrix", tid)

    for row : uint = 0; row < m.rows; row += self.tile_size {
        for col : uint = 0; col < m.cols; col += self.tile_size {
            tile := alloc_matrix_tile(self.data_pool, tid)
            assert(tile != nil)
            tile.mat = m
            tile.row = row / self.tile_size
            tile.col = col / self.tile_size
            tile.rows = min(self.tile_size, m.rows - row)
            tile.cols = min(self.tile_size, m.cols - col)
            tile.data = m.data[row * m.cols + col:]
            tile.ld = m.cols
            switch tid {
            case MatrixA: stg.add_job(ctx, product_state, stg.make_data(cast(^MatrixTileA)tile))
            case MatrixB: stg.add_job(ctx, product_state, stg.make_data(cast(^MatrixTileB)tile))
            case MatrixC: stg.add_job(ctx, sum_state, stg.make_data(cast(^MatrixTileC)tile))
            case: panic("we should not arrive here")
            }
        }
    }
}

ProductData :: struct {
    a: ^MatrixTileA,
    b: ^MatrixTileB,
    p: ^MatrixTileP,
}

ProductStateData :: struct {
    a_tiles: [dynamic]^MatrixTileA,
    b_tiles: [dynamic]^MatrixTileB,
    TM, TN, TK: uint,
    data_pool: ^stg.MultiPool,
}

product_state :: proc(ctx: stg.TaskContext, data: stg.Data) {
    prof.procedure()
    self := stg.task_data(ctx, ProductStateData)
    tile := stg.data_ptr(data, MatrixTile)
    tid := stg.data_type(data)

    log.debugf("product state received tile: {}[{}, {}] (@{})", tid, tile.row, tile.col, uintptr(tile))

    product := proc(ctx: stg.TaskContext, self: ^ProductStateData, a: ^MatrixTileA, b: ^MatrixTileB) {
        product_data, err := stg.multi_pool_alloc_dynamic(self.data_pool, ProductData)
        ensure(err == nil, "failed to allocate ProductData")
        product_data.a = a
        product_data.b = b
        product_data.p = stg.multi_pool_alloc(self.data_pool, MatrixTileP, wait = true)
        product_data.p.row = a.row
        product_data.p.col = b.col
        stg.add_job(ctx, product_task, stg.make_data(product_data))
    }

    switch tid {
    case MatrixTileA:
        a := stg.data_ptr(data, MatrixTileA)
        assert(self.a_tiles[a.row * self.TK + a.col] == nil)
        self.a_tiles[a.row * self.TK + a.col] = a
        for col in 0..<self.TN {
            b := self.b_tiles[tile.col * self.TN + col]
            if b != nil do product(ctx, self, a, b)
        }
    case MatrixTileB:
        b := stg.data_ptr(data, MatrixTileB)
        assert(self.b_tiles[b.row * self.TN + b.col] == nil)
        self.b_tiles[b.row * self.TN + b.col] = b
        for row in 0..<self.TM {
            a := self.a_tiles[row * self.TK + tile.row]
            if a != nil do product(ctx, self, a, b)
        }
    case:
        log.errorf("product_state: type `{}` is not handled by this task.", tid)
        panic("product_state: invalid type")
    }
}

product_task :: proc(ctx: stg.TaskContext, data: stg.Data) {
    prof.procedure()
    tiles := stg.data_ptr(data, ProductData)
    log.debugf("product received, P[{}, {}] = A[{}, {}] * B[{}, {}]",
        tiles.p.row, tiles.p.col, tiles.a.row, tiles.a.col, tiles.b.row, tiles.b.col)

    a := tiles.a
    b := tiles.b
    p := tiles.p
    cblas.dgemm(.NoTrans, .NoTrans, a.rows, b.cols, a.cols, 1.0, a.data, a.ld,
                b.data, b.ld, 0, p.data, p.ld)
    stg.add_job(ctx, sum_state, data)
}

SumQueue :: struct {
    c: ^MatrixTileC,
    ps: queue.Queue(^MatrixTileP),
}

SumData :: struct {
    p: ^MatrixTileP,
    c: ^MatrixTileC,
    link: ^SumData,
}

SumStateData :: struct {
    queues: [dynamic]SumQueue,
    TM, TN, TK: uint,
    progress_counter: uint,
    job: ^stg.Job,
    data_pool: ^stg.MultiPool,
}

sum_state :: proc(ctx: stg.TaskContext, data: stg.Data) {
    prof.procedure()
    self := stg.task_data(ctx, SumStateData)
    tid := stg.data_type(data)

    switch tid {
    case MatrixTileC:
        c := stg.data_ptr(data, MatrixTileC)
        log.debugf("sum state received tile: C[{}, {}] (@{})", c.row, c.col, uintptr(c))

        q := &self.queues[c.row * self.TN + c.col]
        if p, ok := queue.pop_front_safe(&q.ps); ok {
            sum_data, err := stg.multi_pool_alloc_dynamic(self.data_pool, SumData)
            ensure(err == nil)
            sum_data.c = c
            sum_data.p = p
            stg.add_job(ctx, sum_task, stg.make_data(sum_data))
        } else {
            q.c = c
        }
    case ProductData:
        pd := stg.data_ptr(data, ProductData)
        log.debugf("sum state received product data: P[{}, {}] (@{})", pd.p.row, pd.p.col, uintptr(pd))
        p := pd.p
        stg.multi_pool_release(self.data_pool, pd)

        q := &self.queues[p.row * self.TN + p.col]
        if q.c != nil {
            sum_data, err := stg.multi_pool_alloc_dynamic(self.data_pool, SumData)
            ensure(err == nil)
            sum_data.c = q.c
            sum_data.p = p
            q.c = nil
            stg.add_job(ctx, sum_task, stg.make_data(sum_data))
        } else {
            queue.enqueue(&q.ps, p)
        }
    case SumData:
        sd := stg.data_ptr(data, SumData)
        log.debugf("sum state received sum data: C[{}, {}] / {} (@{})", sd.p.row, sd.p.col, self.progress_counter - 1, uintptr(sd))

        stg.multi_pool_release(self.data_pool, sd.p)

        self.progress_counter -= 1
        if self.progress_counter == 0 {
            stg.job_done(self.job)
            return
        }

        q := &self.queues[sd.c.row * self.TN + sd.c.col]
        if p, ok := queue.pop_front_safe(&q.ps); ok {
            sd.p = p
            stg.add_job(ctx, sum_task, stg.make_data(sd))
        } else {
            q.c = sd.c
            stg.multi_pool_release(self.data_pool, sd)
        }
    case:
        log.errorf("sum_state: type `{}` is not handled by this task.", tid)
        panic("sum_state: invalid type")
    }
}

sum_task :: proc(ctx: stg.TaskContext, data: stg.Data) {
    prof.procedure()
    tiles := stg.data_ptr(data, SumData)
    log.debugf("sum task received data: C[{}, {}] (@{})", tiles.c.row, tiles.c.col, uintptr(tiles))

    c := tiles.c
    p := tiles.p
    for row in 0..<tiles.c.rows {
        for col in 0..<tiles.c.cols {
            c.data[row * c.ld + col] += p.data[row * p.ld + col]
        }
    }
    stg.add_job(ctx, sum_state, data)
}

// C[MxN] = A[MxK] * B[KxN]
stg_dgemm :: proc(A, B, C: Matrix, tile_size: uint) {
    prof.procedure()

    log.info("create runner..")
    runner: lr.Runner
    lr.runner_init(&runner, 40)
    defer lr.runner_fini(&runner)


    A := cast(MatrixA)A
    B := cast(MatrixB)B
    C := cast(MatrixC)C
    assert(A.rows == C.rows)
    assert(B.cols == C.cols)
    assert(A.cols == B.rows)

    TM := C.rows / tile_size + (C.rows % tile_size == 0 ? 0 : 1)
    TN := C.cols / tile_size + (C.cols % tile_size == 0 ? 0 : 1)
    TK := A.cols / tile_size + (A.cols % tile_size == 0 ? 0 : 1)

    log.info("setup pools...")
    data_pool: stg.MultiPool
    stg.multi_pool_init_type(&data_pool, MatrixTileA, TM * TK)
    stg.multi_pool_init_type(&data_pool, MatrixTileB, TK * TN)
    stg.multi_pool_init_type(&data_pool, MatrixTileC, TM * TN)
    stg.multi_pool_init_type(&data_pool, MatrixTileP, TM * TN * TK, tile_size,
                             proc(p: ^MatrixTileP, tile_size: uint, allocator: mem.Allocator) {
                                 data := make([dynamic]ftype, tile_size * tile_size, allocator)
                                 p.data = data[:]
                                 p.rows = tile_size
                                 p.cols = tile_size
                                 p.ld = tile_size
                             })
    stg.multi_pool_init_type(&data_pool, ProductData, 0)
    stg.multi_pool_init_type(&data_pool, SumData, 0)
    defer stg.multi_pool_destroy(&data_pool)

    arena: vmem.Arena
    arena_err := vmem.arena_init_growing(&arena)
    ensure(arena_err == nil, "failed to initialize virtual arena")
    defer vmem.arena_destroy(&arena)
    allocator := vmem.arena_allocator(&arena)

    log.info("setup task data...")
    split_matrix_task_data := SplitMatrixTaskData{tile_size, &data_pool}
    product_state_data := ProductStateData{make([dynamic]^MatrixTileA, TM * TK, allocator = allocator),
                                           make([dynamic]^MatrixTileB, TK * TN, allocator = allocator),
                                           TM, TN, TK, &data_pool}
    sum_state_data := SumStateData{make([dynamic]SumQueue, TM * TN, allocator = allocator), TM, TN, TK, TM * TN * TK, nil, &data_pool}
    for &q in sum_state_data.queues do ensure(queue.init(&q.ps, capacity = int(TK / 3), allocator = allocator) == nil)

    log.info("add tasks...")
    stg.add_task(&runner, split_matrix_task, 3, data = &split_matrix_task_data)
    stg.add_task(&runner, product_state, data = &product_state_data)
    stg.add_task(&runner, product_task, 40)
    stg.add_task(&runner, sum_state, data = &sum_state_data)
    stg.add_task(&runner, sum_task, 40)

    log.info("start runner...")
    lr.runner_start(&runner)

    tracker := stg.job()
    sum_state_data.job = &tracker
    stg.add_job(&runner, split_matrix_task, stg.make_data(&A))
    stg.add_job(&runner, split_matrix_task, stg.make_data(&B))
    stg.add_job(&runner, split_matrix_task, stg.make_data(&C))
    stg.job_wait(&tracker)
}

MatrixInitKind :: enum {Zero, Int, Float}

matrix_init :: proc(m: ^Matrix, rows, cols: uint, init_kind: MatrixInitKind) {
    m.rows = rows
    m.cols = cols
    m.data = make([dynamic]ftype, rows * cols)
    if init_kind == .Zero do return
    if init_kind == .Int {
        for i in 0..<m.rows {
            for j in 0..<m.cols {
                m.data[i * m.cols + j] = ftype(i + j + 1)
            }
        }
    } else {
        for i in 0..<m.rows {
            for j in 0..<m.cols {
                m.data[i * m.cols + j] = ftype(1 - ((i + 1) / (j + 1)))
            }
        }
    }
}

matrix_destroy :: proc(m: ^Matrix) {
    delete(m.data)
}

matrix_print :: proc(m: Matrix, name: string) {
    fmt.println(name, "=")
    for i in 0..<m.rows {
        for j in 0..<m.cols {
            fmt.printf("{}  ", m.data[i * m.cols + j])
        }
        fmt.println()
    }
}

@(test)
test_small_int :: proc(t: ^testing.T) {
    MATRIX_SIZE :: 4
    TILE_SIZE :: 2
    A, B, C, E: Matrix

    cblas.openblas_set_num_threads(1);

    matrix_init(&A, MATRIX_SIZE, MATRIX_SIZE, .Int)
    defer matrix_destroy(&A)
    matrix_init(&B, MATRIX_SIZE, MATRIX_SIZE, .Int)
    defer matrix_destroy(&B)
    matrix_init(&C, MATRIX_SIZE, MATRIX_SIZE, .Zero)
    defer matrix_destroy(&C)
    matrix_init(&E, MATRIX_SIZE, MATRIX_SIZE, .Zero)
    defer matrix_destroy(&E)

    dgemm(A, B, E)
    stg_dgemm(A, B, C, TILE_SIZE)

    when ODIN_DEBUG {
        matrix_print(A, "A")
        matrix_print(B, "B")
        matrix_print(E, "E")
        matrix_print(C, "C")
    }
    testing.expect(t, common.matrix_eq(C.data[:], E.data[:]))
}

@(test)
test_medium :: proc(t: ^testing.T) {
    MATRIX_SIZE :: 1024
    TILE_SIZE :: 64
    A, B, C, E: Matrix

    cblas.openblas_set_num_threads(1);

    matrix_init(&A, MATRIX_SIZE, MATRIX_SIZE, .Float)
    defer matrix_destroy(&A)
    matrix_init(&B, MATRIX_SIZE, MATRIX_SIZE, .Float)
    defer matrix_destroy(&B)
    matrix_init(&C, MATRIX_SIZE, MATRIX_SIZE, .Zero)
    defer matrix_destroy(&C)
    matrix_init(&E, MATRIX_SIZE, MATRIX_SIZE, .Zero)
    defer matrix_destroy(&E)

    dgemm(A, B, E)
    stg_dgemm(A, B, C, TILE_SIZE)
    testing.expect(t, common.matrix_eq(C.data[:], E.data[:]))
}

main :: proc() {
    log.info("init profiler...")
    prof.init()
    defer {
        prof.print_report_to_file("report.dot", .Dot)
        prof.fini()
    }

    MATRIX_SIZE :: 10000
    TILE_SIZE :: 1024

    logger := log.create_console_logger(.Error, {.Level, .Short_File_Path, .Line, .Procedure, .Terminal_Color, .Thread_Id})
    defer log.destroy_console_logger(logger)
    context.logger = logger

    Data :: struct {
        A, B, C, E: Matrix
    }
    data: Data

    matrix_init(&data.A, MATRIX_SIZE, MATRIX_SIZE, .Float)
    defer matrix_destroy(&data.A)
    matrix_init(&data.B, MATRIX_SIZE, MATRIX_SIZE, .Float)
    defer matrix_destroy(&data.B)
    matrix_init(&data.C, MATRIX_SIZE, MATRIX_SIZE, .Zero)
    defer matrix_destroy(&data.C)

    cblas.openblas_set_num_threads(1);

    sw: time.Stopwatch
    time.stopwatch_start(&sw)
    stg_dgemm(data.A, data.B, data.C, TILE_SIZE)
    time.stopwatch_stop(&sw)
    dur := prof.duration_to_string(time.stopwatch_duration(sw))
    defer delete(dur)
    fmt.printfln("stg_dgemm: {}", dur)
}
