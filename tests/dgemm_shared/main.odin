package dgemm_shared

import "core:fmt"
import "core:log"
import "core:testing"
import "core:time"
import "../../"
import "../common"
import lr "../../lock_runner"
import "../dgemm/cblas"
import prof "../../profiler"

DgemmParams :: struct {
    A, B, C: []f64,
    N, M, K: uint,
    block_w, block_h: uint,
}

dgemm_task :: proc(ctx: stg.TaskContext, data: stg.Data, thread_idx, thread_count: int) {
    prof.procedure()
    params := stg.data_ptr(data, DgemmParams)
    block_size := params.block_w * params.block_h
    block_row_size := params.N / params.block_w

    log.debugf("thread {}/{} enter dgemm task", thread_idx, thread_count)

    // each thread will compute a portion of the matrix
    prof.region_begin("process_blocks")
    for bi: uint = 0; ; bi += 1 {
        block_idx := bi * uint(thread_count) + uint(thread_idx)
        block_x := block_idx % block_row_size
        block_y := block_idx / block_row_size
        c_row_idx := block_y * params.block_h
        c_col_idx := block_x * params.block_w

        if c_row_idx >= params.M || c_col_idx >= params.N do break

        C := params.C[c_row_idx * params.N + c_col_idx:]

        for k: uint = 0; k < params.K; k += params.block_w {
            a_col_idx := k
            a_row_idx := c_row_idx
            A := params.B[a_row_idx * params.K + a_col_idx:]

            b_col_idx := c_col_idx
            b_row_idx := k
            B := params.A[b_row_idx * params.N + b_col_idx:]

            // compute safe dimensions
            M := min(params.block_h, params.M - c_row_idx)
            N := min(params.block_w, params.N - c_col_idx)
            K := min(params.block_w, params.K - k)

            // log.infof("dgemm({}, {}, {}, A[{},{}], {}, B[{},{}], {}, C[{},{}], {})", M, N, K,
            //     a_row_idx, a_col_idx, params.K, b_row_idx, b_col_idx, params.N, c_row_idx, c_col_idx, params.N)
            cblas.dgemm(.NoTrans, .NoTrans, M, N, K, 1, A, params.K, B, params.N, 1, C, params.N)
        }
    }
    prof.region_end("process_blocks")

    prof.region_begin("sync")
    stg.sync(ctx)
    prof.region_end("sync")

    // complete the job at the end
    if thread_idx == 0 do stg.job_done(data)
}

stg_dgemm :: proc(A, B, C: []f64, M, N, K: uint, block_w, block_h: uint, thread_count: int) {
    prof.procedure()

    runner: lr.Runner
    lr.runner_init(&runner, thread_count)
    defer lr.runner_fini(&runner)

    stg.add_task(&runner, dgemm_task, thread_count)
    lr.runner_start(&runner)

    params := DgemmParams{A, B, C, M, N, K, block_w, block_h}
    job := stg.job()
    stg.add_job(&runner, dgemm_task, stg.make_data(&params, &job))
    stg.job_wait(&job)
}

MatrixInitKind :: enum {Zero, Int, Float}
matrix_init :: proc(data: ^[dynamic]f64, rows, cols: uint, init_kind: MatrixInitKind) {
    data^ = make([dynamic]f64, rows * cols)
    if init_kind == .Zero do return
    if init_kind == .Int {
        for i in 0..<rows {
            for j in 0..<cols {
                data[i * cols + j] = f64(i + j + 1)
            }
        }
    } else {
        for i in 0..<rows {
            for j in 0..<cols {
                data[i * cols + j] = f64(1 - ((i + 1) / (j + 1)))
            }
        }
    }
}

matrix_print :: proc(data: []f64, rows, cols: uint, name: string) {
    fmt.println(name, "=")
    for i in 0..<rows {
        for j in 0..<cols {
            fmt.printf("{}  ", data[i * cols + j])
        }
        fmt.println()
    }
}

@(test)
test_small_int :: proc(t: ^testing.T) {
    MATRIX_SIZE :: 4
    TILE_SIZE :: 2
    A, B, C, E: [dynamic]f64

    cblas.openblas_set_num_threads(1);

    matrix_init(&A, MATRIX_SIZE, MATRIX_SIZE, .Int)
    defer delete(A)
    matrix_init(&B, MATRIX_SIZE, MATRIX_SIZE, .Int)
    defer delete(B)
    matrix_init(&C, MATRIX_SIZE, MATRIX_SIZE, .Zero)
    defer delete(C)
    matrix_init(&E, MATRIX_SIZE, MATRIX_SIZE, .Zero)
    defer delete(E)

    cblas.dgemm(.NoTrans, .NoTrans, MATRIX_SIZE, MATRIX_SIZE, MATRIX_SIZE,
        1, A[:], MATRIX_SIZE, B[:], MATRIX_SIZE, 0, E[:], MATRIX_SIZE)
    stg_dgemm(A[:], B[:], C[:], MATRIX_SIZE, MATRIX_SIZE, MATRIX_SIZE, TILE_SIZE, TILE_SIZE, 4)

    when ODIN_DEBUG {
        matrix_print(A[:], MATRIX_SIZE, MATRIX_SIZE, "A")
        matrix_print(B[:], MATRIX_SIZE, MATRIX_SIZE, "B")
        matrix_print(E[:], MATRIX_SIZE, MATRIX_SIZE, "E")
        matrix_print(C[:], MATRIX_SIZE, MATRIX_SIZE, "C")
    }
    testing.expect(t, common.matrix_eq(C[:], E[:]))
}

@(test)
test_medium :: proc(t: ^testing.T) {
    MATRIX_SIZE :: 1024
    TILE_SIZE :: 64
    A, B, C, E: [dynamic]f64

    cblas.openblas_set_num_threads(1);

    matrix_init(&A, MATRIX_SIZE, MATRIX_SIZE, .Float)
    defer delete(A)
    matrix_init(&B, MATRIX_SIZE, MATRIX_SIZE, .Float)
    defer delete(B)
    matrix_init(&C, MATRIX_SIZE, MATRIX_SIZE, .Zero)
    defer delete(C)
    matrix_init(&E, MATRIX_SIZE, MATRIX_SIZE, .Zero)
    defer delete(E)

    cblas.dgemm(.NoTrans, .NoTrans, MATRIX_SIZE, MATRIX_SIZE, MATRIX_SIZE,
        1, A[:], MATRIX_SIZE, B[:], MATRIX_SIZE, 0, E[:], MATRIX_SIZE)
    stg_dgemm(A[:], B[:], C[:], MATRIX_SIZE, MATRIX_SIZE, MATRIX_SIZE, TILE_SIZE, TILE_SIZE, 40)
    testing.expect(t, common.matrix_eq(C[:], E[:]))
}

main :: proc() {
    prof.init()
    defer {
        prof.print_report_to_file("report.dot", .Dot)
        prof.fini()
    }

    MATRIX_SIZE :: 10000
    BLOCK_H :: 512
    BLOCK_W :: 512
    A, B, C: [dynamic]f64

    cblas.openblas_set_num_threads(1);

    logger := log.create_console_logger(.Error, {.Level, .Time, .Short_File_Path, .Line, .Terminal_Color, .Procedure, .Thread_Id})
    defer log.destroy_console_logger(logger)
    context.logger = logger

    matrix_init(&A, MATRIX_SIZE, MATRIX_SIZE, .Int)
    matrix_init(&B, MATRIX_SIZE, MATRIX_SIZE, .Int)
    matrix_init(&C, MATRIX_SIZE, MATRIX_SIZE, .Zero)

    sw: time.Stopwatch
    time.stopwatch_start(&sw)
    stg_dgemm(A[:], B[:], C[:], MATRIX_SIZE, MATRIX_SIZE, MATRIX_SIZE, BLOCK_W, BLOCK_H, 40)
    time.stopwatch_stop(&sw)
    dur := prof.duration_to_string(time.stopwatch_duration(sw))
    defer delete(dur)
    fmt.printfln("stg_dgemm: {}", dur)
    when MATRIX_SIZE <= 8 {
        matrix_print(A[:], MATRIX_SIZE, MATRIX_SIZE, "A")
        matrix_print(B[:], MATRIX_SIZE, MATRIX_SIZE, "B")
        matrix_print(C[:], MATRIX_SIZE, MATRIX_SIZE, "C")
    }
}
