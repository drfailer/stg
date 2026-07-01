package common

matrix_eq :: proc(C, E: []f64) -> bool {
    if len(C) != len(E) do return false
    for i in 0..<len(C) {
        if C[i] != E[i] do return false
    }
    return true
}
