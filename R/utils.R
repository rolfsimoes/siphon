# Compat: base R only provides %||% from 4.4.0; DESCRIPTION requires >= 4.1.0
`%||%` <- function(x, y) if (is.null(x)) y else x

# Monotonic-enough millisecond clock for all internal duration accounting
.pump_now_ms <- function() {
    proc.time()[["elapsed"]] * 1000
}

# Human-readable backend name for status and print output
.pump_backend_name <- function(backend) {
    if (inherits(backend, "pump_main_backend")) {
        "main"
    } else if (inherits(backend, "pump_mirai_backend")) {
        "mirai"
    } else if (inherits(backend, "pump_future_backend")) {
        "future"
    } else if (inherits(backend, "pump_parallel_backend")) {
        "parallel"
    } else {
        "unknown"
    }
}
