# Compat: base R only provides %||% from 4.4.0; DESCRIPTION requires >= 4.1.0
`%||%` <- function(x, y) if (is.null(x)) y else x

# Monotonic-enough millisecond clock for all internal duration accounting
.pump_now_ms <- function() {
    proc.time()[["elapsed"]] * 1000
}

# Human-readable backend name for status and print output
.pump_backend_name <- function(backend) {
    backend$name %||% "unknown"
}

# Shared guard for backend constructors with soft dependencies
.pump_need_pkg <- function(pkg, what) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
        stop(
            "Package '", pkg, "' is required for ", what,
            " but is not installed. Please install it with ",
            "install.packages('", pkg, "')."
        )
    }
}
