# Compat: base R only provides %||% from 4.4.0; DESCRIPTION requires >= 4.1.0
`%||%` <- function(x, y) if (is.null(x)) y else x

# Monotonic-enough millisecond clock for all internal duration accounting
.pump_now_ms <- function() {
    proc.time()[["elapsed"]] * 1000
}

# Per-session counter for stage registration keys, pid-qualified so two R
# sessions sharing one daemon pool cannot collide.
.pump_state <- new.env(parent = emptyenv())

.pump_stage_key <- function() {
    n <- (.pump_state$stage_counter %||% 0L) + 1L
    .pump_state$stage_counter <- n
    sprintf(".siphon_stage_%d_%d", Sys.getpid(), n)
}

# How long .pump_backend_quiesce() waits for in-flight jobs to deliver
# their results before giving a node up, in seconds
.pump_quiesce_timeout <- function() {
    timeout <- getOption("siphon.quiesce_timeout", 30)
    if (!is.numeric(timeout) || length(timeout) != 1L ||
            is.na(timeout) || timeout < 0) {
        stop(
            "options(siphon.quiesce_timeout =) must be a non-negative ",
            "number of seconds.",
            call. = FALSE
        )
    }
    timeout
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
