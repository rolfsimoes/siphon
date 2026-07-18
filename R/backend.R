.pump_resolve_backend <- function(backend) {
    if (is.character(backend)) {
        return(switch(backend,
            future = future_backend(),
            mirai = mirai_backend(),
            main = main_backend(),
            stop(
                "invalid backend \"", backend, "\"; use \"main\", ",
                "\"mirai\", \"future\", or a backend object ",
                "(parallel_backend() has no string alias)"
            )
        ))
    }
    if (!inherits(backend, "pump_backend")) {
        stop(
            "backend must be a backend object or one of ",
            "\"main\", \"mirai\", \"future\""
        )
    }
    backend
}

# A backend must be usable before jobs are dispatched to it.
.pump_check_backend <- function(backend) {
    if (.pump_executor_count(backend) < 1L) {
        if (inherits(backend, "pump_mirai_backend")) {
            stop(
                "No active mirai daemons found. ",
                "Please call mirai::daemons(n) before starting the pipeline."
            )
        }
        stop("backend must have at least one process")
    }
    invisible(backend)
}

# Effective slot count for a stage: the main backend is always serial, the
# default is the backend's executor count, and the result is capped by a
# finite upstream length and validated against the executor count.
.pump_size_max_workers <- function(backend, max_workers, n) {
    if (inherits(backend, "pump_main_backend")) {
        max_workers <- 1L
    } else if (is.null(max_workers)) {
        max_workers <- .pump_executor_count(backend)
    }
    if (is.finite(n)) {
        max_workers <- as.integer(max(1L, min(n, max_workers)))
    } else {
        max_workers <- as.integer(max(1L, max_workers))
    }
    if (max_workers > .pump_executor_count(backend)) {
        stop(
            "max_workers (", max_workers, ") exceeds executor count (",
            .pump_executor_count(backend), ") for backend"
        )
    }
    max_workers
}

.pump_executor_count <- function(backend) {
    UseMethod(".pump_executor_count")
}
# Called once per stage after the backend is open: installs the stage's
# static payload (func and constant args) wherever the backend can keep it
# and returns an opaque handle for .pump_executor_new_job(). Backends with
# persistent workers ship the payload once here so per-job traffic is only
# the item data.
.pump_executor_register <- function(backend, func, args) {
    UseMethod(".pump_executor_register")
}
.pump_executor_new_job <- function(backend, handle, data) {
    UseMethod(".pump_executor_new_job")
}
.pump_job_is_ready <- function(job) {
    UseMethod(".pump_job_is_ready")
}
.pump_job_data <- function(job) {
    UseMethod(".pump_job_data")
}

# Lifecycle verbs: transition between backend specification and live
# resource. Idempotent; the default is a no-op for backends whose resources
# are user-managed (main, mirai daemons, future plans).
.pump_backend_open <- function(backend) {
    UseMethod(".pump_backend_open")
}
#' @export
.pump_backend_open.pump_backend <- function(backend) {
    invisible(backend)
}
.pump_backend_close <- function(backend, ...) {
    UseMethod(".pump_backend_close")
}
#' @export
.pump_backend_close.pump_backend <- function(backend, ...) {
    invisible(backend)
}

# Constructors for the job-result contract: every completed job yields
# list(value, fn_time), with failures carrying a pump_error condition.
.pump_job_result <- function(value, fn_time) {
    list(value = value, fn_time = fn_time)
}
.pump_job_failure <- function(e) {
    list(value = .pump_error(e), fn_time = 0)
}

# Runs on workers: the body must stay self-contained (no siphon helpers),
# because backends ship it with its environment reset to globalenv().
.make_job <- function(expr) {
    start <- Sys.time()
    val <- tryCatch(expr, error = function(e) {
        class(e) <- unique(c("pump_error", class(e)))
        e
    })
    list(
        value = val,
        fn_time = as.numeric(difftime(Sys.time(), start, units = "secs")) * 1000
    )
}

#' Print a backend
#'
#' Displays the backend's name and current worker count.
#'
#' @param x A backend object.
#' @param ... Unused.
#' @return The input `x`, invisibly.
#' @export
print.pump_backend <- function(x, ...) {
    cat("<pump_", x$name, "_backend>\n", sep = "")
    cat("  workers: ", .pump_executor_count(x), "\n", sep = "")
    if (!is.null(x$note)) {
        cat("  ", x$note, "\n", sep = "")
    }
    invisible(x)
}
