.pump_resolve_backend <- function(backend) {
    if (!is.character(backend)) {
        return(backend)
    }
    switch(backend,
        future = future_backend(),
        mirai = mirai_backend(),
        main = main_backend(),
        stop("invalid backend")
    )
}

.pump_executor_count <- function(backend) {
    UseMethod(".pump_executor_count")
}
.pump_executor_new_job <- function(backend, func, args) {
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
.pump_backend_close <- function(backend) {
    UseMethod(".pump_backend_close")
}
#' @export
.pump_backend_close.pump_backend <- function(backend) {
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
    invisible(x)
}
