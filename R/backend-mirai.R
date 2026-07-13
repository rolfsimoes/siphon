#' Create a mirai backend
#'
#' `mirai_backend()` submits jobs through `mirai::mirai()`. The number of
#' available slots is read from `mirai::status()$connections`.
#'
#' @details Note: When using mirai_backend(), you are responsible for managing
#'   the mirai daemon lifecycle. Call `mirai::daemons(n)` to start workers and
#'   `mirai::daemons(0)` to shut them down. See the vignette for examples.
#'
#'   Fault tolerance is delegated to the `mirai` framework: this backend
#'   performs no retries. If a daemon dies while running a job, the
#'   resulting `errorValue` is surfaced as a `pump_error` value for that
#'   item (subject to the `on_error` policy) rather than leaking into the
#'   pipeline. For siphon-managed recovery with retries, see
#'   [parallel_backend()].
#'
#' @return A backend object.
#' @examples
#' if (requireNamespace("mirai", quietly = TRUE) &&
#'     mirai::status()$connections > 0) {
#'     f <- 1:5 |>
#'         pump(function(x) x * 2, backend = mirai_backend())
#'     pump_run(f, verbose = FALSE)
#' }
#' @export
mirai_backend <- function() {
    if (!requireNamespace("mirai", quietly = TRUE)) {
        stop(
            "Package 'mirai' is required for mirai_backend() ",
            "but is not installed. Please install it with ",
            "install.packages('mirai')."
        )
    }
    structure(list(), class = "pump_mirai_backend")
}

#' @export
.pump_executor_count.pump_mirai_backend <- function(backend) {
    mirai::status()$connections
}
#' @export
.pump_executor_new_job.pump_mirai_backend <- function(backend, func, args) {
    m <- mirai::mirai(
        .make_job(do.call(func, args)),
        .args = list(.make_job = .make_job, func = func, args = args)
    )
    structure(list(result = m), class = "pump_mirai_job")
}
#' @export
.pump_job_is_ready.pump_mirai_job <- function(job) {
    !mirai::unresolved(job$result)
}
#' @export
.pump_job_data.pump_mirai_job <- function(job) {
    if (mirai::unresolved(job$result)) {
        return(NULL)
    }
    res <- job$result[]
    if (mirai::is_error_value(res)) {
        msg <- if (is.numeric(res)) {
            nanonext::nng_error(as.integer(res))
        } else {
            as.character(res)
        }
        err <- simpleError(paste0("mirai worker failed: ", msg))
        class(err) <- c("pump_error", class(err))
        res <- list(value = err, fn_time = 0)
    }
    res
}

#' Print a mirai backend
#'
#' @param x A mirai backend object.
#' @param ... Unused.
#' @export
print.pump_mirai_backend <- function(x, ...) {
    cat("<pump_mirai_backend>\n")
    cat("  workers: ", .pump_executor_count(x), "\n", sep = "")
    invisible(x)
}
