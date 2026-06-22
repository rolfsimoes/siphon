#' Create a mirai backend
#'
#' `mirai_backend()` submits jobs through `mirai::mirai()`. The number of
#' available slots is read from `mirai::status()$connections`.
#'
#' @details Note: When using mirai_backend(), you are responsible for managing
#'   the mirai daemon lifecycle. Call `mirai::daemons(n)` to start workers and
#'   `mirai::daemons(0)` to shut them down. See the vignette for examples.
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
    if (!mirai::unresolved(job$result)) job$result[]
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
