#' Create a future backend
#'
#' `future_backend()` submits jobs through `future::future()`. The number of
#' available slots is read from `future::nbrOfWorkers()`.
#'
#' @details Note: When using future_backend(), you are responsible for managing
#'   the future plan lifecycle. Call `future::plan()` to set a plan and restore
#'   the previous plan when done. See the vignette for examples.
#'
#'   Fault tolerance is delegated to the `future` framework: this backend
#'   performs no retries. If a worker dies while running a job, the
#'   resulting `FutureError` is surfaced as a `pump_error` value for that
#'   item (subject to the `on_error` policy) rather than aborting the
#'   pipeline. For siphon-managed recovery with retries, see
#'   [parallel_backend()].
#'
#' @return A backend object.
#' @examples
#' if (requireNamespace("future", quietly = TRUE)) {
#'     old_plan <- future::plan("sequential")
#'     on.exit(future::plan(old_plan), add = TRUE)
#'     f <- 1:5 |>
#'         pump(function(x) x * 2, backend = future_backend())
#'     pump_run(f, verbose = FALSE)
#' }
#' @export
future_backend <- function() {
    .pump_need_pkg("future", "future_backend()")
    structure(
        list(name = "future", owned = FALSE),
        class = c("pump_future_backend", "pump_backend")
    )
}

#' @export
.pump_executor_count.pump_future_backend <- function(backend) {
    future::nbrOfWorkers()
}
#' @export
.pump_executor_new_job.pump_future_backend <- function(backend, func, args) {
    make_job <- .make_job
    f <- future::future(
        make_job(do.call(func, args)),
        globals = list(make_job = make_job, func = func, args = args)
    )
    structure(list(result = f), class = "pump_future_job")
}
#' @export
.pump_job_is_ready.pump_future_job <- function(job) {
    future::resolved(job$result)
}
#' @export
.pump_job_data.pump_future_job <- function(job) {
    if (!future::resolved(job$result)) {
        return(NULL)
    }
    tryCatch(
        future::value(job$result),
        FutureError = function(e) .pump_job_failure(e)
    )
}
