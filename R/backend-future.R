#' Create a future backend
#'
#' `future_backend()` submits jobs through `future::future()`. The number of
#' available slots is read from `future::nbrOfWorkers()`.
#'
#' @details Note: When using future_backend(), you are responsible for managing
#'   the future plan lifecycle. Call `future::plan()` to set a plan and restore
#'   the previous plan when done. See the vignette for examples.
#'
#'   Globals required by the stage function are detected automatically by
#'   the `future` framework, exactly as in a plain `future::future()` call.
#'   Because plans provide no persistent worker state, the function and its
#'   detected globals travel with every job; for stages with large captured
#'   state, prefer [mirai_backend()] or [parallel_backend()], which install
#'   the stage payload on each worker once.
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
.pump_executor_register.pump_future_backend <- function(backend, func, args) {
    list(func = func, args = args)
}
#' @export
.pump_executor_new_job.pump_future_backend <- function(backend, handle, data) {
    # No explicit globals: naming them would disable future's automatic
    # detection, which is what ships the globals that job_fn itself uses.
    make_job <- .make_job
    environment(make_job) <- globalenv()
    job_fn <- handle$func
    job_args <- c(list(data), handle$args)
    f <- future::future(
        make_job(do.call(job_fn, job_args))
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
