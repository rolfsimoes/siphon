#' Create a main-thread backend
#'
#' `main_backend()` executes jobs synchronously in the current R process. It is
#' the default backend and is useful for stages that must stay on the main
#' thread, such as GPU dispatchers.
#'
#' @return A backend object.
#' @examples
#' f <- 1:5 |> pump(function(x) x * 2, backend = main_backend())
#' pump_run(f, verbose = FALSE)
#' @export
main_backend <- function() {
    structure(
        list(name = "main", owned = FALSE),
        class = c("pump_main_backend", "pump_backend")
    )
}

#' @export
.pump_executor_count.pump_main_backend <- function(backend) {
    1L
}
#' @export
.pump_executor_register.pump_main_backend <- function(backend, func, args) {
    list(func = func, args = args)
}
#' @export
.pump_executor_new_job.pump_main_backend <- function(backend, handle, data) {
    result <- .make_job(do.call(handle$func, c(list(data), handle$args)))
    structure(list(result = result), class = "pump_main_job")
}
#' @export
.pump_job_is_ready.pump_main_job <- function(job) TRUE
#' @export
.pump_job_data.pump_main_job <- function(job) job$result
