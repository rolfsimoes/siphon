#' Create a custom backend from plain functions
#'
#' `pump_custom_backend()` builds a siphon backend from a handful of plain R
#' functions, so tools with their own execution machinery (job queues,
#' remote services, worker pools) can plug into `pump()` pipelines without
#' touching siphon internals.
#'
#' @details
#' siphon drives every backend through the same small contract. You supply
#' each operation as a function:
#'
#' * `count()` is consulted when stages are sized and validated: it must
#'   return the number of jobs the backend can run concurrently, at least 1.
#' * `register(func, args)` is called once per stage, after `open()`. It
#'   receives the stage function and its constant extra arguments, and
#'   returns an opaque *handle* that later submissions can use. This is the
#'   place to install per-stage state on your workers so each job ships
#'   only its item data. When omitted, the handle is
#'   `list(func = <stage function>, args = <constant arguments>)`.
#' * `submit(handle, data)` starts one job for one item and returns a job
#'   *token* without blocking. The job must compute
#'   `func(data, ...constant args...)` - with the default handle,
#'   `do.call(handle$func, c(list(data), handle$args))`.
#' * `is_ready(token)` is polled between beats and must return `TRUE` once
#'   `collect()` will not block. It must never block itself.
#' * `collect(token)` returns the finished job's value. Return a condition
#'   object (e.g. from `tryCatch(..., error = identity)`) to mark the item
#'   as *failed*: the pipeline then applies the stage's `on_error` policy.
#'   An error *thrown* by `collect()` itself is treated as an
#'   infrastructure failure and aborts the pipeline. Note that a condition
#'   returned this way is always interpreted as failure - if your items can
#'   legitimately be condition objects, wrap them in a list.
#' * `open()` (optional) is called once before the backend is first used -
#'   the place for lazy resource acquisition. siphon guarantees at most one
#'   `open()` call per backend object.
#'
#' Resource lifecycle stays yours, as with [mirai_backend()] daemons and
#' [future_backend()] plans: shut down whatever `open()` or your
#' constructor started when you no longer need the backend.
#'
#' Per-item function timing (`fn_time` in [pump_status()]) is not measured
#' for custom backends and reports as zero; scheduling and dispatch timings
#' are reported as usual.
#'
#' @param name A single string identifying the backend in `print()` and
#'   [pump_status()] output.
#' @param count A function with no arguments returning the number of
#'   concurrently runnable jobs.
#' @param submit A function `function(handle, data)` starting one job and
#'   returning a job token.
#' @param is_ready A function `function(token)` returning `TRUE` when the
#'   job's result can be collected without blocking.
#' @param collect A function `function(token)` returning the finished value
#'   (or a condition object to mark the item as failed).
#' @param register An optional function `function(func, args)` returning
#'   the stage handle passed to `submit()`. Defaults to bundling `func` and
#'   `args` in a list.
#' @param open An optional function with no arguments, called once before
#'   the backend is first used.
#'
#' @return A backend object usable as the `backend` argument of [pump()],
#'   [pump_run()], and [pump_drain()].
#' @examples
#' # A toy synchronous backend: runs each job at submit time
#' toy_backend <- pump_custom_backend(
#'     name = "toy",
#'     count = function() 1L,
#'     submit = function(handle, data) {
#'         tryCatch(
#'             do.call(handle$func, c(list(data), handle$args)),
#'             error = identity
#'         )
#'     },
#'     is_ready = function(token) TRUE,
#'     collect = function(token) token
#' )
#' f <- 1:5 |> pump(function(x) x * 2, backend = toy_backend)
#' pump_run(f, verbose = FALSE)
#' @seealso `vignette("extending", package = "siphon")` for a walkthrough,
#'   including an asynchronous example.
#' @export
pump_custom_backend <- function(name,
                                count,
                                submit,
                                is_ready,
                                collect,
                                register = NULL,
                                open = NULL) {
    if (!is.character(name) || length(name) != 1L || is.na(name) ||
            !nzchar(name)) {
        stop("name must be a single non-empty string.", call. = FALSE)
    }
    for (arg in c("count", "submit", "is_ready", "collect")) {
        if (!is.function(get(arg, inherits = FALSE))) {
            stop(arg, " must be a function.", call. = FALSE)
        }
    }
    if (!is.null(register) && !is.function(register)) {
        stop("register must be NULL or a function.", call. = FALSE)
    }
    if (!is.null(open) && !is.function(open)) {
        stop("open must be NULL or a function.", call. = FALSE)
    }

    state <- new.env(parent = emptyenv())
    state$opened <- FALSE

    structure(
        list(
            name = name,
            owned = FALSE,
            count = count,
            submit = submit,
            is_ready = is_ready,
            collect = collect,
            register = register,
            open = open,
            state = state
        ),
        class = c("pump_custom_backend", "pump_backend")
    )
}

#' @export
.pump_backend_open.pump_custom_backend <- function(backend) {
    if (!backend$state$opened) {
        if (is.function(backend$open)) {
            backend$open()
        }
        backend$state$opened <- TRUE
    }
    invisible(backend)
}
#' @export
.pump_executor_count.pump_custom_backend <- function(backend) {
    as.integer(backend$count())
}
#' @export
.pump_executor_register.pump_custom_backend <- function(backend, func, args) {
    if (is.function(backend$register)) {
        backend$register(func, args)
    } else {
        list(func = func, args = args)
    }
}
#' @export
.pump_executor_new_job.pump_custom_backend <- function(backend, handle, data) {
    token <- backend$submit(handle, data)
    structure(
        list(backend = backend, token = token),
        class = "pump_custom_job"
    )
}
#' @export
.pump_job_is_ready.pump_custom_job <- function(job) {
    isTRUE(job$backend$is_ready(job$token))
}
#' @export
.pump_job_data.pump_custom_job <- function(job) {
    if (!.pump_job_is_ready(job)) {
        return(NULL)
    }
    value <- job$backend$collect(job$token)
    if (inherits(value, "condition")) {
        .pump_job_failure(value)
    } else {
        .pump_job_result(value, fn_time = 0)
    }
}
