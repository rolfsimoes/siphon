#' Create a mirai backend
#'
#' `mirai_backend()` submits jobs through `mirai::mirai()`. The number of
#' available slots is read from `mirai::status()$connections`.
#'
#' @details Note: When using mirai_backend(), you are responsible for managing
#'   the mirai daemon lifecycle. Call `mirai::daemons(n)` to start workers and
#'   `mirai::daemons(0)` to shut them down. See the vignette for examples.
#'
#'   When a stage first advances, its function and constant arguments are
#'   installed once on every connected daemon (via `mirai::everywhere()`);
#'   each job then ships only the item data. The daemon pool is therefore
#'   assumed to be static for the duration of a run: a daemon that connects
#'   after the stage registered does not have the stage payload and will
#'   fail jobs routed to it. Stage functions must be self-contained or
#'   carry their dependencies in their closure environment; objects they
#'   reference from the global environment are not shipped.
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
    .pump_need_pkg("mirai", "mirai_backend()")
    structure(
        list(name = "mirai", owned = FALSE),
        class = c("pump_mirai_backend", "pump_backend")
    )
}

#' @export
.pump_executor_count.pump_mirai_backend <- function(backend) {
    mirai::status()$connections
}
#' @export
.pump_executor_register.pump_mirai_backend <- function(backend, func, args) {
    key <- .pump_stage_key()
    make_job <- .make_job
    environment(make_job) <- globalenv()
    # Install a per-stage runner in every connected daemon's global
    # environment; jobs reference it by key so only the item data travels
    # per job. Daemons connecting later lack the runner (static-pool
    # assumption, see ?mirai_backend).
    # nolint start: object_usage_linter. The expression is quoted by
    # everywhere() and evaluated daemon-side with bindings from .args.
    installed <- mirai::everywhere(
        assign(key,
            function(data) make_job(do.call(func, c(list(data), args))),
            envir = globalenv()
        ),
        .args = list(key = key, func = func, args = args, make_job = make_job)
    )
    # nolint end
    res <- installed[]
    failed <- vapply(res, mirai::is_error_value, logical(1))
    if (any(failed)) {
        stop(
            "Failed to register the stage on ", sum(failed),
            " mirai daemon(s).",
            call. = FALSE
        )
    }
    list(key = key)
}
#' @export
.pump_executor_unregister.pump_mirai_backend <- function(backend, handle) {
    # Daemons are long-lived: uninstall the runner so pools do not
    # accumulate one runner per stage per run. Fire-and-forget: results
    # are not collected, and a daemon that went away is not an
    # unregistration failure.
    try(
        mirai::everywhere(
            suppressWarnings(rm(list = key, envir = globalenv())),
            .args = list(key = handle$key)
        ),
        silent = TRUE
    )
    invisible(backend)
}
#' @export
.pump_executor_new_job.pump_mirai_backend <- function(backend, handle, data) {
    key <- handle$key
    m <- mirai::mirai(
        get(key, envir = globalenv())(data),
        .args = list(key = key, data = data)
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
        res <- .pump_job_failure(
            simpleError(paste0("mirai worker failed: ", msg))
        )
    }
    res
}
