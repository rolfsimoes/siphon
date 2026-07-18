# Collect every backend used by the stages of a pipeline, terminal first.
.pump_pipeline_backends <- function(x) {
    out <- list()
    current <- x
    while (inherits(current, "pump")) {
        if (is.function(current$backend)) {
            out[[length(out) + 1L]] <- current$backend()
        }
        if (!is.function(current$upstream)) break
        current <- current$upstream()
    }
    out
}

# Shared driver loop behind pump_run() and pump_drain(). Wraps bare sources,
# propagates pipeline-wide defaults, opens every backend in the pipeline,
# polls with backoff, and applies the commit/abort/release lifecycle around
# each delivered item: on_item(v), then commit; abort on error; release
# always. error_on_exhausted controls the one deliberate driver asymmetry:
# re-driving an exhausted pipeline is a usage error for pump_run() but a
# tolerated no-op for daemon-style pump_drain() re-invocation.
.pump_drive <- function(x,
                        on_item,
                        sleep_ms,
                        verbose,
                        on_error,
                        backend,
                        timeout,
                        error_on_exhausted) {
    if (!inherits(x, "pump")) x <- .pump_source_basic(x)
    on.exit(if (is.function(x$close)) x$close(), add = TRUE)

    if (x$done()) {
        # a genuinely empty source yields an empty result either way
        if (error_on_exhausted && length(x) != 0L) {
            stop("source pipeline is exhausted")
        }
        return(invisible(NULL))
    }

    on_error <- match.arg(on_error, c("stop", "collect", "continue"))

    # Set the pipeline-wide defaults so each stage can resolve its effective
    # policy and backend (explicit stage settings win over these defaults).
    if (is.function(x$set_on_error)) x$set_on_error(on_error)
    if (is.function(x$set_backend)) x$set_backend(backend)

    # Lifecycle: transition every backend in the pipeline to a live
    # resource. Run-owned backends (resolved by the driver itself) are
    # closed on exit; user-constructed backends are never closed here.
    for (bk in .pump_pipeline_backends(x)) {
        .pump_backend_open(bk)
    }

    progress <- verbose
    if (is.infinite(x$pipeline_length()) ||
            x$pipeline_length() / .pump_executor_count(x$backend()) < 2L) {
        progress <- FALSE
    }
    if (progress) {
        pb <- utils::txtProgressBar(0, x$pipeline_length(), style = 3)
        on.exit(close(pb), add = TRUE)
    }

    deadline <- if (!is.null(timeout)) {
        Sys.time() + as.difftime(timeout, units = "secs")
    } else {
        NULL
    }

    while (!x$done()) {
        if (!is.null(deadline) && Sys.time() > deadline) {
            stop("pipeline timeout exceeded after ", timeout, " seconds")
        }
        x$next_item()
        v <- x$pop_item()
        if (is.null(v)) {
            Sys.sleep(sleep_ms / 1000)
            next
        }
        if (progress) {
            utils::setTxtProgressBar(pb, x$progress())
        }
        tryCatch(
            {
                on_item(v)
                x$item_commit(v$id, v$data)
            },
            error = function(e) {
                x$item_abort(v$id, error = e, data = v$data)
                stop(e)
            },
            finally = {
                x$item_release(v$id)
            }
        )
    }
    if (progress) {
        utils::setTxtProgressBar(pb, x$pipeline_length())
    }

    invisible(NULL)
}

#' Run a siphon pipeline and collect results
#'
#' `pump_run()` drains a siphon pipeline until all upstream sources, active
#'   slots, and output buffers are complete. Results are collected in the
#'   original input order using internal sequential indices (`idx`).
#'
#' @param x A pump object or a finite R object.
#' @param sleep_ms Delay between polls when no item is ready.
#' @param verbose If `TRUE`, show a text progress bar.
#' @param on_error Default error handling policy for all stages that do not
#'   explicitly set their own `on_error`: `"stop"` throws on first error,
#'   `"collect"` propagates error objects, `"continue"` drops failed items.
#'   Defaults to `"stop"`.
#' @param backend Default backend for all stages that do not explicitly set
#'   their own `backend`. Can be a backend object or one of `"main"`,
#'   `"mirai"`, or `"future"`. Use `parallel_backend()` directly for
#'   fault-tolerant PSOCK execution (no string alias). Defaults to `"main"`.
#' @param timeout Maximum time in seconds to wait for completion. If
#'   NULL (default), waits indefinitely. If exceeded, throws an error.
#'
#' @details The timeout parameter is checked cooperatively between polling
#'   iterations on the main R thread. Because of this, it only works when using
#'   asynchronous backends (such as `mirai_backend()`, `future_backend()`
#'   with a parallel plan, or `parallel_backend()`) that return control to
#'   the main loop. If using a
#'   synchronous backend (like `main_backend()`), a job stuck in an infinite
#'   loop or blocking operation will freeze the thread and prevent the timeout
#'   from being checked. Furthermore, the timeout cannot interrupt blocking
#'   C/C++ code within background workers.
#'
#'   The `sleep_ms` parameter introduces backoff time between polls when no item
#'   is ready. This is necessary for CPU efficiency but adds overhead. For fast
#'   operations (e.g., simple arithmetic), this overhead can dominate total
#'   runtime.
#'   siphon is designed for pipelines with substantial work per item (e.g., I/O,
#'   complex computations, external API calls) **where coordination overhead is
#'   negligible compared to actual work time**.
#'
#' @return A list of results in input order. Items dropped by a `"continue"`
#'   stage are omitted entirely; the result may be shorter than the input.
#' @examples
#' f <- 1:5 |> pump(function(x) x * 2, backend = "main")
#' pump_run(f)
#'
#' # With timeout
#' f <- 1:5 |> pump(function(x) x * 2, backend = "main")
#' pump_run(f, timeout = 10)
#' @export
pump_run <- function(x,
                     sleep_ms = 10,
                     verbose = TRUE,
                     on_error = "stop",
                     backend = "main",
                     timeout = NULL) {
    n_size <- length(x)
    if (is.finite(n_size)) {
        vals <- vector("list", n_size)
        assigned <- logical(n_size)
    } else {
        vals <- list()
        assigned <- logical()
    }

    .pump_drive(
        x,
        on_item = function(v) {
            idx <- .pump_validate_idx(v$idx)

            if (idx > length(vals)) {
                old_len <- length(vals)
                length(vals) <<- idx
                length(assigned) <<- idx
                assigned[(old_len + 1):idx] <<- FALSE
            }
            vals[idx] <<- list(.pump_unwrap_error(v$data))
            assigned[idx] <<- TRUE
        },
        sleep_ms = sleep_ms,
        verbose = verbose,
        on_error = on_error,
        backend = backend,
        timeout = timeout,
        error_on_exhausted = TRUE
    )

    vals[assigned]
}

#' Drain a siphon pipeline
#'
#' `pump_drain()` runs the pipeline, pulling items and passing them to a
#' callback function as they become ready. This is a memory-safe alternative
#' to `pump_run()` suitable for infinite or long-running pipelines.
#'
#' Unlike [pump_run()], draining an already-exhausted pipeline is a silent
#' no-op, so daemon-style loops can re-invoke `pump_drain()` harmlessly.
#'
#' @param x A pump object or a finite R object.
#' @param handle_fn A callback function with signature `function(id, data, ok)`
#'   called for each completed item.
#' @param sleep_ms Delay in milliseconds between polls when no item is ready.
#' @param verbose If `TRUE`, show a text progress bar.
#' @param on_error Default error handling policy for all stages that do not
#'   explicitly set their own `on_error`: `"stop"` throws on first error,
#'   `"collect"` delivers error items to `handle_fn` with `ok = FALSE`,
#'   `"continue"` drops failed items. Defaults to `"stop"`.
#' @param backend Default backend for all stages that do not explicitly set
#'   their own `backend`. Can be a backend object or one of `"main"`,
#'   `"mirai"`, or `"future"`. Use `parallel_backend()` directly for
#'   fault-tolerant PSOCK execution (no string alias). Defaults to `"main"`.
#' @param timeout Maximum time in seconds to wait for completion. If
#'   NULL (default), waits indefinitely. If exceeded, throws an error. See
#'   the Details section of [pump_run()] for the cooperative-checking
#'   caveats, which apply here equally.
#'
#' @return Invisible `NULL`.
#' @examples
#' results <- list()
#' f <- 1:5 |> pump(function(x) x * 2, backend = "main")
#' pump_drain(f, handle_fn = function(id, data, ok) {
#'     results[[id]] <<- data
#' })
#' print(results)
#' @export
pump_drain <- function(x,
                       handle_fn,
                       sleep_ms = 10,
                       verbose = TRUE,
                       on_error = "stop",
                       backend = "main",
                       timeout = NULL) {
    if (!is.function(handle_fn)) stop("handle_fn must be a function")

    .pump_drive(
        x,
        on_item = function(v) {
            handle_fn(v$id, .pump_unwrap_error(v$data), v$ok)
        },
        sleep_ms = sleep_ms,
        verbose = verbose,
        on_error = on_error,
        backend = backend,
        timeout = timeout,
        error_on_exhausted = FALSE
    )
}
