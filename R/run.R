#' Run a siphon pipeline and collect results
#'
#' `pump_run()` drains a siphon pipeline until all upstream sources, active slots, and
#' output buffers are complete. Results are collected in the original input
#' order using internal sequential indices (idx).
#'
#' @param x A pump object or a finite R object.
#' @param sleep_ms Delay between polls when no item is ready.
#' @param verbose If `TRUE`, show a text progress bar.
#' @param on_error Default error handling policy for all stages that do not
#'   explicitly set their own `on_error`: `"stop"` throws on first error,
#'   `"collect"` propagates error objects, `"continue"` drops failed items.
#'   Defaults to `"stop"`.
#' @param timeout Maximum time in seconds to wait for completion. If
#'   NULL (default), waits indefinitely. If exceeded, throws an error.
#'
#' @details The timeout parameter is checked cooperatively between polling
#'   iterations on the main R thread. Because of this, it only works when using
#'   asynchronous backends (such as `mirai_backend()` or `future_backend()` with
#'   a parallel plan) that return control to the main loop. If using a
#'   synchronous backend (like `main_backend()`), a job stuck in an infinite
#'   loop or blocking operation will freeze the thread and prevent the timeout
#'   from being checked. Furthermore, the timeout cannot interrupt blocking
#'   C/C++ code within background workers.
#'
#'   The `sleep_ms` parameter introduces backoff time between polls when no item
#'   is ready. This is necessary for CPU efficiency but adds overhead. For fast
#'   operations (e.g., simple arithmetic), this overhead can dominate total runtime.
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
                      timeout = NULL) {
    if (!inherits(x, "pump")) x <- .pump_source_basic(x)
    on.exit(if (is.function(x$close)) x$close(), add = TRUE)

    if (x$done()) {
        # a genuinely empty source yields an empty result; an already-drained
        # (exhausted) pipeline is a usage error
        if (length(x) == 0L) {
            return(list())
        }
        stop("source pipeline is exhausted")
    }

    on_error <- match.arg(on_error, c("stop", "collect", "continue"))

    # Set the global default on the pipeline so each stage can resolve its
    # effective policy (explicit stage policy wins over this default).
    if (is.function(x$set_on_error)) x$set_on_error(on_error)

    progress <- verbose
    if (is.infinite(x$pipeline_length()) || x$pipeline_length() / .pump_executor_count(x$backend()) < 2L) {
        progress <- FALSE
    }
    if (progress) {
        pb <- utils::txtProgressBar(0, x$pipeline_length(), style = 3)
        on.exit(close(pb), add = TRUE)
    }

    start <- Sys.time()
    deadline <- if (!is.null(timeout)) {
        start + as.difftime(timeout, units = "secs")
    } else {
        NULL
    }
    
    n_size <- length(x)
    if (is.finite(n_size)) {
        vals <- vector("list", n_size)
        assigned <- logical(n_size)
    } else {
        vals <- list()
        assigned <- logical()
    }

    while (!x$done()) {
        if (!is.null(deadline) && Sys.time() > deadline) {
            stop("pump_run() timeout exceeded after ", timeout, " seconds")
        }
        v <- x$next_item()
        if (is.null(v)) {
            Sys.sleep(sleep_ms / 1000)
            next
        }
        if (progress) {
            utils::setTxtProgressBar(pb, x$progress())
        }
        idx <- .pump_validate_idx(v$idx)

        if (idx > length(vals)) {
            old_len <- length(vals)
            length(vals) <- idx
            length(assigned) <- idx
            assigned[(old_len + 1):idx] <- FALSE
        }
        vals[idx] <- list(.pump_unwrap_error(v$data))
        assigned[idx] <- TRUE
    }
    if (progress) {
        utils::setTxtProgressBar(pb, x$pipeline_length())
    }

    vals[assigned]
}

#' Drain a siphon pipeline
#'
#' `pump_drain()` runs the pipeline, pulling items and passing them to a
#' callback function as they become ready. This is a memory-safe alternative
#' to `pump_run()` suitable for infinite or long-running pipelines.
#'
#' @param x A pump object or a finite R object.
#' @param handle_fn A callback function with signature `function(id, data, ok)`
#'   called for each completed item.
#' @param sleep_ms Delay in milliseconds between polls when no item is ready.
#' @param verbose If `TRUE`, show a text progress bar.
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
pump_drain <- function(x, handle_fn, sleep_ms = 10, verbose = TRUE) {
    if (!inherits(x, "pump")) x <- .pump_source_basic(x)
    if (!is.function(handle_fn)) stop("handle_fn must be a function")
    on.exit(if (is.function(x$close)) x$close(), add = TRUE)

    progress <- verbose
    if (is.infinite(x$pipeline_length()) || x$pipeline_length() / .pump_executor_count(x$backend()) < 2L) {
        progress <- FALSE
    }
    if (progress) {
        pb <- utils::txtProgressBar(0, x$pipeline_length(), style = 3)
        on.exit(close(pb), add = TRUE)
    }

    while (!x$done()) {
        v <- x$next_item()
        if (is.null(v)) {
            Sys.sleep(sleep_ms / 1000)
            next
        }
        if (progress) {
            utils::setTxtProgressBar(pb, x$progress())
        }
        handle_fn(v$id, .pump_unwrap_error(v$data), v$ok)
    }
    if (progress) {
        utils::setTxtProgressBar(pb, x$pipeline_length())
    }

    invisible(NULL)
}
