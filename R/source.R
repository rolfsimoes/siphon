# Private: wrap a basic R object as a pull-based item source.
# Each item receives a stable user-visible id, and a sequential integer idx used
# by downstream stages and by pump_run() to collect results in input order.
.pump_validate_idx <- function(idx) {
    if (is.null(idx)) {
        stop("Internal error: item index (idx) is missing")
    }
    if (length(idx) != 1L) {
        stop("Internal error: item index (idx) must be a scalar")
    }
    if (is.na(idx)) {
        stop("Internal error: item index (idx) must be finite and non-missing")
    }
    if (!is.numeric(idx) && !is.integer(idx)) {
        stop("Internal error: item index (idx) must be numeric")
    }
    if (!is.finite(idx)) {
        stop("Internal error: item index (idx) must be finite and non-missing")
    }
    if (idx %% 1 != 0) {
        stop("Internal error: item index (idx) must be a whole number")
    }
    if (idx < 1L) {
        stop("Internal error: item index (idx) must be >= 1")
    }
    if (idx > .Machine$integer.max) {
        stop("Internal error: item index (idx) exceeds maximum integer range")
    }
    as.integer(idx)
}

.pump_error <- function(e) {
    class(e) <- c("pump_error", class(e))
    e
}

.pump_unwrap_error <- function(x) {
    if (inherits(x, "pump_error")) {
        class(x) <- setdiff(class(x), "pump_error")
    }
    x
}

.pump_source_basic <- function(x) {
    # check parameters
    if (inherits(x, "pump")) {
        return(x)
    }

    # private members
    n <- length(x)
    original_data <- x
    i <- 0L
    err_count <- NULL # computed lazily on first access
    default_backend <- "main"

    # private method to compute error count lazily
    get_err_count <- function() {
        if (is.null(err_count)) {
            err_count <<- sum(vapply(seq_len(n), function(i) {
                inherits(original_data[[i]], "pump_error")
            }, logical(1)))
        }
        err_count
    }

    # statistics: pop hit/miss counts and time spent inside pop_item() (ms)
    pop_hits <- 0L
    pop_misses <- 0L
    pull_time <- 0.0

    # public methods
    self <- list(
        next_item = function() invisible(NULL),
        pop_item = function() {
            t0 <- .pump_now_ms()

            if (i >= n) {
                pull_time <<- pull_time + (.pump_now_ms() - t0)
                pop_misses <<- pop_misses + 1L
                return(NULL)
            }
            i <<- i + 1L
            data <- original_data[[i]]
            pull_time <<- pull_time + (.pump_now_ms() - t0)
            pop_hits <<- pop_hits + 1L
            list(
                id = i,
                idx = i,
                data = data,
                ok = !inherits(data, "pump_error")
            )
        },
        length = function() n,
        pipeline_length = function() n,
        buffer = function() invisible(NULL),
        slots = function() invisible(NULL),
        progress = function() i,
        stage_completed = function() i,
        errors = function() get_err_count(),
        stats = function() {
            list(
                pop_hits = pop_hits,
                pop_misses = pop_misses,
                pull_time = pull_time
            )
        },
        reset_stats = function() {
            pop_hits <<- 0L
            pop_misses <<- 0L
            pull_time <<- 0.0
        },
        done = function() i == n,
        close = function() invisible(NULL),
        item_commit = function(id, data) invisible(NULL),
        item_abort = function(id, error = NULL, data = NULL) invisible(NULL),
        item_release = function(id) invisible(NULL),
        backend = function() main_backend(),
        set_backend = function(value) {
            default_backend <<- value
            invisible(NULL)
        },
        get_backend = function() default_backend
    )

    structure(self, class = "pump")
}

#' Create a custom siphon source
#'
#' `pump_source()` creates a custom pull-based source for use with `pump()`
#' pipelines. Use this to connect external data sources such as message queues,
#' databases, or file readers to a siphon pipeline.
#'
#' The `pull_fn` function is called repeatedly by downstream stages to retrieve
#' items. It should return `list(id, data, ok)` when an item is available, or
#' `NULL` when no item is ready. The source is considered infinite by default
#' (suitable for daemon-style processing); pass a `done_fn` to signal when the
#' source is exhausted.
#'
#' @param pull_fn A function with no arguments that returns
#'   `list(id, data, ok)` or `NULL`. The returned list must contain a
#'   non-missing, scalar atomic user-visible `id` (uniqueness is the user's
#'   responsibility), the `data` object, and a non-missing scalar logical `ok`
#'   flag indicating success.
#' @param done_fn A function with no arguments returning `TRUE` or `FALSE`.
#'   Defaults to `NULL` (source never finishes on its own).
#' @param close_fn An optional function with no arguments for resource cleanup
#'   (e.g., closing file connections or database handles). Called automatically
#'   by `pump_run()` and `pump_drain()` when execution completes.
#' @param length The total number of items to expect. Defaults to `Inf`.
#'   Used by `pump_run()` for result pre-allocation and progress reporting.
#' @param item_commit_fn An optional function called when an item successfully
#'   completes the pipeline. Receives `id` and `data` arguments. Used for
#'   acknowledging external resources (e.g., message queue ack).
#' @param item_abort_fn An optional function called when an item fails.
#'   Receives `id`, `error`, and `data` arguments. Used for rejecting external
#'   resources (e.g., message queue nack).
#' @param item_release_fn An optional function called when an item is released
#'   from the pipeline (e.g., on timeout or shutdown). Receives `id` argument.
#'
#' @return A pump object that can be piped into `pump()`.
#' @examples
#' # A simple counter source
#' counter_source <- function(n) {
#'     i <- 0L
#'     pump_source(
#'         pull_fn = function() {
#'             if (i >= n) {
#'                 return(NULL)
#'             }
#'             i <<- i + 1L
#'             list(id = i, data = i, ok = TRUE)
#'         },
#'         done_fn = function() i >= n,
#'         length = n
#'     )
#' }
#' src <- counter_source(5)
#' res <- src |>
#'     pump(function(x) x * 2) |>
#'     pump_run(verbose = FALSE)
#' print(res)
#' @export
pump_source <- function(pull_fn,
                        done_fn = NULL,
                        close_fn = NULL,
                        length = Inf,
                        item_commit_fn = NULL,
                        item_abort_fn = NULL,
                        item_release_fn = NULL) {
    if (!is.function(pull_fn)) stop("pull_fn must be a function")

    # Defaults
    done_resolved <- if (is.null(done_fn)) function() FALSE else done_fn
    close_resolved <- if (is.null(close_fn)) function() invisible(NULL) else close_fn
    length_fn <- if (is.function(length)) length else function() length

    item_commit_resolved <- if (is.null(item_commit_fn)) {
        function(id, data) invisible(NULL)
    } else {
        item_commit_fn
    }

    item_abort_resolved <- if (is.null(item_abort_fn)) {
        function(id, error = NULL, data = NULL) invisible(NULL)
    } else {
        item_abort_fn
    }

    item_release_resolved <- if (is.null(item_release_fn)) {
        function(id) invisible(NULL)
    } else {
        item_release_fn
    }

    internal_ordinal <- 0L
    default_backend <- "main"
    # statistics: pop hit/miss counts and time spent inside pop_item() (ms)
    pop_hits <- 0L
    pop_misses <- 0L
    pull_time <- 0.0

    wrapped_pull_fn <- function() {
        t0 <- .pump_now_ms()

        msg <- pull_fn()
        if (is.null(msg)) {
            pull_time <<- pull_time + (.pump_now_ms() - t0)
            pop_misses <<- pop_misses + 1L
            return(NULL)
        }
        if (!is.list(msg) || !all(c("id", "data", "ok") %in% names(msg))) {
            stop("pull_fn must return NULL or a list with 'id', 'data', and 'ok' elements")
        }
        if (is.null(msg$id) || !is.atomic(msg$id) || length(msg$id) != 1L || is.na(msg$id)) {
            stop("pull_fn must return items with a valid scalar atomic 'id'")
        }
        if (!is.logical(msg$ok) || length(msg$ok) != 1L || is.na(msg$ok)) {
            stop("pull_fn must return items with a valid scalar logical 'ok'")
        }
        internal_ordinal <<- internal_ordinal + 1L
        msg$idx <- internal_ordinal
        pull_time <<- pull_time + (.pump_now_ms() - t0)
        pop_hits <<- pop_hits + 1L
        msg
    }

    self <- list(
        next_item = function() invisible(NULL),
        pop_item = wrapped_pull_fn,
        length = length_fn,
        pipeline_length = length_fn,
        buffer = function() invisible(NULL),
        slots = function() invisible(NULL),
        progress = function() 0L,
        stage_completed = function() 0L,
        errors = function() 0L,
        stats = function() {
            list(
                pop_hits = pop_hits,
                pop_misses = pop_misses,
                pull_time = pull_time
            )
        },
        reset_stats = function() {
            pop_hits <<- 0L
            pop_misses <<- 0L
            pull_time <<- 0.0
        },
        done = done_resolved,
        close = close_resolved,
        item_commit = item_commit_resolved,
        item_abort = item_abort_resolved,
        item_release = item_release_resolved,
        backend = function() main_backend(),
        set_backend = function(value) {
            default_backend <<- value
            invisible(NULL)
        },
        get_backend = function() default_backend
    )
    structure(self, class = "pump")
}

#' @export
length.pump <- function(x) {
    x$length()
}
