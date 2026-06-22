# Internal constructor for error policy resolution
.pump_error_policy <- function(explicit = NULL, upstream = NULL) {
    # private state
    explicit_on_error <- explicit
    default_on_error <- NULL

    # public methods
    list(
        get = function() explicit_on_error %||% default_on_error %||% "stop",
        set_default = function(value) {
            default_on_error <<- value
            if (!is.null(upstream) && is.function(upstream$set_on_error)) {
                upstream$set_on_error(value)
            }
            invisible(NULL)
        },
        should_stop = function() {
            policy <- explicit_on_error %||% default_on_error %||% "stop"
            policy == "stop"
        },
        should_collect = function() {
            policy <- explicit_on_error %||% default_on_error %||% "stop"
            policy == "collect"
        },
        should_continue = function() {
            policy <- explicit_on_error %||% default_on_error %||% "stop"
            policy == "continue"
        }
    )
}

# Internal constructor for backend resolution
.pump_backend_policy <- function(explicit = NULL, upstream = NULL) {
    # private state
    explicit_backend <- explicit
    default_backend <- NULL

    # public methods
    list(
        get = function() explicit_backend %||% default_backend %||% "main",
        set_default = function(value) {
            default_backend <<- value
            if (!is.null(upstream) && is.function(upstream$set_backend)) {
                upstream$set_backend(value)
            }
            invisible(NULL)
        }
    )
}

# Internal constructor for statistics tracking
.pump_stats <- function() {
    # private state
    poll_hits <- 0L
    poll_misses <- 0L
    wall_time <- 0.0
    fn_time <- 0.0
    idle_time <- 0.0
    errors <- 0L
    
    # public methods
    list(
        record_poll_hit = function(elapsed) {
            poll_hits <<- poll_hits + 1L
            wall_time <<- wall_time + elapsed
            invisible(NULL)
        },
        record_poll_miss = function(elapsed) {
            poll_misses <<- poll_misses + 1L
            wall_time <<- wall_time + elapsed
            idle_time <<- idle_time + elapsed
            invisible(NULL)
        },
        add_fn_time = function(milliseconds) {
            fn_time <<- fn_time + milliseconds
            invisible(NULL)
        },
        add_error = function() {
            errors <<- errors + 1L
            invisible(NULL)
        },
        poll_hits = function() poll_hits,
        poll_misses = function() poll_misses,
        wall_time = function() wall_time,
        fn_time = function() fn_time,
        idle_time = function() idle_time,
        errors = function() errors,
        reset = function() {
            poll_hits <<- 0L
            poll_misses <<- 0L
            wall_time <<- 0.0
            fn_time <<- 0.0
            idle_time <<- 0.0
            invisible(NULL)
        }
    )
}

# Internal constructor for empty stage objects
.pump_empty_stage <- function(upstream, backend, explicit_on_error, explicit_backend = NULL) {
    stats <- .pump_stats()
    error_policy <- .pump_error_policy(explicit = explicit_on_error, upstream = upstream)
    backend_policy <- .pump_backend_policy(explicit = explicit_backend, upstream = upstream)

    structure(list(
        next_item = function() NULL,
        length = function() 0L,
        pipeline_length = function() upstream$pipeline_length(),
        buffer = function() NULL,
        slots = function() NULL,
        progress = function() upstream$progress(),
        stage_completed = function() 0L,
        errors = function() stats$errors(),
        poll_hits = function() stats$poll_hits(),
        poll_misses = function() stats$poll_misses(),
        poll_wall_time = function() stats$wall_time(),
        fn_time = function() stats$fn_time(),
        idle_time = function() stats$idle_time(),
        reset_stats = function() stats$reset(),
        set_on_error = function(default) error_policy$set_default(default),
        get_on_error = function() error_policy$get(),
        set_backend = function(default) backend_policy$set_default(default),
        get_backend = function() backend_policy$get(),
        item_commit = function(id, data) {
            upstream$item_commit(id, data)
        },
        item_abort = function(id, error = NULL, data = NULL) {
            upstream$item_abort(id, error = error, data = data)
        },
        item_release = function(id) {
            upstream$item_release(id)
        },
        done = function() TRUE,
        close = function() if (is.function(upstream$close)) upstream$close(),
        backend = function() backend,
        upstream = function() upstream
    ), class = "pump")
}

# Internal constructor for pump stage objects
.pump_stage <- function(upstream,
                        fn,
                        args,
                        backend,
                        max_workers,
                        buffer_size,
                        explicit_on_error,
                        explicit_backend = NULL) {
    # private members
    n <- upstream$length()
    i <- 0L
    stats <- .pump_stats()
    error_policy <- .pump_error_policy(explicit = explicit_on_error, upstream = upstream)
    backend_policy <- .pump_backend_policy(explicit = explicit_backend, upstream = upstream)
    
    # Handle empty upstreams
    if (is.finite(n) && n == 0L) {
        return(.pump_empty_stage(upstream = upstream, backend = backend, explicit_on_error = explicit_on_error, explicit_backend = explicit_backend))
    }
    buf <- .pump_queue(buffer_size)
    sl <- .pump_slots(max_workers)

    # private methods
    discard_item <- function(msg) {
        upstream$item_abort(
            id = msg$id,
            error = if (inherits(msg$data, "pump_error")) {
                .pump_unwrap_error(msg$data)
            } else {
                NULL
            },
            data = msg$data
        )

        upstream$item_release(msg$id)

        invisible(NULL)
    }

    drain_completed <- function() {
        # drain completed jobs and put them in out queue
        while (sl$active() > 0L) {
            # output queue is full
            if (buf$remaining() <= 0L) break
            # get the first ready job from slot
            res <- sl$poll_ready()
            # no job is ready
            if (is.null(res)) break
            # get result and timing metadata
            job_result <- .pump_job_data(res$job)
            data <- job_result$value
            stats$add_fn_time(job_result$fn_time)
            if (!is.list(res$id)) {
                stop("Internal error: item ID metadata is invalid")
            }
            validated_idx <- .pump_validate_idx(res$id$idx)
            msg <- list(
                id = res$id$id,
                idx = validated_idx,
                data = data,
                ok = !inherits(data, "pump_error")
            )
            if (!msg$ok) {
                stats$add_error()
                if (error_policy$should_stop()) stop(.pump_unwrap_error(data))
            }
            i <<- i + 1L
            if (error_policy$should_continue() && !msg$ok) {
                discard_item(msg)
            } else {
                buf$push(msg)
            }
        }
        invisible(NULL)
    }

    advance <- function() {
        # fill free slots with new jobs
        while (sl$n_free() > 0L) {
            # pull from upstream stage
            msg <- upstream$next_item()
            # upstream exhausted
            if (is.null(msg)) break
            # place a job in a free processing slot
            if (msg$ok) {
                validated_idx <- .pump_validate_idx(msg$idx)
                j <- sl$acquire(
                    id = list(id = msg$id, idx = validated_idx),
                    job = .pump_executor_new_job(
                        backend,
                        fn,
                        c(list(msg$data), args)
                    )
                )
                # error: no free slots!
                if (is.null(j)) break
            } else {
                stats$add_error()
                if (error_policy$should_stop()) stop(.pump_unwrap_error(msg$data))
                i <<- i + 1L
                if (error_policy$should_continue() && !msg$ok) {
                    discard_item(msg)
                } else {
                    buf$push(msg)
                }
            }
        }
    }

    # public methods
    self <- list(
        next_item = function() {
            # Measure timing
            start_time <- Sys.time()

            # this function should be called by downstream stage
            # drain slots' completed jobs to buffer queue
            drain_completed()
            # fill free slots with new jobs
            advance()
            # pop an item from buffer queue
            result <- buf$pop()

            end_time <- Sys.time()
            elapsed <- as.numeric(difftime(end_time, start_time, units = "secs")) * 1000

            if (!is.null(result)) {
                stats$record_poll_hit(elapsed)
            } else {
                stats$record_poll_miss(elapsed)
            }
            result
        },
        length = function() n,
        pipeline_length = function() {
            if (is.finite(n) && is.finite(upstream$pipeline_length())) {
                n + upstream$pipeline_length()
            } else {
                Inf
            }
        },
        buffer = function() buf,
        slots = function() sl,
        progress = function() i + upstream$progress(),
        stage_completed = function() i,
        errors = function() stats$errors(),
        poll_hits = function() stats$poll_hits(),
        poll_misses = function() stats$poll_misses(),
        poll_wall_time = function() stats$wall_time(),
        fn_time = function() stats$fn_time(),
        idle_time = function() stats$idle_time(),
        reset_stats = function() stats$reset(),
        set_on_error = function(default) error_policy$set_default(default),
        get_on_error = function() error_policy$get(),
        set_backend = function(default) backend_policy$set_default(default),
        get_backend = function() backend_policy$get(),
        item_commit = function(id, data) {
            upstream$item_commit(id, data)
        },
        item_abort = function(id, error = NULL, data = NULL) {
            upstream$item_abort(id, error = error, data = data)
        },
        item_release = function(id) {
            upstream$item_release(id)
        },
        done = function() upstream$done() && buf$size() == 0L && sl$active() == 0L,
        close = function() if (is.function(upstream$close)) upstream$close(),
        backend = function() backend,
        upstream = function() upstream
    )

    structure(self, class = "pump")
}

#' Add a processing stage to a pipeline
#'
#' `pump()` creates a pull-based stage. The stage pulls items from its upstream
#' source when it has free slots, submits jobs to the selected backend, and
#' yields completed items to downstream stages.
#'
#' @param x A pump object or a finite R object (list, vector) that will be
#'   implicitly wrapped as a basic source.
#' @param fn A function. It receives one item as its first argument.
#' @param ... Additional arguments passed to `fn`.
#' @param backend A backend object or one of `"main"`, `"mirai"`,
#'   or `"future"`. If `NULL` (the default), the stage inherits the backend
#'   set by `pump_run()` or `pump_drain()`.
#' @param max_workers Maximum number of active jobs for this stage. Defaults to the
#'   backend worker count. Ignored for the synchronous `main_backend()` (which always uses 1).
#' @param on_error How to handle item errors: `"stop"` throws on first error,
#'   `"collect"` propagates them, `"continue"` drops failed items. If `NULL`
#'   (the default), the stage inherits the policy set by `pump_run()`.
#' @param buffer_size Maximum size of the output buffer. Defaults to
#'   `min(length(x), 1000L)`. Use a smaller value to enable true backpressure.
#'
#' @return A pump object.
#' @examples
#' # Single stage pipeline
#' f <- 1:5 |> pump(function(x) x * 2, backend = "main")
#' pump_run(f, verbose = FALSE)
#'
#' # Two-stage pipeline
#' f <- 1:5 |>
#'     pump(function(x) x + 1, backend = "main") |>
#'     pump(function(x) x * 3, backend = "main")
#' pump_run(f, verbose = FALSE)
#' @export
pump <- function(x,
                 fn,
                 ...,
                 backend = NULL,
                 max_workers = NULL,
                 on_error = NULL,
                 buffer_size = NULL) {
    # Validate and normalize arguments
    if (!inherits(x, "pump")) x <- .pump_source_basic(x)
    if (!is.function(fn)) stop("fn must be a function")
    if (!is.null(on_error)) {
        on_error <- match.arg(on_error, c("stop", "collect", "continue"))
    }

    # Resolve backend: explicit backend parameter, or default from upstream, or "main"
    if (is.null(backend)) {
        if (is.function(x$get_backend)) {
            backend <- x$get_backend()
        } else {
            backend <- "main"
        }
    }

    # Resolve and validate backend
    backend <- .pump_resolve_backend(backend)
    if (.pump_executor_count(backend) < 1L) {
        if (inherits(backend, "pump_mirai_backend")) {
            stop(
                "No active mirai daemons found. ",
                "Please call mirai::daemons(n) before starting the pipeline."
            )
        } else {
            stop("backend must have at least one process")
        }
    }
    
    # Compute max_workers
    if (inherits(backend, "pump_main_backend")) {
        max_workers <- 1L
    } else if (is.null(max_workers)) {
        max_workers <- .pump_executor_count(backend)
    }
    n <- x$length()
    if (is.finite(n)) {
        max_workers <- as.integer(max(1L, min(n, max_workers)))
    } else {
        max_workers <- as.integer(max(1L, max_workers))
    }
    
    # Compute buffer_size
    if (is.null(buffer_size)) {
        buffer_size <- 1000L
    }
    if (is.finite(n)) {
        buffer_size <- as.integer(max(1L, min(n, buffer_size)))
    } else {
        buffer_size <- as.integer(max(1L, buffer_size))
    }
    
    # Capture additional arguments
    args <- list(...)
    
    # Call internal constructor
    .pump_stage(
        upstream = x,
        fn = fn,
        args = args,
        backend = backend,
        max_workers = max_workers,
        buffer_size = buffer_size,
        explicit_on_error = on_error,
        explicit_backend = if (is.null(backend)) NULL else backend
    )
}
