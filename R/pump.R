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

# Internal constructor for statistics tracking.
#
# All durations are in milliseconds and are accumulated only INSIDE protocol
# calls (next_item()/pop_item()): time between beats is deliberately never
# measured, so interactive stepping and driver backoff sleeps do not pollute
# the numbers. Idle is therefore a per-beat state classification
# (working/starved/blocked/done), not a duration.
.pump_stats <- function() {
    # private state
    beats <- 0L
    beats_working <- 0L
    beats_starved <- 0L
    beats_blocked <- 0L
    pop_hits <- 0L
    pop_misses <- 0L
    errors <- 0L
    fn_time <- 0.0
    tick_time <- 0.0
    submit_time <- 0.0
    pull_time <- 0.0
    first_beat_at <- NA_real_
    last_beat_at <- NA_real_

    # public methods
    list(
        record_beat = function(elapsed_ms, state) {
            beats <<- beats + 1L
            tick_time <<- tick_time + elapsed_ms
            switch(state,
                working = beats_working <<- beats_working + 1L,
                starved = beats_starved <<- beats_starved + 1L,
                blocked = beats_blocked <<- beats_blocked + 1L,
                stop("Internal error: unknown beat state: ", state)
            )
            now <- as.numeric(Sys.time())
            if (is.na(first_beat_at)) first_beat_at <<- now
            last_beat_at <<- now
            invisible(NULL)
        },
        record_pop = function(hit) {
            if (isTRUE(hit)) {
                pop_hits <<- pop_hits + 1L
            } else {
                pop_misses <<- pop_misses + 1L
            }
            invisible(NULL)
        },
        add_fn_time = function(milliseconds) {
            fn_time <<- fn_time + milliseconds
            invisible(NULL)
        },
        add_submit_time = function(milliseconds) {
            submit_time <<- submit_time + milliseconds
            invisible(NULL)
        },
        add_pull_time = function(milliseconds) {
            pull_time <<- pull_time + milliseconds
            invisible(NULL)
        },
        add_error = function() {
            errors <<- errors + 1L
            invisible(NULL)
        },
        errors = function() errors,
        snapshot = function() {
            list(
                beats = beats,
                beats_working = beats_working,
                beats_starved = beats_starved,
                beats_blocked = beats_blocked,
                pop_hits = pop_hits,
                pop_misses = pop_misses,
                errors = errors,
                fn_time = fn_time,
                tick_time = tick_time,
                submit_time = submit_time,
                pull_time = pull_time,
                first_beat_at = first_beat_at,
                last_beat_at = last_beat_at
            )
        },
        reset = function() {
            beats <<- 0L
            beats_working <<- 0L
            beats_starved <<- 0L
            beats_blocked <<- 0L
            pop_hits <<- 0L
            pop_misses <<- 0L
            fn_time <<- 0.0
            tick_time <<- 0.0
            submit_time <<- 0.0
            pull_time <<- 0.0
            first_beat_at <<- NA_real_
            last_beat_at <<- NA_real_
            invisible(NULL)
        }
    )
}

# Build a validated stage message for a completed or failed item
.pump_make_msg <- function(id, idx, data) {
    list(
        id = id,
        idx = .pump_validate_idx(idx),
        data = data,
        ok = !inherits(data, "pump_error")
    )
}

# Internal constructor for empty stage objects
.pump_empty_stage <- function(upstream,
                              explicit_on_error,
                              explicit_backend = NULL) {
    stats <- .pump_stats()
    error_policy <- .pump_error_policy(
        explicit = explicit_on_error, upstream = upstream
    )
    backend_policy <- .pump_backend_policy(
        explicit = explicit_backend, upstream = upstream
    )

    structure(list(
        next_item = function() invisible("done"),
        pop_item = function() invisible(NULL),
        length = function() 0L,
        pipeline_length = function() upstream$pipeline_length(),
        buffer = function() invisible(NULL),
        slots = function() invisible(NULL),
        progress = function() upstream$progress(),
        stage_completed = function() 0L,
        errors = function() stats$errors(),
        stats = function() stats$snapshot(),
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
        backend = function() .pump_resolve_backend(backend_policy$get()),
        upstream = function() upstream
    ), class = "pump")
}

# Internal constructor for pump stage objects
.pump_stage <- function(upstream,
                        fn,
                        args,
                        max_workers,
                        buffer_size,
                        explicit_on_error,
                        explicit_backend = NULL) {
    # private members
    n <- upstream$length()
    i <- 0L
    stats <- .pump_stats()
    error_policy <- .pump_error_policy(
        explicit = explicit_on_error, upstream = upstream
    )
    backend_policy <- .pump_backend_policy(
        explicit = explicit_backend, upstream = upstream
    )

    # Handle empty upstreams
    if (is.finite(n) && n == 0L) {
        return(.pump_empty_stage(
            upstream = upstream,
            explicit_on_error = explicit_on_error,
            explicit_backend = explicit_backend
        ))
    }
    buf <- .pump_queue(buffer_size)

    # Backend resolution is lazy. An explicit backend arrives here already
    # resolved, validated, and sized by pump(), so it is live immediately.
    # An inherited backend (explicit_backend = NULL) is resolved from the
    # policy on the first beat - after pump_run()/pump_drain() had the
    # chance to set the pipeline-wide default - and slot sizing follows it.
    user_max_workers <- max_workers
    exec_backend <- explicit_backend
    sl <- if (is.null(explicit_backend)) NULL else .pump_slots(max_workers)
    opened <- FALSE

    ensure_backend <- function() {
        if (opened) {
            return(invisible(NULL))
        }
        if (is.null(exec_backend)) {
            bk <- .pump_resolve_backend(backend_policy$get())
            .pump_check_backend(bk)
            exec_backend <<- bk
        }
        if (is.null(sl)) {
            sl <<- .pump_slots(
                .pump_size_max_workers(exec_backend, user_max_workers, n)
            )
        }
        .pump_backend_open(exec_backend)
        opened <<- TRUE
        invisible(NULL)
    }

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

    # deliver a finished item according to the error policy (the single
    # place where stop/collect/continue branching happens)
    deliver <- function(msg) {
        if (!msg$ok) {
            stats$add_error()
            if (error_policy$should_stop()) stop(.pump_unwrap_error(msg$data))
        }
        i <<- i + 1L
        if (!msg$ok && error_policy$should_continue()) {
            discard_item(msg)
        } else {
            buf$push(msg)
        }
        invisible(NULL)
    }

    # pop one completed job from a slot and build its output message
    drain_one <- function() {
        res <- sl$poll_ready()
        if (is.null(res)) {
            return(NULL)
        }
        job_result <- .pump_job_data(res$job)
        stats$add_fn_time(job_result$fn_time)
        if (!is.list(res$id)) {
            stop("Internal error: item ID metadata is invalid")
        }
        .pump_make_msg(
            id = res$id$id,
            idx = res$id$idx,
            data = job_result$value
        )
    }

    # move completed jobs from slots into the output buffer
    drain_completed <- function() {
        while (sl$active() > 0L && buf$remaining() > 0L) {
            msg <- drain_one()
            if (is.null(msg)) break
            deliver(msg)
        }
        invisible(NULL)
    }

    # ask the upstream stage for its next finished item; timed so this
    # stage's coord_time excludes the upstream stages' own beats
    pull_upstream <- function() {
        t0 <- .pump_now_ms()
        upstream$next_item()
        msg <- upstream$pop_item()
        stats$add_pull_time(.pump_now_ms() - t0)
        msg
    }

    # dispatch one item to the backend and place it in a free slot
    submit_job <- function(msg) {
        t0 <- .pump_now_ms()
        job <- .pump_executor_new_job(exec_backend, fn, c(list(msg$data), args))
        stats$add_submit_time(.pump_now_ms() - t0)
        sl$acquire(
            id = list(id = msg$id, idx = .pump_validate_idx(msg$idx)),
            job = job
        )
    }

    # fill free slots with new jobs; TRUE if upstream had nothing this beat
    advance <- function() {
        starved <- FALSE
        while (sl$n_free() > 0L) {
            msg <- pull_upstream()
            if (is.null(msg)) {
                starved <- TRUE
                break
            }
            if (msg$ok) {
                if (is.null(submit_job(msg))) break
            } else {
                deliver(msg)
            }
        }
        starved
    }

    # classify what this beat found; priority matters and is documented in
    # ?pump_status: blocked wins over working because it is the signal that
    # downstream is the bottleneck
    classify_beat <- function(starved, delivered) {
        if (buf$remaining() == 0L && sl$active() > 0L) {
            "blocked"
        } else if (sl$active() > 0L || delivered > 0L) {
            "working"
        } else if (starved && !upstream$done()) {
            "starved"
        } else {
            "done"
        }
    }

    # public methods
    self <- list(
        next_item = function() {
            # one beat: harvest finished work, refill slots, then harvest
            # again so jobs that completed during advance (always, for the
            # synchronous main backend) are visible in the same beat
            ensure_backend()
            t0 <- .pump_now_ms()
            i0 <- i
            drain_completed()
            starved <- advance()
            drain_completed()
            state <- classify_beat(starved, delivered = i - i0)
            # a "done" beat is a no-op (nothing can move anymore): it is not
            # recorded, so beating an exhausted pipeline leaves stats frozen
            if (state != "done") {
                stats$record_beat(.pump_now_ms() - t0, state)
            }
            invisible(state)
        },
        pop_item = function() {
            # pop an item from buffer queue
            v <- buf$pop()
            stats$record_pop(!is.null(v))
            v
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
        stats = function() stats$snapshot(),
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
        done = function() {
            upstream$done() && buf$size() == 0L &&
                (is.null(sl) || sl$active() == 0L)
        },
        close = function() if (is.function(upstream$close)) upstream$close(),
        backend = function() {
            if (!is.null(exec_backend)) {
                exec_backend
            } else {
                .pump_resolve_backend(backend_policy$get())
            }
        },
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
#' Stages implement a two-phase protocol: `$next_item()` runs one *beat*
#' (harvest completed jobs into the output buffer, then refill free slots by
#' pulling upstream — safe to call repeatedly) and `$pop_item()` consumes one
#' ready result. For interactive use prefer the exported verbs [pump_step()],
#' [pump_peek()], and [pump_pop()]; inspect state with `print()` or
#' [pump_status()].
#'
#' @param x A pump object or a finite R object (list, vector) that will be
#'   implicitly wrapped as a basic source.
#' @param fn A function. It receives one item as its first argument.
#' @param ... Additional arguments passed to `fn`.
#' @param backend A backend object or one of `"main"`, `"mirai"`,
#'   or `"future"`. Use `parallel_backend()` directly for fault-tolerant
#'   PSOCK execution (no string alias). An explicit backend is resolved and
#'   validated immediately. If `NULL` (the default), the stage inherits the
#'   backend set by [pump_run()] or [pump_drain()]; the inherited backend
#'   is resolved and validated when the stage first advances (its first
#'   beat), and stays fixed from then on.
#' @param max_workers Maximum number of active jobs for this stage. Defaults to
#'   the backend worker count. Ignored for the synchronous `main_backend()`
#'   (which always uses 1).
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

    # An explicit backend is resolved, validated, and sized eagerly so
    # mistakes fail fast at construction. backend = NULL defers to the
    # pipeline default set by pump_run()/pump_drain(): the stage resolves,
    # validates, and sizes it on its first beat.
    n <- x$length()
    if (!is.null(backend)) {
        backend <- .pump_resolve_backend(backend)
        .pump_check_backend(backend)
        max_workers <- .pump_size_max_workers(backend, max_workers, n)
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
        max_workers = max_workers,
        buffer_size = buffer_size,
        explicit_on_error = on_error,
        explicit_backend = backend
    )
}
