#' Pipeline status
#'
#' `pump_status()` returns a snapshot of a pump object's internal state,
#'   including detailed timing metrics that help identify where time is spent.
#'
#' @param x A pump object.
#'
#' @return A list with:
#'   * `buffer_size` - current items in the output buffer.
#'   * `buffer_capacity` - maximum buffer size.
#'   * `workers_active` - currently running jobs.
#'   * `workers_limit` - maximum concurrent jobs for this stage.
#'   * `completed` - items that have finished this stage.
#'   * `errors` - items with `ok = FALSE` seen by this stage.
#'   * `poll_hits` - number of successful polls (items ready).
#'   * `poll_misses` - number of failed polls (no item ready).
#'   * `poll_wall_time` - total time spent inside `next_item()` calls in milliseconds.
#'   * `fn_time` - wall-clock time spent executing the user function (useful work) in milliseconds.
#'   * `idle_time` - time spent waiting (starvation or exhaustion) in milliseconds.
#'
#' @details Note that `poll_wall_time` excludes time spent in backoff sleeps
#'   (controlled by the `sleep_ms` parameter in `pump_run()` and `pump_drain()`).
#'   Total wall-clock time when using `pump_run()` is approximately
#'   `poll_wall_time + poll_misses * sleep_ms`. For fast operations,
#'   the backoff overhead can dominate total runtime. siphon is designed for
#'   pipelines with substantial work per item **where coordination overhead is
#'   negligible compared to actual work time**.
#' @examples
#' f <- 1:5 |> pump(function(x) x * 2, backend = "main")
#' pump_status(f)
#' @export
pump_status <- function(x) {
    UseMethod("pump_status")
}

#' @export
pump_status.pump <- function(x) {
    buf <- x$buffer()
    sl <- x$slots()

    # Check if this is a source (no slots/buffer)
    is_source <- is.null(buf) && is.null(sl)

    if (is_source) {
        # For sources, return only the source's stats (no stages list)
        result <- list(
            buffer_size = 0L,
            buffer_capacity = 0L,
            workers_active = 0L,
            workers_limit = 0L,
            completed = x$stage_completed(),
            errors = x$errors(),
            poll_hits = if (is.function(x$poll_hits)) x$poll_hits() else 0L,
            poll_misses = if (is.function(x$poll_misses)) x$poll_misses() else 0L,
            poll_wall_time = if (is.function(x$poll_wall_time)) x$poll_wall_time() else NA_real_
        )
        structure(result, class = "pump_status")
    } else {
        # For stages, collect status for all stages in the upstream chain
        stages <- list()
        source_info <- NULL
        current <- x
        stage_num <- 1

        while (inherits(current, "pump")) {
            buf <- current$buffer()
            sl <- current$slots()

            # Check if this is a source (no slots/buffer)
            is_current_source <- is.null(buf) && is.null(sl)

            if (is_current_source) {
                # This is a source, save it and stop
                backend_info <- current$backend()
                backend_name <- if (inherits(backend_info, "pump_main_backend")) {
                    "main"
                } else if (inherits(backend_info, "pump_mirai_backend")) {
                    "mirai"
                } else if (inherits(backend_info, "pump_future_backend")) {
                    "future"
                } else {
                    "unknown"
                }

                source_info <- list(
                    type = backend_name,
                    length = current$length(),
                    position = current$progress(),
                    errors = current$errors()
                )
                break
            } else {
                if (!is.null(buf)) {
                    buffer_size <- buf$size()
                    buffer_capacity <- buf$size() + buf$remaining()
                } else {
                    buffer_size <- 0L
                    buffer_capacity <- 0L
                }

                if (!is.null(sl)) {
                    workers_active <- sl$active()
                    workers_limit <- sl$limit()
                } else {
                    workers_active <- 0L
                    workers_limit <- 0L
                }

                backend_info <- current$backend()
                backend_name <- if (inherits(backend_info, "pump_main_backend")) {
                    "main"
                } else if (inherits(backend_info, "pump_mirai_backend")) {
                    "mirai"
                } else if (inherits(backend_info, "pump_future_backend")) {
                    "future"
                } else {
                    "unknown"
                }

                stages[[stage_num]] <- list(
                    type = backend_name,
                    workers_active = workers_active,
                    workers_limit = workers_limit,
                    buffer_size = buffer_size,
                    buffer_capacity = buffer_capacity,
                    completed = current$stage_completed(),
                    errors = current$errors(),
                    on_error = if (is.function(current$get_on_error)) current$get_on_error() else "stop",
                    poll_hits = if (is.function(current$poll_hits)) current$poll_hits() else 0L,
                    poll_misses = if (is.function(current$poll_misses)) current$poll_misses() else 0L,
                    poll_wall_time = if (is.function(current$poll_wall_time)) current$poll_wall_time() else NA_real_,
                    fn_time = if (is.function(current$fn_time)) current$fn_time() else 0.0,
                    idle_time = if (is.function(current$idle_time)) current$idle_time() else 0.0
                )
                stage_num <- stage_num + 1
            }

            upstream_fn <- current$upstream
            if (is.function(upstream_fn)) {
                current <- upstream_fn()
            } else {
                break
            }
        }

        # Reverse to put upstream stages first
        stages <- rev(stages)

        result <- list(
            source = source_info,
            stages = stages,
            # For backward compatibility, also include the current stage's stats
            buffer_size = if (length(stages) > 0) stages[[length(stages)]]$buffer_size else 0L,
            buffer_capacity = if (length(stages) > 0) stages[[length(stages)]]$buffer_capacity else 0L,
            workers_active = if (length(stages) > 0) stages[[length(stages)]]$workers_active else 0L,
            workers_limit = if (length(stages) > 0) stages[[length(stages)]]$workers_limit else 0L,
            completed = if (length(stages) > 0) stages[[length(stages)]]$completed else 0L,
            errors = if (length(stages) > 0) stages[[length(stages)]]$errors else 0L,
            poll_hits = if (length(stages) > 0) stages[[length(stages)]]$poll_hits else 0L,
            poll_misses = if (length(stages) > 0) stages[[length(stages)]]$poll_misses else 0L,
            poll_wall_time = if (length(stages) > 0) stages[[length(stages)]]$poll_wall_time else NA_real_,
            fn_time = if (length(stages) > 0) stages[[length(stages)]]$fn_time else 0.0,
            idle_time = if (length(stages) > 0) stages[[length(stages)]]$idle_time else 0.0
        )

        structure(result, class = "pump_status")
    }
}

#' @export
print.pump_status <- function(x, ...) {
    cat("<pump_status>\n")

    # Print source if available
    if (!is.null(x$source)) {
        s <- x$source
        cat("  Source (", s$type, "):\n", sep = "")
        cat("    length:   ", s$length, "\n", sep = "")
        cat("    position: ", s$position, "\n", sep = "")
        cat("    errors:   ", s$errors, "\n", sep = "")
    }

    # If stages list is available, print all stages
    if (!is.null(x$stages)) {
        n_stages <- length(x$stages)
        for (i in seq_along(x$stages)) {
            s <- x$stages[[i]]
            is_last_stage <- (i == n_stages)
            cat("  Stage ", i, " (", s$type, "):\n", sep = "")
            if (!is.null(s$on_error) && s$on_error != "stop") {
                cat("    on_error:", s$on_error, "\n")
            }
            cat("    workers: ", s$workers_active, "/", s$workers_limit, "\n", sep = "")
            cat("    buffer:  ", s$buffer_size, "/", s$buffer_capacity, "\n", sep = "")
            cat("    done:    ", s$completed, "\n", sep = "")
            cat("    errors:  ", s$errors, "\n", sep = "")

            # Only show polling stats and timing for the last (current) stage
            if (is_last_stage) {
                total_polls <- s$poll_hits + s$poll_misses
                if (total_polls > 0) {
                    hit_ratio <- round(s$poll_hits / total_polls * 100, 1)
                    cat("    polls:   ", s$poll_hits, " hits, ", s$poll_misses, " misses (", hit_ratio, "% hit)\n", sep = "")
                }

                total_time <- s$fn_time + s$idle_time
                if (total_time > 0) {
                    cat("    time:    ", round(total_time, 1), "ms (fn: ", round(s$fn_time, 1), "ms, idle: ", round(s$idle_time, 1), "ms)\n", sep = "")
                }
            }
        }
    } else {
        # Fallback to single-stage display for backward compatibility
        cat("  workers: ", x$workers_active, "/", x$workers_limit, "\n", sep = "")
        cat("  buffer:  ", x$buffer_size, "/", x$buffer_capacity, "\n", sep = "")
        cat("  done:    ", x$completed, "\n", sep = "")
        cat("  errors:  ", x$errors, "\n", sep = "")

        total_polls <- x$poll_hits + x$poll_misses
        if (total_polls > 0) {
            hit_ratio <- round(x$poll_hits / total_polls * 100, 1)
            cat("  polls:   ", x$poll_hits, " hits, ", x$poll_misses, " misses (", hit_ratio, "% hit)\n", sep = "")
        }

        total_time <- x$fn_time + x$idle_time
        if (total_time > 0) {
            cat("  time:    ", round(total_time, 1), "ms (fn: ", round(x$fn_time, 1), "ms, idle: ", round(x$idle_time, 1), "ms)\n", sep = "")
        }
    }

    # Global summary
    if (!is.null(x$stages) && length(x$stages) > 0) {
        total_fn <- sum(vapply(x$stages, function(s) s$fn_time, numeric(1)))
        total_idle <- sum(vapply(x$stages, function(s) s$idle_time, numeric(1)))
        total_time <- total_fn + total_idle
        pipeline_time <- x$poll_wall_time

        if (total_time > 0 && is.finite(total_time)) {
            cat("\n  Summary:\n")
            cat("    poll_wall_time: ", round(pipeline_time, 1), "ms\n", sep = "")
            cat("    fn:             ", round(total_fn, 1), "ms\n", sep = "")
            cat("    idle:           ", round(total_idle, 1), "ms\n", sep = "")
        }
    }

    invisible(x)
}

#' Print a pump pipeline
#'
#' `print.pump()` displays the status of all stages in a pipeline.
#'
#' @param x A pump object.
#' @param ... Unused.
#' @export
print.pump <- function(x, ...) {
    if (!inherits(x, "pump")) {
        cat("<pump source>\n")
        return(invisible(x))
    }

    cat("<pump pipeline>\n")

    # Use pump_status which now collects all stages in the upstream chain
    status <- pump_status(x)

    # Print source if available
    if (!is.null(status$source)) {
        s <- status$source
        cat("  Source (", s$type, "):\n", sep = "")
        cat("    length:   ", s$length, "\n", sep = "")
        cat("    position: ", s$position, "\n", sep = "")
        cat("    errors:   ", s$errors, "\n", sep = "")
    }

    # Print stages
    if (!is.null(status$stages)) {
        n_stages <- length(status$stages)
        for (i in seq_along(status$stages)) {
            s <- status$stages[[i]]
            is_last_stage <- (i == n_stages)
            cat("  Stage ", i, " (", s$type, "):\n", sep = "")
            if (!is.null(s$on_error) && s$on_error != "stop") {
                cat("    on_error:", s$on_error, "\n")
            }
            cat("    workers: ", s$workers_active, "/", s$workers_limit, "\n", sep = "")
            cat("    buffer:  ", s$buffer_size, "/", s$buffer_capacity, "\n", sep = "")
            cat("    completed: ", s$completed, "\n", sep = "")
            cat("    errors:  ", s$errors, "\n", sep = "")

            # Only show timing for the last (current) stage
            if (is_last_stage) {
                total_time <- s$fn_time + s$idle_time
                if (total_time > 0) {
                    cat("    time:    ", round(total_time, 1), "ms (fn: ", round(s$fn_time, 1), "ms, idle: ", round(s$idle_time, 1), "ms)\n", sep = "")
                }
            }
        }
    }

    invisible(x)
}
