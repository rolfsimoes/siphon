#' Advance a pipeline without consuming results
#'
#' `pump_step()` runs one or more beats on a pipeline. A beat lets the
#' terminal stage harvest finished jobs into its output buffer and pull new
#' work from upstream (recursively beating every upstream stage). Nothing is
#' consumed: stepping is safe to repeat while inspecting a pipeline with
#' `print()`, [pump_status()], or [pump_peek()].
#'
#' @param x A pump object.
#' @param beats Number of beats to run. Defaults to 1. Stepping stops early
#'   once further beats cannot change anything: when the pipeline is
#'   finished (source exhausted, nothing in flight), or when it is fully
#'   stuck behind backpressure (output buffer full and all slots busy - pop
#'   or run to unblock it). Beat counts therefore do not inflate on a
#'   pipeline that cannot move.
#'
#' @return `x`, visibly, so a bare `pump_step(p)` at the console also prints
#'   the pipeline state.
#' @examples
#' p <- 1:5 |> pump(function(x) x * 2, backend = "main")
#' pump_step(p, 2)
#' p # inspect state: items in flight and ready
#' pump_peek(p) # look at the next result without consuming it
#' pump_run(p, verbose = FALSE) # resumes and completes the pipeline
#' @family inspection
#' @export
pump_step <- function(x, beats = 1L) {
    if (!inherits(x, "pump")) stop("x must be a pump object")
    if (!is.numeric(beats) || length(beats) != 1L || is.na(beats) || beats < 1L) {
        stop("beats must be a positive number")
    }
    for (b in seq_len(as.integer(beats))) {
        state <- x$next_item()
        # nothing left to do: source exhausted and nothing in flight
        if (identical(state, "done")) break
        # fully stuck: output buffer full and every slot busy, so further
        # beats are guaranteed no-ops until something is consumed
        if (identical(state, "blocked") && x$slots()$n_free() == 0L) break
    }
    x
}

#' Peek at ready results without consuming them
#'
#' `pump_peek()` returns up to `n` items that are ready in the terminal
#' stage's output buffer, without removing them: calling it repeatedly
#' returns the same items, and the pipeline is unaffected.
#'
#' Each item is a list with `id`, `idx`, `data`, and `ok`. Use [pump_step()]
#' first to advance work into the buffer.
#'
#' @param x A pump object.
#' @param n Maximum number of items to return. Defaults to 1.
#'
#' @return A list of up to `n` ready items (empty when nothing is ready).
#'   Sources have no buffer, so peeking a bare source returns an empty list.
#' @examples
#' p <- 1:5 |> pump(function(x) x * 2, backend = "main")
#' pump_step(p)
#' pump_peek(p)
#' @family inspection
#' @export
pump_peek <- function(x, n = 1L) {
    if (!inherits(x, "pump")) stop("x must be a pump object")
    buf <- x$buffer()
    if (is.null(buf)) {
        return(list())
    }
    buf$peek(n)
}

#' Consume one ready result from a pipeline
#'
#' `pump_pop()` removes and returns the next ready item from the terminal
#' stage's output buffer, or `NULL` when nothing is ready. **This consumes
#' the item**: it will not appear in the results of a later `pump_run()`,
#' and responsibility for its lifecycle transfers to you. For managed
#' sources (see [pump_source()]), call `x$item_commit(id, data)` and
#' `x$item_release(id)` after successfully handling the item, exactly as
#' `pump_run()` does; use [pump_peek()] instead when you only want to look.
#'
#' @param x A pump object.
#'
#' @return A list with `id`, `idx`, `data`, and `ok`, or `NULL` when no item
#'   is ready.
#' @examples
#' p <- 1:5 |> pump(function(x) x * 2, backend = "main")
#' pump_step(p)
#' v <- pump_pop(p)
#' v$data
#' @family inspection
#' @export
pump_pop <- function(x) {
    if (!inherits(x, "pump")) stop("x must be a pump object")
    x$pop_item()
}
