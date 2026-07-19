#' Pipeline status
#'
#' `pump_status()` returns a snapshot of a pump object's internal state,
#'   including per-stage timing metrics that help identify where time is spent
#'   and which stage is the bottleneck.
#'
#' @param x A pump object.
#'
#' @return A list of class `pump_status`. For a pipeline it contains `source`
#'   (source info) and `stages`, a list with one entry per stage:
#'   * `type` - backend name (`"main"`, `"mirai"`, `"future"`, `"parallel"`).
#'   * `workers_active` / `workers_limit` - in-flight jobs vs slot limit.
#'   * `buffer_size` / `buffer_capacity` - items ready in the output buffer.
#'   * `completed` - items that have finished this stage.
#'   * `errors` - items with `ok = FALSE` seen by this stage.
#'   * `beats` - number of effective `next_item()` calls (beats) on this
#'     stage. Beats on a finished stage are no-ops and are not counted.
#'   * `beats_working`, `beats_starved`, `beats_blocked` - how each beat was
#'     classified (see Details).
#'   * `pop_hits` / `pop_misses` - `pop_item()` calls that returned an item
#'     vs `NULL`.
#'   * `fn_time` - cumulative time executing the user function, in
#'     milliseconds. On asynchronous backends this sums time across workers
#'     and can exceed real elapsed time.
#'   * `tick_time` - cumulative time inside `next_item()` calls (ms).
#'   * `submit_time` - cumulative time dispatching jobs to the backend (ms).
#'     For the synchronous main backend this includes the function execution
#'     itself; for asynchronous backends it is serialization/dispatch cost.
#'   * `pull_time` - cumulative time spent asking upstream for items (ms).
#'     This includes the upstream stages' own beats, so it is reported on
#'     the upstream stages and excluded from this stage's `coord_time`.
#'   * `coord_time` - derived scheduling overhead for this stage alone:
#'     `max(0, tick_time - submit_time - pull_time)` (ms).
#'   * `fn_per_item` - average `fn_time` per completed item (ms).
#'   * `share_working` / `share_starved` / `share_blocked` - beat-state
#'     shares in `[0, 1]` (`NA` before the first beat).
#'   * `throughput` - completed items per second over the observed beat span
#'     (`NA` until two beats have happened; the span includes any pauses
#'     between interactive calls).
#'   * `in_flight` - one entry per active slot: `id`, `idx`, `since`
#'     (submission time), and `age_secs` (seconds in flight at snapshot time)
#'     of the item currently being processed.
#'   * `buffered_ids` - ids of the first few items ready in the buffer.
#'
#'   For plain sources the result is flat and reports `completed`, `errors`,
#'   `pop_hits`, `pop_misses`, and `pull_time` (ms inside `pop_item()`).
#'   For pipelines, the result also carries `delivered` - the number of items
#'   that left the pipeline (popped from the terminal stage by `pump_run()`,
#'   `pump_pop()`, or `pop_item()`); shown as the sink in `print()`.
#'
#'   For pipelines, the terminal stage's scalar fields are additionally copied
#'   to the top level. **This flat copy is deprecated** and will be removed in
#'   a future minor release; read per-stage values from `x$stages` instead.
#'
#' @details Durations are accumulated only inside `next_item()`/`pop_item()`
#'   calls: time between beats (interactive pauses, `sleep_ms` backoff in
#'   `pump_run()`) is deliberately never measured. Idle time is therefore not
#'   reported as a duration; instead each beat is classified, in priority
#'   order:
#'   * `blocked` - the output buffer is full while jobs are still in flight:
#'     downstream is not consuming. This wins over `working` because it is
#'     the bottleneck signal.
#'   * `working` - jobs in flight, or items delivered during this beat.
#'   * `starved` - free slots, but upstream had nothing to offer.
#'
#'   A beat that finds the stage finished (upstream exhausted, nothing in
#'   flight) cannot move anything and is not recorded, so beat counts and
#'   shares freeze once a pipeline is exhausted.
#'
#'   Under `pump_run()` beats are frequent, so beat shares approximate time
#'   shares. When stepping interactively, shares describe what each beat
#'   found.
#'
#' @section Display:
#' The status can be displayed using `print()` or `format()`. The display shows
#' a connected frame from source to sink with worker and buffer occupancy per
#' stage, timing, beat-state shares, a stuck-job warning for old in-flight
#' items, and a `* bottleneck` marker when one stage clearly dominates.
#'
#' The header shows the class name followed by the total number of beats
#' (pipeline scheduling cycles) in parentheses, e.g., `<pump (10)>`.
#'
#' @section Legend:
#' Compressed labels used in the output:
#' \describe{
#'   \item{wrk}{workers (active/limit)}
#'   \item{buf}{output buffer (size/capacity)}
#'   \item{done}{completed items}
#'   \item{err}{errors}
#'   \item{fn}{function time per item}
#'   \item{crd}{coordination time per beat}
#'   \item{wrk/stv/blk}{working/starved/blocked beat shares}
#' }
#'
#' @section Customization:
#' Colors can be customized via `options(siphon.colors = list(...))` to override
#' specific elements. The system uses standard 16-color ANSI codes:
#' \describe{
#'   \item{bold = "1"}{bold text}
#'   \item{dim = "2"}{dimmed text}
#'   \item{blue = "34"}{primary color}
#'   \item{green = "32"}{success}
#'   \item{yellow = "33"}{warning}
#'   \item{red = "31"}{error}
#'   \item{bright_blue = "94"}{bright primary}
#'   \item{bright_green = "92"}{bright success}
#'   \item{bright_yellow = "93"}{bright warning}
#'   \item{bright_red = "91"}{bright error}
#' }
#' Element-specific color names:
#' \describe{
#'   \item{header}{pipeline header text}
#'   \item{source}{"source" label}
#'   \item{sink}{"sink" label}
#'   \item{stage}{stage number (e.g., "stage 1")}
#'   \item{backend}{backend name (e.g., "main", "mirai")}
#'   \item{wrk}{workers label}
#'   \item{buf}{buffer label}
#'   \item{done}{completed label}
#'   \item{err}{errors label}
#'   \item{fn}{function time label}
#'   \item{crd}{coordination time label}
#'   \item{beat}{beats label}
#'   \item{wrk_share}{working share label}
#'   \item{stv}{starved share label}
#'   \item{blk}{blocked share label}
#'   \item{bottleneck}{bottleneck marker}
#' }
#' Examples:
#' \code{options(siphon.colors = list(stage = "96"))} to make stage names
#'   cyan
#' \code{options(siphon.colors = list(wrk = "1;34"))} to make workers label
#'   bold blue
#' \code{options(siphon.colors = list(bright_red = "35"))} to change error
#'   color to purple
#' To disable colors globally, set `options(siphon.color = FALSE)`.
#'
#' @examples
#' f <- 1:5 |> pump(function(x) x * 2, backend = "main")
#' pump_status(f)
#' @export
pump_status <- function(x) {
    UseMethod("pump_status")
}

# Number of ready buffer ids surfaced in a stage's status snapshot.
.pump_status_peek <- 8L

# Bottleneck heuristic (see .pump_find_bottleneck): a stage needs at least this
# many beats to be considered, must be working at least this share of the time,
# and a neighbor must corroborate above the neighbor share.
.pump_bottleneck_min_beats <- 5L
.pump_bottleneck_min_share <- 0.5
.pump_bottleneck_neighbor <- 0.25

# Pick the bottleneck stage index, or NA when there is no clear signal. This is
# analysis over the snapshot (not rendering): the stage with the highest
# working share, provided it has seen enough beats to be meaningful, is working
# most of the time, and its neighbors corroborate (downstream starving, or
# upstream blocked behind it).
.pump_find_bottleneck <- function(stages) {
    n <- length(stages)
    if (n < 2L) {
        return(NA_integer_)
    }
    shares <- vapply(stages, function(s) {
        if (isTRUE(s$beats >= .pump_bottleneck_min_beats) &&
                is.finite(s$share_working)) {
            s$share_working
        } else {
            -Inf
        }
    }, numeric(1))
    cand <- which.max(shares)
    if (!is.finite(shares[cand]) || shares[cand] < .pump_bottleneck_min_share) {
        return(NA_integer_)
    }
    corroborates <- .pump_bottleneck_neighbor
    downstream_starved <- cand < n &&
        isTRUE(stages[[cand + 1L]]$share_starved > corroborates)
    upstream_blocked <- cand > 1L &&
        isTRUE(stages[[cand - 1L]]$share_blocked > corroborates)
    if (downstream_starved || upstream_blocked) {
        cand
    } else {
        NA_integer_
    }
}

# Derived per-stage metrics computed from a stats snapshot
.pump_stage_derived <- function(snap, completed) {
    span_s <- if (!is.na(snap$first_beat_at) && !is.na(snap$last_beat_at)) {
        snap$last_beat_at - snap$first_beat_at
    } else {
        NA_real_
    }
    shares <- if (snap$beats > 0L) {
        list(
            share_working = snap$beats_working / snap$beats,
            share_starved = snap$beats_starved / snap$beats,
            share_blocked = snap$beats_blocked / snap$beats
        )
    } else {
        list(
            share_working = NA_real_,
            share_starved = NA_real_,
            share_blocked = NA_real_
        )
    }
    c(
        list(
            coord_time = max(
                0,
                snap$tick_time - snap$submit_time - snap$pull_time
            ),
            fn_per_item = snap$fn_time / max(1L, completed),
            throughput = if (!is.na(span_s) && span_s > 0) {
                completed / span_s
            } else {
                NA_real_
            }
        ),
        shares
    )
}

#' @export
pump_status.pump <- function(x) {
    # Detect source vs stage by the explicit role tag, not by sniffing NULL
    # buffers/slots: an empty stage also has neither, and used to be
    # misreported here as a bare source (F4).
    is_source <- identical(attr(x, "role"), "source")

    if (is_source) {
        snap <- if (is.function(x$stats)) x$stats() else list()
        result <- list(
            buffer_size = 0L,
            buffer_capacity = 0L,
            workers_active = 0L,
            workers_limit = 0L,
            completed = x$stage_completed(),
            errors = x$errors(),
            pop_hits = snap$pop_hits %||% 0L,
            pop_misses = snap$pop_misses %||% 0L,
            pull_time = snap$pull_time %||% NA_real_
        )
        return(structure(result, class = "pump_status"))
    }

    # For stages, collect status for all stages in the upstream chain
    stages <- list()
    source_info <- NULL
    current <- x
    stage_num <- 1

    while (inherits(current, "pump")) {
        buf <- current$buffer()
        sl <- current$slots()

        # Source vs stage by role tag (see the top-level check above).
        is_current_source <- identical(attr(current, "role"), "source")

        if (is_current_source) {
            # This is a source, save it and stop
            snap <- if (is.function(current$stats)) current$stats() else list()
            source_info <- list(
                type = .pump_backend_name(current$backend()),
                length = current$length(),
                position = current$progress(),
                errors = current$errors(),
                pop_hits = snap$pop_hits %||% 0L,
                pop_misses = snap$pop_misses %||% 0L,
                pull_time = snap$pull_time %||% NA_real_
            )
            break
        }

        if (!is.null(buf)) {
            buffer_size <- buf$size()
            buffer_capacity <- buf$size() + buf$remaining()
            buffered_ids <- lapply(
                buf$peek(.pump_status_peek), function(m) m$id
            )
        } else {
            buffer_size <- 0L
            buffer_capacity <- 0L
            buffered_ids <- list()
        }

        if (!is.null(sl)) {
            workers_active <- sl$active()
            workers_limit <- sl$limit()
            in_flight <- if (is.function(sl$inspect)) sl$inspect() else list()
            # Stamp each in-flight item's age at snapshot time so the renderer
            # stays a pure function of the snapshot (no Sys.time() at render).
            now <- Sys.time()
            in_flight <- lapply(in_flight, function(fl) {
                fl$age_secs <- as.numeric(
                    difftime(now, fl$since, units = "secs")
                )
                fl
            })
        } else {
            workers_active <- 0L
            workers_limit <- 0L
            in_flight <- list()
        }

        completed <- current$stage_completed()
        snap <- if (is.function(current$stats)) {
            current$stats()
        } else {
            .pump_stats()$snapshot()
        }

        stages[[stage_num]] <- c(
            list(
                type = .pump_backend_name(current$backend()),
                workers_active = workers_active,
                workers_limit = workers_limit,
                buffer_size = buffer_size,
                buffer_capacity = buffer_capacity,
                completed = completed,
                errors = current$errors(),
                on_error = if (is.function(current$get_on_error)) {
                    current$get_on_error()
                } else {
                    "stop"
                }
            ),
            snap[setdiff(
                names(snap),
                c("errors", "first_beat_at", "last_beat_at")
            )],
            .pump_stage_derived(snap, completed),
            list(
                in_flight = in_flight,
                buffered_ids = buffered_ids
            )
        )
        stage_num <- stage_num + 1

        upstream_fn <- current$upstream
        if (is.function(upstream_fn)) {
            current <- upstream_fn()
        } else {
            break
        }
    }

    # Reverse to put upstream stages first
    stages <- rev(stages)

    # Deprecated: copy the terminal stage's scalar fields to the top level.
    # Retained for backward compatibility; scheduled for removal in a future
    # minor release (F6). New code should read from `stages` instead.
    last <- if (length(stages) > 0) stages[[length(stages)]] else NULL
    top_fields <- c(
        "buffer_size", "buffer_capacity", "workers_active", "workers_limit",
        "completed", "errors", "beats", "beats_working", "beats_starved",
        "beats_blocked", "pop_hits", "pop_misses", "fn_time",
        "tick_time", "submit_time", "coord_time", "fn_per_item", "throughput"
    )
    top <- if (is.null(last)) {
        stats <- as.list(rep(0L, length(top_fields)))
        names(stats) <- top_fields
        stats
    } else {
        last[top_fields]
    }

    result <- c(
        list(source = source_info, stages = stages),
        top,
        # items that actually left the pipeline (the sink)
        list(delivered = if (is.null(last)) 0L else last$pop_hits)
    )

    structure(result, class = "pump_status")
}
