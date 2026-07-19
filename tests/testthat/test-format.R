# Rendering tests for format.pump_status(). The regex tests drive real
# pipelines; the snapshot tests render fully-fabricated, deterministic status
# objects so every branch (bottleneck, stuck job, error chips, color, unicode)
# is exercised without wall-clock noise - the renderer is a pure function of
# the snapshot (item ages are stamped by pump_status(), not read at render).

# A complete fabricated stage entry; override any field per test.
fake_stage <- function(type = "main", workers_active = 0L, workers_limit = 1L,
                       buffer_size = 0L, buffer_capacity = 5L, completed = 0L,
                       errors = 0L, beats = 0L, fn_per_item = 0, coord_time = 0,
                       share_working = NA_real_, share_starved = NA_real_,
                       share_blocked = NA_real_, in_flight = list()) {
    list(
        type = type, workers_active = workers_active,
        workers_limit = workers_limit, buffer_size = buffer_size,
        buffer_capacity = buffer_capacity, completed = completed,
        errors = errors, beats = beats, fn_per_item = fn_per_item,
        coord_time = coord_time, share_working = share_working,
        share_starved = share_starved, share_blocked = share_blocked,
        in_flight = in_flight, buffered_ids = list()
    )
}

fake_status <- function(stages, source = NULL, delivered = 0L) {
    structure(
        list(source = source, stages = stages, delivered = delivered),
        class = "pump_status"
    )
}

fake_flat <- function(completed = 0L, errors = 0L, pop_hits = 0L,
                      pop_misses = 0L) {
    structure(
        list(completed = completed, errors = errors, pop_hits = pop_hits,
             pop_misses = pop_misses),
        class = "pump_status"
    )
}

# --- Regression tests over real pipelines (moved from test-stats.R) --------

test_that("format.pump_status renders the source-to-sink frame", {
    old <- options(siphon.color = FALSE, siphon.unicode = FALSE)
    on.exit(options(old), add = TRUE)

    p <- 1:5 |>
        pump(function(x) x + 1, backend = "main") |>
        pump(function(x) x * 2, backend = "main")
    pump_step(p, 3)

    lines <- format(pump_status(p), header = "pump pipeline")
    txt <- paste(lines, collapse = "\n")

    expect_match(lines[1], "^<pump pipeline \\(6\\)>$")
    expect_match(lines[2], "^\\+- source   \\d+/5$")
    expect_match(txt, "\\+- stage 1 main")
    expect_match(txt, "\\+- stage 2 main")
    expect_match(txt, "wrk \\[")
    expect_match(txt, "buf \\[")
    expect_match(txt, "fn .*ms/it")
    # the sink closes the frame with the delivered count
    expect_match(lines[length(lines)], "^\\+- sink   \\d+/5$")
    # no ANSI escapes with colors off
    expect_false(grepl("\033", txt, fixed = TRUE))

    # popping an item moves it to the sink
    pump_pop(p)
    lines <- format(pump_status(p), header = "pump pipeline")
    expect_match(lines[length(lines)], "^\\+- sink   1/5$")
})

test_that("format.pump_status uses box-drawing glyphs when unicode is on", {
    old <- options(siphon.color = FALSE, siphon.unicode = TRUE)
    on.exit(options(old), add = TRUE)

    p <- 1:3 |> pump(function(x) x, backend = "main")
    pump_step(p)
    lines <- format(pump_status(p), header = "pump pipeline")
    expect_match(lines[2], "^\u250c\u2500 source")
    expect_match(lines[length(lines)], "^\u2514\u2500 sink")
})

test_that("format.pump_status renders a flat source status", {
    old <- options(siphon.color = FALSE)
    on.exit(options(old), add = TRUE)
    src <- siphon:::.pump_source_basic(1:3)
    src$next_item()
    src$pop_item()
    lines <- format(pump_status(src))
    expect_equal(lines[1], "pump_status")
    expect_match(paste(lines, collapse = "\n"), "pulled:  1")
    expect_match(paste(lines, collapse = "\n"), "pops:    1 hits, 0 misses")
})

# --- Deterministic snapshot tests over fabricated statuses -----------------

test_that("flat source status snapshot", {
    st <- fake_flat(completed = 7L, errors = 2L, pop_hits = 7L, pop_misses = 3L)
    expect_snapshot(cat(format(st, color = FALSE), sep = "\n"))
})

test_that("multi-stage frame snapshot (ascii, no color)", {
    st <- fake_status(
        source = list(type = "main", length = 20, position = 12, errors = 0L),
        stages = list(
            fake_stage(workers_active = 1L, workers_limit = 4L, buffer_size = 2L,
                       completed = 8L, beats = 10L, fn_per_item = 42,
                       coord_time = 15, share_working = 0.8,
                       share_starved = 0.1, share_blocked = 0.1),
            fake_stage(type = "mirai", workers_limit = 4L, buffer_size = 5L,
                       completed = 4L, beats = 10L, fn_per_item = 8,
                       coord_time = 3, share_working = 0.3,
                       share_starved = 0.6, share_blocked = 0.1)
        ),
        delivered = 4L
    )
    expect_snapshot(
        cat(format(st, header = "pump", color = FALSE, unicode = FALSE),
            sep = "\n")
    )
})

test_that("bottleneck marker + error chips snapshot", {
    st <- fake_status(
        source = list(type = "main", length = 20, position = 12, errors = 3L),
        stages = list(
            fake_stage(workers_active = 2L, workers_limit = 4L, buffer_size = 3L,
                       completed = 8L, errors = 0L, beats = 10L,
                       fn_per_item = 42, coord_time = 15, share_working = 0.9,
                       share_starved = 0, share_blocked = 0.1),
            fake_stage(type = "mirai", workers_active = 1L, workers_limit = 4L,
                       buffer_size = 5L, completed = 4L, errors = 2L,
                       beats = 10L, fn_per_item = 8, coord_time = 3,
                       share_working = 0.2, share_starved = 0.7,
                       share_blocked = 0)
        ),
        delivered = 4L
    )
    # both source (err 3) and stage 2 (err 2) chips should be bright red
    expect_snapshot(
        cat(format(st, header = "pump", color = FALSE, unicode = TRUE),
            sep = "\n")
    )
})

test_that("stuck-job warning snapshot", {
    st <- fake_status(
        source = list(type = "main", length = 20, position = 5, errors = 0L),
        stages = list(
            fake_stage(workers_active = 1L, workers_limit = 4L, completed = 4L,
                       beats = 10L, fn_per_item = 8, coord_time = 3,
                       share_working = 1, share_starved = 0, share_blocked = 0,
                       in_flight = list(list(id = 7L, idx = 7L, age_secs = 5.0)))
        ),
        delivered = 4L
    )
    expect_snapshot(
        cat(format(st, header = "pump", color = FALSE, unicode = TRUE),
            sep = "\n")
    )
})

test_that("colored render snapshot exercises the ANSI path", {
    old <- options(siphon.colors = NULL)
    on.exit(options(old), add = TRUE)
    st <- fake_status(
        source = list(type = "main", length = 10, position = 6, errors = 1L),
        stages = list(
            fake_stage(workers_active = 1L, workers_limit = 2L, buffer_size = 2L,
                       buffer_capacity = 2L, completed = 6L, errors = 1L,
                       beats = 8L, fn_per_item = 120, coord_time = 40,
                       share_working = 0.5, share_starved = 0.25,
                       share_blocked = 0.25)
        ),
        delivered = 6L
    )
    expect_snapshot(
        cat(format(st, header = "pump", color = TRUE, unicode = TRUE),
            sep = "\n")
    )
})
