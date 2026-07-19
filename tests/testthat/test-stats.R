test_that(".pump_stats records beats, pops, and durations", {
    s <- siphon:::.pump_stats()
    snap <- s$snapshot()
    expect_equal(snap$beats, 0L)
    expect_equal(snap$pop_hits, 0L)
    expect_true(is.na(snap$first_beat_at))

    s$record_beat(2.5, "working")
    s$record_beat(1.0, "starved")
    s$record_beat(1.0, "blocked")
    s$record_pop(TRUE)
    s$record_pop(FALSE)
    s$add_fn_time(100)
    s$add_submit_time(10)
    s$add_error()

    snap <- s$snapshot()
    expect_equal(snap$beats, 3L)
    expect_equal(snap$beats_working, 1L)
    expect_equal(snap$beats_starved, 1L)
    expect_equal(snap$beats_blocked, 1L)
    expect_equal(snap$pop_hits, 1L)
    expect_equal(snap$pop_misses, 1L)
    expect_equal(snap$tick_time, 4.5)
    expect_equal(snap$fn_time, 100)
    expect_equal(snap$submit_time, 10)
    expect_equal(snap$errors, 1L)
    expect_false(is.na(snap$first_beat_at))

    expect_error(s$record_beat(1, "bogus"), "unknown beat state")

    s$reset()
    snap <- s$snapshot()
    expect_equal(snap$beats, 0L)
    expect_equal(snap$tick_time, 0)
    expect_true(is.na(snap$first_beat_at))
    # errors survive a stats reset
    expect_equal(snap$errors, 1L)
})

test_that("fn_time and submit_time are recorded in milliseconds", {
    f <- 1:3 |> pump(function(x) {
        Sys.sleep(0.02)
        x
    }, backend = "main")
    pump_run(f, verbose = FALSE)

    st <- pump_status(f)
    # 3 items x 20ms, with clock tolerance
    expect_gte(st$fn_time, 3 * 20 * 0.9)
    expect_lt(st$fn_time, 3000)
    expect_equal(st$fn_per_item, st$fn_time / 3)
    # main backend runs the job inside submit, so submit_time covers
    # fn_time; generous tolerance because the two are taken with different
    # clocks (proc.time() vs Sys.time()) that can drift apart under load
    expect_gte(st$submit_time, st$fn_time * 0.75)
    expect_gte(st$coord_time, 0)
})

make_fake_stage <- function(beats = 10L, working = 0, starved = 0, blocked = 0) {
    list(
        beats = beats,
        share_working = working,
        share_starved = starved,
        share_blocked = blocked
    )
}

test_that(".pump_find_bottleneck spots the dominant stage", {
    # downstream of the bottleneck is starved
    stages <- list(
        make_fake_stage(working = 0.9, starved = 0.1),
        make_fake_stage(working = 0.2, starved = 0.8)
    )
    expect_equal(siphon:::.pump_find_bottleneck(stages), 1L)

    # terminal bottleneck: upstream blocked behind it
    stages <- list(
        make_fake_stage(working = 0.4, blocked = 0.6),
        make_fake_stage(working = 0.95)
    )
    expect_equal(siphon:::.pump_find_bottleneck(stages), 2L)

    # balanced pipeline: no bottleneck
    stages <- list(
        make_fake_stage(working = 0.9),
        make_fake_stage(working = 0.9)
    )
    expect_true(is.na(siphon:::.pump_find_bottleneck(stages)))

    # too few beats: no verdict
    stages <- list(
        make_fake_stage(beats = 2L, working = 1),
        make_fake_stage(beats = 2L, working = 0.1, starved = 0.9)
    )
    expect_true(is.na(siphon:::.pump_find_bottleneck(stages)))

    # single stage: bottleneck is meaningless
    expect_true(is.na(siphon:::.pump_find_bottleneck(list(make_fake_stage(working = 1)))))
})

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
