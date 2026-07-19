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
    # errors are part of the snapshot, so reset() clears them too (F6)
    expect_equal(snap$errors, 0L)
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
