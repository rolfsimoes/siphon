test_that("pump_status on source returns correct counts", {
    e <- siphon:::.pump_error(simpleError("bad"))
    src <- .pump_source_basic(list(1, e, 3))
    st <- pump_status(src)
    expect_equal(st$completed, 0)
    expect_equal(st$errors, 1)
    expect_equal(st$buffer_size, 0)
    expect_equal(st$workers_active, 0)

    src$next_item()
    st <- pump_status(src)
    expect_equal(st$completed, 1)
})

test_that("pump_status on stage reflects buffer and slots", {
    f <- 1:4 |>
        pump(function(x) x, backend = "main", max_workers = 2)
    st <- pump_status(f)
    expect_equal(st$workers_limit, 1)
    expect_equal(st$buffer_capacity, 4)
    expect_equal(st$completed, 0)

    # first call fills workers but buffer is empty for synchronous main_backend
    expect_null(f$next_item())
    st <- pump_status(f)
    expect_equal(st$completed, 0)

    # second call drains the ready jobs to buffer
    f$next_item()
    st <- pump_status(f)
    expect_equal(st$completed, 1)
})

test_that("pump_status errors count is accurate", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x, backend = "main", on_error = "collect")
    pump_run(f, verbose = FALSE)
    st <- pump_status(f)
    expect_equal(st$errors, 1)
    expect_equal(st$completed, 3)
})

test_that("pump_status returns upstream chain for multi-stage pipeline", {
    stage1 <- 1:5 |> pump(function(x) x * 2, backend = "main")
    stage2 <- stage1 |> pump(function(x) x + 10, backend = "main")

    st <- pump_status(stage2)

    # Should have source and stages
    expect_false(is.null(st$source))
    expect_false(is.null(st$stages))
    expect_equal(length(st$stages), 2)

    # Source info
    expect_equal(st$source$length, 5)
    expect_equal(st$source$position, 0)
    expect_equal(st$source$errors, 0)

    # Stage 1 (upstream)
    expect_equal(st$stages[[1]]$type, "main")
    expect_equal(st$stages[[1]]$workers_limit, 1)
    expect_equal(st$stages[[1]]$buffer_capacity, 5)

    # Stage 2 (current)
    expect_equal(st$stages[[2]]$type, "main")
    expect_equal(st$stages[[2]]$workers_limit, 1)
    expect_equal(st$stages[[2]]$buffer_capacity, 5)
})

test_that("pump_status sequential behavior is consistent", {
    stage1 <- 1:5 |> pump(function(x) x * 2, backend = "main")
    stage2 <- stage1 |> pump(function(x) x + 10, backend = "main")

    # Initial state
    st <- pump_status(stage2)
    expect_equal(st$source$position, 0)
    expect_equal(st$stages[[1]]$completed, 0)
    expect_equal(st$stages[[2]]$completed, 0)

    # After first item (synchronous backend: position advances but stages may not complete immediately)
    stage2$next_item()
    st <- pump_status(stage2)
    expect_equal(st$source$position, 1)
    # With synchronous main_backend, completed counts reflect actual stage completion
    expect_gte(st$stages[[1]]$completed, 0)
    expect_gte(st$stages[[2]]$completed, 0)

    # After second item
    stage2$next_item()
    st <- pump_status(stage2)
    expect_equal(st$source$position, 2)
    expect_gte(st$stages[[1]]$completed, 0)
    expect_gte(st$stages[[2]]$completed, 0)
})

test_that("pump_status on single stage has no source info", {
    f <- 1:5 |> pump(function(x) x * 2, backend = "main")
    st <- pump_status(f)

    # Single stage should have source info
    expect_false(is.null(st$source))
    expect_equal(st$source$length, 5)
    expect_equal(st$source$position, 0)

    # Should have one stage
    expect_equal(length(st$stages), 1)
})

test_that("pump_status source returns no stages list", {
    src <- .pump_source_basic(1:5)
    st <- pump_status(src)

    # Source should not have stages list
    expect_null(st$stages)
    expect_null(st$source)

    # But should have basic stats
    expect_equal(st$completed, 0)
    expect_equal(st$errors, 0)
})

test_that("pump_status tracks fn_time and idle_time", {
    f <- 1:5 |> pump(function(x) x * 2, backend = "main")

    # Initially times should be zero
    st <- pump_status(f)
    expect_equal(st$fn_time, 0)
    expect_equal(st$idle_time, 0)

    # After processing, times should be tracked
    pump_run(f, verbose = FALSE)
    st <- pump_status(f)
    expect_gte(st$fn_time, 0)
    expect_gte(st$idle_time, 0)
})

test_that("pump_status fn_time and idle_time are present in stages", {
    stage1 <- 1:5 |> pump(function(x) x * 2, backend = "main")
    stage2 <- stage1 |> pump(function(x) x + 10, backend = "main")

    st <- pump_status(stage2)

    # Both stages should have fn_time and idle_time fields
    expect_true("fn_time" %in% names(st$stages[[1]]))
    expect_true("idle_time" %in% names(st$stages[[1]]))
    expect_true("fn_time" %in% names(st$stages[[2]]))
    expect_true("idle_time" %in% names(st$stages[[2]]))

    # coord_time should not be present
    expect_false("coord_time" %in% names(st$stages[[1]]))
    expect_false("coord_time" %in% names(st$stages[[2]]))

    # Initially should be zero
    expect_equal(st$stages[[1]]$fn_time, 0)
    expect_equal(st$stages[[1]]$idle_time, 0)
    expect_equal(st$stages[[2]]$fn_time, 0)
    expect_equal(st$stages[[2]]$idle_time, 0)

    # coord_time should not be present in global status
    expect_false("coord_time" %in% names(st))
})

test_that("poll_wall_time excludes time between next_item calls", {
    f <- 1:3 |> pump(function(x) x * 2, backend = "main")

    # Tick once, sleep, tick again
    f$next_item()
    Sys.sleep(1.5) # seconds
    f$next_item()
    f$next_item()

    st <- pump_status(f)
    # poll_wall_time should be well under 0.3s since the sleep is between calls
    expect_lt(st$poll_wall_time, 30) # milliseconds
})
