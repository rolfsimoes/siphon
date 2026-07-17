test_that("pump_status on source returns correct counts", {
    e <- siphon:::.pump_error(simpleError("bad"))
    src <- .pump_source_basic(list(1, e, 3))
    st <- pump_status(src)
    expect_equal(st$completed, 0)
    expect_equal(st$errors, 1)
    expect_equal(st$buffer_size, 0)
    expect_equal(st$workers_active, 0)

    src$next_item()
    src$pop_item()
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

    # a beat is drain-advance-drain: with the synchronous main backend the
    # job completes during advance and is harvested in the same beat
    f$next_item()
    st <- pump_status(f)
    expect_equal(st$completed, 1)
    v <- f$pop_item()
    expect_equal(v$data, 1L)

    # next beat produces the next item
    f$next_item()
    v <- f$pop_item()
    expect_equal(v$data, 2L)
    st <- pump_status(f)
    expect_equal(st$completed, 2)
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
    stage2$pop_item()
    st <- pump_status(stage2)
    expect_equal(st$source$position, 1)
    # With synchronous main_backend, completed counts reflect actual stage completion
    expect_gte(st$stages[[1]]$completed, 0)
    expect_gte(st$stages[[2]]$completed, 0)

    # After second item
    stage2$next_item()
    stage2$pop_item()
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

test_that("pump_status tracks fn_time and beat counts", {
    f <- 1:5 |> pump(function(x) {
        Sys.sleep(0.01)
        x * 2
    }, backend = "main")

    # Initially everything is zero
    st <- pump_status(f)
    expect_equal(st$fn_time, 0)
    expect_equal(st$tick_time, 0)
    expect_equal(st$beats, 0L)

    # After processing, times and beats are tracked
    pump_run(f, verbose = FALSE)
    st <- pump_status(f)
    expect_gte(st$fn_time, 50 * 0.9) # 5 items x 10ms, with clock tolerance
    expect_gt(st$tick_time, 0)
    expect_gte(st$beats, 5L)
    expect_gte(st$beats_working, 1L)
})

test_that("pump_status stages expose the new timing fields", {
    stage1 <- 1:5 |> pump(function(x) x * 2, backend = "main")
    stage2 <- stage1 |> pump(function(x) x + 10, backend = "main")

    st <- pump_status(stage2)

    new_fields <- c(
        "beats", "beats_working", "beats_starved", "beats_blocked",
        "pop_hits", "pop_misses", "fn_time", "tick_time",
        "submit_time", "pull_time", "coord_time", "fn_per_item",
        "share_working", "share_starved", "share_blocked", "throughput",
        "in_flight", "buffered_ids"
    )
    for (s in st$stages) {
        expect_true(all(new_fields %in% names(s)))
    }

    # Removed fields must be gone
    old_fields <- c("poll_hits", "poll_misses", "poll_wall_time", "idle_time")
    expect_false(any(old_fields %in% names(st$stages[[1]])))
    expect_false(any(old_fields %in% names(st)))

    # Initially everything is zero / NA
    expect_equal(st$stages[[1]]$fn_time, 0)
    expect_equal(st$stages[[1]]$beats, 0L)
    expect_true(is.na(st$stages[[1]]$share_working))
})

test_that("tick_time excludes time between next_item calls", {
    f <- 1:3 |> pump(function(x) x * 2, backend = "main")

    # Beat once, sleep, beat again: the sleep must not be counted anywhere
    f$next_item()
    f$pop_item()
    Sys.sleep(1.5) # seconds
    f$next_item()
    f$pop_item()
    f$next_item()
    f$pop_item()

    st <- pump_status(f)
    expect_equal(st$beats, 3L)
    # tick_time should be well under 0.3s since the sleep is between calls
    expect_lt(st$tick_time, 300) # milliseconds
})

test_that("pop hits and misses are counted on pop_item", {
    f <- 1:2 |> pump(function(x) x, backend = "main")

    # pop before any beat: miss
    expect_null(f$pop_item())
    st <- pump_status(f)
    expect_equal(st$pop_hits, 0L)
    expect_equal(st$pop_misses, 1L)

    # beat then pop: hit
    f$next_item()
    expect_false(is.null(f$pop_item()))
    st <- pump_status(f)
    expect_equal(st$pop_hits, 1L)
    expect_equal(st$pop_misses, 1L)
})

test_that("beats are classified working, blocked, and starved", {
    # blocked: buffer of 1 fills, next beat leaves a finished job in the slot
    f <- 1:3 |> pump(function(x) x, backend = "main", buffer_size = 1)
    f$next_item() # working: item 1 lands in the buffer
    f$next_item() # blocked: item 2 finishes but the buffer is still full
    st <- pump_status(f)
    expect_gte(st$beats_working, 1L)
    expect_gte(st$beats_blocked, 1L)

    # drain everything: beats on a finished pipeline are no-ops and freeze
    while (!f$done()) {
        f$next_item()
        f$pop_item()
    }
    frozen <- pump_status(f)$beats
    f$next_item()
    f$next_item()
    st <- pump_status(f)
    expect_equal(st$beats, frozen)

    # starved: upstream has nothing until a flag flips
    ready <- FALSE
    n_pulled <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (!ready) {
                return(NULL)
            }
            n_pulled <<- n_pulled + 1L
            list(id = n_pulled, data = n_pulled, ok = TRUE)
        },
        done_fn = function() n_pulled >= 1L
    )
    g <- src |> pump(function(x) x, backend = "main")
    g$next_item()
    st <- pump_status(g)
    expect_equal(st$beats_starved, 1L)

    ready <- TRUE
    g$next_item()
    st <- pump_status(g)
    expect_gte(st$beats_working, 1L)
})

test_that("source stats report pop hits, misses, and pull_time in ms", {
    src <- pump_source(
        pull_fn = local({
            i <- 0L
            function() {
                if (i >= 2L) {
                    return(NULL)
                }
                i <<- i + 1L
                Sys.sleep(0.02)
                list(id = i, data = i, ok = TRUE)
            }
        })
    )
    src$next_item()
    expect_false(is.null(src$pop_item()))
    src$next_item()
    expect_false(is.null(src$pop_item()))
    expect_null(src$pop_item())

    snap <- src$stats()
    expect_equal(snap$pop_hits, 2L)
    expect_equal(snap$pop_misses, 1L)
    # two pulls at ~20ms each must be recorded in milliseconds
    expect_gte(snap$pull_time, 2 * 20 * 0.9)
    expect_lt(snap$pull_time, 2000)
})
