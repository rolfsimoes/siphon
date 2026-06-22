test_that("downstream stage starts before upstream finishes (main backend)", {
    # Even though main backend is synchronous/single-threaded (forcing max_workers = 1),
    # the pull-based nature of the pipeline still interleaves processing (item 1 flows
    # to the end before item 2 is pulled from source).
    f <- 1:6 |>
        pump(function(x) {
            Sys.sleep(0.01)
            x
        }, backend = "main", max_workers = 2) |>
        pump(function(x) {
            x * 10
        }, backend = "main", max_workers = 2)

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list(1:6 * 10))
})

test_that("cpu -> main-thread -> io style pipeline works (main backend)", {
    f <- 1:4 |>
        pump(function(x) {
            Sys.sleep(0.01)
            x + 1
        }, backend = "main", max_workers = 2) |>
        pump(function(x) {
            Sys.sleep(0.01)
            x * 3
        }, backend = "main", max_workers = 1) |>
        pump(function(x) {
            Sys.sleep(0.01)
            paste0("out:", x)
        }, backend = "main", max_workers = 2)

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list(paste0("out:", (1:4 + 1) * 3)))
})

test_that("uneven job durations do not deadlock (main backend)", {
    f <- 1:4 |>
        pump(function(x) {
            if (x %% 2 == 0) Sys.sleep(0.02) else Sys.sleep(0.005)
            x
        }, backend = "main", max_workers = 2) |>
        pump(function(x) x + 100, backend = "main", max_workers = 2)

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list(1:4 + 100))
})

test_that("concurrency, backpressure, and stage propagation are timing-validated (future backend)", {
    skip_if_not_installed("future")
    old_plan <- future::plan("multisession", workers = 3)
    on.exit(future::plan(old_plan), add = TRUE)

    stage1_fn <- function(x) {
        start <- Sys.time()
        Sys.sleep(0.2)
        end <- Sys.time()
        list(x = x, stage1_start = start, stage1_end = end)
    }

    stage2_fn <- function(res) {
        start <- Sys.time()
        Sys.sleep(0.5)
        end <- Sys.time()
        res$stage2_start <- start
        res$stage2_end <- end
        res
    }

    f <- 1:3 |>
        pump(stage1_fn, backend = future_backend(), max_workers = 1, buffer_size = 1) |>
        pump(stage2_fn, backend = future_backend(), max_workers = 1, buffer_size = 1)

    out <- pump_run(f, verbose = FALSE)

    s1_start <- sapply(out, function(o) as.numeric(o$stage1_start))
    s1_end <- sapply(out, function(o) as.numeric(o$stage1_end))
    s2_start <- sapply(out, function(o) as.numeric(o$stage2_start))
    s2_end <- sapply(out, function(o) as.numeric(o$stage2_end))

    # 1. Propagation: Downstream starts before upstream finishes all items.
    # Stage 2 starts item 1 before Stage 1 ends item 3.
    expect_true(s2_start[1] < s1_end[3])

    # 2. Concurrency: Stage 2 processing item 1 overlaps with Stage 1 processing item 2.
    # i.e., s2_start[1] < s1_end[2] and s1_start[2] < s2_end[1]
    expect_true(s2_start[1] < s1_end[2])
    expect_true(s1_start[2] < s2_end[1])

    # 3. Backpressure: Stage 1 is delayed from starting item 3 until Stage 2 finishes item 1.
    # Without backpressure, Stage 1 would start item 3 right after ending item 2 (around t = 0.40s).
    # With backpressure, Stage 1 starts item 3 around the time Stage 2 finishes item 1 (around t = 0.70s).
    # Allow small tolerance/latency (e.g., 0.05s) for scheduling overhead.
    expect_true(s1_start[3] >= (s2_end[1] - 0.05))
    expect_true((s1_start[3] - s1_end[2]) > 0.1)
})

test_that("concurrency, backpressure, and stage propagation are timing-validated (mirai backend)", {
    skip_if_not_installed("mirai")
    mirai::daemons(2)
    on.exit(mirai::daemons(0), add = TRUE)

    stage1_fn <- function(x) {
        start <- Sys.time()
        Sys.sleep(0.2)
        end <- Sys.time()
        list(x = x, stage1_start = start, stage1_end = end)
    }

    stage2_fn <- function(res) {
        start <- Sys.time()
        Sys.sleep(0.5)
        end <- Sys.time()
        res$stage2_start <- start
        res$stage2_end <- end
        res
    }

    f <- 1:3 |>
        pump(stage1_fn, backend = mirai_backend(), max_workers = 1, buffer_size = 1) |>
        pump(stage2_fn, backend = mirai_backend(), max_workers = 1, buffer_size = 1)

    out <- pump_run(f, verbose = FALSE)

    s1_start <- sapply(out, function(o) as.numeric(o$stage1_start))
    s1_end <- sapply(out, function(o) as.numeric(o$stage1_end))
    s2_start <- sapply(out, function(o) as.numeric(o$stage2_start))
    s2_end <- sapply(out, function(o) as.numeric(o$stage2_end))

    # 1. Propagation: Downstream starts before upstream finishes all items.
    expect_true(s2_start[1] < s1_end[3])

    # 2. Concurrency: Stage 2 processing item 1 overlaps with Stage 1 processing item 2.
    expect_true(s2_start[1] < s1_end[2])
    expect_true(s1_start[2] < s2_end[1])

    # 3. Backpressure: Stage 1 is delayed from starting item 3 until Stage 2 finishes item 1.
    expect_true(s1_start[3] >= (s2_end[1] - 0.05))
    expect_true((s1_start[3] - s1_end[2]) > 0.1)
})
