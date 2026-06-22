test_that("future_backend jobs execute under sequential plan", {
    skip_if_not_installed("future")
    library(siphon)
    old_plan <- future::plan("sequential")
    on.exit(future::plan(old_plan), add = TRUE)

    bk <- future_backend()
    job <- siphon:::.pump_executor_new_job(bk, function(x) x^2, list(6))
    expect_true(siphon:::.pump_job_is_ready(job))
    result <- siphon:::.pump_job_data(job)
    expect_equal(result$value, 36)
    expect_gte(result$fn_time, 0)
})

test_that("future_backend jobs execute under multisession plan", {
    skip_if_not_installed("future")
    old_plan <- future::plan("multisession", workers = 2)
    on.exit(future::plan(old_plan), add = TRUE)

    bk <- future_backend()
    job <- siphon:::.pump_executor_new_job(bk, function(x) x + 1, list(9))
    for (i in 1:50) {
        Sys.sleep(0.1)
        if (siphon:::.pump_job_is_ready(job)) break
    }
    expect_true(siphon:::.pump_job_is_ready(job))
    result <- siphon:::.pump_job_data(job)
    expect_equal(result$value, 10)
    expect_gte(result$fn_time, 0)
})

test_that("future_backend executor count matches nbrOfWorkers", {
    skip_if_not_installed("future")
    old_plan <- future::plan("sequential")
    on.exit(future::plan(old_plan), add = TRUE)

    bk <- future_backend()
    expect_equal(siphon:::.pump_executor_count(bk), future::nbrOfWorkers())
})

test_that("future_backend pipeline scheduling preserves order", {
    skip_if_not_installed("future")
    old_plan <- future::plan("multisession", workers = 2)
    on.exit(future::plan(old_plan), add = TRUE)

    f <- 1:20 |>
        pump(function(x) {
            Sys.sleep(0.01)
            x * 2
        }, backend = future_backend(), max_workers = 2) |>
        pump(function(x) x + 10, backend = future_backend(), max_workers = 2)

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list((1:20 * 2) + 10))
})

test_that("future_backend two-stage ordering is preserved", {
    skip_if_not_installed("future")
    old_plan <- future::plan("multisession", workers = 2)
    on.exit(future::plan(old_plan), add = TRUE)

    f <- 1:20 |>
        pump(function(x) x + 1, backend = future_backend(), max_workers = 2) |>
        pump(function(x) x * 3, backend = future_backend(), max_workers = 2)
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list((1:20 + 1) * 3))
})

test_that("future_backend print output is correct", {
    skip_if_not_installed("future")
    old_plan <- future::plan("sequential")
    on.exit(future::plan(old_plan), add = TRUE)

    bk <- future_backend()
    output <- capture.output(print(bk))
    expect_equal(output, c("<pump_future_backend>", "  workers: 1"))
})

test_that("future_backend processes returned error objects as data and correctly catches thrown errors", {
    skip_if_not_installed("future")
    old_plan <- future::plan("sequential")
    on.exit(future::plan(old_plan), add = TRUE)

    # 1. Check returned error object
    f1 <- 1:1 |>
        pump(function(x) simpleError("returned"), backend = future_backend()) |>
        pump_run(verbose = FALSE, on_error = "collect")
    expect_s3_class(f1[[1]], "error")
    expect_equal(conditionMessage(f1[[1]]), "returned")

    # 2. Check thrown error
    f2 <- 1:1 |>
        pump(function(x) stop("thrown"), backend = future_backend()) |>
        pump_run(verbose = FALSE, on_error = "collect")
    expect_s3_class(f2[[1]], "error")
    expect_equal(conditionMessage(f2[[1]]), "thrown")
})

test_that("future_backend works with pump_drain", {
    skip_if_not_installed("future")
    old_plan <- future::plan("multisession", workers = 2)
    on.exit(future::plan(old_plan), add = TRUE)

    results <- list()
    f <- 1:20 |>
        pump(function(x) {
            Sys.sleep(0.01)
            x * 2
        }, backend = future_backend(), max_workers = 2)
    pump_drain(f, handle_fn = function(id, data, ok) {
        results[[id]] <<- data
    }, verbose = FALSE)
    expect_equal(results, as.list(1:20 * 2))
})
