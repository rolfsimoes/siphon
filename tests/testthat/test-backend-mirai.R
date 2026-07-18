test_that("mirai_backend operations work with local daemons", {
    skip_if_not_installed("mirai")
    library(siphon)

    # Start 2 local daemons for the duration of this test block
    mirai::daemons(2)
    on.exit(mirai::daemons(0), add = TRUE)

    # 1. mirai_backend jobs execute and return values
    bk <- mirai_backend()
    h <- siphon:::.pump_executor_register(bk, function(x) x^2, list())
    job <- siphon:::.pump_executor_new_job(bk, h, 5)
    expect_true(siphon:::.pump_job_is_ready(job) || {
        for (i in 1:50) {
            Sys.sleep(0.1)
            if (siphon:::.pump_job_is_ready(job)) break
        }
        siphon:::.pump_job_is_ready(job)
    })
    result <- siphon:::.pump_job_data(job)
    expect_equal(result$value, 25)
    expect_gte(result$fn_time, 0)

    # 2. mirai_backend executor count matches connections
    expect_equal(
        siphon:::.pump_executor_count(bk),
        mirai::status()$connections
    )

    # 3. mirai_backend pipeline scheduling preserves order
    f <- 1:20 |>
        pump(function(x) {
            Sys.sleep(0.01)
            x * 2
        }, backend = mirai_backend(), max_workers = 2) |>
        pump(function(x) x + 10, backend = mirai_backend(), max_workers = 2)

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list((1:20 * 2) + 10))

    # 4. mirai_backend two-stage ordering is preserved
    f <- 1:20 |>
        pump(function(x) x + 1, backend = mirai_backend(), max_workers = 2) |>
        pump(function(x) x * 3, backend = mirai_backend(), max_workers = 2)
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list((1:20 + 1) * 3))

    # 5. mirai_backend print output is correct
    output <- capture.output(print(bk))
    expected_workers <- mirai::status()$connections
    expect_equal(output, c("<pump_mirai_backend>", paste0("  workers: ", expected_workers)))

    # 6. mirai_backend processes returned error objects as data and correctly catches thrown errors
    f1 <- 1:20 |>
        pump(function(x) simpleError("returned"), backend = mirai_backend()) |>
        pump_run(verbose = FALSE, on_error = "collect")
    expect_s3_class(f1[[1]], "error")
    expect_equal(conditionMessage(f1[[1]]), "returned")

    f2 <- 1:20 |>
        pump(function(x) stop("thrown"), backend = mirai_backend()) |>
        pump_run(verbose = FALSE, on_error = "collect")
    expect_s3_class(f2[[1]], "error")
    expect_equal(conditionMessage(f2[[1]]), "thrown")

    # 7. mirai_backend works with pump_drain
    results <- list()
    f <- 1:20 |>
        pump(function(x) {
            Sys.sleep(0.01)
            x * 2
        }, backend = mirai_backend(), max_workers = 2)
    pump_drain(f, handle_fn = function(id, data, ok) {
        results[[id]] <<- data
    }, verbose = FALSE)
    expect_equal(results, as.list(1:20 * 2))
})

# Boundary contract: fault tolerance is delegated to mirai, but siphon must
# surface a dead daemon as a pump_error value instead of leaking a raw
# errorValue into the pipeline.
test_that("mirai_backend surfaces daemon death as a pump_error", {
    skip_if_not_installed("mirai")
    skip_on_cran()
    skip_on_os("windows")
    mirai::daemons(2)
    on.exit(mirai::daemons(0), add = TRUE)

    # adapter level
    bk <- mirai_backend()
    h <- siphon:::.pump_executor_register(
        bk,
        function(x) {
            tools::pskill(Sys.getpid())
            Sys.sleep(30)
        },
        list()
    )
    job <- siphon:::.pump_executor_new_job(bk, h, 1)
    for (i in 1:100) {
        if (siphon:::.pump_job_is_ready(job)) break
        Sys.sleep(0.1)
    }
    expect_true(siphon:::.pump_job_is_ready(job))
    result <- siphon:::.pump_job_data(job)
    expect_s3_class(result$value, "pump_error")
    expect_match(conditionMessage(result$value), "mirai worker failed")

    # pipeline level: the error is collectable, the pipeline does not abort
    out <- 1:1 |>
        pump(function(x) {
            tools::pskill(Sys.getpid())
            Sys.sleep(30)
        }, backend = mirai_backend()) |>
        pump_run(verbose = FALSE, on_error = "collect")
    # pump_run strips the internal pump_error class before returning
    expect_s3_class(out[[1]], "error")
    expect_match(conditionMessage(out[[1]]), "mirai worker failed")
})
