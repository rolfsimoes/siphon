test_that("mirai_backend operations work with local daemons", {
    skip_if_not_installed("mirai")

    # Start 2 local daemons for the duration of this test block
    mirai::daemons(2)
    on.exit(mirai::daemons(0), add = TRUE)

    # 1. mirai_backend jobs execute and return values
    bk <- mirai_backend()
    job <- siphon:::.pump_executor_new_job(bk, function(x) x^2, list(5))
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
    f <- 1:5 |>
        pump(function(x) {
            Sys.sleep(0.01)
            x * 2
        }, backend = mirai_backend(), max_workers = 2) |>
        pump(function(x) x + 10, backend = mirai_backend(), max_workers = 2)

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list((1:5 * 2) + 10))

    # 4. mirai_backend two-stage ordering is preserved
    f <- 1:4 |>
        pump(function(x) x + 1, backend = mirai_backend(), max_workers = 2) |>
        pump(function(x) x * 3, backend = mirai_backend(), max_workers = 2)
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list((1:4 + 1) * 3))

    # 5. mirai_backend print output is correct
    output <- capture.output(print(bk))
    expected_workers <- mirai::status()$connections
    expect_equal(output, c("<pump_mirai_backend>", paste0("  workers: ", expected_workers)))

    # 6. mirai_backend processes returned error objects as data and correctly catches thrown errors
    f1 <- 1:1 |>
        pump(function(x) simpleError("returned"), backend = mirai_backend()) |>
        pump_run(verbose = FALSE, on_error = "collect")
    expect_s3_class(f1[[1]], "error")
    expect_equal(conditionMessage(f1[[1]]), "returned")

    f2 <- 1:1 |>
        pump(function(x) stop("thrown"), backend = mirai_backend()) |>
        pump_run(verbose = FALSE, on_error = "collect")
    expect_s3_class(f2[[1]], "error")
    expect_equal(conditionMessage(f2[[1]]), "thrown")
})
