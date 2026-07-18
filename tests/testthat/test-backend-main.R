test_that("main_backend executes in current process", {
    bk <- main_backend()
    h <- siphon:::.pump_executor_register(bk, function(x) Sys.getpid(), list())
    job <- siphon:::.pump_executor_new_job(bk, h, 1)
    result <- siphon:::.pump_job_data(job)
    expect_equal(result$value, Sys.getpid())
    expect_gte(result$fn_time, 0)
})

test_that("main_backend returns correct values", {
    bk <- main_backend()
    h <- siphon:::.pump_executor_register(bk, function(x) x * 2, list())
    job <- siphon:::.pump_executor_new_job(bk, h, 7)
    result <- siphon:::.pump_job_data(job)
    expect_equal(result$value, 14)
    expect_gte(result$fn_time, 0)
})

test_that("main_backend passes registered constant args after the data", {
    bk <- main_backend()
    h <- siphon:::.pump_executor_register(
        bk, function(x, k) x + k, list(k = 10)
    )
    job <- siphon:::.pump_executor_new_job(bk, h, 5)
    expect_equal(siphon:::.pump_job_data(job)$value, 15)
})

test_that("main_backend captures errors without throwing", {
    bk <- main_backend()
    h <- siphon:::.pump_executor_register(bk, function(x) stop("boom"), list())
    job <- siphon:::.pump_executor_new_job(bk, h, 1)
    result <- siphon:::.pump_job_data(job)
    expect_s3_class(result$value, "error")
    expect_gte(result$fn_time, 0)
})

test_that("main_backend job is always ready", {
    bk <- main_backend()
    h <- siphon:::.pump_executor_register(bk, identity, list())
    job <- siphon:::.pump_executor_new_job(bk, h, 1)
    expect_true(siphon:::.pump_job_is_ready(job))
})

test_that("main_backend has executor count 1", {
    bk <- main_backend()
    expect_equal(siphon:::.pump_executor_count(bk), 1)
})

test_that("main_backend print output is correct", {
    bk <- main_backend()
    output <- capture.output(print(bk))
    expect_equal(output, c("<pump_main_backend>", "  workers: 1"))
})
