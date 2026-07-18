test_that("backends carry the pump_backend parent class, name, and owned", {
    bk <- main_backend()
    expect_s3_class(bk, "pump_backend")
    expect_identical(bk$name, "main")
    expect_false(bk$owned)

    skip_if_not_installed("future")
    bk <- future_backend()
    expect_s3_class(bk, "pump_backend")
    expect_identical(bk$name, "future")
    expect_false(bk$owned)
})

test_that("print dispatches through print.pump_backend", {
    output <- capture.output(print(main_backend()))
    expect_equal(output, c("<pump_main_backend>", "  workers: 1"))
})

test_that("backend open and close default to no-ops returning the backend", {
    bk <- main_backend()
    expect_identical(siphon:::.pump_backend_open(bk), bk)
    expect_identical(siphon:::.pump_backend_close(bk), bk)
})

test_that(".pump_error is idempotent", {
    e <- siphon:::.pump_error(siphon:::.pump_error(simpleError("x")))
    expect_identical(sum(class(e) == "pump_error"), 1L)
})

test_that(".pump_job_failure wraps a condition into the job-result contract", {
    res <- siphon:::.pump_job_failure(simpleError("boom"))
    expect_named(res, c("value", "fn_time"))
    expect_s3_class(res$value, "pump_error")
    expect_identical(res$fn_time, 0)

    rewrapped <- siphon:::.pump_job_failure(res$value)
    expect_identical(sum(class(rewrapped$value) == "pump_error"), 1L)
})

test_that(".pump_backend_name reads the name field", {
    expect_identical(siphon:::.pump_backend_name(main_backend()), "main")
    expect_identical(siphon:::.pump_backend_name(list()), "unknown")
})
