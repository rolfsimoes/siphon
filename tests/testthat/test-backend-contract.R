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

test_that("stage keys are unique and pid-qualified", {
    k1 <- siphon:::.pump_stage_key()
    k2 <- siphon:::.pump_stage_key()
    expect_false(identical(k1, k2))
    expect_match(k1, sprintf("^\\.siphon_stage_%d_", Sys.getpid()))
})

test_that("future backend auto-detects globals used by fn", {
    skip_if_not_installed("future")
    old_plan <- future::plan("multisession", workers = 2)
    on.exit(future::plan(old_plan), add = TRUE)

    # a top-level function depending on a top-level object: neither travels
    # unless future's automatic globals detection is active (it is disabled
    # by any explicit `globals =` argument to future())
    assign(".siphon_test_dep", 7, envir = globalenv())
    on.exit(rm(".siphon_test_dep", envir = globalenv()), add = TRUE)
    fn <- eval(
        parse(text = "function(x) x + .siphon_test_dep"),
        envir = globalenv()
    )

    f <- as.list(1:3) |> pump(fn, backend = future_backend())
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(8, 9, 10))
})
