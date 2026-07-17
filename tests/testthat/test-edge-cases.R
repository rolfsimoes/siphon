test_that("backend with zero workers errors at stage creation", {
    zero_backend <- structure(list(), class = "pump_zero_backend")
    registerS3method(".pump_executor_count", "pump_zero_backend", function(x) 0L)

    expect_error(
        pump(1:3, identity, backend = zero_backend),
        "at least one process"
    )
})

test_that("function returning condition object is preserved", {
    cond <- simpleWarning("look out")
    f <- .pump_source_basic(1:2) |>
        pump(function(x) if (x == 1) cond else x, backend = "main")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out[[1]], cond)
    expect_equal(out[[2]], 2)
})

test_that("item-level error followed by additional stage", {
    e <- siphon:::.pump_error(simpleError("bad"))
    f <- .pump_source_basic(list(1, e, 3)) |>
        pump(function(x) x + 10, backend = "main", on_error = "collect") |>
        pump(function(x) x * 2, backend = "main", on_error = "collect")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out[[1]], 22)
    expect_s3_class(out[[2]], "error")
    expect_equal(conditionMessage(out[[2]]), "bad")
    expect_equal(out[[3]], 26)
})

test_that("progress accounting for one, two, and three stages", {
    s <- .pump_source_basic(1:2)
    expect_equal(s$progress(), 0)

    f1 <- pump(s, identity, backend = "main")
    expect_equal(f1$progress(), 0)

    f2 <- f1 |>
        pump(identity, backend = "main")
    f3 <- f2 |>
        pump(identity, backend = "main")

    expect_equal(f3$pipeline_length(), 8)

    f3$next_item()
    f3$pop_item()
    expect_true(f3$progress() > 0)
})

test_that("non-pump object passed to pump is auto-wrapped", {
    f <- pump(1:3, function(x) x + 1, backend = "main")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list(2:4))
})

test_that("chained pump calls are cumulative", {
    f <- 1:3 |>
        pump(function(x) x + 1, backend = "main") |>
        pump(function(x) x * 2, backend = "main")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list((1:3 + 1) * 2))
})
