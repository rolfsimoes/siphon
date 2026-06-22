test_that("single stage ordering is preserved", {
    f <- 1:5 |>
        pump(function(x) x * 2, backend = "main", max_workers = 2)
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list(1:5 * 2))
})

test_that("two-stage ordering is preserved", {
    f <- 1:4 |>
        pump(function(x) x + 1, backend = "main", max_workers = 2) |>
        pump(function(x) x * 3, backend = "main", max_workers = 2)
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list((1:4 + 1) * 3))
})

test_that("failed upstream items skip fn and propagate ok=FALSE", {
    e <- siphon:::.pump_error(simpleError("bad"))
    f <- .pump_source_basic(list(1, e, 3)) |>
        pump(function(x) x * 10, backend = "main", max_workers = 2, on_error = "collect")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out[[1]], 10)
    expect_s3_class(out[[2]], "error")
    expect_equal(conditionMessage(out[[2]]), "bad")
    expect_equal(out[[3]], 30)
})

test_that("max_workers larger than input works", {
    f <- 1:3 |>
        pump(function(x) x, backend = "main", max_workers = 10)
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list(1:3))
})

test_that("max_workers = 1 works", {
    f <- 1:3 |>
        pump(function(x) x + 1, backend = "main", max_workers = 1)
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list(2:4))
})

test_that("zero-length input works", {
    f <- list() |>
        pump(function(x) x, backend = "main", max_workers = 2)
    expect_equal(pump_run(f, verbose = FALSE), list())
})

test_that("functions returning NULL are handled", {
    f <- 1:3 |>
        pump(function(x) NULL, backend = "main", max_workers = 2)
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(NULL, NULL, NULL))
})

test_that("small input suppresses progress bar", {
    f <- list(1) |>
        pump(function(x) x * 2, backend = "main", max_workers = 2)
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(2))
})
