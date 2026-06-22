test_that("pump_run collects in input order", {
    out <- 3:1 |>
        pump(function(x) x * 2, backend = "main") |>
        pump_run(verbose = FALSE)
    expect_equal(out, list(6, 4, 2))
})

test_that("pump_run auto-wraps non-pump inputs", {
    out <- pump_run(1:3, verbose = FALSE)
    expect_equal(out, as.list(1:3))
})

test_that("repeated pump_run on exhausted pump errors", {
    f <- 1:2 |>
        pump(function(x) x, backend = "main")
    out1 <- pump_run(f, verbose = FALSE)
    expect_equal(out1, as.list(1:2))
    expect_error(pump_run(f, verbose = FALSE), "exhausted")
})

test_that("pump_run with single item works", {
    out <- pump_run(list(42), verbose = FALSE)
    expect_equal(out, list(42))
})

test_that("pump_drain drains items to callback", {
    results <- list()
    f <- 1:5 |> pump(function(x) x * 2, backend = "main")
    pump_drain(f, handle_fn = function(id, data, ok) {
        results[[id]] <<- data
    })
    expect_equal(results, list(2, 4, 6, 8, 10))
})

test_that("pump_run handles infinite length sources by dynamically expanding", {
    # Custom source with Inf length but done_fn that finishes
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 4L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 3, ok = TRUE)
        },
        done_fn = function() i >= 4L,
        length = Inf
    )
    f <- src |> pump(function(x) x + 1, backend = "main")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(4, 7, 10, 13))
})

