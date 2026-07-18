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

test_that("pump_run backend default is inherited by pump stages", {
    # When backend is set in pump_run, pump() without explicit backend should use it
    out <- 1:3 |>
        pump(function(x) x * 2) |>
        pump_run(backend = "main", verbose = FALSE)
    expect_equal(out, list(2, 4, 6))
})

test_that("pump() explicit backend overrides pump_run default", {
    # Explicit backend in pump() should override pump_run default
    out <- 1:3 |>
        pump(function(x) x * 2, backend = "main") |>
        pump_run(backend = "main", verbose = FALSE)
    expect_equal(out, list(2, 4, 6))
})

test_that("pump_drain backend default is inherited by pump stages", {
    # When backend is set in pump_drain, pump() without explicit backend should use it
    results <- list()
    f <- 1:3 |> pump(function(x) x * 2)
    pump_drain(f, handle_fn = function(id, data, ok) {
        results[[id]] <<- data
    }, backend = "main")
    expect_equal(results, list(2, 4, 6))
})

test_that("pump_drain on an exhausted pipeline is a silent no-op", {
    f <- 1:2 |> pump(function(x) x, backend = "main")
    pump_run(f, verbose = FALSE)
    calls <- 0L
    expect_no_error(
        pump_drain(f, handle_fn = function(id, data, ok) calls <<- calls + 1L)
    )
    expect_identical(calls, 0L)
})

test_that("pump_drain honors timeout", {
    src <- pump_source(pull_fn = function() NULL) # never ready, never done
    f <- src |> pump(function(x) x, backend = "main")
    expect_error(
        pump_drain(
            f,
            handle_fn = function(id, data, ok) NULL,
            sleep_ms = 1,
            timeout = 0.2
        ),
        "timeout"
    )
})

test_that("pump_drain on_error = 'collect' delivers failures to handle_fn", {
    oks <- list()
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x, backend = "main")
    pump_drain(f, handle_fn = function(id, data, ok) {
        oks[[id]] <<- ok
    }, on_error = "collect")
    expect_equal(oks, list(TRUE, FALSE, TRUE))
})

test_that("pump_drain on_error = 'stop' (default) throws on first error", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x, backend = "main")
    expect_error(
        pump_drain(f, handle_fn = function(id, data, ok) NULL),
        "boom"
    )
})
