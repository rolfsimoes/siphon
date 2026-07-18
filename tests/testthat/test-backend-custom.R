# A deterministic simulated-async backend: jobs complete after their token
# has been polled twice, capturing errors as condition objects.
fake_async_backend <- function(workers = 2L, register = NULL, open = NULL) {
    pump_custom_backend(
        name = "fakeasync",
        count = function() workers,
        submit = function(handle, data) {
            token <- new.env(parent = emptyenv())
            token$result <- tryCatch(
                do.call(handle$func, c(list(data), handle$args)),
                error = identity
            )
            token$polls <- 0L
            token
        },
        is_ready = function(token) {
            token$polls <- token$polls + 1L
            token$polls >= 2L
        },
        collect = function(token) token$result,
        register = register,
        open = open
    )
}

test_that("pump_custom_backend validates its arguments", {
    expect_error(
        pump_custom_backend(1, identity, identity, identity, identity),
        "single non-empty string"
    )
    expect_error(
        pump_custom_backend("x", 1, identity, identity, identity),
        "count must be a function"
    )
    expect_error(
        pump_custom_backend(
            "x", identity, identity, identity, identity,
            register = 1
        ),
        "register must be NULL or a function"
    )
    expect_error(
        pump_custom_backend(
            "x", identity, identity, identity, identity,
            open = 1
        ),
        "open must be NULL or a function"
    )
})

test_that("a custom backend is a pump_backend and prints its name", {
    bk <- fake_async_backend(3L)
    expect_s3_class(bk, "pump_backend")
    expect_equal(siphon:::.pump_executor_count(bk), 3L)
    output <- capture.output(print(bk))
    expect_equal(output, c("<pump_fakeasync_backend>", "  workers: 3"))
})

test_that("a custom backend runs a pipeline with results in order", {
    bk <- fake_async_backend(2L)
    out <- 1:6 |>
        pump(function(x) x * 2, backend = bk) |>
        pump_run(verbose = FALSE, sleep_ms = 1)
    expect_equal(out, as.list(1:6 * 2))
})

test_that("the backend name flows into pump_status", {
    bk <- fake_async_backend(2L)
    f <- 1:4 |> pump(function(x) x, backend = bk)
    st <- pump_status(f)
    expect_equal(st$stages[[1]]$type, "fakeasync")
})

test_that("constant stage arguments reach the default handle", {
    bk <- fake_async_backend(1L)
    out <- 1:3 |>
        pump(function(x, k) x + k, k = 10, backend = bk) |>
        pump_run(verbose = FALSE, sleep_ms = 1)
    expect_equal(out, list(11, 12, 13))
})

test_that("a custom register hook is called once per stage", {
    seen <- list()
    bk <- fake_async_backend(2L, register = function(func, args) {
        seen[[length(seen) + 1L]] <<- args
        list(func = func, args = args)
    })
    out <- 1:4 |>
        pump(function(x) x + 1, backend = bk) |>
        pump_run(verbose = FALSE, sleep_ms = 1)
    expect_equal(out, as.list(2:5))
    expect_length(seen, 1L)
})

test_that("open runs lazily and at most once", {
    opens <- 0L
    bk <- fake_async_backend(2L, open = function() opens <<- opens + 1L)
    expect_identical(opens, 0L) # construction does not open

    f <- 1:3 |> pump(function(x) x, backend = bk)
    expect_identical(opens, 0L) # neither does stage construction
    invisible(pump_run(f, verbose = FALSE, sleep_ms = 1))
    expect_identical(opens, 1L)

    g <- 1:3 |> pump(function(x) x, backend = bk)
    invisible(pump_run(g, verbose = FALSE, sleep_ms = 1))
    expect_identical(opens, 1L) # reuse does not reopen
})

test_that("a returned condition is an item failure under on_error", {
    bk <- fake_async_backend(2L)
    res <- 1:4 |>
        pump(function(x) if (x == 3) stop("bad item") else x, backend = bk) |>
        pump_run(verbose = FALSE, sleep_ms = 1, on_error = "collect")
    expect_s3_class(res[[3]], "error")
    expect_equal(res[[1]], 1)

    bk2 <- fake_async_backend(2L)
    f <- 1:4 |>
        pump(function(x) if (x == 3) stop("bad item") else x, backend = bk2)
    expect_error(pump_run(f, verbose = FALSE, sleep_ms = 1), "bad item")
})

test_that("an error thrown by collect aborts the pipeline", {
    bk <- pump_custom_backend(
        name = "broken",
        count = function() 1L,
        submit = function(handle, data) data,
        is_ready = function(token) TRUE,
        collect = function(token) stop("transport lost")
    )
    f <- 1:2 |> pump(function(x) x, backend = bk)
    expect_error(pump_run(f, verbose = FALSE), "transport lost")
})

test_that("max_workers is validated against the custom count", {
    bk <- fake_async_backend(2L)
    expect_error(
        1:10 |> pump(function(x) x, backend = bk, max_workers = 5),
        "exceeds executor count"
    )
})

test_that("job data is NULL until the job is ready", {
    bk <- fake_async_backend(1L)
    h <- siphon:::.pump_executor_register(bk, function(x) x + 1, list())
    job <- siphon:::.pump_executor_new_job(bk, h, 1)
    expect_null(siphon:::.pump_job_data(job)) # first poll: not ready
    expect_true(siphon:::.pump_job_is_ready(job)) # second poll: ready
    expect_equal(siphon:::.pump_job_data(job)$value, 2)
})
