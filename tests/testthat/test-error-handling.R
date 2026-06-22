test_that("on_error = collect preserves errors in output", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x, backend = "main")
    out <- pump_run(f, on_error = "collect")
    expect_equal(out[[1]], 1)
    expect_s3_class(out[[2]], "error")
    expect_equal(out[[3]], 3)
})

test_that("on_error = stop throws on first error", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x, backend = "main")
    expect_error(pump_run(f, on_error = "stop"), "boom")
})

test_that("on_error = continue drops error items", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x, backend = "main")
    out <- pump_run(f, on_error = "continue")
    expect_equal(out, list(1, 3))
})

test_that("stage-level on_error = continue drops upstream errors", {
    e <- siphon:::.pump_error(simpleError("bad"))
    f <- .pump_source_basic(list(1, e, 3)) |>
        pump(function(x) x + 10, backend = "main", on_error = "continue")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(11, 13))
})

test_that("stage-level on_error = continue drops stage errors", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x,
            backend = "main", on_error = "continue")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(1, 3))
})

test_that("on_error default is stop", {
    f <- 1:2 |>
        pump(function(x) if (x == 1) stop("boom") else x, backend = "main")
    expect_error(pump_run(f, verbose = FALSE), "boom")
})

test_that("continue preserves NULL return values", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else NULL,
            backend = "main", on_error = "continue")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(NULL, NULL))
})

test_that("legitimate error objects as data are processed by downstream functions", {
    e <- simpleError("as data")
    f <- list(e) |>
        pump(function(x) {
            if (inherits(x, "error")) {
                paste0("processed: ", conditionMessage(x))
            } else {
                "not an error"
            }
        }, backend = "main")
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list("processed: as data"))
})


# Three-stage integration tests -------------------------------------------------

.make_three_stage_pipeline <- function(p1, p2, p3, origin = 1L) {
    fragile <- function(x) {
        if (x == 99) stop("boom") else x
    }
    identity <- function(x) x

    fn1 <- if (origin == 1L) fragile else identity
    fn2 <- if (origin == 2L) fragile else identity
    fn3 <- if (origin == 3L) fragile else identity

    list(99) |>
        pump(fn1, backend = "main", on_error = p1) |>
        pump(fn2, backend = "main", on_error = p2) |>
        pump(fn3, backend = "main", on_error = p3)
}

.expected_three_stage <- function(origin, p1, p2, p3) {
    policies <- c(p1, p2, p3)
    for (i in origin:3) {
        if (policies[i] == "stop") return("throw")
        if (policies[i] == "continue") return("empty")
    }
    "error"
}

test_that("three-stage error handling follows the oracle table", {
    policies <- c("stop", "collect", "continue")
    for (origin in 1:3) {
        for (p1 in policies) {
            for (p2 in policies) {
                for (p3 in policies) {
                    f <- .make_three_stage_pipeline(p1, p2, p3, origin)
                    expected <- .expected_three_stage(origin, p1, p2, p3)
                    info <- sprintf(
                        "origin=%d, p1=%s, p2=%s, p3=%s, expected=%s",
                        origin, p1, p2, p3, expected
                    )
                    if (expected == "throw") {
                        expect_error(pump_run(f, verbose = FALSE), "boom", info = info)
                    } else if (expected == "empty") {
                        out <- pump_run(f, verbose = FALSE)
                        expect_equal(out, list(), info = info)
                    } else {
                        out <- pump_run(f, verbose = FALSE)
                        expect_equal(length(out), 1, info = info)
                        expect_true(inherits(out[[1]], "error"), info = info)
                    }
                }
            }
        }
    }
})

test_that("continue drops errors and removes them from results", {
    f <- 1:5 |>
        pump(function(x) if (x == 2) stop("drop2") else x,
             backend = "main", on_error = "continue") |>
        pump(function(x) if (x == 4) stop("drop4") else x,
             backend = "main", on_error = "continue") |>
        pump(function(x) x, backend = "main", on_error = "continue")

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(1, 3, 5))
})

test_that("continue at final stage drops after upstream collect", {
    f <- 1:5 |>
        pump(function(x) if (x == 2) stop("err2") else x,
             backend = "main", on_error = "collect") |>
        pump(function(x) x, backend = "main", on_error = "collect") |>
        pump(function(x) x, backend = "main", on_error = "continue")

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(1, 3, 4, 5))
})

test_that("continue at an intermediate stage shields downstream stop", {
    f <- list(99) |>
        pump(function(x) stop("err"), backend = "main", on_error = "continue") |>
        pump(function(x) x, backend = "main", on_error = "stop") |>
        pump(function(x) x, backend = "main", on_error = "continue")

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list())
})

test_that("intermediate continue with final collect drops the item", {
    f <- list(99) |>
        pump(function(x) stop("err"), backend = "main", on_error = "continue") |>
        pump(function(x) x, backend = "main", on_error = "collect") |>
        pump(function(x) x, backend = "main", on_error = "collect")

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list())
})


# Global-default inheritance tests ---------------------------------------------

test_that("global default stop halts pipeline on first error", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x, backend = "main") |>
        pump(function(x) x, backend = "main") |>
        pump(function(x) x, backend = "main")
    expect_error(pump_run(f, verbose = FALSE), "boom")
})

test_that("global default continue drops all errors", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x, backend = "main") |>
        pump(function(x) x, backend = "main") |>
        pump(function(x) x, backend = "main")
    out <- pump_run(f, on_error = "continue", verbose = FALSE)
    expect_equal(out, list(1, 3))
})

test_that("global default collect propagates all errors", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x, backend = "main") |>
        pump(function(x) x, backend = "main") |>
        pump(function(x) x, backend = "main")
    out <- pump_run(f, on_error = "collect", verbose = FALSE)
    expect_equal(out[[1]], 1)
    expect_s3_class(out[[2]], "error")
    expect_equal(out[[3]], 3)
})

test_that("explicit stage on_error overrides global default", {
    f <- 1:3 |>
        pump(function(x) if (x == 2) stop("boom") else x,
             backend = "main", on_error = "collect") |>
        pump(function(x) x, backend = "main", on_error = "continue") |>
        pump(function(x) x, backend = "main")
    # Global default is stop, but Stage 2 explicitly drops errors
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list(1, 3))
})

test_that("final stage explicit on_error overrides global default", {
    f <- list(99) |>
        pump(function(x) stop("err"), backend = "main", on_error = "collect") |>
        pump(function(x) x, backend = "main", on_error = "collect") |>
        pump(function(x) x, backend = "main", on_error = "continue")
    # Global default is stop, but Stage 3 explicitly drops the error
    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, list())
})
