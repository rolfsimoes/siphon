test_that("pump_step advances work without consuming", {
    p <- 1:4 |> pump(function(x) x * 2, backend = "main")

    pump_step(p)
    st <- pump_status(p)
    expect_equal(st$completed, 1)
    expect_equal(st$buffer_size, 1)

    pump_step(p, 2)
    st <- pump_status(p)
    expect_equal(st$completed, 3)
    expect_equal(st$buffer_size, 3)
})

test_that("pump_peek is repeatable and does not consume", {
    p <- 1:4 |> pump(function(x) x * 2, backend = "main")
    expect_equal(pump_peek(p), list())

    pump_step(p, 2)
    a <- pump_peek(p)
    b <- pump_peek(p)
    expect_equal(a, b)
    expect_length(a, 1)
    expect_equal(a[[1]]$data, 2)

    both <- pump_peek(p, 2)
    expect_length(both, 2)
    expect_equal(both[[2]]$data, 4)

    # buffer untouched
    expect_equal(pump_status(p)$buffer_size, 2)
})

test_that("pump_pop consumes exactly the peeked item", {
    p <- 1:3 |> pump(function(x) x * 2, backend = "main")
    expect_null(pump_pop(p))

    pump_step(p)
    peeked <- pump_peek(p)[[1]]
    popped <- pump_pop(p)
    expect_equal(popped, peeked)
    expect_equal(pump_status(p)$buffer_size, 0)
})

test_that("stepping then pump_run resumes with the full result", {
    p <- 1:6 |>
        pump(function(x) x + 1, backend = "main") |>
        pump(function(x) x * 10, backend = "main")

    pump_step(p, 3)
    res <- pump_run(p, verbose = FALSE)
    # nothing was consumed, so everything comes out in input order
    expect_equal(res, as.list((2:7) * 10))
})

test_that("popped items are owned by the caller and excluded from pump_run", {
    p <- 1:6 |>
        pump(function(x) x + 1, backend = "main") |>
        pump(function(x) x * 10, backend = "main")

    pump_step(p, 3)
    v <- pump_pop(p)
    expect_equal(v$data, 20)

    res <- pump_run(p, verbose = FALSE)
    expect_equal(res, as.list((3:7) * 10))
})

test_that("pump_step does not inflate beats on an exhausted pipeline", {
    p <- 1:3 |> pump(function(x) x, backend = "main")
    pump_step(p, 3) # everything processed, results wait in the buffer

    before <- pump_status(p)$beats
    pump_step(p, 50)
    pump_step(p, 50)
    expect_equal(pump_status(p)$beats, before)

    # results are still all there
    res <- pump_run(p, verbose = FALSE)
    expect_equal(res, as.list(1:3))
})

test_that("pump_step stops beating a fully stuck (blocked) pipeline", {
    p <- 1:5 |> pump(function(x) x, backend = "main", buffer_size = 1)
    pump_step(p, 50) # buffer of 1 fills, then a job blocks in the slot

    st <- pump_status(p)
    # a single beat recorded the blocked signal, then stepping stopped
    expect_lte(st$beats, 3L)
    expect_gte(st$beats_blocked, 1L)

    # consuming unblocks it and everything still comes out
    res <- pump_run(p, verbose = FALSE)
    expect_equal(res, as.list(1:5))
})

test_that("pump_peek on a bare source returns an empty list", {
    src <- siphon:::.pump_source_basic(1:3)
    expect_equal(pump_peek(src), list())
})

test_that("inspection verbs validate their inputs", {
    expect_error(pump_step(42), "pump object")
    expect_error(pump_peek(42), "pump object")
    expect_error(pump_pop(42), "pump object")

    p <- 1:3 |> pump(function(x) x, backend = "main")
    expect_error(pump_step(p, 0), "positive")
    expect_error(pump_step(p, NA), "positive")
})
