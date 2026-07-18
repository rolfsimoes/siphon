# --- Unit tests (no cluster) ---

# Builds a backend-shaped object without spawning workers, to exercise
# guard-rail branches in isolation.
fake_parallel_backend <- function(cl, busy, workers = NULL) {
    state <- new.env(parent = emptyenv())
    state$cl <- cl
    state$busy <- busy
    state$workers <- workers
    structure(list(state = state), class = "pump_parallel_backend")
}

test_that(".parallel_expr_inject substitutes {{ }} values from the caller env", {
    env <- list2env(list(y = 5))

    # note: computed outside expect_equal() because testthat applies rlang
    # quasiquotation, which would capture {{ y }} itself
    simple <- siphon:::.parallel_expr_inject(quote(x <- {{ y }}), env)
    expect_equal(simple, quote(x <- 5))

    # injection nested inside a larger call
    nested <- siphon:::.parallel_expr_inject(quote(f(a, list({{ y }}))), env)
    expect_equal(nested, quote(f(a, list(5))))
})

test_that(".parallel_expr_inject leaves non-injection expressions unchanged", {
    env <- list2env(list(y = 5))

    # plain symbol reference is not injected
    expect_equal(
        siphon:::.parallel_expr_inject(quote(x <- y), env),
        quote(x <- y)
    )

    # single braces are not an injection marker
    expect_equal(
        siphon:::.parallel_expr_inject(quote(x <- { y }), env),
        quote(x <- { y })
    )

    # non-call inputs pass through
    expect_equal(siphon:::.parallel_expr_inject(quote(y), env), quote(y))
    expect_equal(siphon:::.parallel_expr_inject(5, env), 5)
})

test_that("register installs a per-stage runner shipped once", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    n_setup <- length(bk$state$setup_exprs)
    h <- siphon:::.pump_executor_register(bk, function(x) x + 1, list())
    expect_true(is.character(h$key) && nzchar(h$key))

    # recorded for replay on replacement nodes
    expect_length(bk$state$setup_exprs, n_setup + 1L)

    # the runner is present in every worker's global environment
    installed <- parallel_eval_workers(
        bk, exists({{ h$key }}, envir = globalenv())
    )
    expect_true(all(unlist(installed)))

    # jobs reference the runner by key and carry only the item data
    job <- siphon:::.pump_executor_new_job(bk, h, 41)
    expect_identical(job$state$key, h$key)
    expect_identical(job$state$data, 41)
    expect_null(job$state$fn)
    for (i in 1:50) {
        if (siphon:::.pump_job_is_ready(job)) break
        Sys.sleep(0.1)
    }
    expect_equal(siphon:::.pump_job_data(job)$value, 42)
})

test_that(".parallel_worker_spec resolves numeric and hostname specs", {
    bk_num <- fake_parallel_backend(cl = list("n1"), busy = FALSE, workers = 4)
    expect_identical(siphon:::.parallel_worker_spec(bk_num, 3L), 1L)

    bk_hosts <- fake_parallel_backend(
        cl = list("n1", "n2"),
        busy = c(FALSE, FALSE),
        workers = c("hostA", "hostB")
    )
    expect_identical(siphon:::.parallel_worker_spec(bk_hosts, 2L), "hostB")
})

test_that("parallel_setup_workers guards stopped and busy backends", {
    bk_stopped <- fake_parallel_backend(cl = NULL, busy = logical())
    expect_error(
        parallel_setup_workers(bk_stopped, quote(1)),
        "has no workers"
    )

    bk_busy <- fake_parallel_backend(cl = list("n1"), busy = TRUE)
    expect_error(
        parallel_setup_workers(bk_busy, quote(1)),
        "Cannot set up workers while jobs are active"
    )
})

test_that("parallel_stop refuses to stop a busy backend without force", {
    bk_busy <- fake_parallel_backend(cl = list("n1"), busy = TRUE)
    expect_error(
        parallel_stop(bk_busy),
        "Cannot stop a parallel backend with active jobs"
    )
})

test_that(".parallel_submit_job enforces the free-node invariant", {
    bk_busy <- fake_parallel_backend(cl = list("n1"), busy = TRUE)
    expect_error(
        siphon:::.parallel_submit_job(bk_busy, identity, list(1)),
        "no free cluster node"
    )
})

# --- Integration tests (real cluster) ---

test_that("parallel_backend requires a workers specification", {
    skip_if_not_installed("parallel")
    expect_error(parallel_backend(), "argument \"workers\" is missing")
})

test_that("parallel_backend validates retries and retry_sleep", {
    skip_if_not_installed("parallel")
    expect_error(
        parallel_backend(1, retries = -1L),
        "retries must be a non-negative integer"
    )
    expect_error(
        parallel_backend(1, retry_sleep = -1),
        "retry_sleep must be a non-negative number"
    )
})

test_that("parallel_backend single job round-trip", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    h <- siphon:::.pump_executor_register(bk, function(x) x^2, list())
    job <- siphon:::.pump_executor_new_job(bk, h, 6)
    for (i in 1:50) {
        if (siphon:::.pump_job_is_ready(job)) break
        Sys.sleep(0.1)
    }
    expect_true(siphon:::.pump_job_is_ready(job))
    result <- siphon:::.pump_job_data(job)
    expect_equal(result$value, 36)
    expect_gte(result$fn_time, 0)
})

test_that("parallel_backend executor count matches worker count", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    expect_equal(siphon:::.pump_executor_count(bk), 2L)
})

test_that("parallel_backend frees node after job completes", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    h <- siphon:::.pump_executor_register(bk, function(x) x + 1, list())
    job1 <- siphon:::.pump_executor_new_job(bk, h, 1)
    job2 <- siphon:::.pump_executor_new_job(bk, h, 2)
    expect_equal(sum(bk$state$busy), 2L)
    for (i in 1:50) {
        r1 <- siphon:::.pump_job_is_ready(job1)
        r2 <- siphon:::.pump_job_is_ready(job2)
        if (r1 && r2) break
        Sys.sleep(0.1)
    }
    expect_equal(sum(bk$state$busy), 0L)
    expect_equal(siphon:::.pump_job_data(job1)$value, 2)
    expect_equal(siphon:::.pump_job_data(job2)$value, 3)
})

test_that("parallel_backend pipeline scheduling preserves order", {
    skip_if_not_installed("parallel")
    bk1 <- parallel_backend(2)
    on.exit(parallel_stop(bk1, force = TRUE), add = TRUE)
    bk2 <- parallel_backend(2)
    on.exit(parallel_stop(bk2, force = TRUE), add = TRUE)

    f <- 1:20 |>
        pump(function(x) {
            Sys.sleep(0.01)
            x * 2
        }, backend = bk1, max_workers = 2) |>
        pump(function(x) x + 10, backend = bk2, max_workers = 2)

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list((1:20 * 2) + 10))
})

test_that("pump() errors when max_workers exceeds worker count", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    expect_error(
        1:20 |>
            pump(function(x) x, backend = bk, max_workers = 3),
        "exceeds executor count"
    )
})

test_that("parallel_backend print output is correct", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    output <- capture.output(print(bk))
    expect_equal(output, c("<pump_parallel_backend>", "  workers: 2"))
})

test_that("parallel_backend processes returned error objects as data and correctly catches thrown errors", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    # 1. Check returned error object
    f1 <- 1:1 |>
        pump(function(x) simpleError("returned"), backend = bk) |>
        pump_run(verbose = FALSE, on_error = "collect")
    expect_s3_class(f1[[1]], "error")
    expect_equal(conditionMessage(f1[[1]]), "returned")

    # 2. Check thrown error
    f2 <- 1:1 |>
        pump(function(x) stop("thrown"), backend = bk) |>
        pump_run(verbose = FALSE, on_error = "collect")
    expect_s3_class(f2[[1]], "error")
    expect_equal(conditionMessage(f2[[1]]), "thrown")
})

test_that("parallel_backend works with pump_drain", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    results <- list()
    f <- 1:20 |>
        pump(function(x) {
            Sys.sleep(0.01)
            x * 2
        }, backend = bk, max_workers = 2)
    pump_drain(f, handle_fn = function(id, data, ok) {
        results[[id]] <<- data
    }, verbose = FALSE)
    expect_equal(results, as.list(1:20 * 2))
})

test_that("parallel_setup_workers reports worker-side setup failures", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(1)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    expect_error(
        parallel_setup_workers(bk, stop("boom")),
        "Failed to set up parallel worker"
    )
})

test_that("parallel_setup_workers runs setup expressions on all nodes", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    parallel_setup_workers(bk, assign(".setup_value", 42, envir = globalenv()))

    f <- 1:2 |>
        pump(function(x) get(".setup_value", envir = globalenv()), backend = bk) |>
        pump_run(verbose = FALSE)
    expect_equal(f, list(42, 42))

    expect_error(
        parallel_setup_workers(list(), quote(1)),
        "backend must be a parallel backend"
    )
})

test_that("parallel_eval_workers guards stopped and busy backends", {
    expect_error(
        parallel_eval_workers(list(), quote(1)),
        "backend must be a parallel backend"
    )

    bk_stopped <- fake_parallel_backend(cl = NULL, busy = logical())
    expect_error(
        parallel_eval_workers(bk_stopped, quote(1)),
        "has no workers"
    )

    bk_busy <- fake_parallel_backend(cl = list("n1"), busy = TRUE)
    expect_error(
        parallel_eval_workers(bk_busy, quote(1)),
        "Cannot evaluate on workers while jobs are active"
    )
})

test_that("parallel_eval_workers returns one result per worker in order", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    pids <- parallel_eval_workers(bk, Sys.getpid())
    expect_length(pids, 2L)
    expect_false(pids[[1]] == pids[[2]])
    expect_false(any(unlist(pids) == Sys.getpid()))

    # evaluation happens in the workers' global environment
    parallel_eval_workers(bk, assign(".eval_value", 7, envir = globalenv()))
    got <- parallel_eval_workers(bk, get(".eval_value", envir = globalenv()))
    expect_equal(got, list(7, 7))
})

test_that("parallel_eval_workers injects {{ }} values from the caller env", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    # note: computed outside expect_equal() because testthat applies rlang
    # quasiquotation, which would capture {{ offset }} itself
    offset <- 40
    injected <- parallel_eval_workers(bk, 2 + {{ offset }})
    expect_equal(injected, list(42, 42))
})

test_that("parallel_eval_workers reports worker-side failures with node ids", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    expect_error(
        parallel_eval_workers(bk, stop("boom")),
        "Error evaluating expression on worker\\(s\\) 1, 2"
    )

    # a failure does not poison the backend for later use
    expect_equal(parallel_eval_workers(bk, 1 + 1), list(2, 2))
})

test_that("parallel_eval_workers broadcasts instead of visiting nodes serially", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    t0 <- Sys.time()
    parallel_eval_workers(bk, Sys.sleep(1))
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

    # serial visiting would take >= 2 s; broadcast is bounded by the
    # slowest worker (margin for dispatch overhead)
    expect_lt(elapsed, 1.7)
})

test_that("one parallel_backend can be shared across stages", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    # read (1 slot) -> main -> write (1 slot) on one shared pool;
    # max_workers must be set explicitly and sum to <= worker count
    out <- 1:24 |>
        pump(function(x) {
            Sys.sleep(0.02)
            x * 2
        }, backend = bk, max_workers = 1) |>
        pump(function(x) x + 1, backend = "main") |>
        pump(function(x) x * 10, backend = bk, max_workers = 1) |>
        pump_run(verbose = FALSE)
    expect_equal(out, as.list((1:24 * 2 + 1) * 10))

    # oversubscribing the shared pool fails at dispatch time
    f <- 1:24 |>
        pump(function(x) {
            Sys.sleep(0.02)
            x
        }, backend = bk, max_workers = 2) |>
        pump(function(x) x, backend = bk, max_workers = 2)
    expect_error(
        pump_run(f, verbose = FALSE),
        "no free cluster node"
    )
})

test_that("parallel_setup_workers broadcasts while preserving expression order", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    # per-node ordering: second expression reads the first one's result
    parallel_setup_workers(bk, assign(".ord", 1, envir = globalenv()))
    parallel_setup_workers(
        bk, assign(".ord", get(".ord", envir = globalenv()) + 1,
            envir = globalenv()
        )
    )
    expect_equal(parallel_eval_workers(bk, get(".ord", envir = globalenv())),
        list(2, 2))

    # nodes run in parallel: serial would take >= 2 s
    t0 <- Sys.time()
    parallel_setup_workers(bk, Sys.sleep(1))
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    expect_lt(elapsed, 1.7)
})

test_that("parallel_stop stops the cluster and validates input", {
    skip_if_not_installed("parallel")
    bk <- parallel_backend(2)
    h <- siphon:::.pump_executor_register(bk, identity, list())

    expect_error(parallel_stop(list()), "backend must be a parallel backend")

    parallel_stop(bk)
    expect_equal(siphon:::.pump_executor_count(bk), 0L)

    # stopping an already stopped backend is a no-op
    expect_invisible(parallel_stop(bk))

    expect_error(
        siphon:::.pump_executor_new_job(bk, h, 1),
        "has been stopped"
    )
    expect_error(
        siphon:::.pump_executor_register(bk, identity, list()),
        "has been stopped"
    )
})
