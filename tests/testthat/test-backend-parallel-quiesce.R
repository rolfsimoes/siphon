# Quiesce-on-abort, runner unregistration, and the public introspection
# accessors for the parallel backend. Motivated by integration with hosts
# that own the cluster (parallel_backend(cluster =)): an aborted run must
# not leave orphaned results in the node sockets, and long-lived pools
# must not accumulate one stage runner per pipeline run.

test_that("parallel_workers and parallel_busy report backend state", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    # specification: capacity known, no live workers yet
    bk <- parallel_backend(2)
    expect_identical(parallel_workers(bk), 2L)
    expect_identical(parallel_busy(bk), logical())

    f <- 1:4 |> pump(function(x) x, backend = bk)
    pump_run(f, verbose = FALSE)
    expect_identical(parallel_workers(bk), 2L)
    expect_identical(parallel_busy(bk), c(FALSE, FALSE))
    parallel_stop(bk)
    expect_identical(parallel_workers(bk), 0L)
    expect_identical(parallel_busy(bk), logical())

    # attached cluster
    cl <- parallel::makePSOCKcluster(2L)
    on.exit(parallel::stopCluster(cl), add = TRUE)
    bk2 <- parallel_backend(cluster = cl)
    expect_identical(parallel_workers(bk2), 2L)
    expect_identical(parallel_busy(bk2), c(FALSE, FALSE))

    expect_error(parallel_workers(list()), "parallel backend")
    expect_error(parallel_busy(list()), "parallel backend")
})

test_that("stage runners are uninstalled from workers at pipeline exit", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    cl <- parallel::makePSOCKcluster(2L)
    on.exit(parallel::stopCluster(cl), add = TRUE)
    bk <- parallel_backend(cluster = cl)

    worker_keys <- function() {
        grep(
            "^\\.siphon_stage_",
            unique(unlist(parallel::clusterEvalQ(
                cl, ls(globalenv(), all.names = TRUE)
            ))),
            value = TRUE
        )
    }

    for (i in 1:3) {
        res <- as.list(1:4) |>
            pump(function(x) x + 1L, backend = bk, max_workers = 1L) |>
            pump(function(x) x * 2L, backend = bk, max_workers = 1L) |>
            pump_run(verbose = FALSE)
        expect_identical(unlist(res), (1:4 + 1L) * 2L)
    }

    expect_length(worker_keys(), 0L)
    expect_length(bk$state$setup_exprs, 0L)
})

test_that("unregistration preserves setup expressions and replays them", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    bk <- parallel_backend(1)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)
    parallel_setup_workers(bk, threshold <- 10L)

    res <- as.list(1:3) |>
        pump(function(x) x + threshold, backend = bk) |>
        pump_run(verbose = FALSE)
    expect_identical(unlist(res), 1:3 + 10L)

    # only the runner install was dropped; user setup is still recorded
    expect_length(bk$state$setup_exprs, 1L)
    res <- as.list(1:3) |>
        pump(function(x) x * threshold, backend = bk) |>
        pump_run(verbose = FALSE)
    expect_identical(unlist(res), 1:3 * 10L)
})

test_that("an aborted run leaves an attached cluster clean", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    cl <- parallel::makePSOCKcluster(2L)
    on.exit(parallel::stopCluster(cl), add = TRUE)
    bk <- parallel_backend(cluster = cl)

    # one job fails fast while the other is still running: the abort must
    # drain the in-flight job's pending result from the node socket
    err <- tryCatch(
        as.list(1:8) |>
            pump(function(x) {
                if (x == 1L) stop("boom")
                Sys.sleep(0.3)
                x + 100L
            }, backend = bk, max_workers = 2L) |>
            pump_run(verbose = FALSE, on_error = "stop"),
        error = function(e) e
    )
    expect_s3_class(err, "error")
    expect_match(conditionMessage(err), "boom")
    expect_false(any(parallel_busy(bk)))

    # without quiesce, this clusterApply would read the orphaned siphon
    # result (x + 100 wrapped in the job protocol) instead of its own
    out <- parallel::clusterApply(cl, 1:2, function(i) i * 1000L)
    expect_identical(unlist(out), c(1000L, 2000L))

    # and no stale runners remain installed on the (freed) nodes
    keys <- grep(
        "^\\.siphon_stage_",
        unique(unlist(parallel::clusterEvalQ(
            cl, ls(globalenv(), all.names = TRUE)
        ))),
        value = TRUE
    )
    expect_length(keys, 0L)
})

test_that("an aborted run leaves an owned backend reusable", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    err <- tryCatch(
        as.list(1:8) |>
            pump(function(x) {
                if (x == 1L) stop("boom")
                Sys.sleep(0.3)
                x
            }, backend = bk, max_workers = 2L) |>
            pump_run(verbose = FALSE, on_error = "stop"),
        error = function(e) e
    )
    expect_s3_class(err, "error")
    expect_false(any(parallel_busy(bk)))

    res <- as.list(1:4) |>
        pump(function(x) x + 1L, backend = bk) |>
        pump_run(verbose = FALSE)
    expect_identical(unlist(res), 1:4 + 1L)
})

test_that("quiesce quarantines attached nodes that cannot be drained", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    cl <- parallel::makePSOCKcluster(2L)
    on.exit(parallel::stopCluster(cl), add = TRUE)
    bk <- parallel_backend(cluster = cl)

    old <- options(siphon.quiesce_timeout = 0)
    on.exit(options(old), add = TRUE)

    err <- tryCatch(
        as.list(1:8) |>
            pump(function(x) {
                if (x == 1L) stop("boom")
                Sys.sleep(5)
                x
            }, backend = bk, max_workers = 2L) |>
            pump_run(verbose = FALSE, on_error = "stop"),
        error = function(e) e
    )
    expect_s3_class(err, "error")
    # the slow job cannot be drained within the zero budget: siphon must
    # not replace a node it does not own, so the node stays quarantined
    # for the owner to find via parallel_busy() and repair
    expect_identical(sum(parallel_busy(bk)), 1L)
})

test_that("quiesce restores owned nodes that cannot be drained", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    bk <- parallel_backend(2)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    old <- options(siphon.quiesce_timeout = 0)
    on.exit(options(old), add = TRUE)

    err <- tryCatch(
        as.list(1:8) |>
            pump(function(x) {
                if (x == 1L) stop("boom")
                Sys.sleep(5)
                x
            }, backend = bk, max_workers = 2L) |>
            pump_run(verbose = FALSE, on_error = "stop"),
        error = function(e) e
    )
    expect_s3_class(err, "error")
    # the undrainable node of an owned pool is replaced, not quarantined
    expect_false(any(parallel_busy(bk)))

    res <- as.list(1:4) |>
        pump(function(x) x * 2L, backend = bk) |>
        pump_run(verbose = FALSE)
    expect_identical(unlist(res), 1:4 * 2L)
})

test_that(".pump_quiesce_timeout validates its option", {
    old <- options(siphon.quiesce_timeout = -1)
    on.exit(options(old), add = TRUE)
    expect_error(siphon:::.pump_quiesce_timeout(), "non-negative")
    options(siphon.quiesce_timeout = 12)
    expect_identical(siphon:::.pump_quiesce_timeout(), 12)
})
