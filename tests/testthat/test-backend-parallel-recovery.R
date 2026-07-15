# Fault-tolerance integration tests: these kill real worker processes to
# exercise .parallel_retry_job() / .parallel_recover_worker().

# Job function that kills its worker on the first call (tracked via a flag
# file) and returns normally on subsequent calls.
crash_once_fn <- function(flag) {
    if (!file.exists(flag)) {
        file.create(flag)
        tools::pskill(Sys.getpid())
        Sys.sleep(30)
    }
    "recovered"
}
environment(crash_once_fn) <- globalenv()

# Job function that always kills its worker.
crash_always_fn <- function(x) {
    tools::pskill(Sys.getpid())
    Sys.sleep(30)
}
environment(crash_always_fn) <- globalenv()

wait_until_ready <- function(job, timeout = 30) {
    deadline <- Sys.time() + timeout
    while (Sys.time() < deadline) {
        if (siphon:::.pump_job_is_ready(job)) {
            return(TRUE)
        }
        Sys.sleep(0.1)
    }
    FALSE
}

test_that("worker crash triggers a retry that succeeds on a fresh node", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    # retry_sleep > 0 also exercises the backoff branch of the retry loop
    bk <- parallel_backend(1, retries = 3, retry_sleep = 0.1)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    flag <- tempfile()
    job <- siphon:::.pump_executor_new_job(bk, crash_once_fn, list(flag))

    expect_true(wait_until_ready(job))
    result <- siphon:::.pump_job_data(job)
    expect_equal(result$value, "recovered")
    expect_false(any(bk$state$busy))
})

test_that("exhausted retries produce a pump_error and a recovered node", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    bk <- parallel_backend(1, retries = 1L, retry_sleep = 0)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    job <- siphon:::.pump_executor_new_job(bk, crash_always_fn, list(1))

    expect_true(wait_until_ready(job))
    result <- siphon:::.pump_job_data(job)
    expect_s3_class(result$value, "pump_error")
    expect_match(
        conditionMessage(result$value),
        "Parallel job failed after 1 retries"
    )
    expect_false(any(bk$state$busy))

    # the replacement node must be fully usable afterwards
    job2 <- siphon:::.pump_executor_new_job(bk, function(x) x + 1, list(1))
    expect_true(wait_until_ready(job2))
    expect_equal(siphon:::.pump_job_data(job2)$value, 2)
})

test_that("setup expressions are replayed on replacement nodes", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    bk <- parallel_backend(1, retries = 3)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    parallel_setup_workers(bk, .replayed_value <- 42)

    crash_then_read <- function(flag) {
        if (!file.exists(flag)) {
            file.create(flag)
            tools::pskill(Sys.getpid())
            Sys.sleep(30)
        }
        get(".replayed_value", envir = globalenv())
    }
    environment(crash_then_read) <- globalenv()

    flag <- tempfile()
    job <- siphon:::.pump_executor_new_job(bk, crash_then_read, list(flag))

    expect_true(wait_until_ready(job))
    expect_equal(siphon:::.pump_job_data(job)$value, 42)
})

test_that("submission recovers a worker that died while idle", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    bk <- parallel_backend(1, retries = 3)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    # learn the worker pid, then kill the worker while it is idle
    pid_fn <- function(x) Sys.getpid()
    environment(pid_fn) <- globalenv()
    job <- siphon:::.pump_executor_new_job(bk, pid_fn, list(1))
    expect_true(wait_until_ready(job))
    pid <- siphon:::.pump_job_data(job)$value
    tools::pskill(pid)
    Sys.sleep(0.5)

    # the next submission must transparently replace the dead node, either
    # at dispatch time (send failure) or at receive time (poll failure)
    job2 <- siphon:::.pump_executor_new_job(bk, function(x) x + 1, list(1))
    expect_true(wait_until_ready(job2))
    expect_equal(siphon:::.pump_job_data(job2)$value, 2)
    expect_false(any(bk$state$busy))
})

test_that("pipeline survives a mid-stream worker crash with order intact", {
    skip_if_not_installed("parallel")
    skip_on_cran()
    skip_on_os("windows")

    bk <- parallel_backend(2, retries = 3)
    on.exit(parallel_stop(bk, force = TRUE), add = TRUE)

    make_fn <- function(flag) {
        force(flag)
        fn <- function(x) {
            if (x == 5 && !file.exists(flag)) {
                file.create(flag)
                tools::pskill(Sys.getpid())
                Sys.sleep(30)
            }
            x * 2
        }
        fn
    }

    f <- 1:10 |>
        pump(make_fn(tempfile()), backend = bk, max_workers = 2)

    out <- pump_run(f, verbose = FALSE)
    expect_equal(out, as.list(1:10 * 2))
})
