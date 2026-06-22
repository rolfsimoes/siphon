test_that("slots acquire up to batch limit", {
    sl <- siphon:::.pump_slots(3)
    expect_equal(sl$n_free(), 3)
    expect_equal(sl$active(), 0)

    j1 <- sl$acquire(1, list())
    expect_equal(sl$n_free(), 2)
    expect_equal(sl$active(), 1)

    j2 <- sl$acquire(2, list())
    j3 <- sl$acquire(3, list())
    expect_equal(sl$n_free(), 0)
    expect_equal(sl$active(), 3)

    expect_null(sl$acquire(4, list()))
})

test_that("slots poll_ready returns NULL when empty", {
    sl <- siphon:::.pump_slots(2)
    expect_null(sl$poll_ready())
})

test_that("slots rotate unresolved jobs", {
    ready_job <- structure(list(val = 42), class = "pump_main_job")
    not_ready_job <- structure(list(), class = "pump_test_not_ready")
    assign(".pump_job_is_ready.pump_test_not_ready", function(job) FALSE, envir = .GlobalEnv)
    on.exit(rm(".pump_job_is_ready.pump_test_not_ready", envir = .GlobalEnv), add = TRUE)

    sl <- siphon:::.pump_slots(2)
    sl$acquire(1, ready_job)
    sl$acquire(2, not_ready_job)

    res <- sl$poll_ready()
    expect_equal(res$id, 1)
    expect_equal(sl$active(), 1)
    expect_equal(sl$n_free(), 1)

    expect_null(sl$poll_ready())
    expect_equal(sl$active(), 1)
})

test_that("slots invariant holds after mixed operations", {
    ready_job <- structure(list(val = 42), class = "pump_main_job")
    sl <- siphon:::.pump_slots(3)
    sl$acquire(1, ready_job)
    sl$acquire(2, ready_job)
    sl$poll_ready()
    sl$acquire(3, ready_job)
    sl$poll_ready()
    sl$poll_ready()
    expect_equal(sl$n_free(), 3)
    expect_equal(sl$active(), 0)
})
