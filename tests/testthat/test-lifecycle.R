# Compatibility tests - ensure old behavior still works

test_that("vector source still works", {
    out <- 1:3 |>
        pump(function(x) x * 2, backend = "main") |>
        pump_run(verbose = FALSE)
    expect_equal(out, list(2, 4, 6))
})

test_that("list source still works", {
    out <- list("a", "b", "c") |>
        pump(function(x) toupper(x), backend = "main") |>
        pump_run(verbose = FALSE)
    expect_equal(out, list("A", "B", "C"))
})

test_that("custom pump_source without lifecycle callbacks still works", {
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 3L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 3L,
        length = 3
    )
    out <- src |> pump(function(x) x + 1, backend = "main") |> pump_run(verbose = FALSE)
    expect_equal(out, list(11, 21, 31))
})

test_that("existing error policies still work for simple sources", {
    # on_error = "stop"
    expect_error(
        1:3 |> pump(function(x) if (x == 2) stop("bad") else x, backend = "main", on_error = "stop") |> pump_run(verbose = FALSE),
        "bad"
    )
    
    # on_error = "collect"
    out <- 1:3 |> pump(function(x) if (x == 2) stop("bad") else x, backend = "main", on_error = "collect") |> pump_run(verbose = FALSE)
    expect_equal(length(out), 3)
    expect_true(inherits(out[[2]], "simpleError"))
    
    # on_error = "continue"
    out <- 1:3 |> pump(function(x) if (x == 2) stop("bad") else x, backend = "main", on_error = "continue") |> pump_run(verbose = FALSE)
    expect_equal(length(out), 2)
    expect_equal(out, list(1, 3))
})

test_that("existing ordering behavior of pump_run still works", {
    out <- 3:1 |>
        pump(function(x) x * 2, backend = "main") |>
        pump_run(verbose = FALSE)
    expect_equal(out, list(6, 4, 2))
})

test_that("empty upstream behavior still works", {
    out <- integer(0) |>
        pump(function(x) x * 2, backend = "main") |>
        pump_run(verbose = FALSE)
    expect_equal(out, list())
})

test_that(".pump_empty_stage still reports done as TRUE", {
    src <- .pump_source_basic(integer(0))
    stage <- pump(src, function(x) x, backend = "main")
    expect_true(stage$done())
})

# Lifecycle tests - test new behavior

test_that("successful item in pump_drain calls item_commit_fn once", {
    commit_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 2L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 2L,
        item_commit_fn = function(id, data) {
            commit_calls <<- c(commit_calls, list(list(id = id, data = data)))
        }
    )
    
    pump_drain(src, handle_fn = function(id, data, ok) {}, verbose = FALSE)
    expect_equal(length(commit_calls), 2)
    expect_equal(commit_calls[[1]]$id, 1)
    expect_equal(commit_calls[[1]]$data, 10)
    expect_equal(commit_calls[[2]]$id, 2)
    expect_equal(commit_calls[[2]]$data, 20)
})

test_that("successful item in pump_drain calls item_release_fn once", {
    release_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 2L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 2L,
        item_release_fn = function(id) {
            release_calls <<- c(release_calls, id)
        }
    )
    
    pump_drain(src, handle_fn = function(id, data, ok) {}, verbose = FALSE)
    expect_equal(length(release_calls), 2)
    expect_equal(release_calls, list(1, 2))
})

test_that("failing handle_fn in pump_drain calls item_abort_fn once", {
    abort_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L,
        item_abort_fn = function(id, error = NULL, data = NULL) {
            abort_calls <<- c(abort_calls, list(list(id = id, error = error, data = data)))
        }
    )
    
    expect_error(
        pump_drain(src, handle_fn = function(id, data, ok) stop("handler failed"), verbose = FALSE),
        "handler failed"
    )
    expect_equal(length(abort_calls), 1)
    expect_equal(abort_calls[[1]]$id, 1)
    expect_true(inherits(abort_calls[[1]]$error, "simpleError"))
    expect_equal(abort_calls[[1]]$data, 10)
})

test_that("failing handle_fn in pump_drain calls item_release_fn once", {
    release_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L,
        item_release_fn = function(id) {
            release_calls <<- c(release_calls, id)
        }
    )
    
    expect_error(
        pump_drain(src, handle_fn = function(id, data, ok) stop("handler failed"), verbose = FALSE),
        "handler failed"
    )
    expect_equal(length(release_calls), 1)
    expect_equal(release_calls, list(1))
})

test_that("on_error = continue in drain_completed calls item_abort_fn once for dropped item", {
    abort_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L,
        item_abort_fn = function(id, error = NULL, data = NULL) {
            abort_calls <<- c(abort_calls, list(list(id = id, error = error, data = data)))
        }
    )
    
    pipeline <- src |>
        pump(function(x) stop("stage error"), backend = "main", on_error = "continue")
    
    pump_drain(pipeline, handle_fn = function(id, data, ok) {}, verbose = FALSE)
    expect_equal(length(abort_calls), 1)
    expect_equal(abort_calls[[1]]$id, 1)
    expect_true(inherits(abort_calls[[1]]$error, "simpleError"))
})

test_that("on_error = continue in drain_completed calls item_release_fn once for dropped item", {
    release_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L,
        item_release_fn = function(id) {
            release_calls <<- c(release_calls, id)
        }
    )
    
    pipeline <- src |>
        pump(function(x) stop("stage error"), backend = "main", on_error = "continue")
    
    pump_drain(pipeline, handle_fn = function(id, data, ok) {}, verbose = FALSE)
    expect_equal(length(release_calls), 1)
    expect_equal(release_calls, list(1))
})

test_that("on_error = continue in advance calls item_abort_fn once for dropped upstream error item", {
    abort_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = .pump_error(simpleError("source error")), ok = FALSE)
        },
        done_fn = function() i >= 1L,
        item_abort_fn = function(id, error = NULL, data = NULL) {
            abort_calls <<- c(abort_calls, list(list(id = id, error = error, data = data)))
        }
    )
    
    pipeline <- src |>
        pump(function(x) x, backend = "main", on_error = "continue")
    
    pump_drain(pipeline, handle_fn = function(id, data, ok) {}, verbose = FALSE)
    expect_equal(length(abort_calls), 1)
    expect_equal(abort_calls[[1]]$id, 1)
    expect_true(inherits(abort_calls[[1]]$error, "simpleError"))
})

test_that("on_error = continue in advance calls item_release_fn once for dropped upstream error item", {
    release_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = .pump_error(simpleError("source error")), ok = FALSE)
        },
        done_fn = function() i >= 1L,
        item_release_fn = function(id) {
            release_calls <<- c(release_calls, id)
        }
    )
    
    pipeline <- src |>
        pump(function(x) x, backend = "main", on_error = "continue")
    
    pump_drain(pipeline, handle_fn = function(id, data, ok) {}, verbose = FALSE)
    expect_equal(length(release_calls), 1)
    expect_equal(release_calls, list(1))
})

test_that("on_error = collect sends failed item to terminal runner", {
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L
    )
    
    pipeline <- src |>
        pump(function(x) stop("stage error"), backend = "main", on_error = "collect")
    
    out <- pump_run(pipeline, verbose = FALSE)
    expect_equal(length(out), 1)
    expect_true(inherits(out[[1]], "simpleError"))
})

test_that("terminal runner accepting collected error item calls item_commit_fn", {
    commit_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L,
        item_commit_fn = function(id, data) {
            commit_calls <<- c(commit_calls, list(list(id = id, data = data)))
        }
    )
    
    pipeline <- src |>
        pump(function(x) stop("stage error"), backend = "main", on_error = "collect")
    
    out <- pump_run(pipeline, verbose = FALSE)
    expect_equal(length(commit_calls), 1)
    expect_equal(commit_calls[[1]]$id, 1)
    expect_true(inherits(commit_calls[[1]]$data, "pump_error"))
})

test_that("lifecycle callbacks receive id, not idx", {
    commit_ids <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 2L) return(NULL)
            i <<- i + 1L
            list(id = paste0("item-", i), data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 2L,
        item_commit_fn = function(id, data) {
            commit_ids <<- c(commit_ids, id)
        }
    )
    
    pump_drain(src, handle_fn = function(id, data, ok) {}, verbose = FALSE)
    expect_equal(commit_ids, list("item-1", "item-2"))
})

test_that("item_release_fn is not called twice for the same item", {
    release_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L,
        item_release_fn = function(id) {
            release_calls <<- c(release_calls, id)
        }
    )
    
    pump_drain(src, handle_fn = function(id, data, ok) {}, verbose = FALSE)
    expect_equal(length(release_calls), 1)
    expect_equal(release_calls, list(1))
})

test_that(".pump_empty_stage forwards item_commit to upstream", {
    commit_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L,
        item_commit_fn = function(id, data) {
            commit_calls <<- c(commit_calls, list(list(id = id, data = data)))
        }
    )
    
    # Create an empty stage by pumping on the custom source (which will be empty after one item)
    # Actually, let's create it from the source directly
    empty_stage <- .pump_empty_stage(src, main_backend(), NULL)
    
    # The empty stage should forward item_commit to upstream
    empty_stage$item_commit(1, 100)
    expect_equal(length(commit_calls), 1)
    expect_equal(commit_calls[[1]]$id, 1)
    expect_equal(commit_calls[[1]]$data, 100)
})

test_that(".pump_empty_stage forwards item_abort to upstream", {
    abort_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L,
        item_abort_fn = function(id, error = NULL, data = NULL) {
            abort_calls <<- c(abort_calls, list(list(id = id, error = error, data = data)))
        }
    )
    
    # Create an empty stage from the source directly
    empty_stage <- .pump_empty_stage(src, main_backend(), NULL)
    
    # The empty stage should forward item_abort to upstream
    test_error <- simpleError("test")
    empty_stage$item_abort(1, error = test_error, data = 100)
    expect_equal(length(abort_calls), 1)
    expect_equal(abort_calls[[1]]$id, 1)
    expect_equal(abort_calls[[1]]$error, test_error)
    expect_equal(abort_calls[[1]]$data, 100)
})

test_that(".pump_empty_stage forwards item_release to upstream", {
    release_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 1L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 1L,
        item_release_fn = function(id) {
            release_calls <<- c(release_calls, id)
        }
    )
    
    # Create an empty stage from the source directly
    empty_stage <- .pump_empty_stage(src, main_backend(), NULL)
    
    # The empty stage should forward item_release to upstream
    empty_stage$item_release(1)
    expect_equal(length(release_calls), 1)
    expect_equal(release_calls, list(1))
})

test_that("pump_run commits successful items", {
    commit_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 2L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 2L,
        item_commit_fn = function(id, data) {
            commit_calls <<- c(commit_calls, list(list(id = id, data = data)))
        }
    )
    
    out <- src |> pump(function(x) x + 1, backend = "main") |> pump_run(verbose = FALSE)
    expect_equal(length(commit_calls), 2)
    expect_equal(commit_calls[[1]]$id, 1)
    expect_equal(commit_calls[[1]]$data, 11)
    expect_equal(commit_calls[[2]]$id, 2)
    expect_equal(commit_calls[[2]]$data, 21)
})

test_that("pump_run releases successful items", {
    release_calls <- list()
    i <- 0L
    src <- pump_source(
        pull_fn = function() {
            if (i >= 2L) return(NULL)
            i <<- i + 1L
            list(id = i, data = i * 10, ok = TRUE)
        },
        done_fn = function() i >= 2L,
        item_release_fn = function(id) {
            release_calls <<- c(release_calls, id)
        }
    )
    
    out <- src |> pump(function(x) x + 1, backend = "main") |> pump_run(verbose = FALSE)
    expect_equal(length(release_calls), 2)
    expect_equal(release_calls, list(1, 2))
})

