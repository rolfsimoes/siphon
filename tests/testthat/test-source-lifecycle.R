test_that("pump_item_registry() stores and retrieves values", {
    registry <- pump_item_registry()

    registry$set("a", 1)
    registry$set("b", 2)
    registry$set(3, "c")

    expect_equal(registry$get("a"), 1)
    expect_equal(registry$get("b"), 2)
    expect_equal(registry$get(3), "c")
})

test_that("pump_item_registry() has() returns correct status", {
    registry <- pump_item_registry()

    expect_false(registry$has("a"))

    registry$set("a", 1)

    expect_true(registry$has("a"))
    expect_false(registry$has("b"))
})

test_that("pump_item_registry() remove() removes one id", {
    registry <- pump_item_registry()

    registry$set("a", 1)
    registry$set("b", 2)

    registry$remove("a")

    expect_false(registry$has("a"))
    expect_true(registry$has("b"))
})

test_that("pump_item_registry() ids() returns current ids", {
    registry <- pump_item_registry()

    expect_equal(registry$ids(), character(0))

    registry$set("a", 1)
    registry$set("b", 2)

    ids <- registry$ids()
    expect_setequal(ids, c("a", "b"))
})

test_that("pump_item_registry() clear() removes all ids", {
    registry <- pump_item_registry()

    registry$set("a", 1)
    registry$set("b", 2)

    registry$clear()

    expect_false(registry$has("a"))
    expect_false(registry$has("b"))
    expect_equal(registry$ids(), character(0))
})

test_that("pump_item_registry() drain() calls fn for all entries", {
    registry <- pump_item_registry()

    registry$set("a", 1)
    registry$set("b", 2)
    registry$set("c", 3)

    collected <- list()
    registry$drain(function(id, value) {
        collected[[id]] <<- value
    })

    expect_equal(collected$a, 1)
    expect_equal(collected$b, 2)
    expect_equal(collected$c, 3)
    expect_equal(registry$ids(), character(0))
})

test_that("pump_item_registry() converts ids to character", {
    registry <- pump_item_registry()

    registry$set(1, "one")
    registry$set(2, "two")

    expect_equal(registry$get(1), "one")
    expect_equal(registry$get(2), "two")
    expect_true(registry$has(1))
    expect_true(registry$has(2))
})

test_that("pump_managed_source() validates arguments", {
    expect_error(pump_managed_source(
        pull_fn = "not a function",
        id_fn = function(x) x,
        data_fn = function(x) x,
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL
    ), "pull_fn must be a function")

    expect_error(pump_managed_source(
        pull_fn = function() NULL,
        id_fn = "not a function",
        data_fn = function(x) x,
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL
    ), "id_fn must be a function")

    expect_error(pump_managed_source(
        pull_fn = function() NULL,
        id_fn = function(x) x,
        data_fn = "not a function",
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL
    ), "data_fn must be a function")

    expect_error(pump_managed_source(
        pull_fn = function() NULL,
        id_fn = function(x) x,
        data_fn = function(x) x,
        commit_fn = "not a function",
        abort_fn = function(x, y, z) NULL
    ), "commit_fn must be a function")

    expect_error(pump_managed_source(
        pull_fn = function() NULL,
        id_fn = function(x) x,
        data_fn = function(x) x,
        commit_fn = function(x, y) NULL,
        abort_fn = "not a function"
    ), "abort_fn must be a function")

    expect_error(pump_managed_source(
        pull_fn = function() NULL,
        id_fn = function(x) x,
        data_fn = function(x) x,
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL,
        release_fn = "not a function"
    ), "release_fn must be NULL or a function")
})

test_that("pump_managed_source() pull_fn returning NULL produces no item", {
    items <- list(
        list(id = 1, value = "a"),
        list(id = 2, value = "b"),
        NULL
    )
    idx <- 1

    source <- pump_managed_source(
        pull_fn = function() {
            item <- items[[idx]]
            idx <<- idx + 1
            item
        },
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL
    )

    result1 <- source$next_item()
    expect_false(is.null(result1))
    expect_equal(result1$id, 1)

    result2 <- source$next_item()
    expect_false(is.null(result2))
    expect_equal(result2$id, 2)

    result3 <- source$next_item()
    expect_true(is.null(result3))
})

test_that("pump_managed_source() id_fn defines the emitted id", {
    source <- pump_managed_source(
        pull_fn = function() list(id = 123, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL
    )

    result <- source$next_item()
    expect_equal(result$id, 123)
})

test_that("pump_managed_source() data_fn defines the emitted data", {
    source <- pump_managed_source(
        pull_fn = function() list(id = 1, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL
    )

    result <- source$next_item()
    expect_equal(result$data, "test")
})

test_that("pump_managed_source() data_fn errors set ok to FALSE", {
    source <- pump_managed_source(
        pull_fn = function() list(id = 1, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) stop("error"),
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL
    )

    result <- source$next_item()
    expect_false(result$ok)
    expect_true(inherits(result$data, "error"))
})

test_that("pump_managed_source() successful terminal item calls commit_fn", {
    commit_called <- FALSE
    commit_item <- NULL
    commit_data <- NULL

    source <- pump_managed_source(
        pull_fn = function() list(id = 1, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(item, data) {
            commit_called <<- TRUE
            commit_item <<- item
            commit_data <<- data
        },
        abort_fn = function(x, y, z) NULL
    )

    source$next_item()
    source$item_commit(1, "test")

    expect_true(commit_called)
    expect_equal(commit_item$value, "test")
    expect_equal(commit_data, "test")
})

test_that("pump_managed_source() successful terminal item calls release_fn if supplied", {
    release_called <- FALSE
    release_item <- NULL

    source <- pump_managed_source(
        pull_fn = function() list(id = 1, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL,
        release_fn = function(item) {
            release_called <<- TRUE
            release_item <<- item
        }
    )

    source$next_item()
    source$item_commit(1, "test")
    source$item_release(1)

    expect_true(release_called)
    expect_equal(release_item$value, "test")
})

test_that("pump_managed_source() failed terminal handler calls abort_fn", {
    abort_called <- FALSE
    abort_item <- NULL
    abort_error <- NULL
    abort_data <- NULL

    source <- pump_managed_source(
        pull_fn = function() list(id = 1, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(x, y) NULL,
        abort_fn = function(item, error, data) {
            abort_called <<- TRUE
            abort_item <<- item
            abort_error <<- error
            abort_data <<- data
        }
    )

    source$next_item()
    source$item_abort(1, error = "error", data = "test")

    expect_true(abort_called)
    expect_equal(abort_item$value, "test")
    expect_equal(abort_error, "error")
    expect_equal(abort_data, "test")
})

test_that("pump_managed_source() close_fn aborts pending items before closing", {
    abort_calls <- list()

    source <- pump_managed_source(
        pull_fn = function() list(id = 1, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(x, y) NULL,
        abort_fn = function(item, error, data) {
            abort_calls <<- c(abort_calls, list(item$id))
        }
    )

    source$next_item()
    source$close()

    expect_length(abort_calls, 1)
    expect_equal(abort_calls[[1]], 1)
})

test_that("pump_managed_source() close_fn calls user close_fn after draining", {
    close_called <- FALSE
    abort_called <- FALSE

    source <- pump_managed_source(
        pull_fn = function() list(id = 1, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(x, y) NULL,
        abort_fn = function(item, error, data) {
            abort_called <<- TRUE
        },
        close_fn = function() {
            close_called <<- TRUE
        }
    )

    source$next_item()
    source$close()

    expect_true(abort_called)
    expect_true(close_called)
})

test_that("pump_managed_source() done_fn is passed through", {
    source <- pump_managed_source(
        pull_fn = function() list(id = 1, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL,
        done_fn = function() TRUE
    )

    expect_true(source$done())
})

test_that("pump_managed_source() length is passed through", {
    source <- pump_managed_source(
        pull_fn = function() list(id = 1, value = "test"),
        id_fn = function(x) x$id,
        data_fn = function(x) x$value,
        commit_fn = function(x, y) NULL,
        abort_fn = function(x, y, z) NULL,
        length = 10
    )

    expect_equal(source$length(), 10)
})
