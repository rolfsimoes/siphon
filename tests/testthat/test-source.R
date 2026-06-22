test_that("source item ids are sequential", {
    src <- .pump_source_basic(list(10, 20, 30))
    expect_equal(src$next_item()$id, 1)
    expect_equal(src$next_item()$id, 2)
    expect_equal(src$next_item()$id, 3)
})

test_that("source is done after exhaustion", {
    src <- .pump_source_basic(list(1, 2))
    expect_false(src$done())
    src$next_item()
    expect_false(src$done())
    src$next_item()
    expect_true(src$done())
    expect_null(src$next_item())
})

test_that("source handles empty input", {
    src <- .pump_source_basic(list())
    expect_true(src$done())
    expect_null(src$next_item())
    expect_equal(length(src), 0)
})

test_that("source marks captured error items as not ok, but not general error objects", {
    e_legit <- simpleError("bad")
    e_captured <- siphon:::.pump_error(simpleError("captured"))
    src <- .pump_source_basic(list(1, e_legit, e_captured))
    expect_true(src$next_item()$ok)
    expect_true(src$next_item()$ok)
    expect_false(src$next_item()$ok)
})

test_that("source passes through data correctly", {
    src <- .pump_source_basic(list("a", 2, TRUE))
    expect_equal(src$next_item()$data, "a")
    expect_equal(src$next_item()$data, 2)
    expect_equal(src$next_item()$data, TRUE)
})

test_that("source length and pipeline_length match", {
    src <- .pump_source_basic(1:5)
    expect_equal(length(src), 5)
    expect_equal(src$pipeline_length(), 5)
})

test_that("pump_source creates custom source", {
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
    expect_equal(length(src), 3)
    expect_false(src$done())
    expect_equal(src$next_item()$data, 10)
    expect_equal(src$next_item()$data, 20)
    expect_equal(src$next_item()$data, 30)
    expect_null(src$next_item())
    expect_true(src$done())
})

test_that("pump_source defaults to infinite source", {
    src <- pump_source(
        pull_fn = function() NULL
    )
    expect_equal(length(src), Inf)
    expect_false(src$done())
})

test_that("pump_source close_fn is callable", {
    closed <- FALSE
    src <- pump_source(
        pull_fn = function() NULL,
        close_fn = function() closed <<- TRUE
    )
    src$close()
    expect_true(closed)
})

test_that("pump_source validates pull_fn output format", {
    # Missing required keys
    src <- pump_source(pull_fn = function() list(id = 1, data = "hello"))
    expect_error(src$next_item(), "pull_fn must return NULL or a list with 'id', 'data', and 'ok' elements")

    # NULL id
    src2 <- pump_source(pull_fn = function() list(id = NULL, data = "hello", ok = TRUE))
    expect_error(src2$next_item(), "pull_fn must return items with a valid scalar atomic 'id'")

    # Non-scalar id
    src3 <- pump_source(pull_fn = function() list(id = 1:2, data = "hello", ok = TRUE))
    expect_error(src3$next_item(), "pull_fn must return items with a valid scalar atomic 'id'")

    # Non-atomic list id
    src_list_id <- pump_source(pull_fn = function() list(id = list("abc"), data = "hello", ok = TRUE))
    expect_error(src_list_id$next_item(), "pull_fn must return items with a valid scalar atomic 'id'")

    # Non-scalar or invalid ok
    src4 <- pump_source(pull_fn = function() list(id = 1, data = "hello", ok = "yes"))
    expect_error(src4$next_item(), "pull_fn must return items with a valid scalar logical 'ok'")
})

test_that("pump_run handles arbitrary non-integer and large integer ids", {
    # Test arbitrary string ids
    i <- 0L
    ids <- c("first", "second", "third")
    src <- pump_source(
        pull_fn = function() {
            if (i >= 3L) return(NULL)
            i <<- i + 1L
            list(id = ids[i], data = i * 2, ok = TRUE)
        },
        done_fn = function() i >= 3L
    )
    
    out <- src |>
        pump(function(x) x + 1, backend = "main") |>
        pump_run(verbose = FALSE)
    expect_equal(out, list(3, 5, 7))

    # Test large integer id (e.g. 1000000) does not allocate a giant list
    j <- 0L
    src_large <- pump_source(
        pull_fn = function() {
            if (j >= 1L) return(NULL)
            j <<- j + 1L
            list(id = 1000000L, data = 42, ok = TRUE)
        },
        done_fn = function() j >= 1L
    )

    out_large <- src_large |>
        pump(identity, backend = "main") |>
        pump_run(verbose = FALSE)
    expect_equal(length(out_large), 1)
    expect_equal(out_large[[1]], 42)
})

test_that("pump_run throws error if idx is missing", {
    # If a source somehow bypasses pump_source wrapping or doesn't have idx
    src <- structure(list(
        next_item = function() {
            list(id = "abc", data = "hello", ok = TRUE) # no idx!
        },
        length = function() Inf,
        pipeline_length = function() Inf,
        buffer = function() NULL,
        slots = function() NULL,
        progress = function() 0L,
        stage_completed = function() 0L,
        errors = function() 0L,
        done = function() FALSE,
        close = function() invisible(NULL),
        item_commit = function(id, data) invisible(NULL),
        item_abort = function(id, error = NULL, data = NULL) invisible(NULL),
        item_release = function(id) invisible(NULL),
        backend = function() main_backend()
    ), class = "pump")

    expect_error(pump_run(src, verbose = FALSE), "item index \\(idx\\) is missing")
})

test_that(".pump_validate_idx performs strict validation", {
    # Valid whole numbers
    expect_equal(.pump_validate_idx(1), 1L)
    expect_equal(.pump_validate_idx(42L), 42L)
    expect_equal(.pump_validate_idx(100.0), 100L)

    # Missing/NULL/NA
    expect_error(.pump_validate_idx(NULL), "index \\(idx\\) is missing")
    expect_error(.pump_validate_idx(NA), "index \\(idx\\) must be finite and non-missing")
    expect_error(.pump_validate_idx(NA_integer_), "index \\(idx\\) must be finite and non-missing")

    # Non-numeric
    expect_error(.pump_validate_idx("1"), "index \\(idx\\) must be numeric")
    expect_error(.pump_validate_idx(list(1)), "index \\(idx\\) must be numeric")

    # Non-scalar
    expect_error(.pump_validate_idx(c(1, 2)), "index \\(idx\\) must be a scalar")
    expect_error(.pump_validate_idx(integer(0)), "index \\(idx\\) must be a scalar")

    # Non-finite
    expect_error(.pump_validate_idx(Inf), "index \\(idx\\) must be finite and non-missing")
    expect_error(.pump_validate_idx(NaN), "index \\(idx\\) must be finite and non-missing")

    # Non-whole number
    expect_error(.pump_validate_idx(1.5), "index \\(idx\\) must be a whole number")

    # Less than 1
    expect_error(.pump_validate_idx(0), "index \\(idx\\) must be >= 1")
    expect_error(.pump_validate_idx(-5L), "index \\(idx\\) must be >= 1")

    # Exceeds integer range
    expect_error(.pump_validate_idx(3000000000), "index \\(idx\\) exceeds maximum integer range")
})



