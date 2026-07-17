test_that("queue maintains FIFO order", {
    q <- siphon:::.pump_queue(4)
    q$push(1)
    q$push(2)
    q$push(3)
    expect_equal(q$pop(), 1)
    expect_equal(q$pop(), 2)
    q$push(4)
    expect_equal(q$pop(), 3)
    expect_equal(q$pop(), 4)
    expect_null(q$pop())
})

test_that("queue overflows when full", {
    q <- siphon:::.pump_queue(2)
    q$push(1)
    q$push(2)
    expect_error(q$push(3), "overflow")
})

test_that("queue pop returns NULL when empty", {
    q <- siphon:::.pump_queue(2)
    expect_null(q$pop())
    q$push(1)
    expect_equal(q$pop(), 1)
    expect_null(q$pop())
})

test_that("queue slot reuse works", {
    q <- siphon:::.pump_queue(2)
    q$push(1)
    q$push(2)
    q$pop()
    q$push(3)
    expect_equal(q$pop(), 2)
    expect_equal(q$pop(), 3)
})

test_that("queue peek is non-destructive and FIFO-ordered", {
    q <- siphon:::.pump_queue(4)
    expect_equal(q$peek(), list())
    q$push(1)
    q$push(2)
    q$push(3)
    expect_equal(q$peek(), list(1))
    expect_equal(q$peek(2), list(1, 2))
    # n larger than size is clamped
    expect_equal(q$peek(10), list(1, 2, 3))
    # nothing consumed
    expect_equal(q$size(), 3)
    expect_equal(q$pop(), 1)
    # peek follows head after wrap-around
    q$push(4)
    q$push(5)
    expect_equal(q$peek(4), list(2, 3, 4, 5))
})

test_that("queue size and remaining are accurate", {
    q <- siphon:::.pump_queue(3)
    expect_equal(q$size(), 0)
    expect_equal(q$remaining(), 3)
    q$push(1)
    expect_equal(q$size(), 1)
    expect_equal(q$remaining(), 2)
    q$pop()
    expect_equal(q$size(), 0)
    expect_equal(q$remaining(), 3)
})
