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
