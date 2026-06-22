.pump_queue <- function(capacity = 1024L) {
    # check parameters
    if (!is.numeric(capacity) || length(capacity) != 1L) {
        stop("capacity must be scalar")
    }
    capacity <- as.integer(capacity)
    if (capacity < 1L) stop("capacity must be >= 1")

    # private members
    buf <- vector("list", capacity)
    head <- 1L
    tail <- 1L
    size <- 0L

    # private methods
    inc <- function(i) if (i == capacity) 1L else i + 1L

    # public methods
    self <- list(
        push = function(x) {
            if (size == capacity) stop("queue overflow")
            buf[[tail]] <<- x
            tail <<- inc(tail)
            size <<- size + 1L
            invisible(NULL)
        },
        pop = function() {
            if (size == 0L) {
                return(NULL)
            }
            x <- buf[[head]]
            buf[head] <<- list(NULL)
            head <<- inc(head)
            size <<- size - 1L
            x
        },
        remaining = function() capacity - size,
        size = function() size
    )

    structure(self, class = "pump_queue")
}
