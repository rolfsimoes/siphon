.pump_slots <- function(slots) {
    # check parameters
    if (!is.numeric(slots) || length(slots) != 1L) stop("slots must be scalar")
    slots <- as.integer(slots)
    if (slots < 1L) stop("slots must be >= 1")

    # private members
    data <- vector("list", slots)
    free <- .pump_queue(slots)
    active <- .pump_queue(slots)

    # init private members
    # data contains a list with: list(id = ..., job = ...)
    for (j in seq_len(slots)) data[[j]] <- list(id = NULL, job = NULL)
    for (j in seq_len(slots)) free$push(j)

    # public methods
    self <- list(
        limit = function() slots,
        n_free = function() slots - free$remaining(),
        active = function() slots - active$remaining(),
        acquire = function(id, job) {
            # get a index of first free slot
            j <- free$pop()
            if (is.null(j)) {
                return(NULL)
            }
            # allocate an job
            data[[j]]$id <<- id
            data[[j]]$job <<- job
            active$push(j)

            # invariant check
            stopifnot(free$size() + active$size() == slots)
            j
        },
        poll_ready = function() {
            n_active <- slots - active$remaining()
            if (n_active == 0L) {
                return(NULL)
            }

            res <- NULL
            for (i in seq_len(n_active)) {
                j <- active$pop()
                if (is.null(j)) break

                # not ready: back to the queue
                if (.pump_job_is_ready(data[[j]]$job)) {
                    # get result
                    res <- data[[j]]

                    # free slot
                    data[[j]]$id <<- NULL
                    data[[j]]$job <<- NULL
                    free$push(j)
                    return(res)
                } else {
                    # rotate: keep in queue
                    active$push(j)
                }
            }
            NULL
        }
    )

    structure(self, class = "pump_slots")
}
