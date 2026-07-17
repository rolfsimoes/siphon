#' Create a parallel (PSOCK) backend
#'
#' `parallel_backend()` creates and manages its own PSOCK cluster and submits
#' jobs to it. Jobs are dispatched with a non-blocking send and readiness is
#' polled with `socketSelect()`, so the main process never blocks while jobs
#' run on the cluster nodes.
#'
#' @details The backend owns its cluster: it is created when
#'   `parallel_backend()` is called and must be shut down with
#'   [parallel_stop()] when no longer needed. Each node runs at most one job
#'   at a time, so `max_workers` for a stage using this backend must not
#'   exceed the number of workers (enforced by `pump()`).
#'
#'   Passing the same backend to several `pump()` stages shares its cluster
#'   between them — this is the normal way to run a multi-stage pipeline on
#'   one worker pool. Node availability is tracked at the backend level and
#'   results are received per node, so concurrent stages cannot cross-wire.
#'   The one sizing rule is that the `max_workers` of the stages sharing the
#'   backend must not sum to more than the number of workers; since each
#'   stage's `max_workers` defaults to the full worker count, set it
#'   explicitly on every sharing stage (e.g. a pool of `n + 1` workers
#'   serving a read stage with `max_workers = n` and a write stage with
#'   `max_workers = 1`). Oversubscribing fails at dispatch time with a
#'   free-node invariant error. Jobs in a shared pool have no node affinity;
#'   to isolate worker pools per stage (different hosts, retry settings, or
#'   worker state), create a separate backend for each stage instead — see
#'   the "Pooling strategies" section of `vignette("siphon")`.
#'
#'   This backend uses the unexported `sendCall()` and `recvResult()`
#'   functions from the `parallel` package to communicate with cluster nodes.
#'
#' @section Fault tolerance:
#'   The backend is fault tolerant to worker process failures: if a worker
#'   connection fails while a job is running or while a job is being
#'   dispatched (e.g. the node died while idle), the backend replaces the
#'   node with a fresh one and resubmits the job, up to `retries` times,
#'   sleeping `retry_sleep` seconds before each attempt. Setup expressions
#'   registered with [parallel_setup_workers()] are replayed on replacement
#'   nodes. When retries are exhausted, a job that failed while running
#'   yields an error value (a `pump_error` condition) instead of failing the
#'   pipeline, and the node is restored for subsequent jobs; a job that
#'   could not be dispatched at all raises an error.
#'
#'   Because failed jobs are resubmitted, execution follows at-least-once
#'   semantics: a job's side effects (file writes, database inserts, API
#'   calls) may run more than once if a worker dies after the effect but
#'   before the result is delivered. Job functions should be idempotent.
#'
#'   The following failures are **not** handled: hung workers (failure
#'   detection is connection-based, so a worker that stalls without dying is
#'   never detected and there is no job timeout) and failures of the main R
#'   process (there is no persistence or checkpointing; in-flight work is
#'   lost).
#'
#' @param workers The number of local worker processes to start, or a
#'   character vector of host names to run workers on (as accepted by
#'   `parallel::makePSOCKcluster()`).
#' @param ... Additional arguments passed to
#'   `parallel::makePSOCKcluster()`.
#' @param retries Number of times a job is resubmitted after a worker
#'   connection failure before the job is marked as failed. A non-negative
#'   integer.
#' @param retry_sleep Seconds to wait before each retry attempt. A
#'   non-negative number.
#' @return A backend object.
#' @seealso [parallel_setup_workers()], [parallel_stop()]
#' @examples
#' if (requireNamespace("parallel", quietly = TRUE)) {
#'     bk <- parallel_backend(2)
#'     f <- 1:5 |>
#'         pump(function(x) x * 2, backend = bk)
#'     pump_run(f, verbose = FALSE)
#'     parallel_stop(bk)
#'
#'     # one pool shared by two stages (max_workers sum == worker count)
#'     bk <- parallel_backend(2)
#'     f <- 1:5 |>
#'         pump(function(x) x * 2, backend = bk, max_workers = 1) |>
#'         pump(function(x) x + 1, backend = bk, max_workers = 1)
#'     pump_run(f, verbose = FALSE)
#'     parallel_stop(bk)
#' }
#' @export
parallel_backend <- function(workers,
                             ...,
                             retries = 3L,
                             retry_sleep = 0) {
    if (!requireNamespace("parallel", quietly = TRUE)) {
        stop(
            "Package 'parallel' is required for parallel_backend() ",
            "but is not installed."
        )
    }

    retries <- as.integer(retries)
    if (length(retries) != 1L || is.na(retries) || retries < 0L) {
        stop("retries must be a non-negative integer.", call. = FALSE)
    }

    if (length(retry_sleep) != 1L || is.na(retry_sleep) || retry_sleep < 0) {
        stop("retry_sleep must be a non-negative number.", call. = FALSE)
    }

    args <- list(...)
    cl <- do.call(parallel::makePSOCKcluster, c(list(names = workers), args))

    state <- new.env(parent = emptyenv())
    state$cl <- cl
    state$workers <- workers
    state$make_args <- args
    state$busy <- rep(FALSE, length(cl))
    state$setup_exprs <- list()
    state$retries <- retries
    state$retry_sleep <- retry_sleep

    structure(list(state = state), class = "pump_parallel_backend")
}

#' Run setup code on all workers of a parallel backend
#'
#' `parallel_setup_workers()` evaluates an expression in the global
#' environment of every worker of a parallel backend. Use it to load
#' packages, source files, or define objects that jobs need. The expression
#' is recorded and replayed automatically on any replacement node created
#' after a worker failure.
#'
#' @details The expression is captured unevaluated. Values from the calling
#'   frame can be injected by wrapping them in double braces, e.g.
#'   `parallel_setup_workers(bk, x <- {{ y }})` assigns the current value of
#'   `y` to `x` on each worker.
#'
#'   Setup can only run while no jobs are active on the backend.
#'
#' @param backend A backend object created by [parallel_backend()].
#' @param expr An expression to evaluate on each worker.
#' @return The backend, invisibly.
#' @seealso [parallel_backend()], [parallel_stop()]
#' @examples
#' if (requireNamespace("parallel", quietly = TRUE)) {
#'     bk <- parallel_backend(2)
#'     parallel_setup_workers(bk, threshold <- 10)
#'     f <- 1:5 |>
#'         pump(function(x) x + threshold, backend = bk)
#'     pump_run(f, verbose = FALSE)
#'     parallel_stop(bk)
#' }
#' @export
parallel_setup_workers <- function(backend, expr) {
    if (!inherits(backend, "pump_parallel_backend")) {
        stop("backend must be a parallel backend.", call. = FALSE)
    }

    state <- backend$state

    if (!length(state$cl)) {
        stop("The parallel backend has no workers.", call. = FALSE)
    }
    if (any(state$busy)) {
        stop(
            "Cannot set up workers while jobs are active.",
            call. = FALSE
        )
    }

    expr <- .parallel_expr_inject(substitute(expr), parent.frame())
    .parallel_setup_cluster(state$cl, list(expr))

    state$setup_exprs[[length(state$setup_exprs) + 1L]] <- expr

    invisible(backend)
}

#' Evaluate an expression on all workers of a parallel backend
#'
#' `parallel_eval_workers()` evaluates an expression in the global
#' environment of every worker of a parallel backend and returns the
#' per-worker results, like `parallel::clusterEvalQ()`. Unlike
#' `clusterEvalQ()`, values from the calling frame can be injected by
#' wrapping them in double braces, e.g.
#' `parallel_eval_workers(bk, x + {{ y }})` evaluates `x + <value of y>`
#' on each worker.
#'
#' @details The expression is captured unevaluated and broadcast: it is
#'   dispatched to every worker before any result is collected, so the total
#'   wall time is bounded by the slowest worker rather than by the sum of
#'   all workers.
#'
#'   Unlike [parallel_setup_workers()], the expression is **not** recorded
#'   for replay on replacement nodes created after a worker failure. Use
#'   `parallel_setup_workers()` for state that jobs depend on (packages,
#'   options, objects) so it survives worker recovery; use
#'   `parallel_eval_workers()` for one-shot queries (diagnostics, versions,
#'   process ids) and warm-up work whose loss on a replaced node is
#'   acceptable.
#'
#'   Evaluation can only run while no jobs are active on the backend. If the
#'   expression fails on any worker, an error is raised naming the failing
#'   workers.
#'
#' @param backend A backend object created by [parallel_backend()].
#' @param expr An expression to evaluate on each worker.
#' @return A list with one element per worker, in worker order.
#' @seealso [parallel_setup_workers()], [parallel_backend()]
#' @examples
#' if (requireNamespace("parallel", quietly = TRUE)) {
#'     bk <- parallel_backend(2)
#'     offset <- 40
#'     parallel_eval_workers(bk, 2 + {{ offset }})
#'     parallel_stop(bk)
#' }
#' @export
parallel_eval_workers <- function(backend, expr) {
    if (!inherits(backend, "pump_parallel_backend")) {
        stop("backend must be a parallel backend.", call. = FALSE)
    }

    state <- backend$state

    if (!length(state$cl)) {
        stop("The parallel backend has no workers.", call. = FALSE)
    }
    if (any(state$busy)) {
        stop(
            "Cannot evaluate on workers while jobs are active.",
            call. = FALSE
        )
    }

    expr <- .parallel_expr_inject(substitute(expr), parent.frame())
    results <- .parallel_broadcast_expr(state$cl, expr, collect = TRUE)

    failed <- which(vapply(results, inherits, logical(1), "try-error"))
    if (length(failed)) {
        stop(
            "Error evaluating expression on worker(s) ",
            paste(failed, collapse = ", "), ": ",
            as.character(results[[failed[[1L]]]]),
            call. = FALSE
        )
    }

    results
}

#' Stop a parallel backend
#'
#' `parallel_stop()` shuts down the PSOCK cluster owned by a parallel
#' backend. Call it when the backend is no longer needed to release the
#' worker processes and their socket connections.
#'
#' @details By default, stopping fails if jobs are still active. Set
#'   `force = TRUE` to stop the cluster regardless. Stopping an already
#'   stopped backend is a no-op.
#'
#' @param backend A backend object created by [parallel_backend()].
#' @param force If `TRUE`, stop the cluster even if jobs are active.
#' @return The backend, invisibly.
#' @seealso [parallel_backend()]
#' @examples
#' if (requireNamespace("parallel", quietly = TRUE)) {
#'     bk <- parallel_backend(2)
#'     parallel_stop(bk)
#' }
#' @export
parallel_stop <- function(backend, force = FALSE) {
    if (!inherits(backend, "pump_parallel_backend")) {
        stop("backend must be a parallel backend.", call. = FALSE)
    }

    state <- backend$state

    if (!length(state$cl)) {
        return(invisible(backend))
    }

    if (!force && any(state$busy)) {
        stop("Cannot stop a parallel backend with active jobs.", call. = FALSE)
    }

    tryCatch(
        parallel::stopCluster(state$cl),
        finally = {
            state$cl <- NULL
            state$busy <- logical()
        }
    )

    invisible(backend)
}

# --- .parallel_* S3 implementation ---

.parallel_expr_inject <- function(expr, env) {
    if (is.call(expr)) {
        if (identical(expr[[1L]], as.name("{")) &&
            length(expr) == 2L &&
            is.call(expr[[2L]]) &&
            identical(expr[[2L]][[1L]], as.name("{")) &&
            length(expr[[2L]]) == 2L) {
            return(eval(expr[[2L]][[2L]], envir = env))
        }

        for (i in seq_along(expr)) {
            expr[[i]] <- .parallel_expr_inject(expr[[i]], env)
        }
    }

    expr
}
.parallel_setup_worker <- function(node, exprs) {
    if (!length(exprs)) {
        return(invisible(NULL))
    }

    eval_expr <- function(expr) {
        eval(expr, envir = globalenv())
        invisible(NULL)
    }
    environment(eval_expr) <- globalenv()

    send_call <- get("sendCall", envir = asNamespace("parallel"))
    recv_result <- get("recvResult", envir = asNamespace("parallel"))

    for (expr in exprs) {
        send_call(
            con = node,
            fun = eval_expr,
            args = list(expr)
        )
        res <- recv_result(node)

        if (inherits(res, "try-error")) {
            stop(
                "Failed to set up parallel worker: ",
                as.character(res),
                call. = FALSE
            )
        }
    }

    invisible(NULL)
}
# Evaluates one expression on every node of a cluster. The expression is
# dispatched to all nodes before any result is collected, so total wall time
# is bounded by the slowest node, not by the sum. With collect = FALSE the
# worker discards the value (returns NULL) to avoid serializing setup
# results back. Returns a list of per-node results; worker-side failures
# appear as "try-error" elements and are left to the caller to handle.
.parallel_broadcast_expr <- function(cl, expr, collect = TRUE) {
    if (!length(cl)) {
        return(list())
    }

    eval_expr <- if (collect) {
        function(expr) {
            eval(expr, envir = globalenv())
        }
    } else {
        function(expr) {
            eval(expr, envir = globalenv())
            invisible(NULL)
        }
    }
    environment(eval_expr) <- globalenv()

    send_call <- get("sendCall", envir = asNamespace("parallel"))
    recv_result <- get("recvResult", envir = asNamespace("parallel"))

    for (node in cl) {
        send_call(
            con = node,
            fun = eval_expr,
            args = list(expr)
        )
    }

    lapply(cl, recv_result)
}
.parallel_setup_cluster <- function(cl, exprs) {
    # Broadcast each expression across all nodes before moving to the next
    # one: nodes run in parallel, while the per-node ordering of setup
    # expressions is preserved.
    for (expr in exprs) {
        results <- .parallel_broadcast_expr(cl, expr, collect = FALSE)
        for (res in results) {
            if (inherits(res, "try-error")) {
                stop(
                    "Failed to set up parallel worker: ",
                    as.character(res),
                    call. = FALSE
                )
            }
        }
    }

    invisible(NULL)
}
.parallel_worker_spec <- function(backend, worker_id) {
    state <- backend$state
    if (is.numeric(state$workers) && length(state$workers) == 1L) {
        return(1L)
    }

    state$workers[[worker_id]]
}
.parallel_create_node <- function(backend, worker_id) {
    worker_spec <- .parallel_worker_spec(backend, worker_id)

    cl <- do.call(
        parallel::makePSOCKcluster,
        c(list(names = worker_spec), backend$state$make_args)
    )

    cl[[1L]]
}
.parallel_job_call <- function(fn, args) {
    make_job <- .make_job
    environment(make_job) <- globalenv()

    wrapper <- function(make_job, fn, args) {
        make_job(do.call(fn, args))
    }
    environment(wrapper) <- globalenv()

    list(
        fn = wrapper,
        args = list(
            make_job = make_job,
            fn = fn,
            args = args
        )
    )
}
.parallel_send_job <- function(backend, worker_id, fn, args) {
    state <- backend$state
    node <- state$cl[[worker_id]]

    send_call <- get("sendCall", envir = asNamespace("parallel"))
    call <- .parallel_job_call(fn, args)
    send_call(
        node,
        fun = call$fn,
        args = call$args
    )

    invisible(NULL)
}
.parallel_submit_job <- function(backend, fn, args) {
    state <- backend$state
    if (!length(state$cl)) {
        stop("The parallel backend has been stopped.", call. = FALSE)
    }
    worker_id <- which(!state$busy)[1]

    if (is.na(worker_id)) {
        stop(
            "parallel backend has no free cluster node. ",
            "This is an internal invariant violation.",
            call. = FALSE
        )
    }

    state$busy[worker_id] <- TRUE

    sent <- tryCatch(
        {
            .parallel_send_job(
                backend = backend,
                worker_id = worker_id,
                fn = fn,
                args = args
            )
            list(ok = TRUE)
        },
        error = function(e) {
            list(ok = FALSE, cause = e)
        }
    )

    attempt <- 0L
    repeat {
        if (sent$ok || attempt >= state$retries) {
            break
        }

        attempt <- attempt + 1L

        if (state$retry_sleep > 0) {
            Sys.sleep(state$retry_sleep)
        }

        sent <- .parallel_recover_and_send(
            backend = backend,
            worker_id = worker_id,
            fn = fn,
            args = args
        )
    }

    if (!sent$ok) {
        state$busy[worker_id] <- FALSE
        stop(
            "Error while sending a job: ", conditionMessage(sent$cause),
            call. = FALSE
        )
    }

    worker_id
}
# Replaces the node and resends a job in one step. Returns list(ok = TRUE)
# on success or list(ok = FALSE, cause = <condition>) on failure.
.parallel_recover_and_send <- function(backend, worker_id, fn, args) {
    tryCatch(
        {
            .parallel_recover_worker(
                backend = backend,
                worker_id = worker_id
            )
            .parallel_send_job(
                backend = backend,
                worker_id = worker_id,
                fn = fn,
                args = args
            )
            list(ok = TRUE)
        },
        error = function(e) {
            list(ok = FALSE, cause = e)
        }
    )
}
.parallel_recover_worker <- function(backend, worker_id) {
    state <- backend$state
    old_node <- state$cl[[worker_id]]

    try(close(old_node$con), silent = TRUE)

    # Setup worker
    node <- .parallel_create_node(backend, worker_id)
    tryCatch(
        {
            .parallel_setup_worker(node, state$setup_exprs)
            state$cl[[worker_id]] <- node
        },
        error = function(e) {
            try(parallel::closeNode(node), silent = TRUE)
            stop("Error setting up worker: ", e$message, call. = FALSE)
        }
    )
}
.parallel_retry_job <- function(job, cause) {
    job_state <- job$state
    state <- job_state$backend$state
    worker_id <- job_state$worker_id

    repeat {
        if (job_state$retries >= state$retries) {
            break
        }

        job_state$retries <- job_state$retries + 1L

        if (state$retry_sleep > 0) {
            Sys.sleep(state$retry_sleep)
        }

        sent <- .parallel_recover_and_send(
            backend = job_state$backend,
            worker_id = worker_id,
            fn = job_state$fn,
            args = job_state$args
        )

        # a successful resend follows the 'is_ready' contract: not ready yet
        if (sent$ok) {
            return(FALSE)
        }

        cause <- sent$cause
    }

    restored <- tryCatch(
        {
            .parallel_recover_worker(
                backend = job_state$backend,
                worker_id = worker_id
            )
            TRUE
        },
        error = function(e) {
            cause <<- e
            FALSE
        }
    )

    if (!restored) {
        stop(
            "Parallel worker could not be recovered after job failure: ",
            conditionMessage(cause),
            call. = FALSE
        )
    }

    state$busy[worker_id] <- FALSE

    err <- simpleError(
        paste0(
            "Parallel job failed after ",
            job_state$retries,
            " retries: ",
            conditionMessage(cause)
        )
    )
    class(err) <- c("pump_error", class(err))

    job_state$result <- list(value = err, fn_time = 0)
    job_state$done <- TRUE

    TRUE
}

# --- .pump_* S3 implementation ---

#' @export
.pump_executor_count.pump_parallel_backend <- function(backend) {
    length(backend$state$cl)
}
#' @export
.pump_executor_new_job.pump_parallel_backend <- function(backend, func, args) {
    worker_id <- .parallel_submit_job(
        backend = backend,
        fn = func,
        args = args
    )

    job_state <- new.env(parent = emptyenv())
    job_state$backend <- backend
    job_state$worker_id <- worker_id
    job_state$fn <- func
    job_state$args <- args
    job_state$retries <- 0L
    job_state$done <- FALSE
    job_state$result <- NULL

    structure(
        list(state = job_state),
        class = "pump_parallel_job"
    )
}
#' @export
.pump_job_is_ready.pump_parallel_job <- function(job) {
    if (job$state$done) {
        return(TRUE)
    }

    state <- job$state$backend$state
    worker_id <- job$state$worker_id
    node <- state$cl[[worker_id]]

    ready <- tryCatch(
        list(
            ok = TRUE,
            value = socketSelect(list(node$con), write = FALSE, timeout = 0)
        ),
        error = function(e) {
            list(ok = FALSE, error = e)
        }
    )

    # An error in worker connection occurred, retry job
    if (!ready$ok) {
        return(.parallel_retry_job(job = job, cause = ready$error))
    }

    # Not ready
    if (!ready$value) {
        return(FALSE)
    }

    recv_result <- get("recvResult", envir = asNamespace("parallel"))
    received <- tryCatch(
        list(ok = TRUE, value = recv_result(node)),
        error = function(e) {
            list(ok = FALSE, error = e)
        }
    )

    # An error in worker connection occurred, retry job
    if (!received$ok) {
        return(.parallel_retry_job(job = job, cause = received$error))
    }

    res <- received$value
    state$busy[worker_id] <- FALSE

    if (inherits(res, "try-error")) {
        err <- simpleError(as.character(res))
        class(err) <- c("pump_error", class(err))
        res <- list(value = err, fn_time = 0)
    } else if (inherits(res, "pump_error")) {
        res <- list(value = res, fn_time = 0)
    }

    job$state$result <- res
    job$state$done <- TRUE

    TRUE
}
#' @export
.pump_job_data.pump_parallel_job <- function(job) {
    if (.pump_job_is_ready(job)) job$state$result
}

#' Print a parallel backend
#'
#' @param x A parallel backend object.
#' @param ... Unused.
#' @return The input `x`, invisibly.
#' @export
print.pump_parallel_backend <- function(x, ...) {
    cat("<pump_parallel_backend>\n")
    cat("  workers: ", .pump_executor_count(x), "\n", sep = "")
    invisible(x)
}
