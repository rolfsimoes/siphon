#' Create a parallel (PSOCK) backend
#'
#' `parallel_backend()` manages a PSOCK cluster and submits jobs to it. Jobs
#' are dispatched with a non-blocking send and readiness is polled with
#' `socketSelect()`, so the main process never blocks while jobs run on the
#' cluster nodes.
#'
#' @details With `workers`, the backend owns its cluster: `parallel_backend()`
#'   returns a cheap specification and the worker processes are started when
#'   the backend first opens (first pipeline run, [parallel_eval_workers()]
#'   call, or stage registration). Shut it down with [parallel_stop()] when
#'   no longer needed. [parallel_setup_workers()] may be called before the
#'   cluster exists: expressions are recorded and replayed at open.
#'
#'   With `cluster`, the backend attaches to an externally managed cluster
#'   instead (for integration with packages that run their own worker pool).
#'   It never creates, replaces, or stops those nodes: worker recovery is
#'   disabled (see the fault tolerance section), `retries`/`retry_sleep` are
#'   ignored, and [parallel_stop()] refuses - stop the cluster yourself with
#'   `parallel::stopCluster()`.
#'
#'   Each node runs at most one job at a time, so `max_workers` for a stage
#'   using this backend must not exceed the number of workers (enforced by
#'   `pump()`).
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
#'   When a stage first advances, its function and constant arguments are
#'   installed once on every node as a per-stage runner; each job then
#'   ships only the item data. Runner installations are recorded alongside
#'   [parallel_setup_workers()] expressions and replayed on replacement
#'   nodes after a worker failure. Stage functions must be self-contained
#'   or carry their dependencies in their closure environment; objects they
#'   reference from the global environment are not shipped.
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
#'   On a backend created with `cluster`, none of the above recovery
#'   applies: a worker connection failure surfaces as a `pump_error` value
#'   for the affected item (subject to the `on_error` policy) and the dead
#'   node is quarantined - no further jobs are dispatched to it, so
#'   capacity shrinks for the rest of the run.
#'
#' @param workers The number of local worker processes to start, or a
#'   character vector of host names to run workers on (as accepted by
#'   `parallel::makePSOCKcluster()`). Mutually exclusive with `cluster`.
#' @param ... Additional arguments passed to
#'   `parallel::makePSOCKcluster()`. Only valid with `workers`.
#' @param retries Number of times a job is resubmitted after a worker
#'   connection failure before the job is marked as failed. A non-negative
#'   integer. Ignored when `cluster` is supplied.
#' @param retry_sleep Seconds to wait before each retry attempt. A
#'   non-negative number. Ignored when `cluster` is supplied.
#' @param cluster An existing cluster object (as returned by
#'   `parallel::makePSOCKcluster()`) to attach to instead of creating one.
#'   The backend does not take ownership: you remain responsible for
#'   stopping the cluster. Mutually exclusive with `workers`.
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
#'
#'     # attach to a cluster you manage yourself (no ownership taken)
#'     cl <- parallel::makePSOCKcluster(2)
#'     bk <- parallel_backend(cluster = cl)
#'     f <- 1:5 |> pump(function(x) x + 1, backend = bk)
#'     pump_run(f, verbose = FALSE)
#'     parallel::stopCluster(cl)
#' }
#' @export
parallel_backend <- function(workers = NULL,
                             ...,
                             retries = 3L,
                             retry_sleep = 0,
                             cluster = NULL) {
    .pump_need_pkg("parallel", "parallel_backend()")

    retries <- as.integer(retries)
    if (length(retries) != 1L || is.na(retries) || retries < 0L) {
        stop("retries must be a non-negative integer.", call. = FALSE)
    }

    if (length(retry_sleep) != 1L || is.na(retry_sleep) || retry_sleep < 0) {
        stop("retry_sleep must be a non-negative number.", call. = FALSE)
    }

    if (is.null(workers) == is.null(cluster)) {
        stop("Supply exactly one of workers or cluster.", call. = FALSE)
    }

    args <- list(...)

    state <- new.env(parent = emptyenv())
    state$make_args <- args
    state$setup_exprs <- list()
    state$retries <- retries
    state$retry_sleep <- retry_sleep
    state$stopped <- FALSE

    # Adapter mode: wrap an externally managed cluster. The backend never
    # creates, replaces, or stops its nodes (no worker recovery).
    if (!is.null(cluster)) {
        if (!inherits(cluster, "cluster")) {
            stop(
                "cluster must be a cluster object from the parallel package.",
                call. = FALSE
            )
        }
        if (length(args)) {
            stop(
                "... arguments cannot be combined with cluster.",
                call. = FALSE
            )
        }
        state$cl <- cluster
        state$workers <- length(cluster)
        state$busy <- rep(FALSE, length(cluster))
        return(structure(
            list(
                name = "parallel",
                owned = FALSE,
                note = "cluster: attached, not owned (no worker recovery)",
                state = state
            ),
            class = c("pump_parallel_backend", "pump_backend")
        ))
    }

    valid_count <- is.numeric(workers) && length(workers) == 1L &&
        !is.na(workers) && workers >= 1
    valid_hosts <- is.character(workers) && length(workers) >= 1L &&
        !anyNA(workers)
    if (!valid_count && !valid_hosts) {
        stop(
            "workers must be a positive number or a character vector ",
            "of host names.",
            call. = FALSE
        )
    }

    # Owned mode is a specification: the cluster is created when the
    # backend opens (first pipeline run or worker operation).
    state$cl <- NULL
    state$workers <- workers
    state$busy <- logical()

    structure(
        list(name = "parallel", owned = TRUE, state = state),
        class = c("pump_parallel_backend", "pump_backend")
    )
}

#' @export
.pump_backend_open.pump_parallel_backend <- function(backend) {
    state <- backend$state
    if (state$stopped) {
        stop("The parallel backend has been stopped.", call. = FALSE)
    }
    if (!is.null(state$cl)) {
        return(invisible(backend))
    }

    cl <- do.call(
        parallel::makePSOCKcluster,
        c(list(names = state$workers), state$make_args)
    )

    # Replay setup expressions queued before the cluster existed; do not
    # leak worker processes if a setup expression fails.
    if (length(state$setup_exprs)) {
        tryCatch(
            .parallel_setup_cluster(cl, state$setup_exprs),
            error = function(e) {
                try(parallel::stopCluster(cl), silent = TRUE)
                stop(e)
            }
        )
    }

    state$cl <- cl
    state$busy <- rep(FALSE, length(cl))
    invisible(backend)
}

#' @export
.pump_backend_close.pump_parallel_backend <- function(backend,
                                                      force = FALSE,
                                                      ...) {
    state <- backend$state

    # Never stop a cluster the backend does not own.
    if (!isTRUE(backend$owned)) {
        return(invisible(backend))
    }
    if (state$stopped) {
        return(invisible(backend))
    }
    if (!is.null(state$cl) && !force && any(state$busy)) {
        stop("Cannot stop a parallel backend with active jobs.", call. = FALSE)
    }

    state$stopped <- TRUE
    if (!is.null(state$cl)) {
        tryCatch(
            parallel::stopCluster(state$cl),
            finally = {
                state$cl <- NULL
                state$busy <- logical()
            }
        )
    } else {
        state$busy <- logical()
    }

    invisible(backend)
}

#' @export
.pump_backend_quiesce.pump_parallel_backend <- function(backend) {
    state <- backend$state
    if (is.null(state$cl) || !any(state$busy)) {
        return(invisible(backend))
    }

    recv_result <- get("recvResult", envir = asNamespace("parallel"))
    deadline <- .pump_now_ms() + .pump_quiesce_timeout() * 1000

    for (worker_id in which(state$busy)) {
        node <- state$cl[[worker_id]]
        budget <- max(0, (deadline - .pump_now_ms()) / 1000)
        # Drain the pending result and discard it. drained is FALSE when
        # the job is still running at the deadline, NA when the node is
        # dead (connection error).
        drained <- tryCatch(
            {
                if (socketSelect(list(node$con), write = FALSE,
                                 timeout = budget)) {
                    recv_result(node)
                    TRUE
                } else {
                    FALSE
                }
            },
            error = function(e) NA
        )
        if (isTRUE(drained)) {
            state$busy[worker_id] <- FALSE
        } else if (isTRUE(backend$owned)) {
            # A timed-out or dead node of an owned pool is replaced so the
            # backend always comes out of an abort at full capacity.
            tryCatch(
                {
                    .parallel_recover_worker(backend, worker_id)
                    state$busy[worker_id] <- FALSE
                },
                error = function(e) NULL
            )
        }
        # On an attached cluster an undrained node stays busy
        # (quarantined): siphon must not replace nodes it does not own.
        # The owner can find it with parallel_busy() and repair it.
    }

    invisible(backend)
}

#' Run setup code on all workers of a parallel backend
#'
#' `parallel_setup_workers()` evaluates an expression in the global
#' environment of every worker of a parallel backend. Use it to load
#' packages, source files, or define objects that jobs need. The expression
#' is recorded and replayed automatically on any replacement node created
#' after a worker failure. It may be called before the backend's cluster
#' exists: the expression is queued and replayed when the backend opens.
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

    if (state$stopped) {
        stop("The parallel backend has been stopped.", call. = FALSE)
    }
    if (any(state$busy)) {
        stop(
            "Cannot set up workers while jobs are active.",
            call. = FALSE
        )
    }

    expr <- .parallel_expr_inject(substitute(expr), parent.frame())

    # Declarative: run now if the cluster is live, otherwise the recorded
    # expression is replayed when the backend opens. Either way it is also
    # replayed on replacement nodes after a worker failure.
    if (!is.null(state$cl)) {
        .parallel_setup_cluster(state$cl, list(expr))
    }

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

    if (any(state$busy)) {
        stop(
            "Cannot evaluate on workers while jobs are active.",
            call. = FALSE
        )
    }

    # Evaluation needs live workers: open the backend if necessary.
    .pump_backend_open(backend)

    if (!length(state$cl)) {
        stop("The parallel backend has no workers.", call. = FALSE)
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
#'   stopped backend (or one whose cluster was never started) is a no-op.
#'   Stopping a backend created with `parallel_backend(cluster = )` is an
#'   error: the backend does not own that cluster.
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
    if (!isTRUE(backend$owned)) {
        stop(
            "The parallel backend does not own its cluster; stop the ",
            "cluster yourself with parallel::stopCluster().",
            call. = FALSE
        )
    }

    .pump_backend_close(backend, force = force)

    invisible(backend)
}

#' Inspect the workers of a parallel backend
#'
#' `parallel_workers()` returns the number of worker processes of a
#' parallel backend. `parallel_busy()` returns a logical vector with one
#' element per worker: `TRUE` for workers currently holding an in-flight
#' job. Owners of attached clusters (see `parallel_backend(cluster =)`)
#' can use `parallel_busy()` after a failed run to find nodes that were
#' quarantined (left busy) and repair them before reusing the cluster
#' elsewhere.
#'
#' @details A backend created with `workers` is a specification until its
#'   first use: `parallel_workers()` then reports the configured capacity
#'   and `parallel_busy()` returns a zero-length vector. On a stopped
#'   backend, `parallel_workers()` returns 0 and `parallel_busy()` a
#'   zero-length vector.
#'
#' @param backend A backend object created by [parallel_backend()].
#' @return `parallel_workers()`: an integer count. `parallel_busy()`: a
#'   logical vector with one element per live worker.
#' @seealso [parallel_backend()], [parallel_stop()]
#' @examples
#' if (requireNamespace("parallel", quietly = TRUE)) {
#'     bk <- parallel_backend(2)
#'     parallel_workers(bk)
#'     parallel_busy(bk)
#'     parallel_stop(bk)
#' }
#' @export
parallel_workers <- function(backend) {
    if (!inherits(backend, "pump_parallel_backend")) {
        stop("backend must be a parallel backend.", call. = FALSE)
    }
    .pump_executor_count(backend)
}

#' @rdname parallel_workers
#' @export
parallel_busy <- function(backend) {
    if (!inherits(backend, "pump_parallel_backend")) {
        stop("backend must be a parallel backend.", call. = FALSE)
    }
    backend$state$busy
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
# Ships one job to a node: the stage runner is already installed in the
# node's global environment under `key` (see the register method), so the
# per-job payload is just the key, the item data, and a tiny fetch-and-run
# shim.
.parallel_send_job <- function(backend, worker_id, key, data) {
    state <- backend$state
    node <- state$cl[[worker_id]]

    send_call <- get("sendCall", envir = asNamespace("parallel"))
    exec <- function(key, data) get(key, envir = globalenv())(data)
    environment(exec) <- globalenv()
    send_call(
        node,
        fun = exec,
        args = list(key = key, data = data)
    )

    invisible(NULL)
}
.parallel_submit_job <- function(backend, key, data) {
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
                key = key,
                data = data
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
            key = key,
            data = data
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
# Replaces the node and resends a job in one step. Node replacement replays
# setup_exprs, which reinstalls every registered stage runner, so the
# resent key still resolves. Returns list(ok = TRUE) on success or
# list(ok = FALSE, cause = <condition>) on failure.
.parallel_recover_and_send <- function(backend, worker_id, key, data) {
    tryCatch(
        {
            .parallel_recover_worker(
                backend = backend,
                worker_id = worker_id
            )
            .parallel_send_job(
                backend = backend,
                worker_id = worker_id,
                key = key,
                data = data
            )
            list(ok = TRUE)
        },
        error = function(e) {
            list(ok = FALSE, cause = e)
        }
    )
}
.parallel_recover_worker <- function(backend, worker_id) {
    if (!isTRUE(backend$owned)) {
        stop(
            "Cannot replace a node of a cluster not owned by siphon.",
            call. = FALSE
        )
    }

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
    backend <- job_state$backend
    state <- backend$state
    worker_id <- job_state$worker_id

    # A non-owned cluster offers no recovery: the failure surfaces as an
    # item-level error and the dead node stays marked busy (quarantined),
    # so no further jobs are dispatched to it.
    if (!isTRUE(backend$owned)) {
        job_state$result <- .pump_job_failure(simpleError(paste0(
            "Parallel job failed on a non-owned cluster (no recovery): ",
            conditionMessage(cause)
        )))
        job_state$done <- TRUE
        return(TRUE)
    }

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
            key = job_state$key,
            data = job_state$data
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

    job_state$result <- .pump_job_failure(simpleError(
        paste0(
            "Parallel job failed after ",
            job_state$retries,
            " retries: ",
            conditionMessage(cause)
        )
    ))
    job_state$done <- TRUE

    TRUE
}

# --- .pump_* S3 implementation ---

#' @export
.pump_executor_count.pump_parallel_backend <- function(backend) {
    state <- backend$state
    if (!is.null(state$cl)) {
        return(length(state$cl))
    }
    if (state$stopped) {
        return(0L)
    }
    # not yet opened: capacity is known from the specification
    if (is.numeric(state$workers) && length(state$workers) == 1L) {
        as.integer(state$workers)
    } else {
        length(state$workers)
    }
}
#' @export
.pump_executor_register.pump_parallel_backend <- function(backend,
                                                          func,
                                                          args) {
    .pump_backend_open(backend)

    state <- backend$state
    if (!length(state$cl)) {
        stop("The parallel backend has been stopped.", call. = FALSE)
    }

    key <- .pump_stage_key()
    make_job <- .make_job
    environment(make_job) <- globalenv()

    runner_env <- new.env(parent = globalenv())
    runner_env$func <- func
    runner_env$args <- args
    runner_env$make_job <- make_job
    runner <- function(data) make_job(do.call(func, c(list(data), args)))
    environment(runner) <- runner_env

    # Installed once per node and recorded in setup_exprs so replacement
    # nodes created after a worker failure replay it.
    install_expr <- bquote(assign(.(key), .(runner), envir = globalenv()))
    .parallel_setup_cluster(state$cl, list(install_expr))
    state$setup_exprs[[length(state$setup_exprs) + 1L]] <- install_expr

    list(key = key)
}
#' @export
.pump_executor_unregister.pump_parallel_backend <- function(backend,
                                                            handle) {
    state <- backend$state
    key <- handle$key

    # Forget the recorded install expression first, so replacement nodes
    # and future opens of an owned specification never replay it.
    is_install <- vapply(
        state$setup_exprs,
        function(e) {
            is.call(e) && identical(e[[1L]], as.name("assign")) &&
                identical(e[[2L]], key)
        },
        logical(1L)
    )
    state$setup_exprs <- state$setup_exprs[!is_install]

    if (is.null(state$cl)) {
        return(invisible(backend))
    }

    # Remove the runner from free nodes only: a busy node would answer the
    # removal message only after its in-flight job, blocking collection.
    # Busy nodes here are quarantined leftovers of a failed run; they keep
    # a stale runner until the node is replaced (harmless: keys are never
    # reused within a session).
    free <- state$cl[!state$busy]
    expr <- bquote(
        suppressWarnings(rm(list = .(key), envir = globalenv()))
    )
    try(.parallel_broadcast_expr(free, expr, collect = FALSE), silent = TRUE)

    invisible(backend)
}
#' @export
.pump_executor_new_job.pump_parallel_backend <- function(backend,
                                                         handle,
                                                         data) {
    worker_id <- .parallel_submit_job(
        backend = backend,
        key = handle$key,
        data = data
    )

    job_state <- new.env(parent = emptyenv())
    job_state$backend <- backend
    job_state$worker_id <- worker_id
    job_state$key <- handle$key
    job_state$data <- data
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
        res <- .pump_job_failure(simpleError(as.character(res)))
    } else if (inherits(res, "pump_error")) {
        res <- .pump_job_failure(res)
    }

    job$state$result <- res
    job$state$done <- TRUE

    TRUE
}
#' @export
.pump_job_data.pump_parallel_job <- function(job) {
    if (.pump_job_is_ready(job)) job$state$result
}
