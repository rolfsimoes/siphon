# Asynchronous Daemons with siphon and liteq

## Introduction

In modern web applications built with frameworks like **Shiny** or
**Plumber**, running long-running or resource-intensive computations
directly in the main request handler blocks the user interface thread.
This leads to high latency and a poor user experience.

A robust production architecture separates concerns:

1.  **The Web Server (Shiny/Plumber)**: Quickly accepts user requests,
    writes jobs to a persistent queue (like `liteq`), and queries a
    database asynchronously to display results.
2.  **The Worker Daemon**: A background R process that runs
    indefinitely, consumes tasks from the queue, processes them in
    parallel with controlled concurrency via `siphon`, and saves results
    back to the database.

This vignette demonstrates how to implement this background daemon
pattern using `siphon` and `liteq`.

It builds on the core concepts -
[`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md),
[`pump_source()`](https://rolfsimoes.github.io/siphon/reference/pump_source.md),
and
[`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md) -
introduced in [Pull-Based Staged Pipelines with
siphon](https://rolfsimoes.github.io/siphon/articles/siphon.md)
([`vignette("siphon", package = "siphon")`](https://rolfsimoes.github.io/siphon/articles/siphon.md)).
The examples use the `liteq`, `mirai`, and `jsonlite` packages; install
them to run the code below.

## Why `liteq`?

The `liteq` package provides a serverless, database-backed queue using
SQLite. It is concurrent-safe, supports lock-free message consumption,
and ensures that messages are either permanently acknowledged (`ack`) or
returned to the queue on failure (`nack`), providing transaction-like
reliability.

## Architecture

![A web front end enqueues jobs; a siphon daemon consumes them,
processes in parallel, and persists
results.](daemons_files/figure-html/daemon-diagram-1.png)

A web front end enqueues jobs; a siphon daemon consumes them, processes
in parallel, and persists results.

## Step 1: A custom queue source

We wrap `liteq` consumption in
[`pump_source()`](https://rolfsimoes.github.io/siphon/reference/pump_source.md).
Two design points are essential:

- **Keep the message on the main process.** A `liteq` message holds an
  open lock connection and must *not* be serialized to a parallel
  worker. We store each message in a main-process registry keyed by `id`
  and send only its (JSON) payload downstream. Workers compute on plain
  data; `ack()`/`nack()` happen later on the main process.
- **Release resources with `close_fn`.**
  [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
  and
  [`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md)
  call it automatically when the pipeline finishes.

A daemon runs forever (`done_fn = function() FALSE`); for the runnable
demo in Step 2 we stop when the queue drains. The factory below supports
both:

``` r

library(siphon)

make_liteq_source <- function(queue, registry, run_forever = TRUE) {
    pump_source(
        pull_fn = function() {
            msg <- liteq::try_consume(queue)   # a liteq_message, or NULL if empty
            if (is.null(msg)) return(NULL)

            # Keep the live message on the main process so we can ack/nack it later
            assign(as.character(msg$id), msg, envir = registry)

            payload <- tryCatch(
                jsonlite::fromJSON(msg$message),
                error = function(e) e
            )
            list(
                id = msg$id,
                data = payload,                # only serializable data goes downstream
                ok = !inherits(payload, "error")
            )
        },
        done_fn = if (run_forever) function() FALSE else function() liteq::is_empty(queue),
        close_fn = function() invisible(NULL)  # close DB handles / connections here
    )
}
```

## Step 2: A runnable end-to-end example

Let’s exercise the source against a temporary queue. We publish three
jobs, process them in parallel, save the results, and acknowledge each
message. This example runs if `liteq`, `mirai`, and `jsonlite` are
installed:

``` r

# 1. Create a temporary queue and publish 3 jobs
db <- tempfile(fileext = ".sqlite")
q <- liteq::ensure_queue("jobs", db = db)
for (x in 1:3) {
    liteq::publish(q, title = paste0("job-", x),
                   message = jsonlite::toJSON(list(x = x), auto_unbox = TRUE))
}

# 2. Build the pipeline: process payloads on 2 parallel workers
registry <- new.env(parent = emptyenv())
mirai::daemons(2)

results <- list()
pipeline <- make_liteq_source(q, registry, run_forever = FALSE) |>
    pump(function(payload) payload$x * 10,
         backend = mirai_backend(), max_workers = 2)

# 3. Drain: save successes and ack/nack on the main process via the registry
pump_drain(pipeline, verbose = FALSE, handle_fn = function(id, data, ok) {
    msg <- get(as.character(id), envir = registry)
    if (ok && !inherits(data, "error")) {
        results[[as.character(id)]] <<- data
        liteq::ack(msg)    # success: remove from the queue
    } else {
        liteq::nack(msg)   # failure: return to the queue for retry
    }
})

mirai::daemons(0)

str(results)
```

Each job’s payload `x` is multiplied by 10. The `liteq` message never
leaves the main process - only the numeric payload is sent to a worker -
and `ack()`/`nack()` are issued from the registry once the result comes
back.

## Step 3: The production daemon

A real daemon never stops on its own. Use
`make_liteq_source(..., run_forever = TRUE)` (the default) and wrap the
drain in [`on.exit()`](https://rdrr.io/r/base/on.exit.html) so workers
always shut down on interrupt or error. Because
[`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md)
streams each item and frees its memory, the process stays bounded even
over millions of jobs:

``` r

run_daemon <- function(db_path, queue_name = "jobs", n_workers = 4) {
    q <- liteq::ensure_queue(queue_name, db = db_path)
    registry <- new.env(parent = emptyenv())

    save_result <- function(task_id, result) {
        # Persist to your database here
        message("Task ", task_id, " -> ", result)
    }

    mirai::daemons(n_workers)
    on.exit(mirai::daemons(0), add = TRUE)   # always release workers

    pipeline <- make_liteq_source(q, registry) |>
        pump(function(payload) payload$x * 10,
             backend = mirai_backend(), max_workers = n_workers, buffer_size = n_workers)

    message("Daemon started. Polling queue: ", queue_name)
    pump_drain(
        pipeline,
        handle_fn = function(id, data, ok) {
            msg <- get(as.character(id), envir = registry)
            if (ok && !inherits(data, "error")) {
                save_result(id, data)
                liteq::ack(msg)
            } else {
                liteq::nack(msg)
            }
        },
        sleep_ms = 100   # backoff when the queue is empty
    )
}
```

**Stopping the daemon.** Because the loop is infinite, stop it with an
interrupt (`Ctrl-C` / `SIGINT`); the
[`on.exit()`](https://rdrr.io/r/base/on.exit.html) hook then shuts the
`mirai` workers down cleanly. For programmatic shutdown, publish a
sentinel “poison-pill” message whose handler flips a flag that your
`done_fn` checks.

## Advantages of this architecture

1.  **Bounded concurrency and memory.** `max_workers` caps how many jobs
    run at once, protecting memory and database connections. Adding
    `buffer_size` applies buffer backpressure so the daemon stops
    pulling from the queue when result persistence is the bottleneck
    (see the backpressure section of the [main
    vignette](https://rolfsimoes.github.io/siphon/articles/siphon.md)).
2.  **Crash resilience.** A message is removed only after
    `liteq::ack()`. If the daemon crashes mid-task, the un-acked message
    is released by SQLite’s lock mechanism and another worker retries
    it.
3.  **Responsive UI.** A Shiny or Plumber front end only calls
    `liteq::publish()` to enqueue work, which returns instantly. It can
    then poll the database asynchronously (e.g. with a `later` loop or
    reactive timer) to check when the job’s result has been saved.
