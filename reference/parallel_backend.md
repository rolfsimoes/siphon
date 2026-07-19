# Create a parallel (PSOCK) backend

`parallel_backend()` manages a PSOCK cluster and submits jobs to it.
Jobs are dispatched with a non-blocking send and readiness is polled
with [`socketSelect()`](https://rdrr.io/r/base/socketSelect.html), so
the main process never blocks while jobs run on the cluster nodes.

## Usage

``` r
parallel_backend(
  workers = NULL,
  ...,
  retries = 3L,
  retry_sleep = 0,
  cluster = NULL
)
```

## Arguments

- workers:

  The number of local worker processes to start, or a character vector
  of host names to run workers on (as accepted by
  [`parallel::makePSOCKcluster()`](https://rdrr.io/r/parallel/makeCluster.html)).
  Mutually exclusive with `cluster`.

- ...:

  Additional arguments passed to
  [`parallel::makePSOCKcluster()`](https://rdrr.io/r/parallel/makeCluster.html).
  Only valid with `workers`.

- retries:

  Number of times a job is resubmitted after a worker connection failure
  before the job is marked as failed. A non-negative integer. Ignored
  when `cluster` is supplied.

- retry_sleep:

  Seconds to wait before each retry attempt. A non-negative number.
  Ignored when `cluster` is supplied.

- cluster:

  An existing cluster object (as returned by
  [`parallel::makePSOCKcluster()`](https://rdrr.io/r/parallel/makeCluster.html))
  to attach to instead of creating one. The backend does not take
  ownership: you remain responsible for stopping the cluster. Mutually
  exclusive with `workers`.

## Value

A backend object.

## Details

With `workers`, the backend owns its cluster: `parallel_backend()`
returns a cheap specification and the worker processes are started when
the backend first opens (first pipeline run,
[`parallel_eval_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_eval_workers.md)
call, or stage registration). Shut it down with
[`parallel_stop()`](https://rolfsimoes.github.io/siphon/reference/parallel_stop.md)
when no longer needed.
[`parallel_setup_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_setup_workers.md)
may be called before the cluster exists: expressions are recorded and
replayed at open.

With `cluster`, the backend attaches to an externally managed cluster
instead (for integration with packages that run their own worker pool).
It never creates, replaces, or stops those nodes: worker recovery is
disabled (see the fault tolerance section), `retries`/`retry_sleep` are
ignored, and
[`parallel_stop()`](https://rolfsimoes.github.io/siphon/reference/parallel_stop.md)
refuses - stop the cluster yourself with
[`parallel::stopCluster()`](https://rdrr.io/r/parallel/makeCluster.html).

Each node runs at most one job at a time, so `max_workers` for a stage
using this backend must not exceed the number of workers (enforced by
[`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md)).

Passing the same backend to several
[`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md) stages
shares its cluster between them — this is the normal way to run a
multi-stage pipeline on one worker pool. Node availability is tracked at
the backend level and results are received per node, so concurrent
stages cannot cross-wire. The one sizing rule is that the `max_workers`
of the stages sharing the backend must not sum to more than the number
of workers; since each stage's `max_workers` defaults to the full worker
count, set it explicitly on every sharing stage (e.g. a pool of `n + 1`
workers serving a read stage with `max_workers = n` and a write stage
with `max_workers = 1`). Oversubscribing fails at dispatch time with a
free-node invariant error. Jobs in a shared pool have no node affinity;
to isolate worker pools per stage (different hosts, retry settings, or
worker state), create a separate backend for each stage instead — see
the "Pooling strategies" section of
[`vignette("siphon")`](https://rolfsimoes.github.io/siphon/articles/siphon.md).

This backend uses the unexported `sendCall()` and `recvResult()`
functions from the `parallel` package to communicate with cluster nodes.

When a stage first advances, its function and constant arguments are
installed once on every node as a per-stage runner; each job then ships
only the item data. Runner installations are recorded alongside
[`parallel_setup_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_setup_workers.md)
expressions, replayed on replacement nodes after a worker failure, and
uninstalled when the pipeline closes, so long-lived pools do not
accumulate one runner per stage per run. Stage functions must be
self-contained or carry their dependencies in their closure environment;
objects they reference from the global environment are not shipped.

## Fault tolerance

The backend is fault tolerant to worker process failures: if a worker
connection fails while a job is running or while a job is being
dispatched (e.g. the node died while idle), the backend replaces the
node with a fresh one and resubmits the job, up to `retries` times,
sleeping `retry_sleep` seconds before each attempt. Setup expressions
registered with
[`parallel_setup_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_setup_workers.md)
are replayed on replacement nodes. When retries are exhausted, a job
that failed while running yields an error value (a `pump_error`
condition) instead of failing the pipeline, and the node is restored for
subsequent jobs; a job that could not be dispatched at all raises an
error.

Because failed jobs are resubmitted, execution follows at-least-once
semantics: a job's side effects (file writes, database inserts, API
calls) may run more than once if a worker dies after the effect but
before the result is delivered. Job functions should be idempotent.

The following failures are **not** handled: hung workers (failure
detection is connection-based, so a worker that stalls without dying is
never detected and there is no job timeout) and failures of the main R
process (there is no persistence or checkpointing; in-flight work is
lost).

When a run aborts (an error escapes
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)/[`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md)),
the backend is quiesced: pending results of in-flight jobs are drained
and discarded, bounded by `options(siphon.quiesce_timeout =)` (default
30 seconds), so they cannot be read as the results of a later submission
on the same nodes. A node that cannot be drained in time is replaced on
an owned pool and left quarantined (busy) on an attached cluster; use
[`parallel_busy()`](https://rolfsimoes.github.io/siphon/reference/parallel_workers.md)
to locate quarantined nodes.

On a backend created with `cluster`, none of the above recovery applies:
a worker connection failure surfaces as a `pump_error` value for the
affected item (subject to the `on_error` policy) and the dead node is
quarantined - no further jobs are dispatched to it, so capacity shrinks
for the rest of the run.

## See also

[`parallel_setup_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_setup_workers.md),
[`parallel_stop()`](https://rolfsimoes.github.io/siphon/reference/parallel_stop.md),
[`parallel_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_workers.md)

## Examples

``` r
if (requireNamespace("parallel", quietly = TRUE)) {
    bk <- parallel_backend(2)
    f <- 1:5 |>
        pump(function(x) x * 2, backend = bk)
    pump_run(f, verbose = FALSE)
    parallel_stop(bk)

    # one pool shared by two stages (max_workers sum == worker count)
    bk <- parallel_backend(2)
    f <- 1:5 |>
        pump(function(x) x * 2, backend = bk, max_workers = 1) |>
        pump(function(x) x + 1, backend = bk, max_workers = 1)
    pump_run(f, verbose = FALSE)
    parallel_stop(bk)

    # attach to a cluster you manage yourself (no ownership taken)
    cl <- parallel::makePSOCKcluster(2)
    bk <- parallel_backend(cluster = cl)
    f <- 1:5 |> pump(function(x) x + 1, backend = bk)
    pump_run(f, verbose = FALSE)
    parallel::stopCluster(cl)
}
```
