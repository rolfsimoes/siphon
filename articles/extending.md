# Extending siphon with Custom Backends

## The execution contract

siphon drives all of its backends -
[`main_backend()`](https://rolfsimoes.github.io/siphon/reference/main_backend.md),
[`mirai_backend()`](https://rolfsimoes.github.io/siphon/reference/mirai_backend.md),
[`future_backend()`](https://rolfsimoes.github.io/siphon/reference/future_backend.md),
[`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md) -
through one small contract.
[`pump_custom_backend()`](https://rolfsimoes.github.io/siphon/reference/pump_custom_backend.md)
exposes that contract as plain R functions, so any tool that can start a
job and poll for its completion can power a
[`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md) stage:
a job queue, a remote service, or a worker pool that another package
already manages.

| Function | Called | Purpose |
|----|----|----|
| `count()` | at stage sizing | how many jobs can run concurrently |
| `register(func, args)` | once per stage | install per-stage state, return a *handle* (optional) |
| `submit(handle, data)` | once per item | start a job, return a *token*, never block |
| `is_ready(token)` | polled every beat | `TRUE` when the result can be collected without blocking |
| `collect(token)` | once, when ready | the finished value, or a condition for item failure |
| [`open()`](https://rdrr.io/r/base/connections.html) | once, before first use | lazy resource acquisition (optional) |

Two contract details matter in practice:

- **Failure is a returned condition, not a thrown error.** `collect()`
  returning a condition object marks that *item* as failed, and the
  stage’s `on_error` policy decides what happens next. An error thrown
  *by* `collect()` is an infrastructure failure and aborts the pipeline.
- **`register()` is the ship-once hook.** The built-in mirai and
  parallel backends use their equivalent of this step to install the
  stage function on every worker a single time, so each job carries only
  its item data. If your transport has per-job serialization costs, do
  the same here.

## A minimal backend

The smallest possible backend runs the job at submit time and hands the
result straight back:

``` r

library(siphon)

toy_backend <- pump_custom_backend(
    name = "toy",
    count = function() 1L,
    submit = function(handle, data) {
        tryCatch(
            do.call(handle$func, c(list(data), handle$args)),
            error = identity
        )
    },
    is_ready = function(token) TRUE,
    collect = function(token) token
)

1:5 |>
    pump(function(x) x * 2, backend = toy_backend) |>
    pump_run(verbose = FALSE)
#. [[1]]
#. [1] 2
#. 
#. [[2]]
#. [1] 4
#. 
#. [[3]]
#. [1] 6
#. 
#. [[4]]
#. [1] 8
#. 
#. [[5]]
#. [1] 10
```

The backend’s `name` flows into
[`print()`](https://rdrr.io/r/base/print.html) and
[`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md):

``` r

print(toy_backend)
#. <pump_toy_backend>
#.   workers: 1
```

## A simulated asynchronous backend

Real backends are asynchronous: `submit()` starts work and returns
immediately, and readiness arrives later. This simulated version makes
the mechanics visible without any extra dependency - each job
“completes” only after it has been polled twice, so you can watch slots
stay occupied across beats:

``` r

new_fake_async_backend <- function(workers = 2L) {
    pump_custom_backend(
        name = "fakeasync",
        count = function() workers,
        submit = function(handle, data) {
            token <- new.env(parent = emptyenv())
            token$result <- tryCatch(
                do.call(handle$func, c(list(data), handle$args)),
                error = identity
            )
            token$polls <- 0L
            token
        },
        is_ready = function(token) {
            token$polls <- token$polls + 1L
            token$polls >= 2L
        },
        collect = function(token) token$result
    )
}

bk <- new_fake_async_backend(2L)
p <- 1:6 |> pump(function(x) x + 100, backend = bk)
pump_step(p) # first beat: jobs submitted, none ready yet
#. <pump (1)>
#. ┌─ source   2/6
#. ├─ stage 1 fakeasync
#. │    wrk [#####] 2/2   buf [-----] 0/6   done 0   err 0
#. │    fn 0.0ms/it   crd 0.0ms/bt   wrk 100% stv 0% blk 0%
#. └─ sink   0/6
p
#. <pump (1)>
#. ┌─ source   2/6
#. ├─ stage 1 fakeasync
#. │    wrk [#####] 2/2   buf [-----] 0/6   done 0   err 0
#. │    fn 0.0ms/it   crd 0.0ms/bt   wrk 100% stv 0% blk 0%
#. └─ sink   0/6
pump_run(p, verbose = FALSE)
#. [[1]]
#. [1] 101
#. 
#. [[2]]
#. [1] 102
#. 
#. [[3]]
#. [1] 103
#. 
#. [[4]]
#. [1] 104
#. 
#. [[5]]
#. [1] 105
#. 
#. [[6]]
#. [1] 106
```

Item failures follow the same `on_error` policies as every other
backend, because `submit()` captured errors as condition objects:

``` r

bk <- new_fake_async_backend(2L)
res <- 1:4 |>
    pump(function(x) if (x == 3) stop("bad item") else x, backend = bk) |>
    pump_run(verbose = FALSE, on_error = "collect")
res[[3]]
#. <simpleError in (function (x) if (x == 3) stop("bad item") else x)(3L): bad item>
```

## Sketch: a callr-based backend

A production-shaped example: one R subprocess per job via the `callr`
package. `submit()` launches a background process, `is_ready()` polls
it, and `collect()` harvests its result - returning the error as a
condition when the subprocess failed:

``` r

callr_backend <- function(workers = 2L) {
    pump_custom_backend(
        name = "callr",
        count = function() workers,
        submit = function(handle, data) {
            callr::r_bg(
                function(func, args) do.call(func, args),
                args = list(
                    func = handle$func,
                    args = c(list(data), handle$args)
                )
            )
        },
        is_ready = function(token) !token$is_alive(),
        collect = function(token) {
            tryCatch(token$get_result(), error = identity)
        }
    )
}

bk <- callr_backend(2L)
1:10 |>
    pump(function(x) x^2, backend = bk) |>
    pump_run(verbose = FALSE)
```

## Practical notes

- **`count()` is a promise you must keep.** siphon dispatches at most
  `count()` concurrent jobs per stage (and validates `max_workers`
  against it), but it never queues on your behalf beyond that.
- **Polling cadence** is governed by the driver’s `sleep_ms`;
  `is_ready()` should be cheap since it runs on every beat while jobs
  are in flight.
- **Lifecycle stays yours.** As with mirai daemons and future plans,
  siphon never tears down your resources:
  [`open()`](https://rdrr.io/r/base/connections.html) gives you a lazy
  start hook, and shutdown is your responsibility.
- **Per-item `fn_time`** is not measured for custom backends (it reports
  as zero in
  [`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md));
  scheduling and dispatch timings are unaffected.
