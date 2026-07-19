# Create a custom backend from plain functions

`pump_custom_backend()` builds a siphon backend from a handful of plain
R functions, so tools with their own execution machinery (job queues,
remote services, worker pools) can plug into
[`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md)
pipelines without touching siphon internals.

## Usage

``` r
pump_custom_backend(
  name,
  count,
  submit,
  is_ready,
  collect,
  register = NULL,
  open = NULL
)
```

## Arguments

- name:

  A single string identifying the backend in
  [`print()`](https://rdrr.io/r/base/print.html) and
  [`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md)
  output.

- count:

  A function with no arguments returning the number of concurrently
  runnable jobs.

- submit:

  A function `function(handle, data)` starting one job and returning a
  job token.

- is_ready:

  A function `function(token)` returning `TRUE` when the job's result
  can be collected without blocking.

- collect:

  A function `function(token)` returning the finished value (or a
  condition object to mark the item as failed).

- register:

  An optional function `function(func, args)` returning the stage handle
  passed to `submit()`. Defaults to bundling `func` and `args` in a
  list.

- open:

  An optional function with no arguments, called once before the backend
  is first used.

## Value

A backend object usable as the `backend` argument of
[`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md),
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md),
and
[`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md).

## Details

siphon drives every backend through the same small contract. You supply
each operation as a function:

- `count()` is consulted when stages are sized and validated: it must
  return the number of jobs the backend can run concurrently, at least
  1.

- `register(func, args)` is called once per stage, after
  [`open()`](https://rdrr.io/r/base/connections.html). It receives the
  stage function and its constant extra arguments, and returns an opaque
  *handle* that later submissions can use. This is the place to install
  per-stage state on your workers so each job ships only its item data.
  When omitted, the handle is
  `list(func = <stage function>, args = <constant arguments>)`.

- `submit(handle, data)` starts one job for one item and returns a job
  *token* without blocking. The job must compute
  `func(data, ...constant args...)` - with the default handle,
  `do.call(handle$func, c(list(data), handle$args))`.

- `is_ready(token)` is polled between beats and must return `TRUE` once
  `collect()` will not block. It must never block itself.

- `collect(token)` returns the finished job's value. Return a condition
  object (e.g. from `tryCatch(..., error = identity)`) to mark the item
  as *failed*: the pipeline then applies the stage's `on_error` policy.
  An error *thrown* by `collect()` itself is treated as an
  infrastructure failure and aborts the pipeline. Note that a condition
  returned this way is always interpreted as failure - if your items can
  legitimately be condition objects, wrap them in a list.

- [`open()`](https://rdrr.io/r/base/connections.html) (optional) is
  called once before the backend is first used - the place for lazy
  resource acquisition. siphon guarantees at most one
  [`open()`](https://rdrr.io/r/base/connections.html) call per backend
  object.

Resource lifecycle stays yours, as with
[`mirai_backend()`](https://rolfsimoes.github.io/siphon/reference/mirai_backend.md)
daemons and
[`future_backend()`](https://rolfsimoes.github.io/siphon/reference/future_backend.md)
plans: shut down whatever
[`open()`](https://rdrr.io/r/base/connections.html) or your constructor
started when you no longer need the backend.

Per-item function timing (`fn_time` in
[`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md))
is not measured for custom backends and reports as zero; scheduling and
dispatch timings are reported as usual.

## See also

[`vignette("extending", package = "siphon")`](https://rolfsimoes.github.io/siphon/articles/extending.md)
for a walkthrough, including an asynchronous example.

## Examples

``` r
# A toy synchronous backend: runs each job at submit time
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
f <- 1:5 |> pump(function(x) x * 2, backend = toy_backend)
pump_run(f, verbose = FALSE)
#> [[1]]
#> [1] 2
#> 
#> [[2]]
#> [1] 4
#> 
#> [[3]]
#> [1] 6
#> 
#> [[4]]
#> [1] 8
#> 
#> [[5]]
#> [1] 10
#> 
```
