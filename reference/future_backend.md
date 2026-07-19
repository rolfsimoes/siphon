# Create a future backend

`future_backend()` submits jobs through
[`future::future()`](https://future.futureverse.org/reference/future.html).
The number of available slots is read from
[`future::nbrOfWorkers()`](https://future.futureverse.org/reference/nbrOfWorkers.html).

## Usage

``` r
future_backend()
```

## Value

A backend object.

## Details

Note: When using future_backend(), you are responsible for managing the
future plan lifecycle. Call
[`future::plan()`](https://future.futureverse.org/reference/plan.html)
to set a plan and restore the previous plan when done. See the vignette
for examples.

Globals required by the stage function are detected automatically by the
`future` framework, exactly as in a plain
[`future::future()`](https://future.futureverse.org/reference/future.html)
call. Because plans provide no persistent worker state, the function and
its detected globals travel with every job; for stages with large
captured state, prefer
[`mirai_backend()`](https://rolfsimoes.github.io/siphon/reference/mirai_backend.md)
or
[`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md),
which install the stage payload on each worker once.

Fault tolerance is delegated to the `future` framework: this backend
performs no retries. If a worker dies while running a job, the resulting
`FutureError` is surfaced as a `pump_error` value for that item (subject
to the `on_error` policy) rather than aborting the pipeline. For
siphon-managed recovery with retries, see
[`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md).

## Examples

``` r
if (requireNamespace("future", quietly = TRUE)) {
    old_plan <- future::plan("sequential")
    on.exit(future::plan(old_plan), add = TRUE)
    f <- 1:5 |>
        pump(function(x) x * 2, backend = future_backend())
    pump_run(f, verbose = FALSE)
}
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
