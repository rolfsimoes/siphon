# Drain a siphon pipeline

`pump_drain()` runs the pipeline, pulling items and passing them to a
callback function as they become ready. This is a memory-safe
alternative to
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
suitable for infinite or long-running pipelines.

## Usage

``` r
pump_drain(x, handle_fn, sleep_ms = 10, verbose = TRUE, backend = "main")
```

## Arguments

- x:

  A pump object or a finite R object.

- handle_fn:

  A callback function with signature `function(id, data, ok)` called for
  each completed item.

- sleep_ms:

  Delay in milliseconds between polls when no item is ready.

- verbose:

  If `TRUE`, show a text progress bar.

- backend:

  Default backend for all stages that do not explicitly set their own
  `backend`. Can be a backend object or one of `"main"`, `"mirai"`, or
  `"future"`. Use
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)
  directly for fault-tolerant PSOCK execution (no string alias).
  Defaults to `"main"`.

## Value

Invisible `NULL`.

## Examples

``` r
results <- list()
f <- 1:5 |> pump(function(x) x * 2, backend = "main")
pump_drain(f, handle_fn = function(id, data, ok) {
    results[[id]] <<- data
})
#>   |                                                                              |                                                                      |   0%  |                                                                              |==============                                                        |  20%  |                                                                              |============================                                          |  40%  |                                                                              |==========================================                            |  60%  |                                                                              |========================================================              |  80%  |                                                                              |======================================================================| 100%
print(results)
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
