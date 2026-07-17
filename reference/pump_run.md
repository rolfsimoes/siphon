# Run a siphon pipeline and collect results

`pump_run()` drains a siphon pipeline until all upstream sources, active
slots, and output buffers are complete. Results are collected in the
original input order using internal sequential indices (`idx`).

## Usage

``` r
pump_run(
  x,
  sleep_ms = 10,
  verbose = TRUE,
  on_error = "stop",
  backend = "main",
  timeout = NULL
)
```

## Arguments

- x:

  A pump object or a finite R object.

- sleep_ms:

  Delay between polls when no item is ready.

- verbose:

  If `TRUE`, show a text progress bar.

- on_error:

  Default error handling policy for all stages that do not explicitly
  set their own `on_error`: `"stop"` throws on first error, `"collect"`
  propagates error objects, `"continue"` drops failed items. Defaults to
  `"stop"`.

- backend:

  Default backend for all stages that do not explicitly set their own
  `backend`. Can be a backend object or one of `"main"`, `"mirai"`, or
  `"future"`. Use
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)
  directly for fault-tolerant PSOCK execution (no string alias).
  Defaults to `"main"`.

- timeout:

  Maximum time in seconds to wait for completion. If NULL (default),
  waits indefinitely. If exceeded, throws an error.

## Value

A list of results in input order. Items dropped by a `"continue"` stage
are omitted entirely; the result may be shorter than the input.

## Details

The timeout parameter is checked cooperatively between polling
iterations on the main R thread. Because of this, it only works when
using asynchronous backends (such as
[`mirai_backend()`](https://rolfsimoes.github.io/siphon/reference/mirai_backend.md),
[`future_backend()`](https://rolfsimoes.github.io/siphon/reference/future_backend.md)
with a parallel plan, or
[`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md))
that return control to the main loop. If using a synchronous backend
(like
[`main_backend()`](https://rolfsimoes.github.io/siphon/reference/main_backend.md)),
a job stuck in an infinite loop or blocking operation will freeze the
thread and prevent the timeout from being checked. Furthermore, the
timeout cannot interrupt blocking C/C++ code within background workers.

The `sleep_ms` parameter introduces backoff time between polls when no
item is ready. This is necessary for CPU efficiency but adds overhead.
For fast operations (e.g., simple arithmetic), this overhead can
dominate total runtime. siphon is designed for pipelines with
substantial work per item (e.g., I/O, complex computations, external API
calls) **where coordination overhead is negligible compared to actual
work time**.

## Examples

``` r
f <- 1:5 |> pump(function(x) x * 2, backend = "main")
pump_run(f)
#>   |                                                                              |                                                                      |   0%  |                                                                              |==============                                                        |  20%  |                                                                              |============================                                          |  40%  |                                                                              |==========================================                            |  60%  |                                                                              |========================================================              |  80%  |                                                                              |======================================================================| 100%
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

# With timeout
f <- 1:5 |> pump(function(x) x * 2, backend = "main")
pump_run(f, timeout = 10)
#>   |                                                                              |                                                                      |   0%  |                                                                              |==============                                                        |  20%  |                                                                              |============================                                          |  40%  |                                                                              |==========================================                            |  60%  |                                                                              |========================================================              |  80%  |                                                                              |======================================================================| 100%
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
