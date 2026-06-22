# Add a processing stage to a pipeline

`pump()` creates a pull-based stage. The stage pulls items from its
upstream source when it has free slots, submits jobs to the selected
backend, and yields completed items to downstream stages.

## Usage

``` r
pump(
  x,
  fn,
  ...,
  backend = "main",
  max_workers = NULL,
  on_error = NULL,
  buffer_size = NULL
)
```

## Arguments

- x:

  A pump object or a finite R object (list, vector) that will be
  implicitly wrapped as a basic source.

- fn:

  A function. It receives one item as its first argument.

- ...:

  Additional arguments passed to `fn`.

- backend:

  A backend object or one of `"main"`, `"default"`, `"mirai"`, or
  `"future"`.

- max_workers:

  Maximum number of active jobs for this stage. Defaults to the backend
  worker count. Ignored for the synchronous
  [`main_backend()`](https://rolfsimoes.github.io/siphon/reference/main_backend.md)
  (which always uses 1).

- on_error:

  How to handle item errors: `"stop"` throws on first error, `"collect"`
  propagates them, `"continue"` drops failed items. If `NULL` (the
  default), the stage inherits the policy set by
  [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md).

- buffer_size:

  Maximum size of the output buffer. Defaults to
  `min(length(x), 1000L)`. Use a smaller value to enable true
  backpressure.

## Value

A pump object.

## Examples

``` r
# Single stage pipeline
f <- 1:5 |> pump(function(x) x * 2, backend = "main")
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

# Two-stage pipeline
f <- 1:5 |>
    pump(function(x) x + 1, backend = "main") |>
    pump(function(x) x * 3, backend = "main")
pump_run(f, verbose = FALSE)
#> [[1]]
#> [1] 6
#> 
#> [[2]]
#> [1] 9
#> 
#> [[3]]
#> [1] 12
#> 
#> [[4]]
#> [1] 15
#> 
#> [[5]]
#> [1] 18
#> 
```
