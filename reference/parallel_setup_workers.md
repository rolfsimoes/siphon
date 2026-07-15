# Run setup code on all workers of a parallel backend

`parallel_setup_workers()` evaluates an expression in the global
environment of every worker of a parallel backend. Use it to load
packages, source files, or define objects that jobs need. The expression
is recorded and replayed automatically on any replacement node created
after a worker failure.

## Usage

``` r
parallel_setup_workers(backend, expr)
```

## Arguments

- backend:

  A backend object created by
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md).

- expr:

  An expression to evaluate on each worker.

## Value

The backend, invisibly.

## Details

The expression is captured unevaluated. Values from the calling frame
can be injected by wrapping them in double braces, e.g.
`parallel_setup_workers(bk, x <- {{ y }})` assigns the current value of
`y` to `x` on each worker.

Setup can only run while no jobs are active on the backend.

## See also

[`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md),
[`parallel_stop()`](https://rolfsimoes.github.io/siphon/reference/parallel_stop.md)

## Examples

``` r
if (requireNamespace("parallel", quietly = TRUE)) {
    bk <- parallel_backend(2)
    parallel_setup_workers(bk, threshold <- 10)
    f <- 1:5 |>
        pump(function(x) x + threshold, backend = bk)
    pump_run(f, verbose = FALSE)
    parallel_stop(bk)
}
```
