# Evaluate an expression on all workers of a parallel backend

`parallel_eval_workers()` evaluates an expression in the global
environment of every worker of a parallel backend and returns the
per-worker results, like
[`parallel::clusterEvalQ()`](https://rdrr.io/r/parallel/clusterApply.html).
Unlike `clusterEvalQ()`, values from the calling frame can be injected
by wrapping them in double braces, e.g.
`parallel_eval_workers(bk, x + {{ y }})` evaluates `x + <value of y>` on
each worker.

## Usage

``` r
parallel_eval_workers(backend, expr)
```

## Arguments

- backend:

  A backend object created by
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md).

- expr:

  An expression to evaluate on each worker.

## Value

A list with one element per worker, in worker order.

## Details

The expression is captured unevaluated and broadcast: it is dispatched
to every worker before any result is collected, so the total wall time
is bounded by the slowest worker rather than by the sum of all workers.

Unlike
[`parallel_setup_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_setup_workers.md),
the expression is **not** recorded for replay on replacement nodes
created after a worker failure. Use
[`parallel_setup_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_setup_workers.md)
for state that jobs depend on (packages, options, objects) so it
survives worker recovery; use `parallel_eval_workers()` for one-shot
queries (diagnostics, versions, process ids) and warm-up work whose loss
on a replaced node is acceptable.

Evaluation can only run while no jobs are active on the backend. If the
expression fails on any worker, an error is raised naming the failing
workers.

## See also

[`parallel_setup_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_setup_workers.md),
[`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)

## Examples

``` r
if (requireNamespace("parallel", quietly = TRUE)) {
    bk <- parallel_backend(2)
    offset <- 40
    parallel_eval_workers(bk, 2 + {{ offset }})
    parallel_stop(bk)
}
```
