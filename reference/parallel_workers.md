# Inspect the workers of a parallel backend

`parallel_workers()` returns the number of worker processes of a
parallel backend. `parallel_busy()` returns a logical vector with one
element per worker: `TRUE` for workers currently holding an in-flight
job. Owners of attached clusters (see `parallel_backend(cluster =)`) can
use `parallel_busy()` after a failed run to find nodes that were
quarantined (left busy) and repair them before reusing the cluster
elsewhere.

## Usage

``` r
parallel_workers(backend)

parallel_busy(backend)
```

## Arguments

- backend:

  A backend object created by
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md).

## Value

`parallel_workers()`: an integer count. `parallel_busy()`: a logical
vector with one element per live worker.

## Details

A backend created with `workers` is a specification until its first use:
`parallel_workers()` then reports the configured capacity and
`parallel_busy()` returns a zero-length vector. On a stopped backend,
`parallel_workers()` returns 0 and `parallel_busy()` a zero-length
vector.

## See also

[`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md),
[`parallel_stop()`](https://rolfsimoes.github.io/siphon/reference/parallel_stop.md)

## Examples

``` r
if (requireNamespace("parallel", quietly = TRUE)) {
    bk <- parallel_backend(2)
    parallel_workers(bk)
    parallel_busy(bk)
    parallel_stop(bk)
}
```
