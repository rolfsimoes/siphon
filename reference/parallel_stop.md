# Stop a parallel backend

`parallel_stop()` shuts down the PSOCK cluster owned by a parallel
backend. Call it when the backend is no longer needed to release the
worker processes and their socket connections.

## Usage

``` r
parallel_stop(backend, force = FALSE)
```

## Arguments

- backend:

  A backend object created by
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md).

- force:

  If `TRUE`, stop the cluster even if jobs are active.

## Value

The backend, invisibly.

## Details

By default, stopping fails if jobs are still active. Set `force = TRUE`
to stop the cluster regardless. Stopping an already stopped backend is a
no-op.

## See also

[`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)

## Examples

``` r
if (requireNamespace("parallel", quietly = TRUE)) {
    bk <- parallel_backend(2)
    parallel_stop(bk)
}
```
