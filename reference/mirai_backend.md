# Create a mirai backend

`mirai_backend()` submits jobs through
[`mirai::mirai()`](https://mirai.r-lib.org/reference/mirai.html). The
number of available slots is read from `mirai::status()$connections`.

## Usage

``` r
mirai_backend()
```

## Value

A backend object.

## Details

Note: When using mirai_backend(), you are responsible for managing the
mirai daemon lifecycle. Call `mirai::daemons(n)` to start workers and
`mirai::daemons(0)` to shut them down. See the vignette for examples.

Fault tolerance is delegated to the `mirai` framework: this backend
performs no retries. If a daemon dies while running a job, the resulting
`errorValue` is surfaced as a `pump_error` value for that item (subject
to the `on_error` policy) rather than leaking into the pipeline. For
siphon-managed recovery with retries, see
[`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md).

## Examples

``` r
if (requireNamespace("mirai", quietly = TRUE) &&
    mirai::status()$connections > 0) {
    f <- 1:5 |>
        pump(function(x) x * 2, backend = mirai_backend())
    pump_run(f, verbose = FALSE)
}
```
