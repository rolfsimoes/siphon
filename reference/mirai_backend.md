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

## Examples

``` r
if (requireNamespace("mirai", quietly = TRUE) &&
    mirai::status()$connections > 0) {
    f <- 1:5 |>
        pump(function(x) x * 2, backend = mirai_backend())
    pump_run(f, verbose = FALSE)
}
```
