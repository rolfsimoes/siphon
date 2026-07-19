# Number of items in a pump pipeline

Returns the item count of the pipeline's source. Infinite sources (the
default for
[`pump_source()`](https://rolfsimoes.github.io/siphon/reference/pump_source.md))
report `NA_integer_` rather than `Inf`, because base R's
[`length()`](https://rdrr.io/r/base/length.html) contract requires a
non-negative integer and an `Inf` would break callers such as
`seq_len(length(x))`. Internal code that needs the raw count (including
`Inf`) reads `x$length()` directly.

## Usage

``` r
# S3 method for class 'pump'
length(x)
```

## Arguments

- x:

  A pump object.

## Value

A non-negative integer, or `NA_integer_` for an infinite source.
