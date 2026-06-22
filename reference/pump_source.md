# Create a custom siphon source

`pump_source()` creates a custom pull-based source for use with
[`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md)
pipelines. Use this to connect external data sources such as message
queues, databases, or file readers to a siphon pipeline.

## Usage

``` r
pump_source(pull_fn, done_fn = NULL, close_fn = NULL, length = Inf)
```

## Arguments

- pull_fn:

  A function with no arguments that returns `list(id, data, ok)` or
  `NULL`. The returned list must contain a non-missing, scalar atomic
  user-visible `id` (uniqueness is the user's responsibility), the
  `data` object, and a non-missing scalar logical `ok` flag indicating
  success.

- done_fn:

  A function with no arguments returning `TRUE` or `FALSE`. Defaults to
  `NULL` (source never finishes on its own).

- close_fn:

  An optional function with no arguments for resource cleanup (e.g.,
  closing file connections or database handles). Called automatically by
  [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
  and
  [`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md)
  when execution completes.

- length:

  The total number of items to expect. Defaults to `Inf`. Used by
  [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
  for result pre-allocation and progress reporting.

## Value

A pump object that can be piped into
[`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md).

## Details

The `pull_fn` function is called repeatedly by downstream stages to
retrieve items. It should return `list(id, data, ok)` when an item is
available, or `NULL` when no item is ready. The source is considered
infinite by default (suitable for daemon-style processing); pass a
`done_fn` to signal when the source is exhausted.

## Examples

``` r
# A simple counter source
counter_source <- function(n) {
    i <- 0L
    pump_source(
        pull_fn = function() {
            if (i >= n) {
                return(NULL)
            }
            i <<- i + 1L
            list(id = i, data = i, ok = TRUE)
        },
        done_fn = function() i >= n,
        length = n
    )
}
src <- counter_source(5)
res <- src |>
    pump(function(x) x * 2) |>
    pump_run(verbose = FALSE)
print(res)
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
