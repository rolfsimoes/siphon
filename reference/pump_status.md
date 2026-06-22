# Pipeline status

`pump_status()` returns a snapshot of a pump object's internal state,
including detailed timing metrics that help identify where time is
spent.

## Usage

``` r
pump_status(x)
```

## Arguments

- x:

  A pump object.

## Value

A list with:

- `buffer_size` - current items in the output buffer.

- `buffer_capacity` - maximum buffer size.

- `workers_active` - currently running jobs.

- `workers_limit` - maximum concurrent jobs for this stage.

- `completed` - items that have finished this stage.

- `errors` - items with `ok = FALSE` seen by this stage.

- `poll_hits` - number of successful polls (items ready).

- `poll_misses` - number of failed polls (no item ready).

- `poll_wall_time` - total time spent inside `next_item()` calls in
  milliseconds.

- `fn_time` - wall-clock time spent executing the user function (useful
  work) in milliseconds.

- `idle_time` - time spent waiting (starvation or exhaustion) in
  milliseconds.

## Details

Note that `poll_wall_time` excludes time spent in backoff sleeps
(controlled by the `sleep_ms` parameter in
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
and
[`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md)).
Total wall-clock time when using
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
is approximately `poll_wall_time + poll_misses * sleep_ms`. For fast
operations, the backoff overhead can dominate total runtime. siphon is
designed for pipelines with substantial work per item **where
coordination overhead is negligible compared to actual work time**.

## Examples

``` r
f <- 1:5 |> pump(function(x) x * 2, backend = "main")
pump_status(f)
#> <pump_status>
#>   Source (main):
#>     length:   5
#>     position: 0
#>     errors:   0
#>   Stage 1 (main):
#>     workers: 0/1
#>     buffer:  0/5
#>     done:    0
#>     errors:  0
```
