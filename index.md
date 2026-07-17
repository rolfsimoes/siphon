# siphon

[![R-CMD-check](https://github.com/rolfsimoes/siphon/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/rolfsimoes/siphon/actions/workflows/R-CMD-check.yaml)
[![pkgdown](https://img.shields.io/badge/pkgdown-website-blue.svg)](https://rolfsimoes.github.io/siphon/)
[![License:
MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Lifecycle:
maturing](https://img.shields.io/badge/lifecycle-maturing-blue.svg)](https://lifecycle.r-lib.org/articles/stages.html#maturing)

`siphon` is a small runtime for pull-based staged pipelines in R.

It is not a replacement for `future`, `mirai`, `parallel`, or other
execution backends. Those tools execute jobs. `siphon` connects stages,
limits active jobs with slots, and lets items move to downstream stages
as soon as they are ready.

## Installation

Install the development version from GitHub:

``` r

# install.packages("devtools")
devtools::install_github("rolfsimoes/siphon")
```

Once published on CRAN, install with:

``` r

install.packages("siphon")
```

``` r

library(siphon)

# Define test data
items <- 1:5

# Two-stage pipeline showing flow composition
res <- items |>
  pump(function(x) x + 1, backend = "main") |>
  pump(function(x) x * 2, backend = "main") |>
  pump_run(verbose = FALSE)

# Results in original input order
print(res)
```

``` R
## [[1]]
## [1] 4
## 
## [[2]]
## [1] 6
## 
## [[3]]
## [1] 8
## 
## [[4]]
## [1] 10
## 
## [[5]]
## [1] 12
```

## Features

- **Pull-based staged pipelines** - Items move to downstream stages as
  soon as they are ready, without waiting for upstream completion
- **Multiple execution backends** - Support for main thread, mirai,
  future, and parallel (PSOCK) backends
- **Fault-tolerant execution** -
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)
  replaces crashed workers and resubmits their jobs (at-least-once
  semantics)
- **Backpressure control** - Bounded buffers with `buffer_size` to limit
  memory usage
- **Concurrency control** - Per-stage `max_workers` to manage resource
  constraints
- **Error handling** - Flexible error policies: `"collect"`, `"stop"`,
  or `"continue"`
- **Timeout protection** - Optional `timeout` in
  [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
  to avoid infinite polling
- **Interactive inspection** -
  [`pump_step()`](https://rolfsimoes.github.io/siphon/reference/pump_step.md),
  [`pump_peek()`](https://rolfsimoes.github.io/siphon/reference/pump_peek.md),
  [`pump_pop()`](https://rolfsimoes.github.io/siphon/reference/pump_pop.md),
  and a rich [`print()`](https://rdrr.io/r/base/print.html) to step
  through and debug a live pipeline
- **Status inspection** -
  [`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md)
  for monitoring pipeline state
- **Order preservation** - Results returned in original input order
  regardless of execution order
- **Custom sources** -
  [`pump_source()`](https://rolfsimoes.github.io/siphon/reference/pump_source.md)
  to connect external data sources (queues, databases, files)
- **Streaming drain** -
  [`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md)
  for memory-safe daemon-style processing

Use direct parallel map tools when the problem is simply `lapply(x, f)`
with workers. Use `siphon` when the problem is a staged flow with
different resource constraints per stage, such as CPU preparation,
main-thread GPU dispatch, and parallel I/O. Note that stages using
[`main_backend()`](https://rolfsimoes.github.io/siphon/reference/main_backend.md)
run sequentially on the main thread but do not block pipeline
coordination-other stages on async backends continue processing in
parallel.

## Inspecting a pipeline

A pipeline can be stepped and inspected interactively without consuming
it.
[`pump_step()`](https://rolfsimoes.github.io/siphon/reference/pump_step.md)
runs beats (advancing work through every stage),
[`print()`](https://rdrr.io/r/base/print.html) shows per-stage
occupancy, timing, and the bottleneck,
[`pump_peek()`](https://rolfsimoes.github.io/siphon/reference/pump_peek.md)
looks at ready results without removing them, and
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
resumes and finishes the remainder:

``` r

p <- 1:6 |>
  pump(function(x) {
    Sys.sleep(0.05)
    x * 2
  }, backend = "main") |>
  pump(function(x) x + 1, backend = "main")

pump_step(p, 3) # advance 3 beats; nothing is consumed
```

``` R
## <pump pipeline>
## ┌─ source (main)   pulled 3/6
## ├─ stage 1 (main)
## │    work [-] 0/1   out [------] 0/6   done 3   err 0
## │    fn 50.1ms/item   coord 0.0ms/beat   beats(3): work 100% starv 0% block 0%
## ├─ stage 2 (main)
## │    work [-] 0/1   out [###---] 3/6   done 3   err 0
## │    fn 0.0ms/item   coord 0.0ms/beat   beats(3): work 100% starv 0% block 0%
## └─ sink   delivered 0/6
```

``` r

print(p) # workers, buffers, per-stage times, beat states
```

``` R
## <pump pipeline>
## ┌─ source (main)   pulled 3/6
## ├─ stage 1 (main)
## │    work [-] 0/1   out [------] 0/6   done 3   err 0
## │    fn 50.1ms/item   coord 0.0ms/beat   beats(3): work 100% starv 0% block 0%
## ├─ stage 2 (main)
## │    work [-] 0/1   out [###---] 3/6   done 3   err 0
## │    fn 0.0ms/item   coord 0.0ms/beat   beats(3): work 100% starv 0% block 0%
## └─ sink   delivered 0/6
```

``` r

pump_peek(p) # look at the next ready result, non-destructively
```

``` R
## [[1]]
## [[1]]$id
## [1] 1
## 
## [[1]]$idx
## [1] 1
## 
## [[1]]$data
## [1] 3
## 
## [[1]]$ok
## [1] TRUE
```

``` r

res <- pump_run(p, verbose = FALSE) # resumes; full ordered result
print(res)
```

``` R
## [[1]]
## [1] 3
## 
## [[2]]
## [1] 5
## 
## [[3]]
## [1] 7
## 
## [[4]]
## [1] 9
## 
## [[5]]
## [1] 11
## 
## [[6]]
## [1] 13
```

Use
[`pump_pop()`](https://rolfsimoes.github.io/siphon/reference/pump_pop.md)
only when you intend to consume an item yourself: it removes the item
from the stream (a later
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
will not return it) and, for managed sources, transfers commit/release
responsibility to you.

See the
[`vignette("siphon")`](https://rolfsimoes.github.io/siphon/articles/siphon.md)
for a longer introduction with error handling and status inspection
examples.
