# Pipeline status

`pump_status()` returns a snapshot of a pump object's internal state,
including per-stage timing metrics that help identify where time is
spent and which stage is the bottleneck.

## Usage

``` r
pump_status(x)
```

## Arguments

- x:

  A pump object.

## Value

A list of class `pump_status`. For a pipeline it contains `source`
(source info) and `stages`, a list with one entry per stage:

- `type` - backend name (`"main"`, `"mirai"`, `"future"`, `"parallel"`).

- `workers_active` / `workers_limit` - in-flight jobs vs slot limit.

- `buffer_size` / `buffer_capacity` - items ready in the output buffer.

- `completed` - items that have finished this stage.

- `errors` - items with `ok = FALSE` seen by this stage.

- `beats` - number of effective `next_item()` calls (beats) on this
  stage. Beats on a finished stage are no-ops and are not counted.

- `beats_working`, `beats_starved`, `beats_blocked` - how each beat was
  classified (see Details).

- `pop_hits` / `pop_misses` - `pop_item()` calls that returned an item
  vs `NULL`.

- `fn_time` - cumulative time executing the user function, in
  milliseconds. On asynchronous backends this sums time across workers
  and can exceed real elapsed time.

- `tick_time` - cumulative time inside `next_item()` calls (ms).

- `submit_time` - cumulative time dispatching jobs to the backend (ms).
  For the synchronous main backend this includes the function execution
  itself; for asynchronous backends it is serialization/dispatch cost.

- `pull_time` - cumulative time spent asking upstream for items (ms).
  This includes the upstream stages' own beats, so it is reported on the
  upstream stages and excluded from this stage's `coord_time`.

- `coord_time` - derived scheduling overhead for this stage alone:
  `max(0, tick_time - submit_time - pull_time)` (ms).

- `fn_per_item` - average `fn_time` per completed item (ms).

- `share_working` / `share_starved` / `share_blocked` - beat-state
  shares in `[0, 1]` (`NA` before the first beat).

- `throughput` - completed items per second over the observed beat span
  (`NA` until two beats have happened; the span includes any pauses
  between interactive calls).

- `in_flight` - one entry per active slot: `id`, `idx`, and `since`
  (submission time) of the item currently being processed.

- `buffered_ids` - ids of the first few items ready in the buffer.

For plain sources the result is flat and reports `completed`, `errors`,
`pop_hits`, `pop_misses`, and `pull_time` (ms inside `pop_item()`). For
pipelines, the last stage's fields are also copied to the top level for
convenience, plus `delivered` - the number of items that left the
pipeline (popped from the terminal stage by
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md),
[`pump_pop()`](https://rolfsimoes.github.io/siphon/reference/pump_pop.md),
or `pop_item()`); shown as the sink in
[`print()`](https://rdrr.io/r/base/print.html).

## Details

Durations are accumulated only inside `next_item()`/`pop_item()` calls:
time between beats (interactive pauses, `sleep_ms` backoff in
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md))
is deliberately never measured. Idle time is therefore not reported as a
duration; instead each beat is classified, in priority order:

- `blocked` - the output buffer is full while jobs are still in flight:
  downstream is not consuming. This wins over `working` because it is
  the bottleneck signal.

- `working` - jobs in flight, or items delivered during this beat.

- `starved` - free slots, but upstream had nothing to offer.

A beat that finds the stage finished (upstream exhausted, nothing in
flight) cannot move anything and is not recorded, so beat counts and
shares freeze once a pipeline is exhausted.

Under
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
beats are frequent, so beat shares approximate time shares. When
stepping interactively, shares describe what each beat found.

## Display

The status can be displayed using
[`print()`](https://rdrr.io/r/base/print.html) or
[`format()`](https://rdrr.io/r/base/format.html). The display shows a
connected frame from source to sink with worker and buffer occupancy per
stage, timing, beat-state shares, a stuck-job warning for old in-flight
items, and a `* bottleneck` marker when one stage clearly dominates.

The header shows the class name followed by the total number of beats
(pipeline scheduling cycles) in parentheses, e.g., `<pump (10)>`.

## Legend

Compressed labels used in the output:

- wrk:

  workers (active/limit)

- buf:

  output buffer (size/capacity)

- done:

  completed items

- err:

  errors

- fn:

  function time per item

- crd:

  coordination time per beat

- wrk/stv/blk:

  working/starved/blocked beat shares

## Customization

Colors can be customized via `options(siphon.colors = list(...))` to
override specific elements. The system uses standard 16-color ANSI
codes:

- bold = "1":

  bold text

- dim = "2":

  dimmed text

- blue = "34":

  primary color

- green = "32":

  success

- yellow = "33":

  warning

- red = "31":

  error

- bright_blue = "94":

  bright primary

- bright_green = "92":

  bright success

- bright_yellow = "93":

  bright warning

- bright_red = "91":

  bright error

Element-specific color names:

- header:

  pipeline header text

- source:

  "source" label

- sink:

  "sink" label

- stage:

  stage number (e.g., "stage 1")

- backend:

  backend name (e.g., "main", "mirai")

- wrk:

  workers label

- buf:

  buffer label

- done:

  completed label

- err:

  errors label

- fn:

  function time label

- crd:

  coordination time label

- beat:

  beats label

- wrk_share:

  working share label

- stv:

  starved share label

- blk:

  blocked share label

- bottleneck:

  bottleneck marker

Examples: `options(siphon.colors = list(stage = "96"))` to make stage
names cyan `options(siphon.colors = list(wrk = "1;34"))` to make workers
label bold blue `options(siphon.colors = list(bright_red = "35"))` to
change error color to purple

To disable colors globally, set `options(siphon.color = FALSE)`.

## Examples

``` r
f <- 1:5 |> pump(function(x) x * 2, backend = "main")
pump_status(f)
#> <pump_status>
#> ┌─ source   0/5
#> ├─ stage 1 main
#> │    wrk [-----] 0/1   buf [-----] 0/5   done 0   err 0
#> └─ sink   0/5
```
