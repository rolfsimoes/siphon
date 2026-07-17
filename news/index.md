# Changelog

## siphon 0.6.0

### Pipeline inspection

- Added
  [`pump_step()`](https://rolfsimoes.github.io/siphon/reference/pump_step.md)
  for advancing work without consuming results.
- Added
  [`pump_peek()`](https://rolfsimoes.github.io/siphon/reference/pump_peek.md)
  for viewing ready results without removing them.
- Added
  [`pump_pop()`](https://rolfsimoes.github.io/siphon/reference/pump_pop.md)
  for explicitly consuming one result.
- Redesigned [`print()`](https://rdrr.io/r/base/print.html) to display
  pipelines as connected frames with worker/buffer occupancy bars,
  timing metrics, beat-state shares, and bottleneck markers.
- Added ANSI color support for terminal output (honors `NO_COLOR` and
  `options(siphon.color =)`).
- Added box-drawing glyphs with ASCII fallback via
  `options(siphon.unicode = FALSE)`.
- Added `delivered` field to
  [`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md)
  reporting items that left the pipeline.
- Added `in_flight` and `buffered_ids` fields to
  [`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md)
  for per-stage tracking.

### Timing model (breaking)

- Redesigned stage timing metrics to accumulate only inside
  `next_item()`/`pop_item()` calls.
- Removed fields: `poll_hits`, `poll_misses`, `poll_wall_time`,
  `idle_time`.
- Added fields: `beats`,
  `beats_working`/`beats_starved`/`beats_blocked`, `share_*` ratios,
  `pop_hits`/`pop_misses`, `tick_time`, `submit_time`, `pull_time`,
  `coord_time`, `fn_per_item`, `throughput`.
- Beats on finished pipelines are no longer recorded; stats freeze when
  source is exhausted.
- [`pump_step()`](https://rolfsimoes.github.io/siphon/reference/pump_step.md)
  stops early when further beats cannot change anything.
- All durations now consistently in milliseconds (sources previously
  used seconds).

### Scheduling

- Changed beat to drain-advance-drain; jobs completing during advance
  land in buffer immediately.

### Fixes

- Added `%||%` compatibility shim for R \< 4.4.0.
- Fixed
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)
  stages to report as `"parallel"` instead of `"unknown"`.
- Refactored internal stage tick into single-responsibility helpers.

## siphon 0.5.0

- Added
  [`parallel_eval_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_eval_workers.md)
  for evaluating expressions in worker global environments.
- Corrected
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)
  documentation for shared backends across stages.
- Added vignette section on pooling strategies.
- Changed worker setup to broadcast to all nodes before collecting
  results.

## siphon 0.4.2

### Package Renaming & API Standardization

- Standardized public API with the `pump_` prefix:
  - [`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md)
    adds a processing stage to a pipeline.
  - [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
    executes the pipeline and collects results.
  - [`pump_source()`](https://rolfsimoes.github.io/siphon/reference/pump_source.md)
    creates custom input sources using closures.
  - [`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md)
    returns a snapshot of stage state.
  - Added new
    [`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md)
    function to run pipelines without accumulating items in memory,
    suitable for persistent daemon processes.
- Implemented S3 generic method `length.pump()` (replaces `$size()`
  method) for R-idiomatic dataset length queries.

### Enhancements & Bug Fixes

- Decoupled total pipeline `length` from intermediate `buffer_size` to
  prevent memory bloat and coercion errors on infinite pipelines.
- Stage `buffer_size` now defaults to `min(length(x), 1000L)`, capping
  memory usage for large/infinite sources while allocating correctly for
  small ones.
- Custom sources created via
  [`pump_source()`](https://rolfsimoes.github.io/siphon/reference/pump_source.md)
  support resource cleanup callbacks via the `close_fn` argument.
- Clean up custom sources automatically upon pipeline exit in both
  [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
  and
  [`pump_drain()`](https://rolfsimoes.github.io/siphon/reference/pump_drain.md).
- Fixed `progress` bar in
  [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
  to correctly suppress for small inputs.
- Fixed empty input semantics;
  [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
  now returns [`list()`](https://rdrr.io/r/base/list.html) for
  zero-length sources instead of throwing an error.
- Fixed per-stage `on_error = "stop"` to ensure it is honored at each
  stage instead of only at the final stage.
- Added `timeout` parameter to
  [`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
  to avoid infinite polling on stuck R jobs.
- Added [`requireNamespace()`](https://rdrr.io/r/base/ns-load.html)
  checks for `mirai`, `future`, and `parallel` backends with clear error
  messages.
- Added async-backend scheduling tests for `mirai`, `future`, and
  `parallel` backends.
