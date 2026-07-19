# Change log

## siphon (development version)

### New features

- Added `parallel_workers()` and `parallel_busy()` to inspect a parallel
  backend's worker count and per-worker in-flight state (e.g. to find
  quarantined nodes of an attached cluster after a failed run).

### Fixes

- An aborted pipeline run now quiesces its parallel backends: pending
  results of in-flight jobs are drained (bounded by
  `options(siphon.quiesce_timeout =)`, default 30 s) so they can no longer
  be read as the results of the next submission on the same nodes.
  Previously, reusing a cluster after an abort — an owned backend driven
  again, or the externally managed cluster behind
  `parallel_backend(cluster =)` used directly — could silently deliver
  stale results to the next user. Nodes that cannot be drained in time are
  replaced on owned pools and left quarantined (busy) on attached
  clusters, where the owner can locate them with `parallel_busy()`.
- Stage runners installed on persistent workers (parallel, mirai) are now
  uninstalled when the pipeline closes, so long-lived pools no longer
  accumulate one runner per stage per run.

## siphon 0.8.0

### Behavior changes

- `length()` on a pump pipeline returns `NA_integer_` for infinite sources instead of `Inf`. The raw count remains available via `x$length()`.

### Deprecations

- The flat top-level copy of the terminal stage's fields in `pump_status()` is deprecated and will be removed in a future minor release. Use `x$stages` for per-stage values.

### Fixes

- `pump_status()` on a pipeline with a single stage over an empty source reports it as a stage with its source and sink.
- `reset_stats()` clears the error count.
- Unreachable slot-acquisition failures in the scheduler raise an internal error.

### Internal

- The stage/source protocol uses a single constructor (`.pump_protocol()`) with safe defaults and explicit `"stage"`/`"source"` role tags, replacing four hand-maintained copies.

## siphon 0.7.0

### Behavior changes

- `pump_run(backend =)` and `pump_drain(backend =)` now apply to stages without explicit backend. Inherited backends resolve at first beat; explicit backends validate at construction.
- `parallel_backend()` is now a specification: workers start on first use. `parallel_setup_workers()` queues expressions before cluster starts.
- Future backend: removed explicit globals list, restoring automatic detection. Mirai and parallel stage functions must be self-contained.
- All backends inherit from `pump_backend` class; objects without it are rejected.

### Performance

- Stage functions and constant arguments ship once per stage (mirai, parallel) instead of per item. Payloads replay on replacement nodes.

### New features

- Added `pump_custom_backend()` for custom execution via plain R functions: `count`, `submit`, `is_ready`, `collect`, optional `register` and `open`. See "Extending siphon" vignette.
- Added `parallel_backend(cluster =)` to attach externally-managed PSOCK clusters without ownership. Worker failures surface as item errors.
- `pump_drain()` gained `on_error` and `timeout`. Draining exhausted pipelines is a no-op.

### Internal

- Unified backend contract: lifecycle (`.pump_backend_open()`/`.pump_backend_close()`), registration (`.pump_executor_register()`), submission (`.pump_executor_new_job(backend, handle, data)`). Shared driver for `pump_run()` and `pump_drain()`.

### Fixes

- Fixed CRAN policy issues; added spelling and linting configuration.

## siphon 0.6.0

### Pipeline inspection

- Added `pump_step()` for advancing work without consuming results.
- Added `pump_peek()` for viewing ready results without removing them.
- Added `pump_pop()` for explicitly consuming one result.
- Redesigned `print()` to display pipelines as connected frames with worker/buffer occupancy bars, timing metrics, beat-state shares, and bottleneck markers.
- Added ANSI color support for terminal output (honors `NO_COLOR` and `options(siphon.color =)`).
- Added box-drawing glyphs with ASCII fallback via `options(siphon.unicode = FALSE)`.
- Added `delivered` field to `pump_status()` reporting items that left the pipeline.
- Added `in_flight` and `buffered_ids` fields to `pump_status()` for per-stage tracking.

### Timing model (breaking)

- Redesigned stage timing metrics to accumulate only inside `next_item()`/`pop_item()` calls.
- Removed fields: `poll_hits`, `poll_misses`, `poll_wall_time`, `idle_time`.
- Added fields: `beats`, `beats_working`/`beats_starved`/`beats_blocked`, `share_*` ratios, `pop_hits`/`pop_misses`, `tick_time`, `submit_time`, `pull_time`, `coord_time`, `fn_per_item`, `throughput`.
- Beats on finished pipelines are no longer recorded; stats freeze when source is exhausted.
- `pump_step()` stops early when further beats cannot change anything.
- All durations now consistently in milliseconds (sources previously used seconds).

### Scheduling

- Changed beat to drain-advance-drain; jobs completing during advance land in buffer immediately.

### Fixes

- Added `%||%` compatibility shim for R < 4.4.0.
- Fixed `parallel_backend()` stages to report as `"parallel"` instead of `"unknown"`.
- Refactored internal stage tick into single-responsibility helpers.

## siphon 0.5.0

- Added `parallel_eval_workers()` for evaluating expressions in worker global environments.
- Corrected `parallel_backend()` documentation for shared backends across stages.
- Added vignette section on pooling strategies.
- Changed worker setup to broadcast to all nodes before collecting results.

## siphon 0.4.2

### Package Renaming & API Standardization

- Standardized public API with the `pump_` prefix:
  - `pump()` adds a processing stage to a pipeline.
  - `pump_run()` executes the pipeline and collects results.
  - `pump_source()` creates custom input sources using closures.
  - `pump_status()` returns a snapshot of stage state.
  - Added new `pump_drain()` function to run pipelines without accumulating items in memory, suitable for persistent daemon processes.
- Implemented S3 generic method `length.pump()` (replaces `$size()` method) for R-idiomatic dataset length queries.

### Enhancements & Bug Fixes

- Decoupled total pipeline `length` from intermediate `buffer_size` to prevent memory bloat and coercion errors on infinite pipelines.
- Stage `buffer_size` now defaults to `min(length(x), 1000L)`, capping memory usage for large/infinite sources while allocating correctly for small ones.
- Custom sources created via `pump_source()` support resource cleanup callbacks via the `close_fn` argument.
- Clean up custom sources automatically upon pipeline exit in both `pump_run()` and `pump_drain()`.
- Fixed `progress` bar in `pump_run()` to correctly suppress for small inputs.
- Fixed empty input semantics; `pump_run()` now returns `list()` for zero-length sources instead of throwing an error.
- Fixed per-stage `on_error = "stop"` to ensure it is honored at each stage instead of only at the final stage.
- Added `timeout` parameter to `pump_run()` to avoid infinite polling on stuck R jobs.
- Added `requireNamespace()` checks for `mirai`, `future`, and `parallel` backends with clear error messages.
- Added async-backend scheduling tests for `mirai`, `future`, and `parallel` backends.
