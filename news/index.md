# Changelog

## siphon (development version)

## siphon 0.5.0

- Added
  [`parallel_eval_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_eval_workers.md),
  equivalent to
  [`parallel::clusterEvalQ()`](https://rdrr.io/r/parallel/clusterApply.html)
  for parallel backends. Evaluates expressions in worker global
  environments with `{{ }}` injection support. Not recorded for replay
  on replacement nodes.
- Corrected
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)
  documentation: backends can be shared across stages. Sharing stages’
  `max_workers` must sum to at most the worker count. Documented with
  examples and tests including oversubscription failure mode.
- Added vignette section on pooling strategies: sharing one
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)
  across stages versus isolated pools per stage.
- Worker setup and evaluation now broadcast to all nodes before
  collecting results, completing in the time of the slowest worker
  instead of summing all workers. Preserves per-node ordering and replay
  behavior.

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
