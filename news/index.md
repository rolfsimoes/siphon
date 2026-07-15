# Changelog

## siphon (development version)

## siphon 0.5.0

- Added
  [`parallel_eval_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_eval_workers.md),
  an equivalent of
  [`parallel::clusterEvalQ()`](https://rdrr.io/r/parallel/clusterApply.html)
  for parallel backends: evaluates an expression in the global
  environment of every worker and returns the per-worker results.
  Supports injecting values from the calling frame with `{{ }}`, like
  [`parallel_setup_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_setup_workers.md).
  Unlike setup expressions, evaluations are not recorded for replay on
  replacement nodes.
- Corrected
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)
  documentation: one backend **can** be shared across multiple
  [`pump()`](https://rolfsimoes.github.io/siphon/reference/pump.md)
  stages — results are received per node and cannot cross-wire. The
  requirement is that the `max_workers` of the sharing stages sum to at
  most the worker count (each stage defaults to the full count, so set
  it explicitly). Documented with an example and covered by tests,
  including the failure mode when the pool is oversubscribed.
- New vignette section “Pooling strategies: one shared pool or isolated
  pools” documenting when to share one
  [`parallel_backend()`](https://rolfsimoes.github.io/siphon/reference/parallel_backend.md)
  across stages versus creating a separate backend per stage to isolate
  worker pools (job placement, worker state, and sizing trade-offs).
- Worker setup and evaluation are now broadcast: expressions are
  dispatched to all nodes before results are collected, so
  [`parallel_setup_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_setup_workers.md)
  and
  [`parallel_eval_workers()`](https://rolfsimoes.github.io/siphon/reference/parallel_eval_workers.md)
  complete in the time of the slowest worker instead of the sum of all
  workers. Per-node ordering of setup expressions is preserved, and
  replay on replacement nodes is unchanged.

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
