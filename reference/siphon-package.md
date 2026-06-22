# siphon: Pull-Based Staged Pipelines

siphon provides a pull-based pipeline runtime where items flow through a
chain of stages. Each stage pulls work from its upstream source when it
has free slots, submits jobs to a backend (main, mirai, future), and
yields completed items downstream. Results are collected by
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
in the original input order via sequential internal indices (idx).

## Details

A small runtime for pull-based staged pipelines. siphon connects item
streams through asynchronous stages with bounded slots and
backend-specific execution.

## Backends

- [`main_backend()`](https://rolfsimoes.github.io/siphon/reference/main_backend.md):

  Executes jobs synchronously in the current R process (default)

- [`mirai_backend()`](https://rolfsimoes.github.io/siphon/reference/mirai_backend.md):

  Submits jobs through mirai::mirai() for async execution

- [`future_backend()`](https://rolfsimoes.github.io/siphon/reference/future_backend.md):

  Submits jobs through future::future() for async execution

## Error Handling

Each stage can configure error handling via the `on_error` parameter, or
inherit the pipeline-wide default from `pump_run(..., on_error = ...)`:

- `"stop"`:

  Throws on first error (default for `pump_run`)

- `"collect"`:

  Propagates error objects

- `"continue"`:

  Drops failed items

Explicit stage-level `on_error` always overrides the
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
default.

## See also

Useful links:

- <https://github.com/rolfsimoes/siphon>

- Report bugs at <https://github.com/rolfsimoes/siphon/issues>

## Author

**Maintainer**: Rolf Simoes <rolfsimoes@gmail.com>

Authors:

- Rolf Simoes <rolfsimoes@gmail.com>
