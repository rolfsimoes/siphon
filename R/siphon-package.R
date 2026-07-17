#' siphon: Pull-Based Staged Pipelines
#'
#' A small runtime for pull-based staged pipelines. siphon connects item
#' streams through asynchronous stages with bounded slots and backend-specific
#' execution.
#'
#' @description
#' siphon provides a pull-based pipeline runtime where items flow through a
#' chain of stages. Each stage pulls work from its upstream source when it has
#' free slots, submits jobs to a backend (main, mirai, future, parallel), and yields
#' completed items downstream. Results are collected by `pump_run()` in the
#' original input order via sequential internal indices (`idx`).
#'
#' @section Backends:
#' \describe{
#'   \item{`main_backend()`}{Executes jobs synchronously in the current R process (default)}
#'   \item{`mirai_backend()`}{Submits jobs through mirai::mirai() for async execution}
#'   \item{`future_backend()`}{Submits jobs through future::future() for async execution}
#'   \item{`parallel_backend()`}{Owns a PSOCK cluster with fault tolerance to worker failures; use parallel_setup_workers() and parallel_stop()}
#' }
#'
#' @section Error Handling:
#' Each stage can configure error handling via the `on_error` parameter, or
#' inherit the pipeline-wide default from `pump_run(..., on_error = ...)`:
#' \describe{
#'   \item{`"stop"`}{Throws on first error (default for `pump_run`)}
#'   \item{`"collect"`}{Propagates error objects}
#'   \item{`"continue"`}{Drops failed items}
#' }
#' Explicit stage-level `on_error` always overrides the `pump_run()` default.
#'
#' @keywords internal
"_PACKAGE"
