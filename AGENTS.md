# Rules for writing or changing R code in this package

## 1. R style

* Base R plus `mirai`, `future`, and `parallel` only. Do not add new dependencies. No R6, tidyverse, data.table, cli, glue, or frameworks.
* Objects are plain S3 lists built with `structure()`.
* Return by last expression. Use `return()` only for early exit, like `break`. Never as final punctuation.
* Validate with `if () stop()` at the top of the function. Keep validation simple. No validation framework. No custom condition classes.
* Style facts: 4-space indent, integer literals (`1L`), `[[` for scalar extraction, `invisible()` when returning from state-mutating functions.
* Long function declarations: use hanging indent. First argument stays on the `function(` line. Count characters before it and indent every subsequent argument that many spaces exactly. Example:

  ```r
  pump_drain <- function(x,
                         handle_fn,
                         sleep_ms = 10,
                         verbose = TRUE,
                         backend = "main") {
      ...
  }
  ```

* Prefer direct code. No clever abstractions. No premature generalization. No metaprogramming outside necessary backend interfaces.
* Comment only architecture or non-obvious choices. Never restate obvious code.
* Keep files small. One file, one responsibility. Do not create a new file if an existing file owns the concept.

File map:

```text
R/backend.R          backend contract generics and shared helpers
R/backend-main.R    main_backend() synchronous execution
R/backend-mirai.R   mirai_backend() async via mirai::mirai()
R/backend-future.R  future_backend() async via future::future()
R/backend-parallel.R parallel_backend() PSOCK cluster with fault tolerance
R/pump.R            pump() stage construction and pipeline composition
R/run.R             pump_run() and pump_drain() execution drivers
R/source.R          source implementations (basic, managed)
R/utils.R           small shared helpers (%||%, timing, error handling)
```

### `@export` policy

* `#' @export` on a public function exports user API.
* `#' @export` on an S3 method registers it in NAMESPACE (`S3method()`). Add it to **every** S3 method, including dot-prefixed ones.
* Dot prefix means internal naming. `@export` on a dot-prefixed S3 method does NOT make it user API.
* Never use `@S3method`.

### Documentation (CRAN rules, mandatory)

Every exported function gets a full roxygen block:

* Title (Title Case), short description, `@param` for every argument, `@return`, `@examples`, then `@export`.
* `@param` and `@return` are sentences: capital letter, full stop. `@param` states the type (e.g. "A string with ...", "An integer vector ...").
* `@return` describes the structure/class of the output and what it means. CRAN requires it even for side-effect functions: "..., invisibly. Called for its side effect of ...".
* `@examples` must run without errors and be self-contained: write only to `tempfile()`, clean up, never change user state.
* Cross-reference with markdown `[fn()]` links and `@seealso` (markdown is enabled via `Roxygen: list(markdown = TRUE)`).
* Internal functions and internal S3 methods get NO roxygen title block: `#' @export` alone registers without generating an `.Rd`.
* Package-level doc lives in `R/siphon-package.R` (`"_PACKAGE"` sentinel).

DESCRIPTION rules:

* Title in Title Case, no final period, does not start with the package name.
* Description is a full paragraph, does not start with "This package" or the package name; quote software names like 'mirai'.
* Only `Authors@R` (no `Author`/`Maintainer` fields). No `LazyData` without a `data/` directory.

Non-package files (`AGENTS.md`, example scripts, check artifacts) must be listed in `.Rbuildignore`.

After any change: run `roxygen2::roxygenise()`, then `R CMD build` + `R CMD check --as-cran`. Zero ERRORs, zero WARNINGs; the only accepted NOTE is "new submission / dev version".

## 2. Architecture

```text
pump()           composes stages into a pipeline
pump_run()       drains pipeline and collects results in order
pump_drain()     drains pipeline with callback for infinite streams
backend          execution context (main/mirai/future/parallel)
source           item providers (basic, managed with ack/nack)
.pump_drive()    shared driver for pump_run/pump_drain
```

Backend contract:

* `.pump_executor_count(backend)` → number of workers
* `.pump_executor_register(backend, func, args)` → handle (called once per stage at first beat)
* `.pump_executor_new_job(backend, handle, data)` → job (called per item)
* `.pump_job_is_ready(job)` → logical
* `.pump_job_data(job)` → list(value, fn_time)
* `.pump_backend_open(backend)` → lifecycle open (no-op for user-managed backends)
* `.pump_backend_close(backend)` → lifecycle close (no-op for user-managed backends)

Backend-specific payload installation:

* `main_backend()`: stores func+args in handle; no persistent state
* `mirai_backend()`: installs runner via `mirai::everywhere()` on all connected daemons; static pool assumption
* `future_backend()`: no persistent worker state; func+globals travel with every job (future's automatic detection)
* `parallel_backend()`: installs runner on each node's global env and records in `setup_exprs` for recovery replay

Must-nots per layer:

* Stage functions must not depend on backend state. Backend is resolved at construction (explicit) or first beat (inherited).
* Sources must not manage pipeline execution. They only provide items and handle commit/abort/release.
* Backends must not understand pipeline structure. They only execute jobs and report results.
* The driver (`.pump_drive()`) must not know backend internals. It uses only the contract generics.

Load-bearing facts. Do not "fix" these:

* Backend resolution: explicit backends are resolved/validated at construction; inherited backends (NULL) are resolved/validated at first beat and stay fixed.
* Stage registration: `.pump_executor_register()` is called once per stage when the backend opens; this installs the stage's static payload (func+args) wherever the backend can keep it. Jobs then ship only the item data.
* Error handling: all job failures carry a `pump_error` condition class. Use `.pump_job_failure(e)` to construct the standard failure result.
* Result contract: every completed job yields `list(value, fn_time)`. Use `.pump_job_result(value, fn_time)` for success.
* Timeout checking is cooperative: it only works with async backends that return control to the main loop. Synchronous backends (main) cannot be interrupted.
* The `parallel_backend` fault tolerance: worker failures trigger automatic recovery via `.parallel_recover_worker()`, which replays `setup_exprs` including stage runner installations.

Public API stays close to: `pump()`, `pump_run()`, `pump_drain()`, `main_backend()`, `mirai_backend()`, `future_backend()`, `parallel_backend()`, `pump_source()`, `pump_managed_source()`. Do not expose backend internals or driver helpers.

Naming: `backend`, `stage`, `source`, `job`, `handle`, `slot`, `buffer`, `on_error`, `max_workers`, `buffer_size`. Prefer `item` over `element` internally.

## 3. Canonical extension: adding a backend

All new backends follow exactly this shape.

```r
#' @export
my_backend <- function(...) {
    # validate inputs
    structure(
        list(name = "my", owned = FALSE, ...),
        class = c("pump_my_backend", "pump_backend")
    )
}

#' @export
.pump_executor_count.pump_my_backend <- function(backend) {
    # return number of workers
}

#' @export
.pump_executor_register.pump_my_backend <- function(backend, func, args) {
    # install stage payload once; return opaque handle
    list(...)
}

#' @export
.pump_executor_new_job.pump_my_backend <- function(backend, handle, data) {
    # submit job using handle; return job object
    structure(list(result = ...), class = "pump_my_job")
}

#' @export
.pump_job_is_ready.pump_my_job <- function(job) {
    # check if job completed
}

#' @export
.pump_job_data.pump_my_job <- function(job) {
    # return list(value, fn_time)
}
```

Rules:

* The public backend function needs a full roxygen block (see documentation rules).
* Must inherit from `pump_backend` class for the print method and backend name resolution.
* If the backend owns resources (like parallel_backend's cluster), implement `.pump_backend_open()` and `.pump_backend_close()`.
* If the backend has persistent workers, use `.pump_executor_register()` to install the stage payload once per worker.
* Jobs must carry only the item data after registration; the handle contains func+args.
* All failures must surface as `pump_error` conditions via `.pump_job_failure()`.

## 4. Do not overbuild

Do not implement unless explicitly requested: remote backends beyond current set, operation registry, protected DSL validator, JSON pipeline serialization, remote file service, scheduler, storage abstraction, public driver API.

Leave design space. No scaffolding without current use.

## 5. Checklist before finishing

1. Added a dependency or R6? Remove it.
2. Final `return()`? Remove it.
3. Backend state inside stage objects? Move it out.
4. New S3 method without `#' @export`? Add it.
5. Mixed backend-specific logic in driver code? Move to backend methods.
6. Job failures without `pump_error` class? Use `.pump_job_failure()`.
7. Future infrastructure with no current use? Remove it.
8. Exported function missing title, `@param`, `@return`, or runnable `@examples`? Add them.
9. Ran `roxygen2::roxygenise()` and `R CMD check --as-cran`? Must be clean (see documentation rules).
