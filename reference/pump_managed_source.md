# Create a managed custom source

Create a custom source that keeps source-owned items in the main R
process while sending only serializable data through the pipeline.

This helper is useful for queues, databases, streams, and other sources
where pulling an item creates a later control action, such as
committing, aborting, or releasing that item.

## Usage

``` r
pump_managed_source(
  pull_fn,
  id_fn,
  data_fn,
  commit_fn,
  abort_fn,
  release_fn = NULL,
  done_fn = NULL,
  close_fn = NULL,
  length = Inf
)
```

## Arguments

- pull_fn:

  Function that returns one source-owned item or `NULL`.

- id_fn:

  Function that extracts the item id.

- data_fn:

  Function that extracts serializable data for the pipeline.

- commit_fn:

  Function called when the item is accepted by the terminal runner.

- abort_fn:

  Function called when the item leaves the runtime before acceptance.

- release_fn:

  Optional function called when item-level tracking is released.

- done_fn:

  Optional source-level done function.

- close_fn:

  Optional source-level close function.

- length:

  Source length or function returning source length.

## Value

A `pump` source object.
