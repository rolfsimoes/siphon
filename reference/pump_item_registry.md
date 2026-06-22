# Create an item registry for custom sources

Create a small key-value registry for item-level state owned by a custom
source. This is useful when a source emits serializable data to the
pipeline but must keep source-specific objects in the main R process.

## Usage

``` r
pump_item_registry(parent = emptyenv())
```

## Arguments

- parent:

  Parent environment for the internal registry.

## Value

A list of registry methods: `set()`,
[`get()`](https://rdrr.io/r/base/get.html), `has()`,
[`remove()`](https://rdrr.io/r/base/rm.html), `ids()`, `clear()`, and
`drain()`.
