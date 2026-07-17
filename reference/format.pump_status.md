# Format a pipeline status snapshot

Renders a `pump_status` object as a character vector, one element per
output line. Used by the print methods for `pump_status` and `pump`. The
pipeline is drawn as one connected frame from source to sink.

## Usage

``` r
# S3 method for class 'pump_status'
format(
  x,
  ...,
  header = "pump_status",
  color = .pump_use_color(),
  unicode = .pump_use_unicode()
)
```

## Arguments

- x:

  A `pump_status` object.

- ...:

  Unused.

- header:

  First line of the output.

- color:

  Whether to use ANSI colors. Defaults to terminal detection; set
  `options(siphon.color = FALSE)` to disable globally.

- unicode:

  Whether to use box-drawing characters for the frame. Defaults to
  locale detection; set `options(siphon.unicode = FALSE)` to force the
  ASCII frame.

## Value

A character vector of lines.
