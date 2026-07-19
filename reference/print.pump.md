# Print a pump pipeline

Displays a pipeline as a connected frame from source to sink.

`print.pump()` displays a pipeline as a connected frame from source to
sink: worker and buffer occupancy per stage, timing, beat-state shares,
a stuck-job warning for old in-flight items, and a `* bottleneck` marker
when one stage clearly dominates. The sink line shows how many items
have left the pipeline. See
[`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md)
for the meaning of each metric.

The header shows the class name followed by the total number of beats
(pipeline scheduling cycles) in parentheses, e.g., `<pump (10)>`.

## Usage

``` r
# S3 method for class 'pump'
print(x, ...)
```

## Arguments

- x:

  A pump object.

- ...:

  Unused.

## Value

The input `x`, invisibly.

## Legend

Compressed labels used in the output:

- wrk:

  workers (active/limit)

- buf:

  output buffer (size/capacity)

- done:

  completed items

- err:

  errors

- fn:

  function time per item

- crd:

  coordination time per beat

- wrk/stv/blk:

  working/starved/blocked beat shares

## Customization

Colors can be customized via `options(siphon.colors = list(...))` to
override specific elements. The system uses standard 16-color ANSI
codes:

- bold = "1":

  bold text

- dim = "2":

  dimmed text

- blue = "34":

  primary color

- green = "32":

  success

- yellow = "33":

  warning

- red = "31":

  error

- bright_blue = "94":

  bright primary

- bright_green = "92":

  bright success

- bright_yellow = "93":

  bright warning

- bright_red = "91":

  bright error

Element-specific color names:

- header:

  pipeline header text

- source:

  "source" label

- sink:

  "sink" label

- stage:

  stage number (e.g., "stage 1")

- backend:

  backend name (e.g., "main", "mirai")

- wrk:

  workers label

- buf:

  buffer label

- done:

  completed label

- err:

  errors label

- fn:

  function time label

- crd:

  coordination time label

- beat:

  beats label

- wrk_share:

  working share label

- stv:

  starved share label

- blk:

  blocked share label

- bottleneck:

  bottleneck marker

Examples: `options(siphon.colors = list(stage = "96"))` to make stage
names cyan `options(siphon.colors = list(wrk = "1;34"))` to make workers
label bold blue `options(siphon.colors = list(bright_red = "35"))` to
change error color to purple To disable colors globally, set
`options(siphon.color = FALSE)`.
