# Consume one ready result from a pipeline

`pump_pop()` removes and returns the next ready item from the terminal
stage's output buffer, or `NULL` when nothing is ready. **This consumes
the item**: it will not appear in the results of a later
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md),
and responsibility for its lifecycle transfers to you. For managed
sources (see
[`pump_source()`](https://rolfsimoes.github.io/siphon/reference/pump_source.md)),
call `x$item_commit(id, data)` and `x$item_release(id)` after
successfully handling the item, exactly as
[`pump_run()`](https://rolfsimoes.github.io/siphon/reference/pump_run.md)
does; use
[`pump_peek()`](https://rolfsimoes.github.io/siphon/reference/pump_peek.md)
instead when you only want to look.

## Usage

``` r
pump_pop(x)
```

## Arguments

- x:

  A pump object.

## Value

A list with `id`, `idx`, `data`, and `ok`, or `NULL` when no item is
ready.

## See also

Other inspection:
[`pump_peek()`](https://rolfsimoes.github.io/siphon/reference/pump_peek.md),
[`pump_step()`](https://rolfsimoes.github.io/siphon/reference/pump_step.md)

## Examples

``` r
p <- 1:5 |> pump(function(x) x * 2, backend = "main")
pump_step(p)
#> <pump (1)>
#> ┌─ source   1/5
#> ├─ stage 1 main
#> │    wrk [-----] 0/1   buf [#----] 1/5   done 1   err 0
#> │    fn 0.0ms/it   crd 1.0ms/bt   wrk 100% stv 0% blk 0%
#> └─ sink   0/5
v <- pump_pop(p)
v$data
#> [1] 2
```
