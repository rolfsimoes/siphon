# Peek at ready results without consuming them

`pump_peek()` returns up to `n` items that are ready in the terminal
stage's output buffer, without removing them: calling it repeatedly
returns the same items, and the pipeline is unaffected.

## Usage

``` r
pump_peek(x, n = 1L)
```

## Arguments

- x:

  A pump object.

- n:

  Maximum number of items to return. Defaults to 1.

## Value

A list of up to `n` ready items (empty when nothing is ready). Sources
have no buffer, so peeking a bare source returns an empty list.

## Details

Each item is a list with `id`, `idx`, `data`, and `ok`. Use
[`pump_step()`](https://rolfsimoes.github.io/siphon/reference/pump_step.md)
first to advance work into the buffer.

## See also

Other inspection:
[`pump_pop()`](https://rolfsimoes.github.io/siphon/reference/pump_pop.md),
[`pump_step()`](https://rolfsimoes.github.io/siphon/reference/pump_step.md)

## Examples

``` r
p <- 1:5 |> pump(function(x) x * 2, backend = "main")
pump_step(p)
#> <pump (1)>
#> ┌─ source   1/5
#> ├─ stage 1 main
#> │    wrk [-----] 0/1   buf [#----] 1/5   done 1   err 0
#> │    fn 0.0ms/it   crd 0.0ms/bt   wrk 100% stv 0% blk 0%
#> └─ sink   0/5
pump_peek(p)
#> [[1]]
#> [[1]]$id
#> [1] 1
#> 
#> [[1]]$idx
#> [1] 1
#> 
#> [[1]]$data
#> [1] 2
#> 
#> [[1]]$ok
#> [1] TRUE
#> 
#> 
```
