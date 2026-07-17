# Advance a pipeline without consuming results

`pump_step()` runs one or more beats on a pipeline. A beat lets the
terminal stage harvest finished jobs into its output buffer and pull new
work from upstream (recursively beating every upstream stage). Nothing
is consumed: stepping is safe to repeat while inspecting a pipeline with
[`print()`](https://rdrr.io/r/base/print.html),
[`pump_status()`](https://rolfsimoes.github.io/siphon/reference/pump_status.md),
or
[`pump_peek()`](https://rolfsimoes.github.io/siphon/reference/pump_peek.md).

## Usage

``` r
pump_step(x, beats = 1L)
```

## Arguments

- x:

  A pump object.

- beats:

  Number of beats to run. Defaults to 1. Stepping stops early once
  further beats cannot change anything: when the pipeline is finished
  (source exhausted, nothing in flight), or when it is fully stuck
  behind backpressure (output buffer full and all slots busy - pop or
  run to unblock it). Beat counts therefore do not inflate on a pipeline
  that cannot move.

## Value

`x`, visibly, so a bare `pump_step(p)` at the console also prints the
pipeline state.

## See also

Other inspection:
[`pump_peek()`](https://rolfsimoes.github.io/siphon/reference/pump_peek.md),
[`pump_pop()`](https://rolfsimoes.github.io/siphon/reference/pump_pop.md)

## Examples

``` r
p <- 1:5 |> pump(function(x) x * 2, backend = "main")
pump_step(p, 2)
#> <pump (2)>
#> ┌─ source   2/5
#> ├─ stage 1 main
#> │    wrk [-----] 0/1   buf [##---] 2/5   done 2   err 0
#> │    fn 0.0ms/it   crd 0.0ms/bt   wrk 100% stv 0% blk 0%
#> └─ sink   0/5
p # inspect state: items in flight and ready
#> <pump (2)>
#> ┌─ source   2/5
#> ├─ stage 1 main
#> │    wrk [-----] 0/1   buf [##---] 2/5   done 2   err 0
#> │    fn 0.0ms/it   crd 0.0ms/bt   wrk 100% stv 0% blk 0%
#> └─ sink   0/5
pump_peek(p) # look at the next result without consuming it
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
pump_run(p, verbose = FALSE) # resumes and completes the pipeline
#> [[1]]
#> [1] 2
#> 
#> [[2]]
#> [1] 4
#> 
#> [[3]]
#> [1] 6
#> 
#> [[4]]
#> [1] 8
#> 
#> [[5]]
#> [1] 10
#> 
```
