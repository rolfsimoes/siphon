# Create a main-thread backend

`main_backend()` executes jobs synchronously in the current R process.
It is the default backend and is useful for stages that must stay on the
main thread, such as GPU dispatchers.

## Usage

``` r
main_backend()
```

## Value

A backend object.

## Examples

``` r
f <- 1:5 |> pump(function(x) x * 2, backend = main_backend())
pump_run(f, verbose = FALSE)
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
