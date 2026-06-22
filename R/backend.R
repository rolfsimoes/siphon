.pump_resolve_backend <- function(backend) {
    if (!is.character(backend)) {
        return(backend)
    }
    switch(backend,
        future = future_backend(),
        mirai = mirai_backend(),
        main = main_backend(),
        stop("invalid backend")
    )
}

.pump_executor_count <- function(backend) {
    UseMethod(".pump_executor_count")
}
.pump_executor_new_job <- function(backend, func, args) {
    UseMethod(".pump_executor_new_job")
}
.pump_job_is_ready <- function(job) {
    UseMethod(".pump_job_is_ready")
}
.pump_job_data <- function(job) {
    UseMethod(".pump_job_data")
}

.make_job <- function(expr) {
    start <- Sys.time()
    val <- tryCatch(expr, error = function(e) {
        class(e) <- c("pump_error", class(e))
        e
    })
    list(value = val, fn_time = as.numeric(difftime(Sys.time(), start, units = "secs")) * 1000)
}
