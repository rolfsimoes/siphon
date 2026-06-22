#!/usr/bin/env Rscript
# Minimal reproduction of liteq try_consume() blocking.
# No siphon, no parallel backend involved.
library(liteq)

tt <- function(label, expr) {
    t0 <- Sys.time()
    v <- force(expr)
    cat(sprintf("%-40s %.2fs -> %s\n", label,
                as.numeric(difftime(Sys.time(), t0, units = "secs")),
                if (is.null(v)) "NULL" else v$id))
    v
}

db <- tempfile(fileext = ".db")
q <- liteq::ensure_queue("jobs", db = db)
for (i in 1:3) liteq::publish(q, title = paste(i), message = paste(i))

# Consume ALL ready messages, leaving them WORKING (un-acked), as a parallel
# pipeline does while jobs are in flight.
m1 <- tt("consume #1 (READY available)", liteq::try_consume(q))
m2 <- tt("consume #2 (READY available)", liteq::try_consume(q))
m3 <- tt("consume #3 (READY available)", liteq::try_consume(q))

# Now NO message is READY, but 3 are WORKING. try_consume() runs crash
# recovery and blocks on the SQLite busy timeout for each held lock.
m4 <- tt("consume #4 (0 READY, 3 WORKING)", liteq::try_consume(q))
