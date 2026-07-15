# Findings: `liteq::try_consume()` blocking with in-flight (WORKING) messages

## Summary

A siphon pipeline using a `liteq`-backed `pump_managed_source()` with a
**parallel backend** (`mirai_backend()` / `future_backend()` / `parallel_backend()`) appears to hang.
The stall is **not** in siphon. It is caused by `liteq::try_consume()`'s
crash-recovery path, which blocks on the SQLite busy timeout (~10s) for **each
in-flight (`WORKING`) message** whenever it is called while no `READY` message
is available.

This affects both `pump_drain()` and `pump_run()`. `pump_run()` merely *finishes*
(slowly) because the source is finite; a long-running/infinite `pump_drain()`
daemon looks like a permanent hang.

## Root cause (in `liteq`, not siphon)

`liteq::try_consume()` -> `liteq:::db_try_consume()`:

```r
# only runs crash recovery when there is NO READY message
if (crashed && db_clean_crashed(con, queue)) { ... }
```

`liteq:::db_clean_crashed()` probes every `WORKING` message's lock file:

```r
lcon <- db_connect(lock)
dbGetQuery(lcon, "SELECT * FROM foo")   # blocks on SQLite busy timeout if lock held
```

Because the live consumer (the main R process) still holds the lock for each
checked-out-but-not-yet-acked message, this probe blocks for the full SQLite
busy timeout (~10s) per in-flight message, then concludes "still alive".

A parallel siphon stage keeps several messages `WORKING` while their jobs run on
workers. As soon as the queue has no `READY` messages but in-flight ones remain,
siphon's `advance()` calls `try_consume()` -> no `READY` -> `db_clean_crashed()`
-> blocks ~10s x (in-flight messages).

## Minimal reproduction (pure liteq, no siphon, no parallelism)

See `inst/scripts/repro_minimal.R`:

```
consume #1 (READY available)             0.01s -> 1
consume #2 (READY available)             0.01s -> 2
consume #3 (READY available)             0.02s -> 3
consume #4 (0 READY, 3 WORKING)          30.03s -> NULL   # 3 x ~10s
```

## Evidence matrix

| Scenario                                              | Result            |
|-------------------------------------------------------|-------------------|
| Pure liteq, consume all READY then `try_consume`      | blocks ~N x 10s   |
| liteq + idle parallel workers                         | fast              |
| liteq + actively running plain futures                | fast              |
| siphon parallel pipeline + liteq + `pump_drain`       | hangs (perceived) |
| siphon parallel pipeline + liteq + `pump_run` (finite)| completes ~30s    |
| siphon parallel pipeline + in-memory managed source   | works, fast       |

## Workaround (applied in this repo)

Only call `try_consume()` when a `READY` message actually exists, so the
crash-recovery branch is never triggered:

```r
n_ready <- function(queue) {
    msgs <- liteq::list_messages(queue)
    if (nrow(msgs) == 0L) 0L else sum(msgs$status == "READY")
}

pull_fn = function() {
    if (n_ready(queue) < 1L) return(NULL)
    liteq::try_consume(queue)
}
```

Note: `liteq::is_empty()` cannot be used as the guard because it counts *all*
messages (including `WORKING`), so it stays `FALSE` while jobs are in flight.

With this guard, the parallel + `pump_drain()` daemon completes promptly
(see `inst/scripts/daemon.R`). A small race remains: another consumer could take
the message between the count and the `try_consume()` call; in that rare case
`try_consume()` returns `NULL` after running crash recovery. For a single daemon
this does not occur.

## Suggested upstream fix for `liteq`

Set a short/zero SQLite `busy_timeout` (PRAGMA) on the lock-file probe in
`db_clean_crashed()`, or expose a `crashed = FALSE` option on the public
`try_consume()` so callers that manage their own lifecycle can skip
crash recovery.
