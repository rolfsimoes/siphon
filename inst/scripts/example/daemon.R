#!/usr/bin/env Rscript

# Test daemon for siphon + liteq
# This script runs a background daemon that consumes from a liteq queue
# and processes tasks in parallel using siphon + mirai.
#
# Usage:
#   Rscript inst/scripts/daemon.R /path/to/db.sqlite
#
# Stop the daemon by pressing Ctrl-C

library(siphon)
library(liteq)
library(mirai)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  db_path <- tempfile(fileext = ".db")
  message("No database path provided, using temporary: ", db_path)
} else {
  db_path <- args[1]
}

queue_name <- "jobs"
n_workers <- 4
sleep_ms <- 100

# Count READY messages in the queue.
#
# This guard is required because liteq::try_consume() runs crash recovery
# whenever it is called with no READY message available, blocking on the SQLite
# busy timeout (~10s) for each in-flight (WORKING) message. A parallel siphon
# stage keeps several messages WORKING while jobs run, so we must only consume
# when a READY message actually exists. liteq::is_empty() cannot be used here:
# it counts all messages (including WORKING), so it stays FALSE while jobs are
# in flight. See inst/scripts/liteq-blocking-findings.md for details.
n_ready <- function(queue) {
  msgs <- liteq::list_messages(queue)
  if (nrow(msgs) == 0L) 0L else sum(msgs$status == "READY")
}

# Create the managed liteq source (from vignette)
make_liteq_source <- function(queue, run_forever = TRUE) {
  pump_managed_source(
    pull_fn = function() {
      if (n_ready(queue) < 1L) return(NULL)
      liteq::try_consume(queue)
    },

    id_fn = function(msg) {
      msg$id
    },

    data_fn = function(msg) {
      jsonlite::fromJSON(msg$message)
    },

    commit_fn = function(msg, data) {
      liteq::ack(msg)
    },

    abort_fn = function(msg, error = NULL, data = NULL) {
      liteq::nack(msg)
    },

    done_fn = if (run_forever) {
      function() FALSE
    } else {
      function() liteq::is_empty(queue)
    }
  )
}

# Save result function
save_result <- function(task_id, result) {
  message("Task ", task_id, " -> ", result)
}

# Main daemon function (from vignette)
run_mirai_daemon <- function(db_path,
                             queue_name = "jobs",
                             n_workers = 4,
                             sleep_ms = 100) {
  q <- liteq::ensure_queue(queue_name, db = db_path)

  mirai::daemons(n_workers)
  on.exit(mirai::daemons(0), add = TRUE)

  pipeline <- make_liteq_source(q) |>
    pump(
      function(payload) {
        payload$x * 10
      },
      backend = "mirai",
      max_workers = n_workers,
      buffer_size = n_workers
    )

  message("Daemon started. Polling queue: ", queue_name)
  message("Database: ", db_path)
  message("Press Ctrl-C to stop")

  pump_drain(
    pipeline,
    handle_fn = function(id, data, ok) {
      if (ok && !inherits(data, "error")) {
        save_result(id, data)
      }
    },
    sleep_ms = sleep_ms
  )
}

# Run the daemon
run_mirai_daemon(db_path, queue_name, n_workers, sleep_ms)
