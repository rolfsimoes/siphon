#!/usr/bin/env Rscript

# Test publisher for siphon + liteq daemon
# This script publishes test messages to a liteq queue for the daemon to process.
#
# Usage:
#   Rscript inst/scripts/publisher.R /path/to/db.sqlite
#
#   With custom queue name:
#   Rscript inst/scripts/publisher.R /path/to/db.sqlite --queue myqueue

library(liteq)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)

# Parse arguments
db_path <- NULL
queue_name <- "jobs"

i <- 1
while (i <= length(args)) {
  if (args[i] == "--queue" && i + 1 <= length(args)) {
    queue_name <- args[i + 1]
    i <- i + 2
  } else if (is.null(db_path)) {
    db_path <- args[i]
    i <- i + 1
  } else {
    i <- i + 1
  }
}

if (is.null(db_path)) {
  stop("Please provide a database path as the first argument")
}

# Ensure queue exists
q <- liteq::ensure_queue(queue_name, db = db_path)

message("Publishing to queue: ", queue_name)
message("Database: ", db_path)

# Publish test messages (from vignette)
message("\n--- Publishing test messages ---")
liteq::publish(
  q,
  title = "Job 1",
  message = jsonlite::toJSON(list(x = 5), auto_unbox = TRUE)
)
message("Published: Job 1 (x = 5)")

liteq::publish(
  q,
  title = "Job 2",
  message = jsonlite::toJSON(list(x = 10), auto_unbox = TRUE)
)
message("Published: Job 2 (x = 10)")

liteq::publish(
  q,
  title = "Job 3",
  message = jsonlite::toJSON(list(x = 15), auto_unbox = TRUE)
)
message("Published: Job 3 (x = 15)")

message("\nDone! Check the daemon output for processing results.")
