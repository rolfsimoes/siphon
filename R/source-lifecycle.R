#' @title Create an item registry for custom sources
#' @name pump_item_registry
#' @description
#' Create a small key-value registry for item-level state owned by a custom
#' source. This is useful when a source emits serializable data to the pipeline
#' but must keep source-specific objects in the main R process.
#'
#' @param parent Parent environment for the internal registry.
#'
#' @return
#' A list of registry methods: `set()`, `get()`, `has()`, `remove()`,
#' `ids()`, `clear()`, and `drain()`.
#'
#' @export
pump_item_registry <- function(parent = emptyenv()) {
    env <- new.env(parent = parent)

    key <- function(id) {
        as.character(id)
    }

    list(
        set = function(id, value) {
            assign(key(id), value, envir = env)
            invisible(value)
        },

        get = function(id) {
            get(key(id), envir = env, inherits = FALSE)
        },

        has = function(id) {
            exists(key(id), envir = env, inherits = FALSE)
        },

        remove = function(id) {
            k <- key(id)
            if (exists(k, envir = env, inherits = FALSE)) {
                rm(list = k, envir = env)
            }
            invisible(NULL)
        },

        ids = function() {
            ls(envir = env, all.names = TRUE)
        },

        clear = function() {
            ids <- ls(envir = env, all.names = TRUE)
            if (length(ids) > 0L) {
                rm(list = ids, envir = env)
            }
            invisible(NULL)
        },

        drain = function(fn) {
            ids <- ls(envir = env, all.names = TRUE)

            for (id in ids) {
                value <- get(id, envir = env, inherits = FALSE)
                fn(id, value)
            }

            if (length(ids) > 0L) {
                rm(list = ids, envir = env)
            }

            invisible(NULL)
        }
    )
}

#' @title Create a managed custom source
#' @name pump_managed_source
#' @description
#' Create a custom source that keeps source-owned items in the main R process
#' while sending only serializable data through the pipeline.
#'
#' This helper is useful for queues, databases, streams, and other sources where
#' pulling an item creates a later control action, such as committing, aborting,
#' or releasing that item.
#'
#' @param pull_fn Function that returns one source-owned item or `NULL`.
#' @param id_fn Function that extracts the item id.
#' @param data_fn Function that extracts serializable data for the pipeline.
#' @param commit_fn Function called when the item is accepted by the terminal runner.
#' @param abort_fn Function called when the item leaves the runtime before acceptance.
#' @param release_fn Optional function called when item-level tracking is released.
#' @param done_fn Optional source-level done function.
#' @param close_fn Optional source-level close function.
#' @param length Source length or function returning source length.
#'
#' @return A `pump` source object.
#'
#' @export
pump_managed_source <- function(pull_fn,
                                id_fn,
                                data_fn,
                                commit_fn,
                                abort_fn,
                                release_fn = NULL,
                                done_fn = NULL,
                                close_fn = NULL,
                                length = Inf) {
    if (!is.function(pull_fn)) stop("pull_fn must be a function")
    if (!is.function(id_fn)) stop("id_fn must be a function")
    if (!is.function(data_fn)) stop("data_fn must be a function")
    if (!is.function(commit_fn)) stop("commit_fn must be a function")
    if (!is.function(abort_fn)) stop("abort_fn must be a function")
    if (!is.null(release_fn) && !is.function(release_fn)) {
        stop("release_fn must be NULL or a function")
    }

    registry <- pump_item_registry()

    pump_source(
        pull_fn = function() {
            item <- pull_fn()
            if (is.null(item)) return(NULL)

            id <- id_fn(item)
            registry$set(id, item)

            data <- tryCatch(
                data_fn(item),
                error = function(e) e
            )

            list(
                id = id,
                data = data,
                ok = !inherits(data, "error")
            )
        },

        item_commit_fn = function(id, data) {
            item <- registry$get(id)
            commit_fn(item, data)
        },

        item_abort_fn = function(id, error = NULL, data = NULL) {
            item <- registry$get(id)
            abort_fn(item, error = error, data = data)
        },

        item_release_fn = function(id) {
            item <- registry$get(id)

            if (!is.null(release_fn)) {
                release_fn(item)
            }

            registry$remove(id)
        },

        done_fn = done_fn,

        close_fn = function() {
            registry$drain(function(id, item) {
                try(
                    abort_fn(item, error = NULL, data = NULL),
                    silent = TRUE
                )
            })

            if (!is.null(close_fn)) {
                close_fn()
            }

            invisible(NULL)
        },

        length = length
    )
}
