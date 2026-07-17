# Rendering layer for pump_status and pump objects.
#
# format.pump_status() returns a character vector (one element per line) so
# output is snapshot-testable; the print methods just cat() it. Colors are
# hand-rolled ANSI kept behind .pump_use_color() so the package needs no hard
# dependencies; every signal (bottleneck, blocked, errors) also survives with
# colors off. The pipeline is drawn as one connected frame from source to
# sink; box-drawing glyphs degrade to ASCII in non-UTF-8 locales.

.pump_use_color <- function() {
    opt <- getOption("siphon.color", NULL)
    if (!is.null(opt)) {
        return(isTRUE(opt))
    }
    if (requireNamespace("cli", quietly = TRUE)) {
        return(cli::num_ansi_colors() > 1L)
    }
    if (nzchar(Sys.getenv("NO_COLOR"))) {
        return(FALSE)
    }
    isatty(stdout()) || identical(Sys.getenv("RSTUDIO"), "1")
}

.pump_use_unicode <- function() {
    opt <- getOption("siphon.unicode", NULL)
    if (!is.null(opt)) {
        return(isTRUE(opt))
    }
    isTRUE(l10n_info()[["UTF-8"]])
}

.pump_style <- function(x, style, use = TRUE) {
    if (!use || !nzchar(x)) {
        return(x)
    }
    # Default Nord-inspired palette using standard 16-color ANSI for compatibility
    # Base styles
    default_codes <- c(
        bold = "1",
        dim = "2",
        # Primary colors (Nord-inspired)
        blue = "34",        # frost blue (primary)
        cyan = "36",        # lighter frost
        # Status colors (aurora)
        green = "32",       # success
        yellow = "33",      # warning
        orange = "33",      # alert (using yellow)
        red = "31",         # error
        purple = "35",      # info
        # Bright variants for better visibility
        bright_blue = "94",
        bright_cyan = "96",
        bright_green = "92",
        bright_yellow = "93",
        bright_red = "91",
        bright_purple = "95"
    )
    
    # Element-specific colors (can be overridden via options)
    element_colors <- c(
        header = "1",              # bold
        source = "1",              # bold
        sink = "1",                # bold
        stage = "94",              # bright blue
        backend = "",              # no color (black)
        wrk = "2",                 # dim
        buf = "2",                 # dim
        done = "2",                # dim
        err = "2",                 # dim (values use bright_red when > 0)
        fn = "2",                  # dim
        crd = "2",                 # dim
        beat = "2",                # dim
        wrk_share = "92",          # bright green (was work)
        stv = "93",                # bright yellow (was starv)
        blk = "91",                # bright red (was block)
        bottleneck = "91"          # bright red
    )
    
    # Allow custom colors via options(siphon.colors = list(...))
    custom_colors <- getOption("siphon.colors", NULL)
    if (!is.null(custom_colors) && is.list(custom_colors)) {
        # Merge custom colors with both base codes and element colors
        for (name in names(custom_colors)) {
            if (name %in% names(default_codes)) {
                default_codes[name] <- custom_colors[[name]]
            }
            if (name %in% names(element_colors)) {
                element_colors[name] <- custom_colors[[name]]
            }
        }
    }
    
    # Combine all codes
    codes <- c(default_codes, element_colors)
    
    # Handle compound styles like "bold blue"
    style_parts <- strsplit(style, " ")[[1]]
    ansi_codes <- unname(codes[style_parts])
    # Filter out empty strings
    ansi_codes <- ansi_codes[nzchar(ansi_codes)]
    if (length(ansi_codes) > 1) {
        ansi_code <- paste(ansi_codes, collapse = ";")
    } else if (length(ansi_codes) == 1) {
        ansi_code <- ansi_codes
    } else {
        ansi_code <- ""
    }
    
    if (nzchar(ansi_code)) {
        paste0("\x1b[", ansi_code, "m", x, "\x1b[0m")
    } else {
        x
    }
}

# Width of a string as seen on screen, ignoring ANSI escapes
.pump_visible_width <- function(x) {
    nchar(gsub("\033\\[[0-9;]*m", "", x))
}

.pump_pad_to <- function(x, width) {
    paste0(x, strrep(" ", max(0L, width - .pump_visible_width(x))))
}

# Occupancy bar like [##--]; fixed width of 5 chars inside brackets.
# The fill is colored (green, or red when full-and-meaningful), the empty
# part is dimmed.
.pump_bar <- function(filled, capacity, color, fill_style = "green",
                      width = 5L) {
    if (!is.finite(capacity) || capacity < 1L) {
        return("[]")
    }
    n_fill <- as.integer(round(filled / capacity * width))
    if (filled > 0L) n_fill <- max(n_fill, 1L)
    n_fill <- min(n_fill, width)
    paste0(
        "[",
        .pump_style(strrep("#", n_fill), fill_style, color),
        .pump_style(strrep("-", width - n_fill), "dim", color),
        "]"
    )
}

.pump_fmt_ms <- function(ms) {
    if (is.null(ms) || !is.finite(ms)) {
        return("--")
    }
    if (ms >= 10000) {
        sprintf("%.1fs", ms / 1000)
    } else if (ms >= 100) {
        sprintf("%.0fms", ms)
    } else {
        sprintf("%.1fms", ms)
    }
}

.pump_fmt_share <- function(share) {
    if (is.null(share) || !is.finite(share)) {
        return("--")
    }
    sprintf("%.0f%%", share * 100)
}

.pump_fmt_len <- function(n) {
    if (!is.finite(n)) "Inf" else format(n)
}

# Pick the bottleneck stage index, or NA when there is no clear signal.
# Heuristic: the stage with the highest working share, provided it has seen
# enough beats to be meaningful, is working most of the time, and its
# neighbors corroborate (downstream starving, or upstream blocked behind it).
.pump_find_bottleneck <- function(stages) {
    n <- length(stages)
    if (n < 2L) {
        return(NA_integer_)
    }
    shares <- vapply(stages, function(s) {
        if (isTRUE(s$beats >= 5L) && is.finite(s$share_working)) {
            s$share_working
        } else {
            -Inf
        }
    }, numeric(1))
    cand <- which.max(shares)
    if (!is.finite(shares[cand]) || shares[cand] < 0.5) {
        return(NA_integer_)
    }
    downstream_starved <- cand < n &&
        isTRUE(stages[[cand + 1L]]$share_starved > 0.25)
    upstream_blocked <- cand > 1L &&
        isTRUE(stages[[cand - 1L]]$share_blocked > 0.25)
    if (downstream_starved || upstream_blocked) {
        cand
    } else {
        NA_integer_
    }
}

#' Format a pipeline status snapshot
#'
#' Renders a `pump_status` object as a character vector, one element per
#' output line. Used by the print methods for `pump_status` and `pump`.
#' The pipeline is drawn as one connected frame from source to sink.
#'
#' @param x A `pump_status` object.
#' @param ... Unused.
#' @param header First line of the output.
#' @param color Whether to use ANSI colors. Defaults to terminal detection;
#'   set `options(siphon.color = FALSE)` to disable globally.
#' @param unicode Whether to use box-drawing characters for the frame.
#'   Defaults to locale detection; set `options(siphon.unicode = FALSE)` to
#'   force the ASCII frame.
#'
#' @return A character vector of lines.
#' @export
format.pump_status <- function(x, ...,
                               header = "pump_status",
                               color = .pump_use_color(),
                               unicode = .pump_use_unicode()) {
    dim_ <- function(txt) .pump_style(txt, "dim", color)

    # Flat source status (no stages)
    if (is.null(x$stages)) {
        lines <- c(
            .pump_style(header, "bold", color),
            paste0("  ", dim_("pulled:"), "  ", x$completed %||% 0L),
            paste0("  ", dim_("errors:"), "  ", x$errors %||% 0L),
            paste0(
                "  ", dim_("pops:"), "    ",
                x$pop_hits %||% 0L, " hits, ", x$pop_misses %||% 0L, " misses"
            )
        )
        return(lines)
    }

    g <- if (unicode) {
        # box-drawing glyphs as \u escapes to keep the source ASCII-only
        c(
            top = "\u250c\u2500", mid = "\u251c\u2500",
            pipe = "\u2502", bot = "\u2514\u2500"
        )
    } else {
        c(top = "+-", mid = "+-", pipe = "|", bot = "+-")
    }

    stages <- x$stages
    bottleneck <- .pump_find_bottleneck(stages)
    
    # Calculate total beats across all stages for header
    total_beats <- if (length(stages) > 0) {
        sum(vapply(stages, function(s) s$beats %||% 0L, integer(1)))
    } else {
        0L
    }
    
    # Build header with class and beats
    if (total_beats > 0) {
        header_with_beats <- paste0("<", header, " (", total_beats, ")>")
    } else {
        header_with_beats <- paste0("<", header, ">")
    }
    lines <- .pump_style(header_with_beats, "header", color)

    # --- source (top of the frame) ---
    if (!is.null(x$source)) {
        s <- x$source
        src_line <- paste0(
            dim_(g[["top"]]), " ", .pump_style("source", "source", color),
            "   ", s$position, "/", .pump_fmt_len(s$length)
        )
        if (s$errors > 0) {
            src_line <- paste0(
                src_line, "   ", .pump_style(sprintf("e %d", s$errors), "red", color)
            )
        }
        lines <- c(lines, src_line)
    }

    # --- stages: build the occupancy segments first so they can be padded
    # to aligned columns (padding is ANSI-aware) ---
    seg_work <- character(length(stages))
    seg_out <- character(length(stages))
    seg_done <- character(length(stages))
    for (i in seq_along(stages)) {
        s <- stages[[i]]
        buffer_full <- s$buffer_capacity > 0L && s$buffer_size >= s$buffer_capacity
        seg_work[i] <- paste0(
            .pump_style("wrk", "wrk", color), " ",
            .pump_bar(s$workers_active, s$workers_limit, color),
            " ", s$workers_active, "/", s$workers_limit
        )
        seg_out[i] <- paste0(
            .pump_style("buf", "buf", color), " ",
            .pump_bar(
                s$buffer_size, s$buffer_capacity, color,
                fill_style = if (buffer_full) "red" else "green"
            ),
            " ", s$buffer_size, "/", s$buffer_capacity
        )
        seg_done[i] <- paste0(.pump_style("done", "done", color), " ", s$completed)
    }
    w_work <- max(vapply(seg_work, .pump_visible_width, integer(1)))
    w_out <- max(vapply(seg_out, .pump_visible_width, integer(1)))
    w_done <- max(vapply(seg_done, .pump_visible_width, integer(1)))

    for (i in seq_along(stages)) {
        s <- stages[[i]]
        is_bottleneck <- !is.na(bottleneck) && i == bottleneck

        glyph <- if (is.null(x$source) && i == 1L) g[["top"]] else g[["mid"]]
        label <- sprintf("stage %d", i)
        if (is_bottleneck) {
            label <- paste0(
                .pump_style(label, "stage", color), " ", .pump_style(s$type, "backend", color), "  ",
                .pump_style("* bottleneck", "bottleneck", color)
            )
        } else {
            label <- paste0(.pump_style(label, "stage", color), " ", .pump_style(s$type, "backend", color))
        }
        lines <- c(lines, paste0(dim_(glyph), " ", label))

        err_txt <- if (s$errors > 0) {
            .pump_style(sprintf("err %d", s$errors), "bright_red", color)
        } else {
            .pump_style("err 0", "err", color)
        }
        lines <- c(lines, paste0(
            dim_(g[["pipe"]]), "    ",
            .pump_pad_to(seg_work[i], w_work), "   ",
            .pump_pad_to(seg_out[i], w_out), "   ",
            .pump_pad_to(seg_done[i], w_done), "   ",
            err_txt
        ))

        if (isTRUE(s$beats > 0L)) {
            share_txt <- function(share, style, name) {
                val <- .pump_fmt_share(share)
                val <- if (isTRUE(share > 0)) {
                    .pump_style(val, style, color)
                } else {
                    .pump_style(val, "dim", color)
                }
                paste0(.pump_style(name, name, color), " ", val)
            }
            lines <- c(lines, paste0(
                dim_(g[["pipe"]]), "    ",
                .pump_style("fn", "fn", color), " ", .pump_fmt_ms(s$fn_per_item), .pump_style("/it", "fn", color), "   ",
                .pump_style("crd", "crd", color), " ", .pump_fmt_ms(s$coord_time / s$beats), .pump_style("/bt", "crd", color), "   ",
                share_txt(s$share_working, "wrk_share", "wrk"), " ",
                share_txt(s$share_starved, "stv", "stv"), " ",
                share_txt(s$share_blocked, "blk", "blk")
            ))
        }

        # Stuck-job tell: an in-flight item much older than the average
        # per-item time deserves attention
        if (length(s$in_flight) > 0 && isTRUE(s$fn_per_item > 0)) {
            ages <- vapply(s$in_flight, function(fl) {
                as.numeric(difftime(Sys.time(), fl$since, units = "secs"))
            }, numeric(1))
            oldest <- which.max(ages)
            if (ages[oldest] * 1000 > 10 * s$fn_per_item && ages[oldest] > 1) {
                lines <- c(lines, paste0(
                    dim_(g[["pipe"]]), "    ",
                    .pump_style(sprintf(
                        "oldest in flight: %.1fs (id %s)",
                        ages[oldest], format(s$in_flight[[oldest]]$id)
                    ), "yellow", color)
                ))
            }
        }
    }

    # --- sink (bottom of the frame): items that left the pipeline ---
    total <- if (!is.null(x$source)) .pump_fmt_len(x$source$length) else NULL
    sink_line <- paste0(
        dim_(g[["bot"]]), " ", .pump_style("sink", "sink", color), "   ",
        x$delivered %||% 0L,
        if (!is.null(total)) paste0("/", total) else ""
    )
    c(lines, sink_line)
}

#' @title Print a pump_status object
#' @description Displays a pump_status snapshot.
#' @param x A pump_status object.
#' @param ... Unused.
#' @export
print.pump_status <- function(x, ...) {
    cat(format(x), sep = "\n")
    invisible(x)
}

#' @title Format a pump pipeline
#' @description Formats a pump pipeline for display.
#' @param x A pump object.
#' @param ... Unused.
#' @return A character vector of lines.
#' @export
format.pump <- function(x, ...) {
    format(pump_status(x), header = "pump")
}

#' @title Print a pump pipeline
#' @description Displays a pipeline as a connected frame from source to sink.
#'
#' `print.pump()` displays a pipeline as a connected frame from source to
#' sink: worker and buffer occupancy per stage, timing, beat-state shares, a
#' stuck-job warning for old in-flight items, and a `* bottleneck` marker
#' when one stage clearly dominates. The sink line shows how many items have
#' left the pipeline. See [pump_status()] for the meaning of each metric.
#'
#' The header shows the class name followed by the total number of beats
#' (pipeline scheduling cycles) in parentheses, e.g., `<pump (10)>`.
#'
#' @section Legend:
#' Compressed labels used in the output:
#' \describe{
#'   \item{wrk}{workers (active/limit)}
#'   \item{buf}{output buffer (size/capacity)}
#'   \item{done}{completed items}
#'   \item{err}{errors}
#'   \item{fn}{function time per item}
#'   \item{crd}{coordination time per beat}
#'   \item{wrk/stv/blk}{working/starved/blocked beat shares}
#' }
#'
#' @section Customization:
#' Colors can be customized via `options(siphon.colors = list(...))` to override
#' specific elements. The system uses standard 16-color ANSI codes:
#' \describe{
#'   \item{bold = "1"}{bold text}
#'   \item{dim = "2"}{dimmed text}
#'   \item{blue = "34"}{primary color}
#'   \item{green = "32"}{success}
#'   \item{yellow = "33"}{warning}
#'   \item{red = "31"}{error}
#'   \item{bright_blue = "94"}{bright primary}
#'   \item{bright_green = "92"}{bright success}
#'   \item{bright_yellow = "93"}{bright warning}
#'   \item{bright_red = "91"}{bright error}
#' }
#' 
#' Element-specific color names:
#' \describe{
#'   \item{header}{pipeline header text}
#'   \item{source}{"source" label}
#'   \item{sink}{"sink" label}
#'   \item{stage}{stage number (e.g., "stage 1")}
#'   \item{backend}{backend name (e.g., "main", "mirai")}
#'   \item{wrk}{workers label}
#'   \item{buf}{buffer label}
#'   \item{done}{completed label}
#'   \item{err}{errors label}
#'   \item{fn}{function time label}
#'   \item{crd}{coordination time label}
#'   \item{beat}{beats label}
#'   \item{wrk_share}{working share label}
#'   \item{stv}{starved share label}
#'   \item{blk}{blocked share label}
#'   \item{bottleneck}{bottleneck marker}
#' }
#' 
#' Examples:
#' \code{options(siphon.colors = list(stage = "96"))} to make stage names cyan
#' \code{options(siphon.colors = list(wrk = "1;34"))} to make workers label bold blue
#' \code{options(siphon.colors = list(bright_red = "35"))} to change error color to purple
#' 
#' To disable colors globally, set `options(siphon.color = FALSE)`.
#'
#' @param x A pump object.
#' @param ... Unused.
#' @export
print.pump <- function(x, ...) {
    cat(format(x), sep = "\n")
    invisible(x)
}
