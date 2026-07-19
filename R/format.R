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

# Semantic color palette: name -> ANSI SGR code. Nord-inspired, using the
# standard 16-color codes for broad terminal compatibility. This is the single
# source of truth for every color siphon emits; hoisted to a package constant
# so it is built once at load, not rebuilt on every styled fragment.
.pump_palette <- c(
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

# UI element -> palette name. Naming the palette entry (rather than repeating a
# raw code) keeps the two in sync and makes the intent readable; "" means no
# color. Resolved to raw codes once below.
.pump_element_styles <- c(
    header = "bold",
    source = "bold",
    sink = "bold",
    stage = "bright_blue",
    backend = "",
    wrk = "dim",
    buf = "dim",
    done = "dim",
    err = "dim",                # values use bright_red when > 0
    fn = "dim",
    crd = "dim",
    beat = "dim",
    wrk_share = "bright_green",
    stv = "bright_yellow",
    blk = "bright_red",
    bottleneck = "bright_red"
)

# Element -> ANSI code, resolved once against the default palette at load.
# Held as its own table (not re-derived per call) so that a runtime palette
# override does not silently retint elements: the documented contract is that
# options(siphon.colors=) overrides a palette name OR an element name, each
# independently.
.pump_element_codes <- vapply(
    .pump_element_styles,
    function(sem) if (nzchar(sem)) unname(.pump_palette[[sem]]) else "",
    character(1)
)

# Resolve the full name -> ANSI lookup for one format() call. Merges any
# options(siphon.colors = list(...)) override into the palette and element
# tables exactly once, then flattens both namespaces into a single vector that
# .pump_style() indexes per fragment. Returns NULL when color is off, which is
# the signal .pump_style() reads to skip styling.
.pump_theme <- function(color = TRUE) {
    if (!isTRUE(color)) {
        return(NULL)
    }
    palette <- .pump_palette
    elements <- .pump_element_codes
    custom <- getOption("siphon.colors", NULL)
    if (is.list(custom)) {
        for (name in names(custom)) {
            if (name %in% names(palette)) palette[name] <- custom[[name]]
            if (name %in% names(elements)) elements[name] <- custom[[name]]
        }
    }
    c(palette, elements)
}

# Wrap x in the ANSI codes named by `style` (space-separated for compound
# styles like "bold blue"), looked up in a theme built by .pump_theme(). A
# NULL theme (color off) or an empty x is returned unchanged.
.pump_style <- function(x, style, theme) {
    if (is.null(theme) || !nzchar(x)) {
        return(x)
    }
    ansi_codes <- unname(theme[strsplit(style, " ")[[1]]])
    ansi_codes <- ansi_codes[nzchar(ansi_codes)]
    if (length(ansi_codes) == 0L) {
        return(x)
    }
    paste0("\x1b[", paste(ansi_codes, collapse = ";"), "m", x, "\x1b[0m")
}

# --- Rendering constants ---------------------------------------------------

# Box-drawing glyphs for the pipeline frame. \u escapes keep the source
# ASCII-only; the ASCII set is the degraded form for non-UTF-8 locales.
.pump_glyphs_unicode <- c(
    top = "\u250c\u2500", mid = "\u251c\u2500",
    pipe = "\u2502", bot = "\u2514\u2500"
)
.pump_glyphs_ascii <- c(top = "+-", mid = "+-", pipe = "|", bot = "+-")

# Occupancy bar: number of cells inside the [.....] brackets.
.pump_bar_width <- 5L

# Stuck-job tell: flag an in-flight item older than factor x the average
# per-item time, but only once it has run at least this many seconds.
.pump_stuck_factor <- 10
.pump_stuck_floor_secs <- 1

# .pump_fmt_ms thresholds: whole seconds at or above the first, whole
# milliseconds at or above the second, else one decimal millisecond.
.pump_ms_secs_at <- 10000
.pump_ms_whole_at <- 100

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
.pump_bar <- function(filled, capacity, theme, fill_style = "green",
                      width = .pump_bar_width) {
    if (!is.finite(capacity) || capacity < 1L) {
        return("[]")
    }
    n_fill <- as.integer(round(filled / capacity * width))
    if (filled > 0L) n_fill <- max(n_fill, 1L)
    n_fill <- min(n_fill, width)
    paste0(
        "[",
        .pump_style(strrep("#", n_fill), fill_style, theme),
        .pump_style(strrep("-", width - n_fill), "dim", theme),
        "]"
    )
}

# "styled label + value" building block, e.g. `wrk [##--] 2/4` or `done 8`.
# The label is styled with `style` (defaulting to the label name, which is also
# its element style); the value is placed verbatim after a single space.
.pump_metric <- function(label, value, theme, style = label) {
    paste0(.pump_style(label, style, theme), " ", value)
}

# Error-count chip, unified across the source and stage rows (F11/D3): `err N`
# in bright red when N > 0, a dim `err 0` otherwise.
.pump_err_chip <- function(n, theme) {
    if (n > 0L) {
        .pump_style(sprintf("err %d", n), "bright_red", theme)
    } else {
        .pump_style("err 0", "err", theme)
    }
}

# One beat-state share, e.g. `wrk 100%`: the value is styled with `style` when
# positive and dimmed when zero, then labelled.
.pump_share_metric <- function(share, style, name, theme) {
    val <- .pump_fmt_share(share)
    val <- if (isTRUE(share > 0)) {
        .pump_style(val, style, theme)
    } else {
        .pump_style(val, "dim", theme)
    }
    .pump_metric(name, val, theme)
}

.pump_fmt_ms <- function(ms) {
    if (is.null(ms) || !is.finite(ms)) {
        return("--")
    }
    if (ms >= .pump_ms_secs_at) {
        sprintf("%.1fs", ms / 1000)
    } else if (ms >= .pump_ms_whole_at) {
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

# Pad a column of (ANSI-styled) cells to their common visible width.
.pump_pad_column <- function(cells) {
    w <- max(vapply(cells, .pump_visible_width, integer(1)))
    vapply(cells, .pump_pad_to, character(1), width = w)
}

# Build the three aligned columns (workers, buffer, done) shared by the stage
# rows, so each is padded to a width common across every stage.
.pump_stage_columns <- function(stages, theme) {
    work <- character(length(stages))
    out <- character(length(stages))
    done <- character(length(stages))
    for (i in seq_along(stages)) {
        s <- stages[[i]]
        buffer_full <- s$buffer_capacity > 0L &&
            s$buffer_size >= s$buffer_capacity
        work[i] <- .pump_metric("wrk", paste0(
            .pump_bar(s$workers_active, s$workers_limit, theme),
            " ", s$workers_active, "/", s$workers_limit
        ), theme)
        out[i] <- .pump_metric("buf", paste0(
            .pump_bar(
                s$buffer_size, s$buffer_capacity, theme,
                fill_style = if (buffer_full) "red" else "green"
            ),
            " ", s$buffer_size, "/", s$buffer_capacity
        ), theme)
        done[i] <- .pump_metric("done", s$completed, theme)
    }
    list(
        work = .pump_pad_column(work),
        out = .pump_pad_column(out),
        done = .pump_pad_column(done)
    )
}

# --- Per-section renderers. Each returns a character vector of lines. -------

# Flat status for a bare source (no stages).
.pump_render_flat <- function(x, header, theme) {
    dim_ <- function(txt) .pump_style(txt, "dim", theme)
    c(
        .pump_style(header, "bold", theme),
        paste0("  ", dim_("pulled:"), "  ", x$completed),
        paste0("  ", dim_("errors:"), "  ", x$errors),
        paste0(
            "  ", dim_("pops:"), "    ",
            x$pop_hits, " hits, ", x$pop_misses, " misses"
        )
    )
}

# Header line: class name plus the total beat count across all stages.
.pump_render_header <- function(stages, header, theme) {
    total_beats <- sum(vapply(stages, function(s) s$beats, integer(1)))
    label <- if (total_beats > 0) {
        paste0("<", header, " (", total_beats, ")>")
    } else {
        paste0("<", header, ">")
    }
    .pump_style(label, "header", theme)
}

# Source line (top of the frame): position/length and an error chip if any.
.pump_render_source <- function(source, ctx) {
    if (is.null(source)) {
        return(character(0))
    }
    theme <- ctx$theme
    line <- paste0(
        .pump_style(ctx$g[["top"]], "dim", theme), " ",
        .pump_style("source", "source", theme),
        "   ", source$position, "/", .pump_fmt_len(source$length)
    )
    if (source$errors > 0) {
        line <- paste0(line, "   ", .pump_err_chip(source$errors, theme))
    }
    line
}

# One stage: label row, worker/buffer/done + error row, an optional timing and
# beat-share row, and an optional stuck-job warning.
.pump_render_stage <- function(s, i, ctx) {
    theme <- ctx$theme
    g <- ctx$g
    pipe <- .pump_style(g[["pipe"]], "dim", theme)

    glyph <- if (!ctx$has_source && i == 1L) g[["top"]] else g[["mid"]]
    label <- .pump_metric(sprintf("stage %d", i),
                          .pump_style(s$type, "backend", theme),
                          theme, style = "stage")
    if (!is.na(ctx$bottleneck) && i == ctx$bottleneck) {
        label <- paste0(label, "  ",
                        .pump_style("* bottleneck", "bottleneck", theme))
    }

    lines <- c(
        paste0(.pump_style(glyph, "dim", theme), " ", label),
        paste0(
            pipe, "    ",
            ctx$cols$work[i], "   ",
            ctx$cols$out[i], "   ",
            ctx$cols$done[i], "   ",
            .pump_err_chip(s$errors, theme)
        )
    )

    if (isTRUE(s$beats > 0L)) {
        lines <- c(lines, paste0(
            pipe, "    ",
            .pump_metric("fn", paste0(
                .pump_fmt_ms(s$fn_per_item),
                .pump_style("/it", "fn", theme)
            ), theme), "   ",
            .pump_metric("crd", paste0(
                .pump_fmt_ms(s$coord_time / s$beats),
                .pump_style("/bt", "crd", theme)
            ), theme), "   ",
            .pump_share_metric(s$share_working, "wrk_share", "wrk", theme), " ",
            .pump_share_metric(s$share_starved, "stv", "stv", theme), " ",
            .pump_share_metric(s$share_blocked, "blk", "blk", theme)
        ))
    }

    c(lines, .pump_render_stuck(s, pipe, theme))
}

# Stuck-job tell: an in-flight item much older than the average per-item time
# deserves attention. Item ages are stamped by pump_status() at snapshot time
# (in_flight$age_secs), so this is a pure function of the snapshot.
.pump_render_stuck <- function(s, pipe, theme) {
    if (length(s$in_flight) == 0 || !isTRUE(s$fn_per_item > 0)) {
        return(character(0))
    }
    ages <- vapply(s$in_flight, function(fl) fl$age_secs, numeric(1))
    oldest <- which.max(ages)
    if (ages[oldest] * 1000 > .pump_stuck_factor * s$fn_per_item &&
            ages[oldest] > .pump_stuck_floor_secs) {
        paste0(
            pipe, "    ",
            .pump_style(sprintf(
                "oldest in flight: %.1fs (id %s)",
                ages[oldest], format(s$in_flight[[oldest]]$id)
            ), "yellow", theme)
        )
    } else {
        character(0)
    }
}

# Sink line (bottom of the frame): items that left the pipeline.
.pump_render_sink <- function(x, ctx) {
    theme <- ctx$theme
    total <- if (!is.null(x$source)) {
        paste0("/", .pump_fmt_len(x$source$length))
    } else {
        ""
    }
    paste0(
        .pump_style(ctx$g[["bot"]], "dim", theme), " ",
        .pump_style("sink", "sink", theme), "   ",
        x$delivered, total
    )
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
    # Resolve the color theme once, then hand it (and the per-render context) to
    # the section renderers. A NULL $stages means a bare source.
    theme <- .pump_theme(color)
    if (is.null(x$stages)) {
        return(.pump_render_flat(x, header, theme))
    }

    stages <- x$stages
    ctx <- list(
        theme = theme,
        g = if (isTRUE(unicode)) .pump_glyphs_unicode else .pump_glyphs_ascii,
        bottleneck = .pump_find_bottleneck(stages),
        has_source = !is.null(x$source),
        cols = .pump_stage_columns(stages, theme)
    )

    c(
        .pump_render_header(stages, header, theme),
        .pump_render_source(x$source, ctx),
        unlist(
            lapply(seq_along(stages),
                   function(i) .pump_render_stage(stages[[i]], i, ctx)),
            use.names = FALSE
        ),
        .pump_render_sink(x, ctx)
    )
}

#' @title Print a pump_status object
#' @description Displays a pump_status snapshot.
#' @param x A pump_status object.
#' @param ... Unused.
#' @return The input `x`, invisibly.
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
#' @inheritSection pump_status Legend
#' @inheritSection pump_status Customization
#'
#' @param x A pump object.
#' @param ... Unused.
#' @return The input `x`, invisibly.
#' @export
print.pump <- function(x, ...) {
    cat(format(x), sep = "\n")
    invisible(x)
}
