"""The rows each section of a node dashboard takes of its canvas.

The canvas is every row of the terminal but the dashboard's vertical
padding (Terminal's canvas_size), laid out again on every resize. Rows are
given so the sections always add up to the canvas -- nothing passes its
bottom:

- the header row its identity column's lines, the status line its one;
- the panels their share of the canvas, between their bounds;
- the chart half of the rest, never fewer rows than it needs to be read;
- the table every row that remains.

On a short canvas the sections shrink in this order: first the chart is
dropped (it draws nothing below its minimum), then the table gives up rows
down to its minimum, then the panels down to theirs. A canvas too short
for the header, the status line and every minimum is too short for the
dashboard (the terminal-capability check then selects the CI-safe summary
lines).

Below the panels each section draws only its bottom border: the border
above it is the bottom border of the row before.
"""

import math

# The header row: the Hyperscale header's art (three lines) beside the
# identity column -- role and node id, TCP address, UDP address, uptime and
# lifecycle state -- which never pages.
IDENTITY_LINE_COUNT = 4
HEADER_ROWS = IDENTITY_LINE_COUNT
# A section's top or bottom border.
BORDER_ROWS = 1
# The status line: one line above its bottom border.
STATUS_ROWS = 1 + BORDER_ROWS
# A panel shows between one line (its title, paging the rest) and its title
# and six lines, between its top and bottom borders; it prefers the
# "xx-small" share of the canvas, and at least its title and two lines.
PANEL_MIN_LINES = 1
PANEL_PREFERRED_MIN_LINES = 3
PANEL_MAX_LINES = 7
PANEL_CANVAS_SHARE = 0.15
# The chart's share of the rows the header, panels and status line leave.
CHART_REMAINDER_SHARE = 0.5
# A plot draws four lines besides its values (the value axis' label and top
# tick, the time axis and its ticks) and a multi-series plot its legend;
# read, it shows at least two rows of values.
PLOT_FIXED_LINES = 4
LEGEND_LINES = 1
PLOT_MIN_VALUE_ROWS = 2
CHART_MIN_ROWS = LEGEND_LINES + PLOT_FIXED_LINES + PLOT_MIN_VALUE_ROWS + BORDER_ROWS
# The table draws its header and the header's rule, and shows at least one
# row.
TABLE_MIN_ROWS = 2 + 1 + BORDER_ROWS


def header_rows(canvas_height: int) -> int:
    """The header row's rows: the identity column's lines."""
    return HEADER_ROWS


def status_rows(canvas_height: int) -> int:
    """The status line's rows."""
    return STATUS_ROWS


def panel_rows(canvas_height: int) -> int:
    """The panels' rows: their share of the canvas within their bounds,
    fewer only where the table would otherwise drop below its minimum."""
    preferred_lines = min(
        max(math.floor(canvas_height * PANEL_CANVAS_SHARE), PANEL_PREFERRED_MIN_LINES),
        PANEL_MAX_LINES,
    )
    lines_left = canvas_height - HEADER_ROWS - STATUS_ROWS - TABLE_MIN_ROWS - 2 * BORDER_ROWS
    return max(min(preferred_lines, lines_left), PANEL_MIN_LINES) + 2 * BORDER_ROWS


def remaining_rows(canvas_height: int) -> int:
    """The rows the header, panels and status line leave the chart and
    the table."""
    return canvas_height - HEADER_ROWS - panel_rows(canvas_height) - STATUS_ROWS


def chart_rows(canvas_height: int) -> int:
    """The chart row's rows: half of what remains, at least its minimum
    and leaving the table its minimum -- or none, where that cannot be."""
    remaining = remaining_rows(canvas_height)
    rows = min(max(math.floor(remaining * CHART_REMAINDER_SHARE), CHART_MIN_ROWS), remaining - TABLE_MIN_ROWS)
    return rows if rows >= CHART_MIN_ROWS else 0


def table_rows(canvas_height: int) -> int:
    """The table's rows: every row the chart leaves."""
    return remaining_rows(canvas_height) - chart_rows(canvas_height)
