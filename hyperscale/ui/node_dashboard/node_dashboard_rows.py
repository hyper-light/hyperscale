"""The rows each section of a node dashboard takes of its canvas.

The canvas is every row of the terminal but the dashboard's vertical
padding (Terminal's canvas_size), laid out again on every resize and
whenever the rows its badges or its table need change. Rows are given so
the sections always add up to the canvas -- nothing passes its bottom:

- the header its identity column's lines, the badges the lines their
  badges flow onto, the status line its one;
- the tiles their label stacked over their value;
- the table the rows it needs -- its header and one per row, or its
  empty state's one line -- and the chart every row left.

On a short canvas the sections shrink in this order: first the chart
gives up rows down to its minimum, then the table down to its own (its
header and a row, paging the rest), then the tiles draw label and value
on one line; then the chart is dropped (it draws nothing below its
minimum) and the table takes what is left. A canvas too short for the
header, the badges, the status line and the table's minimum is too short
for the dashboard (the terminal-capability check then selects the CI-safe
summary lines).

Each group above the status line ends in a rule: the section's last row.
"""

from hyperscale.ui.components.stat_tile.stat_tile import STACKED_LINES

# A group's rule: one row below it.
RULE_ROWS = 1
# The header: the Hyperscale header's art (three lines) beside the identity
# column -- role and node id, lifecycle state and uptime, TCP address and
# UDP address -- which never pages.
IDENTITY_LINE_COUNT = 4
HEADER_ROWS = IDENTITY_LINE_COUNT + RULE_ROWS
# The badges' lines until a sample says how many they flow onto.
FIRST_BADGE_LINES = 1
# The tiles: label over value, or both on one line.
TILE_STACKED_ROWS = STACKED_LINES + RULE_ROWS
TILE_INLINE_ROWS = 1 + RULE_ROWS
# The status line, the last: no rule below it.
STATUS_ROWS = 1
# A plot draws four lines besides its values (the value axis' label and top
# tick, the time axis and its ticks) and its legend; read, it shows at
# least two rows of values.
PLOT_FIXED_LINES = 4
LEGEND_LINES = 1
PLOT_MIN_VALUE_ROWS = 2
CHART_MIN_ROWS = LEGEND_LINES + PLOT_FIXED_LINES + PLOT_MIN_VALUE_ROWS + RULE_ROWS
# The table draws its header over its rows; with none, its empty state's
# one line in their place.
TABLE_HEADER_LINES = 1
TABLE_EMPTY_LINES = 1
# The table's least: its header and one row (paging the rest).
TABLE_MIN_ROWS = TABLE_HEADER_LINES + 1 + RULE_ROWS


def table_rows_needed(row_count: int) -> int:
    """The rows a table of ``row_count`` rows needs: its header and its
    rows, or its empty state's line, and its rule."""
    return max(TABLE_HEADER_LINES + row_count, TABLE_EMPTY_LINES) + RULE_ROWS


class NodeDashboardRows:
    """The rows each section takes of a canvas ``canvas_height`` rows tall
    (each method is a section's ``height_rows``), given the lines the
    badges flow onto and the rows the table holds -- set by ``need`` from
    each sample, which tells the dashboard when to lay out again."""

    def __init__(self) -> None:
        self._badge_rows = FIRST_BADGE_LINES + RULE_ROWS
        self._table_rows = table_rows_needed(0)

    def need(self, badge_line_count: int, table_row_count: int) -> bool:
        """Size the badges for ``badge_line_count`` lines and the table for
        ``table_row_count`` rows; whether either changed (the sections
        must then be laid out again)."""
        needed = (badge_line_count + RULE_ROWS, table_rows_needed(table_row_count))
        changed = needed != (self._badge_rows, self._table_rows)
        self._badge_rows, self._table_rows = needed
        return changed

    def header_rows(self, canvas_height: int) -> int:
        """The header's rows: the identity column's lines and the rule."""
        return HEADER_ROWS

    def badge_rows(self, canvas_height: int) -> int:
        """The badges' rows: the lines they flow onto and the rule."""
        return self._badge_rows

    def status_rows(self, canvas_height: int) -> int:
        """The status line's row."""
        return STATUS_ROWS

    def tile_rows(self, canvas_height: int) -> int:
        """The tiles' rows: label stacked over value while the chart and the
        table keep their minimums beside them, else on one line."""
        spare_rows = canvas_height - self._fixed_rows() - TILE_STACKED_ROWS - CHART_MIN_ROWS - self._table_minimum()
        return TILE_STACKED_ROWS if spare_rows >= 0 else TILE_INLINE_ROWS

    def chart_rows(self, canvas_height: int) -> int:
        """The chart's rows: every row the table's need leaves, at least its
        minimum while the table keeps its own -- or none, where it cannot."""
        remaining = self._remaining_rows(canvas_height)
        rows = max(remaining - self._table_rows, min(CHART_MIN_ROWS, remaining - self._table_minimum()))
        return rows if rows >= CHART_MIN_ROWS else 0

    def table_rows(self, canvas_height: int) -> int:
        """The table's rows: every row the chart leaves."""
        return self._remaining_rows(canvas_height) - self.chart_rows(canvas_height)

    def fits(self, canvas_height: int) -> bool:
        """Whether a canvas ``canvas_height`` rows tall holds the dashboard:
        its fixed rows, its tiles on one line and the table's minimum."""
        return canvas_height - self._fixed_rows() - TILE_INLINE_ROWS >= TABLE_MIN_ROWS

    def _fixed_rows(self) -> int:
        return HEADER_ROWS + self._badge_rows + STATUS_ROWS

    def _table_minimum(self) -> int:
        return min(self._table_rows, TABLE_MIN_ROWS)

    def _remaining_rows(self, canvas_height: int) -> int:
        """The rows the header, badges, tiles and status line leave the
        chart and the table."""
        return canvas_height - self._fixed_rows() - self.tile_rows(canvas_height)
