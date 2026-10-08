"""A dashboard table's cells as the table draws them: a status as a
badge's text, a ratio as a meter's, and any other value as it is."""

from collections.abc import Callable

from hyperscale.ui.components.meter import METER_GLYPHS, MeterReading, meter_text
from hyperscale.ui.components.status_badge import StatusBadgeReading, badge_text
from hyperscale.ui.components.table.tabulate import TableCell
from hyperscale.ui.config.mode import TerminalMode

from .models import TableRow, TableRowValue

# A meter in a table cell, its label included, takes this share of its
# column's even share of the table's width: the rest keeps it apart from
# the next column.
CELL_METER_SHARE_OF_COLUMN = 0.75

CellRenderer = Callable[[TableRowValue, TerminalMode, int], TableCell]

CELL_RENDERERS: dict[type, CellRenderer] = {
    StatusBadgeReading: lambda cell, mode, meter_width: badge_text(cell, mode),
    MeterReading: lambda cell, mode, meter_width: meter_text(cell, meter_width, METER_GLYPHS[mode]),
}


def cell_meter_width(table_width: int, column_count: int) -> int:
    """The columns a meter cell's bar and label take in a table
    ``table_width`` wide of ``column_count`` columns."""
    return int(table_width / max(column_count, 1) * CELL_METER_SHARE_OF_COLUMN)


def table_cell(value: TableRowValue, mode: TerminalMode, meter_width: int) -> TableCell:
    """One cell as text: a status or a ratio drawn, any other value as is."""
    renderer = CELL_RENDERERS.get(type(value))
    return value if renderer is None else renderer(value, mode, meter_width)


def table_cells(rows: list[TableRow], mode: TerminalMode, meter_width: int) -> list[dict[str, TableCell]]:
    """Every row's cells as text."""
    return [{header: table_cell(value, mode, meter_width) for header, value in row.items()} for row in rows]
