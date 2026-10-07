from typing import Dict

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)

from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.colors import (
    ColorName,
    ExtendedColorName,
)

from .header_options import HeaderOptions as HeaderOptions
from .tabulate import CellAlignment, Colorizer, TableBorderType


class TableConfig(BaseModel):
    table_format: TableBorderType = "simple"
    headers: Dict[
        StrictStr,
        HeaderOptions,
    ]
    header_alignment: CellAlignment = "CENTER"
    cell_alignment: CellAlignment = "CENTER"
    minimum_column_width: StrictInt | None = None
    border_color: ColorName | ExtendedColorName | None = None
    terminal_mode: TerminalDisplayMode = "compatability"
    no_update_on_push: StrictBool = False
    pagination_refresh_rate: StrictInt | StrictFloat = 3
    # Size each column to its content (column_layout): the first column and
    # any fixed one are never truncated; columns are dropped from the right
    # when the values do not fit. Off: every column is the same width.
    size_columns_to_content: StrictBool = False
    # What a table with no rows shows in place of its header: one dim line,
    # centered (``waiting for workers to register``). None draws the header
    # over no rows.
    empty_message: StrictStr | None = None
