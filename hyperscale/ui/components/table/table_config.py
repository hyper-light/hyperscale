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

from .tabulate import CellAlignment, Colorizer, TableBorderType


class HeaderOptions(BaseModel):
    precision_format: StrictStr | None = None
    header_color: Colorizer | None = None
    data_color: Colorizer | None = None
    default: StrictInt | StrictFloat | StrictBool | StrictStr | None = None


class TableConfig(BaseModel):
    table_format: TableBorderType = "simple"
    headers: Dict[
        StrictStr,
        HeaderOptions,
    ]
    cell_alignment: CellAlignment = "CENTER"
    border_color: ColorName | ExtendedColorName | None = None
    terminal_mode: TerminalDisplayMode = "compatability"
    pagination_refresh_rate: StrictInt | StrictFloat = 3
