from typing import Dict, Literal

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)

from hyperscale.terminal.config.mode import TerminalDisplayMode
from hyperscale.terminal.styling.colors import (
    ColorName,
    ExtendedColorName,
)

from .tabulate import (
    DataColorMap,
    HeaderColorMap,
    NumberAlignmentType,
    StringAlignmentType,
    TableFormatType,
)


class HeaderOptions(BaseModel):
    precision: StrictStr | None = None
    field_type: Literal[
        "string",
        "integer",
        "float",
        "bool",
    ]
    default: StrictInt | StrictFloat | StrictBool | StrictStr | None = None


class TableConfig(BaseModel):
    table_format: TableFormatType = "simple"
    null_value: StrictStr | StrictInt | StrictFloat | StrictBool = "None"
    headers: Dict[
        StrictStr,
        HeaderOptions,
    ]
    table_color: ColorName | ExtendedColorName | None = None
    number_alignment_type: NumberAlignmentType | None = "right"
    string_alignment_type: StringAlignmentType | None = "right"
    terminal_mode: TerminalDisplayMode = "compatability"
    pagination_refresh_rate: StrictInt | StrictFloat = 3
    header_color_map: HeaderColorMap | None = None
    data_color_map: DataColorMap | None = None
