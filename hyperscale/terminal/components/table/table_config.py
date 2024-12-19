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
    table_format: Literal[
        "plain",
        "simple",
        "grid",
        "orgtbl",
        "rst",
        "mediawiki",
        "latex",
    ] = "simple"
    null_value: StrictStr | StrictInt | StrictFloat | StrictBool = "None"
    headers: Dict[
        StrictStr,
        HeaderOptions,
    ]
    table_color: ColorName | ExtendedColorName | None = None
    terminal_mode: TerminalDisplayMode = "compatability"
    pagination_refresh_rate: StrictInt | StrictFloat = 3
