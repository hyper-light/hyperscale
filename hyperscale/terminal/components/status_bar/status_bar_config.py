from hyperscale.terminal.styling.attributes import Attributizer
from hyperscale.terminal.styling.colors import Colorizer, HighlightColorizer
from hyperscale.terminal.config.mode import TerminalDisplayMode
from pydantic import BaseModel, StrictStr, StrictInt
from typing import Dict, Literal


HorizontalAlignment = Literal["left", "center", "right"]

StylingMap = Dict[
    StrictStr,
    Dict[
        Literal["color", "highlight", "attrs"],
        Colorizer | HighlightColorizer | Attributizer | None,
    ],
]


class StatusBarConfig(BaseModel):
    status_styles: StylingMap | None = None
    horizontal_padding: StrictInt = 0
    horizontal_alignment: HorizontalAlignment = "center"
    terminal_mode: TerminalDisplayMode = "compatability"
