from hyperscale.ui.styling.attributes import Attributizer
from hyperscale.ui.styling.colors import Colorizer, HighlightColorizer
from hyperscale.ui.config.mode import TerminalDisplayMode
from pydantic import BaseModel, StrictStr, StrictInt
from typing import Dict, Literal, List


HorizontalAlignment = Literal["left", "center", "right"]

StylingMap = Dict[
    StrictStr,
    Dict[
        Literal["color", "highlight", "attrs"],
        Colorizer | HighlightColorizer | List[Attributizer] | None,
    ],
]


class StatusBarConfig(BaseModel):
    default_status: StrictStr
    status_styles: StylingMap | None = None
    horizontal_padding: StrictInt = 0
    horizontal_alignment: HorizontalAlignment = "center"
    terminal_mode: TerminalDisplayMode = "compatability"
