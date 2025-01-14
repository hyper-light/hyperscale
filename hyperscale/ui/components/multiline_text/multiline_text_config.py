from pydantic import BaseModel, StrictStr, StrictInt, StrictFloat
from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.attributes import (
    Attributizer,
)
from hyperscale.ui.styling.colors import (
    Colorizer,
    HighlightColorizer,
)
from typing import List, Literal


HorizontalAlignment = Literal["left", "center", "right"]


class MultilineTextConfig(BaseModel):
    text: List[StrictStr]
    color: Colorizer | None = None
    highlight: HighlightColorizer | None = None
    attributes: List[Attributizer] | None = None
    horizontal_alignment: HorizontalAlignment = "center"
    pagination_refresh_rate: StrictInt | StrictFloat = 3
    terminal_mode: TerminalDisplayMode = "compatability"
