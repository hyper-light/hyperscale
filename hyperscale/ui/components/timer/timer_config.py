from pydantic import BaseModel, StrictStr, StrictInt
from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.attributes import (
    Attributizer,
)
from hyperscale.ui.styling.colors import (
    Colorizer,
    HighlightColorizer,
)
from typing import List


class TimerConfig(BaseModel):
    attributes: List[Attributizer] | None = None
    color: Colorizer | None = None
    highlight: HighlightColorizer | None = None
    terminal_mode: TerminalDisplayMode = 'compatability'
