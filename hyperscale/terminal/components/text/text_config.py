from pydantic import BaseModel, StrictStr
from hyperscale.terminal.config.mode import TerminalDisplayMode
from hyperscale.terminal.styling.attributes import (
    Attributizer,
)
from hyperscale.terminal.styling.colors import (
    Colorizer,
    HighlightColorizer,
)
from typing import List


class TextConfig(BaseModel):
    default_text: StrictStr = ""
    color: Colorizer | None = None
    highlight: HighlightColorizer | None = None
    attributes: List[Attributizer] | None = None
    terminal_mode: TerminalDisplayMode = 'compatability'