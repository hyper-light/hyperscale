from pydantic import BaseModel, StrictInt, StrictStr
from hyperscale.terminal.styling.colors import (
    Colorizer,
    HighlightColorizer,
)
from hyperscale.terminal.styling.attributes import Attributizer
from hyperscale.terminal.config.mode import TerminalDisplayMode
from typing import List


class CounterConfig(BaseModel):
    unit: StrictStr | None = None
    precision: StrictInt = 3
    initial_amount: StrictInt = 0
    color: Colorizer | None = None
    highlight: HighlightColorizer | None = None
    attributes: List[Attributizer] | None = None
    terminal_mode: TerminalDisplayMode = "compatability"
