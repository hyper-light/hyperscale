from pydantic import BaseModel, StrictInt, StrictStr
from hyperscale.terminal.config.mode import TerminalDisplayMode
from hyperscale.terminal.styling.attributes import (
    Attributizer,
)
from hyperscale.terminal.styling.colors import (
    Colorizer,
    HighlightColorizer,
)
from typing import List


class TotalRateConfig(BaseModel):
    attributes: List[Attributizer] | None = None
    color: Colorizer | None = None
    highlight: HighlightColorizer | None = None
    precision: StrictInt = 3
    terminal_mode: TerminalDisplayMode = 'compatability'
    unit: StrictStr | None = None
