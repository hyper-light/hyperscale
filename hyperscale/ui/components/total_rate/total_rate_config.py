from pydantic import BaseModel, StrictInt, StrictStr
from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.attributes import (
    Attributizer,
)
from hyperscale.ui.styling.colors import (
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
