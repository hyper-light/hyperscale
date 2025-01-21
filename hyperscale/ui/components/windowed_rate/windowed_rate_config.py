from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.attributes import (
    Attributizer,
)
from hyperscale.ui.styling.colors import (
    Colorizer,
    HighlightColorizer,
)
from typing import List
from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr
from typing import Literal


class WindowedRateConfig(BaseModel):
    attributes: List[Attributizer] | None = None
    color: Colorizer | None = None
    highlight: HighlightColorizer | None = None
    precision: StrictInt = 3
    rate_period: StrictInt | StrictFloat = 1
    rate_unit: Literal["s", "m", "h", "d"] = "s"
    terminal_mode: TerminalDisplayMode = "compatability"
    unit: StrictStr | None = None
