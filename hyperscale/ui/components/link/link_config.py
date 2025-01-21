from pydantic import BaseModel, AnyUrl, StrictStr
from hyperscale.ui.styling.colors import Colorizer, HighlightColorizer
from hyperscale.ui.styling.attributes import Attributizer
from hyperscale.ui.config.mode import TerminalDisplayMode
from typing import List


class LinkConfig(BaseModel):
    link_url: AnyUrl | None = None
    link_text: StrictStr
    color: Colorizer | None = None
    highlight: HighlightColorizer | None = None
    attributes: List[Attributizer] | None = None
    terminal_mode: TerminalDisplayMode = "compatability"
