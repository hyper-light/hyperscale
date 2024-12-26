from pydantic import BaseModel, AnyUrl, StrictStr
from hyperscale.terminal.styling.colors import Colorizer, HighlightColorizer
from hyperscale.terminal.styling.attributes import Attributizer
from hyperscale.terminal.config.mode import TerminalDisplayMode
from typing import List


class LinkConfig(BaseModel):
    default_url: AnyUrl | None = None
    default_link_text: StrictStr | None = None
    fallback_text: StrictStr
    color: Colorizer | None = None
    highlight: HighlightColorizer | None = None
    attributes: List[Attributizer] | None = None
    terminal_mode: TerminalDisplayMode = "compatability"
