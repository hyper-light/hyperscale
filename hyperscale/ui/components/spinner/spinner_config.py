from pydantic import BaseModel, StrictStr, StrictBool
from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.colors import Colorizer, HighlightColorizer
from hyperscale.ui.styling.attributes import Attributizer
from typing import List
from .spinner_types import SpinnerName


class SpinnerConfig(BaseModel):
    attributes: List[Attributizer] | None = None
    color: Colorizer | None = None
    fail_attrbutes: List[Attributizer] | None = None
    fail_char: StrictStr = "✘"
    fail_color: Colorizer | None = None
    fail_highlight: HighlightColorizer | None = None
    highlight: HighlightColorizer | None = None
    ok_attributes: List[Attributizer] | None = None
    ok_char: StrictStr = "✔"
    ok_color: Colorizer | None = None
    ok_highlight: HighlightColorizer | None = None
    reverse_spinner_direction: StrictBool = False
    spinner: SpinnerName
    terminal_mode: TerminalDisplayMode = "compatability"
