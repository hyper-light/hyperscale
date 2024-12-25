from pydantic import BaseModel, StrictStr
from hyperscale.terminal.styling.colors import (
    ColorName,
    ExtendedColorName,
    HighlightName,
)
from hyperscale.terminal.styling.attributes import AttributeName
from hyperscale.terminal.config.mode import TerminalDisplayMode
from typing import Dict, List, Literal
from .font import FormatterSet, SupportedLetters


HeaderHorizontalAlignment = Literal["left", "center", "right"]
HeaderVerticalAlignment = Literal["top", "center", "bottom"]


class HeaderConfig(BaseModel):
    header_text: StrictStr
    color: ColorName | ExtendedColorName | None = None
    horizontal_alignment: HeaderHorizontalAlignment = "left"
    vertical_alignment: HeaderVerticalAlignment = "center"
    highlight: HighlightName | ExtendedColorName | None = None
    attributes: List[AttributeName] | None = None
    terminal_mode: TerminalDisplayMode = "compatability"
    formatters: Dict[SupportedLetters, FormatterSet] | None = None
