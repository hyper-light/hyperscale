from pydantic import BaseModel
from hyperscale.terminal.styling.colors import ColorName, ExtendedColorName
from hyperscale.terminal.config.mode import TerminalDisplayMode


class HeaderConfig(BaseModel):
    color: ColorName | ExtendedColorName | None = None
    terminal_mode: TerminalDisplayMode = "compatability"
