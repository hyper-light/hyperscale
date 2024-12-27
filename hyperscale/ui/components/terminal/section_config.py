from typing import Literal

from pydantic import BaseModel, StrictInt, StrictStr

from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.colors import ColorName, ExtendedColorName

SectionSize = Literal[
    "smallest",
    "xx-small",
    "x-small",
    "small",
    "medium",
    "large",
    "x-large",
    "xx-large",
    "full",
]


class SectionConfig(BaseModel):
    width: SectionSize = "medium"
    height: SectionSize = "medium"
    left_padding: StrictInt = 0
    right_padding: StrictInt = 0
    top_padding: StrictInt = 0
    bottom_padding: StrictInt = 0
    top_border: StrictStr | None = None
    bottom_border: StrictStr | None = None
    left_border: StrictStr | None = None
    inside_border: StrictStr | None = None
    right_border: StrictStr | None = None
    border_color: ColorName | ExtendedColorName = "white"
    mode: TerminalDisplayMode = "compatability"
