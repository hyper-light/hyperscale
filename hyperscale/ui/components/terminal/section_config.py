from typing import Literal

from pydantic import BaseModel, StrictInt, StrictStr

from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.colors import ColorName, ExtendedColorName

HorizontalSectionSize = Literal[
    "auto",
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

VerticalSectionSize = Literal[
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


HorizontalAlignment = Literal["left", "center", "right"]
VerticalAlignment = Literal["top", "center", "bottom"]


class SectionConfig(BaseModel):
    width: HorizontalSectionSize = "auto"
    height: VerticalSectionSize = "medium"
    left_padding: StrictInt = 0
    right_padding: StrictInt = 0
    top_padding: StrictInt = 0
    bottom_padding: StrictInt = 0
    max_height: StrictInt | None = None
    max_width: StrictInt | None = None
    top_border: StrictStr | None = None
    bottom_border: StrictStr | None = None
    left_border: StrictStr | None = None
    inside_border: StrictStr | None = None
    right_border: StrictStr | None = None
    border_color: ColorName | ExtendedColorName = "white"
    mode: TerminalDisplayMode = "compatability"
    horizontal_alignment: HorizontalAlignment = "left"
    vertical_alignment: VerticalAlignment = "top"
