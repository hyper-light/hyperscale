from typing import Literal

from pydantic import BaseModel

HorizontalAlignment = Literal["left", "center", "right"]
VerticalAlignment = Literal["top", "center", "bottom"]
AlignmentPriority = Literal["low", "medium", "high", "exclusive", "auto"]


class Alignment(BaseModel):
    horizontal: HorizontalAlignment = "left"
    vertical: VerticalAlignment = "top"
    priority: AlignmentPriority = "auto"
    vertical_priority: AlignmentPriority = "auto"
