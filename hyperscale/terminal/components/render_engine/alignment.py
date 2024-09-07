from typing import Literal

from pydantic import BaseModel

HorizontalAlignment = Literal["left", "center", "right"]
VerticalAlignment = Literal["top", "center", "bottom"]


class Alignment(BaseModel):
    horizontal: HorizontalAlignment = "left"
    vertical: VerticalAlignment = "top"
