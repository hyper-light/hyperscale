from hyperscale.ui.styling.colors import Colorizer, HighlightColorizer
from hyperscale.ui.styling.attributes import AttributeName
from pydantic import BaseModel
from typing import Dict, Literal, TypeVar, Type, Generic


T = TypeVar(
    "T",
    bound=dict[
        str,
        dict[Literal["color", "highlight"], Colorizer | HighlightColorizer],
    ],
)


class StatusBarConfig(
    BaseModel,
    Generic[T],
):
    status_map: T
    attributes: AttributeName | None = None


status = StatusBarConfig(status_map={"ready": {"color": "blue", "highlight": "green"}})


status.status_map["ready"]
