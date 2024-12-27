from enum import Enum
from typing import Dict, Literal


class PointCharType(Enum):
    X = "X"
    DOT = "●"
    PERIOD = "."
    LEFT_ARROW_EMPTY = "▹"
    LEFT_ARROW_FULL = "▸"
    EMPTY_DOT = "o"
    SQUARE = "▄"
    DASH = "-"
    CENTER_PERIOD = "∙"
    TRIPLE_EQUALS = "≡"
    CIRCLE_TOGGLE = "◉"


PointCharName = Literal[
    "x",
    "dot",
    "period",
    "left_arrow_empty",
    "left_arrow_full",
    "empty_dot",
    "square",
    "dash",
    "center_period",
    "triple_equals",
    "circle_toggle",
]


class PointChar:
    names: Dict[PointCharName, PointCharType] = {
        char.name.lower(): char.value for char in PointCharType
    }

    types: Dict[PointCharType, str] = {char: char.value for char in PointCharType}

    def __iter__(self):
        for name in self.names:
            yield name

    def __contains__(self, char: str):
        return char in self.names

    @classmethod
    def by_name(
        cls,
        char: PointCharName,
        default: str | None = None,
    ):
        return cls.names.get(char, default if default else cls.names.get("x"))

    @classmethod
    def by_type(
        cls,
        char: PointCharType,
        default: str | None = None,
    ):
        return cls.types.get(
            char, default if default else cls.types.get(PointCharType.X)
        )
