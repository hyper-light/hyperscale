from enum import Enum
from typing import Dict, Literal


class BackgroundCharType(Enum):
    EMPTY = " "
    DASH = "-"
    CENTER_PERIOD = "∙"
    PERIOD = "."
    EMPTY_SLANT_RECT = "▱"
    NOISE_LIGHT = "░"
    EMPTY_DOT = "o"
    TRIPLE_EQUALS = "≡"
    TOGGLE = "⊷"
    CIRCLE_TOGGLE = "◎"
    EMPTY_SQUARE = "□"
    LEFT_ARROW_EMPTY = "▹"


BackgroundCharName = Literal[
    "empty",
    "dash",
    "center_period",
    "period",
    "empty_slant_rect",
    "noise_light",
    "empty_dot",
    "triple_equals",
    "toggle",
    "circle_toggle",
    "empty_square",
    "left_arrow_empty",
]


class BackgroundChar:
    names: Dict[BackgroundCharName, str] = {
        char.name.lower(): char.value for char in BackgroundCharType
    }

    types: Dict[BackgroundCharType, str] = {
        char: char.value for char in BackgroundCharType
    }

    def __iter__(self):
        for name in self.names:
            yield name

    def __contains__(self, char: str):
        return char in self.names

    @classmethod
    def by_name(
        cls,
        char: BackgroundCharName,
        default: str | None = None,
    ):
        return cls.names.get(
            char,
            default if default else cls.names.get("empty"),
        )

    @classmethod
    def by_type(
        cls,
        char: BackgroundCharType,
        default: str | None = None,
    ):
        return cls.types.get(
            char,
            default if default else cls.types.get(BackgroundCharType.EMPTY),
        )
