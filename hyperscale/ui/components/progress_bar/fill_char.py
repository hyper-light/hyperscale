from enum import Enum
from typing import Dict, Literal


class FillCharType(Enum):
    EMPTY = " "
    BLOCK = "█"
    EQUALS = "="
    DOT = "●"
    PERIOD = "."
    LEFT_ARROW_EMPTY = "▹"
    LEFT_ARROW_FULL = "▸"
    NOISE_LIGHT = "░"
    NOISE_MEDIUM = "▒"
    NOISE_HEAVY = "▓"
    EMPTY_DOT = "o"
    SQUARE = "▄"
    DASH = "-"
    DOT_BLOCK = "⣿"
    SLANT_RECT = "▰"
    CENTER_PERIOD = "∙"
    TRIPLE_EQUALS = "≡"
    TOGGLE = "⊶"
    CIRCLE_TOGGLE = "◉"


FillCharName = Literal[
    "empty",
    "block",
    "equals",
    "dot",
    "period",
    "left_arrow_empty",
    "left_arrow_full",
    "noise_light",
    "noise_medium",
    "noise_heavy",
    "empty_dot",
    "square",
    "dash",
    "dot_block",
    "slant_rect",
    "center_period",
    "triple_equals",
    "toggle",
    "circle_toggle",
]


class FillChar:
    names: Dict[FillCharName, FillCharType] = {
        char.name.lower(): char.value for char in FillCharType
    }

    types: Dict[FillCharType, str] = {char: char.value for char in FillCharType}

    def __iter__(self):
        for name in self.names:
            yield name

    def __contains__(self, char: str):
        return char in self.names

    @classmethod
    def by_name(
        cls,
        char: FillCharName,
        default: str | None = None,
    ):
        return cls.names.get(char, default if default else cls.names.get("empty"))

    @classmethod
    def by_type(
        cls,
        char: FillCharType,
        default: str | None = None,
    ):
        return cls.types.get(
            char, default if default else cls.types.get(FillCharType.BLOCK)
        )
