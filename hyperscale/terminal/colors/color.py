from __future__ import annotations

from enum import Enum
from typing import Dict, Literal

from .mode import TerminalMode

ColorName = Literal[
    "black",
    "grey",
    "red",
    "yellow",
    "blue",
    "magenta",
    "cyan",
    "light_grey",
    "dark_grey",
    "light_red",
    "light_green",
    "light_yellow",
    "light_blue",
    "light_magenta",
    "light_cyan",
    "white",
]


class BaseColorType(Enum):
    BLACK = 30
    GREY = 30
    RED = 31
    GREEN = 32
    YELLOW = 33
    BLUE = 34
    MAGENTA = 35
    CYAN = 36
    LIGHT_GREY = 37
    DARK_GREY = 90
    LIGHT_REd = 91
    LIGHT_GREEN = 92
    LIGHT_YELLOW = 93
    LIGHT_BLUE = 94
    LIGHT_MAGENTA = 95
    LIGHT_CYAN = 96
    WHITE = 97


class ExtendedColorType(Enum):
    BLACK = 0
    MAROON = 1
    GREEN = 2
    OLIVE = 3
    NAVY = 4
    PURPLE = 5
    TEAL = 6
    SILVER = 7
    GREY = 8
    RED = 9
    LIME = 10
    YELLOW = 11
    BLUE = 12
    FUCHSIA = 13
    AQUA = 14
    WHITE = 15
    GREY_1 = 16
    NAVY_BLUE = 17
    DARK_BLUE = 18
    BLUE_2 = 19
    BLUE_3 = 20
    BLUE_4 = 21
    DARK_GREEN = 22
    DEEP_SKY_BLUE = 23
    DEEP_SKY_BLUE_2 = 24
    DEEP_SKY_BLUE_3 = 25
    DODGER_BLUE_4 = 26
    DODGER_BLUE_2 = 27


class Color:
    names: Dict[
        ColorName,
        int,
    ] = {attr.name.lower(): attr.value for attr in BaseColorType}

    types: Dict[
        BaseColorType,
        int,
    ] = {attr: attr.value for attr in BaseColorType}

    def __iter__(self):
        for name in self.names:
            yield name

    def __contains__(self, color: ColorName):
        return color in self.names

    @classmethod
    def by_name(
        cls,
        color: ColorName,
        default: int = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        return cls.names.get(color, default)

    @classmethod
    def by_type(
        cls,
        color: BaseColorType,
        default: int = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        return cls.types.get(color, default)
