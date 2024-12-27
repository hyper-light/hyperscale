from __future__ import annotations

from enum import Enum
from typing import Dict, Literal

from hyperscale.ui.config.mode import TerminalMode

from .extended_color import ExtendedColorName, ExtendedColorType

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


class Color:
    names: Dict[
        ColorName,
        int,
    ] = {attr.name.lower(): attr.value for attr in BaseColorType}

    extended_names: Dict[
        ExtendedColorName,
        int,
    ] = {attr.name.lower(): attr.value for attr in ExtendedColorType}

    types: Dict[
        BaseColorType,
        int,
    ] = {attr: attr.value for attr in BaseColorType}

    extended_types: Dict[
        ExtendedColorType,
        int,
    ] = {attr: attr.value for attr in ExtendedColorType}

    def __iter__(self):
        for name in self.names:
            yield name

    def __contains__(self, color: ColorName | ExtendedColorName):
        return color in self.names or color in self.extended_names

    @classmethod
    def by_name(
        cls,
        color: ColorName | ExtendedColorName,
        default: int = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        if mode == TerminalMode.EXTENDED:
            return cls.extended_names.get(
                color, default if default else cls.extended_names.get("white")
            )

        return cls.names.get(color, default if default else cls.names.get("white"))

    @classmethod
    def by_type(
        cls,
        color: BaseColorType | ExtendedColorType,
        default: int = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        if mode == TerminalMode.EXTENDED:
            return cls.extended_types.get(
                color,
                default if default else cls.extended_types.get(ExtendedColorType.WHITE),
            )

        return cls.types.get(
            color, default if default else cls.types.get(BaseColorType.WHITE)
        )
