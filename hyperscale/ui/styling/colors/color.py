from __future__ import annotations

from typing import Dict, Literal

from hyperscale.ui.config.mode import TerminalMode

from .base_color_type import BaseColorType as BaseColorType
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
