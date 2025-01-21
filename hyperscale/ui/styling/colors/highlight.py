from __future__ import annotations

from enum import Enum
from typing import Dict, Literal

from hyperscale.ui.config.mode import TerminalMode

from .extended_color import ExtendedColorName, ExtendedColorType

HighlightName = Literal[
    "on_black",
    "on_grey",  # Actually black but kept for backwards compatibility
    "on_red",
    "on_green",
    "on_yellow",
    "on_blue",
    "on_magenta",
    "on_cyan",
    "on_light_grey",
    "on_dark_grey",
    "on_light_red",
    "on_light_green",
    "on_light_yellow",
    "on_light_blue",
    "on_light_magenta",
    "on_light_cyan",
    "on_white",
]


class HighlightType(Enum):
    ON_BLACK = 40
    ON_GREY = 40
    ON_RED = 41
    ON_GREEN = 42
    ON_YELLOW = 43
    ON_BLUE = 44
    ON_MAGENTA = 45
    ON_CYAN = 46
    ON_LIGHT_GREY = 47
    ON_DARK_GREY = 100
    ON_LIGHT_RED = 101
    ON_LIGHT_GREEN = 102
    ON_LIGHT_YELLOW = 103
    ON_LIGHT_BLUE = 104
    ON_LIGHT_MAGENTA = 105
    ON_LIGHT_CYAN = 106
    ON_WHITE = 107


class Highlight:
    names: Dict[
        HighlightName,
        int,
    ] = {attr.name.lower(): attr.value for attr in HighlightType}

    extended_names: Dict[
        ExtendedColorName,
        int,
    ] = {attr.name.lower(): attr.value for attr in ExtendedColorType}

    types: Dict[
        HighlightType,
        int,
    ] = {attr: attr.value for attr in HighlightType}

    extended_types: Dict[
        ExtendedColorType,
        int,
    ] = {attr: attr.value for attr in ExtendedColorType}

    def __iter__(self):
        for name in self.names:
            yield name

    def __contains__(self, highlight: HighlightName | ExtendedColorName):
        return highlight in self.names or highlight in self.extended_names

    @classmethod
    def by_name(
        cls,
        highlight: HighlightName | ExtendedColorName,
        default: int = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        if mode == TerminalMode.EXTENDED:
            return cls.extended_names.get(highlight, default)

        if default is None:
            default = 0

        return cls.names.get(highlight, default)

    @classmethod
    def by_type(
        cls,
        highlight: HighlightType,
        default: int = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        if mode == TerminalMode.EXTENDED:
            return cls.extended_types.get(mode, default)

        if default is None:
            default = 0

        return cls.types.get(highlight, default)
