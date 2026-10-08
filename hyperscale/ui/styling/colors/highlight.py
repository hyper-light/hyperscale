from __future__ import annotations

from typing import Dict, Literal

from hyperscale.ui.config.mode import TerminalMode

from .highlight_type import HighlightType as HighlightType
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
