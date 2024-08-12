from typing import Literal

from hyperscale.terminal.components.spinner import Spinner
from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.styling import stylize
from hyperscale.terminal.styling.colors import (
    ColorName,
    ExtendedColorName,
    HighlightName,
)

from .segment import Segment
from .segment_type import SegmentType


class Bar:
    def __init__(
        self,
        length: int,
        fill_char: str | Spinner | None = None,
        start_char: str | None = None,
        end_char: str | None = None,
        background_char: str | None = None,
        fill_color: ColorName | ExtendedColorName | int | None = None,
        fill_highlight: HighlightName | ExtendedColorName | int | None = None,
        border_color: ColorName | ExtendedColorName | int | None = None,
        border_highlight: HighlightName | ExtendedColorName | int | None = None,
        background_color: ColorName | ExtendedColorName | int | None = None,
        background_highlight: HighlightName | ExtendedColorName | int | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ) -> None:
        self.fill_char = fill_char
        self.start_char = start_char
        self.end_char = end_char
        self.background_char = background_char
        self.length = length
        self.fill_color = fill_color
        self.fill_highlight = fill_highlight
        self.border_color = border_color
        self.border_highlight = border_highlight
        self.background_color = background_color
        self.background_highlight = background_highlight
        self.mode = mode

        self._styled_fill = fill_char
        self._styled_start = start_char
        self._styled_end = end_char
        self._styled_background = background_char

        self.segments = []

    async def style(
        self,
        fill_color: ColorName | ExtendedColorName | int | None = None,
        fill_highlight: HighlightName | ExtendedColorName | int | None = None,
        border_color: ColorName | ExtendedColorName | int | None = None,
        border_highlight: HighlightName | ExtendedColorName | int | None = None,
        background_color: ColorName | ExtendedColorName | int | None = None,
        background_highlight: HighlightName | ExtendedColorName | int | None = None,
        mode: Literal["compatability", "extended"] | None = None,
    ):
        if fill_color is None:
            fill_color = self.fill_color

        if fill_highlight is None:
            fill_highlight = self.fill_highlight

        if border_color is None:
            border_color = self.border_color

        if border_highlight is None:
            border_highlight = self.border_highlight

        if background_color is None:
            background_color = self.background_color

        if background_highlight is None:
            background_highlight = self.background_highlight

        if isinstance(mode, str):
            mode: TerminalMode = TerminalMode.to_mode(mode)

        elif mode is None:
            mode = self.mode

        if fill_color or fill_highlight:
            self._styled_fill = await stylize(
                self.fill_char,
                color=fill_color,
                highlight=fill_highlight,
                mode=mode,
            )

        if border_color or border_highlight:
            self._styled_start = await stylize(
                self.start_char,
                color=border_color,
                highlight=border_highlight,
                mode=mode,
            )

            self._styled_end = await stylize(
                self.end_char,
                color=border_color,
                highlight=border_highlight,
                mode=mode,
            )

        if background_color or background_highlight:
            self._styled_background = await stylize(
                self.background_char,
                color=background_color,
                highlight=background_highlight,
                mode=mode,
            )

    async def run(self):
        self.segments.append(Segment(self._styled_start, SegmentType.START))

        self.segments.extend(
            [
                Segment(
                    self._styled_fill,
                    SegmentType.BAR,
                    segment_default_char=self._styled_background
                    if isinstance(self._styled_fill, Spinner) is False
                    else None,
                )
                for _ in range(self.length)
            ]
        )

        self.segments.append(Segment(self._styled_end, SegmentType.END))
