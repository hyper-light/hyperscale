from typing import Literal

from hyperscale.terminal.components.spinner import (
    Spinner,
    SpinnerName,
    SpinnerType,
)
from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.styling.colors import (
    ColorName,
    ExtendedColorName,
    HighlightName,
)

from .background_char import BackgroundChar, BackgroundCharName
from .bar import Bar
from .end_char import EndChar, EndCharName
from .fill_char import FillChar, FillCharName
from .start_char import StartChar, StartCharName


class BarFactory:
    def __init__(self) -> None:
        self.fill = FillChar()
        self.start = StartChar()
        self.end = EndChar()
        self.background = BackgroundChar()
        self.spinner_types = [spinner.value for spinner in SpinnerType]

    def create_bar(
        self,
        length: int,
        fill_char: FillCharName | SpinnerName | str | None = None,
        borders_char: StartCharName | EndCharName | str | None = None,
        background_char: BackgroundCharName | str | None = None,
        fill_color: ColorName | ExtendedColorName | int | None = None,
        fill_highlight: HighlightName | ExtendedColorName | int | None = None,
        border_color: ColorName | ExtendedColorName | int | None = None,
        border_highlight: HighlightName | ExtendedColorName | int | None = None,
        background_color: ColorName | ExtendedColorName | int | None = None,
        background_highlight: HighlightName | ExtendedColorName | int | None = None,
        mode: Literal["compatability", "extended"] = "compatability",
    ):
        return Bar(
            length,
            fill_char=(
                Spinner(
                    spinner=fill_char,
                    color=fill_color,
                    highlight=fill_highlight,
                )
                if fill_char in self.spinner_types
                else self.fill.by_name(
                    fill_char,
                    default=fill_char if fill_char not in self.fill.names else None,
                )
            ),
            start_char=self.start.by_name(
                borders_char,
                default=borders_char if borders_char not in self.start.names else None,
            ),
            end_char=self.end.by_name(
                borders_char,
                default=borders_char if borders_char not in self.end.names else None,
            ),
            background_char=self.background.by_name(
                background_char,
                default=background_char
                if background_char not in self.background.names
                else None,
            ),
            fill_color=fill_color,
            fill_highlight=fill_highlight,
            border_color=border_color,
            border_highlight=border_highlight,
            background_color=background_color,
            background_highlight=background_highlight,
            mode=TerminalMode.to_mode(mode),
        )
