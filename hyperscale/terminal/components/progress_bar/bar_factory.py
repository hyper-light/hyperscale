from typing import Literal

from hyperscale.terminal.components.spinner import (
    SpinnerName,
    SpinnerType,
)
from hyperscale.terminal.config.mode import TerminalMode

from .background_char import BackgroundChar, BackgroundCharName
from .bar import Bar
from .end_char import EndChar, EndCharName
from .fill_char import FillChar, FillCharName
from .progress_bar_chars import ProgressBarChars
from .progress_bar_color_config import ProgressBarColorConfig
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
        active_char: SpinnerName | str = "dots",
        ok_char: FillCharName | str = "block",
        fail_char: FillCharName | str = "block",
        borders_char: StartCharName | EndCharName | str | None = None,
        background_char: BackgroundCharName | str | None = None,
        colors: ProgressBarColorConfig | None = None,
        mode: Literal["compatability", "extended"] = "compatability",
    ):
        return Bar(
            length,
            ProgressBarChars(
                **{
                    "active_char": self.fill.by_name(
                        active_char,
                        default=active_char,
                    ),
                    "ok_char": self.fill.by_name(
                        ok_char,
                        default=ok_char,
                    ),
                    "fail_char": self.fill.by_name(
                        fail_char,
                        default=fail_char,
                    ),
                    "start_char": self.start.by_name(
                        borders_char,
                        default=borders_char,
                    ),
                    "end_char": self.end.by_name(
                        borders_char,
                        default=borders_char,
                    ),
                    "background_char": self.background.by_name(
                        background_char,
                        default=" ",
                    ),
                }
            ),
            colors=colors,
            mode=TerminalMode.to_mode(mode),
        )
