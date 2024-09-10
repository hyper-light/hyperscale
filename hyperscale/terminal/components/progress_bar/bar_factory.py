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
    def __init__(
        self,
        default_size: int = 10,
    ) -> None:
        self.fill = FillChar()
        self.start = StartChar()
        self.end = EndChar()
        self.background = BackgroundChar()
        self.spinner_types = [spinner.value for spinner in SpinnerType]
        self._default_size = default_size

    def set_default_size(
        self,
        size: int,
    ):
        self._default_size = size

    def create_bar(
        self,
        size: int | None = None,
        active_char: SpinnerName | str = "dots",
        ok_char: FillCharName | str = "block",
        fail_char: FillCharName | str = "block",
        borders_char: StartCharName | EndCharName | str | None = None,
        background_char: BackgroundCharName | str | None = None,
        colors: ProgressBarColorConfig | None = None,
        mode: Literal["compatability", "extended"] = "compatability",
        disable_output: bool = False,
    ):
        if size is None:
            size = self._default_size

        return Bar(
            size,
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
                },
            ),
            colors=colors,
            disable_output=disable_output,
            mode=TerminalMode.to_mode(mode),
        )
