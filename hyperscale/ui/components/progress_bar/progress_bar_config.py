import inspect
from pydantic import BaseModel, StrictStr, StrictInt
from hyperscale.ui.components.spinner.spinner_factory import SpinnerFactory
from hyperscale.ui.components.spinner.spinner_types import SpinnerName
from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.colors import Colorizer, HighlightColorizer
from typing import Any, AsyncGenerator, Iterable, Literal
from .background_char import BackgroundCharName, BackgroundChar
from .end_char import EndCharName, EndChar
from .fill_char import FillCharName, FillChar
from .start_char import StartCharName, StartChar


BarTextPosition = Literal["left", "right"]


class ProgressBarConfig(BaseModel):
    total: StrictInt
    active: SpinnerName | StrictStr = "dots"
    active_color: Colorizer | None = None
    active_highlight: HighlightColorizer | None = None
    border: StartCharName | EndCharName | StrictStr | None = None
    border_color: Colorizer | None = None
    border_highlight: HighlightColorizer | None = None
    complete: FillCharName | StrictStr = "block"
    complete_color: Colorizer | None = None
    complete_highlight: HighlightColorizer | None = None
    failed: FillCharName | StrictStr = "block"
    failed_color: Colorizer | None = None
    failed_highlight: HighlightColorizer | None = None
    incomlpete: BackgroundCharName | StrictStr = "empty"
    incomplete_color: Colorizer | None = None
    incomplete_highlight: HighlightColorizer | None = None
    terminal_mode: TerminalDisplayMode = "compatability"

    class Config:
        arbitrary_types_allowed = True

    def get_static_chars(self):
        complete_char = FillChar.by_name(self.complete, default=self.complete)
        failed_char = FillChar.by_name(self.failed, default=self.failed)

        incomplete_char = BackgroundChar.by_name(
            self.incomlpete, default=self.incomlpete
        )

        start_char: str | None = None
        end_char: str | None = None
        if self.border is not None:
            start_char = StartChar.by_name(start_char, default=start_char)
            end_char = EndChar.by_name(end_char, default=end_char)

        return (
            complete_char,
            end_char,
            failed_char,
            incomplete_char,
            start_char,
        )

    def get_active_spinner(self):
        factory = SpinnerFactory()
        spinner = factory.get(self.active)

        return spinner.frames
