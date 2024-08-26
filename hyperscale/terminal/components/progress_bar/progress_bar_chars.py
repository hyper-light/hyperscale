from pydantic import BaseModel, StrictStr

from hyperscale.terminal.components.spinner import (
    SpinnerName,
)


class ProgressBarChars(BaseModel):
    active_char: SpinnerName | None
    ready_char: StrictStr = "-"
    ok_char: StrictStr = "✔"
    fail_char: StrictStr = "✘"
    start_char: StrictStr = "["
    end_char: StrictStr = "]"
    background_char: StrictStr = " "
