from pydantic import BaseModel, conint

from hyperscale.terminal.styling.colors import (
    ColorName,
    ExtendedColorName,
    HighlightName,
)


class ProgressBarColorConfig(BaseModel):
    active_color: ColorName | ExtendedColorName | conint(ge=0, le=255) = "white"
    active_color_highlight: (
        HighlightName | ExtendedColorName | conint(ge=0, le=255) | None
    ) = None
    ready_color: ColorName | ExtendedColorName | conint(ge=0, le=255) | None = "white"
    ready_color_highlight: (
        HighlightName | ExtendedColorName | conint(ge=0, le=255) | None
    ) = None
    fail_color: ColorName | ExtendedColorName | conint(ge=0, le=255) | None = "white"
    fail_color_highlight: (
        HighlightName | ExtendedColorName | conint(ge=0, le=255) | None
    ) = None
    ok_color: ColorName | ExtendedColorName | conint(ge=0, le=255) | None = "white"
    ok_color_highlight: (
        HighlightName | ExtendedColorName | conint(ge=0, le=255) | None
    ) = None
