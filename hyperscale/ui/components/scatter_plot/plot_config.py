from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)

from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.colors import Colorizer

from .point_char import PointCharName


class PlotConfig(BaseModel):
    plot_name: StrictStr
    x_range: StrictInt | None = None
    x_range_start: StrictInt = 0
    x_range_inclusive: StrictBool = False
    x_axis_name: StrictStr
    y_axis_name: StrictStr
    x_min: StrictInt | StrictFloat = 0
    y_min: StrictInt | StrictFloat = 0
    x_max: StrictInt | StrictFloat | None = None
    y_max: StrictInt | StrictFloat | None = None
    use_origin: StrictBool = True
    line_color: Colorizer | None = None
    terminal_mode: TerminalDisplayMode = "compatability"
    point_char: PointCharName | None = None
