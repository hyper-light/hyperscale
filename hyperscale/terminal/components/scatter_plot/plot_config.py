from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr

from hyperscale.terminal.styling.colors import (
    ColorName,
)

from .point_char import PointCharName


class PlotConfig(BaseModel):
    plot_name: StrictStr
    x_axis_name: StrictStr
    y_axis_name: StrictStr
    x_min: StrictInt | StrictFloat = 0
    y_min: StrictInt | StrictFloat = 0
    x_max: StrictInt | StrictFloat
    y_max: StrictInt | StrictFloat
    x_label: StrictStr
    y_label: StrictStr
    line_color: ColorName | None = None
    point_char: PointCharName | None = None
