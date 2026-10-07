from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)

from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.colors import Colorizer

from .models import PlotSeries
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
    # Several series in one plot (see ScatterPlot); None plots one series
    # in ``line_color`` and ``point_char``.
    series: list[PlotSeries] | None = None
    # The order a multi-series plot's legend lists its series in, by name;
    # None lists them in their declared (drawing) order.
    legend_order: list[StrictStr] | None = None
    # The fewest rows between two labelled value-axis ticks: 1 labels as
    # many rows as nice ticks allow; plot_axes.TIME_MATCHED_VALUE_TICK_ROWS
    # spaces them as far apart as the time axis' labels.
    value_tick_rows: StrictInt = 1
