from dataclasses import dataclass

from hyperscale.ui.components.scatter_plot.point_char import PointCharName
from hyperscale.ui.styling.colors import Colorizer


@dataclass(slots=True, frozen=True)
class NodeDashboardChart:
    """One live time series a role's chart plots: its key (``name``),
    the title its legend and reading show, and the color and point
    character its points are drawn in. The chart's value axis scales to
    every series' values, as the run UI's chart does to its one: a fixed
    top would sit on the largest possible value, which the plot never
    draws (no point lands on an axis' maximum).

    A series that does not ``plots_zero`` -- failures -- draws no point
    for a zero: its points show only when it happens (its legend reading
    still shows its zero), so a quiet node's chart is not lined with
    alarm-colored marks along its axis."""

    name: str
    title: str
    color: Colorizer
    point_char: PointCharName
    plots_zero: bool = True
