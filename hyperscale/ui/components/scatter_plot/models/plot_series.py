from dataclasses import dataclass

from hyperscale.ui.styling.colors import Colorizer

from hyperscale.ui.components.scatter_plot.point_char import PointCharName


@dataclass(slots=True, frozen=True)
class PlotSeries:
    """One series of a multi-series scatter plot: the name its legend shows
    and its updates are keyed by, and the color and point character its
    points (and its legend entry) are drawn in."""

    name: str
    color: Colorizer | None = None
    point_char: PointCharName | None = None
