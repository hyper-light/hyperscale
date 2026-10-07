from dataclasses import dataclass

from hyperscale.ui.styling.colors import Colorizer


@dataclass(slots=True, frozen=True)
class NodeDashboardChart:
    """One live time series a role's dashboard plots: the component it
    renders as (``name``), the title on its value axis and its point
    color. Its value axis scales to the plotted values, as the run UI's
    chart does: a fixed top would sit on the largest possible value, which
    the plot never draws (no point lands on an axis' maximum)."""

    name: str
    title: str
    color: Colorizer
