from dataclasses import dataclass

from hyperscale.ui.components.table.table_config import HeaderOptions

from .node_dashboard_chart import NodeDashboardChart


@dataclass(slots=True, frozen=True)
class NodeDashboardLayout:
    """What a role's dashboard shows besides its live values: the role's
    name, its table's columns, the unit its chart's value axis is in and
    the chart's series (in the order a frame's ``chart_values`` follow --
    series of that one unit only; values in other units are listed beside
    the chart, as a frame's ``value_lines``)."""

    role: str
    table_headers: dict[str, HeaderOptions]
    chart_unit: str
    charts: tuple[NodeDashboardChart, ...]
