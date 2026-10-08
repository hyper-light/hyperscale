from dataclasses import dataclass

from hyperscale.ui.components.table.table_config import HeaderOptions

from .node_dashboard_chart import NodeDashboardChart


@dataclass(slots=True, frozen=True)
class NodeDashboardLayout:
    """What a role's dashboard shows besides its live values: the role's
    name, its table's columns and the line its table shows while it has
    no rows, the label of each of its stat tiles (a frame's ``tiles``
    follow their order), the unit its chart's value axis is in, the unit
    after each series' reading in the chart's legend, and the chart's
    series (in the order a frame's ``chart_values`` follow -- series of
    that one unit only; values in other units are in its tiles and the
    chart's extra reading)."""

    role: str
    table_headers: dict[str, HeaderOptions]
    table_empty_message: str
    tile_labels: tuple[str, ...]
    chart_unit: str
    chart_reading_unit: str
    charts: tuple[NodeDashboardChart, ...]
