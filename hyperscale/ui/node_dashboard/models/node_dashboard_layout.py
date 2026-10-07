from dataclasses import dataclass

from hyperscale.ui.components.table.table_config import HeaderOptions

from .node_dashboard_chart import NodeDashboardChart


@dataclass(slots=True, frozen=True)
class NodeDashboardLayout:
    """What a role's dashboard shows besides its live values: the role's
    name, its table's columns and its charts (in the order a frame's
    ``chart_values`` follow)."""

    role: str
    table_headers: dict[str, HeaderOptions]
    charts: tuple[NodeDashboardChart, ...]
