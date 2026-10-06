from dataclasses import dataclass

from hyperscale.ui.components.table.table_config import HeaderOptions


@dataclass(slots=True, frozen=True)
class NodeDashboardLayout:
    """What a role's dashboard shows besides its live values: the role's
    name and its table's columns."""

    role: str
    table_headers: dict[str, HeaderOptions]
