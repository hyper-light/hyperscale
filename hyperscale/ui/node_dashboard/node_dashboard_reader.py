from typing import Protocol

from .models import NodeDashboardFrame, NodeDashboardLayout


class NodeDashboardReader(Protocol):
    """Reads one role's dashboard frame from its node's state --
    synchronously, without awaiting or sending anything."""

    layout: NodeDashboardLayout

    def read(self) -> NodeDashboardFrame:
        """Sample the node's state into one dashboard frame."""
        ...
