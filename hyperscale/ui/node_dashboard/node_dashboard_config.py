from pydantic import BaseModel, PositiveFloat


class NodeDashboardConfig(BaseModel):
    """How a node dashboard samples its node.

    ``sample_interval_seconds`` defaults to one second: the finest value
    the dashboard shows is uptime in whole seconds, and the rates it shows
    are the nodes' own windowed rates, recomputed over windows of a second
    or more -- sampling faster would redraw nothing new while costing the
    node's event loop a full sample each time. The dashboard never samples
    faster than its terminal refreshes.
    """

    sample_interval_seconds: PositiveFloat = 1.0
