"""The node dashboards' update actions: each publishes one panel's new
content to the components subscribed to its channel.

A channel is never named as its action: the terminal also subscribes a
component to the channel named after an action, so a component listening
on such a channel would receive every update twice.
"""

from hyperscale.ui.components.terminal import action

from .models import TableRow
from .node_dashboard_chart_series import ChartPoint

IDENTITY_CHANNEL = "node_dashboard_identity_content"
CLUSTER_CHANNEL = "node_dashboard_cluster_content"
SUMMARY_CHANNEL = "node_dashboard_summary_content"
DETAIL_CHANNEL = "node_dashboard_detail_content"
TABLE_CHANNEL = "node_dashboard_table_content"
STATUS_CHANNEL = "node_dashboard_status_content"


def chart_channel(chart_name: str) -> str:
    """The channel the chart named ``chart_name`` (``NodeDashboardChart.
    name``) listens on."""
    return f"node_dashboard_chart_{chart_name}_content"


def chart_waiting_channel(chart_name: str) -> str:
    """The channel the line a chart shows while it waits for a value
    listens on."""
    return f"node_dashboard_chart_{chart_name}_waiting_content"


@action()
async def update_node_dashboard_identity(lines: list[str]):
    return (IDENTITY_CHANNEL, lines)


@action()
async def update_node_dashboard_cluster(lines: list[str]):
    return (CLUSTER_CHANNEL, lines)


@action()
async def update_node_dashboard_summary(lines: list[str]):
    return (SUMMARY_CHANNEL, lines)


@action()
async def update_node_dashboard_detail(lines: list[str]):
    return (DETAIL_CHANNEL, lines)


@action()
async def update_node_dashboard_table(rows: list[TableRow]):
    return (TABLE_CHANNEL, rows)


@action()
async def update_node_dashboard_status(status: str):
    return (STATUS_CHANNEL, status)


@action()
async def update_node_dashboard_chart(chart_name: str, points: list[ChartPoint]):
    return (chart_channel(chart_name), points)


@action()
async def update_node_dashboard_chart_waiting(chart_name: str, waiting_text: str):
    return (chart_waiting_channel(chart_name), waiting_text)
