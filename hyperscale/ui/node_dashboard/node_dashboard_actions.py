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
READINGS_CHANNEL = "node_dashboard_readings_content"
CHART_CHANNEL = "node_dashboard_chart_content"


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
async def update_node_dashboard_readings(lines: list[str]):
    return (READINGS_CHANNEL, lines)


@action()
async def update_node_dashboard_chart(series_points: dict[str, list[ChartPoint]]):
    return (CHART_CHANNEL, series_points)
