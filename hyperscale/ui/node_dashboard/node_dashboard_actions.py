"""The node dashboards' update actions: each publishes one section's new
content to the components subscribed to its channel.

A channel is never named as its action: the terminal also subscribes a
component to the channel named after an action, so a component listening
on such a channel would receive every update twice.
"""

from hyperscale.ui.components.scatter_plot import SeriesUpdate
from hyperscale.ui.components.stat_tile import StatTileReading
from hyperscale.ui.components.status_badge import StatusBadgeReading
from hyperscale.ui.components.table.tabulate import TableCell
from hyperscale.ui.components.terminal import action

IDENTITY_CHANNEL = "node_dashboard_identity_content"
BADGES_CHANNEL = "node_dashboard_badges_content"
TABLE_CHANNEL = "node_dashboard_table_content"
STATUS_CHANNEL = "node_dashboard_status_content"
CHART_CHANNEL = "node_dashboard_chart_content"


def tile_channel(tile_index: int) -> str:
    """The channel of the tile at ``tile_index`` in its row."""
    return f"node_dashboard_tile_{tile_index}_content"


@action()
async def update_node_dashboard_identity(lines: list[str]):
    return (IDENTITY_CHANNEL, lines)


@action()
async def update_node_dashboard_badges(badges: list[StatusBadgeReading]):
    return (BADGES_CHANNEL, badges)


@action()
async def update_node_dashboard_tile(channel: str, reading: StatTileReading):
    return (channel, reading)


@action()
async def update_node_dashboard_table(rows: list[dict[str, TableCell]]):
    return (TABLE_CHANNEL, rows)


@action()
async def update_node_dashboard_status(status: str):
    return (STATUS_CHANNEL, status)


@action()
async def update_node_dashboard_chart(update: SeriesUpdate):
    return (CHART_CHANNEL, update)
