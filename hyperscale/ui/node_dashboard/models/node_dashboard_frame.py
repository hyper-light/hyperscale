from dataclasses import dataclass

from hyperscale.ui.components.meter.models import MeterReading
from hyperscale.ui.components.stat_tile.models import StatTileReading
from hyperscale.ui.components.status_badge.models import StatusBadgeReading
from hyperscale.ui.components.table.tabulate import TableCell

# A table cell: a value drawn as text, a status drawn as a badge, or a
# ratio drawn as a meter.
TableRowValue = TableCell | StatusBadgeReading | MeterReading
TableRow = dict[str, TableRowValue]


@dataclass(slots=True)
class NodeDashboardFrame:
    """One sample of a node's state, as the dashboard shows it: who the
    node is (``identity_lines``), its lifecycle state and how long it has
    run, the lines of each panel, the rows of its table, one value per
    chart series of the role's layout -- ``None`` where the node has no
    value to plot this sample (the clock has not moved), which leaves a
    gap, not a zero -- and its values in units other than the chart's
    (``value_lines``), taken at ``sampled_at`` on the node's own monotonic
    clock.

    The panel and value lines are every value the node reports, as the
    CI-safe summary line lists them; the full dashboard shows the same
    values as its ``badges`` (the node's status at a glance), its
    ``tiles`` (one per tile label of the layout, in order), its table and
    its chart, whose legend carries each series' reading and
    ``chart_extra_reading`` (None: none)."""

    identity_lines: list[str]
    lifecycle_state: str
    uptime_seconds: float
    cluster_lines: list[str]
    summary_lines: list[str]
    detail_lines: list[str]
    table_rows: list[TableRow]
    chart_values: list[float | None]
    value_lines: list[str]
    sampled_at: float
    badges: list[StatusBadgeReading]
    tiles: list[StatTileReading]
    chart_extra_reading: str | None
