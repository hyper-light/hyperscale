from dataclasses import dataclass

TableRow = dict[str, str | int | float]


@dataclass(slots=True)
class NodeDashboardFrame:
    """One sample of a node's state, as the dashboard shows it: the lines
    of each panel, the rows of its table, and one value per chart of the
    role's layout -- ``None`` where the node has no value to plot this
    sample (no latency observed yet), which leaves a gap, not a zero --
    taken at ``sampled_at`` on the node's own monotonic clock."""

    identity_lines: list[str]
    cluster_lines: list[str]
    summary_lines: list[str]
    detail_lines: list[str]
    table_rows: list[TableRow]
    chart_values: list[float | None]
    sampled_at: float
