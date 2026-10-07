from dataclasses import dataclass

TableRow = dict[str, str | int | float]


@dataclass(slots=True)
class NodeDashboardFrame:
    """One sample of a node's state, as the dashboard shows it: who the
    node is (``identity_lines``), its lifecycle state and how long it has
    run, the lines of each panel, the rows of its table, one value per
    chart series of the role's layout -- ``None`` where the node has no
    value to plot this sample (the clock has not moved), which leaves a
    gap, not a zero -- and its values in units other than the chart's
    (``value_lines``), taken at ``sampled_at`` on the node's own monotonic
    clock."""

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
