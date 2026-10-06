from dataclasses import dataclass

TableRow = dict[str, str | int | float]


@dataclass(slots=True)
class NodeDashboardFrame:
    """One sample of a node's state, as the dashboard shows it: the lines
    of each panel and the rows of its table."""

    identity_lines: list[str]
    cluster_lines: list[str]
    summary_lines: list[str]
    detail_lines: list[str]
    table_rows: list[TableRow]
