from hyperscale.ui.components.multiline_text import MultilineText, MultilineTextConfig
from hyperscale.ui.components.scatter_plot import PlotConfig, ScatterPlot
from hyperscale.ui.components.table import Table, TableConfig
from hyperscale.ui.components.terminal import Section, SectionConfig
from hyperscale.ui.components.text import Text, TextConfig
from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.hyperscale_header import create_hyperscale_header

from .models import NodeDashboardChart, NodeDashboardLayout
from .node_dashboard_actions import (
    CLUSTER_CHANNEL,
    DETAIL_CHANNEL,
    IDENTITY_CHANNEL,
    STATUS_CHANNEL,
    SUMMARY_CHANNEL,
    TABLE_CHANNEL,
    chart_channel,
    chart_waiting_channel,
)

WAITING_TEXT = "waiting for the first sample"
WAITING_FOR_VALUE_TEXT = "no value yet"
# A panel holds its title and up to six lines between its top and bottom
# borders; a shorter terminal pages the lines (MultilineText cycles them).
PANEL_MAX_HEIGHT = 9
# The status line: one line between its borders.
STATUS_MAX_HEIGHT = 3
# Panels and charts are "small" sections, a third of the canvas wide: three
# to a row (the last section of a row widens to fill what is left of it).
SECTIONS_PER_ROW = 3


def header_sections(display_mode: TerminalDisplayMode) -> list[Section]:
    """The run UI's header row: the Hyperscale header and, beside it where
    `run workflow` names its workflow, the node's role and identity."""
    return [
        Section(
            SectionConfig(height="xx-small", width="large"),
            components=[create_hyperscale_header(display_mode)],
        ),
        Section(
            SectionConfig(height="xx-small", width="small", vertical_alignment="center"),
            components=[
                MultilineText(
                    "node_dashboard_identity",
                    MultilineTextConfig(
                        text=[WAITING_TEXT],
                        horizontal_alignment="right",
                        color="hot_pink_3",
                        terminal_mode=display_mode,
                    ),
                    subscriptions=[IDENTITY_CHANNEL],
                )
            ],
        ),
    ]


def panel_section(
    component_name: str,
    channel: str,
    display_mode: TerminalDisplayMode,
    right_border: str | None,
) -> Section:
    """One third-width panel of lines, updated through ``channel``."""
    return Section(
        SectionConfig(
            width="small",
            height="x-small",
            max_height=PANEL_MAX_HEIGHT,
            left_border="|",
            right_border=right_border,
            top_border="-",
            bottom_border="-",
            left_padding=1,
            right_padding=1,
            mode=display_mode,
        ),
        components=[
            MultilineText(
                component_name,
                MultilineTextConfig(
                    text=[WAITING_TEXT],
                    color="aquamarine_2",
                    horizontal_alignment="left",
                    terminal_mode=display_mode,
                ),
                subscriptions=[channel],
            )
        ],
    )


def chart_plot_name(chart_name: str) -> str:
    """The component name of the chart named ``chart_name``'s plot."""
    return f"node_dashboard_chart_{chart_name}"


def chart_waiting_name(chart_name: str) -> str:
    """The component name of the line a chart shows while its window holds
    no value to plot."""
    return f"node_dashboard_chart_{chart_name}_waiting"


def chart_waiting_text(chart: NodeDashboardChart) -> str:
    """The line a chart shows while its window holds no value to plot."""
    return f"{chart.title}: {WAITING_FOR_VALUE_TEXT}"


def chart_section(
    chart: NodeDashboardChart,
    window_seconds: float,
    display_mode: TerminalDisplayMode,
    right_border: str | None,
) -> Section:
    """One third-width chart of the last ``window_seconds`` of samples, as
    the run UI plots its completions: seconds into the window across (the
    newest sample at ``window_seconds``), the chart's values up. The time
    axis is named by the window alone: the scatter plot narrows its plot by
    the width of its axis labels.

    The section opens on a line naming the chart and saying it waits for a
    value -- a plot with no point draws nothing at all, not even its axes --
    and the dashboard switches to the plot while its window holds values."""
    return Section(
        SectionConfig(
            width="small",
            height="x-small",
            left_border="|",
            right_border=right_border,
            top_border="-",
            bottom_border="-",
            left_padding=1,
            right_padding=1,
            horizontal_alignment="center",
            mode=display_mode,
        ),
        components=[
            Text(
                chart_waiting_name(chart.name),
                TextConfig(
                    text=chart_waiting_text(chart),
                    color=chart.color,
                    horizontal_alignment="center",
                    terminal_mode=display_mode,
                ),
                subscriptions=[chart_waiting_channel(chart.name)],
            ),
            ScatterPlot(
                chart_plot_name(chart.name),
                PlotConfig(
                    plot_name=chart.title,
                    x_axis_name=f"{window_seconds:g}s",
                    y_axis_name=chart.title,
                    line_color=chart.color,
                    point_char="dot",
                    terminal_mode=display_mode,
                ),
                subscriptions=[chart_channel(chart.name)],
            )
        ],
    )


def closes_chart_row(chart_index: int, last_chart_index: int) -> bool:
    """Whether the chart at ``chart_index`` is the last of its row: the
    third of it, or the last chart (which widens to fill its row)."""
    return chart_index % SECTIONS_PER_ROW == SECTIONS_PER_ROW - 1 or chart_index == last_chart_index


def chart_sections(
    layout: NodeDashboardLayout,
    window_seconds: float,
    display_mode: TerminalDisplayMode,
) -> list[Section]:
    """The role's charts, three to a row; the last of a row (and the last
    chart, which widens to fill its row) closes it with a right border."""
    last_chart_index = len(layout.charts) - 1
    return [
        chart_section(
            chart,
            window_seconds,
            display_mode,
            "|" if closes_chart_row(chart_index, last_chart_index) else None,
        )
        for chart_index, chart in enumerate(layout.charts)
    ]


def table_section(layout: NodeDashboardLayout, display_mode: TerminalDisplayMode) -> Section:
    """The role's table across the canvas below its charts: its columns
    need the width (a third of the canvas clips a worker's address), and
    the table cycles its rows when there are more than its height holds."""
    return Section(
        SectionConfig(
            width="full",
            height="xx-small",
            left_border="|",
            right_border="|",
            top_border="-",
            bottom_border="-",
            left_padding=1,
            right_padding=1,
            horizontal_alignment="center",
            mode=display_mode,
        ),
        components=[
            Table(
                f"node_dashboard_{layout.role}_table",
                TableConfig(
                    headers=layout.table_headers,
                    minimum_column_width=8,
                    border_color="aquamarine_2",
                    terminal_mode=display_mode,
                    table_format="simple",
                ),
                subscriptions=[TABLE_CHANNEL],
            )
        ],
    )


def status_section(display_mode: TerminalDisplayMode) -> Section:
    """The status line: where the node's logs go, or a sampling failure."""
    return Section(
        SectionConfig(
            width="full",
            height="smallest",
            max_height=STATUS_MAX_HEIGHT,
            left_border="|",
            right_border="|",
            top_border="-",
            bottom_border="-",
            left_padding=1,
            right_padding=1,
            mode=display_mode,
        ),
        components=[
            Text(
                "node_dashboard_status",
                TextConfig(text=WAITING_TEXT, horizontal_alignment="left", terminal_mode=display_mode),
                subscriptions=[STATUS_CHANNEL],
            )
        ],
    )


def generate_node_dashboard_sections(
    layout: NodeDashboardLayout,
    window_seconds: float,
    display_mode: TerminalDisplayMode,
) -> list[Section]:
    """The sections of a node's dashboard, laid out as `run workflow`'s:
    the Hyperscale header with the node's identity; the cluster panel and
    the role's summary and detail panels; the role's charts over the last
    ``window_seconds``, three to a row; the role's table; and a status line
    naming where the node's logs go (or a sampling failure)."""
    return [
        *header_sections(display_mode),
        panel_section("node_dashboard_cluster", CLUSTER_CHANNEL, display_mode, None),
        panel_section("node_dashboard_summary", SUMMARY_CHANNEL, display_mode, None),
        panel_section("node_dashboard_detail", DETAIL_CHANNEL, display_mode, "|"),
        *chart_sections(layout, window_seconds, display_mode),
        table_section(layout, display_mode),
        status_section(display_mode),
    ]
