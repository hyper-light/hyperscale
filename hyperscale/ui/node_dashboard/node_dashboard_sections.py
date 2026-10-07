from hyperscale.ui.components.multiline_text import MultilineText, MultilineTextConfig
from hyperscale.ui.components.scatter_plot import PlotConfig, PlotSeries, ScatterPlot
from hyperscale.ui.components.table import Table, TableConfig
from hyperscale.ui.components.terminal import Section, SectionConfig
from hyperscale.ui.components.text import Text, TextConfig
from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.hyperscale_header import create_hyperscale_header

from .models import NodeDashboardLayout
from .node_dashboard_actions import (
    CHART_CHANNEL,
    CLUSTER_CHANNEL,
    DETAIL_CHANNEL,
    IDENTITY_CHANNEL,
    READINGS_CHANNEL,
    STATUS_CHANNEL,
    SUMMARY_CHANNEL,
    TABLE_CHANNEL,
)
from .node_dashboard_rows import chart_rows, header_rows, panel_rows, status_rows, table_rows

WAITING_TEXT = "waiting for the first sample"
IDENTITY_COMPONENT_NAME = "node_dashboard_identity"
CHART_COMPONENT_NAME = "node_dashboard_chart"
READINGS_COMPONENT_NAME = "node_dashboard_readings"


def header_sections(display_mode: TerminalDisplayMode) -> list[Section]:
    """The run UI's header row: the Hyperscale header and, beside it where
    `run workflow` names its workflow, the node's role and identity."""
    return [
        Section(
            SectionConfig(height="xx-small", height_rows=header_rows, width="large"),
            components=[create_hyperscale_header(display_mode)],
        ),
        Section(
            SectionConfig(
                height="xx-small",
                height_rows=header_rows,
                width="small",
                vertical_alignment="center",
            ),
            components=[
                MultilineText(
                    IDENTITY_COMPONENT_NAME,
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
            height="xx-small",
            height_rows=panel_rows,
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


def chart_section(layout: NodeDashboardLayout, display_mode: TerminalDisplayMode) -> Section:
    """The role's one chart, padded and centered as the run UI's chart:
    every series of the layout in one plot, seconds across and the series'
    shared unit up, with a legend naming each series in its color."""
    return Section(
        SectionConfig(
            width="large",
            height="medium",
            height_rows=chart_rows,
            left_border="|",
            bottom_border="-",
            left_padding=4,
            right_padding=4,
            horizontal_alignment="center",
            mode=display_mode,
        ),
        components=[
            ScatterPlot(
                CHART_COMPONENT_NAME,
                PlotConfig(
                    plot_name=layout.chart_unit,
                    x_axis_name="Time (sec)",
                    y_axis_name=layout.chart_unit,
                    point_char="dot",
                    terminal_mode=display_mode,
                    series=[
                        PlotSeries(name=chart.title, color=chart.color, point_char=chart.point_char)
                        for chart in layout.charts
                    ],
                ),
                subscriptions=[CHART_CHANNEL],
            )
        ],
    )


def readings_section(display_mode: TerminalDisplayMode) -> Section:
    """Beside the chart, as the run UI's statistics table beside its own:
    each series' newest value, and the role's values in other units."""
    return Section(
        SectionConfig(
            width="small",
            height="medium",
            height_rows=chart_rows,
            left_border="|",
            right_border="|",
            bottom_border="-",
            left_padding=2,
            right_padding=2,
            mode=display_mode,
        ),
        components=[
            MultilineText(
                READINGS_COMPONENT_NAME,
                MultilineTextConfig(
                    text=[WAITING_TEXT],
                    color="aquamarine_2",
                    horizontal_alignment="left",
                    terminal_mode=display_mode,
                ),
                subscriptions=[READINGS_CHANNEL],
            )
        ],
    )


def node_dashboard_table_config(layout: NodeDashboardLayout, display_mode: TerminalDisplayMode) -> TableConfig:
    """The role's table: its columns, each sized to its content -- the
    first column (a worker's address, a workflow's or a datacenter's name)
    is never cut; on a narrow terminal the columns are dropped from the
    right, the lowest priority last -- paging its rows when there are more
    than it has room for."""
    return TableConfig(
        headers=layout.table_headers,
        size_columns_to_content=True,
        border_color="aquamarine_2",
        terminal_mode=display_mode,
        table_format="simple",
    )


def table_section(
    layout: NodeDashboardLayout,
    table_config: TableConfig,
    display_mode: TerminalDisplayMode,
) -> Section:
    """The role's table across the canvas below its chart, given every row
    the other sections leave: its columns need the width (a third of the
    canvas clips a worker's address), and it pages its rows when there are
    more than its height holds."""
    return Section(
        SectionConfig(
            width="full",
            height="xx-small",
            height_rows=table_rows,
            left_border="|",
            right_border="|",
            bottom_border="-",
            left_padding=1,
            right_padding=1,
            horizontal_alignment="center",
            mode=display_mode,
        ),
        components=[
            Table(
                f"node_dashboard_{layout.role}_table",
                table_config,
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
            height_rows=status_rows,
            left_border="|",
            right_border="|",
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
    table_config: TableConfig,
    display_mode: TerminalDisplayMode,
) -> list[Section]:
    """The sections of a node's dashboard, laid out as `run workflow`'s:
    the Hyperscale header with the node's identity; the cluster panel and
    the role's summary and detail panels; the role's chart beside its
    newest readings; the role's table (configured by ``table_config``);
    and a status line naming where the node's logs go (or a sampling
    failure). Each takes its rows of the canvas (node_dashboard_rows), so
    together they fill it exactly."""
    return [
        *header_sections(display_mode),
        panel_section("node_dashboard_cluster", CLUSTER_CHANNEL, display_mode, None),
        panel_section("node_dashboard_summary", SUMMARY_CHANNEL, display_mode, None),
        panel_section("node_dashboard_detail", DETAIL_CHANNEL, display_mode, "|"),
        chart_section(layout, display_mode),
        readings_section(display_mode),
        table_section(layout, table_config, display_mode),
        status_section(display_mode),
    ]
