"""The sections of a node's dashboard, top to bottom: the Hyperscale header
beside the node's identity; a line of status badges; a row of stat tiles;
the role's chart across the canvas, its legend carrying each series'
reading; the role's table; and a status line. Groups are set apart by a
single dim rule (a section's ``bottom_rule``), not boxed in, and every
section is indented from the canvas' edge by ``SECTION_PADDING``. Each
takes its rows of the canvas from a ``NodeDashboardRows``, so together
they fill it exactly."""

from collections.abc import Callable

from hyperscale.ui.components.multiline_text import MultilineText, MultilineTextConfig
from hyperscale.ui.components.scatter_plot import PlotConfig, PlotSeries, ScatterPlot
from hyperscale.ui.components.scatter_plot.plot_axes import TIME_MATCHED_VALUE_TICK_ROWS
from hyperscale.ui.components.stat_tile import StatTile, StatTileConfig
from hyperscale.ui.components.status_badge import StatusBadge, StatusBadgeConfig, tone_of_badge_text
from hyperscale.ui.components.table import Table, TableConfig
from hyperscale.ui.components.table.table_config import HeaderOptions
from hyperscale.ui.components.terminal import Section, SectionConfig
from hyperscale.ui.components.terminal.section_config import HorizontalSectionSize
from hyperscale.ui.components.text import Text, TextConfig
from hyperscale.ui.config.mode import TerminalDisplayMode, TerminalMode
from hyperscale.ui.hyperscale_header import create_hyperscale_header
from hyperscale.ui.styling.tones import TONE_PALETTES, PaletteColor

from .models import NodeDashboardLayout
from .node_dashboard_actions import (
    BADGES_CHANNEL,
    CHART_CHANNEL,
    IDENTITY_CHANNEL,
    STATUS_CHANNEL,
    TABLE_CHANNEL,
    tile_channel,
)
from .node_dashboard_rows import NodeDashboardRows

WAITING_TEXT = "waiting for the first sample"
IDENTITY_COMPONENT_NAME = "node_dashboard_identity"
BADGES_COMPONENT_NAME = "node_dashboard_badges"
CHART_COMPONENT_NAME = "node_dashboard_chart"
STATUS_COMPONENT_NAME = "node_dashboard_status"
# The columns between the canvas' edges and every section's content.
SECTION_PADDING = 1
# The share of the canvas each of a row of tiles takes, by their count.
TILE_SECTION_WIDTHS: dict[int, HorizontalSectionSize] = {2: "medium", 3: "small", 4: "x-small"}
# The color the identity column and the chart's axes are drawn in: the
# accent the Hyperscale header and `run workflow` share.
ACCENT_COLOR = "aquamarine_2"


def tile_component_name(tile_index: int) -> str:
    return f"node_dashboard_tile_{tile_index}"


def table_component_name(layout: NodeDashboardLayout) -> str:
    return f"node_dashboard_{layout.role}_table"


def content_width(section_width: int) -> int:
    """The columns a section ``section_width`` wide gives its content."""
    return section_width - 2 * SECTION_PADDING


def rule_section_config(
    display_mode: TerminalDisplayMode,
    width: HorizontalSectionSize,
    height_rows: Callable[[int], int],
    bottom_rule: bool = True,
) -> SectionConfig:
    """A borderless section indented by ``SECTION_PADDING``, set off from
    the next by a dim rule (``bottom_rule``)."""
    return SectionConfig(
        width=width,
        height="xx-small",
        height_rows=height_rows,
        left_padding=SECTION_PADDING,
        right_padding=SECTION_PADDING,
        bottom_rule=bottom_rule,
        border_color=TONE_PALETTES[TerminalMode.to_mode(display_mode)].rule_color,
        mode=display_mode,
    )


def header_sections(display_mode: TerminalDisplayMode, rows: NodeDashboardRows) -> list[Section]:
    """The run UI's header row: the Hyperscale header and, beside it where
    `run workflow` names its workflow, the node's role and identity."""
    return [
        Section(
            rule_section_config(display_mode, "large", rows.header_rows),
            components=[create_hyperscale_header(display_mode)],
        ),
        Section(
            rule_section_config(display_mode, "small", rows.header_rows),
            components=[
                MultilineText(
                    IDENTITY_COMPONENT_NAME,
                    MultilineTextConfig(
                        text=[WAITING_TEXT],
                        horizontal_alignment="right",
                        color=TONE_PALETTES[TerminalMode.to_mode(display_mode)].value_color,
                        terminal_mode=display_mode,
                    ),
                    subscriptions=[IDENTITY_CHANNEL],
                )
            ],
        ),
    ]


def badge_section(display_mode: TerminalDisplayMode, rows: NodeDashboardRows) -> Section:
    """The node's status at a glance: one line of badges, flowing onto
    more where they need them."""
    return Section(
        rule_section_config(display_mode, "full", rows.badge_rows),
        components=[
            StatusBadge(
                BADGES_COMPONENT_NAME,
                StatusBadgeConfig(terminal_mode=display_mode),
                subscriptions=[BADGES_CHANNEL],
            )
        ],
    )


def tile_sections(
    layout: NodeDashboardLayout,
    display_mode: TerminalDisplayMode,
    rows: NodeDashboardRows,
) -> list[Section]:
    """The role's headline numbers: a row of tiles of equal width."""
    tile_width = TILE_SECTION_WIDTHS[len(layout.tile_labels)]
    return [
        Section(
            rule_section_config(display_mode, tile_width, rows.tile_rows),
            components=[
                StatTile(
                    tile_component_name(tile_index),
                    StatTileConfig(label=label, meter_fill_color=ACCENT_COLOR, terminal_mode=display_mode),
                    subscriptions=[tile_channel(tile_index)],
                )
            ],
        )
        for tile_index, label in enumerate(layout.tile_labels)
    ]


def chart_section(layout: NodeDashboardLayout, display_mode: TerminalDisplayMode, rows: NodeDashboardRows) -> Section:
    """The role's one chart across the canvas: every series of the layout
    in one plot, seconds across and the series' shared unit up, under a
    legend naming each series in its color with its current reading --
    the series drawn last (on top) first."""
    return Section(
        rule_section_config(display_mode, "full", rows.chart_rows),
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
                    legend_order=[chart.title for chart in reversed(layout.charts)],
                    value_tick_rows=TIME_MATCHED_VALUE_TICK_ROWS,
                ),
                subscriptions=[CHART_CHANNEL],
            )
        ],
    )


def badge_cell_colorizer(display_mode: TerminalDisplayMode) -> Callable[[object], PaletteColor | None]:
    """The color of a table cell: its tone's, for a cell holding a badge's
    text; none for any other."""
    mode = TerminalMode.to_mode(display_mode)
    tone_colors = TONE_PALETTES[mode].tone_colors

    def badge_cell_color(cell: object) -> PaletteColor | None:
        tone = tone_of_badge_text(cell, mode) if isinstance(cell, str) else None
        return tone_colors.get(tone)

    return badge_cell_color


def node_dashboard_table_config(layout: NodeDashboardLayout, display_mode: TerminalDisplayMode) -> TableConfig:
    """The role's table: its columns left-aligned under dim headers, with
    no rule under them, each sized to its content -- the first column (a
    worker's address, a workflow's or a datacenter's name) is never cut;
    on a narrow terminal the columns are dropped from the right, the
    lowest priority last -- its status cells in their tones' colors,
    paging its rows when there are more than it has room for, and its
    role's line in place of its header while it has no rows."""
    label_color = TONE_PALETTES[TerminalMode.to_mode(display_mode)].label_color
    cell_color = badge_cell_colorizer(display_mode)
    return TableConfig(
        headers={
            header: HeaderOptions(
                **options.model_dump(exclude={"header_color", "data_color"}),
                header_color=label_color,
                data_color=cell_color,
            )
            for header, options in layout.table_headers.items()
        },
        size_columns_to_content=True,
        terminal_mode=display_mode,
        table_format="plain",
        header_alignment="LEFT",
        cell_alignment="LEFT",
        empty_message=layout.table_empty_message,
    )


def table_section(
    layout: NodeDashboardLayout,
    table_config: TableConfig,
    display_mode: TerminalDisplayMode,
    rows: NodeDashboardRows,
) -> Section:
    """The role's table across the canvas below its chart, given the rows
    it needs: it pages its rows when there are more than its height
    holds."""
    return Section(
        rule_section_config(display_mode, "full", rows.table_rows),
        components=[Table(table_component_name(layout), table_config, subscriptions=[TABLE_CHANNEL])],
    )


def status_section(display_mode: TerminalDisplayMode, rows: NodeDashboardRows) -> Section:
    """The status line: how to stop the node and where its logs go, or a
    sampling failure."""
    return Section(
        rule_section_config(display_mode, "full", rows.status_rows, bottom_rule=False),
        components=[
            Text(
                STATUS_COMPONENT_NAME,
                TextConfig(
                    text=WAITING_TEXT,
                    color=TONE_PALETTES[TerminalMode.to_mode(display_mode)].label_color,
                    horizontal_alignment="left",
                    terminal_mode=display_mode,
                ),
                subscriptions=[STATUS_CHANNEL],
            )
        ],
    )


def generate_node_dashboard_sections(
    layout: NodeDashboardLayout,
    table_config: TableConfig,
    display_mode: TerminalDisplayMode,
    rows: NodeDashboardRows,
) -> list[Section]:
    """The sections of a node's dashboard, laid out by ``rows``: the
    header with the node's identity, its badges, its tiles, its chart, its
    table (configured by ``table_config``) and its status line."""
    return [
        *header_sections(display_mode, rows),
        badge_section(display_mode, rows),
        *tile_sections(layout, display_mode, rows),
        chart_section(layout, display_mode, rows),
        table_section(layout, table_config, display_mode, rows),
        status_section(display_mode, rows),
    ]
