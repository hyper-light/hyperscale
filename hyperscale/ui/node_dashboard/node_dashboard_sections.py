from hyperscale.ui.components.multiline_text import MultilineText, MultilineTextConfig
from hyperscale.ui.components.table import Table, TableConfig
from hyperscale.ui.components.terminal import Section, SectionConfig
from hyperscale.ui.components.text import Text, TextConfig
from hyperscale.ui.config.mode import TerminalDisplayMode

from .models import NodeDashboardLayout
from .node_dashboard_actions import (
    CLUSTER_CHANNEL,
    DETAIL_CHANNEL,
    IDENTITY_CHANNEL,
    STATUS_CHANNEL,
    SUMMARY_CHANNEL,
    TABLE_CHANNEL,
)

WAITING_TEXT = "waiting for the first sample"
# A panel holds its title and up to six lines between its top and bottom
# borders; a shorter terminal pages the lines (MultilineText cycles them).
PANEL_MAX_HEIGHT = 9
# The status line: one line between its borders.
STATUS_MAX_HEIGHT = 3


def panel_section(
    component_name: str,
    channel: str,
    display_mode: TerminalDisplayMode,
    right_border: str | None,
    color: str,
) -> Section:
    """One half-width panel of lines, updated through ``channel``."""
    return Section(
        SectionConfig(
            width="medium",
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
                    color=color,
                    horizontal_alignment="left",
                    terminal_mode=display_mode,
                ),
                subscriptions=[channel],
            )
        ],
    )


def generate_node_dashboard_sections(
    layout: NodeDashboardLayout,
    display_mode: TerminalDisplayMode,
) -> list[Section]:
    """The sections of a node's dashboard, in rows: identity and cluster
    panels, the role's summary and detail panels, the role's table, and a
    status line naming where the node's logs go (or a sampling failure)."""
    return [
        panel_section("node_dashboard_identity", IDENTITY_CHANNEL, display_mode, None, "aquamarine_2"),
        panel_section("node_dashboard_cluster", CLUSTER_CHANNEL, display_mode, "|", "aquamarine_2"),
        panel_section("node_dashboard_summary", SUMMARY_CHANNEL, display_mode, None, "hot_pink_3"),
        panel_section("node_dashboard_detail", DETAIL_CHANNEL, display_mode, "|", "hot_pink_3"),
        Section(
            SectionConfig(
                width="full",
                height="small",
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
        ),
        Section(
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
        ),
    ]
