"""Whether a node's stdout can show the full dashboard, detected at runtime.

The environment checks are the shared ones (``hyperscale.ui.ci_safe``);
whether the terminal is large enough and can encode the glyphs is the
node dashboard's own layout's to say (``layout_fallback``).
"""

import functools

from hyperscale.core.jobs.models import TerminalMode
from hyperscale.ui.ci_safe import TerminalSelection
from hyperscale.ui.ci_safe.terminal_capability import (
    PLOT_POINT_GLYPH,
    Fallback,
    empty_frame_lines,
    encodes,
    first_fallback,
    select_terminal_mode,
)
from hyperscale.ui.components.meter.meter_glyph_sets import EXTENDED_METER_GLYPHS
from hyperscale.ui.components.status_badge.status_badge_glyph_sets import EXTENDED_BADGE_GLYPHS
from hyperscale.ui.components.terminal.canvas import Canvas
from hyperscale.ui.components.terminal.terminal import canvas_size
from hyperscale.ui.hyperscale_header import create_hyperscale_header

from .models import NodeDashboardLayout
from .node_dashboard import HORIZONTAL_PADDING, VERTICAL_PADDING, WIDTH_SHARE
from .node_dashboard_rows import IDENTITY_LINE_COUNT, NodeDashboardRows
from .node_dashboard_sections import (
    IDENTITY_COMPONENT_NAME,
    generate_node_dashboard_sections,
    node_dashboard_table_config,
)

# What a degraded node's reason calls the UI it could not show.
FULL_UI_NAME = "the full dashboard"
# The glyphs an empty dashboard frame (which the encoding check renders)
# does not draw: the plot's point character, the badges' and the meters'.
UNDRAWN_GLYPHS = "".join(
    [
        PLOT_POINT_GLYPH,
        *EXTENDED_BADGE_GLYPHS.values(),
        *EXTENDED_METER_GLYPHS.cell_steps,
        EXTENDED_METER_GLYPHS.full,
        EXTENDED_METER_GLYPHS.empty,
    ]
)
# The display mode the layout check renders the dashboard in: the full
# dashboard's own, so the frame it encodes carries the full dashboard's
# glyphs (its rules among them).
NODE_LAYOUT_CHECK_DISPLAY_MODE = "extended"


async def layout_fallback(layout: NodeDashboardLayout, columns: int, lines: int, encoding: str | None) -> Fallback | None:
    """Why a terminal of ``columns`` x ``lines`` (0 x 0: size unknown)
    writing ``encoding`` cannot show ``layout``'s dashboard, or None when
    it can. The dashboard's own layout decides, sized as its Terminal sizes
    it: an empty frame must fit the canvas, the header row must hold the
    Hyperscale header's art unclipped and the identity column without
    paging, the table must have room for a row (an empty frame draws
    none), and the frame and the glyphs it draws once sampled must
    encode."""
    if columns < 1 or lines <= 2 * VERTICAL_PADDING:
        return ("ci-safe", f"the terminal's size is unknown or too small ({columns}x{lines})")

    canvas_width, canvas_height = canvas_size(columns, lines, HORIZONTAL_PADDING, VERTICAL_PADDING, WIDTH_SHARE)
    rows = NodeDashboardRows()
    canvas = Canvas(
        generate_node_dashboard_sections(
            layout,
            node_dashboard_table_config(layout, NODE_LAYOUT_CHECK_DISPLAY_MODE),
            NODE_LAYOUT_CHECK_DISPLAY_MODE,
            rows,
        )
    )
    frame_lines = await empty_frame_lines(canvas, canvas_width, canvas_height)
    # The header's art at its own size: a narrower header section clips it.
    header = create_hyperscale_header(NODE_LAYOUT_CHECK_DISPLAY_MODE)
    await header.fit()
    layout_overflows = any(
        (
            len(frame_lines) > canvas_height,
            max(map(len, frame_lines)) > canvas_width,
            canvas.get_section(IDENTITY_COMPONENT_NAME).height < IDENTITY_LINE_COUNT,
            not rows.fits(canvas_height),
            canvas.get_section(header.name).width < header.raw_size,
        )
    )
    return first_fallback(
        (
            (layout_overflows, f"the terminal ({columns}x{lines}) is smaller than the dashboard's layout needs"),
            (
                not encodes("".join(frame_lines) + UNDRAWN_GLYPHS, encoding),
                f"stdout's encoding ({encoding}) cannot write the dashboard's glyphs",
            ),
        )
    )


async def select_node_terminal_mode(
    configured_mode: TerminalMode,
    quiet: bool,
    layout: NodeDashboardLayout,
) -> TerminalSelection:
    """The mode a ``hyperscale run worker|manager|gate`` node's dashboard
    runs in: none with ``--quiet`` (as ``run workflow``); an explicitly
    configured "ci", "ci-safe" or "disabled" as configured; "full" only
    where stdout can show it, otherwise "ci-safe" (or "disabled" with no
    usable stdout), with the reason."""
    return await select_terminal_mode(
        configured_mode,
        quiet,
        FULL_UI_NAME,
        functools.partial(layout_fallback, layout),
    )
