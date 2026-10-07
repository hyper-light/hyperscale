"""Whether a node's stdout can show the full dashboard, detected at runtime.

The environment checks are the shared ones (``hyperscale.ui.ci_safe``);
whether the terminal is large enough and can encode the glyphs is the
node dashboard's own layout's to say (``layout_fallback``).
"""

import functools

from hyperscale.core.jobs.models import TerminalMode
from hyperscale.ui.ci_safe import TerminalSelection
from hyperscale.ui.ci_safe.terminal_capability import (
    LAYOUT_CHECK_DISPLAY_MODE,
    PLOT_POINT_GLYPH,
    Fallback,
    empty_frame_lines,
    encodes,
    first_fallback,
    select_terminal_mode,
)
from hyperscale.ui.components.terminal.canvas import Canvas
from hyperscale.ui.components.terminal.terminal import canvas_size
from hyperscale.ui.hyperscale_header import create_hyperscale_header

from .models import NodeDashboardLayout
from .node_dashboard import HORIZONTAL_PADDING, VERTICAL_PADDING
from .node_dashboard_rows import IDENTITY_LINE_COUNT, TABLE_MIN_ROWS
from .node_dashboard_sections import (
    IDENTITY_COMPONENT_NAME,
    generate_node_dashboard_sections,
    node_dashboard_table_config,
)

# What a degraded node's reason calls the UI it could not show.
FULL_UI_NAME = "the full dashboard"


async def layout_fallback(layout: NodeDashboardLayout, columns: int, lines: int, encoding: str | None) -> Fallback | None:
    """Why a terminal of ``columns`` x ``lines`` (0 x 0: size unknown)
    writing ``encoding`` cannot show ``layout``'s dashboard, or None when
    it can. The dashboard's own layout decides, sized as its Terminal sizes
    it: an empty frame must fit the canvas, the header row must hold the
    Hyperscale header's art unclipped and the identity column without
    paging, the table must have room for a row (an empty frame draws
    none), and the frame and the plot's point must encode."""
    if columns < 1 or lines <= 2 * VERTICAL_PADDING:
        return ("ci-safe", f"the terminal's size is unknown or too small ({columns}x{lines})")

    canvas_width, canvas_height = canvas_size(columns, lines, HORIZONTAL_PADDING, VERTICAL_PADDING)
    canvas = Canvas(
        generate_node_dashboard_sections(
            layout,
            node_dashboard_table_config(layout, LAYOUT_CHECK_DISPLAY_MODE),
            LAYOUT_CHECK_DISPLAY_MODE,
        )
    )
    frame_lines = await empty_frame_lines(canvas, canvas_width, canvas_height)
    # The header's art at its own size: a narrower header section clips it.
    header = create_hyperscale_header(LAYOUT_CHECK_DISPLAY_MODE)
    await header.fit()
    layout_overflows = any(
        (
            len(frame_lines) > canvas_height,
            max(map(len, frame_lines)) > canvas_width,
            canvas.get_section(IDENTITY_COMPONENT_NAME).height < IDENTITY_LINE_COUNT,
            canvas.get_section(f"node_dashboard_{layout.role}_table").height < TABLE_MIN_ROWS,
            canvas.get_section(header.name).width < header.raw_size,
        )
    )
    return first_fallback(
        (
            (layout_overflows, f"the terminal ({columns}x{lines}) is smaller than the dashboard's layout needs"),
            (
                not encodes("".join(frame_lines) + PLOT_POINT_GLYPH, encoding),
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
