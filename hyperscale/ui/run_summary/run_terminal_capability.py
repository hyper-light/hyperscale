"""Whether stdout can show ``hyperscale run workflow``'s full progress UI,
detected at runtime with the checks the node dashboards use
(``hyperscale.ui.ci_safe``); the run UI's own layout judges the terminal's
size and encoding (``run_layout_fallback``)."""

import functools

from hyperscale.core.graph import Workflow
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
from hyperscale.ui.components.terminal.canvas import Canvas
from hyperscale.ui.components.terminal.terminal import canvas_size
from hyperscale.ui.generate_ui_sections import generate_ui_sections
from hyperscale.ui.hyperscale_header import create_hyperscale_header
from hyperscale.ui.hyperscale_interface import HORIZONTAL_PADDING, VERTICAL_PADDING

# What a degraded run's reason calls the UI it could not show.
FULL_UI_NAME = "the full run UI"
# The run UI's display mode in its "full" terminal mode, whose glyphs the
# encoding check renders.
FULL_TERMINAL_MODE: TerminalMode = "full"
# The run UI's header art: what its header section must hold unclipped.
FULL_DISPLAY_MODE = "extended"


def has_clipped_section(canvas: Canvas) -> bool:
    """Whether any of ``canvas``'s sections is laid out shorter than its
    full height (its maximum): a bordered one-line section loses its line
    to its borders."""
    return any(section.height < (section.config.max_height or 0) for section in canvas.sections)


async def run_layout_fallback(
    workflows: list[Workflow],
    columns: int,
    lines: int,
    encoding: str | None,
) -> Fallback | None:
    """Why a terminal of ``columns`` x ``lines`` (0 x 0: size unknown)
    writing ``encoding`` cannot show the run UI of ``workflows``, or None
    when it can. The run UI's own layout decides, sized as its Terminal
    sizes it: an empty frame must fit the canvas, the header row must hold
    the Hyperscale header's art unclipped, every bordered one-line section
    must reach its full height (a shorter one loses its line to its
    borders), and the frame and the plot's point must encode."""
    if columns < 1 or lines <= 2 * VERTICAL_PADDING:
        return ("ci-safe", f"the terminal's size is unknown or too small ({columns}x{lines})")

    canvas_width, canvas_height = canvas_size(columns, lines, HORIZONTAL_PADDING, VERTICAL_PADDING)
    canvas = Canvas(generate_ui_sections(workflows, FULL_TERMINAL_MODE))
    frame_lines = await empty_frame_lines(canvas, canvas_width, canvas_height)
    header = create_hyperscale_header(FULL_DISPLAY_MODE)
    await header.fit()
    layout_overflows = any(
        (
            len(frame_lines) > canvas_height,
            max(map(len, frame_lines)) > canvas_width,
            canvas.get_section(header.name).width < header.raw_size,
            has_clipped_section(canvas),
        )
    )
    return first_fallback(
        (
            (layout_overflows, f"the terminal ({columns}x{lines}) is smaller than the run UI's layout needs"),
            (
                not encodes("".join(frame_lines) + PLOT_POINT_GLYPH, encoding),
                f"stdout's encoding ({encoding}) cannot write the run UI's glyphs",
            ),
        )
    )


async def select_run_terminal_mode(
    configured_mode: TerminalMode,
    quiet: bool,
    workflows: list[Workflow],
) -> TerminalSelection:
    """The mode ``hyperscale run workflow``'s progress UI runs in: none
    with ``--quiet``; an explicitly configured "ci", "ci-safe" or
    "disabled" as configured; "full" only where stdout can show the run UI
    of ``workflows``, otherwise "ci-safe" (or "disabled" with no usable
    stdout), with the reason."""
    return await select_terminal_mode(
        configured_mode,
        quiet,
        FULL_UI_NAME,
        functools.partial(run_layout_fallback, workflows),
    )
