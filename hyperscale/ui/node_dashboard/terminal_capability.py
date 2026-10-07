"""Whether a node's stdout can show the full dashboard, detected at runtime.

The full dashboard redraws a screen of Unicode in place; anywhere it
cannot -- a pipe, a log collector, a CI job, a container without a
terminal, a terminal too small or unable to show its glyphs -- a node
configured for it writes append-only plain ASCII summary lines instead
("ci-safe"), and with no usable stdout at all, nothing.
"""

import asyncio
import re
import os
import shutil
import sys
from collections.abc import Callable, Mapping
from typing import TextIO

from hyperscale.core.jobs.models import TerminalMode
from hyperscale.ui.components.scatter_plot.point_char import PointChar
from hyperscale.ui.components.terminal.canvas import Canvas
from hyperscale.ui.components.terminal.terminal import canvas_size
from hyperscale.ui.hyperscale_header import create_hyperscale_header

from .models import NodeDashboardLayout, NodeTerminalSelection
from .node_dashboard import HORIZONTAL_PADDING, VERTICAL_PADDING
from .node_dashboard_rows import IDENTITY_LINE_COUNT, TABLE_MIN_ROWS
from .node_dashboard_sections import (
    IDENTITY_COMPONENT_NAME,
    generate_node_dashboard_sections,
    node_dashboard_table_config,
)

# Environment variables CI providers set in every job: GitHub Actions sets
# CI=true and GITHUB_ACTIONS=true ("Variables reference", default
# environment variables); GitLab CI/CD, CircleCI, Travis CI, Buildkite and
# Bitbucket Pipelines set CI=true (each one's predefined variables); Azure
# Pipelines sets TF_BUILD=True; Jenkins sets JENKINS_URL.
CI_ENVIRONMENT_VARIABLES = ("CI", "GITHUB_ACTIONS", "TF_BUILD", "JENKINS_URL")
# What a terminal reports as TERM when it can do nothing but print text.
DUMB_TERMINAL = "dumb"
# The plot's point character: the one glyph an empty dashboard frame (which
# the encoding check renders) does not draw.
PLOT_POINT_GLYPH = PointChar.by_name("dot")
# The display mode the layout check renders in: the full dashboard's glyphs.
LAYOUT_CHECK_DISPLAY_MODE = "compatability"
# Color sequences (styled text on a terminal) take no column: a line's
# width is its length without them.
COLOR_SEQUENCE = re.compile(r"\x1b\[[0-9;:]*m")

Fallback = tuple[TerminalMode, str]
EnvironmentCheck = tuple[Callable[[], bool], TerminalMode, str]


def environment_fallback(stdout: TextIO | None, environment: Mapping[str, str]) -> Fallback | None:
    """The mode a node falls back to, and why, when its stdout or its
    environment rules the full dashboard out; None when neither does."""
    checks: tuple[EnvironmentCheck, ...] = (
        (lambda: stdout is None or stdout.closed, "disabled", "stdout is closed"),
        (lambda: not stdout.isatty(), "ci-safe", "stdout is not a terminal"),
        (lambda: environment.get("TERM", DUMB_TERMINAL) == DUMB_TERMINAL, "ci-safe", "TERM is unset or dumb"),
        (
            lambda: any(name in environment for name in CI_ENVIRONMENT_VARIABLES),
            "ci-safe",
            f"a CI environment variable ({', '.join(CI_ENVIRONMENT_VARIABLES)}) is set",
        ),
    )
    return next(((mode, reason) for check, mode, reason in checks if check()), None)


def encodes(text: str, encoding: str | None) -> bool:
    """Whether ``text`` can be written in ``encoding`` (None: unknown)."""
    try:
        text.encode(encoding or "ascii")

    except UnicodeEncodeError:
        return False

    return True


def first_fallback(checks: tuple[tuple[bool, str], ...]) -> Fallback | None:
    """The CI-safe fallback for the first of ``checks`` that failed, or
    None when none did."""
    return next((("ci-safe", reason) for failed, reason in checks if failed), None)


async def empty_frame_lines(layout: NodeDashboardLayout, canvas: Canvas, canvas_width: int, canvas_height: int) -> list[str]:
    """``layout``'s frame before any sample, laid out on ``canvas``, as
    lines of text: their length is their width (the canvas ends each with
    a carriage return, and styled text carries color sequences, neither
    of which takes a column)."""
    await canvas.initialize(width=canvas_width, height=canvas_height)
    return COLOR_SEQUENCE.sub("", await canvas.render()).replace("\r", "").split("\n")


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
    frame_lines = await empty_frame_lines(layout, canvas, canvas_width, canvas_height)
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


async def stdout_fallback(layout: NodeDashboardLayout, stdout: TextIO | None) -> Fallback | None:
    """Why ``stdout`` cannot show ``layout``'s full dashboard -- its
    environment, then its terminal's size and encoding -- or None."""
    loop = asyncio.get_running_loop()
    if (fallback := await loop.run_in_executor(None, environment_fallback, stdout, os.environ)) is not None:
        return fallback

    terminal_size = await loop.run_in_executor(None, shutil.get_terminal_size, (0, 0))
    return await layout_fallback(layout, terminal_size.columns, terminal_size.lines, stdout.encoding)


async def select_node_terminal_mode(
    configured_mode: TerminalMode,
    quiet: bool,
    layout: NodeDashboardLayout,
) -> NodeTerminalSelection:
    """The mode a ``hyperscale run worker|manager|gate`` node's dashboard
    runs in: none with ``--quiet`` (as ``run workflow``); an explicitly
    configured "ci", "ci-safe" or "disabled" as configured; "full" only
    where stdout can show it, otherwise "ci-safe" (or "disabled" with no
    usable stdout), with the reason."""
    if quiet:
        return NodeTerminalSelection("disabled", None)

    if configured_mode != "full":
        return NodeTerminalSelection(configured_mode, None)

    return await full_mode_selection(layout)


async def full_mode_selection(layout: NodeDashboardLayout) -> NodeTerminalSelection:
    """"full" where stdout can show ``layout``'s dashboard, otherwise its
    fallback with the reason."""
    if (fallback := await stdout_fallback(layout, sys.stdout)) is None:
        return NodeTerminalSelection("full", None)

    fallback_mode, reason = fallback
    return NodeTerminalSelection(fallback_mode, f"the full dashboard cannot show here ({reason}): mode {fallback_mode}")
