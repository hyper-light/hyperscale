"""Whether stdout can show a full terminal UI, detected at runtime.

A full terminal UI -- a node's dashboard, a run's progress -- redraws a
screen of Unicode in place; anywhere it cannot -- a pipe, a log
collector, a CI job, a container without a terminal, a terminal too
small or unable to show its glyphs -- a UI configured for it writes
append-only plain ASCII summary lines instead ("ci-safe"), and with no
usable stdout at all, nothing.

The environment checks are the same for every UI; whether the terminal
is large enough and can encode the glyphs is each UI's own layout's to
say (a ``LayoutCheck``).
"""

import asyncio
import os
import re
import shutil
import sys
from collections.abc import Awaitable, Callable, Mapping
from typing import TextIO

from hyperscale.core.jobs.models import TerminalMode
from hyperscale.ui.components.scatter_plot.point_char import PointChar
from hyperscale.ui.components.terminal.canvas import Canvas

from .models import TerminalSelection

# Environment variables CI providers set in every job: GitHub Actions sets
# CI=true and GITHUB_ACTIONS=true ("Variables reference", default
# environment variables); GitLab CI/CD, CircleCI, Travis CI, Buildkite and
# Bitbucket Pipelines set CI=true (each one's predefined variables); Azure
# Pipelines sets TF_BUILD=True; Jenkins sets JENKINS_URL.
CI_ENVIRONMENT_VARIABLES = ("CI", "GITHUB_ACTIONS", "TF_BUILD", "JENKINS_URL")
# What a terminal reports as TERM when it can do nothing but print text.
DUMB_TERMINAL = "dumb"
# The plot's point character: the one glyph an empty frame (which the
# encoding check renders) does not draw.
PLOT_POINT_GLYPH = PointChar.by_name("dot")
# The display mode the layout check renders in: the full UI's glyphs.
LAYOUT_CHECK_DISPLAY_MODE = "compatability"
# Color sequences (styled text on a terminal) take no column: a line's
# width is its length without them.
COLOR_SEQUENCE = re.compile(r"\x1b\[[0-9;:]*m")

Fallback = tuple[TerminalMode, str]
EnvironmentCheck = tuple[Callable[[], bool], TerminalMode, str]
# Why a terminal of (columns, lines) writing an encoding cannot show a
# UI's layout, or None when it can.
LayoutCheck = Callable[[int, int, str | None], Awaitable[Fallback | None]]


def environment_fallback(stdout: TextIO | None, environment: Mapping[str, str]) -> Fallback | None:
    """The mode a UI falls back to, and why, when its stdout or its
    environment rules the full UI out; None when neither does."""
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


async def empty_frame_lines(canvas: Canvas, canvas_width: int, canvas_height: int) -> list[str]:
    """The frame ``canvas`` draws before any update, laid out at
    ``canvas_width`` x ``canvas_height``, as lines of text: their length is
    their width (the canvas ends each with a carriage return, and styled
    text carries color sequences, neither of which takes a column)."""
    await canvas.initialize(width=canvas_width, height=canvas_height)
    return COLOR_SEQUENCE.sub("", await canvas.render()).replace("\r", "").split("\n")


async def stdout_fallback(stdout: TextIO | None, layout_check: LayoutCheck) -> Fallback | None:
    """Why ``stdout`` cannot show a full UI -- its environment, then its
    terminal's size and encoding as ``layout_check`` judges them -- or
    None."""
    loop = asyncio.get_running_loop()
    if (fallback := await loop.run_in_executor(None, environment_fallback, stdout, os.environ)) is not None:
        return fallback

    terminal_size = await loop.run_in_executor(None, shutil.get_terminal_size, (0, 0))
    return await layout_check(terminal_size.columns, terminal_size.lines, stdout.encoding)


async def select_terminal_mode(
    configured_mode: TerminalMode,
    quiet: bool,
    full_ui_name: str,
    layout_check: LayoutCheck,
) -> TerminalSelection:
    """The mode a terminal UI runs in: none with ``--quiet``; an
    explicitly configured "ci", "ci-safe" or "disabled" as configured;
    "full" only where stdout can show it (``layout_check`` judging the
    terminal's size and encoding), otherwise "ci-safe" (or "disabled" with
    no usable stdout), with the reason naming ``full_ui_name``."""
    if quiet:
        return TerminalSelection("disabled", None)

    if configured_mode != "full":
        return TerminalSelection(configured_mode, None)

    return await full_mode_selection(full_ui_name, layout_check)


async def full_mode_selection(full_ui_name: str, layout_check: LayoutCheck) -> TerminalSelection:
    """"full" where stdout can show the UI ``layout_check`` lays out,
    otherwise its fallback with the reason."""
    if (fallback := await stdout_fallback(sys.stdout, layout_check)) is None:
        return TerminalSelection("full", None)

    fallback_mode, reason = fallback
    return TerminalSelection(fallback_mode, f"{full_ui_name} cannot show here ({reason}): mode {fallback_mode}")
