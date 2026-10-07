"""
The terminal framework draws each frame in place on a real terminal (a
pty), for the node dashboards and `run workflow`'s UI alike:

- every frame is one write: synchronized output around cursor-home and the
  frame's lines -- no screen or scrollback clear between frames;
- a frame fills the terminal's rows and never passes its bottom: no
  newline after its last row and no line as wide as the terminal, so the
  screen never scrolls and the header stays on the same row;
- a line whose text did not change is byte-identical between frames;
- a resize (TIOCSWINSZ + SIGWINCH) lays every section out again: the next
  frame fits the new size.

A small screen model replays the bytes the terminal received: it tracks
the cursor's row through homes, newlines and autowraps and records any
scroll.
"""

import asyncio
import contextlib
import fcntl
import os
import pathlib
import pty
import re
import signal
import struct
import sys
import termios
import time
from collections.abc import AsyncIterator

import pytest

from hyperscale.core.graph import Workflow
from hyperscale.distributed.nodes import ManagerServer
from hyperscale.logging import Logger
from hyperscale.ui import actions
from hyperscale.ui.components.terminal import Terminal
from hyperscale.ui.generate_ui_sections import generate_ui_sections
from hyperscale.ui.node_dashboard import ManagerDashboardReader, NodeDashboard, NodeDashboardConfig
from tests.integration.cli.node_processes import reserve_port_blocks
from tests.integration.ui.node_dashboard_harness import dashboard_env, dashboard_tasks

STDOUT_DESCRIPTOR = 1
FRAME_START = b"\x1b[?2026h\x1b[H"
FRAME_END = b"\x1b[?2026l"
SCREEN_CLEAR = b"\x1b[2J"
SCROLLBACK_CLEAR = b"\x1b[3J"
ESCAPE_SEQUENCE = re.compile(rb"\x1b\[[0-9;:?]*[A-Za-z]")
HEADER_ART_LINE = "//__ \\\\// //_// //_// // // //__  //    __// // //_//"
CONSECUTIVE_FRAMES = 10
FRAME_WAIT_SECONDS = 30.0
RUN_UI_COMPONENT_NAMES = (
    "workflow_metadata",
    "run_progress",
    "run_message_display",
    "run_timer",
    "executions_counter",
    "total_executions",
    "executions_over_time",
    "execution_stats_table",
)


class RunUIWorkflow(Workflow):
    """A workflow for `run workflow`'s UI sections."""

    vus = 10
    duration = "30s"


def set_terminal_size(terminal_descriptor: int, columns: int, lines: int) -> None:
    fcntl.ioctl(terminal_descriptor, termios.TIOCSWINSZ, struct.pack("HHHH", lines, columns, 0, 0))


@contextlib.asynccontextmanager
async def pty_stdout(columns: int, lines: int, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[tuple[int, bytearray]]:
    """Stand a pty of ``columns`` x ``lines`` in for the terminal on
    stdout; yield its terminal side and what it receives (as written: the
    pty's output processing is off)."""
    # The terminal's size comes from the pty, not the environment.
    monkeypatch.delenv("COLUMNS", raising=False)
    monkeypatch.delenv("LINES", raising=False)
    controller_descriptor, terminal_descriptor = pty.openpty()
    attributes = termios.tcgetattr(terminal_descriptor)
    attributes[1] &= ~termios.OPOST
    termios.tcsetattr(terminal_descriptor, termios.TCSANOW, attributes)
    set_terminal_size(terminal_descriptor, columns, lines)
    os.set_blocking(controller_descriptor, False)
    saved_stdout_descriptor = os.dup(STDOUT_DESCRIPTOR)
    os.dup2(terminal_descriptor, STDOUT_DESCRIPTOR)
    saved_stdout = sys.stdout
    sys.stdout = open(STDOUT_DESCRIPTOR, "w", closefd=False)
    received = bytearray()
    loop = asyncio.get_running_loop()

    def on_readable() -> None:
        with contextlib.suppress(BlockingIOError):
            received.extend(os.read(controller_descriptor, 1 << 16))

    loop.add_reader(controller_descriptor, on_readable)
    try:
        yield terminal_descriptor, received
    finally:
        sys.stdout.close()
        sys.stdout = saved_stdout
        os.dup2(saved_stdout_descriptor, STDOUT_DESCRIPTOR)
        os.close(saved_stdout_descriptor)
        loop.remove_reader(controller_descriptor)
        os.close(controller_descriptor)
        os.close(terminal_descriptor)


def frames_in(received: bytes) -> list[bytes]:
    """Each complete frame the terminal received: its lines, between the
    frame's start and end sequences."""
    return [
        chunk.split(FRAME_END, 1)[0]
        for chunk in received.split(FRAME_START)[1:]
        if FRAME_END in chunk
    ]


async def consecutive_frames(received: bytearray, count: int, after: int = 0) -> list[bytes]:
    deadline = time.monotonic() + FRAME_WAIT_SECONDS
    while time.monotonic() < deadline:
        if len(frames := frames_in(bytes(received[after:]))) >= count:
            return frames[:count]
        await asyncio.sleep(0.1)
    raise AssertionError(f"fewer than {count} frames arrived:\n{bytes(received[-3000:])!r}")


def visible(line: bytes) -> str:
    return ESCAPE_SEQUENCE.sub(b"", line).decode().replace("\r", "")


def scrolled_rows(stream: bytes, lines: int) -> int:
    """How many times a terminal of ``lines`` rows receiving ``stream``
    scrolls on a newline written on its last row (lines narrower than the
    terminal, asserted apart, never autowrap)."""
    row, scrolls = 1, 0
    for token in re.split(rb"(\x1b\[H|\n)", stream):
        if token == b"\x1b[H":
            row = 1
        elif token == b"\n":
            scrolls += row == lines
            row = min(row + 1, lines)
    return scrolls


def assert_frames_fit_in_place(frames: list[bytes], columns: int, lines: int, fills_rows: bool = True) -> None:
    """Every frame fits the terminal's rows (``fills_rows``: exactly),
    never scrolls it, keeps the header on its row, and repeats unchanged
    lines byte for byte."""
    header_rows: set[int] = set()
    for frame in frames:
        frame_lines = frame.split(b"\n")
        assert not frame.endswith(b"\n"), "a frame ends with a newline: the screen scrolls"
        assert len(frame_lines) == lines or (not fills_rows and len(frame_lines) < lines), (
            f"a frame of {len(frame_lines)} rows on a terminal of {lines}"
        )
        assert max(len(visible(line)) for line in frame_lines) < columns, max(frame_lines, key=lambda line: len(visible(line)))
        # Every line is padded to the frame's width: none leaves a stale tail.
        assert len({len(visible(line)) for line in frame_lines}) == 1, sorted(
            {len(visible(line)): visible(line) for line in frame_lines}.items()
        )
        assert SCREEN_CLEAR not in frame and SCROLLBACK_CLEAR not in frame
        header_rows.update(index for index, line in enumerate(frame_lines) if HEADER_ART_LINE in visible(line))
        assert scrolled_rows(FRAME_START + frame + FRAME_END, lines) == 0

    assert len(header_rows) == 1, f"the header moved between frames: rows {sorted(header_rows)}"
    for earlier, later in zip(frames, frames[1:]):
        for earlier_line, later_line in zip(earlier.split(b"\n"), later.split(b"\n")):
            if visible(earlier_line) == visible(later_line):
                assert earlier_line == later_line, (earlier_line, later_line)


@contextlib.asynccontextmanager
async def manager_dashboard(tmp_path: pathlib.Path) -> AsyncIterator[NodeDashboard]:
    (manager_port,) = reserve_port_blocks([2])
    env = dashboard_env(tmp_path)
    manager = ManagerServer(host="127.0.0.1", tcp_port=manager_port, udp_port=manager_port + 1, env=env, dc_id="DC-DASH")
    dashboard = NodeDashboard(
        ManagerDashboardReader(manager),
        manager,
        "full",
        env,
        tmp_path / "manager.log",
        NodeDashboardConfig(sample_interval_seconds=0.2),
        Logger(),
    )
    await dashboard.start()
    try:
        yield dashboard
    finally:
        await dashboard.stop()


@pytest.mark.parametrize("lines", [30, 22])
async def test_dashboard_frames_stay_in_place_on_a_terminal(
    lines: int, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    columns = 120
    async with pty_stdout(columns, lines, monkeypatch) as (_, received):
        async with manager_dashboard(tmp_path):
            frames = await consecutive_frames(received, CONSECUTIVE_FRAMES)

    assert_frames_fit_in_place(frames, columns, lines)
    assert dashboard_tasks() == []


async def test_a_resize_lays_the_dashboard_out_for_the_new_size(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    columns = 120
    async with pty_stdout(columns, 38, monkeypatch) as (terminal_descriptor, received):
        async with manager_dashboard(tmp_path):
            assert_frames_fit_in_place(await consecutive_frames(received, 2), columns, 38)
            resized_at = len(received)
            set_terminal_size(terminal_descriptor, columns, 24)
            os.kill(os.getpid(), signal.SIGWINCH)
            frames = await consecutive_frames(received, 3, after=resized_at)
            await asyncio.sleep(0)

    # The resize clears the screen once, then frames fit the new size --
    # and keep showing the node's values (a refit replays the last update;
    # never a frame of the panels' configured text).
    assert SCREEN_CLEAR in received[resized_at:]
    assert_frames_fit_in_place(frames, columns, 24)
    assert not any(b"waiting for the first sample" in frame for frame in frames)


async def test_run_workflow_ui_frames_stay_in_place_on_a_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    columns, lines = 120, 30
    workflow = RunUIWorkflow()
    workflow.graph = "run_ui_workflow.py"
    async with pty_stdout(columns, lines, monkeypatch) as (_, received):
        terminal = Terminal(generate_ui_sections([workflow]))
        for component_name in RUN_UI_COMPONENT_NAMES:
            await terminal.set_component_active(f"{component_name}_runuiworkflow")
        await terminal.render(horizontal_padding=4, vertical_padding=1)
        try:
            for second in range(1, CONSECUTIVE_FRAMES + 2):
                await actions.update_workflow_executions_counter("runuiworkflow", second * 100)
                await actions.update_workflow_executions_rates(
                    "runuiworkflow", [(float(index), index * 10) for index in range(1, second + 1)]
                )
                await asyncio.sleep(0.1)
            frames = await consecutive_frames(received, CONSECUTIVE_FRAMES)
        finally:
            await terminal.stop()
            await terminal.close()

    # `run workflow`'s sections take shares of the canvas that need not fill it.
    assert_frames_fit_in_place(frames, columns, lines, fills_rows=False)
