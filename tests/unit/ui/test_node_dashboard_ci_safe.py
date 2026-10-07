"""
A node configured for the full dashboard detects at runtime whether its
stdout can show it, and where it cannot writes the CI-safe form instead:
append-only plain ASCII summary lines.

- Each condition that rules the full dashboard out selects "ci-safe" (a
  closed stdout, "disabled"), with the reason; a capable terminal keeps
  "full"; --quiet and an explicitly configured mode win.
- Summary lines are ASCII, carry the panels and readings, are written when
  the summary changes (at most once per change interval) and repeated
  after the heartbeat interval; a reader that goes away (EPIPE) is
  reported once and nothing more is written.
- A "ci-safe" dashboard on a real manager writes such lines to stdout and
  nothing else, and leaves no task behind.
"""

import asyncio
import io
import os
import pathlib
import pty
import re

from hyperscale.distributed.nodes import ManagerServer
from hyperscale.logging import Logger
from hyperscale.ui.node_dashboard import ManagerDashboardReader, NodeDashboard, NodeDashboardConfig
from hyperscale.ui.node_dashboard.models import NodeDashboardFrame
from hyperscale.ui.node_dashboard.node_dashboard_summary_lines import NodeDashboardSummaryLines
from hyperscale.ui.ci_safe.terminal_capability import CI_ENVIRONMENT_VARIABLES, environment_fallback
from hyperscale.ui.node_dashboard.terminal_capability import layout_fallback, select_node_terminal_mode
from tests.integration.cli.node_processes import reserve_port_blocks
from tests.integration.ui.node_dashboard_harness import (
    dashboard_env,
    dashboard_tasks,
    roomy_terminal,
    terminal_pipe,
)

__all__ = ["roomy_terminal"]

ESCAPE = "\x1b"
CAPABLE_ENVIRONMENT = {"TERM": "xterm-256color"}
CHANGE_INTERVAL_SECONDS = 3.0
HEARTBEAT_INTERVAL_SECONDS = 15.0
LAYOUT = ManagerDashboardReader.layout


def summary_frame(sampled_at: float, workers: int) -> NodeDashboardFrame:
    return NodeDashboardFrame(
        identity_lines=["MANAGER DC-01-9000", "tcp 127.0.0.1:9000", "udp 127.0.0.1:9001"],
        lifecycle_state="active",
        uptime_seconds=sampled_at,
        cluster_lines=["CLUSTER standalone"],
        summary_lines=[f"WORKERS {workers} unhealthy 0"],
        detail_lines=["JOBS 0 leading 0/0"],
        table_rows=[],
        chart_values=[],
        value_lines=["cores in use % 75.0"],
        sampled_at=sampled_at,
    )


def summary_lines(output: io.BytesIO) -> list[str]:
    return output.getvalue().decode("ascii").splitlines()


def test_each_unfit_environment_selects_ci_safe_and_a_capable_terminal_keeps_full() -> None:
    controller_descriptor, terminal_descriptor = pty.openpty()
    read_descriptor, write_descriptor = os.pipe()
    with (
        open(terminal_descriptor, "w", closefd=True) as terminal,
        open(write_descriptor, "w", closefd=True) as pipe,
    ):
        closed = open(os.devnull, "w")
        closed.close()
        assert environment_fallback(None, CAPABLE_ENVIRONMENT)[0] == "disabled"
        assert environment_fallback(closed, CAPABLE_ENVIRONMENT)[0] == "disabled"
        assert environment_fallback(pipe, CAPABLE_ENVIRONMENT) == ("ci-safe", "stdout is not a terminal")
        assert environment_fallback(terminal, {})[0] == "ci-safe"
        assert environment_fallback(terminal, {"TERM": "dumb"})[0] == "ci-safe"
        for variable in CI_ENVIRONMENT_VARIABLES:
            assert environment_fallback(terminal, {**CAPABLE_ENVIRONMENT, variable: "true"})[0] == "ci-safe"
        assert environment_fallback(terminal, CAPABLE_ENVIRONMENT) is None

    os.close(controller_descriptor)
    os.close(read_descriptor)


async def test_a_terminal_too_small_or_unable_to_encode_the_glyphs_selects_ci_safe() -> None:
    assert await layout_fallback(LAYOUT, 120, 38, "utf-8") is None
    assert await layout_fallback(LAYOUT, 160, 48, "utf-8") is None
    for columns, lines in ((40, 10), (0, 0), (120, 14), (100, 38)):
        assert (await layout_fallback(LAYOUT, columns, lines, "utf-8"))[0] == "ci-safe", (columns, lines)
    assert (await layout_fallback(LAYOUT, 120, 38, "ascii"))[0] == "ci-safe"


async def test_quiet_and_an_explicit_mode_win_and_full_falls_back_off_a_terminal() -> None:
    # pytest's stdout is not a terminal.
    assert (await select_node_terminal_mode("full", True, LAYOUT)).mode == "disabled"
    for configured_mode in ("ci", "ci-safe", "disabled"):
        assert await select_node_terminal_mode(configured_mode, False, LAYOUT) == (
            await select_node_terminal_mode(configured_mode, False, LAYOUT)
        )
        assert (await select_node_terminal_mode(configured_mode, False, LAYOUT)).mode == configured_mode
    selection = await select_node_terminal_mode("full", False, LAYOUT)
    assert selection.mode == "ci-safe"
    assert "stdout is not a terminal" in selection.degraded_reason


async def test_summary_lines_are_ascii_and_written_on_change_and_heartbeat() -> None:
    output = io.BytesIO()
    lines = NodeDashboardSummaryLines(output, CHANGE_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS)
    readings = ["dispatches /s 0.0", "dispatch p95 ms -", "café ● 1.0"]

    assert await lines.write(summary_frame(0.0, 0), readings) is None
    # Unchanged: nothing until the heartbeat.
    await lines.write(summary_frame(1.0, 0), readings)
    await lines.write(summary_frame(HEARTBEAT_INTERVAL_SECONDS - 1.0, 0), readings)
    assert len(summary_lines(output)) == 1
    await lines.write(summary_frame(HEARTBEAT_INTERVAL_SECONDS, 0), readings)
    assert len(summary_lines(output)) == 2
    # Changed: written once the change interval has passed since the last.
    await lines.write(summary_frame(HEARTBEAT_INTERVAL_SECONDS + 1.0, 1), readings)
    assert len(summary_lines(output)) == 2
    await lines.write(summary_frame(HEARTBEAT_INTERVAL_SECONDS + CHANGE_INTERVAL_SECONDS, 1), readings)

    written = summary_lines(output)
    assert len(written) == 3
    assert written[0].startswith("up 0h00m00s | MANAGER DC-01-9000; tcp 127.0.0.1:9000; udp 127.0.0.1:9001; active")
    assert "WORKERS 0 unhealthy 0" in written[1] and "WORKERS 1 unhealthy 0" in written[2]
    assert "dispatches /s 0.0; dispatch p95 ms -; caf? ? 1.0" in written[2]
    assert all(line.isascii() and ESCAPE not in line for line in written)


async def test_a_reader_that_goes_away_is_reported_once_and_nothing_more_is_written() -> None:
    read_descriptor, write_descriptor = os.pipe()
    os.close(read_descriptor)
    with open(write_descriptor, "wb", closefd=True) as output:
        lines = NodeDashboardSummaryLines(output, CHANGE_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS)
        first_error = await lines.write(summary_frame(0.0, 0), [])
        assert isinstance(first_error, BrokenPipeError)
        assert await lines.write(summary_frame(HEARTBEAT_INTERVAL_SECONDS, 1), []) is None
        # The output now points at the null device: its own flush cannot
        # fail as well.
        output.write(b"after the reader left\n")
        output.flush()


async def test_a_ci_safe_dashboard_writes_summary_lines_and_nothing_else(tmp_path: pathlib.Path) -> None:
    (manager_port,) = reserve_port_blocks([2])
    env = dashboard_env(tmp_path)
    manager = ManagerServer(host="127.0.0.1", tcp_port=manager_port, udp_port=manager_port + 1, env=env, dc_id="DC-DASH")
    async with terminal_pipe() as collected:
        dashboard = NodeDashboard(
            ManagerDashboardReader(manager),
            manager,
            "ci-safe",
            env,
            tmp_path / "manager.log",
            NodeDashboardConfig(),
            Logger(),
            degraded_reason="stdout is not a terminal",
        )
        await dashboard.start()
        try:
            for _ in range(50):
                if b"\n" in collected:
                    break
                await asyncio.sleep(0.1)
        finally:
            await dashboard.stop()

    output = collected.decode("ascii")
    assert ESCAPE not in output
    assert re.match(rf"up 0h00m\d\ds \| MANAGER {manager.node_id.short}; tcp 127\.0\.0\.1:{manager_port}; ", output)
    assert "WORKERS 0 unhealthy 0" in output and "dispatch p95 ms -" in output
    assert dashboard_tasks() == []
