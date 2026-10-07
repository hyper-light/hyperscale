"""
The live node dashboard's lifecycle against a real manager, in-process:
the dashboard renders "ci" frames into a pipe standing in for the
terminal.

- A reader that fails is logged (to the log file stderr goes to while the
  dashboard renders) and shown on the status line, and sampling goes on.
- Ctrl-C (a real SIGINT through ShutdownSignals) stops the node, stops the
  dashboard and leaves none of its tasks, its subscriptions or its signal
  handlers behind.
- "disabled" renders and redirects nothing; --quiet and a non-terminal
  stdout select the modes `run workflow` would.

The dashboards' live values against a registering worker are covered by
tests/integration/ui/test_node_dashboard_live_cluster.py and, through real
`hyperscale run` processes, tests/integration/cli/test_cli_node_dashboard.py.
"""

import asyncio
import contextvars
import os
import pathlib
import signal
import sys

from hyperscale.commands.run.node_lifecycle import run_node_until_stopped
from hyperscale.commands.run.shared import node_terminal_mode
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes import ManagerServer
from hyperscale.logging import Logger, LoggingConfig
from hyperscale.ui.components.terminal import Terminal
from hyperscale.ui.node_dashboard import (
    ManagerDashboardReader,
    NodeDashboard,
    NodeDashboardConfig,
    StderrLogRedirect,
)
from hyperscale.ui.node_dashboard.models import NodeDashboardFrame, NodeDashboardLayout
from hyperscale.ui.node_dashboard.node_dashboard_actions import IDENTITY_CHANNEL
from tests.integration.cli.node_processes import reserve_port_blocks
from tests.integration.ui.node_dashboard_harness import (
    MANAGER_BOOT_SECONDS,
    SHUTDOWN_SECONDS,
    ci_dashboard,
    dashboard_env,
    dashboard_tasks,
    roomy_terminal,
    terminal_pipe,
    wait_for_frame,
)

__all__ = ["roomy_terminal"]


class FailingReader:
    """A reader whose node state cannot be read."""

    layout: NodeDashboardLayout = ManagerDashboardReader.layout

    def __init__(self) -> None:
        self.reads = 0

    def read(self) -> NodeDashboardFrame:
        self.reads += 1
        raise RuntimeError(f"state unreadable (read {self.reads})")


async def test_a_failing_sample_is_logged_and_shown_and_sampling_goes_on(tmp_path: pathlib.Path) -> None:
    # The log level is raised for this test alone: its own context.
    await asyncio.get_running_loop().create_task(
        failing_sample_is_logged_and_shown(tmp_path), context=contextvars.copy_context()
    )


async def failing_sample_is_logged_and_shown(tmp_path: pathlib.Path) -> None:
    (manager_port,) = reserve_port_blocks([2])
    env = dashboard_env(tmp_path)
    # Constructed, never started: the dashboard needs only its identity.
    manager = ManagerServer(host="127.0.0.1", tcp_port=manager_port, udp_port=manager_port + 1, env=env, dc_id="DC-DASH")
    log_path = tmp_path / "logs" / "manager.log"
    reader = FailingReader()
    LoggingConfig().update(log_level="error", log_output="stderr")

    # The node's log streams duplicate sys.stderr's descriptor, which is
    # descriptor 2 in a node process; pytest's capture points it elsewhere.
    saved_stderr = sys.stderr
    sys.stderr = open(2, "w", closefd=False)
    try:
        async with StderrLogRedirect(log_path, enabled=True):
            await show_failing_samples(reader, manager, env, log_path)
    finally:
        sys.stderr.close()
        sys.stderr = saved_stderr

    logged = await asyncio.get_running_loop().run_in_executor(None, log_path.read_text)
    assert "dashboard sample failed: RuntimeError: state unreadable (read 1)" in logged
    assert "ERROR" in logged


async def show_failing_samples(reader: FailingReader, manager: ManagerServer, env: Env, log_path: pathlib.Path) -> None:
    """Run a dashboard whose every sample fails until it has shown two more
    failures than the first."""
    async with terminal_pipe() as collected:
        dashboard = NodeDashboard(
            reader,
            manager,
            "ci",
            env,
            log_path,
            NodeDashboardConfig(sample_interval_seconds=0.1),
            Logger(),
        )
        await dashboard.start()
        try:
            await wait_for_frame(collected, "dashboard sample failed: RuntimeError: state unreadable")
            reads_seen = reader.reads
            await wait_for_frame(collected, f"(read {reads_seen + 2})")
        finally:
            await dashboard.stop()


async def test_ctrl_c_stops_the_node_and_leaves_nothing_of_the_dashboard(tmp_path: pathlib.Path) -> None:
    (manager_port,) = reserve_port_blocks([2])
    env = dashboard_env(tmp_path)
    manager = ManagerServer(host="127.0.0.1", tcp_port=manager_port, udp_port=manager_port + 1, env=env, dc_id="DC-DASH")
    subscribers_before = len(Terminal._updates.updates.get(IDENTITY_CHANNEL, []))
    dashboard = ci_dashboard(ManagerDashboardReader(manager), manager, env, tmp_path / "manager.log")

    async with terminal_pipe() as collected:
        run = asyncio.get_running_loop().create_task(
            run_node_until_stopped(
                manager,
                lambda: asyncio.wait_for(manager.start(), timeout=MANAGER_BOOT_SECONDS),
                dashboard,
                drain_before_stop=True,
                shutdown_timeout_seconds=SHUTDOWN_SECONDS,
            )
        )
        await wait_for_frame(collected, "CLUSTER standalone", "up 0h00m0", within=MANAGER_BOOT_SECONDS)
        assert len(Terminal._updates.updates[IDENTITY_CHANNEL]) == subscribers_before + 1

        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.wait_for(run, timeout=SHUTDOWN_SECONDS * 2)

    assert manager._running is False, "the node is still running after Ctrl-C"
    assert dashboard_tasks() == [], f"dashboard tasks outlived it: {dashboard_tasks()}"
    assert len(Terminal._updates.updates[IDENTITY_CHANNEL]) == subscribers_before, (
        "the stopped terminal's components still subscribe to the dashboard's actions"
    )
    assert signal.getsignal(signal.SIGWINCH) == signal.SIG_DFL, "the terminal's resize handler outlived it"
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    # The manager's idempotency WAL lives in the node's own directory, never
    # in the working directory the tests run from.
    assert list(tmp_path.glob("manager-idempotency-*.wal")) != []


async def test_node_terminal_modes() -> None:
    # pytest's stdout is not a terminal: "full" renders nothing there, an
    # explicit "ci" still renders, and --quiet disables either.
    assert await node_terminal_mode("full", quiet=False) == "disabled"
    assert await node_terminal_mode("ci", quiet=False) == "ci"
    assert await node_terminal_mode("ci", quiet=True) == "disabled"
    assert await node_terminal_mode("disabled", quiet=False) == "disabled"


async def test_a_disabled_dashboard_renders_nothing_and_redirects_nothing(tmp_path: pathlib.Path) -> None:
    (manager_port,) = reserve_port_blocks([2])
    env = dashboard_env(tmp_path)
    manager = ManagerServer(host="127.0.0.1", tcp_port=manager_port, udp_port=manager_port + 1, env=env, dc_id="DC-DASH")
    log_path = tmp_path / "manager.log"
    dashboard = NodeDashboard(
        ManagerDashboardReader(manager), manager, "disabled", env, log_path, NodeDashboardConfig(), Logger()
    )

    async with StderrLogRedirect(log_path, enabled=False):
        async with terminal_pipe() as collected:
            await dashboard.start()
            await asyncio.sleep(0.2)
            await dashboard.stop()

    assert collected == bytearray()
    assert not log_path.exists()
    assert dashboard_tasks() == []


async def test_stderr_goes_to_the_log_file_only_while_redirected(tmp_path: pathlib.Path) -> None:
    log_path = tmp_path / "nested" / "node.log"
    saved_stderr_target = os.fstat(2)

    async with StderrLogRedirect(log_path, enabled=True):
        os.write(2, b"while the dashboard renders\n")

    assert os.fstat(2).st_ino == saved_stderr_target.st_ino, "stderr was not restored"
    logged = await asyncio.get_running_loop().run_in_executor(None, log_path.read_bytes)
    assert logged == b"while the dashboard renders\n"

