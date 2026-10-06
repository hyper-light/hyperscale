"""
E2E: the live dashboards of real `hyperscale run manager|worker` processes.

- "ci" mode (configured) with stdout a pipe: the manager's dashboard shows
  the worker that registers with it, the worker's shows the manager it
  reports to, and each node's logs go to its log file -- never into the
  frames. SIGINT stops both with nothing left running.
- "full" mode on a real terminal (a pty): the dashboard hides the cursor
  while it renders, and Ctrl-C (SIGINT) stops the node and shows the
  cursor again before the process exits.
- "full" mode with stdout a pipe renders nothing: the node logs to stderr
  as it always has (the existing CLI tests rely on it).
"""

import asyncio
import fcntl
import json
import os
import pathlib
import pty
import signal
import struct
import termios
import time

import pytest

from tests.integration.cli.node_processes import (
    BOOT_MARKERS,
    BOOT_TIMEOUT_SECONDS,
    HYPERSCALE,
    LOCALHOST,
    NODE_BLOCK,
    RUN_MARKER_ENVAR,
    SHUTDOWN_TIMEOUT_SECONDS,
    boot,
    command_environment,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    stop_all,
    wait_for_no_survivors,
    worker_block,
)

WORKER_CORES = 1
# Wide enough that no asserted dashboard line is clipped: the layout size
# a node reads when stdout is not a terminal (shutil.get_terminal_size).
TERMINAL_COLUMNS = 200
TERMINAL_LINES = 60
WIDE_TERMINAL = {"COLUMNS": str(TERMINAL_COLUMNS), "LINES": str(TERMINAL_LINES)}
# The `hyperscale run` defaults: workers and managers in "default", gates
# in "global".
NODE_DATACENTER = "default"
GATE_DATACENTER = "global"
HIDE_CURSOR = b"\x1b[?25l"
SHOW_CURSOR = b"\x1b[?25h"


@pytest.fixture
def run_marker() -> str:
    return f"cli-node-dashboard-{time.monotonic_ns()}"


def write_config(directory: pathlib.Path, terminal_mode: str) -> tuple[pathlib.Path, pathlib.Path]:
    """A .hyperscale.json selecting ``terminal_mode``, with a logs
    directory of its own; (config path, logs directory)."""
    logs_directory = directory / "logs"
    logs_directory.mkdir()
    config_path = directory / "hyperscale.json"
    config_path.write_text(json.dumps({"logs_directory": str(logs_directory), "terminal_mode": terminal_mode}))
    return config_path, logs_directory


def node_log(
    logs_directory: pathlib.Path, role: str, tcp_port: int, datacenter: str = NODE_DATACENTER
) -> pathlib.Path:
    """The log file a node's stderr goes to while its dashboard renders."""
    return logs_directory / f"{role}-{datacenter}-{LOCALHOST}-{tcp_port}.log"


async def wait_for_log(log_path: pathlib.Path, fragment: str) -> bool:
    """Whether ``fragment`` reaches the node's log file within the boot
    bound (the dashboard renders before the node has finished booting)."""
    deadline = time.monotonic() + BOOT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if log_path.exists() and fragment in log_path.read_text():
            return True
        await asyncio.sleep(0.1)
    return False


async def test_ci_dashboards_show_the_cluster_and_keep_logs_out_of_the_frames(
    run_marker: str, tmp_path: pathlib.Path
) -> None:
    config_path, logs_directory = write_config(tmp_path, "ci")
    worker_start, manager_start = reserve_port_blocks([worker_block(WORKER_CORES), NODE_BLOCK])
    manager = node_at("manager", manager_start, run_marker, "--config", str(config_path), environment=WIDE_TERMINAL)
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--config", str(config_path),
        "--workers", str(WORKER_CORES),
        "--managers", manager.address,
        environment=WIDE_TERMINAL,
    )
    nodes = [manager, worker]
    try:
        await manager.start()
        assert await manager.wait_for_output("CLUSTER standalone", within=BOOT_TIMEOUT_SECONDS), "".join(manager.lines[-30:])
        assert await manager.wait_for_output("WORKERS 0", within=BOOT_TIMEOUT_SECONDS)

        await worker.start()
        assert await manager.wait_for_output("WORKERS 1", within=BOOT_TIMEOUT_SECONDS), (
            f"the manager's dashboard never showed the worker:\n{''.join(manager.lines[-30:])}"
        )
        assert await manager.wait_for_output(f"{LOCALHOST}:{worker_start}", within=BOOT_TIMEOUT_SECONDS)
        assert await worker.wait_for_output(f"primary {manager.address}", within=BOOT_TIMEOUT_SECONDS), (
            f"the worker's dashboard never showed its manager:\n{''.join(worker.lines[-30:])}"
        )

        # The logs went to each node's log file, not into its frames.
        for node in nodes:
            assert not any(BOOT_MARKERS[node.role] in line for line in node.lines), (
                f"a {node.role} log line tore the dashboard's frames"
            )
            assert await wait_for_log(node_log(logs_directory, node.role, node.tcp_port), BOOT_MARKERS[node.role])

        await stop_all(nodes, signal.SIGINT, whole_group=False)
        assert [node.process.returncode for node in nodes] == [0, 0]
    finally:
        await kill_remaining(nodes)


async def read_terminal(master_descriptor: int, collected: bytearray, closed: asyncio.Event) -> None:
    """Collect what the node writes to its terminal until the terminal
    closes (the node exited)."""
    loop = asyncio.get_running_loop()

    def on_readable() -> None:
        try:
            chunk = os.read(master_descriptor, 1 << 16)
        except OSError:
            chunk = b""
        if not chunk:
            loop.remove_reader(master_descriptor)
            closed.set()
        collected.extend(chunk)

    loop.add_reader(master_descriptor, on_readable)


async def wait_for_bytes(collected: bytearray, fragment: bytes, within: float) -> bool:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if fragment in collected:
            return True
        await asyncio.sleep(0.1)
    return fragment in collected


async def test_full_dashboard_on_a_terminal_restores_the_cursor_on_ctrl_c(
    run_marker: str, tmp_path: pathlib.Path
) -> None:
    config_path, logs_directory = write_config(tmp_path, "full")
    (manager_start,) = reserve_port_blocks([NODE_BLOCK])
    data_directory = tmp_path / "data"
    master_descriptor, slave_descriptor = pty.openpty()
    fcntl.ioctl(slave_descriptor, termios.TIOCSWINSZ, struct.pack("HHHH", TERMINAL_LINES, TERMINAL_COLUMNS, 0, 0))
    os.set_blocking(master_descriptor, False)
    collected = bytearray()
    closed = asyncio.Event()
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE, "run", "manager",
        "--tcp-port", str(manager_start), "--udp-port", str(manager_start + 1),
        "--boot-timeout", f"{int(BOOT_TIMEOUT_SECONDS)}s",
        "--shutdown-timeout", f"{int(SHUTDOWN_TIMEOUT_SECONDS)}s",
        "--log-level", "info",
        "--data-directory", str(data_directory),
        "--config", str(config_path),
        stdin=slave_descriptor,
        stdout=slave_descriptor,
        stderr=slave_descriptor,
        start_new_session=True,
        env=command_environment(**{RUN_MARKER_ENVAR: run_marker}),
    )
    os.close(slave_descriptor)
    await read_terminal(master_descriptor, collected, closed)
    try:
        assert await wait_for_bytes(collected, HIDE_CURSOR, within=BOOT_TIMEOUT_SECONDS)
        assert await wait_for_bytes(collected, b"CLUSTER standalone", within=BOOT_TIMEOUT_SECONDS), bytes(collected[-3000:])

        process.send_signal(signal.SIGINT)
        await asyncio.wait_for(process.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
        await asyncio.wait_for(closed.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)

        assert process.returncode == 0
        assert collected.rfind(SHOW_CURSOR) > collected.rfind(HIDE_CURSOR), "the cursor was left hidden"
        assert BOOT_MARKERS["manager"].encode() not in collected, "a log line tore the dashboard's frames"
        assert await wait_for_log(node_log(logs_directory, "manager", manager_start), BOOT_MARKERS["manager"])
        assert await wait_for_no_survivors(run_marker) == []
    finally:
        asyncio.get_running_loop().remove_reader(master_descriptor)
        os.close(master_descriptor)
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()


async def test_full_dashboard_without_a_terminal_renders_nothing(run_marker: str, tmp_path: pathlib.Path) -> None:
    config_path, logs_directory = write_config(tmp_path, "full")
    (manager_start,) = reserve_port_blocks([NODE_BLOCK])
    manager = node_at("manager", manager_start, run_marker, "--config", str(config_path))
    try:
        await boot(manager)
        assert not any("CLUSTER" in line for line in manager.lines), "a dashboard rendered into a pipe"
        assert not node_log(logs_directory, "manager", manager_start).exists()
        await stop_all([manager], signal.SIGINT, whole_group=False)
    finally:
        await kill_remaining([manager])


async def test_the_gate_dashboard_shows_the_datacenter_its_manager_reports(
    run_marker: str, tmp_path: pathlib.Path
) -> None:
    config_path, logs_directory = write_config(tmp_path, "ci")
    gate_start, manager_start = reserve_port_blocks([NODE_BLOCK, NODE_BLOCK])
    gate = node_at("gate", gate_start, run_marker, "--config", str(config_path), environment=WIDE_TERMINAL)
    manager = node_at(
        "manager",
        manager_start,
        run_marker,
        "--config", str(config_path),
        "--gates", gate.address,
        "--gate-udp", f"{LOCALHOST}:{gate_start + 1}",
        environment=WIDE_TERMINAL,
    )
    nodes = [manager, gate]
    try:
        await gate.start()
        assert await gate.wait_for_output("CLUSTER standalone", within=BOOT_TIMEOUT_SECONDS), "".join(gate.lines[-30:])
        assert await gate.wait_for_output("GATE", within=BOOT_TIMEOUT_SECONDS)

        await manager.start()
        assert await gate.wait_for_output("DATACENTERS 1", within=BOOT_TIMEOUT_SECONDS), (
            f"the gate's dashboard never showed the manager's datacenter:\n{''.join(gate.lines[-30:])}"
        )
        assert await gate.wait_for_output("managers alive 1", within=BOOT_TIMEOUT_SECONDS)
        assert await manager.wait_for_output("gates 1 healthy 1", within=BOOT_TIMEOUT_SECONDS), (
            f"the manager's dashboard never showed its gate:\n{''.join(manager.lines[-30:])}"
        )
        assert await wait_for_log(node_log(logs_directory, "gate", gate_start, GATE_DATACENTER), BOOT_MARKERS["gate"])

        await stop_all(nodes, signal.SIGINT, whole_group=False)
    finally:
        await kill_remaining(nodes)
