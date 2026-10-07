"""
E2E: the live dashboards of real `hyperscale run manager|worker` processes.

- "ci" mode (configured) with stdout a pipe: the manager's dashboard shows
  the worker that registers with it, the worker's shows the manager it
  reports to, and each node's logs go to its log file -- never into the
  frames. SIGINT stops both with nothing left running.
- "full" mode on a real terminal (a pty): the dashboard hides the cursor
  while it renders, and Ctrl-C (SIGINT) stops the node and shows the
  cursor again before the process exits.
- "full" mode where the full dashboard cannot show -- stdout a pipe, TERM
  dumb, a CI environment, an ASCII-only encoding, a terminal too small --
  writes append-only plain ASCII summary lines instead (no escape
  sequence), which change when a worker registers; the node's logs go to
  its log file, and SIGINT stops it cleanly. (The other CLI tests run
  their nodes with --quiet: no dashboard, logs on stderr.)
- Every role's dashboard opens with the run UI's Hyperscale header and
  plots its role's charts.
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

from hyperscale.ui.node_dashboard import GateDashboardReader, ManagerDashboardReader, WorkerDashboardReader
from hyperscale.ui.node_dashboard.models import NodeDashboardLayout
from hyperscale.ui.ci_safe.terminal_capability import CI_ENVIRONMENT_VARIABLES
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
# A line of the Hyperscale header's art (hyperscale/ui/hyperscale_header.py).
HEADER_ART_LINE = "//__ \\\\// //_// //_// // // //__  //    __// // //_//"
HIDE_CURSOR = b"\x1b[?25l"
SHOW_CURSOR = b"\x1b[?25h"


def chart_titles(layout: NodeDashboardLayout) -> list[str]:
    """How the role's chart names its series in a frame: its legend, each
    series beside its reading."""
    return [chart.title for chart in layout.charts]


def capable_terminal_environment(**extra_variables: str) -> dict[str, str]:
    """The environment of a node on a terminal able to show the full
    dashboard -- a TERM naming one, and none of the CI providers' variables
    (the test itself may run in CI) -- then ``extra_variables``."""
    environment = {
        name: value
        for name, value in command_environment(TERM="xterm-256color").items()
        if name not in CI_ENVIRONMENT_VARIABLES
    }
    environment.update(extra_variables)
    return environment


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
    manager = node_at(
        "manager", manager_start, run_marker, "--config", str(config_path), environment=WIDE_TERMINAL, quiet=False
    )
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--config", str(config_path),
        "--workers", str(WORKER_CORES),
        "--managers", manager.address,
        environment=WIDE_TERMINAL,
        quiet=False,
    )
    nodes = [manager, worker]
    try:
        await manager.start()
        assert await manager.wait_for_output("standalone", within=BOOT_TIMEOUT_SECONDS), "".join(manager.lines[-30:])
        assert await manager.wait_for_output("0 healthy", within=BOOT_TIMEOUT_SECONDS)

        await worker.start()
        assert await manager.wait_for_output("1 healthy", within=BOOT_TIMEOUT_SECONDS), (
            f"the manager's dashboard never showed the worker:\n{''.join(manager.lines[-30:])}"
        )
        assert await manager.wait_for_output(f"{LOCALHOST}:{worker_start}", within=BOOT_TIMEOUT_SECONDS)
        assert await worker.wait_for_output(f"manager {manager.address}", within=BOOT_TIMEOUT_SECONDS), (
            f"the worker's dashboard never showed its manager:\n{''.join(worker.lines[-30:])}"
        )
        for node, layout in ((manager, ManagerDashboardReader.layout), (worker, WorkerDashboardReader.layout)):
            assert await node.wait_for_output(HEADER_ART_LINE, within=BOOT_TIMEOUT_SECONDS), (
                f"the {node.role}'s dashboard has no Hyperscale header:\n{''.join(node.lines[-30:])}"
            )
            for title in chart_titles(layout):
                assert await node.wait_for_output(title, within=BOOT_TIMEOUT_SECONDS), (
                    f"the {node.role}'s dashboard has no {title!r} chart:\n{''.join(node.lines[-30:])}"
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
        env=capable_terminal_environment(**{RUN_MARKER_ENVAR: run_marker}),
    )
    os.close(slave_descriptor)
    await read_terminal(master_descriptor, collected, closed)
    try:
        assert await wait_for_bytes(collected, HIDE_CURSOR, within=BOOT_TIMEOUT_SECONDS)
        assert await wait_for_bytes(collected, b"standalone", within=BOOT_TIMEOUT_SECONDS), bytes(collected[-3000:])
        assert await wait_for_bytes(collected, b"(wf /s)", within=BOOT_TIMEOUT_SECONDS), bytes(collected[-3000:])
        assert await wait_for_bytes(collected, HEADER_ART_LINE.encode(), within=BOOT_TIMEOUT_SECONDS)
        # The dashboard draws before the node finishes booting: interrupt
        # only once the boot line has reached the log file, or a slow
        # (cold) boot is cut short before it is ever written.
        assert await wait_for_log(node_log(logs_directory, "manager", manager_start), BOOT_MARKERS["manager"])

        process.send_signal(signal.SIGINT)
        await asyncio.wait_for(process.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
        await asyncio.wait_for(closed.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)

        assert process.returncode == 0
        assert collected.rfind(SHOW_CURSOR) > collected.rfind(HIDE_CURSOR), "the cursor was left hidden"
        assert BOOT_MARKERS["manager"].encode() not in collected, "a log line tore the dashboard's frames"
        assert await wait_for_no_survivors(run_marker) == []
    finally:
        asyncio.get_running_loop().remove_reader(master_descriptor)
        os.close(master_descriptor)
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()


ESCAPE = b"\x1b"
# Each case: why the full dashboard cannot show, as the node's environment
# and its stdout (None: a pipe; else a pty of columns x lines).
CI_SAFE_CASES = {
    "stdout piped": ({}, None),
    "TERM dumb": ({"TERM": "dumb"}, (TERMINAL_COLUMNS, TERMINAL_LINES)),
    "CI set": ({"CI": "true"}, (TERMINAL_COLUMNS, TERMINAL_LINES)),
    "ASCII encoding": ({"LANG": "C", "LC_ALL": "C", "PYTHONIOENCODING": "ascii"}, (TERMINAL_COLUMNS, TERMINAL_LINES)),
    "terminal 40x10": ({}, (40, 10)),
}


class ManagerOutput:
    """A ``hyperscale run manager`` process with its stdout and stderr a
    pipe or a pty, and what it writes there; ``close`` releases the pipe's
    reader task or the pty."""

    def __init__(self, stdout_terminal: tuple[int, int] | None) -> None:
        self.stdout_terminal = stdout_terminal
        self.collected = bytearray()
        self.closed = asyncio.Event()
        self.process: asyncio.subprocess.Process | None = None
        self._pump: asyncio.Task[None] | None = None
        self._master_descriptor: int | None = None

    async def start(self, environment: dict[str, str], arguments: list[str]) -> None:
        if self.stdout_terminal is None:
            self.process = await asyncio.create_subprocess_exec(
                HYPERSCALE, "run", "manager", *arguments,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                start_new_session=True, env=environment,
            )
            self._pump = asyncio.get_running_loop().create_task(self._pump_pipe())
            return

        columns, lines = self.stdout_terminal
        self._master_descriptor, slave_descriptor = pty.openpty()
        fcntl.ioctl(slave_descriptor, termios.TIOCSWINSZ, struct.pack("HHHH", lines, columns, 0, 0))
        os.set_blocking(self._master_descriptor, False)
        self.process = await asyncio.create_subprocess_exec(
            HYPERSCALE, "run", "manager", *arguments,
            stdin=slave_descriptor, stdout=slave_descriptor, stderr=slave_descriptor,
            start_new_session=True, env=environment,
        )
        os.close(slave_descriptor)
        await read_terminal(self._master_descriptor, self.collected, self.closed)

    async def _pump_pipe(self) -> None:
        while chunk := await self.process.stdout.read(1 << 16):
            self.collected.extend(chunk)
        self.closed.set()

    async def close(self) -> None:
        if self.process is not None and self.process.returncode is None:
            os.killpg(self.process.pid, signal.SIGKILL)
            await self.process.wait()

        if self._pump is not None:
            await self._pump

        if self._master_descriptor is not None:
            asyncio.get_running_loop().remove_reader(self._master_descriptor)
            os.close(self._master_descriptor)


def summary_lines(collected: bytearray) -> list[str]:
    return [line for line in collected.decode("ascii", errors="replace").replace("\r", "").split("\n") if line]


@pytest.mark.parametrize("case", list(CI_SAFE_CASES))
async def test_where_the_full_dashboard_cannot_show_a_node_writes_ascii_summary_lines(
    case: str, run_marker: str, tmp_path: pathlib.Path
) -> None:
    extra_environment, stdout_terminal = CI_SAFE_CASES[case]
    config_path, logs_directory = write_config(tmp_path, "full")
    worker_start, manager_start = reserve_port_blocks([worker_block(WORKER_CORES), NODE_BLOCK])
    manager_address = f"{LOCALHOST}:{manager_start}"
    manager = ManagerOutput(stdout_terminal)
    await manager.start(
        capable_terminal_environment(**{RUN_MARKER_ENVAR: run_marker}, **extra_environment),
        [
            "--tcp-port", str(manager_start), "--udp-port", str(manager_start + 1),
            "--boot-timeout", f"{int(BOOT_TIMEOUT_SECONDS)}s",
            "--shutdown-timeout", f"{int(SHUTDOWN_TIMEOUT_SECONDS)}s",
            "--log-level", "info",
            "--data-directory", str(tmp_path / "data"),
            "--config", str(config_path),
        ],
    )
    collected = manager.collected
    worker = node_at("worker", worker_start, run_marker, "--workers", str(WORKER_CORES), "--managers", manager_address)
    try:
        assert await wait_for_bytes(collected, b"WORKERS 0 unhealthy 0", within=BOOT_TIMEOUT_SECONDS), bytes(collected)
        assert await wait_for_log(node_log(logs_directory, "manager", manager_start), BOOT_MARKERS["manager"])
        await worker.start()
        assert await wait_for_bytes(collected, b"WORKERS 1 unhealthy 0", within=BOOT_TIMEOUT_SECONDS), bytes(collected)

        manager.process.send_signal(signal.SIGINT)
        await asyncio.wait_for(manager.process.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
        await asyncio.wait_for(manager.closed.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
        assert manager.process.returncode == 0

        assert ESCAPE not in collected, f"the summary lines carry escape sequences:\n{bytes(collected)!r}"
        assert collected.isascii(), "a summary line is not ASCII"
        lines = summary_lines(collected)
        assert lines and all(line.startswith("up ") and "MANAGER " in line for line in lines), lines
        assert BOOT_MARKERS["manager"] not in collected.decode("ascii"), "a log line was written among the summaries"
        await stop_all([worker], signal.SIGINT, whole_group=False)
    finally:
        await kill_remaining([worker])
        await manager.close()


async def test_the_gate_dashboard_shows_the_datacenter_its_manager_reports(
    run_marker: str, tmp_path: pathlib.Path
) -> None:
    config_path, logs_directory = write_config(tmp_path, "ci")
    gate_start, manager_start = reserve_port_blocks([NODE_BLOCK, NODE_BLOCK])
    gate = node_at("gate", gate_start, run_marker, "--config", str(config_path), environment=WIDE_TERMINAL, quiet=False)
    manager = node_at(
        "manager",
        manager_start,
        run_marker,
        "--config", str(config_path),
        "--gates", gate.address,
        "--gate-udp", f"{LOCALHOST}:{gate_start + 1}",
        environment=WIDE_TERMINAL,
        quiet=False,
    )
    nodes = [manager, gate]
    try:
        await gate.start()
        assert await gate.wait_for_output("+ quorum 1/1", within=BOOT_TIMEOUT_SECONDS), "".join(gate.lines[-30:])
        assert await gate.wait_for_output("GATE", within=BOOT_TIMEOUT_SECONDS)

        await manager.start()
        assert await gate.wait_for_output("DCs accepting 1/1", within=BOOT_TIMEOUT_SECONDS), (
            f"the gate's dashboard never showed the manager's datacenter:\n{''.join(gate.lines[-30:])}"
        )
        assert await gate.wait_for_output(NODE_DATACENTER, within=BOOT_TIMEOUT_SECONDS)
        assert await gate.wait_for_output(HEADER_ART_LINE, within=BOOT_TIMEOUT_SECONDS)
        for title in chart_titles(GateDashboardReader.layout):
            assert await gate.wait_for_output(title, within=BOOT_TIMEOUT_SECONDS), (
                f"the gate's dashboard has no {title!r} chart:\n{''.join(gate.lines[-30:])}"
            )
        assert await manager.wait_for_output("gates 1/1", within=BOOT_TIMEOUT_SECONDS), (
            f"the manager's dashboard never showed its gate:\n{''.join(manager.lines[-30:])}"
        )
        assert await wait_for_log(node_log(logs_directory, "gate", gate_start, GATE_DATACENTER), BOOT_MARKERS["gate"])

        await stop_all(nodes, signal.SIGINT, whole_group=False)
    finally:
        await kill_remaining(nodes)


# --output-mode overrides the config's terminal_mode: on a terminal that
# could show the full dashboard, ci-safe still writes summary lines; piped,
# disabled writes nothing at all.
OUTPUT_MODE_CASES = {
    "ci-safe on a capable terminal": (["--output-mode", "ci-safe"], (TERMINAL_COLUMNS, TERMINAL_LINES)),
    "disabled piped (short flag)": (["-o", "disabled"], None),
}


@pytest.mark.parametrize("case", list(OUTPUT_MODE_CASES))
async def test_the_output_mode_flag_overrides_the_configured_mode(
    case: str, run_marker: str, tmp_path: pathlib.Path
) -> None:
    flag_arguments, stdout_terminal = OUTPUT_MODE_CASES[case]
    config_path, logs_directory = write_config(tmp_path, "full")
    (manager_start,) = reserve_port_blocks([NODE_BLOCK])
    manager = ManagerOutput(stdout_terminal)
    await manager.start(
        capable_terminal_environment(**{RUN_MARKER_ENVAR: run_marker}),
        [
            "--tcp-port", str(manager_start), "--udp-port", str(manager_start + 1),
            "--boot-timeout", f"{int(BOOT_TIMEOUT_SECONDS)}s",
            "--shutdown-timeout", f"{int(SHUTDOWN_TIMEOUT_SECONDS)}s",
            "--log-level", "info",
            "--data-directory", str(tmp_path / "data"),
            "--config", str(config_path),
            *flag_arguments,
        ],
    )
    try:
        if flag_arguments[-1] == "ci-safe":
            # The dashboard writes its lines and the node's logs go to its log file.
            assert await wait_for_log(node_log(logs_directory, "manager", manager_start), BOOT_MARKERS["manager"])
            assert await wait_for_bytes(manager.collected, b"WORKERS 0 unhealthy 0", within=BOOT_TIMEOUT_SECONDS)
        else:
            # No dashboard: the node logs to stderr, which shares the pipe.
            assert await wait_for_bytes(manager.collected, BOOT_MARKERS["manager"].encode(), within=BOOT_TIMEOUT_SECONDS)

        manager.process.send_signal(signal.SIGINT)
        await asyncio.wait_for(manager.process.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
        await asyncio.wait_for(manager.closed.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
        assert manager.process.returncode == 0

        if flag_arguments[-1] == "ci-safe":
            assert ESCAPE not in manager.collected, bytes(manager.collected)
            assert all(line.startswith("up ") for line in summary_lines(manager.collected))
        else:
            assert not any(line.startswith("up ") for line in summary_lines(manager.collected)), bytes(manager.collected)
    finally:
        await manager.close()
