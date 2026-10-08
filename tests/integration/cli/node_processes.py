"""
Real `hyperscale run <role>` processes for CLI end-to-end tests: port
reservation below the OS ephemeral range, a node wrapper capturing its
output, boot/stop helpers bounded by the nodes' own --boot-timeout and
--shutdown-timeout, leak checks scoped to one test run by an
environment marker every launched process inherits, and a temporary
--data-directory per manager and gate, removed at teardown.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psutil

from hyperscale.commands.join import default_join_timeout_seconds
from hyperscale.distributed.env import Env

HYPERSCALE = os.path.join(os.getcwd(), ".venv", "bin", "hyperscale")
LOCALHOST = "127.0.0.1"

# Node boot bound: passed to every node as --boot-timeout, so a node that
# has not reported "started" by then has, by its own contract, failed.
BOOT_TIMEOUT_SECONDS = 90.0
# Node shutdown bound: passed as --shutdown-timeout; any process of ours
# alive past it has outlived the CLI's own shutdown contract — a leak.
SHUTDOWN_TIMEOUT_SECONDS = 60.0
RUN_MARKER_ENVAR = "HYPERSCALE_TEST_RUN_MARKER"
# There is no default cluster secret: every node, CLI command and in-test
# client of a CLI test shares this one, exported to each process as
# MERCURY_SYNC_AUTH_SECRET (never the operator's per-user cluster cookie).
AUTH_SECRET_ENVAR = "MERCURY_SYNC_AUTH_SECRET"
CLI_TEST_AUTH_SECRET = "hyperscale-cli-test-cluster-secret-0123456789"


def command_environment(**extra_variables: str) -> dict[str, str]:
    """The environment a CLI test launches a ``hyperscale`` process with:
    this process's, the shared test secret, then ``extra_variables``."""
    return {**os.environ, AUTH_SECRET_ENVAR: CLI_TEST_AUTH_SECRET, **extra_variables}


IANA_EPHEMERAL_PORT_RANGE = (49152, 65535)
LOWEST_UNPRIVILEGED_PORT = 1024


def ephemeral_port_range() -> tuple[int, int]:
    """The OS's ephemeral (auto-assigned) port range, read at runtime.

    Reserved test ports must sit OUTSIDE this range: a port released by the
    test and then bound by a node can otherwise be handed to any process's
    outgoing socket in between (the race that failed this file's first run).
    """
    linux_range = Path("/proc/sys/net/ipv4/ip_local_port_range")
    if linux_range.exists():
        first, last = linux_range.read_text().split()
        return int(first), int(last)

    if sys.platform == "darwin":
        bounds = [
            subprocess.run(
                ["sysctl", "-n", f"net.inet.ip.portrange.{bound}"],
                capture_output=True, text=True, check=True,
            ).stdout.strip()
            for bound in ("first", "last")
        ]
        return int(bounds[0]), int(bounds[1])

    return IANA_EPHEMERAL_PORT_RANGE


def worker_port_span(cores: int) -> int:
    """Ports a worker binds above its UDP port.

    WorkerLifecycleManager binds its local pool leader at udp + cores**2 and
    executors from udp + 2*cores**2 up to udp + 3*cores**2 - cores.
    """
    return 3 * cores**2


def _port_is_free(port: int) -> bool:
    for socket_type in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
        probe = socket.socket(socket.AF_INET, socket_type)
        try:
            probe.bind((LOCALHOST, port))
        except OSError:
            return False
        finally:
            probe.close()
    return True


def reserve_port_blocks(block_sizes: list[int]) -> list[int]:
    """Return the first port of each contiguous free block, below the
    ephemeral range. The scan starts at a pid-derived offset so concurrent
    runs on one host spread out instead of contending for the same ports."""
    ephemeral_first, _ = ephemeral_port_range()
    search_low, search_high = LOWEST_UNPRIVILEGED_PORT, ephemeral_first
    candidate = search_low + (os.getpid() * 97) % (search_high - search_low)
    block_starts: list[int] = []

    for block_size in block_sizes:
        for _ in range(search_high - search_low):
            if candidate + block_size >= search_high:
                candidate = search_low
            if all(_port_is_free(port) for port in range(candidate, candidate + block_size)):
                block_starts.append(candidate)
                candidate += block_size
                break
            candidate += 1
        else:
            raise RuntimeError(f"no free block of {block_size} ports below {ephemeral_first}")

    return block_starts


DURABLE_ROLES = frozenset({"manager", "gate"})


class CommandNode:
    """One `hyperscale run <role>` process with captured output."""

    def __init__(
        self,
        role: str,
        tcp_port: int,
        udp_port: int,
        marker: str,
        *extra: str,
        environment: dict[str, str] | None = None,
        quiet: bool = True,
    ) -> None:
        self.role = role
        # Per-node settings exported into the process (``hyperscale run``
        # reads its Env from the environment).
        self._environment = environment or {}
        self.tcp_port = tcp_port
        self.address = f"{LOCALHOST}:{tcp_port}"
        self._marker = marker
        # The boot marker is an INFO log: without an explicit level the
        # node runs at the CLI default ("fatal") and never prints it.
        log_level = () if "--log-level" in extra else ("--log-level", "info")
        # Durable roles keep their state in a directory of their own that
        # the test removes at teardown, never in the repository.
        self.data_directory: str | None = (
            tempfile.mkdtemp(prefix=f"hyperscale-{role}-")
            if role in DURABLE_ROLES and "--data-directory" not in extra
            else None
        )
        data_directory = () if self.data_directory is None else ("--data-directory", self.data_directory)
        # The tests read a node's logs from its output: --quiet keeps them on
        # stderr (a node whose stdout is a pipe otherwise writes its
        # dashboard's summary lines there, and its logs to its log file).
        quiet_flag = ("--quiet",) if quiet else ()
        self._arguments = [
            role, "--tcp-port", str(tcp_port), "--udp-port", str(udp_port),
            "--boot-timeout", f"{int(BOOT_TIMEOUT_SECONDS)}s",
            "--shutdown-timeout", f"{int(SHUTDOWN_TIMEOUT_SECONDS)}s",
            *log_level, *data_directory, *quiet_flag, *extra,
        ]
        self.lines: list[str] = []
        self.process: asyncio.subprocess.Process | None = None
        self._pump: asyncio.Task | None = None

    async def start(self) -> None:
        self.process = await asyncio.create_subprocess_exec(
            HYPERSCALE, "run", *self._arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
            env=command_environment(**self._environment, **{RUN_MARKER_ENVAR: self._marker}),
        )
        self._pump = asyncio.create_task(self._read_output())

    async def _read_output(self) -> None:
        while line := await self.process.stdout.readline():
            self.lines.append(line.decode(errors="replace"))

    async def wait_for_output(self, *fragments: str, within: float) -> bool:
        deadline = time.monotonic() + within
        while time.monotonic() < deadline and self.process.returncode is None:
            if any(all(fragment in line for fragment in fragments) for line in self.lines):
                return True
            await asyncio.sleep(0.1)
        return any(all(fragment in line for fragment in fragments) for line in self.lines)

    @property
    def marker(self) -> str:
        return self._marker

    def descendants(self) -> list[int]:
        return [child.pid for child in psutil.Process(self.process.pid).children(recursive=True)]

    async def stop(self, stop_signal: signal.Signals, whole_group: bool) -> tuple[bool, list[int]]:
        """Signal the node; return (exited within bound, surviving descendants)."""
        descendants = self.descendants()
        if whole_group:
            os.killpg(self.process.pid, stop_signal)
        else:
            self.process.send_signal(stop_signal)

        try:
            await asyncio.wait_for(self.process.wait(), timeout=BOOT_TIMEOUT_SECONDS)
            exited = True
        except asyncio.TimeoutError:
            exited = False
            os.killpg(self.process.pid, signal.SIGKILL)
            await self.process.wait()

        await asyncio.wait_for(self._pump, timeout=BOOT_TIMEOUT_SECONDS)
        return exited, [pid for pid in descendants if _alive(pid)]


def _alive(process_id: int) -> bool:
    try:
        return psutil.Process(process_id).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def marked_survivors(marker: str) -> list[int]:
    """Live processes launched by THIS test (nodes and every descendant,
    including executors a pool respawned after any snapshot), found by the
    environment marker they inherit — never other workloads on the host."""
    survivors: list[int] = []
    for process in psutil.process_iter(["pid"]):
        try:
            if process.environ().get(RUN_MARKER_ENVAR) != marker:
                continue
            if process.status() != psutil.STATUS_ZOMBIE:
                survivors.append(process.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return survivors


async def wait_for_no_survivors(marker: str) -> list[int]:
    """Survivors left once the nodes' own shutdown contract has elapsed."""
    deadline = time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS
    while (survivors := marked_survivors(marker)) and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    return survivors


BOOT_MARKERS = {"worker": "Worker started", "manager": "Manager started", "gate": "Gate started"}


async def boot(*nodes: CommandNode) -> None:
    for node in nodes:
        await node.start()
    for node in nodes:
        assert await node.wait_for_output(BOOT_MARKERS[node.role], within=BOOT_TIMEOUT_SECONDS), (
            f"{node.role} at {node.address} did not boot standalone:\n{''.join(node.lines[-30:])}"
        )


async def stop_all(nodes: list[CommandNode], stop_signal: signal.Signals, whole_group: bool) -> None:
    for node in nodes:
        exited, survivors = await node.stop(stop_signal, whole_group)
        assert exited, f"{node.role} did not exit after {stop_signal.name}"
        assert survivors == [], f"{node.role} left descendants alive: {survivors}"
    marker = nodes[0].marker if nodes else ""
    leaked = await wait_for_no_survivors(marker)
    assert leaked == [], f"processes outlived the nodes' shutdown contract: {leaked}"


async def kill_remaining(nodes: list[CommandNode]) -> None:
    """Failure-path cleanup: SIGKILL any node (and its process group) still
    alive, so a failed boot or assertion never leaks processes into later
    tests. A no-op after a successful stop_all."""
    for node in nodes:
        if node.process is None or node.process.returncode is not None:
            continue
        try:
            os.killpg(node.process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await node.process.wait()

    for pid in marked_survivors(nodes[0].marker) if nodes else []:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    for node in nodes:
        if node.data_directory is not None:
            shutil.rmtree(node.data_directory)


NODE_BLOCK = 2  # tcp, udp


def worker_block(cores: int) -> int:
    return NODE_BLOCK + worker_port_span(cores)


def node_at(
    role: str,
    block_start: int,
    marker: str,
    *extra: str,
    environment: dict[str, str] | None = None,
    quiet: bool = True,
) -> CommandNode:
    return CommandNode(role, block_start, block_start + 1, marker, *extra, environment=environment, quiet=quiet)


async def run_join(node: str, target: str, client_port: int) -> tuple[int, str]:
    """Run `hyperscale join --node NODE --target TARGET`; (returncode, output).

    Bounded by twice the CLI's own join budget (one attempt plus its reply).
    """
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE, "join", "--node", node, "--target", target, "--port", str(client_port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=command_environment(),
    )
    output, _ = await asyncio.wait_for(
        process.communicate(), timeout=default_join_timeout_seconds(Env()) * 2
    )
    return process.returncode, output.decode(errors="replace")


async def run_remove(node: str, member: str, client_port: int) -> tuple[int, str]:
    """Run `hyperscale remove --node NODE --member MEMBER`; (returncode,
    output). Bounded by twice the CLI's own budget: the slower tier's
    standard TCP timeout plus a formation interval, per node it asks."""
    environment = Env()
    remove_budget = (
        max(environment.MANAGER_TCP_TIMEOUT_STANDARD, environment.GATE_TCP_TIMEOUT_STANDARD)
        + environment.CLUSTER_FORMATION_INTERVAL_SECONDS
    )
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE, "remove", "--node", node, "--member", member, "--port", str(client_port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=command_environment(),
    )
    output, _ = await asyncio.wait_for(process.communicate(), timeout=remove_budget * 2 * 2)
    return process.returncode, output.decode(errors="replace")


async def run_membership_mode(node: str, mode: str, client_port: int) -> tuple[int, str]:
    """Run `hyperscale membership --node NODE --mode MODE`; (returncode,
    output). Bounded like `run_remove`: one leader hop plus a commit, twice
    over."""
    environment = Env()
    mode_budget = (
        max(environment.MANAGER_TCP_TIMEOUT_STANDARD, environment.GATE_TCP_TIMEOUT_STANDARD)
        + environment.CLUSTER_FORMATION_INTERVAL_SECONDS
    )
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE, "membership", "--node", node, "--mode", mode, "--port", str(client_port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=command_environment(),
    )
    output, _ = await asyncio.wait_for(process.communicate(), timeout=mode_budget * 2 * 2)
    return process.returncode, output.decode(errors="replace")


async def run_resize(node: str, change: str, member: str, client_port: int) -> tuple[int, str]:
    """Run `hyperscale resize --node NODE --add|--remove MEMBER`;
    (returncode, output). Bounded by twice the CLI's own budget: a hop and
    the leader's greeting of every voter (one standard request each), plus
    a formation interval for the commit."""
    environment = Env()
    standard_timeout = max(environment.MANAGER_TCP_TIMEOUT_STANDARD, environment.GATE_TCP_TIMEOUT_STANDARD)
    resize_budget = 2 * standard_timeout + environment.CLUSTER_FORMATION_INTERVAL_SECONDS
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE, "resize", "--node", node, f"--{change}", member, "--port", str(client_port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=command_environment(),
    )
    output, _ = await asyncio.wait_for(process.communicate(), timeout=resize_budget * 2)
    return process.returncode, output.decode(errors="replace")


async def run_cluster_status(node: str, client_port: int) -> tuple[int, str]:
    """Run `hyperscale cluster --node NODE`; (returncode, output). Bounded
    by twice the CLI's own budget: a hop plus the leader's confirmation."""
    environment = Env()
    status_budget = (
        max(environment.MANAGER_TCP_TIMEOUT_STANDARD, environment.GATE_TCP_TIMEOUT_STANDARD)
        + environment.CLUSTER_FORMATION_INTERVAL_SECONDS
    )
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE, "cluster", "--node", node, "--port", str(client_port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=command_environment(),
    )
    output, _ = await asyncio.wait_for(process.communicate(), timeout=status_budget * 2)
    return process.returncode, output.decode(errors="replace")


async def run_cluster_metrics(node: str, client_port: int) -> tuple[int, str]:
    """Run `hyperscale cluster --node NODE --metrics`; (returncode, output),
    bounded like `run_cluster_status`."""
    environment = Env()
    metrics_budget = (
        max(environment.MANAGER_TCP_TIMEOUT_STANDARD, environment.GATE_TCP_TIMEOUT_STANDARD)
        + environment.CLUSTER_FORMATION_INTERVAL_SECONDS
    )
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE, "cluster", "--node", node, "--metrics", "--port", str(client_port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=command_environment(),
    )
    output, _ = await asyncio.wait_for(process.communicate(), timeout=metrics_budget * 2)
    return process.returncode, output.decode(errors="replace")


async def run_job_command(
    subcommand: str,
    job_id: str,
    managers: list[str],
    client_port: int,
    tier_flag: str = "--managers",
    extra_arguments: tuple[str, ...] = (),
) -> tuple[int, str]:
    """Run `hyperscale job SUBCOMMAND --job-id JOB TIER_FLAG NODES... EXTRA...`
    (``managers`` are the nodes asked: gates with ``tier_flag="--gates"``);
    (returncode, output). Bounded by twice the slowest tier's standard TCP
    timeout per node asked -- a forwarded read is one more request."""
    environment = Env()
    budget = 2 * len(managers) * max(environment.MANAGER_TCP_TIMEOUT_STANDARD, environment.GATE_TCP_TIMEOUT_STANDARD)
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE, "job", subcommand, "--job-id", job_id, tier_flag, *managers, *extra_arguments,
        "--port", str(client_port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=command_environment(),
    )
    output, _ = await asyncio.wait_for(process.communicate(), timeout=budget)
    return process.returncode, output.decode(errors="replace")
