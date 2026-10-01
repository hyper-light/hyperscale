"""
E2E: `hyperscale run worker|manager|gate` + `hyperscale join`, real processes.

Every node is a real `hyperscale run <role>` OS process (own session, real
sockets, real executor pool); every join is a real `hyperscale join`
process. Asserts:

1. All three roles boot standalone (no seeds / peers).
2. Cross-tier joins take effect on the TARGET, observed in its own output:
   worker->manager registration, manager->gate registration, and a
   gate->manager join after which the manager's heartbeats reach the gate.
3. Invalid joins are refused fast and readably: same-role peers, wrong
   role, self, unreachable node/target, malformed address.
4. Ctrl-C (SIGINT to the whole process group, like a terminal) and
   SIGTERM (to the parent only, like a supervisor) stop every node with
   zero surviving descendants and no orphaned executor processes.

Ceilings are derived from the nodes' own configuration, not chosen:
boot is bounded by the --boot-timeout each node is launched with; a
join reply by the CLI's configured join budget; the post-join heartbeat
by the manager's gate heartbeat interval plus that send's timeout.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import os
import signal
import socket
import time

import psutil
import pytest

from hyperscale.commands.join import default_join_timeout_seconds
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.manager.config import create_manager_config_from_env

HYPERSCALE = os.path.join(os.getcwd(), ".venv", "bin", "hyperscale")
LOCALHOST = "127.0.0.1"

# Node boot bound: passed to every node as --boot-timeout, so a node that
# has not reported "started" by then has, by its own contract, failed.
BOOT_TIMEOUT_SECONDS = 90.0

ENV = Env()
JOIN_REPLY_BOUND_SECONDS = default_join_timeout_seconds(ENV)
_MANAGER_CONFIG = create_manager_config_from_env(LOCALHOST, 1, 2, ENV)
# The gate heartbeat loop sleeps one interval then sends with a fixed
# 2.0s timeout (manager/server.py _gate_heartbeat_loop), so the first
# heartbeat after a join lands within one interval plus that send.
GATE_HEARTBEAT_SEND_TIMEOUT_SECONDS = 2.0
HEARTBEAT_BOUND_SECONDS = (
    _MANAGER_CONFIG.gate_heartbeat_interval_seconds
    + GATE_HEARTBEAT_SEND_TIMEOUT_SECONDS
)


def reserve_ports(count: int) -> list[int]:
    """Ask the OS for ``count`` distinct free ports (TCP and UDP both free).

    Holding every probe socket until all are chosen keeps the set
    distinct; the window between release and the node binding is the
    standard test-harness TOCTOU and is reported loudly by node boot
    failure rather than masked.
    """
    held: list[socket.socket] = []
    ports: list[int] = []
    while len(ports) < count:
        tcp_probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_probe.bind((LOCALHOST, 0))
        port = tcp_probe.getsockname()[1]
        udp_probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            udp_probe.bind((LOCALHOST, port))
        except OSError:
            tcp_probe.close()
            udp_probe.close()
            continue
        held.extend((tcp_probe, udp_probe))
        ports.append(port)

    for probe in held:
        probe.close()
    return ports


class CommandNode:
    """One `hyperscale run <role>` process with captured output."""

    def __init__(self, role: str, tcp_port: int, udp_port: int, *extra: str) -> None:
        self.role = role
        self.tcp_port = tcp_port
        self.address = f"{LOCALHOST}:{tcp_port}"
        self._arguments = [
            role, "--tcp-port", str(tcp_port), "--udp-port", str(udp_port),
            "--boot-timeout", f"{int(BOOT_TIMEOUT_SECONDS)}s", *extra,
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


def orphaned_executors() -> list[int]:
    return [
        process.pid
        for process in psutil.process_iter(["cmdline"])
        if "multiprocessing-fork" in " ".join(process.info["cmdline"] or [])
    ]


async def run_join(node: str, target: str, client_port: int) -> tuple[int, str]:
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE, "join", "--node", node, "--target", target, "--port", str(client_port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await asyncio.wait_for(process.communicate(), timeout=JOIN_REPLY_BOUND_SECONDS * 2)
    return process.returncode, output.decode(errors="replace")


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
    assert orphaned_executors() == [], "executor processes outlived their worker"


@pytest.fixture
def ports() -> list[int]:
    # 4 nodes x (tcp, udp) + one client port pair per join invocation.
    return reserve_ports(32)


async def test_cross_tier_joins_take_effect_on_targets(ports: list[int]) -> None:
    worker = CommandNode("worker", ports[0], ports[1], "--workers", "2", "--log-level", "info")
    manager_a = CommandNode("manager", ports[2], ports[3], "--log-level", "info")
    manager_b = CommandNode("manager", ports[4], ports[5], "--datacenter", "dc-b", "--log-level", "debug")
    gate = CommandNode("gate", ports[6], ports[7], "--log-level", "info")
    nodes = [worker, manager_a, manager_b, gate]
    client_ports = iter(ports[8::2])

    await boot(*nodes)
    try:
        returncode, output = await run_join(worker.address, manager_a.address, next(client_ports))
        assert returncode == 0, output
        assert await manager_a.wait_for_output("registered with", within=JOIN_REPLY_BOUND_SECONDS)

        returncode, output = await run_join(manager_a.address, gate.address, next(client_ports))
        assert returncode == 0, output
        assert await gate.wait_for_output("Manager registered", within=JOIN_REPLY_BOUND_SECONDS)

        returncode, output = await run_join(gate.address, manager_b.address, next(client_ports))
        assert returncode == 0, output
        assert await manager_b.wait_for_output("Gate", "registered", within=JOIN_REPLY_BOUND_SECONDS)
        assert await manager_b.wait_for_output(
            "Sent heartbeat to 1/1 gates", within=HEARTBEAT_BOUND_SECONDS
        ), "manager accepted the gate but never heartbeated to it"

    finally:
        await stop_all(nodes, signal.SIGINT, whole_group=True)


@pytest.mark.parametrize(
    "case",
    [
        "manager_to_manager",
        "worker_to_gate",
        "worker_to_self",
        "unreachable_target",
        "unreachable_node",
        "malformed_address",
    ],
)
async def test_invalid_joins_are_refused_fast(ports: list[int], case: str) -> None:
    worker = CommandNode("worker", ports[0], ports[1], "--workers", "1")
    manager_a = CommandNode("manager", ports[2], ports[3])
    manager_b = CommandNode("manager", ports[4], ports[5])
    gate = CommandNode("gate", ports[6], ports[7])
    nodes = [worker, manager_a, manager_b, gate]
    closed_port = ports[8]  # reserved and released: nothing listens there

    joins = {
        "manager_to_manager": (manager_a.address, manager_b.address, "managers can only join gates"),
        "worker_to_gate": (worker.address, gate.address, "did not accept worker registration"),
        "worker_to_self": (worker.address, worker.address, "cannot join itself"),
        "unreachable_target": (manager_a.address, f"{LOCALHOST}:{closed_port}", "did not answer"),
        "unreachable_node": (f"{LOCALHOST}:{closed_port}", manager_a.address, "unreachable"),
        "malformed_address": (LOCALHOST, manager_a.address, "needs a port"),
    }
    node_address, target_address, expected_reason = joins[case]

    await boot(*nodes)
    try:
        started = time.monotonic()
        returncode, output = await run_join(node_address, target_address, ports[10])
        elapsed = time.monotonic() - started

        assert returncode == 1, output
        assert expected_reason in output, output
        assert "Traceback" not in output, output
        # A refusal must come from an answer, not from waiting out the
        # join budget (wrong-role targets used to hang until timeout).
        assert elapsed < JOIN_REPLY_BOUND_SECONDS, f"refusal took {elapsed:.1f}s"

    finally:
        await stop_all(nodes, signal.SIGINT, whole_group=True)


@pytest.mark.parametrize(
    ("stop_signal", "whole_group"),
    [(signal.SIGINT, True), (signal.SIGTERM, False)],
    ids=["terminal-ctrl-c", "supervisor-sigterm"],
)
async def test_nodes_stop_without_leaks(
    ports: list[int],
    stop_signal: signal.Signals,
    whole_group: bool,
) -> None:
    worker = CommandNode("worker", ports[0], ports[1], "--workers", "2")
    manager = CommandNode("manager", ports[2], ports[3])
    gate = CommandNode("gate", ports[4], ports[5])
    nodes = [worker, manager, gate]

    await boot(*nodes)
    returncode, output = await run_join(worker.address, manager.address, ports[6])
    assert returncode == 0, output

    await stop_all(nodes, stop_signal, whole_group)
