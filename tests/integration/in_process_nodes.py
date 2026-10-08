"""
Shared pieces for integration tests that run manager, gate and worker
servers in-process on loopback: the cluster's secret, the Env every node
gets (its logs and WALs under the test's own directory), port
reservation, and a bounded wait for a condition the nodes reach on their
own.
"""

import asyncio
import pathlib
import time
from collections.abc import Callable, Iterable

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes import GateServer, ManagerServer, WorkerServer
from tests.integration.cli.node_processes import LOCALHOST, reserve_port_blocks, worker_port_span

# Every node and client of these tests shares one explicit secret: there
# is no default cluster secret.
TEST_AUTH_SECRET = "hyperscale-test-cluster-secret-0123456789"
NODE_PORT_BLOCK = 2  # tcp, udp = tcp + 1
CONDITION_POLL_SECONDS = 0.25

__all__ = [
    "LOCALHOST",
    "NODE_PORT_BLOCK",
    "TEST_AUTH_SECRET",
    "node_env",
    "reserve_cluster_ports",
    "reserve_node_ports",
    "reserve_worker_ports",
    "stop_nodes",
    "wait_until",
]


def node_env(node_directory: pathlib.Path, **overrides: str | int | float | bool) -> Env:
    """The Env of an in-process node. ``MERCURY_SYNC_LOGS_DIRECTORY``'s
    default is the working directory at import time, so every test names
    its own directory: a manager given no WAL data directory keeps its
    idempotency WAL there, and a worker's executors log there.
    ``overrides`` may replace the 2s request timeout, never the secret or
    the directory."""
    settings = {"MERCURY_SYNC_REQUEST_TIMEOUT": "2s"}
    settings.update(overrides)
    settings.update(
        MERCURY_SYNC_AUTH_SECRET=TEST_AUTH_SECRET,
        MERCURY_SYNC_LOGS_DIRECTORY=str(node_directory),
    )
    return Env(**settings)


def reserve_cluster_ports(node_count: int, worker_cores: Iterable[int]) -> tuple[list[int], list[int]]:
    """The TCP ports of ``node_count`` managers or gates and of one worker
    per ``worker_cores`` entry, reserved in ONE scan: separate
    reservations start from the same pid-derived offset and, with nothing
    bound yet, can hand out overlapping blocks."""
    worker_core_counts = list(worker_cores)
    block_starts = reserve_port_blocks(
        [NODE_PORT_BLOCK] * node_count
        + [NODE_PORT_BLOCK + worker_port_span(cores) for cores in worker_core_counts]
    )
    return block_starts[:node_count], block_starts[node_count:]


def reserve_node_ports(node_count: int) -> list[int]:
    """The TCP port of each of ``node_count`` managers or gates (UDP is
    TCP + 1), each a free block below the ephemeral range. Use
    ``reserve_cluster_ports`` when a test also needs worker ports: call
    one reservation per test, never two."""
    return reserve_port_blocks([NODE_PORT_BLOCK] * node_count)


def reserve_worker_ports(worker_cores: Iterable[int]) -> list[int]:
    """The TCP port of each worker (UDP is TCP + 1), with room for the
    local pool leader and executors a worker of that many cores binds
    above its UDP port."""
    return reserve_port_blocks([NODE_PORT_BLOCK + worker_port_span(cores) for cores in worker_cores])


async def wait_until(condition: Callable[[], bool], within_seconds: float, description: str) -> None:
    """Poll ``condition`` until it holds; fail with ``description`` if it
    does not within ``within_seconds``."""
    deadline = time.monotonic() + within_seconds
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(CONDITION_POLL_SECONDS)
    assert condition(), f"{description} did not hold within {within_seconds}s"


async def stop_nodes(nodes: Iterable[ManagerServer | GateServer | WorkerServer], within_seconds: float) -> None:
    """Stop every node, each bounded by ``within_seconds``; every stop runs
    even when one fails, and the first failure is raised after all ran."""
    stop_results = await asyncio.gather(
        *[asyncio.wait_for(node.stop(), timeout=within_seconds) for node in nodes],
        return_exceptions=True,
    )
    if failures := [result for result in stop_results if isinstance(result, BaseException)]:
        raise failures[0]
