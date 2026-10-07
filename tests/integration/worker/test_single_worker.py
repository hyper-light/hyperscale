"""
A standalone worker (no seed managers) with 8 cores starts, reports all of
its cores free with nothing running, and shuts down cleanly: the sanity
check under every other worker integration test.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.nodes import WorkerServer
from tests.integration.in_process_nodes import LOCALHOST, node_env, reserve_worker_ports

DATACENTER_ID = "DC-TEST"
WORKER_CORES = 8
# A worker's start spawns its executor pool, bounded by the pool's own
# WORKER_POOL_STARTUP_TIMEOUT_SECONDS (60 s by default).
WORKER_START_SECONDS = 120.0
WORKER_STOP_SECONDS = 30.0


@pytest.fixture
async def standalone_worker(node_directory: pathlib.Path) -> AsyncIterator[WorkerServer]:
    """An unstarted standalone worker of ``WORKER_CORES`` cores. A worker
    the test left running (a failed start or assertion) is aborted at
    teardown."""
    (worker_tcp_port,) = reserve_worker_ports([WORKER_CORES])
    worker = WorkerServer(
        host=LOCALHOST,
        tcp_port=worker_tcp_port,
        udp_port=worker_tcp_port + 1,
        env=node_env(node_directory),
        dc_id=DATACENTER_ID,
        total_cores=WORKER_CORES,
        seed_managers=[],
    )
    try:
        yield worker
    finally:
        if worker._running:
            await worker.abort_and_wait(timeout=WORKER_STOP_SECONDS)


async def test_standalone_worker_starts_with_every_core_free_and_stops_cleanly(
    standalone_worker: WorkerServer,
) -> None:
    await asyncio.wait_for(standalone_worker.start(), timeout=WORKER_START_SECONDS)

    assert standalone_worker._total_cores == WORKER_CORES, (
        f"total cores: expected {WORKER_CORES}, got {standalone_worker._total_cores}"
    )
    available_cores = standalone_worker._core_allocator.available_cores
    assert available_cores == WORKER_CORES, f"available cores: expected {WORKER_CORES}, got {available_cores}"
    assert standalone_worker._running, "the worker is not running after start"
    assert len(standalone_worker._active_workflows) == 0, (
        f"unexpected active workflows on a fresh worker: {list(standalone_worker._active_workflows)}"
    )

    await asyncio.wait_for(standalone_worker.stop(), timeout=WORKER_STOP_SECONDS)

    assert not standalone_worker._running, "the worker still reports running after stop"
