"""
A worker's startup, phase by phase, each bounded on its own so a hang
names the phase it is in: the executor addresses, the CPU and memory
monitors, the local server pool's setup, the pool leader
(RemoteGraphManager) starting, the executor pool running, and the leader
connecting to every executor. The worker's core count comes from
``WORKER_MAX_CORES`` (no explicit ``total_cores``).
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator, Awaitable

import pytest

from hyperscale.distributed.nodes import WorkerServer
from tests.integration.in_process_nodes import LOCALHOST, node_env, reserve_worker_ports

DATACENTER_ID = "DC-TEST"
WORKER_MAX_CORES = 2
MONITOR_START_SECONDS = 5.0
POOL_PHASE_SECONDS = 10.0
# connect_to_workers' own budget per operation, and the outer bound on the
# whole connection (its poll for every executor's start acknowledgement).
EXECUTOR_CONNECT_SECONDS = 5.0
EXECUTOR_CONNECT_OUTER_SECONDS = 15.0
TEARDOWN_SECONDS = 30.0


async def run_phase(phase_description: str, phase: Awaitable[None], within_seconds: float) -> None:
    """Await one startup phase; fail naming it if it hangs past ``within_seconds``."""
    try:
        await asyncio.wait_for(phase, timeout=within_seconds)
    except TimeoutError as timeout_error:
        raise AssertionError(f"{phase_description} hung past {within_seconds}s") from timeout_error


@pytest.fixture
async def unstarted_worker(node_directory: pathlib.Path) -> AsyncIterator[WorkerServer]:
    """A standalone worker of ``WORKER_MAX_CORES`` cores (from the Env) at
    debug log level, never started as a server: the test drives its
    lifecycle phases itself, so teardown shuts those components down
    (pool leader, monitors, server pool, executor processes)."""
    (worker_tcp_port,) = reserve_worker_ports([WORKER_MAX_CORES])
    worker = WorkerServer(
        host=LOCALHOST,
        tcp_port=worker_tcp_port,
        udp_port=worker_tcp_port + 1,
        env=node_env(node_directory, MERCURY_SYNC_LOG_LEVEL="debug", WORKER_MAX_CORES=WORKER_MAX_CORES),
        dc_id=DATACENTER_ID,
        seed_managers=[],
    )
    try:
        yield worker
    finally:
        await asyncio.wait_for(worker._shutdown_lifecycle_components(), timeout=TEARDOWN_SECONDS)


async def test_every_worker_startup_phase_completes_within_its_bound(unstarted_worker: WorkerServer) -> None:
    lifecycle = unstarted_worker._lifecycle_manager
    datacenter_id = unstarted_worker._node_id.datacenter
    node_id = unstarted_worker._node_id.full
    lifecycle.setup_logging_config()

    assert unstarted_worker._total_cores == WORKER_MAX_CORES, (
        f"WORKER_MAX_CORES={WORKER_MAX_CORES} not honoured: the worker has {unstarted_worker._total_cores} cores"
    )

    executor_addresses = lifecycle.get_worker_ips()
    assert len(executor_addresses) == WORKER_MAX_CORES, (
        f"expected one executor address per core ({WORKER_MAX_CORES}), got {executor_addresses}"
    )

    await run_phase(
        "starting the CPU monitor",
        lifecycle.cpu_monitor.start_background_monitor(datacenter_id, node_id),
        MONITOR_START_SECONDS,
    )
    await run_phase(
        "starting the memory monitor",
        lifecycle.memory_monitor.start_background_monitor(datacenter_id, node_id),
        MONITOR_START_SECONDS,
    )
    await run_phase("setting up the server pool", lifecycle.setup_server_pool(), POOL_PHASE_SECONDS)
    await lifecycle.initialize_remote_manager(
        unstarted_worker._updates_controller,
        unstarted_worker._config.progress_update_interval,
    )
    await run_phase(
        "starting the pool leader (RemoteGraphManager)",
        lifecycle.start_remote_manager(),
        POOL_PHASE_SECONDS,
    )
    await run_phase("running the executor pool", lifecycle.run_worker_pool(), POOL_PHASE_SECONDS)
    await run_phase(
        "connecting the pool leader to every executor",
        lifecycle.connect_to_workers(timeout=EXECUTOR_CONNECT_SECONDS),
        EXECUTOR_CONNECT_OUTER_SECONDS,
    )
