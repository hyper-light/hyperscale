"""
Managers discover the workers of their datacenter (AD-28): every manager
registers every worker for clusters of 1-3 managers and 2-4 workers, and
for 6 and 12 workers (at least 80% of them); workers report their node
id, core count, a valid state and a known manager, and a manager can
select a registered worker for allocation and records dispatch latency
for it; a worker that fails is deregistered by every manager and
registered again once it restarts.

The old script also counted each manager's worker DiscoveryService; that
service was deleted from the manager (the manager discovery coordinator,
commit 8bcef0a7), and every one of its checks was "discovery count or
registered count", so the registered count carries each check alone.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.models import WorkerState
from hyperscale.distributed.nodes import ManagerServer, WorkerServer
from tests.integration.in_process_nodes import (
    node_env,
    reserve_cluster_ports,
    stop_nodes,
    wait_until,
)
from tests.integration.manager.manager_cluster import (
    CRASH_DRAIN_SECONDS,
    NODE_START_SECONDS,
    NODE_STOP_SECONDS,
    QUIET_LOG_LEVEL,
    REQUEST_TIMEOUT,
    new_manager,
    new_worker,
)

DATACENTER_ID = "DC-TEST"
WORKER_CORES = 2
# Workers start a few at a time so their registrations do not all land at once.
WORKER_START_BATCH_SIZE = 5
# A running worker's states (the script's valid set, as WorkerState has them).
VALID_WORKER_STATES = {WorkerState.HEALTHY, WorkerState.DEGRADED, WorkerState.DRAINING}
SCALING_REGISTRATION_FRACTION = 0.8
DISPATCH_LATENCY_SAMPLE_MS = 15.0
# The script slept 7-21s for the manager cluster, 10-15s for registration
# and 15s for failure detection; SWIM's slowest detection leg is over a
# minute, so the bounds are generous.
MANAGER_CLUSTER_SECONDS = 60.0
WORKER_REGISTRATION_SECONDS = 60.0
SCALING_REGISTRATION_SECONDS = 120.0
FAILURE_DETECTION_SECONDS = 120.0
RECOVERY_SECONDS = 60.0


@pytest.fixture
async def datacenter(
    node_directory: pathlib.Path,
    request: pytest.FixtureRequest,
) -> AsyncIterator[tuple[list[int], list[ManagerServer], list[WorkerServer]]]:
    """``request.param`` = (manager count, worker count): started managers
    seeded with each other, then -- once every manager counts every peer
    active -- started workers seeded with every manager, and the workers'
    TCP ports. The test may replace a worker in the list; every node in
    the lists is stopped after the test, workers first."""
    manager_count, worker_count = request.param
    manager_ports, worker_ports = reserve_cluster_ports(manager_count, [WORKER_CORES] * worker_count)
    env = node_env(node_directory, MERCURY_SYNC_LOG_LEVEL=QUIET_LOG_LEVEL, MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT)
    managers = [new_manager(env, manager_port, manager_ports, DATACENTER_ID) for manager_port in manager_ports]
    started_workers: list[WorkerServer] = []
    try:
        await asyncio.wait_for(
            asyncio.gather(*[manager.start() for manager in managers]),
            timeout=NODE_START_SECONDS,
        )
        await wait_until(
            lambda: all(
                len(manager._manager_state.get_active_manager_peers()) >= manager_count - 1 for manager in managers
            ),
            within_seconds=MANAGER_CLUSTER_SECONDS,
            description=f"the {manager_count} managers counting each other active",
        )
        for batch_start in range(0, worker_count, WORKER_START_BATCH_SIZE):
            worker_batch = [
                new_worker(env, worker_port, WORKER_CORES, DATACENTER_ID, manager_ports)
                for worker_port in worker_ports[batch_start : batch_start + WORKER_START_BATCH_SIZE]
            ]
            started_workers.extend(worker_batch)
            await asyncio.wait_for(
                asyncio.gather(*[worker.start() for worker in worker_batch]),
                timeout=NODE_START_SECONDS,
            )
        yield worker_ports, managers, started_workers
    finally:
        try:
            await stop_nodes(started_workers, within_seconds=NODE_STOP_SECONDS)
        finally:
            await stop_nodes(managers, within_seconds=NODE_STOP_SECONDS)


@pytest.mark.parametrize(
    "datacenter",
    [(1, 2), (2, 3), (3, 4)],
    indirect=True,
    ids=lambda counts: f"{counts[0]}_managers_{counts[1]}_workers",
)
async def test_every_manager_registers_every_worker(
    datacenter: tuple[list[int], list[ManagerServer], list[WorkerServer]],
) -> None:
    """Each manager has every worker registered."""
    _worker_ports, managers, workers = datacenter
    await wait_until(
        lambda: all(manager._manager_state.get_worker_count() >= len(workers) for manager in managers),
        within_seconds=WORKER_REGISTRATION_SECONDS,
        description=f"every manager registering all {len(workers)} workers",
    )
    for manager in managers:
        registered_worker_count = manager._manager_state.get_worker_count()
        assert registered_worker_count >= len(workers), (
            f"manager {manager._tcp_port} registered {registered_worker_count} of {len(workers)} workers"
        )


@pytest.mark.parametrize(
    "datacenter",
    [(2, 3)],
    indirect=True,
    ids=lambda counts: f"{counts[0]}_managers_{counts[1]}_workers",
)
async def test_workers_and_managers_track_each_other_select_and_record_latency(
    datacenter: tuple[list[int], list[ManagerServer], list[WorkerServer]],
) -> None:
    """Each worker has a node id, its configured cores, a valid state and
    a known manager; each manager has every worker registered, selects a
    registered worker for a one-core allocation, and records a dispatch
    latency sample for a registered worker."""
    _worker_ports, managers, workers = datacenter
    await wait_until(
        lambda: all(manager._manager_state.get_worker_count() >= len(workers) for manager in managers)
        and all(len(worker._known_managers) >= 1 for worker in workers),
        within_seconds=WORKER_REGISTRATION_SECONDS,
        description=f"every manager registering all {len(workers)} workers and every worker knowing a manager",
    )

    for worker in workers:
        assert worker._node_id is not None and worker._node_id.full, f"worker {worker._tcp_port} has no node id"
        assert worker._total_cores == WORKER_CORES, (
            f"worker {worker._tcp_port} has {worker._total_cores} cores, expected {WORKER_CORES}"
        )
        worker_state = worker._get_worker_state()
        assert worker_state in VALID_WORKER_STATES, f"worker {worker._tcp_port} is in invalid state {worker_state}"
        known_manager_count = len(worker._known_managers)
        assert known_manager_count >= 1, f"worker {worker._tcp_port} knows {known_manager_count} managers"

    for manager in managers:
        registered_worker_count = manager._manager_state.get_worker_count()
        assert registered_worker_count >= len(workers), (
            f"manager {manager._tcp_port} registered {registered_worker_count} of {len(workers)} workers"
        )

        allocations = manager._worker_pool._select_workers_for_allocation(1)
        assert allocations, f"manager {manager._tcp_port} selected no worker for a one-core allocation"
        selected_worker_id = allocations[0][0]
        assert manager._manager_state.get_worker(selected_worker_id) is not None, (
            f"manager {manager._tcp_port} selected {selected_worker_id}, which is not registered"
        )

        sampled_worker_id = manager._manager_state.get_worker_ids()[0]
        sampled_at = manager._clock.monotonic()
        manager._manager_state.record_dispatch_latency(sampled_worker_id, DISPATCH_LATENCY_SAMPLE_MS, sampled_at)
        observation = manager._manager_state.get_worker_dispatch_latency_observations(sampled_at).get(
            sampled_worker_id
        )
        assert observation is not None and observation.p50_ms > 0, (
            f"manager {manager._tcp_port} recorded no dispatch latency for worker {sampled_worker_id}: {observation}"
        )


@pytest.mark.parametrize(
    "datacenter",
    [(2, 3), (3, 4)],
    indirect=True,
    ids=lambda counts: f"{counts[0]}_managers_{counts[1]}_workers",
)
async def test_failed_worker_is_deregistered_and_registers_again_after_restart(
    node_directory: pathlib.Path,
    datacenter: tuple[list[int], list[ManagerServer], list[WorkerServer]],
) -> None:
    """A crashed worker is deregistered by every manager; a worker
    restarted on its ports is registered by every manager again."""
    worker_ports, managers, workers = datacenter
    worker_count = len(workers)
    await wait_until(
        lambda: all(manager._manager_state.get_worker_count() >= worker_count for manager in managers),
        within_seconds=WORKER_REGISTRATION_SECONDS,
        description=f"every manager registering all {worker_count} workers",
    )

    # Out of the teardown list before it stops: it is stopped exactly once.
    failed_worker = workers.pop()
    await failed_worker.stop(drain_timeout=CRASH_DRAIN_SECONDS, broadcast_leave=False)

    await wait_until(
        lambda: all(manager._manager_state.get_worker_count() <= worker_count - 1 for manager in managers),
        within_seconds=FAILURE_DETECTION_SECONDS,
        description=f"every manager deregistering the failed worker {failed_worker._tcp_port}",
    )
    for manager in managers:
        registered_worker_count = manager._manager_state.get_worker_count()
        assert registered_worker_count <= worker_count - 1, (
            f"manager {manager._tcp_port} still registers {registered_worker_count} workers after a failure"
        )

    recovered_worker = new_worker(
        node_env(node_directory, MERCURY_SYNC_LOG_LEVEL=QUIET_LOG_LEVEL, MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT),
        worker_ports[-1],
        WORKER_CORES,
        DATACENTER_ID,
        [manager._tcp_port for manager in managers],
    )
    workers.append(recovered_worker)
    await asyncio.wait_for(recovered_worker.start(), timeout=NODE_START_SECONDS)

    await wait_until(
        lambda: all(manager._manager_state.get_worker_count() >= worker_count for manager in managers),
        within_seconds=RECOVERY_SECONDS,
        description=f"every manager registering the restarted worker {recovered_worker._tcp_port}",
    )
    for manager in managers:
        registered_worker_count = manager._manager_state.get_worker_count()
        assert registered_worker_count >= worker_count, (
            f"manager {manager._tcp_port} registers {registered_worker_count} of {worker_count} workers after recovery"
        )


@pytest.mark.parametrize(
    "datacenter",
    [(2, 6), (3, 12)],
    indirect=True,
    ids=lambda counts: f"{counts[0]}_managers_{counts[1]}_workers",
)
async def test_worker_registration_scales_with_worker_count(
    datacenter: tuple[list[int], list[ManagerServer], list[WorkerServer]],
) -> None:
    """With three or four workers per manager, every manager registers at
    least 80% of them."""
    _worker_ports, managers, workers = datacenter
    required_worker_count = len(workers) * SCALING_REGISTRATION_FRACTION
    await wait_until(
        lambda: all(manager._manager_state.get_worker_count() >= required_worker_count for manager in managers),
        within_seconds=SCALING_REGISTRATION_SECONDS,
        description=f"every manager registering at least {required_worker_count} of {len(workers)} workers",
    )
    for manager in managers:
        registered_worker_count = manager._manager_state.get_worker_count()
        assert registered_worker_count >= required_worker_count, (
            f"manager {manager._tcp_port} registered {registered_worker_count} of {len(workers)} workers, "
            f"needs {required_worker_count}"
        )
