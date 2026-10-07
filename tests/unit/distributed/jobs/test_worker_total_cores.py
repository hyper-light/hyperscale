"""
A manager's record of a worker's total cores (``WorkerStatus.total_cores``).

The total is the worker's own core budget -- its allocator's total, sent
in every heartbeat beside the free count -- and never moves with the free
count. Pinned: a busy heartbeat (one workflow holding both cores of a
two-core worker) keeps the total at two; a heartbeat reordered behind a
newer free count never leaves the record with more free cores than total
ones; a heartbeat that does not carry the total keeps the registered one.
"""

import pytest

from hyperscale.distributed.jobs.worker_pool import WorkerPool
from hyperscale.distributed.models import WorkerHeartbeat, WorkerState
from tests.unit.distributed.jobs.test_workflow_dispatch_routing import _registration

WORKER_ID = "worker-1"
WORKER_CORES = 2


def _worker_heartbeat(
    available_cores: int,
    active_workflows: dict[str, str],
    version: int,
    cores_version: int,
    total_cores: int = WORKER_CORES,
) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        node_id=WORKER_ID,
        state=WorkerState.HEALTHY.value,
        available_cores=available_cores,
        cores_version=cores_version,
        total_cores=total_cores,
        queue_depth=0,
        cpu_percent=0.0,
        memory_percent=0.0,
        version=version,
        active_workflows=active_workflows,
    )


async def _registered_pool() -> WorkerPool:
    worker_pool = WorkerPool()
    await worker_pool.register_worker(_registration(worker_id=WORKER_ID, cores=WORKER_CORES))
    return worker_pool


def _total_and_free(worker_pool: WorkerPool) -> tuple[int, int]:
    worker = worker_pool.get_worker(WORKER_ID)
    assert worker is not None
    return worker.total_cores, worker.available_cores - worker.reserved_cores


@pytest.mark.asyncio
async def test_busy_heartbeat_keeps_the_worker_total() -> None:
    worker_pool = await _registered_pool()

    await worker_pool.process_heartbeat(
        WORKER_ID,
        _worker_heartbeat(available_cores=0, active_workflows={"workflow-1": "running"}, version=1, cores_version=1),
    )

    assert _total_and_free(worker_pool) == (WORKER_CORES, 0)


@pytest.mark.asyncio
async def test_heartbeat_behind_a_newer_free_count_never_leaves_free_above_total() -> None:
    worker_pool = await _registered_pool()
    await worker_pool.process_heartbeat(
        WORKER_ID,
        _worker_heartbeat(available_cores=2, active_workflows={}, version=1, cores_version=4),
    )

    # Built mid-job (free count version 3), delivered after the idle one
    # (version 4): its free count is not applied, and nothing else in it
    # may move the total under the free count that stays.
    await worker_pool.process_heartbeat(
        WORKER_ID,
        _worker_heartbeat(available_cores=0, active_workflows={"workflow-1": "running"}, version=1, cores_version=3),
    )

    assert _total_and_free(worker_pool) == (WORKER_CORES, WORKER_CORES)


@pytest.mark.asyncio
async def test_heartbeat_without_a_total_keeps_the_registered_total() -> None:
    worker_pool = await _registered_pool()

    await worker_pool.process_heartbeat(
        WORKER_ID,
        _worker_heartbeat(available_cores=1, active_workflows={}, version=1, cores_version=1, total_cores=0),
    )

    assert _total_and_free(worker_pool) == (WORKER_CORES, 1)
