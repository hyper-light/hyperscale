"""
AD-41 capacity membership (``WorkerPool.counts_toward_capacity``).

A datacenter's worker capacity counts every worker whose cores are its to
use, busy or idle. Pinned: a saturated worker -- not ready for new work,
so routed DRAIN and not ``is_worker_healthy`` -- still counts; a worker
that is leaving (drain intended, lifecycle DRAINING), judged dead or stuck
(routing EVICT), or suspected by SWIM does not.
"""

import pytest

from hyperscale.distributed.health.worker_health import RoutingDecision
from hyperscale.distributed.jobs.worker_pool import WorkerPool
from hyperscale.distributed.models import WorkerState
from tests.unit.distributed.jobs.test_workflow_dispatch_routing import (
    _heartbeat,
    _registration,
)

WORKER_ID = "worker-1"


async def _registered_pool(swim_status: str = "OK") -> WorkerPool:
    worker_pool = WorkerPool(get_swim_status=lambda _addr: swim_status)
    await worker_pool.register_worker(_registration(worker_id=WORKER_ID))
    return worker_pool


@pytest.mark.asyncio
async def test_saturated_worker_still_counts() -> None:
    worker_pool = await _registered_pool()
    await worker_pool.process_heartbeat(
        WORKER_ID,
        _heartbeat(worker_id=WORKER_ID, available_cores=0, accepting_work=False),
    )

    assert worker_pool.get_worker_routing_decision(WORKER_ID) == RoutingDecision.DRAIN
    assert worker_pool.counts_toward_capacity(WORKER_ID)


@pytest.mark.asyncio
async def test_worker_with_drain_intent_does_not_count() -> None:
    worker_pool = await _registered_pool()
    await worker_pool.mark_workers_draining({WORKER_ID}, reason="planned_scale_down")

    assert not worker_pool.counts_toward_capacity(WORKER_ID)


@pytest.mark.asyncio
async def test_lifecycle_draining_worker_does_not_count() -> None:
    worker_pool = await _registered_pool()
    await worker_pool.process_heartbeat(
        WORKER_ID,
        _heartbeat(worker_id=WORKER_ID, state=WorkerState.DRAINING, accepting_work=False),
    )

    assert not worker_pool.counts_toward_capacity(WORKER_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize("swim_status", ["SUSPECT", "DEAD"])
async def test_swim_suspected_or_dead_worker_does_not_count(swim_status: str) -> None:
    worker_pool = await _registered_pool(swim_status=swim_status)

    assert not worker_pool.counts_toward_capacity(WORKER_ID)


@pytest.mark.asyncio
async def test_evicted_worker_does_not_count() -> None:
    worker_pool = await _registered_pool()
    health_state = worker_pool._worker_health[WORKER_ID]
    for _ in range(health_state.config.max_consecutive_liveness_failures):
        health_state.update_liveness(success=False)

    assert worker_pool.get_worker_routing_decision(WORKER_ID) == RoutingDecision.EVICT
    assert not worker_pool.counts_toward_capacity(WORKER_ID)


def test_unknown_worker_does_not_count() -> None:
    assert not WorkerPool().counts_toward_capacity("never-registered")
