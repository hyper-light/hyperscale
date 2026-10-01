"""
AD-19 systemic eviction hold on the manager's deadline enforcement
(AD-26): a worker past its deadline beyond the grace period is evicted
-- unless more than half the workers are, at once, which points at the
manager's own view (its network, its event loop) rather than the
workers. Then every eviction is held (the workers stay suspected) and
the hold is reported once; it releases, also reported once, when the
failure narrows.

Driven through the manager's real enforcement pass over a real
ManagerState; only the suspect/evict effects and the logger are
recorded.

* One failing worker -- even the only worker -- is always evicted: a
  single failure is never evidence of correlation.
* A minority failing is evicted; a majority is held.
* Hold and release are each logged once across passes.
* An expired deadline on a worker with no work left is cleared, not
  counted.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.health.systemic_failure import is_systemic_failure
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.logging.hyperscale_logging_models import (
    SystemicEvictionHeld,
    SystemicEvictionReleased,
)

GRACE_PERIOD_SECONDS = 30.0
NOW = 1_000.0
BEYOND_GRACE = NOW - GRACE_PERIOD_SECONDS - 1.0
WITHIN_GRACE = NOW - 1.0


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)

    def of_type(self, entry_type: type) -> list:
        return [entry for entry in self.entries if isinstance(entry, entry_type)]


class ActiveWork:
    def __init__(self) -> None:
        self.idle_workers: set[str] = set()

    def get_reassignable_sub_workflows_on_worker(self, worker_id: str) -> list[str]:
        return [] if worker_id in self.idle_workers else [f"{worker_id}-workflow"]


def make_manager(worker_ids: list[str]) -> tuple[ManagerServer, ManagerState, ActiveWork, RecordingLogger, dict]:
    manager = object.__new__(ManagerServer)
    state = ManagerState()
    for worker_id in worker_ids:
        state.add_worker(worker_id, SimpleNamespace(node_id=worker_id))
    active_work = ActiveWork()
    logger = RecordingLogger()
    effects: dict[str, list[str]] = {"suspected": [], "evicted": []}

    async def suspect(worker_id: str) -> None:
        effects["suspected"].append(worker_id)

    async def evict(worker_id: str) -> None:
        effects["evicted"].append(worker_id)
        state.remove_worker(worker_id)
        state.clear_worker_deadline(worker_id)

    manager._manager_state = state
    manager._job_manager = active_work
    manager._udp_logger = logger
    manager._node_id = SimpleNamespace(short="mgr-1")
    manager._systemic_eviction_hold = False
    manager._suspect_worker_deadline_expired = suspect
    manager._evict_worker_deadline_expired = evict
    return manager, state, active_work, logger, effects


async def enforce(manager: ManagerServer) -> None:
    await manager._enforce_worker_deadlines(NOW, GRACE_PERIOD_SECONDS)


@pytest.mark.parametrize("population", range(1, 12))
def test_systemic_means_more_than_half_and_never_a_single_failure(population: int) -> None:
    for failing in range(population + 1):
        expected = failing >= 2 and failing > population / 2
        assert is_systemic_failure(failing, population) is expected, (failing, population)


@pytest.mark.asyncio
async def test_the_only_worker_failing_is_evicted() -> None:
    manager, state, _work, logger, effects = make_manager(["worker-a"])
    state.set_worker_deadline("worker-a", BEYOND_GRACE)

    await enforce(manager)

    assert effects["evicted"] == ["worker-a"]
    assert logger.entries == []


@pytest.mark.asyncio
async def test_a_minority_failing_is_evicted() -> None:
    manager, state, _work, logger, effects = make_manager(["worker-a", "worker-b", "worker-c", "worker-d", "worker-e"])
    state.set_worker_deadline("worker-a", BEYOND_GRACE)
    state.set_worker_deadline("worker-b", BEYOND_GRACE)
    state.set_worker_deadline("worker-c", WITHIN_GRACE)

    await enforce(manager)

    assert sorted(effects["evicted"]) == ["worker-a", "worker-b"]
    assert effects["suspected"] == ["worker-c"]
    assert logger.of_type(SystemicEvictionHeld) == []


@pytest.mark.asyncio
async def test_a_majority_failing_is_held_reported_once_then_released() -> None:
    manager, state, active_work, logger, effects = make_manager(["worker-a", "worker-b", "worker-c"])
    state.set_worker_deadline("worker-a", BEYOND_GRACE)
    state.set_worker_deadline("worker-b", BEYOND_GRACE)

    await enforce(manager)
    await enforce(manager)

    assert effects["evicted"] == []
    assert sorted(effects["suspected"]) == ["worker-a", "worker-a", "worker-b", "worker-b"]
    (held,) = logger.of_type(SystemicEvictionHeld)
    assert (held.held_count, held.population) == (2, 3)

    # worker-b's work drains: one failing worker of three is not systemic.
    active_work.idle_workers.add("worker-b")
    await enforce(manager)

    assert effects["evicted"] == ["worker-a"]
    assert len(logger.of_type(SystemicEvictionReleased)) == 1
    assert len(logger.of_type(SystemicEvictionHeld)) == 1


@pytest.mark.asyncio
async def test_an_idle_workers_expired_deadline_is_cleared_not_counted() -> None:
    manager, state, active_work, logger, effects = make_manager(["worker-a", "worker-b", "worker-c"])
    state.set_worker_deadline("worker-a", BEYOND_GRACE)
    state.set_worker_deadline("worker-b", BEYOND_GRACE)
    active_work.idle_workers.add("worker-b")

    await enforce(manager)

    assert effects["evicted"] == ["worker-a"]
    assert [worker_id for worker_id, _ in state.iter_worker_deadlines()] == []
    assert logger.of_type(SystemicEvictionHeld) == []
