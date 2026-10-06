"""
The grace a worker gives an orphaned workflow -- its job leader died --
before cancelling it (Section 2.7): derived from the cluster's own timings,
raised by the rescues it has seen, and extended (AD-26) while managers keep
heartbeating the worker. Driven through the real orphan loop and worker
state on a stepped clock.

* Isolated -- no manager heartbeat since the orphaning -- it is cancelled
  once the grace ends, never before: there is no cluster to wait for.
* While heartbeats keep arriving it is extended, each grant half the last
  (AD-26's decay), until the extensions run out.
* A rescue (the new leader found) that took longer than the grace raises
  the grace for the next orphan.
"""

import asyncio
from types import SimpleNamespace

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.worker import background_loops, state as worker_state_module
from hyperscale.distributed.nodes.worker.background_loops import WorkerBackgroundLoops
from hyperscale.distributed.nodes.worker.config import derive_orphan_grace_seconds
from hyperscale.distributed.nodes.worker.state import WorkerState

SETTINGS = Env()
GRACE_SECONDS = derive_orphan_grace_seconds(SETTINGS)
CHECK_INTERVAL_SECONDS = SETTINGS.WORKER_ORPHAN_CHECK_INTERVAL


class SteppedClock:
    """Each sleep advances the time by its length."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        await asyncio.sleep(0)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> SteppedClock:
    stepped_clock = SteppedClock()
    monkeypatch.setattr(background_loops, "_DEFAULT_CLOCK", stepped_clock)
    monkeypatch.setattr(worker_state_module, "_DEFAULT_CLOCK", stepped_clock)
    return stepped_clock


def orphan_loop(state: WorkerState) -> WorkerBackgroundLoops:
    loops = WorkerBackgroundLoops(registry=None, state=state, discovery_service=None)
    loops.configure(
        orphan_grace_period=GRACE_SECONDS,
        orphan_check_interval=CHECK_INTERVAL_SECONDS,
        orphan_extension_min_grant=SETTINGS.EXTENSION_MIN_GRANT,
        orphan_extension_max_extensions=SETTINGS.EXTENSION_MAX_EXTENSIONS,
    )
    return loops


def worker_with_orphan(clock: SteppedClock) -> WorkerState:
    state = WorkerState(
        SimpleNamespace(total_cores=4, available_cores=4),
        throughput_interval_seconds=Env().WORKER_THROUGHPUT_INTERVAL_SECONDS,
        completion_times_max_samples=Env().WORKER_COMPLETION_TIMES_MAX_SAMPLES,
    )
    state._active_workflows["workflow-1"] = SimpleNamespace(job_id="job-1")
    state.mark_workflow_orphaned("workflow-1")
    return state


async def cancelled_after(
    state: WorkerState, clock: SteppedClock, heartbeat_every_seconds: float | None, within_seconds: float
) -> float | None:
    """Run the real orphan loop -- a manager heartbeat every
    ``heartbeat_every_seconds`` if given -- until it cancels the workflow:
    seconds after the orphaning, or None if it never did."""
    started = clock.now
    cancelled_at: list[float] = []

    async def cancel(workflow_id: str, reason: str) -> tuple[bool, list[str]]:
        cancelled_at.append(clock.now)
        state._active_workflows.pop(workflow_id, None)
        return True, []

    async def heartbeats() -> None:
        while not cancelled_at and clock.now - started < within_seconds:
            await clock.sleep(heartbeat_every_seconds)
            state.record_manager_heartbeat()

    loop_task = asyncio.ensure_future(
        orphan_loop(state).run_orphan_check_loop(
            cancel, "127.0.0.1", 9000, "worker-1", lambda: not cancelled_at and clock.now - started < within_seconds
        )
    )
    heartbeat_task = asyncio.ensure_future(heartbeats()) if heartbeat_every_seconds else None
    await loop_task
    if heartbeat_task is not None:
        await heartbeat_task
    return cancelled_at[0] - started if cancelled_at else None


@pytest.mark.asyncio
async def test_an_isolated_worker_cancels_when_the_grace_ends(clock: SteppedClock) -> None:
    state = worker_with_orphan(clock)

    waited = await cancelled_after(state, clock, None, within_seconds=10 * GRACE_SECONDS)

    assert waited is not None
    assert GRACE_SECONDS <= waited < GRACE_SECONDS + CHECK_INTERVAL_SECONDS + 1e-9
    assert not state.is_workflow_orphaned("workflow-1")


@pytest.mark.asyncio
async def test_a_heartbeating_cluster_extends_the_grace_with_decaying_grants(clock: SteppedClock) -> None:
    state = worker_with_orphan(clock)

    waited = await cancelled_after(state, clock, CHECK_INTERVAL_SECONDS, within_seconds=10 * GRACE_SECONDS)

    extensions = sum(
        max(SETTINGS.EXTENSION_MIN_GRANT, GRACE_SECONDS / 2**count)
        for count in range(SETTINGS.EXTENSION_MAX_EXTENSIONS)
    )
    assert waited is not None
    assert GRACE_SECONDS + extensions <= waited < GRACE_SECONDS + extensions + CHECK_INTERVAL_SECONDS + 1e-9


@pytest.mark.asyncio
async def test_a_slow_rescue_raises_the_grace(clock: SteppedClock) -> None:
    state = worker_with_orphan(clock)
    slow_rescue_seconds = GRACE_SECONDS * 1.5
    clock.now += slow_rescue_seconds
    state.clear_workflow_orphaned("workflow-1")
    state.mark_workflow_orphaned("workflow-1")

    waited = await cancelled_after(state, clock, None, within_seconds=10 * GRACE_SECONDS)

    assert state.longest_orphan_rescue_seconds == slow_rescue_seconds
    assert waited is not None
    assert slow_rescue_seconds <= waited < slow_rescue_seconds + CHECK_INTERVAL_SECONDS + 1e-9
