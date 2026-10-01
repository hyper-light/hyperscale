"""
AD-41 THROTTLE in the core WorkflowRunner, against its real spawn gate.

A TEST workflow's spawn loop (``_generate``) yields new VUs while the
steps in flight stay at or under the workflow's concurrency cap, and
parks once they exceed it. These tests drive that real loop with a
controlled in-flight count:

* a throttle cuts the cap to ``scale`` of the operating point (the lower
  of cap and in-flight steps), never below one, compounding when
  repeated -- and the loop parks on the cut cap;
* a release restores the original cap and wakes the parked loop, leaving
  the gate re-armable (it parks again when the cap is exceeded again);
* a workflow without a concurrency-gated loop (not running, or ACTION)
  is reported unthrottleable;
* ending the gate leaves no throttle state behind.
"""

import asyncio

import pytest

from hyperscale.core.jobs.graphs.workflow_runner import WorkflowRunner
from hyperscale.core.jobs.models.env import Env

RUN_ID = 7
WORKFLOW = "LoadTest"
BASE_CAP = 10
LOOP_DURATION_SECONDS = 60.0
SETTLE_ITERATIONS = 50


def make_runner(in_flight: int) -> WorkflowRunner:
    runner = WorkflowRunner(Env(), 1, 1, monitors_enabled=False)
    runner._running = True
    runner._concurrency_gated[RUN_ID].add(WORKFLOW)
    runner._max_active[RUN_ID][WORKFLOW] = BASE_CAP
    runner._active[RUN_ID][WORKFLOW] = in_flight
    runner._active_waiters[RUN_ID][WORKFLOW] = None
    return runner


class SpawnLoop:
    """Consumes the runner's real spawn generator, counting yields."""

    def __init__(self, runner: WorkflowRunner) -> None:
        self.yields = 0
        self._generator = runner._generate(RUN_ID, WORKFLOW, {"duration": LOOP_DURATION_SECONDS})
        self._task = asyncio.ensure_future(self._consume())

    async def _consume(self) -> None:
        async for _ in self._generator:
            self.yields += 1

    async def settle(self) -> int:
        """Yields gained across a fixed number of loop turns."""
        before = self.yields
        for _ in range(SETTLE_ITERATIONS):
            await asyncio.sleep(0)
        return self.yields - before

    async def stop(self, runner: WorkflowRunner) -> None:
        runner._running = False
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        await self._generator.aclose()


@pytest.mark.asyncio
async def test_a_throttle_parks_the_spawn_loop_and_a_release_wakes_it() -> None:
    runner = make_runner(in_flight=5)
    loop = SpawnLoop(runner)
    try:
        assert await loop.settle() > 0  # 5 in flight <= cap 10: spawning

        assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.5) == 2  # floor(min(10, 5) * 0.5)
        await loop.settle()
        assert await loop.settle() == 0  # 5 in flight > cap 2: parked
        assert runner._active_waiters[RUN_ID][WORKFLOW] is not None

        assert runner.release_workflow_throttle(RUN_ID, WORKFLOW) is True
        assert runner._max_active[RUN_ID][WORKFLOW] == BASE_CAP
        assert await loop.settle() > 0  # woken, spawning again

        # The gate re-arms: exceeding the restored cap parks the loop again.
        runner._active[RUN_ID][WORKFLOW] = BASE_CAP + 1
        await loop.settle()
        assert await loop.settle() == 0
    finally:
        await loop.stop(runner)


def test_throttles_compound_from_the_operating_point_and_never_reach_zero() -> None:
    runner = make_runner(in_flight=8)

    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.5) == 4
    runner._active[RUN_ID][WORKFLOW] = 4
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.5) == 2
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.01) == 1

    assert runner.release_workflow_throttle(RUN_ID, WORKFLOW) is True
    assert runner._max_active[RUN_ID][WORKFLOW] == BASE_CAP  # the original, not an intermediate cap


def test_an_idle_workflow_is_throttled_from_its_cap() -> None:
    runner = make_runner(in_flight=0)
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.5) == BASE_CAP // 2


def test_a_workflow_without_a_gated_loop_cannot_be_throttled() -> None:
    runner = make_runner(in_flight=5)
    assert runner.throttle_workflow(RUN_ID, "ActionWorkflow", 0.5) is None
    assert runner.throttle_workflow(RUN_ID + 1, WORKFLOW, 0.5) is None
    assert runner.release_workflow_throttle(RUN_ID, WORKFLOW) is False  # never throttled


@pytest.mark.parametrize("scale", [0.0, -0.5, 1.5])
def test_a_scale_outside_zero_to_one_is_refused(scale: float) -> None:
    runner = make_runner(in_flight=5)
    with pytest.raises(ValueError):
        runner.throttle_workflow(RUN_ID, WORKFLOW, scale)


def test_ending_the_gate_leaves_no_throttle_state() -> None:
    runner = make_runner(in_flight=5)
    runner.throttle_workflow(RUN_ID, WORKFLOW, 0.5)

    runner._end_concurrency_gate(RUN_ID, WORKFLOW)

    assert RUN_ID not in runner._concurrency_gated
    assert RUN_ID not in runner._throttle_base_cap
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.5) is None
