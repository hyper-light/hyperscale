"""
AD-41 THROTTLE in the core WorkflowRunner, against its real VUs.

A TEST workflow's long-lived VUs (``_run_long_lived_vu``) each run an
iteration in one of the workflow's slots -- one per VU until a throttle
cuts the cap. A VU that finds every slot taken, or VUs already waiting for
one, waits in line; a VU ending an iteration hands its slot to the first in
line while the cap allows it. These tests drive real VUs whose one step
records how many steps are under way and which VU ran it:

* a throttle cuts the cap to ``scale`` of the operating point (the lower of
  cap and the VUs in an iteration), never below one, compounding when
  repeated; iterations under way finish, then the VUs in iterations settle
  at the cut cap -- and a release restores the original cap, handing the
  freed slots to the VUs in line at once;
* under a cap, the VUs take turns, first come first served;
* VUs waiting in line end promptly when the run stops or its deadline
  passes, not when they would next have been handed a slot;
* a VU that fails hands its slot on, so the others carry on;
* a hard cancel cancels the VUs in line; ending the gate leaves no throttle
  state behind;
* a workflow without gated VUs (not running, or ACTION) is reported
  unthrottleable.
"""

import asyncio
import collections
import contextvars
from typing import Any, Awaitable, Callable

import pytest

from hyperscale.core.jobs.graphs.completion_counter import CompletionCounter
from hyperscale.core.jobs.graphs.workflow_runner import WorkflowRunner
from hyperscale.core.jobs.models.env import Env

RUN_ID = 7
WORKFLOW = "LoadTest"
BASE_CAP = 10
STEP_SECONDS = 0.005
# Long enough for every iteration under way to finish, many times over.
SETTLE_SECONDS = 0.05
RUN_SECONDS = 60.0
# A VU that ends promptly ends within this; one left to its deadline would
# take RUN_SECONDS.
PROMPT_END_SECONDS = 1.0
VU_IDENTITY: contextvars.ContextVar[int] = contextvars.ContextVar("vu_identity")


def make_runner(in_flight: int) -> WorkflowRunner:
    runner = WorkflowRunner(Env(), 1, 1, monitors_enabled=False)
    runner._concurrency_gated[RUN_ID].add(WORKFLOW)
    runner._max_active[RUN_ID][WORKFLOW] = BASE_CAP
    runner._active[RUN_ID][WORKFLOW] = in_flight
    runner._active_waiters[RUN_ID][WORKFLOW] = collections.deque()
    return runner


def make_vu_runner() -> WorkflowRunner:
    """A runner set up as _setup and the concurrency gate leave a TEST workflow of BASE_CAP VUs."""
    runner = make_runner(in_flight=0)
    runner._completed_counts[RUN_ID][WORKFLOW] = CompletionCounter()
    runner._failed_counts[RUN_ID][WORKFLOW] = CompletionCounter()
    runner._workflow_step_stats[RUN_ID][(WORKFLOW, "step")] = {
        "total": CompletionCounter(),
        "ok": CompletionCounter(),
        "err": CompletionCounter(),
    }
    return runner


class LoadStep:
    """The VUs' one step: the steps under way and their peak, each VU's turns, and cancellations."""

    def __init__(self, step_seconds: float = STEP_SECONDS) -> None:
        self.step_seconds = step_seconds
        self.under_way = 0
        self.peak = 0
        self.cancelled = 0
        self.turns: collections.Counter[int] = collections.Counter()

    async def run(self) -> int:
        self.under_way += 1
        self.peak = max(self.peak, self.under_way)
        self.turns[VU_IDENTITY.get()] += 1
        try:
            await asyncio.sleep(self.step_seconds)

        except asyncio.CancelledError:
            self.cancelled += 1
            raise

        finally:
            self.under_way -= 1

        return 1

    def take_peak(self) -> int:
        """The peak since the last call; the next one starts from the steps under way now."""
        peak, self.peak = self.peak, self.under_way
        return peak


class StepHook:
    """What a VU reads from a step's Hook: its call, context arguments and keyword names."""

    def __init__(self, call: Callable[[], Awaitable[int]]) -> None:
        self.call = call
        self.context_args: dict[str, Any] = {}
        self.kwarg_names = ["self"]


class StepAggregation:
    """What a VU reads from the workflow's Results: aggregate_result, failing on one chosen call."""

    def __init__(self, failing_call: int | None = None) -> None:
        self.calls = 0
        self.failing_call = failing_call

    def aggregate_result(self, aggregates: dict[str, list[Any]], step_name: str, result: Any) -> None:
        self.calls += 1
        if self.calls == self.failing_call:
            raise RuntimeError("aggregation failed")


class RunningVUs:
    """BASE_CAP real long-lived VUs of a registered run of the gated workflow, each known to its step."""

    def __init__(
        self,
        runner: WorkflowRunner,
        step: LoadStep,
        run_seconds: float = RUN_SECONDS,
        aggregation: StepAggregation | None = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        hook = StepHook(step.run)
        self.parked: collections.deque[asyncio.Future[None]] = runner._active_waiters[RUN_ID][WORKFLOW]
        self.control = runner.register_run(RUN_ID, WORKFLOW)
        self.deadline = loop.time() + run_seconds
        self.tasks: list[asyncio.Task[None]] = []
        for identity in range(BASE_CAP):
            vu_context = contextvars.copy_context()
            vu_context.run(VU_IDENTITY.set, identity)
            self.tasks.append(
                loop.create_task(
                    runner._run_long_lived_vu(
                        RUN_ID,
                        WORKFLOW,
                        [{"step": hook}],
                        {},
                        aggregation or StepAggregation(),
                        {"step": []},
                        self.deadline,
                        None,
                        self.parked,
                        self.control,
                    ),
                    context=vu_context,
                )
            )

    async def stop(self, runner: WorkflowRunner) -> set[asyncio.Task[None]]:
        """Cancels the run gracefully; the VUs still running after PROMPT_END_SECONDS."""
        assert runner.request_cancellation(RUN_ID, WORKFLOW) is True
        _, still_running = await asyncio.wait(self.tasks, timeout=PROMPT_END_SECONDS)
        return still_running

    async def cancel(self) -> None:
        for task in self.tasks:
            task.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_throttle_settles_the_vus_at_the_cut_cap_and_a_release_restores_them() -> None:
    runner = make_vu_runner()
    step = LoadStep()
    vus = RunningVUs(runner, step)
    try:
        await asyncio.sleep(SETTLE_SECONDS)
        assert step.take_peak() == BASE_CAP  # every VU in an iteration at once
        assert not vus.parked

        assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.5) == 5  # floor(min(10, 10 in flight) * 0.5)
        await asyncio.sleep(SETTLE_SECONDS)
        step.take_peak()
        await asyncio.sleep(SETTLE_SECONDS)
        assert step.take_peak() == 5
        assert len(vus.parked) == BASE_CAP - 5
        assert step.cancelled == 0  # iterations under way at the cut finished

        assert runner.release_workflow_throttle(RUN_ID, WORKFLOW) is True
        assert runner._max_active[RUN_ID][WORKFLOW] == BASE_CAP
        assert runner._active[RUN_ID][WORKFLOW] == BASE_CAP  # the VUs in line were handed the freed slots
        assert not vus.parked
        await asyncio.sleep(SETTLE_SECONDS)
        assert step.take_peak() == BASE_CAP

        assert await vus.stop(runner) == set()
        assert runner._active[RUN_ID][WORKFLOW] == 0  # every slot given back
    finally:
        await vus.cancel()


@pytest.mark.asyncio
async def test_vus_under_a_cap_take_turns_first_come_first_served() -> None:
    runner = make_vu_runner()
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.3) == 3  # idle: floor(10 * 0.3), from the cap
    step = LoadStep()
    vus = RunningVUs(runner, step)
    try:
        await asyncio.sleep(SETTLE_SECONDS * 6)
        assert step.take_peak() == 3

        assert await vus.stop(runner) == set()
        turns = [step.turns[identity] for identity in range(BASE_CAP)]
        assert min(turns) > 0
        assert max(turns) - min(turns) <= 1, turns
    finally:
        await vus.cancel()


@pytest.mark.asyncio
async def test_vus_waiting_in_line_end_promptly_when_the_run_stops() -> None:
    runner = make_vu_runner()
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.1) == 1
    step = LoadStep(step_seconds=0.02)
    vus = RunningVUs(runner, step)
    try:
        await asyncio.sleep(SETTLE_SECONDS * 2)
        assert len(vus.parked) == BASE_CAP - 1

        assert await vus.stop(runner) == set()
        assert runner._active[RUN_ID][WORKFLOW] == 0
        assert not any(not parked_vu.done() for parked_vu in vus.parked)
    finally:
        await vus.cancel()


@pytest.mark.asyncio
async def test_vus_waiting_in_line_end_at_the_deadline() -> None:
    runner = make_vu_runner()
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.2) == 2
    step = LoadStep(step_seconds=0.01)
    vus = RunningVUs(runner, step, run_seconds=0.2)
    try:
        loop = asyncio.get_running_loop()
        _, still_running = await asyncio.wait(vus.tasks, timeout=vus.deadline - loop.time() + PROMPT_END_SECONDS)
        assert still_running == set()
        assert runner._active[RUN_ID][WORKFLOW] == 0
    finally:
        await vus.cancel()


@pytest.mark.asyncio
async def test_a_failing_vu_hands_its_slot_on_and_the_others_carry_on() -> None:
    runner = make_vu_runner()
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.1) == 1
    step = LoadStep()
    aggregation = StepAggregation(failing_call=5)
    vus = RunningVUs(runner, step, aggregation=aggregation)
    try:
        await asyncio.sleep(SETTLE_SECONDS * 4)
        failed = [task for task in vus.tasks if task.done() and not task.cancelled() and task.exception() is not None]
        assert [repr(task.exception()) for task in failed] == ["RuntimeError('aggregation failed')"]
        assert aggregation.calls > 10  # the others kept running iterations past the failure

        assert await vus.stop(runner) == set()
        assert runner._active[RUN_ID][WORKFLOW] == 0  # the failed VU's slot was handed on, not lost
    finally:
        await vus.cancel()


@pytest.mark.asyncio
async def test_a_hard_cancel_cancels_the_vus_in_line_and_drops_the_line() -> None:
    runner = make_vu_runner()
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.1) == 1
    step = LoadStep(step_seconds=RUN_SECONDS)
    vus = RunningVUs(runner, step)
    try:
        await asyncio.sleep(SETTLE_SECONDS)
        assert len(vus.parked) == BASE_CAP - 1

        runner.hard_cancel(RUN_ID, WORKFLOW)
        await asyncio.sleep(SETTLE_SECONDS)

        assert sum(task.done() and task.cancelled() for task in vus.tasks) == BASE_CAP - 1
        assert WORKFLOW not in runner._active_waiters.get(RUN_ID, {})
        assert not vus.parked
        # The run itself is stopped and marked ended; no other run is touched.
        assert vus.control.running is False
        assert vus.control.ended.is_set()
    finally:
        # The VU in an iteration is cancelled as its executor would cancel it.
        await vus.cancel()


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


def test_a_workflow_without_gated_vus_cannot_be_throttled() -> None:
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
    assert RUN_ID not in runner._active_waiters
    assert runner.throttle_workflow(RUN_ID, WORKFLOW, 0.5) is None
