"""
taskex schedules run, stop, and release what they hold.

A schedule's loop kept running on the flag of its *current* run, which took
a fresh id every interval -- so a stop addressed to the schedule's id was
lost the moment its first interval passed. ``cancel_schedule`` bound ``run``
to the schedule's future through operator precedence and then awaited
``Task.cancel()``'s bool: it raised TypeError whenever it found a schedule.
Every run after the first was built without its task type, shifting the
executor and semaphore into the wrong parameters. And a schedule's future
and flag stayed in the task's maps for the life of the process.

* an always-repeating schedule runs once per interval until cancelled; the
  cancel stops it, cancels its run in flight, and releases it;
* a counted schedule runs exactly that many times, then releases itself;
* ``stop_schedules`` lets every schedule finish its interval and release.

Driven on a ``SimulationLoop`` with every clock on its virtual time.
"""

import asyncio
import contextvars
from typing import Any, Callable, Coroutine, TypeVar

from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import LoggingConfig
from tests.simulation.harness.sim import SimulationLoop, VirtualClock

ScenarioResult = TypeVar("ScenarioResult")

INTERVAL_SECONDS = 1.0
RUN_SECONDS = 0.25


def simulate(
    scenario: Callable[[TaskRunner], Coroutine[Any, Any, ScenarioResult]],
    until: float,
) -> ScenarioResult:
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    swap_defaults(clock=VirtualClock(loop))
    asyncio.set_event_loop(loop)

    def run_through_deadline() -> ScenarioResult:
        LoggingConfig().disable()
        task_runner = TaskRunner(instance_id=0)

        async def scenario_then_shutdown() -> ScenarioResult:
            try:
                return await scenario(task_runner)
            finally:
                await task_runner.shutdown()

        scenario_task = loop.create_task(scenario_then_shutdown())
        loop.run_window(until)
        assert scenario_task.done(), f"the scenario was still running at virtual {until}"
        return scenario_task.result()

    try:
        return contextvars.copy_context().run(run_through_deadline)
    finally:
        loop.close()
        asyncio.set_event_loop(None)
        restore_defaults(snapshot)


def test_a_cancelled_schedule_stops_cancels_its_run_and_releases_itself() -> None:
    async def scenario(task_runner: TaskRunner) -> tuple[list[float], list[float], dict, dict]:
        started: list[float] = []
        cancelled: list[float] = []
        loop = asyncio.get_running_loop()

        async def tick() -> None:
            started.append(loop.time())
            try:
                await asyncio.sleep(RUN_SECONDS)
            except asyncio.CancelledError:
                cancelled.append(loop.time())
                raise

        run = task_runner.run(tick, schedule=f"{INTERVAL_SECONDS}s", repeat="ALWAYS")
        await asyncio.sleep(3 * INTERVAL_SECONDS + RUN_SECONDS / 2)
        await task_runner.cancel_schedule(f"tick:{run.run_id}")
        await asyncio.sleep(3 * INTERVAL_SECONDS)

        task = task_runner.tasks["tick"]
        return started, cancelled, task._schedules, task._schedule_running_statuses

    started, cancelled, schedules, statuses = simulate(scenario, until=10.0)

    assert started == [0.0, 1.0, 2.0, 3.0]
    assert cancelled == [3.0 + RUN_SECONDS / 2]
    assert (schedules, statuses) == ({}, {})


def test_a_counted_schedule_runs_that_many_times_then_releases_itself() -> None:
    async def scenario(task_runner: TaskRunner) -> tuple[list[float], dict, dict]:
        started: list[float] = []
        loop = asyncio.get_running_loop()

        async def tick() -> None:
            started.append(loop.time())

        task_runner.run(tick, schedule=f"{INTERVAL_SECONDS}s", repeat=3)
        await asyncio.sleep(6 * INTERVAL_SECONDS)

        task = task_runner.tasks["tick"]
        return started, task._schedules, task._schedule_running_statuses

    started, schedules, statuses = simulate(scenario, until=10.0)

    assert started == [0.0, 1.0, 2.0]
    assert (schedules, statuses) == ({}, {})


def test_stopped_schedules_finish_their_interval_and_release() -> None:
    async def scenario(task_runner: TaskRunner) -> tuple[list[float], dict, dict]:
        started: list[float] = []
        loop = asyncio.get_running_loop()

        async def tick() -> None:
            started.append(loop.time())

        task_runner.run(tick, schedule=f"{INTERVAL_SECONDS}s", repeat="ALWAYS")
        await asyncio.sleep(1.5 * INTERVAL_SECONDS)
        task_runner.stop_schedules("tick")
        await asyncio.sleep(3 * INTERVAL_SECONDS)

        task = task_runner.tasks["tick"]
        return started, task._schedules, task._schedule_running_statuses

    started, schedules, statuses = simulate(scenario, until=10.0)

    assert started == [0.0, 1.0]
    assert (schedules, statuses) == ({}, {})
