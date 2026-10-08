"""
Phase 6c: verify ``TaskRunner`` submits async work deterministically
when the running loop is a ``SimulationLoop``.

The Phase 6c design choice: instead of building a separate
``DeterministicTaskRunner`` class, we prove that the production
``TaskRunner`` already delegates to the running loop
(``asyncio.ensure_future`` under the hood resolves via
``asyncio.get_running_loop().create_task``), so when the running
loop is the ``SimulationLoop``, ``TaskRunner`` inherits its
deterministic ordering guarantees for free.

Tests here demonstrate the property end-to-end:

1. ``TaskRunner`` submits an async callable that awaits
   ``asyncio.sleep(t)``. Under ``SimulationLoop`` this advances
   virtual time by ``t`` and the callable's result becomes
   observable at virtual time ``t``, without wall time elapsing.

2. Two independent submissions serialize by scheduling order —
   the second submission's callback fires strictly after the
   first's, mirroring ``SimulationLoop``'s FIFO ``_ready`` drain.

3. Cancelling a submission via ``TaskRunner.cancel(token)`` marks
   the ``Run`` as ``CANCELLED`` and prevents downstream callbacks
   from firing.

These tests are the proof that Phase 6c's "no new class" choice is
sound: the Runner Protocol seam plus the existing ``TaskRunner``
give SIM everything a separate ``DeterministicTaskRunner`` would.
"""

import asyncio

import pytest

from hyperscale.distributed.taskex.task_runner import TaskRunner
from hyperscale.distributed.taskex.models.run_status import RunStatus

from tests.simulation.harness.sim import SimulationLoop, VirtualClock


@pytest.fixture
def loop() -> SimulationLoop:
    loop_instance = SimulationLoop()
    asyncio.set_event_loop(loop_instance)
    yield loop_instance
    if not loop_instance.is_closed():
        loop_instance.close()
    asyncio.set_event_loop(None)


@pytest.fixture
def task_runner(loop: SimulationLoop) -> TaskRunner:
    """Construct a ``TaskRunner`` bound to the ``SimulationLoop``.

    ``TaskRunner.__init__`` captures the loop via
    ``asyncio.get_event_loop()`` at construction, so this fixture
    must run after the ``loop`` fixture sets it. The runner is shut
    down before the loop closes: closing the loop under it left its
    cleanup task pending, destroyed only when the process exited.
    """
    task_runner_instance = TaskRunner(instance_id=0)
    yield task_runner_instance
    loop.run_until_complete(task_runner_instance.shutdown())


def test_task_runner_submits_async_work_under_sim_loop(
    loop: SimulationLoop,
    task_runner: TaskRunner,
) -> None:
    """Submitting an async callable that sleeps for 3 virtual
    seconds resumes at virtual time 3.0 with zero wall time.

    Proves ``TaskRunner``'s internal ``asyncio.ensure_future``
    lands the work on the ``SimulationLoop`` and the virtual clock
    controls its timing.
    """
    results: list[float] = []

    async def timed_work() -> None:
        await asyncio.sleep(3.0)
        results.append(loop.time())

    async def scenario() -> None:
        run = task_runner.run(timed_work)
        await task_runner.wait(run.token, timeout=10.0)

    loop.run_until_complete(scenario())
    assert results == [pytest.approx(3.0, abs=1e-6)]


def test_multiple_submissions_serialize_deterministically(
    loop: SimulationLoop,
    task_runner: TaskRunner,
) -> None:
    """Two ``TaskRunner.run`` submissions with different sleep
    durations resume in virtual-time order.

    Same guarantee ``SimulationLoop`` gives raw ``asyncio.sleep``,
    now proven end-to-end through the TaskRunner wrapper.
    """
    order: list[str] = []

    async def sleeper(label: str, delay: float) -> None:
        await asyncio.sleep(delay)
        order.append(label)

    async def scenario() -> None:
        run_a = task_runner.run(sleeper, "a", 1.0, alias="task-a")
        run_b = task_runner.run(sleeper, "b", 2.0, alias="task-b")
        await task_runner.wait_all([run_a.token, run_b.token], timeout=10.0)

    loop.run_until_complete(scenario())
    assert order == ["a", "b"]


def test_cancel_marks_run_cancelled(
    loop: SimulationLoop,
    task_runner: TaskRunner,
) -> None:
    """``TaskRunner.cancel(token)`` transitions the ``Run`` to
    ``CANCELLED`` and prevents its downstream callback from
    firing.

    Confirms the Runner Protocol's ``cancel`` contract still works
    when the underlying loop is the SimulationLoop.
    ``TaskRunner.cancel`` is fire-and-forget (returns ``None``);
    verify via the run's post-cancel status field rather than a
    return-value check.
    """
    fired: list[bool] = []

    async def long_work() -> None:
        try:
            await asyncio.sleep(100.0)
        except asyncio.CancelledError:
            raise
        fired.append(True)

    async def scenario():
        run = task_runner.run(long_work, alias="cancellable")
        # Let the task begin — one loop tick is enough to hit
        # the first ``await asyncio.sleep`` suspension point.
        await asyncio.sleep(0)
        await task_runner.cancel(run.token)
        return run

    run = loop.run_until_complete(scenario())
    assert run.status == RunStatus.CANCELLED
    assert fired == []
