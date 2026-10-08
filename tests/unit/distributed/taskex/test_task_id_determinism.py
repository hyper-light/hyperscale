"""
taskex Task / run ids are monotone, deterministic, and RNG-free.

The generators previously returned ``uuid.uuid4().int >> 64`` — random,
which (a) is non-deterministic under SIM replay and (b) silently broke
``Task.latest()`` (``max(self._runs)``) and the count-eviction policy
(``sorted(self._runs)``), both of which assume larger id => later run.
Ids now come from a per-runner ``SnowflakeGenerator`` (clock-seamed,
monotone, drawing NO randomness), which fixes all three: deterministic
under a virtual clock, strictly increasing, and — critically — not
coupled to the shared protocol RNG stream, so generating ids never
perturbs probe scheduling / jitter.
"""

import asyncio
from contextlib import contextmanager

from hyperscale.distributed.taskex.run import Run
from hyperscale.distributed.taskex.models import TaskType
from hyperscale.distributed.taskex.snowflake import SnowflakeGenerator
from hyperscale.distributed.taskex.task import Task
from tests.simulation.harness.sim import SimulationLoop, VirtualClock


def _noop() -> None:
    return None


@contextmanager
def _virtual_generator(instance: int = 0):
    """Yield a snowflake generator bound to a fresh virtual timeline.

    Sets the SimulationLoop as the current event loop so ``Task``'s
    internal ``asyncio.Semaphore`` construction resolves, and tears it
    down afterwards so timelines don't leak across tests.
    """
    loop = SimulationLoop()
    asyncio.set_event_loop(loop)
    try:
        yield SnowflakeGenerator(instance=instance, clock=VirtualClock(loop))
    finally:
        if not loop.is_closed():
            loop.close()
        asyncio.set_event_loop(None)


def test_ids_are_strictly_monotone_and_unique():
    with _virtual_generator() as generator:
        ids = [generator.generate_sync() for _ in range(200)]
    assert ids == sorted(ids), "ids must be strictly increasing"
    assert len(set(ids)) == len(ids), "ids must be unique"


def test_ids_are_deterministic_across_identical_virtual_timelines():
    def id_sequence() -> list[int]:
        with _virtual_generator() as generator:
            return [generator.generate_sync() for _ in range(32)]

    # Same instance + same (fresh) virtual timeline => identical ids.
    assert id_sequence() == id_sequence()


def test_task_uses_injected_generator_and_ids_stay_monotone():
    with _virtual_generator() as generator:
        task = Task("t", _noop, None, asyncio.Semaphore(1), id_generator=generator)
        # task_id was drawn first from the injected generator; subsequent
        # run ids continue the same monotone stream.
        run_ids = [task.generate_id() for _ in range(10)]
    assert task.task_id < run_ids[0]
    assert run_ids == sorted(run_ids)


def test_latest_returns_the_most_recently_created_run():
    """The headline fix: with monotone ids, ``max(self._runs)`` is the
    latest-created run. Random uuid ids made this return an arbitrary run.
    """
    with _virtual_generator() as generator:
        task = Task("t", _noop, None, asyncio.Semaphore(1), id_generator=generator)
        created_run_ids = []
        for _ in range(5):
            run_id = task.generate_id()
            task._runs[run_id] = Run(
                run_id,
                task.name,
                task.call,
                TaskType.CALLABLE,
                None,
                task._executor_semaphore,
            )
            created_run_ids.append(run_id)

        assert task.latest() is task._runs[created_run_ids[-1]]
