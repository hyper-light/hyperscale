"""
A task's finished runs are released; its live runs never are.

Every ``TaskRunner.run`` call leaves a ``Run`` on its task, released only by
the retention sweep. With ``keep`` unset (every call site but two), the
sweep raised ``TypeError`` on ``len(...) > None`` and a bare ``except``
hid it -- aborting the whole sweep -- so a server kept every run of every
call (each log line, each send) for its whole life. And the COUNT policy,
when it did run, cancelled the oldest ``keep`` runs, live ones included.
"""

import asyncio

import pytest

from hyperscale.distributed.taskex import TaskRunner


async def finished_call(value: int) -> int:
    return value


async def long_call(release: asyncio.Event) -> None:
    await release.wait()


@pytest.mark.asyncio
async def test_finished_runs_beyond_keep_are_released() -> None:
    runner = TaskRunner(0)
    for index in range(1000):
        runner.run(finished_call, index)
    await asyncio.sleep(0.05)

    await runner._cleanup_scheduled_tasks()

    (task,) = runner.tasks.values()
    assert len(task._runs) == task.keep


@pytest.mark.asyncio
async def test_retention_never_cancels_a_live_run() -> None:
    runner = TaskRunner(0)
    release = asyncio.Event()
    live_runs = [runner.run(long_call, release, keep=2) for _ in range(5)]
    await asyncio.sleep(0.01)

    await runner._cleanup_scheduled_tasks()
    statuses_after_sweep = [run.status.name for run in live_runs]
    release.set()
    await asyncio.sleep(0.01)

    assert statuses_after_sweep == ["RUNNING"] * 5
