"""
A taskex run calls what it was given.

``Run`` re-bound its call: a bound method to its own instance (a no-op), and
a ``functools.partial`` of a bound method through ``partial.__get__`` --
which does not exist before Python 3.14 (AttributeError) and from 3.14
binds like a method, prepending the instance as an extra argument. It then
cached the result on the instance, replacing the instance's method with the
partial for every later caller.

* a partial of a bound method runs with exactly its own arguments;
* a bound method runs with the run's arguments;
* neither leaves anything on the instance.
"""

import asyncio
import functools

import pytest

from hyperscale.distributed.taskex import TaskRunner


class Recorder:
    def __init__(self) -> None:
        self.received: list[tuple[int, ...]] = []

    async def record(self, *values: int) -> tuple[int, ...]:
        self.received.append(values)
        return values


@pytest.mark.asyncio
async def test_partials_and_methods_run_with_their_own_arguments() -> None:
    task_runner = TaskRunner(instance_id=0)
    recorder = Recorder()
    try:
        partial_run = task_runner.run(
            functools.partial(recorder.record, 7),
            8,
            alias="record_partial",
        )
        method_run = task_runner.run(recorder.record, 9, alias="record_method")
        await asyncio.gather(
            task_runner.wait(f"record_partial:{partial_run.run_id}"),
            task_runner.wait(f"record_method:{method_run.run_id}"),
        )

        assert sorted(recorder.received) == [(7, 8), (9,)]
        assert "record" not in vars(recorder)
    finally:
        await task_runner.shutdown()
