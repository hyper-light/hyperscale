"""
core/jobs Task ids are monotone, unique, and injected — not uuid4.

``Task.create_id`` previously returned ``uuid.uuid4().int >> 64`` —
random, which silently broke ``Task.latest()`` (``max(self._runs)``)
and the count-eviction policy (``sorted(self._runs)``), both of which
assume larger id => later run. Ids now come from the constructing
TaskRunner's ``SnowflakeGenerator`` (monotone, borrow-not-spin), which
is a REQUIRED constructor dependency — no module-level fallback global.
"""

import asyncio

import pytest

from hyperscale.core.jobs.tasks.task_hook import Task
from hyperscale.core.snowflake.snowflake_generator import SnowflakeGenerator


class _HookStub:
    """Minimal shape of a task hook: the attributes Task.__init__ reads."""

    name = "stub-task"
    schedule = None
    trigger = "MANUAL"
    repeat = "NEVER"
    timeout = None
    keep = 2
    max_age = None
    keep_policy = "COUNT"

    def __call__(self) -> None:
        return None


@pytest.fixture
def event_loop_context():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


def test_task_ids_are_strictly_monotone_and_unique(event_loop_context):
    task = Task(_HookStub(), SnowflakeGenerator(0))
    ids = [task.create_id() for _ in range(200)]
    assert task.task_id < ids[0]
    assert ids == sorted(ids), "ids must be strictly increasing"
    assert len(set(ids)) == len(ids), "ids must be unique"


def test_latest_returns_the_most_recently_created_run(event_loop_context):
    """The headline fix: with monotone ids, ``max(self._runs)`` is the
    latest-created run. Random uuid ids made this return an arbitrary run.
    """
    task = Task(_HookStub(), SnowflakeGenerator(0))
    created_run_ids = []
    for index in range(5):
        run_id = task.create_id()
        task._runs[run_id] = f"run-{index}"
        created_run_ids.append(run_id)

    assert task.latest() == f"run-{len(created_run_ids) - 1}"


def test_id_generator_is_a_required_dependency(event_loop_context):
    """No module-global fallback: constructing a Task without its
    runner-owned generator must fail loudly at construction."""
    with pytest.raises(TypeError):
        Task(_HookStub())
