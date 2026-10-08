"""
A task's schedules on the core TaskRunner.

* A stop names the schedule it stops and stops only that one: a stop
  without a run id once ended every schedule of the task -- the way one
  workflow completing ended every workflow's status aggregation.
* A schedule that ends -- stopped or cancelled -- leaves neither its future
  nor its running flag behind (every finished schedule kept its future).
* A stop for a schedule that already ended leaves nothing behind (it set the
  flag again, and the flag stayed).
"""

import asyncio
import collections

from hyperscale.core.jobs.hooks import task
from hyperscale.core.jobs.models.env import Env
from hyperscale.core.jobs.tasks.task_runner import TaskRunner

SCHEDULE_SECONDS = 0.01
# Several repetitions of the schedule.
SETTLE_SECONDS = SCHEDULE_SECONDS * 5


class Pinger:
    def __init__(self) -> None:
        self.calls: collections.Counter[str] = collections.Counter()

    @task(trigger="MANUAL", repeat="ALWAYS", schedule=SCHEDULE_SECONDS, keep=10, keep_policy="COUNT")
    async def ping(self, name: str) -> None:
        self.calls[name] += 1


def make_tasks(pinger: Pinger) -> TaskRunner:
    tasks = TaskRunner(1, Env())
    tasks.add(pinger.ping)
    return tasks


async def test_a_stop_ends_only_its_own_schedule_and_leaves_nothing_behind() -> None:
    pinger = Pinger()
    tasks = make_tasks(pinger)
    ping = tasks.tasks["ping"]
    tasks.run("ping", "first", run_id=101)
    tasks.run("ping", "second", run_id=102)
    await asyncio.sleep(SETTLE_SECONDS)

    tasks.stop("ping", 101)
    await asyncio.sleep(SETTLE_SECONDS)
    first_calls, second_calls = pinger.calls["first"], pinger.calls["second"]
    await asyncio.sleep(SETTLE_SECONDS)

    assert pinger.calls["first"] == first_calls  # stopped
    assert pinger.calls["second"] > second_calls  # still running
    assert 101 not in ping._schedules
    assert 101 not in ping._schedule_running_statuses

    tasks.stop("ping", 102)
    await asyncio.sleep(SETTLE_SECONDS)

    assert ping._schedules == {}
    assert not ping._schedule_running_statuses


async def test_a_stop_for_a_schedule_that_ended_leaves_nothing_behind() -> None:
    pinger = Pinger()
    tasks = make_tasks(pinger)
    ping = tasks.tasks["ping"]
    tasks.run("ping", "only", run_id=201)
    await asyncio.sleep(SETTLE_SECONDS)
    tasks.stop("ping", 201)
    await asyncio.sleep(SETTLE_SECONDS)

    tasks.stop("ping", 201)

    assert 201 not in ping._schedule_running_statuses
    assert ping._schedules == {}


async def test_a_cancelled_schedule_leaves_nothing_behind() -> None:
    pinger = Pinger()
    tasks = make_tasks(pinger)
    ping = tasks.tasks["ping"]
    tasks.run("ping", "only", run_id=301)
    await asyncio.sleep(SETTLE_SECONDS)

    await ping.cancel_schedule()

    assert ping._schedules == {}
    assert not ping._schedule_running_statuses
