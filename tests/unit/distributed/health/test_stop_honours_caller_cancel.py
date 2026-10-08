"""
Stopping a component cancels its task and waits for it to end -- and that
wait swallows only the cancel it caused.

* A cancel aimed at the stopping task while it waits goes on: swallowed,
  the stopping task would carry on as if never cancelled (a Ctrl+C lost
  mid-shutdown).
* A stop run by a task already being cancelled -- a shutdown in a
  cancelled task's cleanup -- still finishes, and the cleanup after it
  still runs: re-raising for a cancel that predates the wait would skip
  every step after the first.

Driven through a real ``FederatedHealthMonitor`` (its probe loop) on the
running loop; every site that cancels and awaits its own task holds the
same rule.
"""

import asyncio

from hyperscale.distributed.swim.health.federated_health_monitor import FederatedHealthMonitor


def started_probe(name: str) -> FederatedHealthMonitor:
    """A monitor watching no datacenter: its probe loop only waits."""
    return FederatedHealthMonitor()


async def test_a_cancel_aimed_at_the_stopping_task_while_it_waits_goes_on() -> None:
    probe = started_probe("probe")
    await probe.start()
    await asyncio.sleep(0)

    stopping_task = asyncio.get_running_loop().create_task(probe.stop())
    # Let it cancel the probe's task and start waiting on it...
    await asyncio.sleep(0)
    # ...then cancel the stopping task itself mid-wait.
    stopping_task.cancel()
    await asyncio.wait({stopping_task})

    assert stopping_task.cancelled(), "the stop swallowed a cancel aimed at its own task"


async def test_a_stop_in_an_already_cancelled_task_still_finishes_its_cleanup() -> None:
    first_probe = started_probe("first")
    second_probe = started_probe("second")
    await first_probe.start()
    await second_probe.start()
    await asyncio.sleep(0)
    stopped: list[str] = []

    async def run_until_cancelled_then_clean_up() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # Cleanup in the cancelled task: both stops must run.
            await first_probe.stop()
            stopped.append("first")
            await second_probe.stop()
            stopped.append("second")
            raise

    owner_task = asyncio.get_running_loop().create_task(run_until_cancelled_then_clean_up())
    await asyncio.sleep(0)
    owner_task.cancel()
    await asyncio.wait({owner_task})

    assert stopped == ["first", "second"], stopped
    assert owner_task.cancelled()
