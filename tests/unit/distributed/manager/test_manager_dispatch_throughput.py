"""
Manager dispatch throughput (AD-19), owned by ManagerStatsCoordinator.

The manager advertised its dispatch throughput in every heartbeat, but
nothing live counted a dispatch (only the never-called dispatch
coordinator did), so it was always 0. Dispatches a worker accepts are
now recorded, and throughput is read on the node's injected clock (the
coordinator used the module-global real clock, not SIM-safe).

* within an interval, throughput is dispatches over elapsed time;
* at the interval boundary the interval's rate becomes the advertised
  value and counting restarts;
* expected throughput follows the healthy worker count.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.env import Env
from hyperscale.distributed.slo import SLOConfig
from hyperscale.distributed.nodes.manager.stats import ManagerStatsCoordinator

INTERVAL_SECONDS = 10.0


class SteppedClock:
    def __init__(self) -> None:
        self.now = 100.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now


def make_stats(healthy_workers: list[int]) -> tuple[ManagerStatsCoordinator, SteppedClock]:
    clock = SteppedClock()
    stats = ManagerStatsCoordinator(
        state=ManagerState(slo_config=SLOConfig.from_env(Env())),
        config=SimpleNamespace(throughput_interval_seconds=INTERVAL_SECONDS),
        logger=None,
        node_id="manager-a",
        task_runner=None,
        stats_buffer=None,
        windowed_stats=None,
        clock=clock,
        get_healthy_worker_count=lambda: healthy_workers[0],
    )
    return stats, clock


@pytest.mark.asyncio
async def test_throughput_counts_recorded_dispatches_over_the_interval() -> None:
    stats, clock = make_stats([2])
    await stats.refresh_dispatch_throughput()  # opens the interval at t=100

    for _ in range(5):
        await stats.record_dispatch()
    clock.now += 2.0
    assert stats.get_dispatch_throughput() == pytest.approx(5 / 2.0)

    clock.now += INTERVAL_SECONDS - 2.0
    assert await stats.refresh_dispatch_throughput() == pytest.approx(5 / INTERVAL_SECONDS)
    assert stats.get_dispatch_throughput() == pytest.approx(5 / INTERVAL_SECONDS)

    clock.now += 1.0  # a fresh interval with nothing dispatched yet
    assert stats.get_dispatch_throughput() == 0.0


def test_expected_throughput_follows_healthy_workers() -> None:
    healthy = [3]
    stats, _clock = make_stats(healthy)
    assert stats.get_expected_throughput() == 3.0
    healthy[0] = 0
    assert stats.get_expected_throughput() == 0.0
