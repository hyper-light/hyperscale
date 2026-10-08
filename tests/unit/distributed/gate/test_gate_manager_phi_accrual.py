"""
A gate suspects a datacenter manager by a phi-accrual detector over its
heartbeats (AD-52 section 8), not a fixed staleness window -- driven through
the real ``DatacenterHealthManager`` under the production configuration, on
a stepped clock.

The fixed window it replaced held a manager alive for 30s after its last
heartbeat, whatever its heartbeats had been like: six heartbeat intervals of
a dead manager still routed to. Phi learns each edge's arrivals:

* heartbeats that come like clockwork and stop are suspected within the
  interval, the tolerated pause and a few deviations -- never sooner;
* heartbeats with wide, honest jitter are never suspected while they keep
  coming, and still are once they stop -- later, by as much as their own
  spread warrants.
"""

import random

import pytest

from hyperscale.distributed.datacenters import datacenter_health_manager
from hyperscale.distributed.datacenters.datacenter_health_manager import DatacenterHealthManager
from hyperscale.distributed.env import Env
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
from hyperscale.distributed.health.phi_accrual_config import PhiAccrualConfig
from hyperscale.distributed.models import DatacenterHealth, ManagerHeartbeat

SETTINGS = Env()
PHI = PhiAccrualConfig.for_manager_heartbeats(SETTINGS)
DATACENTER = "dc-1"
MANAGER_ADDR = ("10.0.0.1", 8080)
# The window the detector replaced.
FIXED_STALENESS_SECONDS = 30.0
# How finely the test reads health after the last heartbeat.
READ_STEP_SECONDS = 0.05


class SteppedClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> SteppedClock:
    stepped_clock = SteppedClock()
    monkeypatch.setattr(datacenter_health_manager, "_DEFAULT_CLOCK", stepped_clock)
    return stepped_clock


def heartbeat(version: int) -> ManagerHeartbeat:
    return ManagerHeartbeat(
        node_id="manager-1",
        datacenter=DATACENTER,
        is_leader=True,
        term=1,
        version=version,
        active_jobs=0,
        active_workflows=0,
        worker_count=4,
        healthy_worker_count=4,
        available_cores=32,
        total_cores=40,
    )


def is_routable(health_manager: DatacenterHealthManager) -> bool:
    return health_manager.get_datacenter_health(DATACENTER).health == DatacenterHealth.HEALTHY.value


def run_heartbeats(
    health_manager: DatacenterHealthManager, clock: SteppedClock, intervals: list[float]
) -> list[float]:
    """Deliver a heartbeat after each interval, reading health just before
    each arrival; the arrival gaps the manager was suspected in."""
    suspected_gaps = []
    for version, interval in enumerate(intervals, start=1):
        clock.now += interval
        if not is_routable(health_manager):
            suspected_gaps.append(interval)
        health_manager.update_manager(DATACENTER, MANAGER_ADDR, heartbeat(version))
    return suspected_gaps


def seconds_until_suspected(health_manager: DatacenterHealthManager, clock: SteppedClock) -> float:
    stopped_at = clock.now
    while is_routable(health_manager):
        clock.now += READ_STEP_SECONDS
        assert clock.now - stopped_at < 10 * FIXED_STALENESS_SECONDS, "never suspected"
    return clock.now - stopped_at


def test_clockwork_heartbeats_that_stop_are_suspected_well_inside_the_fixed_window(clock: SteppedClock) -> None:
    health_manager = DatacenterHealthManager(PHI)
    health_manager.update_manager(DATACENTER, MANAGER_ADDR, heartbeat(0))

    suspected_gaps = run_heartbeats(health_manager, clock, [SETTINGS.MANAGER_HEARTBEAT_INTERVAL] * 200)
    detected_after = seconds_until_suspected(health_manager, clock)

    assert suspected_gaps == []
    # Never before the interval plus the tolerated pause...
    assert detected_after > SETTINGS.MANAGER_HEARTBEAT_INTERVAL + PHI.acceptable_heartbeat_pause_seconds
    # ...and well inside the window it replaced.
    assert detected_after < FIXED_STALENESS_SECONDS / 2


def test_jittery_heartbeats_are_never_suspected_while_they_keep_coming(clock: SteppedClock) -> None:
    """Arrivals spread from a fifth of the interval to nearly twice it --
    an honest, noisy edge."""
    health_manager = DatacenterHealthManager(PHI)
    health_manager.update_manager(DATACENTER, MANAGER_ADDR, heartbeat(0))
    interval = SETTINGS.MANAGER_HEARTBEAT_INTERVAL
    jitter = random.Random(52)
    intervals = [jitter.uniform(0.2 * interval, 1.8 * interval) for _ in range(2000)]

    suspected_gaps = run_heartbeats(health_manager, clock, intervals)
    detected_after = seconds_until_suspected(health_manager, clock)

    assert suspected_gaps == []
    # Its own spread earns it more time than a clockwork edge, and the
    # longest gap it actually showed passes unsuspected.
    assert detected_after > max(intervals)


async def test_a_suspected_managers_circuit_is_open_until_it_is_heard_again(clock: SteppedClock) -> None:
    """Phi accrual's breaker consumer (AD-52 section 8): a manager whose
    heartbeats stopped has its circuit OPEN -- requests to it fail fast and
    routing counts it out -- as soon as phi suspects it, with no request
    errors needed; a heartbeat closes it again. A manager never heard from
    is unknown, never suspected."""
    health_manager = DatacenterHealthManager(PHI)
    breakers = CircuitBreakerManager(SETTINGS, is_peer_suspected=health_manager.is_manager_suspected)
    unheard_manager = ("10.0.0.9", 8080)
    health_manager.update_manager(DATACENTER, MANAGER_ADDR, heartbeat(0))
    run_heartbeats(health_manager, clock, [SETTINGS.MANAGER_HEARTBEAT_INTERVAL] * 200)

    assert not await breakers.is_circuit_open(MANAGER_ADDR)
    assert not await breakers.is_circuit_open(unheard_manager)

    seconds_until_suspected(health_manager, clock)
    assert await breakers.is_circuit_open(MANAGER_ADDR)
    assert breakers.count_open_circuits([MANAGER_ADDR, unheard_manager]) == 1
    assert not await breakers.is_circuit_open(unheard_manager)

    health_manager.update_manager(DATACENTER, MANAGER_ADDR, heartbeat(201))
    assert not await breakers.is_circuit_open(MANAGER_ADDR)
    assert breakers.count_open_circuits([MANAGER_ADDR, unheard_manager]) == 0
