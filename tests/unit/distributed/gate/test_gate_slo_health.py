"""
AD-42: a datacenter's health is the worse of what its managers report and
what its latency SLO compliance shows.

``SLOHealthClassifier`` was built and never used: a datacenter whose
workflows ran at many times their latency targets for minutes still read
HEALTHY to the gate. Now the gate's one classification grades it down
by its SLO -- only after the violation has lasted the configured window,
never up, not on too few observations -- and back up once it complies.

Driven through the real GateHealthCoordinator over a real runtime state
fed managers' heartbeat SLO fields, on a stepped clock.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.capacity import DatacenterCapacityAggregator
from hyperscale.distributed.env import Env
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
from hyperscale.distributed.models import DatacenterStatus
from hyperscale.distributed.nodes.gate.health_coordinator import GateHealthCoordinator
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.slo.latency_slo import LatencySLO
from hyperscale.distributed.slo.slo_config import SLOConfig
from hyperscale.distributed.slo.slo_health_classifier import SLOHealthClassifier
from hyperscale.distributed.swim.health.federated_health_monitor import DCReachability

DATACENTER = "dc-a"
MANAGER = ("10.0.0.5", 9000)
SETTINGS = Env()
SLO_CONFIG = SLOConfig.from_env(SETTINGS)


class SteppedClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now


class ManagersReport:
    def __init__(self, health: str) -> None:
        self.health = health

    def get_datacenter_health(self, datacenter_id: str) -> DatacenterStatus:
        return DatacenterStatus(dc_id=datacenter_id, health=self.health, available_capacity=8, manager_count=1)


class Reachable:
    def get_dc_health(self, datacenter_id: str):
        return SimpleNamespace(reachability=DCReachability.REACHABLE, last_ack=None)


def make_coordinator(clock: SteppedClock, managers_health: str = "healthy") -> tuple[GateHealthCoordinator, GateRuntimeState]:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    coordinator = GateHealthCoordinator(
        clock=clock,
        state=state,
        logger=None,
        task_runner=None,
        dc_health_manager=ManagersReport(managers_health),
        dc_health_monitor=Reachable(),
        cross_dc_correlation=SimpleNamespace(
            register_partition_healed_callback=lambda callback: None,
            register_partition_detected_callback=lambda callback: None,
        ),
        track_manager=None,
        versioned_clock=None,
        manager_health_config=None,
        datacenter_managers={DATACENTER: [MANAGER]},
        get_node_id=None,
        get_host=None,
        get_tcp_port=None,
        confirm_manager_for_dc=None,
        record_manager_heartbeat=None,
        capacity_aggregator=DatacenterCapacityAggregator(
            clock=clock, staleness_threshold_seconds=SETTINGS.CAPACITY_STALENESS_THRESHOLD_SECONDS
        ),
        circuit_breaker_manager=CircuitBreakerManager(SETTINGS, is_peer_suspected=lambda _peer_addr: False),
        resource_aggregator=None,
        resource_predictor=None,
        latency_slo=LatencySLO.from_env(SETTINGS),
        slo_health_classifier=SLOHealthClassifier.from_env(SETTINGS),
    )
    return coordinator, state


def report_latency(state: GateRuntimeState, clock: SteppedClock, p95_ms: float, sample_count: int) -> None:
    """The DC's manager reports its latency percentiles (heartbeat SLO fields)."""
    state._datacenter_manager_status.setdefault(DATACENTER, {})[MANAGER] = SimpleNamespace(
        slo_p50_ms=SLO_CONFIG.p50_target_ms,
        slo_p95_ms=p95_ms,
        slo_p99_ms=SLO_CONFIG.p99_target_ms,
        slo_sample_count=sample_count,
        slo_updated_at=clock.now,
    )


def test_a_sustained_violation_degrades_the_datacenter_only_after_its_window() -> None:
    clock = SteppedClock()
    coordinator, state = make_coordinator(clock)
    violating_p95 = SLO_CONFIG.p95_target_ms * SLO_CONFIG.degraded_p95_ratio

    report_latency(state, clock, violating_p95, SLO_CONFIG.min_sample_count)
    assert coordinator.classify_datacenter_health(DATACENTER).health == "healthy"  # it just began

    clock.now += SLO_CONFIG.degraded_window_seconds / 2
    report_latency(state, clock, violating_p95, SLO_CONFIG.min_sample_count)
    assert coordinator.classify_datacenter_health(DATACENTER).health == "healthy"

    clock.now += SLO_CONFIG.degraded_window_seconds / 2
    report_latency(state, clock, violating_p95, SLO_CONFIG.min_sample_count)
    assert coordinator.classify_datacenter_health(DATACENTER).health == "degraded"

    # Compliant again: healthy at once.
    report_latency(state, clock, SLO_CONFIG.p95_target_ms, SLO_CONFIG.min_sample_count)
    assert coordinator.classify_datacenter_health(DATACENTER).health == "healthy"


def test_too_few_observations_never_grade_a_datacenter() -> None:
    clock = SteppedClock()
    coordinator, state = make_coordinator(clock)
    violating_p95 = SLO_CONFIG.p95_target_ms * SLO_CONFIG.degraded_p95_ratio * 10

    for _ in range(3):
        report_latency(state, clock, violating_p95, SLO_CONFIG.min_sample_count - 1)
        assert coordinator.classify_datacenter_health(DATACENTER).health == "healthy"
        clock.now += SLO_CONFIG.degraded_window_seconds


@pytest.mark.parametrize("managers_health", ["degraded", "unhealthy"])
def test_compliance_never_grades_a_datacenter_up(managers_health: str) -> None:
    clock = SteppedClock()
    coordinator, state = make_coordinator(clock, managers_health)

    report_latency(state, clock, SLO_CONFIG.p95_target_ms, SLO_CONFIG.min_sample_count)

    assert coordinator.classify_datacenter_health(DATACENTER).health == managers_health
