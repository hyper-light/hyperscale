"""
The gate tells a network-wide blip from datacenters failing, and lets go
of the blip once it is over (AD-33 Part 6).

The cross-datacenter correlation detector confirms failures, and
recoveries, only over time -- but nothing ever told it a datacenter's
health. It never saw a correlated failure, so a datacenter turned
UNHEALTHY by a blip that hit every datacenter at once was never held at
DEGRADED (AD-36 took every one of them for lost). And had it detected a
partition, it would never have found it healed: nothing asked, so those
datacenters would have stayed held at DEGRADED for good -- a datacenter
that really failed later was never seen as UNHEALTHY.

Now every heartbeat interval the gate samples each datacenter's merged
health into the detector, and checks whether a partition it detected has
healed. Driven through the real coordinator and a real detector under the
production configuration, on a stepped clock.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.slo.latency_slo import LatencySLO
from hyperscale.distributed.slo.slo_health_classifier import SLOHealthClassifier
from hyperscale.distributed.capacity import DatacenterCapacityAggregator
from hyperscale.distributed.datacenters import cross_dc_correlation_detector, dc_state_info
from hyperscale.distributed.datacenters.cross_dc_correlation import CrossDCCorrelationDetector
from hyperscale.distributed.env import Env
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
from hyperscale.distributed.models import DatacenterStatus
from hyperscale.distributed.nodes.gate.health_coordinator import GateHealthCoordinator
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.resources.datacenter_resource_aggregator import (
    DatacenterResourceAggregator,
)
from hyperscale.distributed.slo.resource_aware_predictor import ResourceAwareSLOPredictor

SETTINGS = Env()
CORRELATION = SETTINGS.get_cross_dc_correlation_config()
DATACENTERS = ["dc-a", "dc-b", "dc-c"]
# The gate samples each heartbeat interval.
SAMPLE_INTERVAL_SECONDS = SETTINGS.MANAGER_HEARTBEAT_INTERVAL


class SteppedClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


class TcpHealth:
    """Each datacenter's managers report the health set for it."""

    def __init__(self) -> None:
        self.health = {datacenter: "healthy" for datacenter in DATACENTERS}

    def get_datacenter_health(self, datacenter_id: str) -> DatacenterStatus:
        return DatacenterStatus(
            dc_id=datacenter_id,
            health=self.health[datacenter_id],
            available_capacity=8,
            manager_count=1,
        )


class RecordingTaskRunner:
    def __init__(self) -> None:
        self.runs: list[tuple] = []

    def run(self, call, *args, **kwargs):
        self.runs.append((call, args))


def make_coordinator(clock: SteppedClock, tcp_health: TcpHealth) -> GateHealthCoordinator:
    detector = CrossDCCorrelationDetector(config=CORRELATION)
    for datacenter in DATACENTERS:
        detector.add_datacenter(datacenter)
    return GateHealthCoordinator(
        clock=clock,
        state=GateRuntimeState(forward_throughput_interval_start=0.0),
        logger=SimpleNamespace(log=None),
        task_runner=RecordingTaskRunner(),
        dc_health_manager=tcp_health,
        # No federated probe has a verdict: the TCP view decides.
        dc_health_monitor=SimpleNamespace(get_dc_health=lambda datacenter_id: None),
        cross_dc_correlation=detector,
        track_manager=None,
        versioned_clock=None,
        manager_health_config=None,
        datacenter_managers={
            datacenter: [("10.0.0.1", 9000 + index)] for index, datacenter in enumerate(DATACENTERS)
        },
        get_node_id=lambda: SimpleNamespace(full="gate-1"),
        get_host=lambda: "127.0.0.1",
        get_tcp_port=lambda: 9100,
        confirm_manager_for_dc=None,
        record_manager_heartbeat=None,
        capacity_aggregator=DatacenterCapacityAggregator(
            clock=clock,
            staleness_threshold_seconds=SETTINGS.CAPACITY_STALENESS_THRESHOLD_SECONDS,
        ),
        circuit_breaker_manager=CircuitBreakerManager(SETTINGS, is_peer_suspected=lambda _peer_addr: False),
        resource_aggregator=DatacenterResourceAggregator(
            clock=clock,
            staleness_seconds=SETTINGS.RESOURCE_VIEW_STALENESS_SECONDS,
        ),
        resource_predictor=ResourceAwareSLOPredictor.from_env(SETTINGS),
        latency_slo=LatencySLO.from_env(SETTINGS),
        slo_health_classifier=SLOHealthClassifier.from_env(SETTINGS),
    )


def routing_view(coordinator: GateHealthCoordinator) -> dict[str, str]:
    return {
        candidate.datacenter_id: candidate.health_bucket
        for candidate in coordinator.build_datacenter_candidates(DATACENTERS)
    }


def run_for(coordinator: GateHealthCoordinator, clock: SteppedClock, seconds: float) -> None:
    """The gate's sampling loop, for ``seconds``: a sample, then the
    routing decisions of that interval."""
    elapsed = 0.0
    while elapsed < seconds:
        coordinator.sample_datacenter_correlation()
        routing_view(coordinator)
        clock.now += SAMPLE_INTERVAL_SECONDS
        elapsed += SAMPLE_INTERVAL_SECONDS


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> SteppedClock:
    stepped_clock = SteppedClock()
    # The detector reads its module's clock.
    # The correlation classes read the clock where each is defined.
    for correlation_class_module in (cross_dc_correlation_detector, dc_state_info):
        monkeypatch.setattr(correlation_class_module, "_DEFAULT_CLOCK", stepped_clock)
    return stepped_clock


def test_a_network_wide_blip_is_held_and_then_let_go(clock: SteppedClock) -> None:
    tcp_health = TcpHealth()
    coordinator = make_coordinator(clock, tcp_health)
    run_for(coordinator, clock, SAMPLE_INTERVAL_SECONDS)

    # Every datacenter turns UNHEALTHY at once, past the confirmation.
    tcp_health.health = {datacenter: "unhealthy" for datacenter in DATACENTERS}
    run_for(coordinator, clock, 2 * CORRELATION.failure_confirmation_seconds + SAMPLE_INTERVAL_SECONDS)
    during_blip = routing_view(coordinator)
    in_partition = coordinator.is_in_partition()

    # The blip ends: every datacenter recovers, past the recovery
    # confirmation and the correlation backoff.
    tcp_health.health = {datacenter: "healthy" for datacenter in DATACENTERS}
    run_for(
        coordinator,
        clock,
        CORRELATION.recovery_confirmation_seconds
        + CORRELATION.correlation_backoff_seconds
        + 2 * SAMPLE_INTERVAL_SECONDS,
    )
    healed = not coordinator.is_in_partition()

    # Later one datacenter really fails, alone.
    tcp_health.health["dc-b"] = "unhealthy"
    run_for(coordinator, clock, SAMPLE_INTERVAL_SECONDS)
    after_one_fails = routing_view(coordinator)

    assert in_partition
    assert during_blip == {datacenter: "DEGRADED" for datacenter in DATACENTERS}
    assert healed
    assert after_one_fails == {"dc-a": "HEALTHY", "dc-b": "UNHEALTHY", "dc-c": "HEALTHY"}
