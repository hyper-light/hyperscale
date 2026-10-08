"""
A datacenter's capacity and routing inputs, as the gate builds them.

Every manager tracks every worker (workers register with each manager,
AD-48), so each manager heartbeat reports the whole datacenter's cores --
the gate summed them, multiplying a datacenter by its manager count
(spillover saw three-manager datacenters as three times their size and
their waits as a third). The store kept up to 10,000 heartbeats with
stale ones pruned only on read. The router's candidates carried
hard-coded circuit pressure (0.0) and healthy-manager counts, a federated
suspicion reset a datacenter's overload severity to neutral, and a
datacenter's queue depth was an attribute no heartbeat carries.

* cores come from the most authoritative heartbeat (the leader with the
  highest term, else the most recent); pending and active work, counted
  per job leader, is summed;
* stale heartbeats age out on every read and write, so the store holds
  only managers heard from within the staleness threshold;
* candidates carry the gate's open-circuit share and their merged health
  keeps its overload severity.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.slo.latency_slo import LatencySLO
from hyperscale.distributed.slo.slo_health_classifier import SLOHealthClassifier
from hyperscale.distributed.capacity import DatacenterCapacityAggregator
from hyperscale.distributed.capacity.active_dispatch import ActiveDispatch
from hyperscale.distributed.capacity.execution_time_estimator import ExecutionTimeEstimator
from hyperscale.distributed.env import Env
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
from hyperscale.distributed.models import DatacenterStatus, ManagerHeartbeat
from hyperscale.distributed.nodes.gate.health_coordinator import GateHealthCoordinator
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.resources.datacenter_resource_aggregator import (
    DatacenterResourceAggregator,
)
from hyperscale.distributed.slo.resource_aware_predictor import ResourceAwareSLOPredictor
from hyperscale.distributed.swim.health.federated_health_monitor import DCReachability

STALENESS_SECONDS = 30.0
DATACENTER = "dc-east"
MANAGERS = [("10.0.0.1", 9000), ("10.0.0.2", 9000), ("10.0.0.3", 9000)]


class SteppedClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


def heartbeat(
    node_id: str,
    is_leader: bool = False,
    term: int = 1,
    available_cores: int = 12,
    total_cores: int = 30,
    pending_workflow_count: int = 0,
    pending_duration_seconds: float = 0.0,
    active_remaining_seconds: float = 0.0,
    datacenter: str = DATACENTER,
    cores_freeing_schedule: list[tuple[float, int]] | None = None,
) -> ManagerHeartbeat:
    return ManagerHeartbeat(
        node_id=node_id,
        datacenter=datacenter,
        is_leader=is_leader,
        term=term,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=3,
        healthy_worker_count=3,
        available_cores=available_cores,
        total_cores=total_cores,
        pending_workflow_count=pending_workflow_count,
        pending_duration_seconds=pending_duration_seconds,
        active_remaining_seconds=active_remaining_seconds,
        cores_freeing_schedule=cores_freeing_schedule or [],
    )


def make_aggregator(clock: SteppedClock) -> DatacenterCapacityAggregator:
    return DatacenterCapacityAggregator(clock=clock, staleness_threshold_seconds=STALENESS_SECONDS)


def test_a_datacenters_cores_are_its_authoritative_managers_not_their_sum() -> None:
    aggregator = make_aggregator(SteppedClock())
    aggregator.record_heartbeat(heartbeat("manager-a", available_cores=12, pending_workflow_count=1))
    aggregator.record_heartbeat(
        heartbeat("manager-b", is_leader=True, available_cores=10, pending_workflow_count=2)
    )
    aggregator.record_heartbeat(heartbeat("manager-c", available_cores=11, pending_workflow_count=3))

    capacity = aggregator.get_capacity(DATACENTER)

    assert (capacity.total_cores, capacity.available_cores) == (30, 10)
    assert capacity.pending_workflow_count == 6


def test_the_leader_with_the_highest_term_is_authoritative() -> None:
    aggregator = make_aggregator(SteppedClock())
    aggregator.record_heartbeat(heartbeat("manager-new", is_leader=True, term=4, available_cores=7))
    aggregator.record_heartbeat(heartbeat("manager-deposed", is_leader=True, term=3, available_cores=29))

    assert aggregator.get_capacity(DATACENTER).available_cores == 7


def test_without_a_leader_the_most_recent_heartbeat_is_authoritative() -> None:
    clock = SteppedClock()
    aggregator = make_aggregator(clock)
    aggregator.record_heartbeat(heartbeat("manager-a", available_cores=3))
    clock.now += 1.0
    aggregator.record_heartbeat(heartbeat("manager-b", available_cores=5))

    assert aggregator.get_capacity(DATACENTER).available_cores == 5


def test_the_spillover_wait_drains_the_backlog_over_the_datacenters_cores() -> None:
    aggregator = make_aggregator(SteppedClock())
    for node_id in ("manager-a", "manager-b", "manager-c"):
        aggregator.record_heartbeat(
            heartbeat(
                node_id,
                available_cores=0,
                pending_duration_seconds=30.0,
                active_remaining_seconds=60.0,
            )
        )

    capacity = aggregator.get_capacity(DATACENTER)

    assert capacity.estimated_wait_for_cores(1) == pytest.approx((3 * 30.0 + 3 * 60.0) / 30)


def test_a_job_waits_for_the_cores_long_dispatches_hold_not_their_average() -> None:
    """AD-43 Part 4: one workflow holding every core for a hundred seconds
    frees them in a hundred -- the drain bound alone read it as ten
    seconds of work on ten cores."""
    aggregator = make_aggregator(SteppedClock())
    aggregator.record_heartbeat(
        heartbeat(
            "manager-a",
            is_leader=True,
            available_cores=0,
            total_cores=10,
            active_remaining_seconds=100.0,
            cores_freeing_schedule=[(100.0, 10)],
        )
    )

    assert aggregator.get_capacity(DATACENTER).estimated_wait_for_cores(10) == pytest.approx(100.0)


def test_cores_freed_before_the_authoritative_report_are_not_counted_twice() -> None:
    clock = SteppedClock(now=1000.0)
    aggregator = make_aggregator(clock)
    # A follower reports releases 2 and 20 seconds out...
    aggregator.record_heartbeat(
        heartbeat("manager-a", available_cores=0, total_cores=12, cores_freeing_schedule=[(2.0, 4), (20.0, 4)])
    )
    # ...and the leader reports later, its free cores counting the first.
    clock.now = 1005.0
    aggregator.record_heartbeat(heartbeat("manager-b", is_leader=True, available_cores=4, total_cores=12))
    clock.now = 1006.0

    capacity = aggregator.get_capacity(DATACENTER)

    assert capacity.release_schedule == ((pytest.approx(14.0), 4),)
    assert capacity.estimated_wait_for_cores(8) == pytest.approx(14.0)


def test_a_job_wanting_more_than_the_datacenter_has_waits_for_all_of_it() -> None:
    clock = SteppedClock()
    aggregator = make_aggregator(clock)
    aggregator.record_heartbeat(
        heartbeat(
            "manager-a",
            is_leader=True,
            available_cores=0,
            total_cores=8,
            cores_freeing_schedule=[(5.0, 4), (12.0, 4)],
        )
    )
    busy = aggregator.get_capacity(DATACENTER)
    aggregator.record_heartbeat(heartbeat("manager-a", is_leader=True, available_cores=8, total_cores=8))
    idle = aggregator.get_capacity(DATACENTER)

    assert busy.estimated_wait_for_cores(20) == pytest.approx(12.0)
    assert idle.can_serve_immediately(20)


def test_a_manager_reports_when_its_dispatches_free_their_cores() -> None:
    """Released at the workflow's duration; running past it, by its timeout
    at the latest; past both, now."""
    clock = SteppedClock(now=500.0)

    def dispatch(dispatched_at: float, cores: int) -> ActiveDispatch:
        return ActiveDispatch(
            workflow_id="workflow",
            job_id="job",
            worker_id="worker",
            cores_allocated=cores,
            dispatched_at=dispatched_at,
            duration_seconds=30.0,
            timeout_seconds=60.0,
        )

    estimator = ExecutionTimeEstimator(
        active_dispatches={
            "running": dispatch(dispatched_at=490.0, cores=2),
            "overrunning": dispatch(dispatched_at=460.0, cores=3),
            "past-its-timeout": dispatch(dispatched_at=400.0, cores=5),
        },
        pending_workflows={},
        total_cores=10,
        clock=clock,
    )

    assert estimator.get_release_schedule() == [(0.0, 5), (20.0, 2), (20.0, 3)]


def test_stale_heartbeats_age_out_and_bound_the_store() -> None:
    clock = SteppedClock()
    aggregator = make_aggregator(clock)
    for restart in range(500):
        aggregator.record_heartbeat(heartbeat(f"manager-incarnation-{restart}"))
        clock.now += STALENESS_SECONDS / 10

    assert len(aggregator._manager_heartbeats) <= 11


def test_a_datacenter_without_fresh_heartbeats_has_unknown_stale_capacity() -> None:
    clock = SteppedClock()
    aggregator = make_aggregator(clock)
    aggregator.record_heartbeat(heartbeat("manager-a"))
    clock.now += STALENESS_SECONDS + 1.0

    capacity = aggregator.get_capacity(DATACENTER)

    assert (capacity.total_cores, capacity.available_cores, capacity.pending_workflow_count) == (0, 0, 0)
    assert capacity.is_stale(clock.now, STALENESS_SECONDS)


def test_a_non_positive_staleness_threshold_is_refused() -> None:
    with pytest.raises(ValueError):
        DatacenterCapacityAggregator(clock=SteppedClock(), staleness_threshold_seconds=0.0)


class SeverelyOverloadedOverTcp:
    """The TCP view: healthy, with an AD-17 overload severity."""

    def known_datacenters(self) -> frozenset[str]:
        return frozenset({DATACENTER})

    def get_datacenter_health(self, datacenter_id: str) -> DatacenterStatus:
        return DatacenterStatus(
            dc_id=datacenter_id,
            health="healthy",
            available_capacity=10,
            manager_count=len(MANAGERS),
            health_severity_weight=1.7,
        )


class Probes:
    def __init__(self, reachability: DCReachability) -> None:
        self.reachability = reachability

    def get_dc_health(self, datacenter_id: str) -> SimpleNamespace:
        return SimpleNamespace(reachability=self.reachability, last_ack=None)


def make_health_coordinator(
    clock: SteppedClock,
    reachability: DCReachability,
    breakers: CircuitBreakerManager,
    aggregator: DatacenterCapacityAggregator,
) -> GateHealthCoordinator:
    settings = Env()
    return GateHealthCoordinator(
        clock=clock,
        state=GateRuntimeState(forward_throughput_interval_start=0.0),
        logger=None,
        task_runner=None,
        dc_health_manager=SeverelyOverloadedOverTcp(),
        dc_health_monitor=Probes(reachability),
        cross_dc_correlation=SimpleNamespace(
            register_partition_healed_callback=lambda callback: None,
            register_partition_detected_callback=lambda callback: None,
        ),
        track_manager=None,
        versioned_clock=None,
        manager_health_config=None,
        datacenter_managers={DATACENTER: list(MANAGERS)},
        get_node_id=None,
        get_host=None,
        get_tcp_port=None,
        confirm_manager_for_dc=None,
        record_manager_heartbeat=None,
        capacity_aggregator=aggregator,
        circuit_breaker_manager=breakers,
        resource_aggregator=DatacenterResourceAggregator(
            clock=clock,
            staleness_seconds=settings.RESOURCE_VIEW_STALENESS_SECONDS,
        ),
        resource_predictor=ResourceAwareSLOPredictor.from_env(settings),
        latency_slo=LatencySLO.from_env(settings),
        slo_health_classifier=SLOHealthClassifier.from_env(settings),
    )


async def open_circuit(breakers: CircuitBreakerManager, manager_address: tuple[str, int]) -> None:
    for _ in range(Env().CIRCUIT_BREAKER_MAX_ERRORS):
        await breakers.record_failure(manager_address)


@pytest.mark.asyncio
async def test_candidates_carry_the_gates_open_circuit_share() -> None:
    clock = SteppedClock()
    breakers = CircuitBreakerManager(Env(), is_peer_suspected=lambda _peer_addr: False)
    aggregator = make_aggregator(clock)
    aggregator.record_heartbeat(heartbeat("manager-a", is_leader=True, available_cores=10))
    coordinator = make_health_coordinator(clock, DCReachability.REACHABLE, breakers, aggregator)

    await open_circuit(breakers, MANAGERS[0])
    [one_open] = coordinator.build_datacenter_candidates([DATACENTER])

    assert (one_open.total_managers, one_open.healthy_managers) == (3, 2)
    assert one_open.circuit_breaker_pressure == pytest.approx(1 / 3)
    assert (one_open.available_cores, one_open.total_cores) == (10, 30)

    for manager_address in MANAGERS[1:]:
        await open_circuit(breakers, manager_address)
    [all_open] = coordinator.build_datacenter_candidates([DATACENTER])

    assert all_open.healthy_managers == 0
    assert all_open.circuit_breaker_pressure == pytest.approx(1.0)


def test_a_suspected_datacenter_keeps_its_overload_severity() -> None:
    clock = SteppedClock()
    coordinator = make_health_coordinator(
        clock,
        DCReachability.SUSPECTED,
        CircuitBreakerManager(Env(), is_peer_suspected=lambda _peer_addr: False),
        make_aggregator(clock),
    )

    [suspected] = coordinator.build_datacenter_candidates([DATACENTER])

    assert suspected.health_bucket == "DEGRADED"
    assert suspected.health_severity_weight == pytest.approx(1.7)
