"""
A manager heartbeat embedded in SWIM does not unsay its TCP report.

The heartbeat a manager embeds in SWIM leaves out what its UDP budget has
no room for -- its capacity backlog, its workers' health, its SLO
percentiles, its LHM -- and so carries their defaults, at the same state
version as the TCP report that carried the real values. The gate ingested
both into the same stores, so every SWIM heartbeat between two reports
(several a second) zeroed the datacenter's queued work, its overloaded
workers and its SLO: spillover saw no backlog and health saw no
overload. A SWIM heartbeat now carries those fields on from the manager's
last report while that is fresh; once no report is, it says only what
SWIM knows.

Also pinned: the fields the gate carries on are exactly those the manager's
TCP report sets and its SWIM embed does not (the resource report aside --
a SWIM heartbeat's None already leaves the last one in place), so a field
added to one builder and not the other fails here until its handling is
decided.
"""

import inspect
import re
from types import SimpleNamespace

import pytest

from hyperscale.distributed.slo.latency_slo import LatencySLO
from hyperscale.distributed.slo.slo_health_classifier import SLOHealthClassifier
from hyperscale.distributed.capacity import DatacenterCapacityAggregator
from hyperscale.distributed.env import Env
from hyperscale.distributed.health import ManagerHealthConfig
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
from hyperscale.distributed.models import ManagerHeartbeat
from hyperscale.distributed.nodes.gate.health_coordinator import (
    TCP_REPORTED_MANAGER_FIELDS,
    GateHealthCoordinator,
)
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.server.events.lamport_clock import VersionedStateClock
from hyperscale.distributed.swim.core.state_embedder import ManagerStateEmbedder

SETTINGS = Env()
DATACENTER = "dc-a"
MANAGER_TCP_ADDRESS = ("10.0.0.5", 9000)
MANAGER_UDP_ADDRESS = ("10.0.0.5", 9001)


class SteppedClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


class StoredHeartbeats:
    """The datacenter health manager's view: the last heartbeat stored
    for each manager."""

    def __init__(self) -> None:
        self.latest: dict[tuple[str, int], ManagerHeartbeat] = {}

    def update_manager(self, datacenter_id, manager_addr, heartbeat) -> None:
        self.latest[manager_addr] = heartbeat


def make_coordinator(clock: SteppedClock, stored: StoredHeartbeats) -> GateHealthCoordinator:
    return GateHealthCoordinator(
        clock=clock,
        state=GateRuntimeState(forward_throughput_interval_start=0.0),
        logger=None,
        task_runner=SimpleNamespace(run=lambda *args, **kwargs: None),
        dc_health_manager=stored,
        dc_health_monitor=SimpleNamespace(),
        cross_dc_correlation=SimpleNamespace(
            register_partition_healed_callback=lambda callback: None,
            register_partition_detected_callback=lambda callback: None,
            record_extension=lambda **kwargs: None,
            record_lhm_score=lambda **kwargs: None,
        ),
        track_manager=lambda datacenter_id, manager_addr: None,
        versioned_clock=VersionedStateClock(),
        manager_health_config=ManagerHealthConfig(),
        datacenter_managers={},
        get_node_id=None,
        get_host=None,
        get_tcp_port=None,
        confirm_manager_for_dc=None,
        record_manager_heartbeat=lambda *args: None,
        capacity_aggregator=DatacenterCapacityAggregator(
            clock=clock,
            staleness_threshold_seconds=SETTINGS.CAPACITY_STALENESS_THRESHOLD_SECONDS,
        ),
        circuit_breaker_manager=CircuitBreakerManager(SETTINGS, is_peer_suspected=lambda _peer_addr: False),
        resource_aggregator=None,
        resource_predictor=None,
        latency_slo=LatencySLO.from_env(Env()),
        slo_health_classifier=SLOHealthClassifier.from_env(Env()),
    )


def manager_heartbeat(**reported) -> ManagerHeartbeat:
    """The manager at its state version 5 -- from TCP with ``reported``,
    or from SWIM without."""
    return ManagerHeartbeat(
        node_id="manager-1",
        datacenter=DATACENTER,
        is_leader=False,
        term=1,
        version=5,
        active_jobs=1,
        active_workflows=3,
        worker_count=4,
        healthy_worker_count=4,
        available_cores=6,
        total_cores=16,
        tcp_host=MANAGER_TCP_ADDRESS[0],
        tcp_port=MANAGER_TCP_ADDRESS[1],
        udp_host=MANAGER_UDP_ADDRESS[0],
        udp_port=MANAGER_UDP_ADDRESS[1],
        **reported,
    )


TCP_REPORT = {
    "pending_workflow_count": 7,
    "pending_duration_seconds": 12.5,
    "active_remaining_seconds": 40.0,
    "overloaded_worker_count": 2,
    "slo_sample_count": 40,
    "slo_p95_ms": 180.0,
}


@pytest.mark.asyncio
async def test_a_swim_heartbeat_carries_the_last_report_on_while_it_is_fresh() -> None:
    clock = SteppedClock()
    stored = StoredHeartbeats()
    coordinator = make_coordinator(clock, stored)

    await coordinator.ingest_manager_heartbeat(
        manager_heartbeat(**TCP_REPORT), MANAGER_TCP_ADDRESS, MANAGER_TCP_ADDRESS
    )
    clock.now += 1.0
    await coordinator.handle_embedded_manager_heartbeat(manager_heartbeat(), MANAGER_UDP_ADDRESS)
    capacity_between_reports = coordinator._capacity_aggregator.get_capacity(DATACENTER)
    stored_between_reports = stored.latest[MANAGER_TCP_ADDRESS]

    # No report for longer than the capacity view holds one: SWIM says
    # what it knows.
    clock.now += SETTINGS.CAPACITY_STALENESS_THRESHOLD_SECONDS
    await coordinator.handle_embedded_manager_heartbeat(manager_heartbeat(), MANAGER_UDP_ADDRESS)
    stored_without_a_report = stored.latest[MANAGER_TCP_ADDRESS]

    assert capacity_between_reports.pending_workflow_count == 7
    assert {name: getattr(stored_between_reports, name) for name in TCP_REPORT} == TCP_REPORT
    # What SWIM itself carries is its own.
    assert stored_between_reports.available_cores == 6
    assert stored_without_a_report.pending_workflow_count == 0
    assert stored_without_a_report.overloaded_worker_count == 0
    assert coordinator._manager_reports == {}


def heartbeat_fields_set_by(builder) -> set[str]:
    """The keyword arguments ``builder`` passes to ``ManagerHeartbeat``."""
    source = inspect.getsource(builder)
    call = source[source.index("ManagerHeartbeat(") :]
    depth = 0
    for position, character in enumerate(call):
        depth += character == "("
        depth -= character == ")"
        if depth == 0 and character == ")":
            call = call[: position + 1]
            break
    return set(re.findall(r"^\s+(\w+)=", call, re.MULTILINE))


def test_the_carried_fields_are_exactly_those_swim_leaves_out() -> None:
    reported_only = heartbeat_fields_set_by(
        ManagerServer._build_manager_heartbeat
    ) - heartbeat_fields_set_by(ManagerStateEmbedder.get_state)

    assert reported_only == set(TCP_REPORTED_MANAGER_FIELDS) | {"resource_report"}
