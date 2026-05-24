import time

from hyperscale.distributed.models import DatacenterHealth, DatacenterStatus
from hyperscale.distributed.nodes.gate.health_coordinator import GateHealthCoordinator
from hyperscale.distributed.swim.health import (
    CrossClusterAck,
    DCHealthState,
    DCReachability,
    FederatedHealthMonitor,
)


def make_ack(
    *,
    datacenter: str = "main",
    node_id: str = "manager-1",
    incarnation: int = 1,
    dc_health: str = "HEALTHY",
) -> CrossClusterAck:
    return CrossClusterAck(
        datacenter=datacenter,
        node_id=node_id,
        incarnation=incarnation,
        is_leader=True,
        leader_term=1,
        cluster_size=1,
        healthy_managers=1,
        worker_count=1,
        healthy_workers=1,
        total_cores=2,
        available_cores=2,
        active_jobs=0,
        active_workflows=0,
        dc_health=dc_health,
    )


def make_tcp_status(health: DatacenterHealth) -> DatacenterStatus:
    return DatacenterStatus(
        dc_id="main",
        health=health.value,
        available_capacity=2,
        queue_depth=0,
        manager_count=1,
        worker_count=1,
        last_update=time.monotonic(),
    )


def test_new_federated_datacenter_starts_unknown() -> None:
    monitor = FederatedHealthMonitor()
    monitor.add_datacenter("main", ("127.0.0.1", 9000))

    state = monitor.get_dc_health("main")

    assert state is not None
    assert state.reachability == DCReachability.UNKNOWN
    assert state.effective_health == "UNKNOWN"
    assert state.is_healthy_for_jobs is False


def test_unknown_datacenter_does_not_become_unreachable_without_ack() -> None:
    monitor = FederatedHealthMonitor(max_consecutive_failures=1)
    monitor.add_datacenter("main", ("127.0.0.1", 9000))
    state = monitor.get_dc_health("main")
    assert state is not None

    state.last_probe_sent = time.monotonic() - 60.0
    monitor._handle_probe_failure(state)
    monitor._check_ack_timeouts()

    assert state.reachability == DCReachability.UNKNOWN
    assert state.has_successful_probe is False


def test_reachable_datacenter_can_become_confirmed_unreachable() -> None:
    monitor = FederatedHealthMonitor(
        max_consecutive_failures=1,
        suspicion_timeout=0.0,
    )
    monitor.add_datacenter("main", ("127.0.0.1", 9000))
    monitor.handle_ack(make_ack())
    state = monitor.get_dc_health("main")
    assert state is not None

    monitor._handle_probe_failure(state)
    state.suspected_at = time.monotonic() - 1.0
    monitor._handle_probe_failure(state)

    assert state.has_successful_probe is True
    assert state.reachability == DCReachability.UNREACHABLE


def test_follower_ack_is_not_authoritative() -> None:
    monitor = FederatedHealthMonitor()
    monitor.add_datacenter("main", ("127.0.0.1", 9000))
    ack = make_ack()
    ack.is_leader = False

    monitor.handle_ack(ack)
    state = monitor.get_dc_health("main")

    assert state is not None
    assert state.reachability == DCReachability.UNKNOWN
    assert state.has_successful_probe is False


def test_leader_change_resets_confirmed_negative_state_to_unknown() -> None:
    monitor = FederatedHealthMonitor()
    monitor.add_datacenter(
        "main",
        ("127.0.0.1", 9000),
        leader_node_id="manager-1",
        leader_term=1,
    )
    monitor.handle_ack(make_ack(node_id="manager-1"))
    state = monitor.get_dc_health("main")
    assert state is not None
    state.reachability = DCReachability.UNREACHABLE

    changed = monitor.update_leader(
        "main",
        ("127.0.0.1", 9001),
        leader_node_id="manager-2",
        leader_term=2,
    )

    assert changed is True
    assert state.reachability == DCReachability.UNKNOWN
    assert state.last_ack is None
    assert state.has_successful_probe is False


def test_unproven_federated_failure_does_not_override_tcp_health() -> None:
    coordinator = GateHealthCoordinator.__new__(GateHealthCoordinator)
    tcp_status = make_tcp_status(DatacenterHealth.HEALTHY)
    federated_state = DCHealthState(
        datacenter="main",
        reachability=DCReachability.UNREACHABLE,
    )

    status = coordinator._merge_unreachable_federated_health(
        "main",
        tcp_status,
        federated_state,
    )

    assert status is tcp_status


def test_confirmed_federated_failure_degrades_fresh_tcp_health() -> None:
    coordinator = GateHealthCoordinator.__new__(GateHealthCoordinator)
    tcp_status = make_tcp_status(DatacenterHealth.HEALTHY)
    federated_state = DCHealthState(
        datacenter="main",
        reachability=DCReachability.UNREACHABLE,
        last_ack=make_ack(),
        last_ack_received=time.monotonic(),
    )

    status = coordinator._merge_unreachable_federated_health(
        "main",
        tcp_status,
        federated_state,
    )

    assert status.health == DatacenterHealth.DEGRADED.value
    assert status.available_capacity == tcp_status.available_capacity
