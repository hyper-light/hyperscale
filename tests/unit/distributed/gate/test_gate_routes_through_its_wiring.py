"""
A gate routes jobs through its real wiring.

The router's construction inside ``GateServer._init_coordinators`` once
referenced a name that was not in scope -- every gate would have failed at
startup -- and no test built a gate to notice. This builds a real
GateServer (never started), feeds manager heartbeats through the gate's
one intake path, and routes:

* with equal (unknown) latency, the datacenter with more free cores wins;
* a job asking for two datacenters gets both;
* routing and spillover estimate every known datacenter's latency;
* opening every circuit of a datacenter's managers takes it out of routing;
* a gate serves its state snapshot, and a peer applies it.
"""

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import GateStateSnapshot, ManagerHeartbeat
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.routing import ExclusionReason

MANAGERS = {"dc-a": ("127.0.0.1", 19201), "dc-b": ("127.0.0.1", 19301)}
TOTAL_CORES = 8


def make_gate() -> GateServer:
    return GateServer(
        host="127.0.0.1",
        tcp_port=19101,
        udp_port=19102,
        env=Env(MERCURY_SYNC_AUTH_SECRET="routing-wiring-test-secret-0123456789"),
        datacenter_managers={
            datacenter_id: [manager_address]
            for datacenter_id, manager_address in MANAGERS.items()
        },
        datacenter_manager_udp={
            datacenter_id: [(manager_address[0], manager_address[1] + 1)]
            for datacenter_id, manager_address in MANAGERS.items()
        },
    )


async def report(gate: GateServer, datacenter_id: str, available_cores: int) -> None:
    manager_address = MANAGERS[datacenter_id]
    await gate._health_coordinator.ingest_manager_heartbeat(
        ManagerHeartbeat(
            node_id=f"manager-{datacenter_id}",
            datacenter=datacenter_id,
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=2,
            healthy_worker_count=2,
            available_cores=available_cores,
            total_cores=TOTAL_CORES,
            tcp_host=manager_address[0],
            tcp_port=manager_address[1],
        ),
        source_addr=manager_address,
        manager_addr=manager_address,
    )


@pytest.mark.asyncio
async def test_a_gate_routes_by_free_capacity_when_latency_is_unknown() -> None:
    gate = make_gate()
    await report(gate, "dc-a", available_cores=TOTAL_CORES)
    await report(gate, "dc-b", available_cores=TOTAL_CORES // 2)

    primaries, fallbacks, worst_health = await gate._select_datacenters_with_fallback(1, None, "job-1")

    assert (primaries, fallbacks, worst_health) == (["dc-a"], ["dc-b"], "healthy")
    assert (await gate._select_datacenters_with_fallback(2, None, "job-1"))[0] == ["dc-a", "dc-b"]


@pytest.mark.asyncio
async def test_routing_and_spillover_estimate_every_known_datacenter() -> None:
    gate = make_gate()

    estimates = gate._dispatch_coordinator._estimate_datacenter_latencies_ms()

    assert set(estimates) == set(MANAGERS)
    assert len(set(estimates.values())) == 1


@pytest.mark.asyncio
async def test_a_datacenter_whose_manager_circuits_are_all_open_is_not_routed_to() -> None:
    gate = make_gate()
    await report(gate, "dc-a", available_cores=TOTAL_CORES)
    await report(gate, "dc-b", available_cores=TOTAL_CORES // 2)

    for _ in range(gate.env.CIRCUIT_BREAKER_MAX_ERRORS):
        await gate._circuit_breaker_manager.record_failure(MANAGERS["dc-a"])
    decision = gate._job_router.route_job("job-1", 1, None)

    assert decision.primary_datacenters == ["dc-b"]
    assert decision.exclusions == {"dc-a": ExclusionReason.ALL_MANAGERS_CIRCUIT_OPEN}


@pytest.mark.asyncio
async def test_a_gate_serves_its_state_snapshot_to_a_peer() -> None:
    """Building the snapshot omitted two required fields and raised
    TypeError, so every peer's state-sync request got an error back."""
    gate = make_gate()
    peer = make_gate()
    await report(gate, "dc-a", available_cores=TOTAL_CORES)

    snapshot = GateStateSnapshot.load(gate._get_state_snapshot().dump())
    await peer._apply_gate_state_snapshot(snapshot)

    assert snapshot.is_leader is gate.is_leader()
    assert snapshot.term == gate._leader_election.state.current_term
    assert set(snapshot.datacenter_managers) == set(MANAGERS)
