"""
AD-19: a gate that cannot do a leader's work leaves leadership to a live
peer that can -- and never leaves the tier without a leader.

A gate is ready when it reaches a datacenter (a manager heartbeat has
arrived and has not stopped) and is not shedding load. An unready gate
refuses to stand for election while a live peer's latest heartbeat
reports it ready; when no live peer is ready -- a global datacenter
outage, a cold start -- every gate stands as usual.

* a ready gate stands;
* an unready gate (no reachable datacenter, or overloaded) defers to a
  live, ready peer;
* it stands when no peer is ready, or the only ready peer is not live.

The SIM scenario test_a_gate_without_datacenters_leaves_leadership_to_a_ready_peer
(tests/unit/simulation/sim/test_multiprocess_gate_faults.py) proves the
rule end to end in a three-gate cluster.
"""

from types import SimpleNamespace

from hyperscale.distributed.models import GateHeartbeat
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

PEER_TCP_ADDR = ("10.0.0.2", 8431)
PEER_UDP_ADDR = ("10.0.0.2", 8441)


def peer_heartbeat(connected_datacenters: int, overload_state: str = "healthy") -> GateHeartbeat:
    return GateHeartbeat(
        node_id="gate-peer",
        datacenter="global",
        is_leader=False,
        term=1,
        version=1,
        state="active",
        active_jobs=0,
        active_datacenters=connected_datacenters,
        manager_count=1,
        tcp_host=PEER_TCP_ADDR[0],
        tcp_port=PEER_TCP_ADDR[1],
        health_has_dc_connectivity=connected_datacenters > 0,
        health_connected_dc_count=connected_datacenters,
        health_overload_state=overload_state,
    )


def make_gate(
    connected_datacenters: int,
    overload_state: str = "healthy",
    peer: GateHeartbeat | None = None,
    peer_is_live: bool = True,
) -> GateServer:
    gate = object.__new__(GateServer)
    gate._node_id = SimpleNamespace(full="gate-self")
    gate._count_active_datacenters = lambda: connected_datacenters
    gate._gate_health_state = overload_state
    gate._modular_state = GateRuntimeState(forward_throughput_interval_start=0.0)
    if peer is not None:
        gate._modular_state.set_gate_peer_heartbeat(PEER_UDP_ADDR, peer)
        if peer_is_live:
            gate._modular_state._active_gate_peers.add(PEER_TCP_ADDR)
    return gate


def test_a_ready_gate_stands() -> None:
    gate = make_gate(connected_datacenters=1, peer=peer_heartbeat(connected_datacenters=1))

    assert gate._refuses_leadership_for_a_ready_peer() is False


def test_a_gate_without_datacenters_defers_to_a_live_ready_peer() -> None:
    gate = make_gate(connected_datacenters=0, peer=peer_heartbeat(connected_datacenters=1))

    assert gate._refuses_leadership_for_a_ready_peer() is True


def test_an_overloaded_gate_defers_to_a_live_ready_peer() -> None:
    gate = make_gate(
        connected_datacenters=1,
        overload_state="overloaded",
        peer=peer_heartbeat(connected_datacenters=1),
    )

    assert gate._refuses_leadership_for_a_ready_peer() is True


def test_an_unready_gate_stands_when_no_peer_is_ready() -> None:
    gate = make_gate(
        connected_datacenters=0,
        peer=peer_heartbeat(connected_datacenters=0),
    )

    assert gate._refuses_leadership_for_a_ready_peer() is False


def test_an_unready_gate_stands_when_its_ready_peer_is_not_live() -> None:
    gate = make_gate(
        connected_datacenters=0,
        peer=peer_heartbeat(connected_datacenters=1),
        peer_is_live=False,
    )

    assert gate._refuses_leadership_for_a_ready_peer() is False
