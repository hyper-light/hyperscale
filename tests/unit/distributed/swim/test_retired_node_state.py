"""
A retired SWIM member's per-node state leaves with it.

The incarnation tracker retired dead members (past their retention, or
over the membership cap) but the server's per-node maps beside it --
peer roles, confirmation sets -- kept every member ever seen. Leader
election counts same-role peers from those roles, so a cluster's
election size only ever grew, raising the majority a shrunken cluster
needed. The gate tier's election also counted every gate ever seen
instead of its configured cohort.

* a member the tracker retires is forgotten by the server: role,
  confirmation and pending-confirmation state;
* a live member keeps its state;
* the gate tier's election size is its configured cluster size.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models.distributed import NodeRole
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.swim.core.node_state import NodeState
from hyperscale.distributed.swim.detection import incarnation_tracker as tracker_module
from hyperscale.distributed.swim.detection.incarnation_tracker import IncarnationTracker
from hyperscale.distributed.swim.health_aware_server import HealthAwareServer

RETIRED_PEER = ("10.0.0.7", 9101)
LIVE_PEER = ("10.0.0.8", 9101)
RETENTION_SECONDS = 60.0


class FixedClock:
    def __init__(self, now: float) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now


def make_server() -> HealthAwareServer:
    server = object.__new__(HealthAwareServer)
    server._peer_roles = {RETIRED_PEER: NodeRole.GATE, LIVE_PEER: NodeRole.GATE}
    server._confirmed_peers = {RETIRED_PEER, LIVE_PEER}
    server._unconfirmed_peers = {RETIRED_PEER}
    server._unconfirmed_peer_added_at = {RETIRED_PEER: 1.0}
    return server


@pytest.mark.asyncio
async def test_a_retired_member_is_forgotten_and_a_live_one_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tracker_module, "_DEFAULT_CLOCK", FixedClock(1000.0))
    server = make_server()
    tracker = IncarnationTracker(dead_node_retention_seconds=RETENTION_SECONDS)
    tracker.set_eviction_callback(server._forget_retired_node)
    tracker.node_states[RETIRED_PEER] = NodeState(
        status=b"DEAD",
        incarnation=3,
        last_update_time=1000.0 - RETENTION_SECONDS - 1.0,
    )
    tracker.node_states[LIVE_PEER] = NodeState(status=b"OK", incarnation=3, last_update_time=1000.0)

    assert await tracker.cleanup_dead_nodes() == 1

    assert server._peer_roles == {LIVE_PEER: NodeRole.GATE}
    assert server._confirmed_peers == {LIVE_PEER}
    assert server._unconfirmed_peers == set()
    assert server._unconfirmed_peer_added_at == {}


def test_the_gate_election_size_is_its_configured_cluster_size() -> None:
    gate = object.__new__(GateServer)
    gate._modular_state = GateRuntimeState(forward_throughput_interval_start=0.0)
    gate._gate_peers = [("gate-1", 8431), ("gate-2", 8431)]
    # The gate cohort: this gate and its two configured peers.
    gate._cluster_membership = SimpleNamespace(
        cohort=frozenset(gate._gate_peers) | {("gate-0", 8431)}
    )
    gate._peer_roles = {(f"gate-retired-{index}", 8441): NodeRole.GATE for index in range(10)}

    assert gate._get_election_member_count() == 3
