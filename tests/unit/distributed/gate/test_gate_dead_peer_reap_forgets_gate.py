"""
A reaped dead gate is forgotten under its gate id.

The dead-peer loop reaps in two passes: after the reap interval a
peer's unhealthy mark becomes a dead mark; after twice the interval the
peer is cleaned up. The first pass deleted the peer's UDP mapping and
last heartbeat -- and threw away the gate id that cleanup returned --
so the second pass could not resolve the peer's gate id and fell back
to "host:port". The known-gates entry (and versioned-clock entity,
hash-ring node, discovery entry) keyed by the real gate id was never
removed: every gate that ever died kept being gossiped and kept
counting toward _configured_gate_count, inflating the quorum size with
each replacement.

Driven through the real ``_dead_peer_reap_loop`` (two passes past both
thresholds) over a real GateRuntimeState and the real
GatePeerCoordinator.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.models import GateHeartbeat, GateInfo
from hyperscale.distributed.nodes.gate.peer_coordinator import GatePeerCoordinator
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

DEAD_GATE_ID = "gate-b-node-id"
DEAD_TCP = ("10.0.0.2", 9000)
DEAD_UDP = ("10.0.0.2", 9001)
REAP_INTERVAL = 10.0


def make_state() -> GateRuntimeState:
    state = GateRuntimeState()
    state.set_udp_to_tcp_mapping(DEAD_UDP, DEAD_TCP)
    state.set_gate_peer_heartbeat(
        DEAD_UDP,
        GateHeartbeat(
            node_id=DEAD_GATE_ID,
            datacenter="dc-1",
            is_leader=False,
            term=1,
            version=1,
            state="active",
            active_jobs=0,
            active_datacenters=0,
            manager_count=0,
            tcp_host=DEAD_TCP[0],
            tcp_port=DEAD_TCP[1],
        ),
    )
    state.add_known_gate(
        DEAD_GATE_ID,
        GateInfo(
            node_id=DEAD_GATE_ID,
            tcp_host=DEAD_TCP[0],
            tcp_port=DEAD_TCP[1],
            udp_host=DEAD_UDP[0],
            udp_port=DEAD_UDP[1],
            datacenter="dc-1",
        ),
    )
    state._active_gate_peers.add(DEAD_TCP)
    state.mark_peer_unhealthy(DEAD_TCP, 0.0)
    return state


class TwoPassGate:
    """Each loop sleep advances virtual time past the next threshold;
    the loop ends after the cleanup pass."""

    def __init__(self, state: GateRuntimeState) -> None:
        self.now = 0.0
        self.passes = 0
        self.removed_entities: list[str] = []
        gate = object.__new__(GateServer)
        gate._running = True
        gate._modular_state = state
        gate._dead_peer_check_interval = REAP_INTERVAL
        gate._dead_peer_reap_interval = REAP_INTERVAL
        gate._clock = SimpleNamespace(sleep=self.sleep, monotonic=lambda: self.now)
        gate._task_runner = SimpleNamespace(run=lambda *args, **kwargs: None)
        gate._udp_logger = SimpleNamespace(log=None)
        gate._host, gate._tcp_port = "10.0.0.1", 9000
        gate._node_id = SimpleNamespace(short="gate-a")
        gate._versioned_clock = SimpleNamespace(remove_entity=self.remove_entity)
        gate._peer_gate_circuit_breaker = SimpleNamespace(remove_circuit=AsyncMock())
        # The loop's other per-tick duties (quorum check, health log,
        # ledger checkpoint) are not under test here.
        gate._check_quorum_status = AsyncMock()
        gate._log_health_transitions = lambda: None
        gate._checkpoint_ledger_if_due = AsyncMock()
        gate._peer_coordinator = GatePeerCoordinator(
            state=state,
            logger=SimpleNamespace(log=None),
            task_runner=SimpleNamespace(run=lambda *args, **kwargs: None),
            peer_discovery=SimpleNamespace(remove_peer=lambda peer_id: None),
            job_hash_ring=SimpleNamespace(remove_node=AsyncMock()),
            job_forwarding_tracker=SimpleNamespace(unregister_peer=lambda gate_id: None),
            job_leadership_tracker=None,
            versioned_clock=None,
            gate_health_config=None,
            recovery_semaphore=asyncio.Semaphore(1),
            recovery_jitter_min=0.0,
            recovery_jitter_max=0.0,
            get_node_id=lambda: SimpleNamespace(short="gate-a"),
            get_host=lambda: "10.0.0.1",
            get_tcp_port=lambda: 9000,
            get_udp_port=lambda: 9001,
            confirm_peer=AsyncMock(),
            handle_job_leader_failure=None,
            remove_peer_circuit=AsyncMock(),
        )
        self.gate = gate
        self.hash_ring = gate._peer_coordinator._job_hash_ring

    async def remove_entity(self, entity_id: str) -> None:
        self.removed_entities.append(entity_id)

    async def sleep(self, seconds: float) -> None:
        self.passes += 1
        if self.passes > 2:
            self.gate._running = False
        self.now += 2.0 * REAP_INTERVAL + 1.0


@pytest.mark.asyncio
async def test_a_reaped_gate_is_forgotten_under_its_gate_id() -> None:
    state = make_state()
    two_passes = TwoPassGate(state)

    await GateServer._dead_peer_reap_loop(two_passes.gate)

    assert DEAD_GATE_ID not in dict(state.iter_known_gates())
    assert state.get_known_gate_count() == 0
    assert two_passes.removed_entities == [DEAD_GATE_ID]
    two_passes.hash_ring.remove_node.assert_awaited_once_with(DEAD_GATE_ID)
    assert DEAD_TCP not in state.get_active_peers()
    assert DEAD_TCP not in state.get_dead_peer_timestamps()
