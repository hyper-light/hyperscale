"""
Every gate quorum decision uses one cluster size.

The gate had three quorum formulas: the leadership coordinator's
(max of a start-time snapshot, known + 1, active + 1), the server's
fallback (configured count), and the leader step-down check (configured
count, ignoring active peers). Submission admission and leader
step-down could disagree on whether quorum held. Now the server's
_configured_gate_count is the single cluster size (configured peers,
known gates and active peers, plus this gate), read live by the
coordinator and used by the step-down check.

Swept over cluster shapes, driven through the real coordinator's
has_quorum and the real _check_quorum_status: admission refuses
exactly when step-down counts a quorum failure.
"""

import itertools
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.models import GateInfo, GateState
from hyperscale.distributed.nodes.gate.leadership_coordinator import GateLeadershipCoordinator
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

PEER_COUNTS = range(0, 5)


def make_gate(configured_peers: int, known_gates: int, active_peers: int) -> GateServer:
    state = GateRuntimeState()
    state.set_gate_state(GateState.ACTIVE)
    for index in range(known_gates):
        state.add_known_gate(
            f"gate-{index}",
            GateInfo(
                node_id=f"gate-{index}",
                tcp_host=f"10.0.1.{index}",
                tcp_port=9000,
                udp_host=f"10.0.1.{index}",
                udp_port=9001,
                datacenter="dc-1",
            ),
        )
    state._active_gate_peers.update((f"10.0.2.{index}", 9000) for index in range(active_peers))
    gate = object.__new__(GateServer)
    gate._modular_state = state
    gate._gate_peers = [(f"10.0.3.{index}", 9000) for index in range(configured_peers)]
    gate._leadership_coordinator = GateLeadershipCoordinator(
        state=state,
        logger=None,
        task_runner=None,
        leadership_tracker=None,
        get_node_id=None,
        get_node_addr=None,
        send_tcp=None,
        get_active_peers=None,
        get_cluster_size=gate._configured_gate_count,
    )
    gate._consecutive_quorum_failures = 0
    gate._quorum_stepdown_consecutive_failures = 1
    gate._leader_election = SimpleNamespace(
        state=SimpleNamespace(is_leader=lambda: False), _step_down=AsyncMock()
    )
    return gate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "configured_peers,known_gates,active_peers",
    list(itertools.product(PEER_COUNTS, PEER_COUNTS, PEER_COUNTS)),
)
async def test_admission_and_step_down_agree_on_quorum(
    configured_peers: int, known_gates: int, active_peers: int
) -> None:
    gate = make_gate(configured_peers, known_gates, active_peers)

    admits = gate._has_quorum_available()
    await GateServer._check_quorum_status(gate)
    step_down_counts_failure = gate._consecutive_quorum_failures > 0

    assert admits == (not step_down_counts_failure)
    cluster_size = max(1, known_gates + 1, configured_peers + 1, active_peers + 1)
    assert gate._quorum_size() == cluster_size // 2 + 1
