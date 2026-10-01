"""
ManagerLeadershipCoordinator: the manager's single quorum authority
(AD-27 -- the server's inline twins are gone).

* Quorum is a majority of the CONFIGURED cluster (AD-3), counting the
  reachable peers plus this node exactly once. An isolated manager of
  three does NOT hold quorum -- the coordinator once counted itself
  twice and said it did, which let a partitioned leader keep taking over
  jobs and delayed its lost-quorum step-down by a peer.
* A leader steps down after MAX_CONSECUTIVE_QUORUM_FAILURES consecutive
  checks without quorum; a follower never does; regaining quorum resets
  the streak.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.nodes.manager.leadership import (
    MAX_CONSECUTIVE_QUORUM_FAILURES,
    ManagerLeadershipCoordinator,
)
from hyperscale.distributed.nodes.manager.state import ManagerState


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)


def make_coordinator(
    configured_peer_count: int, reachable_peer_count: int, is_leader: bool = True
) -> tuple[ManagerLeadershipCoordinator, ManagerState, list[str]]:
    state = ManagerState()
    for peer_index in range(reachable_peer_count):
        state._active_manager_peers.add(("10.0.0.1", 9000 + peer_index))
    step_downs: list[str] = []
    coordinator = ManagerLeadershipCoordinator(
        state=state,
        config=SimpleNamespace(
            manager_udp_peers=[("10.0.0.1", 9100 + index) for index in range(configured_peer_count)],
            host="127.0.0.1",
            tcp_port=9000,
        ),
        logger=RecordingLogger(),
        node_id="manager-a",
        task_runner=None,
        is_leader_fn=lambda: is_leader,
        get_term_fn=lambda: 1,
        step_down_fn=lambda: step_downs.append("stepped down"),
    )
    return coordinator, state, step_downs


@pytest.mark.parametrize(
    ("configured_peers", "reachable_peers", "expected"),
    [
        (2, 0, False),  # isolated manager of three
        (2, 1, True),  # two of three
        (4, 1, False),  # two of five
        (4, 2, True),  # three of five
        (0, 0, True),  # a single-manager cluster is its own quorum
    ],
)
def test_quorum_counts_this_node_once(configured_peers: int, reachable_peers: int, expected: bool) -> None:
    coordinator, _state, _step_downs = make_coordinator(configured_peers, reachable_peers)
    assert coordinator.has_quorum() is expected


@pytest.mark.asyncio
async def test_a_leader_without_quorum_steps_down_after_consecutive_failures() -> None:
    coordinator, _state, step_downs = make_coordinator(configured_peer_count=2, reachable_peer_count=0)

    for _ in range(MAX_CONSECUTIVE_QUORUM_FAILURES - 1):
        await coordinator.check_quorum_status()
    assert step_downs == []

    await coordinator.check_quorum_status()
    assert step_downs == ["stepped down"]


@pytest.mark.asyncio
async def test_regaining_quorum_resets_the_failure_streak() -> None:
    coordinator, state, step_downs = make_coordinator(configured_peer_count=2, reachable_peer_count=0)
    for _ in range(MAX_CONSECUTIVE_QUORUM_FAILURES - 1):
        await coordinator.check_quorum_status()

    state._active_manager_peers.add(("10.0.0.1", 9000))
    await coordinator.check_quorum_status()
    state._active_manager_peers.clear()
    for _ in range(MAX_CONSECUTIVE_QUORUM_FAILURES - 1):
        await coordinator.check_quorum_status()

    assert step_downs == []


@pytest.mark.asyncio
async def test_a_follower_without_quorum_never_steps_down() -> None:
    coordinator, _state, step_downs = make_coordinator(
        configured_peer_count=2, reachable_peer_count=0, is_leader=False
    )
    for _ in range(2 * MAX_CONSECUTIVE_QUORUM_FAILURES):
        await coordinator.check_quorum_status()
    assert step_downs == []
