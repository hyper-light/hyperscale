"""
A three-manager datacenter forms a working cluster on its own: every
manager learns of both peers through SWIM, reaches the ACTIVE state,
the managers elect exactly one leader, and every manager holds quorum.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.models import ManagerState
from hyperscale.distributed.nodes import ManagerServer
from tests.integration.in_process_nodes import node_env, reserve_node_ports, stop_nodes, wait_until
from tests.integration.manager.manager_cluster import NODE_START_SECONDS, NODE_STOP_SECONDS, new_manager

MANAGER_COUNT = 3
DATACENTER_ID = "DC-EAST"
# Leader election is pre-vote (2s) + election (5-7s) per attempt; a split
# vote retries at a higher term. Two cycles took 18s; the bound is generous.
CLUSTER_CONVERGENCE_SECONDS = 60.0


@pytest.fixture
async def manager_cluster(node_directory: pathlib.Path) -> AsyncIterator[list[ManagerServer]]:
    """Three started managers of one datacenter, seeded with each other;
    always stopped after the test."""
    manager_ports = reserve_node_ports(MANAGER_COUNT)
    env = node_env(node_directory)
    managers = [new_manager(env, manager_port, manager_ports, DATACENTER_ID) for manager_port in manager_ports]
    try:
        await asyncio.wait_for(
            asyncio.gather(*[manager.start() for manager in managers]),
            timeout=NODE_START_SECONDS,
        )
        yield managers
    finally:
        await stop_nodes(managers, within_seconds=NODE_STOP_SECONDS)


async def test_three_managers_connect_elect_one_leader_and_hold_quorum(
    manager_cluster: list[ManagerServer],
) -> None:
    """Every manager knows both peers through SWIM, is ACTIVE, exactly one
    leader is elected, and every manager has quorum."""
    expected_peer_count = MANAGER_COUNT - 1
    await wait_until(
        lambda: [manager.is_leader() for manager in manager_cluster].count(True) == 1
        and all(
            len(manager._incarnation_tracker.get_all_nodes()) >= expected_peer_count
            and manager._manager_state.manager_state_enum == ManagerState.ACTIVE
            and manager._leadership.has_quorum()
            for manager in manager_cluster
        ),
        within_seconds=CLUSTER_CONVERGENCE_SECONDS,
        description="the managers connecting, becoming ACTIVE, electing one leader and holding quorum",
    )

    for manager in manager_cluster:
        known_node_count = len(manager._incarnation_tracker.get_all_nodes())
        assert known_node_count >= expected_peer_count, (
            f"manager {manager._tcp_port} knows {known_node_count} SWIM nodes, needs {expected_peer_count}"
        )

    for manager in manager_cluster:
        manager_state = manager._manager_state.manager_state_enum
        assert manager_state == ManagerState.ACTIVE, f"manager {manager._tcp_port} is {manager_state.value}, not active"

    leader_ports = [manager._tcp_port for manager in manager_cluster if manager.is_leader()]
    assert len(leader_ports) == 1, f"expected exactly one leader, got the managers on ports {leader_ports}"

    for manager in manager_cluster:
        assert manager._leadership.has_quorum(), (
            f"manager {manager._tcp_port} lacks quorum: {manager._manager_state.get_active_peer_count()} active "
            f"of a required {manager._leadership.get_quorum_size()}"
        )
