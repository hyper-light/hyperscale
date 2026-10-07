"""
A gate cluster and a manager cluster of one datacenter, started in-process
on loopback, converge on their own: the managers know each other, turn
ACTIVE, elect exactly one leader and hold quorum; the gates know each
other, turn ACTIVE, elect exactly one leader, hold quorum, and know the
datacenter's managers.
"""

import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.nodes import GateServer, ManagerServer
from tests.integration.gates.gate_cluster import new_gates, new_managers, start_nodes
from tests.integration.in_process_nodes import reserve_node_ports, stop_nodes, wait_until

DATACENTER_ID = "DC-EAST"
MANAGER_COUNT = 3
GATE_COUNT = 2
NODE_START_SECONDS = 30.0
# The script this replaces checked the clusters 20s after start; this is
# the bound on the same convergence.
CLUSTER_CONVERGENCE_SECONDS = 60.0
SHUTDOWN_SECONDS = 30.0


@pytest.fixture
async def gate_and_manager_clusters(
    node_directory: pathlib.Path,
) -> AsyncIterator[tuple[list[GateServer], list[ManagerServer]]]:
    """The gates, started first so the managers can register with them,
    then the managers; every node is stopped at teardown."""
    node_ports = reserve_node_ports(GATE_COUNT + MANAGER_COUNT)
    gate_ports, manager_ports = node_ports[:GATE_COUNT], node_ports[GATE_COUNT:]
    gates = new_gates(node_directory, gate_ports, {DATACENTER_ID: manager_ports})
    managers = new_managers(node_directory, DATACENTER_ID, manager_ports, gate_ports)
    try:
        await start_nodes(gates, NODE_START_SECONDS)
        await start_nodes(managers, NODE_START_SECONDS)
        yield gates, managers
    finally:
        await stop_nodes([*gates, *managers], SHUTDOWN_SECONDS)


async def test_managers_form_an_active_quorate_cluster_with_one_leader(
    gate_and_manager_clusters: tuple[list[GateServer], list[ManagerServer]],
) -> None:
    _, managers = gate_and_manager_clusters
    expected_peer_count = MANAGER_COUNT - 1

    await wait_until(
        lambda: all(
            len(manager._incarnation_tracker.get_all_nodes()) >= expected_peer_count
            and manager._manager_state.manager_state_enum.value == "active"
            and manager._leadership.has_quorum()
            for manager in managers
        )
        and sum(manager.is_leader() for manager in managers) == 1,
        within_seconds=CLUSTER_CONVERGENCE_SECONDS,
        description="every manager knowing its peers, ACTIVE and quorate, with exactly one leader",
    )

    for manager in managers:
        known_node_count = len(manager._incarnation_tracker.get_all_nodes())
        assert known_node_count >= expected_peer_count, (
            f"manager {manager._node_id.short} knows {known_node_count}/{expected_peer_count} manager peers"
        )
        manager_state = manager._manager_state.manager_state_enum.value
        assert manager_state == "active", f"manager {manager._node_id.short} is {manager_state}, not active"
        assert manager._leadership.has_quorum(), (
            f"manager {manager._node_id.short} lacks quorum: "
            f"active={manager._manager_state.get_active_peer_count()}, "
            f"required={manager._leadership.get_quorum_size()}"
        )
    leader_ids = [manager._node_id.short for manager in managers if manager.is_leader()]
    assert len(leader_ids) == 1, f"expected exactly one manager leader, found {leader_ids}"


async def test_gates_form_an_active_quorate_cluster_that_knows_the_datacenter_managers(
    gate_and_manager_clusters: tuple[list[GateServer], list[ManagerServer]],
) -> None:
    gates, _ = gate_and_manager_clusters
    expected_gate_peer_count = GATE_COUNT - 1

    await wait_until(
        lambda: all(
            len(gate._incarnation_tracker.get_all_nodes()) >= expected_gate_peer_count
            and gate._modular_state.get_gate_state().value == "active"
            and gate._has_quorum_available()
            for gate in gates
        )
        and sum(gate.is_leader() for gate in gates) == 1,
        within_seconds=CLUSTER_CONVERGENCE_SECONDS,
        description="every gate knowing its peers, ACTIVE and quorate, with exactly one leader",
    )

    for gate in gates:
        known_node_count = len(gate._incarnation_tracker.get_all_nodes())
        assert known_node_count >= expected_gate_peer_count, (
            f"gate {gate._node_id.short} knows {known_node_count} nodes, fewer than its "
            f"{expected_gate_peer_count} gate peers"
        )
        gate_state = gate._modular_state.get_gate_state().value
        assert gate_state == "active", f"gate {gate._node_id.short} is {gate_state}, not active"
        assert gate._has_quorum_available(), (
            f"gate {gate._node_id.short} lacks quorum: "
            f"active={gate._modular_state.get_active_peer_count() + 1}, required={gate._quorum_size()}"
        )
        configured_manager_count = len(gate._datacenter_managers.get(DATACENTER_ID, []))
        assert configured_manager_count > 0, (
            f"gate {gate._node_id.short} has no managers configured for {DATACENTER_ID}"
        )
    leader_ids = [gate._node_id.short for gate in gates if gate.is_leader()]
    assert len(leader_ids) == 1, f"expected exactly one gate leader, found {leader_ids}"
