"""
Gate-to-manager discovery (AD-28): gates configured with each
datacenter's managers, and managers registering with the gates, leave
every gate with one DiscoveryService per datacenter that tracks all of
that datacenter's managers; selecting a manager for a key yields an
addressable manager, deterministically, and latency feedback moves its
effective latency; a failed manager leaves every gate's discovery and,
once restarted on the same address, rejoins it.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.nodes import GateServer, ManagerServer
from tests.integration.gates.gate_cluster import new_gates, new_manager, new_managers, start_nodes
from tests.integration.in_process_nodes import reserve_node_ports, stop_nodes, wait_until

NODE_ENV_OVERRIDES: dict[str, str] = {"MERCURY_SYNC_LOG_LEVEL": "error", "MERCURY_SYNC_REQUEST_TIMEOUT": "5s"}
SINGLE_DATACENTER = ("DC-TEST",)
TWO_DATACENTERS = ("DC-EAST", "DC-WEST")
NODE_START_SECONDS = 30.0
# The script this replaces gave the gates a third of its stabilization
# time (15-20s plus 2s per node) to form their cluster, then the full
# time for the managers to register; these bound the same convergence.
GATE_CLUSTER_SECONDS = 60.0
MANAGER_REGISTRATION_SECONDS = 90.0
# The script waited 20s each for failure and for recovery detection.
FAILURE_DETECTION_SECONDS = 60.0
RECOVERY_SECONDS = 60.0
SHUTDOWN_SECONDS = 30.0
SELECTION_KEYS = ("job-1", "job-2", "job-3")
DETERMINISM_KEY = "determinism-test"
RECORDED_SUCCESS_LATENCIES_MS = (10.0, 15.0)


@pytest.fixture
async def gates_and_managers(
    node_directory: pathlib.Path,
    request: pytest.FixtureRequest,
) -> AsyncIterator[tuple[list[GateServer], dict[str, list[ManagerServer]]]]:
    """``request.param`` is (gate count, managers per datacenter,
    datacenter ids). The gates start first and form their cluster, then
    every datacenter's managers start and register with them. The
    fixture stops every node in the yielded lists at teardown, so a test
    that replaces a manager swaps it in its datacenter's list."""
    gate_count: int = request.param[0]
    managers_per_datacenter: int = request.param[1]
    datacenter_ids: tuple[str, ...] = request.param[2]
    node_ports = reserve_node_ports(gate_count + managers_per_datacenter * len(datacenter_ids))
    gate_ports = node_ports[:gate_count]
    manager_ports_by_datacenter = {
        datacenter_id: node_ports[
            gate_count + datacenter_index * managers_per_datacenter : gate_count
            + (datacenter_index + 1) * managers_per_datacenter
        ]
        for datacenter_index, datacenter_id in enumerate(datacenter_ids)
    }
    gates = new_gates(node_directory, gate_ports, manager_ports_by_datacenter, **NODE_ENV_OVERRIDES)
    managers_by_datacenter = {
        datacenter_id: new_managers(node_directory, datacenter_id, manager_ports, gate_ports, **NODE_ENV_OVERRIDES)
        for datacenter_id, manager_ports in manager_ports_by_datacenter.items()
    }
    try:
        await start_nodes(gates, NODE_START_SECONDS)
        await wait_until(
            lambda: all(gate._modular_state.get_active_peer_count() >= gate_count - 1 for gate in gates),
            within_seconds=GATE_CLUSTER_SECONDS,
            description=f"every gate tracking its {gate_count - 1} peer gates",
        )
        await start_nodes(
            [manager for managers in managers_by_datacenter.values() for manager in managers], NODE_START_SECONDS
        )
        yield gates, managers_by_datacenter
    finally:
        await stop_nodes(
            [*[manager for managers in managers_by_datacenter.values() for manager in managers], *gates],
            SHUTDOWN_SECONDS,
        )


@pytest.mark.parametrize(
    "gates_and_managers",
    [(2, 3, SINGLE_DATACENTER), (3, 3, SINGLE_DATACENTER), (2, 2, TWO_DATACENTERS)],
    indirect=True,
    ids=["2-gates-3-managers", "3-gates-3-managers", "2-gates-2-datacenters-of-2-managers"],
)
async def test_every_gate_discovers_every_manager_of_each_datacenter(
    gates_and_managers: tuple[list[GateServer], dict[str, list[ManagerServer]]],
) -> None:
    gates, managers_by_datacenter = gates_and_managers

    await wait_until(
        lambda: all(
            (datacenter_discovery := gate._dc_manager_discovery.get(datacenter_id)) is not None
            and datacenter_discovery.peer_count >= len(managers)
            for gate in gates
            for datacenter_id, managers in managers_by_datacenter.items()
        ),
        within_seconds=MANAGER_REGISTRATION_SECONDS,
        description="every gate's per-datacenter discovery tracking every manager of that datacenter",
    )

    for gate in gates:
        for datacenter_id, managers in managers_by_datacenter.items():
            datacenter_discovery = gate._dc_manager_discovery.get(datacenter_id)
            assert datacenter_discovery is not None, (
                f"gate {gate._node_id.short} has no manager discovery for {datacenter_id}"
            )
            assert datacenter_discovery.peer_count >= len(managers), (
                f"gate {gate._node_id.short} discovered {datacenter_discovery.peer_count}/{len(managers)} "
                f"managers of {datacenter_id}"
            )


@pytest.mark.parametrize(
    "gates_and_managers", [(2, 3, SINGLE_DATACENTER)], indirect=True, ids=["2-gates-3-managers"]
)
async def test_manager_selection_is_deterministic_and_latency_feedback_is_recorded(
    gates_and_managers: tuple[list[GateServer], dict[str, list[ManagerServer]]],
) -> None:
    gates, managers_by_datacenter = gates_and_managers
    [(datacenter_id, managers)] = managers_by_datacenter.items()

    await wait_until(
        lambda: all(
            (datacenter_discovery := gate._dc_manager_discovery.get(datacenter_id)) is not None
            and datacenter_discovery.peer_count >= len(managers)
            for gate in gates
        ),
        within_seconds=MANAGER_REGISTRATION_SECONDS,
        description=f"every gate discovering the {len(managers)} managers of {datacenter_id}",
    )

    for gate in gates:
        gate_name = gate._node_id.short
        datacenter_discovery = gate._dc_manager_discovery.get(datacenter_id)
        assert datacenter_discovery is not None, f"gate {gate_name} has no manager discovery for {datacenter_id}"

        for selection_key in SELECTION_KEYS:
            selection = datacenter_discovery.select_peer(selection_key)
            assert selection is not None, f"gate {gate_name} selected no manager for key {selection_key!r}"
            assert datacenter_discovery.get_peer_address(selection.peer_id) is not None, (
                f"gate {gate_name} selected manager {selection.peer_id} for key {selection_key!r} with no address"
            )

        first_selection = datacenter_discovery.select_peer(DETERMINISM_KEY)
        second_selection = datacenter_discovery.select_peer(DETERMINISM_KEY)
        assert first_selection is not None and second_selection is not None, (
            f"gate {gate_name} selected no manager for key {DETERMINISM_KEY!r}"
        )
        assert first_selection.peer_id == second_selection.peer_id, (
            f"gate {gate_name} selected {first_selection.peer_id} then {second_selection.peer_id} "
            f"for key {DETERMINISM_KEY!r}"
        )

        discovered_managers = datacenter_discovery.get_all_peers()
        assert discovered_managers, f"gate {gate_name} has no discovered manager to record latency for"
        feedback_peer_id = discovered_managers[0].peer_id
        for latency_ms in RECORDED_SUCCESS_LATENCIES_MS:
            datacenter_discovery.record_success(feedback_peer_id, latency_ms)
        datacenter_discovery.record_failure(feedback_peer_id)

        effective_latency_ms = datacenter_discovery.get_effective_latency(feedback_peer_id)
        assert effective_latency_ms > 0, (
            f"gate {gate_name} recorded no latency for manager {feedback_peer_id}: {effective_latency_ms}"
        )


@pytest.mark.parametrize(
    "gates_and_managers", [(2, 3, SINGLE_DATACENTER)], indirect=True, ids=["2-gates-3-managers"]
)
async def test_gates_drop_a_failed_manager_and_rediscover_it_after_restart(
    node_directory: pathlib.Path,
    gates_and_managers: tuple[list[GateServer], dict[str, list[ManagerServer]]],
) -> None:
    gates, managers_by_datacenter = gates_and_managers
    [(datacenter_id, managers)] = managers_by_datacenter.items()
    manager_count = len(managers)
    gate_ports = [gate._tcp_port for gate in gates]
    manager_ports = [manager._tcp_port for manager in managers]

    await wait_until(
        lambda: all(
            (datacenter_discovery := gate._dc_manager_discovery.get(datacenter_id)) is not None
            and datacenter_discovery.peer_count >= manager_count
            for gate in gates
        ),
        within_seconds=MANAGER_REGISTRATION_SECONDS,
        description=f"every gate discovering the {manager_count} managers of {datacenter_id} before the failure",
    )

    # Fail the last manager without a leave broadcast; it leaves the
    # fixture's list first, so teardown stops only the managers running.
    failed_manager = managers.pop()
    failed_manager_port = failed_manager._tcp_port
    await asyncio.wait_for(failed_manager.stop(drain_timeout=0.5, broadcast_leave=False), timeout=SHUTDOWN_SECONDS)

    await wait_until(
        lambda: all(
            (datacenter_discovery := gate._dc_manager_discovery.get(datacenter_id)) is not None
            and datacenter_discovery.peer_count <= manager_count - 1
            for gate in gates
        ),
        within_seconds=FAILURE_DETECTION_SECONDS,
        description=f"every gate dropping the failed manager from {datacenter_id}'s discovery",
    )
    for gate in gates:
        datacenter_discovery = gate._dc_manager_discovery.get(datacenter_id)
        assert datacenter_discovery is not None, f"gate {gate._node_id.short} has no discovery for {datacenter_id}"
        assert datacenter_discovery.peer_count <= manager_count - 1, (
            f"gate {gate._node_id.short} still discovers {datacenter_discovery.peer_count} managers after the failure"
        )

    recovered_manager = new_manager(
        node_directory, datacenter_id, failed_manager_port, manager_ports, gate_ports, **NODE_ENV_OVERRIDES
    )
    managers.append(recovered_manager)
    await start_nodes([recovered_manager], NODE_START_SECONDS)

    await wait_until(
        lambda: all(
            (datacenter_discovery := gate._dc_manager_discovery.get(datacenter_id)) is not None
            and datacenter_discovery.peer_count >= manager_count
            for gate in gates
        ),
        within_seconds=RECOVERY_SECONDS,
        description=f"every gate rediscovering the restarted manager in {datacenter_id}",
    )
    for gate in gates:
        datacenter_discovery = gate._dc_manager_discovery.get(datacenter_id)
        assert datacenter_discovery is not None, f"gate {gate._node_id.short} has no discovery for {datacenter_id}"
        assert datacenter_discovery.peer_count >= manager_count, (
            f"gate {gate._node_id.short} discovers {datacenter_discovery.peer_count}/{manager_count} managers "
            "after the restart"
        )
