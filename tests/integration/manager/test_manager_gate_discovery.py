"""
Gates and managers discover each other (AD-28): with 2-3 gates and one to
three datacenters of 2-5 managers, every gate's per-datacenter discovery
service tracks every manager of each datacenter and the gate has heard
each manager's heartbeat, and every manager has a gate registered with
it; managers keep their configured datacenter and addresses, and a gate
orders a datacenter's managers for dispatch; a manager that fails is
retired from every gate's discovery service, and tracked again once it
restarts.

A gate tracks its configured managers in discovery from construction
(``GateServer._track_configured_managers``), so these tests also require
each manager's heartbeat at the gate, which only a running manager sends.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.nodes import GateServer, ManagerServer
from tests.integration.in_process_nodes import node_env, reserve_node_ports, stop_nodes, wait_until
from tests.integration.manager.manager_cluster import (
    CRASH_DRAIN_SECONDS,
    NODE_START_SECONDS,
    NODE_STOP_SECONDS,
    QUIET_LOG_LEVEL,
    REQUEST_TIMEOUT,
    new_gate,
    new_manager,
)

# The script slept 15-26s for discovery and 15s each for failure detection
# and recovery; the bounds are generous.
DISCOVERY_SECONDS = 90.0
FAILURE_DETECTION_SECONDS = 90.0
RECOVERY_SECONDS = 60.0
# A gate retires a manager whose heartbeats stopped once they are older
# than GATE_DEAD_PEER_REAP_INTERVAL (120s by default), checked every
# DISCOVERY_FAILURE_DECAY_INTERVAL (60s by default). The failure test
# shortens both so a failed manager is retired within its window; 15s is
# three manager heartbeat intervals, so a live manager is never retired.
FAST_MANAGER_RETIREMENT = {"GATE_DEAD_PEER_REAP_INTERVAL": 15.0, "DISCOVERY_FAILURE_DECAY_INTERVAL": 1.0}


@pytest.fixture
def gate_env_updates() -> dict[str, float]:
    """Env settings the gates get beyond the managers' (none by default)."""
    return {}


@pytest.fixture
async def gate_cluster(
    node_directory: pathlib.Path,
    request: pytest.FixtureRequest,
    gate_env_updates: dict[str, float],
) -> AsyncIterator[tuple[dict[str, list[int]], list[GateServer], list[ManagerServer]]]:
    """``request.param`` = (gate count, managers per datacenter, datacenter
    count): started gates configured with every datacenter's managers,
    then started managers seeded with their datacenter's peers and every
    gate; yields each datacenter's manager TCP ports, the gates and the
    managers. The test may replace a manager in the list; every node in
    the lists is stopped after the test, managers first."""
    gate_count, managers_per_datacenter, datacenter_count = request.param
    all_ports = reserve_node_ports(gate_count + managers_per_datacenter * datacenter_count)
    gate_ports = all_ports[:gate_count]
    manager_ports_by_datacenter = {
        f"DC-{datacenter_index + 1}": all_ports[
            gate_count + datacenter_index * managers_per_datacenter : gate_count
            + (datacenter_index + 1) * managers_per_datacenter
        ]
        for datacenter_index in range(datacenter_count)
    }
    manager_env = node_env(
        node_directory,
        MERCURY_SYNC_LOG_LEVEL=QUIET_LOG_LEVEL,
        MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT,
    )
    gate_env = node_env(
        node_directory,
        MERCURY_SYNC_LOG_LEVEL=QUIET_LOG_LEVEL,
        MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT,
        **gate_env_updates,
    )
    gates = [new_gate(gate_env, gate_port, gate_ports, manager_ports_by_datacenter) for gate_port in gate_ports]
    managers = [
        new_manager(manager_env, manager_port, manager_ports, datacenter_id, gate_ports)
        for datacenter_id, manager_ports in manager_ports_by_datacenter.items()
        for manager_port in manager_ports
    ]
    started_managers: list[ManagerServer] = []
    try:
        await asyncio.wait_for(asyncio.gather(*[gate.start() for gate in gates]), timeout=NODE_START_SECONDS)
        started_managers.extend(managers)
        await asyncio.wait_for(
            asyncio.gather(*[manager.start() for manager in managers]),
            timeout=NODE_START_SECONDS,
        )
        yield manager_ports_by_datacenter, gates, started_managers
    finally:
        try:
            await stop_nodes(started_managers, within_seconds=NODE_STOP_SECONDS)
        finally:
            await stop_nodes(gates, within_seconds=NODE_STOP_SECONDS)


def gate_discovery_problems(gates: list[GateServer], manager_ports_by_datacenter: dict[str, list[int]]) -> list[str]:
    """Every (gate, datacenter) whose discovery service is missing, tracks
    fewer than the datacenter's managers, or whose gate has not heard each
    of those managers' heartbeats."""
    return [
        f"gate {gate._tcp_port} {datacenter_id}: discovery tracks "
        f"{discovery.peer_count if (discovery := gate._dc_manager_discovery.get(datacenter_id)) else 'no service'}, "
        f"heard {len(gate._modular_state.get_datacenter_manager_statuses(datacenter_id))} heartbeats, "
        f"of {len(manager_ports)} managers"
        for gate in gates
        for datacenter_id, manager_ports in manager_ports_by_datacenter.items()
        if (discovery := gate._dc_manager_discovery.get(datacenter_id)) is None
        or discovery.peer_count < len(manager_ports)
        or len(gate._modular_state.get_datacenter_manager_statuses(datacenter_id)) < len(manager_ports)
    ]


@pytest.mark.parametrize(
    "gate_cluster",
    [(2, 2, 1), (3, 3, 1), (3, 5, 1), (2, 2, 2), (3, 3, 2), (3, 2, 3)],
    indirect=True,
    ids=lambda counts: f"{counts[0]}_gates_{counts[1]}_managers_per_dc_{counts[2]}_dcs",
)
async def test_gates_discover_every_datacenters_managers_and_managers_register_gates(
    gate_cluster: tuple[dict[str, list[int]], list[GateServer], list[ManagerServer]],
) -> None:
    """Every gate tracks and has heard every manager of every datacenter,
    and every manager has at least one gate registered."""
    manager_ports_by_datacenter, gates, managers = gate_cluster
    await wait_until(
        lambda: not gate_discovery_problems(gates, manager_ports_by_datacenter)
        and all(len(manager._manager_state.get_known_gate_values()) >= 1 for manager in managers),
        within_seconds=DISCOVERY_SECONDS,
        description="every gate discovering every manager and every manager registering a gate",
    )

    discovery_problems = gate_discovery_problems(gates, manager_ports_by_datacenter)
    assert not discovery_problems, f"gates missing managers: {discovery_problems}"

    for manager in managers:
        registered_gate_count = len(manager._manager_state.get_known_gate_values())
        assert registered_gate_count >= 1, f"manager {manager._tcp_port} has {registered_gate_count} gates registered"


@pytest.mark.parametrize(
    "gate_cluster",
    [(2, 3, 1)],
    indirect=True,
    ids=lambda counts: f"{counts[0]}_gates_{counts[1]}_managers",
)
async def test_manager_identity_gate_registration_and_gate_manager_ordering(
    gate_cluster: tuple[dict[str, list[int]], list[GateServer], list[ManagerServer]],
) -> None:
    """Each manager keeps its configured datacenter and ports and has a
    gate registered; each gate discovers the datacenter's managers and
    orders them for dispatch of a job."""
    manager_ports_by_datacenter, gates, managers = gate_cluster
    await wait_until(
        lambda: not gate_discovery_problems(gates, manager_ports_by_datacenter)
        and all(len(manager._manager_state.get_known_gate_values()) >= 1 for manager in managers),
        within_seconds=DISCOVERY_SECONDS,
        description="every gate discovering every manager and every manager registering a gate",
    )

    configured_managers = [
        (datacenter_id, manager_port)
        for datacenter_id, manager_ports in manager_ports_by_datacenter.items()
        for manager_port in manager_ports
    ]
    for (datacenter_id, manager_port), manager in zip(configured_managers, managers, strict=True):
        assert manager._node_id.datacenter == datacenter_id, (
            f"manager {manager_port} is in datacenter {manager._node_id.datacenter}, expected {datacenter_id}"
        )
        assert (manager._tcp_port, manager._udp_port) == (manager_port, manager_port + 1), (
            f"manager {manager_port} listens on TCP {manager._tcp_port} UDP {manager._udp_port}, "
            f"expected TCP {manager_port} UDP {manager_port + 1}"
        )
        registered_gate_count = len(manager._manager_state.get_known_gate_values())
        assert registered_gate_count >= 1, f"manager {manager_port} has {registered_gate_count} gates registered"

    discovery_problems = gate_discovery_problems(gates, manager_ports_by_datacenter)
    assert not discovery_problems, f"gates missing managers: {discovery_problems}"

    for gate_index, gate in enumerate(gates):
        for datacenter_id in manager_ports_by_datacenter:
            ordered_managers = gate._manager_selector.ordered_managers(
                datacenter_id,
                f"job-{gate_index}",
                gate._datacenter_managers.get(datacenter_id, []),
            )
            assert ordered_managers, f"gate {gate._tcp_port} ordered no manager of {datacenter_id} for dispatch"


@pytest.mark.parametrize("gate_env_updates", [FAST_MANAGER_RETIREMENT], ids=["fast_manager_retirement"])
@pytest.mark.parametrize(
    "gate_cluster",
    [(2, 3, 1), (3, 3, 1)],
    indirect=True,
    ids=lambda counts: f"{counts[0]}_gates_{counts[1]}_managers",
)
async def test_failed_manager_is_retired_from_gate_discovery_and_tracked_after_restart(
    node_directory: pathlib.Path,
    gate_cluster: tuple[dict[str, list[int]], list[GateServer], list[ManagerServer]],
) -> None:
    """A crashed manager is retired from every gate's discovery service;
    a manager restarted on its ports is tracked by every gate again."""
    manager_ports_by_datacenter, gates, managers = gate_cluster
    [(datacenter_id, manager_ports)] = manager_ports_by_datacenter.items()
    manager_count = len(manager_ports)
    await wait_until(
        lambda: not gate_discovery_problems(gates, manager_ports_by_datacenter),
        within_seconds=DISCOVERY_SECONDS,
        description="every gate discovering every manager",
    )

    # Out of the teardown list before it stops: it is stopped exactly once.
    failed_manager = managers.pop()
    await failed_manager.stop(drain_timeout=CRASH_DRAIN_SECONDS, broadcast_leave=False)

    await wait_until(
        lambda: all(gate._dc_manager_discovery[datacenter_id].peer_count <= manager_count - 1 for gate in gates),
        within_seconds=FAILURE_DETECTION_SECONDS,
        description=f"every gate retiring the failed manager {failed_manager._tcp_port}",
    )
    for gate in gates:
        tracked_manager_count = gate._dc_manager_discovery[datacenter_id].peer_count
        assert tracked_manager_count <= manager_count - 1, (
            f"gate {gate._tcp_port} still tracks {tracked_manager_count} managers after a failure"
        )

    recovered_manager = new_manager(
        node_env(node_directory, MERCURY_SYNC_LOG_LEVEL=QUIET_LOG_LEVEL, MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT),
        manager_ports[-1],
        manager_ports,
        datacenter_id,
        [gate._tcp_port for gate in gates],
    )
    managers.append(recovered_manager)
    await asyncio.wait_for(recovered_manager.start(), timeout=NODE_START_SECONDS)

    await wait_until(
        lambda: all(gate._dc_manager_discovery[datacenter_id].peer_count >= manager_count for gate in gates),
        within_seconds=RECOVERY_SECONDS,
        description=f"every gate tracking the restarted manager {recovered_manager._tcp_port}",
    )
    for gate in gates:
        tracked_manager_count = gate._dc_manager_discovery[datacenter_id].peer_count
        assert tracked_manager_count >= manager_count, (
            f"gate {gate._tcp_port} tracks {tracked_manager_count} of {manager_count} managers after recovery"
        )
