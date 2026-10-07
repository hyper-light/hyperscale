"""
Managers of one datacenter discover each other (AD-28): for clusters of
2, 3 and 5 managers every manager learns of every peer (registration) and
counts every peer active; each manager's identity, datacenter, state,
addresses and leadership term are sound, with at most one leader; and a
manager that fails is dropped from its peers' active set, then counted
active again once it restarts.

The old script also proved manager-side peer selection (deterministic
rendezvous choice per key) and EWMA latency feedback; the manager's
DiscoveryService was deleted (the manager discovery coordinator, commit
8bcef0a7), so ManagerServer has no peer selection or peer-latency
feedback left to test.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.models import ManagerState
from hyperscale.distributed.nodes import ManagerServer
from tests.integration.in_process_nodes import node_env, reserve_node_ports, stop_nodes, wait_until
from tests.integration.manager.manager_cluster import (
    CRASH_DRAIN_SECONDS,
    NODE_START_SECONDS,
    NODE_STOP_SECONDS,
    QUIET_LOG_LEVEL,
    REQUEST_TIMEOUT,
    new_manager,
)

DATACENTER_ID = "DC-TEST"
VALID_MANAGER_STATES = {ManagerState.SYNCING, ManagerState.ACTIVE, ManagerState.DRAINING}
# The script waited 10-15s + 2s per manager for discovery and 15s per
# manager for SWIM to declare a stopped one dead; SWIM's slowest detection
# leg is over a minute, so the bounds are generous.
DISCOVERY_SECONDS_PER_MANAGER = 10.0
DISCOVERY_BASE_SECONDS = 30.0
FAILURE_DETECTION_SECONDS_PER_MANAGER = 15.0
FAILURE_DETECTION_BASE_SECONDS = 60.0
RECOVERY_SECONDS = 60.0


@pytest.fixture
async def manager_cluster(
    node_directory: pathlib.Path,
    request: pytest.FixtureRequest,
) -> AsyncIterator[tuple[list[int], list[ManagerServer]]]:
    """``request.param`` started managers of one datacenter, seeded with
    each other, and their TCP ports; the test may replace a manager in
    the list, and every manager in it is stopped after the test."""
    manager_ports = reserve_node_ports(request.param)
    env = node_env(node_directory, MERCURY_SYNC_LOG_LEVEL=QUIET_LOG_LEVEL, MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT)
    managers = [new_manager(env, manager_port, manager_ports, DATACENTER_ID) for manager_port in manager_ports]
    try:
        await asyncio.wait_for(
            asyncio.gather(*[manager.start() for manager in managers]),
            timeout=NODE_START_SECONDS,
        )
        yield manager_ports, managers
    finally:
        await stop_nodes(managers, within_seconds=NODE_STOP_SECONDS)


@pytest.mark.parametrize("manager_cluster", [2, 3, 5], indirect=True, ids=lambda count: f"{count}_managers")
async def test_every_manager_discovers_every_peer(manager_cluster: tuple[list[int], list[ManagerServer]]) -> None:
    """Each manager knows every peer by registration and counts it active."""
    manager_ports, managers = manager_cluster
    expected_peer_count = len(managers) - 1
    await wait_until(
        lambda: all(
            manager._manager_state.get_known_manager_peer_count() >= expected_peer_count
            and len(manager._manager_state.get_active_manager_peers()) >= expected_peer_count
            for manager in managers
        ),
        within_seconds=DISCOVERY_BASE_SECONDS + DISCOVERY_SECONDS_PER_MANAGER * len(managers),
        description=f"every one of {len(managers)} managers discovering its {expected_peer_count} peers",
    )

    for manager in managers:
        known_peer_count = manager._manager_state.get_known_manager_peer_count()
        active_peer_count = len(manager._manager_state.get_active_manager_peers())
        assert known_peer_count >= expected_peer_count, (
            f"manager {manager._tcp_port} knows {known_peer_count} peers, expected {expected_peer_count}"
        )
        assert active_peer_count >= expected_peer_count, (
            f"manager {manager._tcp_port} counts {active_peer_count} peers active, expected {expected_peer_count}"
        )


@pytest.mark.parametrize("manager_cluster", [3], indirect=True, ids=lambda count: f"{count}_managers")
async def test_manager_identity_state_and_peer_tracking_are_sound(
    manager_cluster: tuple[list[int], list[ManagerServer]],
) -> None:
    """After discovery each manager has a node id in the configured
    datacenter, a valid state, its configured TCP/UDP ports, a
    non-negative term, every peer active and known with a usable address,
    and at most one manager leads."""
    manager_ports, managers = manager_cluster
    expected_peer_count = len(managers) - 1
    await wait_until(
        lambda: all(
            manager._manager_state.get_known_manager_peer_count() >= expected_peer_count
            and len(manager._manager_state.get_active_manager_peers()) >= expected_peer_count
            for manager in managers
        ),
        within_seconds=DISCOVERY_BASE_SECONDS + DISCOVERY_SECONDS_PER_MANAGER * len(managers),
        description=f"every one of {len(managers)} managers discovering its {expected_peer_count} peers",
    )

    for manager_port, manager in zip(manager_ports, managers, strict=True):
        assert manager._node_id is not None and manager._node_id.full, f"manager {manager_port} has no node id"
        assert manager._node_id.datacenter == DATACENTER_ID, (
            f"manager {manager_port} is in datacenter {manager._node_id.datacenter}, expected {DATACENTER_ID}"
        )

        active_peer_count = len(manager._manager_state.get_active_manager_peers())
        assert active_peer_count >= expected_peer_count, (
            f"manager {manager_port} counts {active_peer_count} peers active, expected {expected_peer_count}"
        )

        manager_state = manager._manager_state.manager_state_enum
        assert manager_state in VALID_MANAGER_STATES, f"manager {manager_port} is in invalid state {manager_state}"

        assert (manager._tcp_port, manager._udp_port) == (manager_port, manager_port + 1), (
            f"manager {manager_port} listens on TCP {manager._tcp_port} UDP {manager._udp_port}, "
            f"expected TCP {manager_port} UDP {manager_port + 1}"
        )

        current_term = manager._leader_election.state.current_term
        assert current_term >= 0, f"manager {manager_port} has invalid term {current_term}"

    leader_ports = [manager._tcp_port for manager in managers if manager.is_leader()]
    assert len(leader_ports) <= 1, f"split brain: the managers on ports {leader_ports} all lead"

    for manager in managers:
        known_peer_count = manager._manager_state.get_known_manager_peer_count()
        assert known_peer_count >= expected_peer_count, (
            f"manager {manager._tcp_port} knows {known_peer_count} peers, expected {expected_peer_count}"
        )
        unusable_peer_addresses = [
            (peer.node_id, peer.tcp_host, peer.tcp_port)
            for peer in manager._manager_state.get_known_manager_peer_values()
            if not peer.tcp_host or peer.tcp_port <= 0
        ]
        assert not unusable_peer_addresses, (
            f"manager {manager._tcp_port} knows peers with invalid addresses: {unusable_peer_addresses}"
        )


@pytest.mark.parametrize("manager_cluster", [3, 5], indirect=True, ids=lambda count: f"{count}_managers")
async def test_failed_manager_leaves_active_peers_and_rejoins_after_restart(
    node_directory: pathlib.Path,
    manager_cluster: tuple[list[int], list[ManagerServer]],
) -> None:
    """A crashed manager drops out of every survivor's active peers; a
    manager restarted on its ports is counted active by every survivor
    again."""
    manager_ports, managers = manager_cluster
    cluster_size = len(managers)
    await wait_until(
        lambda: all(
            manager._manager_state.get_known_manager_peer_count() >= cluster_size - 1 for manager in managers
        ),
        within_seconds=DISCOVERY_BASE_SECONDS + DISCOVERY_SECONDS_PER_MANAGER * cluster_size,
        description=f"every one of {cluster_size} managers discovering its {cluster_size - 1} peers",
    )

    # Out of the teardown list before it stops: it is stopped exactly once.
    failed_manager = managers.pop()
    surviving_managers = list(managers)
    await failed_manager.stop(drain_timeout=CRASH_DRAIN_SECONDS, broadcast_leave=False)

    await wait_until(
        lambda: all(
            len(manager._manager_state.get_active_manager_peers()) <= cluster_size - 2
            for manager in surviving_managers
        ),
        within_seconds=FAILURE_DETECTION_BASE_SECONDS + FAILURE_DETECTION_SECONDS_PER_MANAGER * cluster_size,
        description=f"every survivor dropping the failed manager {failed_manager._tcp_port} from its active peers",
    )
    for manager in surviving_managers:
        active_peer_count = len(manager._manager_state.get_active_manager_peers())
        assert active_peer_count <= cluster_size - 2, (
            f"manager {manager._tcp_port} still counts {active_peer_count} peers active after a failure"
        )

    recovered_manager = new_manager(
        node_env(node_directory, MERCURY_SYNC_LOG_LEVEL=QUIET_LOG_LEVEL, MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT),
        manager_ports[-1],
        manager_ports,
        DATACENTER_ID,
    )
    managers.append(recovered_manager)
    await asyncio.wait_for(recovered_manager.start(), timeout=NODE_START_SECONDS)

    await wait_until(
        lambda: all(
            len(manager._manager_state.get_active_manager_peers()) >= cluster_size - 1
            for manager in surviving_managers
        ),
        within_seconds=RECOVERY_SECONDS,
        description=f"every survivor counting the restarted manager {recovered_manager._tcp_port} active again",
    )
    for manager in surviving_managers:
        active_peer_count = len(manager._manager_state.get_active_manager_peers())
        assert active_peer_count >= cluster_size - 1, (
            f"manager {manager._tcp_port} counts {active_peer_count} peers active after recovery, "
            f"expected {cluster_size - 1}"
        )
