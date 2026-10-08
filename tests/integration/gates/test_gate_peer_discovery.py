"""
Gate-to-gate peer discovery (AD-28): gates started in-process on loopback
with each other as configured peers discover every peer in their
DiscoveryService and track it as active; their state, addresses and
UDP-to-TCP peer mappings are consistent; selecting a peer for a key is
deterministic and yields a valid address, and latency feedback moves a
peer's effective latency; a gate that fails is dropped from its peers'
active set, and once restarted on the same address it sees every peer
and every peer sees it again.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.nodes import GateServer
from tests.integration.gates.gate_cluster import new_gate, new_gates, start_nodes
from tests.integration.in_process_nodes import LOCALHOST, reserve_node_ports, stop_nodes, wait_until

# Shorter SWIM suspicion timeouts, so a failed gate is detected sooner.
GATE_ENV_OVERRIDES: dict[str, str | float] = {
    "MERCURY_SYNC_LOG_LEVEL": "error",
    "MERCURY_SYNC_REQUEST_TIMEOUT": "5s",
    "SWIM_SUSPICION_MIN_TIMEOUT": 1.0,
    "SWIM_SUSPICION_MAX_TIMEOUT": 3.0,
}
NODE_START_SECONDS = 30.0
# The script this replaces checked discovery 10-25s after start (scaled
# with cluster size); this bounds the same convergence.
PEER_DISCOVERY_SECONDS = 60.0
# The script waited 15s per gate for SWIM to detect the failed gate.
FAILURE_DETECTION_SECONDS_PER_GATE = 15.0
# The script waited 20s for the restarted gate to rejoin.
RECOVERY_SECONDS = 60.0
SHUTDOWN_SECONDS = 30.0
SELECTION_KEYS = ("test-key-1", "test-key-2", "workflow-abc")
SELECTIONS_PER_KEY = 3
RECORDED_SUCCESS_LATENCIES_MS = (10.0, 15.0, 12.0)
HIGHEST_PORT = 65535


@pytest.fixture
async def running_gates(
    node_directory: pathlib.Path,
    request: pytest.FixtureRequest,
) -> AsyncIterator[list[GateServer]]:
    """``request.param`` gates, each with every other as a configured peer
    and no datacenter managers, started together. The fixture stops every
    gate in the list at teardown, so a test that replaces a gate swaps it
    in the list."""
    gate_count: int = request.param
    gates = new_gates(node_directory, reserve_node_ports(gate_count), {}, **GATE_ENV_OVERRIDES)
    try:
        await start_nodes(gates, NODE_START_SECONDS)
        yield gates
    finally:
        await stop_nodes(gates, SHUTDOWN_SECONDS)


@pytest.mark.parametrize("running_gates", [2, 3, 5], indirect=True)
async def test_every_gate_discovers_and_tracks_every_peer(running_gates: list[GateServer]) -> None:
    expected_peer_count = len(running_gates) - 1

    await wait_until(
        lambda: all(
            gate._peer_discovery.peer_count >= expected_peer_count
            and gate._modular_state.get_active_peer_count() >= expected_peer_count
            for gate in running_gates
        ),
        within_seconds=PEER_DISCOVERY_SECONDS,
        description=f"every gate discovering and tracking {expected_peer_count} active peers",
    )

    for gate in running_gates:
        discovered_peer_count = gate._peer_discovery.peer_count
        active_peer_count = gate._modular_state.get_active_peer_count()
        assert discovered_peer_count >= expected_peer_count, (
            f"gate {gate._node_id.short} has {discovered_peer_count}/{expected_peer_count} peers in discovery"
        )
        assert active_peer_count >= expected_peer_count, (
            f"gate {gate._node_id.short} tracks {active_peer_count}/{expected_peer_count} active peers"
        )


@pytest.mark.parametrize("running_gates", [3], indirect=True)
async def test_gate_state_addresses_and_peer_tracking_hold_after_heartbeat_exchange(
    running_gates: list[GateServer],
) -> None:
    expected_peer_count = len(running_gates) - 1
    gate_ports = {gate._node_id.short: (gate._tcp_port, gate._udp_port) for gate in running_gates}

    await wait_until(
        lambda: all(
            gate._modular_state.get_active_peer_count() >= expected_peer_count
            and len(gate._modular_state.get_all_udp_to_tcp_mappings()) >= expected_peer_count
            and gate._peer_discovery.peer_count >= expected_peer_count
            for gate in running_gates
        ),
        within_seconds=PEER_DISCOVERY_SECONDS,
        description=f"every gate tracking and mapping {expected_peer_count} peers after exchanging heartbeats",
    )

    for gate in running_gates:
        gate_name = gate._node_id.short
        assert gate._node_id and str(gate._node_id), "a gate has no node id"
        active_peer_count = gate._modular_state.get_active_peer_count()
        assert active_peer_count >= expected_peer_count, (
            f"gate {gate_name} tracks {active_peer_count}/{expected_peer_count} active peers"
        )
        gate_state = gate._modular_state.get_gate_state().value
        assert gate_state in {"syncing", "active", "draining"}, f"gate {gate_name} is in invalid state {gate_state}"
        assert (gate._tcp_port, gate._udp_port) == gate_ports[gate_name], (
            f"gate {gate_name} moved to TCP:{gate._tcp_port} UDP:{gate._udp_port} from {gate_ports[gate_name]}"
        )
        udp_to_tcp_mapping_count = len(gate._modular_state.get_all_udp_to_tcp_mappings())
        assert udp_to_tcp_mapping_count >= expected_peer_count, (
            f"gate {gate_name} maps {udp_to_tcp_mapping_count}/{expected_peer_count} peer UDP addresses to TCP"
        )
        discovered_peer_count = gate._peer_discovery.peer_count
        assert discovered_peer_count >= expected_peer_count, (
            f"gate {gate_name} has {discovered_peer_count}/{expected_peer_count} peers in discovery"
        )
        invalid_peer_ids = [
            peer.peer_id for peer in gate._peer_discovery.get_all_peers() if not peer.host or peer.port <= 0
        ]
        assert not invalid_peer_ids, f"gate {gate_name} discovered peers with invalid addresses: {invalid_peer_ids}"


@pytest.mark.parametrize("running_gates", [3], indirect=True)
async def test_peer_selection_is_deterministic_and_latency_feedback_is_recorded(
    running_gates: list[GateServer],
) -> None:
    expected_peer_count = len(running_gates) - 1

    await wait_until(
        lambda: all(gate._peer_discovery.peer_count >= expected_peer_count for gate in running_gates),
        within_seconds=PEER_DISCOVERY_SECONDS,
        description=f"every gate discovering {expected_peer_count} peers",
    )

    for gate in running_gates:
        gate_name = gate._node_id.short
        peer_discovery = gate._peer_discovery
        for selection_key in SELECTION_KEYS:
            selections = [peer_discovery.select_peer(selection_key) for _ in range(SELECTIONS_PER_KEY)]
            assert all(selection is not None for selection in selections), (
                f"gate {gate_name} selected no peer for key {selection_key!r}"
            )
            selected_peer_ids = {selection.peer_id for selection in selections if selection is not None}
            assert len(selected_peer_ids) == 1, (
                f"gate {gate_name} selected different peers for key {selection_key!r}: {sorted(selected_peer_ids)}"
            )
            [selected_peer_id] = selected_peer_ids
            selected_address = peer_discovery.get_peer_address(selected_peer_id)
            assert selected_address is not None, (
                f"gate {gate_name} selected peer {selected_peer_id} for key {selection_key!r} with no address"
            )
            selected_host, selected_port = selected_address
            assert isinstance(selected_host, str) and isinstance(selected_port, int), (
                f"gate {gate_name} selected a malformed address {selected_address!r}"
            )
            assert 0 < selected_port <= HIGHEST_PORT, f"gate {gate_name} selected invalid port {selected_port}"

    for gate in running_gates:
        peer_discovery = gate._peer_discovery
        discovered_peers = peer_discovery.get_all_peers()
        assert discovered_peers, f"gate {gate._node_id.short} has no discovered peer to record latency for"
        feedback_peer_id = discovered_peers[0].peer_id
        for latency_ms in RECORDED_SUCCESS_LATENCIES_MS:
            peer_discovery.record_success(feedback_peer_id, latency_ms)
        peer_discovery.record_failure(feedback_peer_id)

        effective_latency_ms = peer_discovery.get_effective_latency(feedback_peer_id)
        assert effective_latency_ms > 0, (
            f"gate {gate._node_id.short} recorded no latency for peer {feedback_peer_id}: {effective_latency_ms}"
        )


@pytest.mark.parametrize("running_gates", [3, 5], indirect=True)
async def test_gates_drop_a_failed_peer_and_rediscover_it_after_restart(
    node_directory: pathlib.Path,
    running_gates: list[GateServer],
) -> None:
    gate_count = len(running_gates)
    expected_peer_count = gate_count - 1
    gate_ports = [gate._tcp_port for gate in running_gates]

    await wait_until(
        lambda: all(gate._modular_state.get_active_peer_count() >= expected_peer_count for gate in running_gates),
        within_seconds=PEER_DISCOVERY_SECONDS,
        description=f"every gate tracking {expected_peer_count} active peers before the failure",
    )

    # Fail the last gate without a leave broadcast; it leaves the fixture's
    # list first, so teardown stops only the gates still running.
    failed_gate = running_gates.pop()
    failed_gate_port = failed_gate._tcp_port
    await asyncio.wait_for(failed_gate.stop(drain_timeout=0.5, broadcast_leave=False), timeout=SHUTDOWN_SECONDS)

    expected_peer_count_after_failure = gate_count - 2
    await wait_until(
        lambda: all(
            gate._modular_state.get_active_peer_count() <= expected_peer_count_after_failure for gate in running_gates
        ),
        within_seconds=FAILURE_DETECTION_SECONDS_PER_GATE * gate_count,
        description=(
            f"every remaining gate dropping the failed gate to {expected_peer_count_after_failure} active peers"
        ),
    )
    for gate in running_gates:
        active_peer_count = gate._modular_state.get_active_peer_count()
        assert active_peer_count <= expected_peer_count_after_failure, (
            f"gate {gate._node_id.short} still tracks {active_peer_count} active peers after the failure"
        )

    # A restarted gate gets a new node id on the same address: SWIM sees a
    # rejoin from that UDP address, and the peers track it by TCP address.
    recovered_gate = new_gate(node_directory, failed_gate_port, gate_ports, {}, **GATE_ENV_OVERRIDES)
    running_gates.append(recovered_gate)
    await start_nodes([recovered_gate], NODE_START_SECONDS)

    recovered_gate_address = (LOCALHOST, failed_gate_port)
    surviving_gates = running_gates[:-1]
    await wait_until(
        lambda: recovered_gate._modular_state.get_active_peer_count() >= expected_peer_count
        and all(gate._modular_state.is_peer_active(recovered_gate_address) for gate in surviving_gates),
        within_seconds=RECOVERY_SECONDS,
        description="the restarted gate seeing every peer and every peer seeing it",
    )
    recovered_peer_count = recovered_gate._modular_state.get_active_peer_count()
    assert recovered_peer_count >= expected_peer_count, (
        f"the restarted gate sees {recovered_peer_count}/{expected_peer_count} peers"
    )
    for gate in surviving_gates:
        assert gate._modular_state.is_peer_active(recovered_gate_address), (
            f"gate {gate._node_id.short} does not see the restarted gate at {recovered_gate_address}; "
            f"active peers: {sorted(gate._modular_state.get_active_peers())}"
        )
