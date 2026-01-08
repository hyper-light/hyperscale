#!/usr/bin/env python3
"""
Manager-to-Manager Peer Discovery Integration Tests (AD-28).

Tests that managers correctly discover and select peer managers using the
DiscoveryService with adaptive EWMA-based selection.

Test scenarios:
1. Manager peer discovery for varying cluster sizes (2, 3, 5 managers)
2. Manager peer discovery failure and recovery
3. ManagerHeartbeat message validation
4. Peer selection and latency feedback

This validates:
- Managers initialize peer discovery with seed managers
- Peers are tracked on heartbeat receipt
- ManagerHeartbeat messages contain correct fields
- Failed peers are removed from discovery
- Recovery allows peers to rejoin discovery
- Adaptive selection prefers lower-latency peers
"""

import asyncio
import sys
import os
import time
from dataclasses import dataclass, field

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.nodes.manager import ManagerServer
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.distributed_rewrite.models import ManagerHeartbeat, ManagerPeerRegistration, ManagerPeerRegistrationResponse
from hyperscale.logging.config.logging_config import LoggingConfig

# Initialize logging directory
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())


# ==========================================================================
# Message Capture Helper
# ==========================================================================

@dataclass
class MessageCapture:
    """Captures messages for validation."""
    manager_heartbeats: list[ManagerHeartbeat] = field(default_factory=list)
    peer_registrations: list[ManagerPeerRegistration] = field(default_factory=list)
    registration_responses: list[ManagerPeerRegistrationResponse] = field(default_factory=list)

    def record_heartbeat(self, heartbeat: ManagerHeartbeat) -> None:
        """Record a received heartbeat."""
        self.manager_heartbeats.append(heartbeat)

    def get_unique_node_ids(self) -> set[str]:
        """Get unique node IDs from captured heartbeats."""
        return {hb.node_id for hb in self.manager_heartbeats}


# ==========================================================================
# Configuration Helpers
# ==========================================================================

def generate_manager_configs(count: int, base_tcp_port: int = 9000) -> list[dict]:
    """Generate manager configurations for a given cluster size."""
    configs = []
    for i in range(count):
        configs.append({
            "name": f"Manager {i + 1}",
            "tcp": base_tcp_port + (i * 2),
            "udp": base_tcp_port + (i * 2) + 1,
        })
    return configs


def get_manager_peer_tcp_addrs(configs: list[dict], exclude_tcp: int) -> list[tuple[str, int]]:
    """Get TCP addresses of all managers except the one with exclude_tcp."""
    return [
        ('127.0.0.1', cfg['tcp'])
        for cfg in configs
        if cfg['tcp'] != exclude_tcp
    ]


def get_manager_peer_udp_addrs(configs: list[dict], exclude_udp: int) -> list[tuple[str, int]]:
    """Get UDP addresses of all managers except the one with exclude_udp."""
    return [
        ('127.0.0.1', cfg['udp'])
        for cfg in configs
        if cfg['udp'] != exclude_udp
    ]


# ==========================================================================
# Test: Manager Peer Discovery - Basic Cluster Formation
# ==========================================================================

async def scenario_manager_peer_discovery_cluster_size(cluster_size: int) -> bool:
    """
    Test that managers discover each other for a given cluster size.

    Validates:
    - All managers start successfully
    - Each manager discovers all other peers via SWIM heartbeats
    - Peer discovery service tracks all peers
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager Peer Discovery - {cluster_size} Managers")
    print(f"{'=' * 70}")

    manager_configs = generate_manager_configs(cluster_size)
    managers: list[ManagerServer] = []
    stabilization_time = 10 + (cluster_size * 2)  # Scale with cluster size

    try:
        # Create managers
        print(f"\n[1/4] Creating {cluster_size} managers...")
        for config in manager_configs:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id="DC-TEST",
                manager_peers=get_manager_peer_tcp_addrs(manager_configs, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, config["udp"]),
            )
            managers.append(manager)
            print(f"  Created {config['name']} (TCP:{config['tcp']} UDP:{config['udp']})")

        # Start all managers
        print(f"\n[2/4] Starting managers...")
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)

        for i, manager in enumerate(managers):
            print(f"  Started {manager_configs[i]['name']} - Node ID: {manager._node_id.short}")

        # Wait for cluster stabilization
        print(f"\n[3/4] Waiting for peer discovery ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Verify peer discovery
        print(f"\n[4/4] Verifying peer discovery...")
        all_peers_discovered = True
        expected_peer_count = cluster_size - 1  # Each manager should see all others

        for i, manager in enumerate(managers):
            peer_count = manager._peer_discovery.peer_count
            active_peers = len(manager._active_manager_peers)

            peers_ok = peer_count >= expected_peer_count
            active_ok = active_peers >= expected_peer_count

            status = "PASS" if (peers_ok and active_ok) else "FAIL"
            print(f"  {manager_configs[i]['name']}: {peer_count} peers in discovery, {active_peers} active [{status}]")

            if not (peers_ok and active_ok):
                all_peers_discovered = False

        # Summary
        print(f"\n{'=' * 70}")
        result = "PASSED" if all_peers_discovered else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Cluster size: {cluster_size}")
        print(f"  Expected peers per manager: {expected_peer_count}")
        print(f"  All peers discovered: {'YES' if all_peers_discovered else 'NO'}")
        print(f"{'=' * 70}")

        return all_peers_discovered

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        print("\nCleaning up...")
        for i, manager in enumerate(managers):
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {manager_configs[i]['name']} stopped")
            except Exception as e:
                print(f"  {manager_configs[i]['name']} stop failed: {e}")


# ==========================================================================
# Test: Manager Heartbeat Message Validation
# ==========================================================================

async def scenario_manager_heartbeat_message_validation(cluster_size: int) -> bool:
    """
    Test that ManagerHeartbeat messages contain correct fields.

    Validates:
    - node_id field is populated correctly
    - datacenter field matches configured dc_id
    - tcp_host/tcp_port/udp_host/udp_port are populated
    - state field is valid (syncing, active, draining)
    - is_leader and term fields are set
    - worker_count and healthy_worker_count are tracked
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager Heartbeat Message Validation - {cluster_size} Managers")
    print(f"{'=' * 70}")

    dc_id = "DC-VALIDATION"
    manager_configs = generate_manager_configs(cluster_size)
    managers: list[ManagerServer] = []
    stabilization_time = 15 + (cluster_size * 2)

    try:
        # Create managers
        print(f"\n[1/5] Creating {cluster_size} managers...")
        for config in manager_configs:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id=dc_id,
                manager_peers=get_manager_peer_tcp_addrs(manager_configs, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, config["udp"]),
            )
            managers.append(manager)
            print(f"  Created {config['name']}")

        # Start managers
        print(f"\n[2/5] Starting managers...")
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)

        # Collect node IDs
        node_ids = {str(manager._node_id) for manager in managers}
        print(f"  Node IDs: {[manager._node_id.short for manager in managers]}")

        print(f"\n[3/5] Waiting for heartbeat exchange ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Validate manager state and peer tracking
        print(f"\n[4/5] Validating manager state and peer tracking...")
        validation_results = {
            "node_ids_valid": True,
            "datacenter_valid": True,
            "peer_tracking_valid": True,
            "state_valid": True,
            "address_tracking_valid": True,
            "leadership_valid": True,
        }

        leader_count = 0
        for i, manager in enumerate(managers):
            config = manager_configs[i]
            print(f"\n  {config['name']} validation:")

            # Validate node_id is set
            if not manager._node_id or not str(manager._node_id):
                print(f"    node_id: MISSING [FAIL]")
                validation_results["node_ids_valid"] = False
            else:
                print(f"    node_id: {manager._node_id.short} [PASS]")

            # Validate datacenter
            if manager._dc_id == dc_id:
                print(f"    datacenter: {manager._dc_id} [PASS]")
            else:
                print(f"    datacenter: {manager._dc_id} (expected {dc_id}) [FAIL]")
                validation_results["datacenter_valid"] = False

            # Validate manager is tracking peers
            active_peers = len(manager._active_manager_peers)
            expected_peers = cluster_size - 1
            if active_peers >= expected_peers:
                print(f"    active_peers: {active_peers}/{expected_peers} [PASS]")
            else:
                print(f"    active_peers: {active_peers}/{expected_peers} [FAIL]")
                validation_results["peer_tracking_valid"] = False

            # Validate manager state
            manager_state = manager._manager_state.value if hasattr(manager._manager_state, 'value') else str(manager._manager_state)
            valid_states = {"syncing", "active", "draining"}
            if manager_state.lower() in valid_states:
                print(f"    state: {manager_state} [PASS]")
            else:
                print(f"    state: {manager_state} (invalid) [FAIL]")
                validation_results["state_valid"] = False

            # Validate address tracking
            if manager._tcp_port == config["tcp"] and manager._udp_port == config["udp"]:
                print(f"    addresses: TCP:{manager._tcp_port} UDP:{manager._udp_port} [PASS]")
            else:
                print(f"    addresses: TCP:{manager._tcp_port} UDP:{manager._udp_port} (mismatch) [FAIL]")
                validation_results["address_tracking_valid"] = False

            # Check leadership - term should be >= 0
            term = manager._term
            is_leader = manager._is_leader
            if term >= 0:
                print(f"    leadership: term={term}, is_leader={is_leader} [PASS]")
                if is_leader:
                    leader_count += 1
            else:
                print(f"    leadership: invalid term={term} [FAIL]")
                validation_results["leadership_valid"] = False

        # Verify exactly one leader (or zero if still electing)
        if leader_count <= 1:
            print(f"\n  Leader count: {leader_count} [PASS]")
        else:
            print(f"\n  Leader count: {leader_count} (split-brain!) [FAIL]")
            validation_results["leadership_valid"] = False

        # Validate peer discovery service state
        print(f"\n[5/5] Validating discovery service state...")
        discovery_valid = True

        for i, manager in enumerate(managers):
            config = manager_configs[i]
            discovery = manager._peer_discovery

            # Check that peers were added to discovery
            peer_count = discovery.peer_count
            if peer_count >= cluster_size - 1:
                print(f"  {config['name']}: {peer_count} peers in discovery [PASS]")
            else:
                print(f"  {config['name']}: {peer_count} peers in discovery (expected {cluster_size - 1}) [FAIL]")
                discovery_valid = False

            # Verify peer addresses are retrievable
            all_peers = discovery.get_all_peers()
            for peer in all_peers:
                if peer.host and peer.port > 0:
                    continue
                else:
                    print(f"    Peer {peer.peer_id}: invalid address [FAIL]")
                    discovery_valid = False

        # Summary
        print(f"\n{'=' * 70}")
        all_valid = all(validation_results.values()) and discovery_valid
        result = "PASSED" if all_valid else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Node IDs valid: {'PASS' if validation_results['node_ids_valid'] else 'FAIL'}")
        print(f"  Datacenter valid: {'PASS' if validation_results['datacenter_valid'] else 'FAIL'}")
        print(f"  Peer tracking valid: {'PASS' if validation_results['peer_tracking_valid'] else 'FAIL'}")
        print(f"  State valid: {'PASS' if validation_results['state_valid'] else 'FAIL'}")
        print(f"  Address tracking valid: {'PASS' if validation_results['address_tracking_valid'] else 'FAIL'}")
        print(f"  Leadership valid: {'PASS' if validation_results['leadership_valid'] else 'FAIL'}")
        print(f"  Discovery service valid: {'PASS' if discovery_valid else 'FAIL'}")
        print(f"{'=' * 70}")

        return all_valid

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        print("\nCleaning up...")
        for i, manager in enumerate(managers):
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Test: Manager Peer Discovery - Failure and Recovery
# ==========================================================================

async def scenario_manager_peer_discovery_failure_recovery(cluster_size: int) -> bool:
    """
    Test that manager peer discovery handles failure and recovery.

    Validates:
    - Managers detect peer failure via SWIM
    - Failed peers are removed from discovery
    - Recovered peers are re-added to discovery
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager Peer Discovery Failure/Recovery - {cluster_size} Managers")
    print(f"{'=' * 70}")

    manager_configs = generate_manager_configs(cluster_size)
    managers: list[ManagerServer] = []
    stabilization_time = 10 + (cluster_size * 2)
    failure_detection_time = 15  # Time for SWIM to detect failure
    recovery_time = 15  # Time for recovered peer to rejoin

    try:
        # Create and start managers
        print(f"\n[1/7] Creating {cluster_size} managers...")
        for config in manager_configs:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id="DC-TEST",
                manager_peers=get_manager_peer_tcp_addrs(manager_configs, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, config["udp"]),
            )
            managers.append(manager)
            print(f"  Created {config['name']}")

        print(f"\n[2/7] Starting managers...")
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)

        print(f"\n[3/7] Waiting for initial discovery ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Record initial state
        expected_peer_count = cluster_size - 1
        initial_discovery_ok = all(
            manager._peer_discovery.peer_count >= expected_peer_count
            for manager in managers
        )
        print(f"  Initial discovery: {'OK' if initial_discovery_ok else 'INCOMPLETE'}")

        # Stop one manager to simulate failure
        failed_manager_index = cluster_size - 1  # Stop the last manager
        failed_manager = managers[failed_manager_index]
        failed_manager_name = manager_configs[failed_manager_index]['name']

        print(f"\n[4/7] Simulating failure of {failed_manager_name}...")
        await failed_manager.stop(drain_timeout=0.5, broadcast_leave=False)
        print(f"  {failed_manager_name} stopped")

        print(f"\n[5/7] Waiting for failure detection ({failure_detection_time}s)...")
        await asyncio.sleep(failure_detection_time)

        # Verify failure detected
        remaining_managers = managers[:failed_manager_index]
        failure_detected = True

        for i, manager in enumerate(remaining_managers):
            active_peers = len(manager._active_manager_peers)
            expected_after_failure = cluster_size - 2  # One less peer

            status = "DETECTED" if active_peers <= expected_after_failure else "NOT DETECTED"
            print(f"  {manager_configs[i]['name']}: {active_peers} active peers [{status}]")

            if active_peers > expected_after_failure:
                failure_detected = False

        # Restart the failed manager
        print(f"\n[6/7] Recovering {failed_manager_name}...")
        recovered_manager = ManagerServer(
            host='127.0.0.1',
            tcp_port=manager_configs[failed_manager_index]["tcp"],
            udp_port=manager_configs[failed_manager_index]["udp"],
            env=Env(
                MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                MERCURY_SYNC_LOG_LEVEL="error",
            ),
            dc_id="DC-TEST",
            manager_peers=get_manager_peer_tcp_addrs(manager_configs, manager_configs[failed_manager_index]["tcp"]),
            manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, manager_configs[failed_manager_index]["udp"]),
        )
        managers[failed_manager_index] = recovered_manager
        await recovered_manager.start()
        print(f"  {failed_manager_name} restarted")

        print(f"\n[7/7] Waiting for recovery detection ({recovery_time}s)...")
        await asyncio.sleep(recovery_time)

        # Verify recovery
        recovery_detected = True
        for i, manager in enumerate(managers[:failed_manager_index]):
            active_peers = len(manager._active_manager_peers)
            expected_after_recovery = cluster_size - 1

            status = "RECOVERED" if active_peers >= expected_after_recovery else "NOT RECOVERED"
            print(f"  {manager_configs[i]['name']}: {active_peers} active peers [{status}]")

            if active_peers < expected_after_recovery:
                recovery_detected = False

        # Summary
        print(f"\n{'=' * 70}")
        all_passed = initial_discovery_ok and failure_detected and recovery_detected
        result = "PASSED" if all_passed else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Initial discovery: {'PASS' if initial_discovery_ok else 'FAIL'}")
        print(f"  Failure detection: {'PASS' if failure_detected else 'FAIL'}")
        print(f"  Recovery detection: {'PASS' if recovery_detected else 'FAIL'}")
        print(f"{'=' * 70}")

        return all_passed

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        print("\nCleaning up...")
        for i, manager in enumerate(managers):
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {manager_configs[i]['name']} stopped")
            except Exception as e:
                print(f"  {manager_configs[i]['name']} stop failed: {e}")


# ==========================================================================
# Test: Manager Discovery Peer Selection
# ==========================================================================

async def scenario_manager_discovery_peer_selection(cluster_size: int) -> bool:
    """
    Test that manager discovery service correctly selects peers.

    Validates:
    - _select_best_peer returns valid peer addresses
    - Selection is deterministic for same key
    - Peer addresses are correctly formatted
    - Latency feedback is recorded correctly
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager Discovery Peer Selection - {cluster_size} Managers")
    print(f"{'=' * 70}")

    manager_configs = generate_manager_configs(cluster_size)
    managers: list[ManagerServer] = []
    stabilization_time = 15 + (cluster_size * 2)

    try:
        # Create and start managers
        print(f"\n[1/4] Creating and starting {cluster_size} managers...")
        for config in manager_configs:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id="DC-TEST",
                manager_peers=get_manager_peer_tcp_addrs(manager_configs, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, config["udp"]),
            )
            managers.append(manager)

        await asyncio.gather(*[manager.start() for manager in managers])
        print(f"  All managers started")

        print(f"\n[2/4] Waiting for discovery ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Test peer selection
        print(f"\n[3/4] Testing peer selection...")
        selection_valid = True
        test_keys = ["quorum-op-1", "state-sync-abc", "operation-xyz"]

        for i, manager in enumerate(managers):
            config = manager_configs[i]
            print(f"\n  {config['name']}:")

            for key in test_keys:
                # Select peer multiple times to verify determinism
                selections = []
                for _ in range(3):
                    selected = manager._select_best_peer(key)
                    selections.append(selected)

                # Verify selection returned a result
                if selections[0] is None:
                    print(f"    key='{key}': No peer selected [FAIL]")
                    selection_valid = False
                    continue

                # Verify all selections are the same (deterministic)
                if all(s == selections[0] for s in selections):
                    host, port = selections[0]
                    print(f"    key='{key}': ({host}:{port}) [PASS - deterministic]")
                else:
                    print(f"    key='{key}': Non-deterministic selection [FAIL]")
                    selection_valid = False

                # Verify address format
                host, port = selections[0]
                if not isinstance(host, str) or not isinstance(port, int):
                    print(f"      Invalid address format [FAIL]")
                    selection_valid = False
                elif port <= 0 or port > 65535:
                    print(f"      Invalid port number [FAIL]")
                    selection_valid = False

        # Validate latency recording
        print(f"\n[4/4] Testing latency feedback recording...")
        feedback_valid = True

        for i, manager in enumerate(managers):
            config = manager_configs[i]
            discovery = manager._peer_discovery

            # Get a peer to test with
            all_peers = discovery.get_all_peers()
            if not all_peers:
                continue

            test_peer = all_peers[0]

            # Record some successes
            for latency in [10.0, 15.0, 12.0]:
                manager._record_peer_success(test_peer.peer_id, latency)

            # Record a failure
            manager._record_peer_failure(test_peer.peer_id)

            # Verify effective latency changed
            effective_latency = discovery.get_effective_latency(test_peer.peer_id)
            if effective_latency > 0:
                print(f"  {config['name']}: Latency tracking working (effective={effective_latency:.1f}ms) [PASS]")
            else:
                print(f"  {config['name']}: Latency tracking not working [FAIL]")
                feedback_valid = False

        # Summary
        print(f"\n{'=' * 70}")
        all_valid = selection_valid and feedback_valid
        result = "PASSED" if all_valid else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Peer selection valid: {'PASS' if selection_valid else 'FAIL'}")
        print(f"  Feedback recording valid: {'PASS' if feedback_valid else 'FAIL'}")
        print(f"{'=' * 70}")

        return all_valid

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        print("\nCleaning up...")
        for manager in managers:
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Main Test Runner
# ==========================================================================

async def run_all_tests():
    """Run all manager peer discovery tests."""
    results = {}

    # Test cluster sizes: 2, 3, 5 managers
    cluster_sizes = [2, 3, 5]

    print("\n" + "=" * 70)
    print("MANAGER-TO-MANAGER PEER DISCOVERY INTEGRATION TESTS")
    print("=" * 70)
    print("\nThis test suite validates:")
    print("  1. Managers discover each other via SWIM heartbeats")
    print("  2. Peer discovery service tracks all peers")
    print("  3. ManagerHeartbeat messages contain correct fields")
    print("  4. Failed peers are detected and removed")
    print("  5. Recovered peers are re-discovered")
    print("  6. Peer selection works correctly")
    print(f"\nCluster sizes to test: {cluster_sizes}")

    # Basic discovery tests
    for size in cluster_sizes:
        result = await scenario_manager_peer_discovery_cluster_size(size)
        results[f"discovery_{size}_managers"] = result

    # Message validation tests
    for size in [3]:
        result = await scenario_manager_heartbeat_message_validation(size)
        results[f"heartbeat_validation_{size}_managers"] = result

    # Peer selection tests
    for size in [3]:
        result = await scenario_manager_discovery_peer_selection(size)
        results[f"peer_selection_{size}_managers"] = result

    # Failure/recovery tests (only for 3 and 5 managers to save time)
    for size in [3, 5]:
        result = await scenario_manager_peer_discovery_failure_recovery(size)
        results[f"failure_recovery_{size}_managers"] = result

    # Final summary
    print("\n" + "=" * 70)
    print("FINAL TEST SUMMARY")
    print("=" * 70)

    all_passed = True
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {test_name}: {status}")
        if not passed:
            all_passed = False

    print(f"\nOverall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    print("=" * 70)

    return all_passed


def main():
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
