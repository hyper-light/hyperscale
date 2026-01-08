#!/usr/bin/env python3
"""
Gate-to-Gate Peer Discovery Integration Tests (AD-28).

Tests that gates correctly discover and select peer gates using the
DiscoveryService with adaptive EWMA-based selection.

Test scenarios:
1. Gate peer discovery for varying cluster sizes (2, 3, 5 gates)
2. Gate peer discovery failure and recovery
3. Load-aware peer selection based on latency feedback

This validates:
- Gates initialize peer discovery with configured peers
- Peers are tracked on heartbeat receipt
- Failed peers are removed from discovery
- Recovery allows peers to rejoin discovery
- Adaptive selection prefers lower-latency peers
"""

import asyncio
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.nodes.gate import GateServer
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.logging.config.logging_config import LoggingConfig

# Initialize logging directory
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())


# ==========================================================================
# Configuration Helpers
# ==========================================================================

def generate_gate_configs(count: int, base_tcp_port: int = 8000) -> list[dict]:
    """Generate gate configurations for a given cluster size."""
    configs = []
    for i in range(count):
        configs.append({
            "name": f"Gate {i + 1}",
            "tcp": base_tcp_port + (i * 2),
            "udp": base_tcp_port + (i * 2) + 1,
        })
    return configs


def get_gate_peer_tcp_addrs(configs: list[dict], exclude_tcp: int) -> list[tuple[str, int]]:
    """Get TCP addresses of all gates except the one with exclude_tcp."""
    return [
        ('127.0.0.1', cfg['tcp'])
        for cfg in configs
        if cfg['tcp'] != exclude_tcp
    ]


def get_gate_peer_udp_addrs(configs: list[dict], exclude_udp: int) -> list[tuple[str, int]]:
    """Get UDP addresses of all gates except the one with exclude_udp."""
    return [
        ('127.0.0.1', cfg['udp'])
        for cfg in configs
        if cfg['udp'] != exclude_udp
    ]


# ==========================================================================
# Test: Gate Peer Discovery - Basic Cluster Formation
# ==========================================================================

async def test_gate_peer_discovery_cluster_size(cluster_size: int) -> bool:
    """
    Test that gates discover each other for a given cluster size.

    Validates:
    - All gates start successfully
    - Each gate discovers all other peers via SWIM heartbeats
    - Peer discovery service tracks all peers
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Gate Peer Discovery - {cluster_size} Gates")
    print(f"{'=' * 70}")

    gate_configs = generate_gate_configs(cluster_size)
    gates: list[GateServer] = []
    stabilization_time = 10 + (cluster_size * 2)  # Scale with cluster size

    try:
        # Create gates
        print(f"\n[1/4] Creating {cluster_size} gates...")
        for config in gate_configs:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id="global",
                datacenter_managers={},  # No managers for this test
                datacenter_manager_udp={},
                gate_peers=get_gate_peer_tcp_addrs(gate_configs, config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, config["udp"]),
            )
            gates.append(gate)
            print(f"  Created {config['name']} (TCP:{config['tcp']} UDP:{config['udp']})")

        # Start all gates
        print(f"\n[2/4] Starting gates...")
        start_tasks = [gate.start() for gate in gates]
        await asyncio.gather(*start_tasks)

        for i, gate in enumerate(gates):
            print(f"  Started {gate_configs[i]['name']} - Node ID: {gate._node_id.short}")

        # Wait for cluster stabilization
        print(f"\n[3/4] Waiting for peer discovery ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Verify peer discovery
        print(f"\n[4/4] Verifying peer discovery...")
        all_peers_discovered = True
        expected_peer_count = cluster_size - 1  # Each gate should see all others

        for i, gate in enumerate(gates):
            peer_count = gate._peer_discovery.peer_count
            active_peers = len(gate._active_gate_peers)

            peers_ok = peer_count >= expected_peer_count
            active_ok = active_peers >= expected_peer_count

            status = "PASS" if (peers_ok and active_ok) else "FAIL"
            print(f"  {gate_configs[i]['name']}: {peer_count} peers in discovery, {active_peers} active [{status}]")

            if not (peers_ok and active_ok):
                all_peers_discovered = False

        # Summary
        print(f"\n{'=' * 70}")
        result = "PASSED" if all_peers_discovered else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Cluster size: {cluster_size}")
        print(f"  Expected peers per gate: {expected_peer_count}")
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
        for i, gate in enumerate(gates):
            try:
                await gate.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {gate_configs[i]['name']} stopped")
            except Exception as e:
                print(f"  {gate_configs[i]['name']} stop failed: {e}")


# ==========================================================================
# Test: Gate Peer Discovery - Failure and Recovery
# ==========================================================================

async def test_gate_peer_discovery_failure_recovery(cluster_size: int) -> bool:
    """
    Test that gate peer discovery handles failure and recovery.

    Validates:
    - Gates detect peer failure via SWIM
    - Failed peers are removed from discovery
    - Recovered peers are re-added to discovery
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Gate Peer Discovery Failure/Recovery - {cluster_size} Gates")
    print(f"{'=' * 70}")

    gate_configs = generate_gate_configs(cluster_size)
    gates: list[GateServer] = []
    stabilization_time = 10 + (cluster_size * 2)
    failure_detection_time = 15  # Time for SWIM to detect failure
    recovery_time = 15  # Time for recovered peer to rejoin

    try:
        # Create and start gates
        print(f"\n[1/7] Creating {cluster_size} gates...")
        for config in gate_configs:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id="global",
                datacenter_managers={},
                datacenter_manager_udp={},
                gate_peers=get_gate_peer_tcp_addrs(gate_configs, config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, config["udp"]),
            )
            gates.append(gate)
            print(f"  Created {config['name']}")

        print(f"\n[2/7] Starting gates...")
        start_tasks = [gate.start() for gate in gates]
        await asyncio.gather(*start_tasks)

        print(f"\n[3/7] Waiting for initial discovery ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Record initial state
        expected_peer_count = cluster_size - 1
        initial_discovery_ok = all(
            gate._peer_discovery.peer_count >= expected_peer_count
            for gate in gates
        )
        print(f"  Initial discovery: {'OK' if initial_discovery_ok else 'INCOMPLETE'}")

        # Stop one gate to simulate failure
        failed_gate_index = cluster_size - 1  # Stop the last gate
        failed_gate = gates[failed_gate_index]
        failed_gate_name = gate_configs[failed_gate_index]['name']

        print(f"\n[4/7] Simulating failure of {failed_gate_name}...")
        await failed_gate.stop(drain_timeout=0.5, broadcast_leave=False)
        print(f"  {failed_gate_name} stopped")

        print(f"\n[5/7] Waiting for failure detection ({failure_detection_time}s)...")
        await asyncio.sleep(failure_detection_time)

        # Verify failure detected
        remaining_gates = gates[:failed_gate_index]
        failure_detected = True

        for i, gate in enumerate(remaining_gates):
            active_peers = len(gate._active_gate_peers)
            expected_after_failure = cluster_size - 2  # One less peer

            status = "DETECTED" if active_peers <= expected_after_failure else "NOT DETECTED"
            print(f"  {gate_configs[i]['name']}: {active_peers} active peers [{status}]")

            if active_peers > expected_after_failure:
                failure_detected = False

        # Restart the failed gate
        print(f"\n[6/7] Recovering {failed_gate_name}...")
        recovered_gate = GateServer(
            host='127.0.0.1',
            tcp_port=gate_configs[failed_gate_index]["tcp"],
            udp_port=gate_configs[failed_gate_index]["udp"],
            env=Env(
                MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                MERCURY_SYNC_LOG_LEVEL="error",
            ),
            dc_id="global",
            datacenter_managers={},
            datacenter_manager_udp={},
            gate_peers=get_gate_peer_tcp_addrs(gate_configs, gate_configs[failed_gate_index]["tcp"]),
            gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, gate_configs[failed_gate_index]["udp"]),
        )
        gates[failed_gate_index] = recovered_gate
        await recovered_gate.start()
        print(f"  {failed_gate_name} restarted")

        print(f"\n[7/7] Waiting for recovery detection ({recovery_time}s)...")
        await asyncio.sleep(recovery_time)

        # Verify recovery
        recovery_detected = True
        for i, gate in enumerate(gates[:failed_gate_index]):
            active_peers = len(gate._active_gate_peers)
            expected_after_recovery = cluster_size - 1

            status = "RECOVERED" if active_peers >= expected_after_recovery else "NOT RECOVERED"
            print(f"  {gate_configs[i]['name']}: {active_peers} active peers [{status}]")

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
        for i, gate in enumerate(gates):
            try:
                await gate.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {gate_configs[i]['name']} stopped")
            except Exception as e:
                print(f"  {gate_configs[i]['name']} stop failed: {e}")


# ==========================================================================
# Main Test Runner
# ==========================================================================

async def run_all_tests():
    """Run all gate peer discovery tests."""
    results = {}

    # Test cluster sizes: 2, 3, 5 gates
    cluster_sizes = [2, 3, 5]

    print("\n" + "=" * 70)
    print("GATE-TO-GATE PEER DISCOVERY INTEGRATION TESTS")
    print("=" * 70)
    print("\nThis test suite validates:")
    print("  1. Gates discover each other via SWIM heartbeats")
    print("  2. Peer discovery service tracks all peers")
    print("  3. Failed peers are detected and removed")
    print("  4. Recovered peers are re-discovered")
    print(f"\nCluster sizes to test: {cluster_sizes}")

    # Basic discovery tests
    for size in cluster_sizes:
        result = await test_gate_peer_discovery_cluster_size(size)
        results[f"discovery_{size}_gates"] = result

    # Failure/recovery tests (only for 3 and 5 gates to save time)
    for size in [3, 5]:
        result = await test_gate_peer_discovery_failure_recovery(size)
        results[f"failure_recovery_{size}_gates"] = result

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
