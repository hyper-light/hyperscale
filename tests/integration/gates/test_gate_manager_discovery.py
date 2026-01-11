#!/usr/bin/env python3
"""
Gate-Manager Discovery Integration Tests (AD-28).

Tests that gates correctly discover and select managers using the
DiscoveryService with per-datacenter adaptive EWMA-based selection.

Test scenarios:
1. Gate-manager discovery for varying cluster sizes
2. Gate-manager discovery failure and recovery
3. Multi-datacenter manager discovery
4. Manager selection and latency feedback

This validates:
- Gates initialize per-DC manager discovery services
- Managers register with gates and are tracked in discovery
- Failed managers are detected and removed from discovery
- Recovery allows managers to rejoin discovery
- Adaptive selection prefers lower-latency managers
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.env.env import Env
from hyperscale.logging.config.logging_config import LoggingConfig

# Initialize logging directory
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())


# ==========================================================================
# Configuration Helpers
# ==========================================================================

def generate_gate_configs(count: int, base_tcp_port: int = 9200) -> list[dict]:
    """Generate gate configurations for a given cluster size."""
    configs = []
    for i in range(count):
        configs.append({
            "name": f"Gate {i + 1}",
            "tcp": base_tcp_port + (i * 2),
            "udp": base_tcp_port + (i * 2) + 1,
        })
    return configs


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


def get_all_manager_tcp_addrs(configs: list[dict]) -> list[tuple[str, int]]:
    """Get TCP addresses of all managers."""
    return [('127.0.0.1', cfg['tcp']) for cfg in configs]


def get_all_manager_udp_addrs(configs: list[dict]) -> list[tuple[str, int]]:
    """Get UDP addresses of all managers."""
    return [('127.0.0.1', cfg['udp']) for cfg in configs]


def get_all_gate_tcp_addrs(configs: list[dict]) -> list[tuple[str, int]]:
    """Get TCP addresses of all gates."""
    return [('127.0.0.1', cfg['tcp']) for cfg in configs]


def get_all_gate_udp_addrs(configs: list[dict]) -> list[tuple[str, int]]:
    """Get UDP addresses of all gates."""
    return [('127.0.0.1', cfg['udp']) for cfg in configs]


# ==========================================================================
# Test: Gate-Manager Discovery - Basic Discovery
# ==========================================================================

async def scenario_gate_manager_discovery_basic(
    gate_count: int,
    manager_count: int,
) -> bool:
    """
    Test that gates discover managers for given cluster sizes.

    Validates:
    - All nodes start successfully
    - Managers register with gates
    - Gate's per-DC manager discovery service tracks all managers
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Gate-Manager Discovery - {gate_count} Gates, {manager_count} Managers")
    print(f"{'=' * 70}")

    dc_id = "DC-TEST"
    gate_configs = generate_gate_configs(gate_count)
    manager_configs = generate_manager_configs(manager_count)

    gates: list[GateServer] = []
    managers: list[ManagerServer] = []
    stabilization_time = 15 + (gate_count + manager_count) * 2

    try:
        # Create gates
        print(f"\n[1/5] Creating {gate_count} gates...")
        datacenter_managers = {dc_id: get_all_manager_tcp_addrs(manager_configs)}
        datacenter_manager_udp = {dc_id: get_all_manager_udp_addrs(manager_configs)}

        for config in gate_configs:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                gate_peers=get_gate_peer_tcp_addrs(gate_configs, config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, config["udp"]),
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
            )
            gates.append(gate)
            print(f"  Created {config['name']} (TCP:{config['tcp']})")

        # Create managers
        print(f"\n[2/5] Creating {manager_count} managers...")
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
                gate_addrs=get_all_gate_tcp_addrs(gate_configs),
                gate_udp_addrs=get_all_gate_udp_addrs(gate_configs),
            )
            managers.append(manager)
            print(f"  Created {config['name']} (TCP:{config['tcp']})")

        # Start gates first
        print(f"\n[3/5] Starting gates...")
        await asyncio.gather(*[gate.start() for gate in gates])
        for i, gate in enumerate(gates):
            print(f"  Started {gate_configs[i]['name']} - Node ID: {gate._node_id.short}")

        # Wait for gate cluster stabilization
        print(f"  Waiting for gate cluster ({stabilization_time // 3}s)...")
        await asyncio.sleep(stabilization_time // 3)

        # Start managers
        print(f"\n[4/5] Starting managers...")
        await asyncio.gather(*[manager.start() for manager in managers])
        for i, manager in enumerate(managers):
            print(f"  Started {manager_configs[i]['name']} - Node ID: {manager._node_id.short}")

        # Wait for manager registration
        print(f"  Waiting for manager registration ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Verify gate-manager discovery
        print(f"\n[5/5] Verifying gate-manager discovery...")
        discovery_ok = True

        for i, gate in enumerate(gates):
            config = gate_configs[i]
            print(f"\n  {config['name']} manager discovery:")

            # Check per-DC discovery service
            dc_discovery = gate._dc_manager_discovery.get(dc_id)
            if dc_discovery is None:
                print(f"    DC '{dc_id}' discovery: NOT INITIALIZED [FAIL]")
                discovery_ok = False
                continue

            discovery_count = dc_discovery.peer_count
            status = "PASS" if discovery_count >= manager_count else "FAIL"
            print(f"    Discovery peers: {discovery_count}/{manager_count} [{status}]")

            if discovery_count < manager_count:
                discovery_ok = False

            # Check datacenter manager config
            dc_managers = gate._datacenter_managers.get(dc_id, [])
            print(f"    Configured managers: {len(dc_managers)}")

            # Check registration states
            reg_state = gate._dc_registration_states.get(dc_id)
            if reg_state:
                print(f"    Registration state: registered={reg_state.registered_count}, failed={reg_state.failed_count}")

        # Summary
        print(f"\n{'=' * 70}")
        result = "PASSED" if discovery_ok else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Gate count: {gate_count}")
        print(f"  Manager count: {manager_count}")
        print(f"  Manager discovery: {'PASS' if discovery_ok else 'FAIL'}")
        print(f"{'=' * 70}")

        return discovery_ok

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
        for gate in gates:
            try:
                await gate.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Test: Gate-Manager Discovery - Failure and Recovery
# ==========================================================================

async def scenario_gate_manager_discovery_failure_recovery(
    gate_count: int,
    manager_count: int,
) -> bool:
    """
    Test that gate-manager discovery handles failure and recovery.

    Validates:
    - Gates detect manager failure
    - Failed managers are removed from discovery
    - Recovered managers are re-added to discovery
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Gate-Manager Discovery Failure/Recovery - {gate_count} Gates, {manager_count} Managers")
    print(f"{'=' * 70}")

    dc_id = "DC-TEST"
    gate_configs = generate_gate_configs(gate_count)
    manager_configs = generate_manager_configs(manager_count)

    gates: list[GateServer] = []
    managers: list[ManagerServer] = []
    stabilization_time = 15 + (gate_count + manager_count) * 2
    failure_detection_time = 20
    recovery_time = 20

    try:
        # Create infrastructure
        print(f"\n[1/8] Creating infrastructure...")
        datacenter_managers = {dc_id: get_all_manager_tcp_addrs(manager_configs)}
        datacenter_manager_udp = {dc_id: get_all_manager_udp_addrs(manager_configs)}

        for config in gate_configs:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                gate_peers=get_gate_peer_tcp_addrs(gate_configs, config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, config["udp"]),
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
            )
            gates.append(gate)

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
                gate_addrs=get_all_gate_tcp_addrs(gate_configs),
                gate_udp_addrs=get_all_gate_udp_addrs(gate_configs),
            )
            managers.append(manager)

        print(f"  Created {gate_count} gates and {manager_count} managers")

        # Start gates
        print(f"\n[2/8] Starting gates...")
        await asyncio.gather(*[gate.start() for gate in gates])
        await asyncio.sleep(stabilization_time // 3)

        # Start managers
        print(f"\n[3/8] Starting managers...")
        await asyncio.gather(*[manager.start() for manager in managers])

        print(f"\n[4/8] Waiting for initial registration ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Check initial state
        initial_discovery_ok = True
        for gate in gates:
            dc_discovery = gate._dc_manager_discovery.get(dc_id)
            if dc_discovery is None or dc_discovery.peer_count < manager_count:
                initial_discovery_ok = False
                break

        print(f"  Initial discovery: {'OK' if initial_discovery_ok else 'INCOMPLETE'}")

        # Fail a manager
        failed_idx = manager_count - 1
        failed_manager = managers[failed_idx]
        failed_name = manager_configs[failed_idx]['name']

        print(f"\n[5/8] Simulating failure of {failed_name}...")
        await failed_manager.stop(drain_timeout=0.5, broadcast_leave=False)

        print(f"\n[6/8] Waiting for failure detection ({failure_detection_time}s)...")
        await asyncio.sleep(failure_detection_time)

        # Check failure detection
        failure_detected = True
        expected_after_failure = manager_count - 1

        for i, gate in enumerate(gates):
            dc_discovery = gate._dc_manager_discovery.get(dc_id)
            if dc_discovery is None:
                print(f"  {gate_configs[i]['name']}: NO DISCOVERY [FAIL]")
                failure_detected = False
                continue

            discovery_count = dc_discovery.peer_count
            detected = discovery_count <= expected_after_failure
            status = "DETECTED" if detected else "NOT DETECTED"
            print(f"  {gate_configs[i]['name']}: {discovery_count} managers [{status}]")
            if not detected:
                failure_detected = False

        # Recover the manager
        print(f"\n[7/8] Recovering {failed_name}...")
        recovered_manager = ManagerServer(
            host='127.0.0.1',
            tcp_port=manager_configs[failed_idx]["tcp"],
            udp_port=manager_configs[failed_idx]["udp"],
            env=Env(
                MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                MERCURY_SYNC_LOG_LEVEL="error",
            ),
            dc_id=dc_id,
            manager_peers=get_manager_peer_tcp_addrs(manager_configs, manager_configs[failed_idx]["tcp"]),
            manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, manager_configs[failed_idx]["udp"]),
            gate_addrs=get_all_gate_tcp_addrs(gate_configs),
            gate_udp_addrs=get_all_gate_udp_addrs(gate_configs),
        )
        managers[failed_idx] = recovered_manager
        await recovered_manager.start()

        print(f"\n[8/8] Waiting for recovery detection ({recovery_time}s)...")
        await asyncio.sleep(recovery_time)

        # Check recovery
        recovery_detected = True
        for i, gate in enumerate(gates):
            dc_discovery = gate._dc_manager_discovery.get(dc_id)
            if dc_discovery is None:
                print(f"  {gate_configs[i]['name']}: NO DISCOVERY [FAIL]")
                recovery_detected = False
                continue

            discovery_count = dc_discovery.peer_count
            recovered = discovery_count >= manager_count
            status = "RECOVERED" if recovered else "NOT RECOVERED"
            print(f"  {gate_configs[i]['name']}: {discovery_count} managers [{status}]")
            if not recovered:
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
        for manager in managers:
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        for gate in gates:
            try:
                await gate.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Test: Gate-Manager Discovery - Multi-Datacenter
# ==========================================================================

async def scenario_gate_manager_discovery_multi_dc(
    gate_count: int,
    managers_per_dc: int,
) -> bool:
    """
    Test that gates discover managers across multiple datacenters.

    Validates:
    - Gates track managers per datacenter
    - Each DC has its own DiscoveryService
    - Manager selection works within each DC
    """
    dc_ids = ["DC-EAST", "DC-WEST"]
    total_managers = len(dc_ids) * managers_per_dc

    print(f"\n{'=' * 70}")
    print(f"TEST: Gate-Manager Multi-DC Discovery - {gate_count} Gates, {total_managers} Managers ({len(dc_ids)} DCs)")
    print(f"{'=' * 70}")

    gate_configs = generate_gate_configs(gate_count)

    # Generate manager configs per DC with different port ranges
    dc_manager_configs: dict[str, list[dict]] = {}
    base_port = 9000
    for dc_id in dc_ids:
        dc_manager_configs[dc_id] = generate_manager_configs(managers_per_dc, base_tcp_port=base_port)
        base_port += managers_per_dc * 2 + 10

    gates: list[GateServer] = []
    managers: list[ManagerServer] = []
    stabilization_time = 20 + total_managers * 2

    try:
        # Build datacenter manager address maps
        datacenter_managers: dict[str, list[tuple[str, int]]] = {}
        datacenter_manager_udp: dict[str, list[tuple[str, int]]] = {}

        for dc_id, configs in dc_manager_configs.items():
            datacenter_managers[dc_id] = get_all_manager_tcp_addrs(configs)
            datacenter_manager_udp[dc_id] = get_all_manager_udp_addrs(configs)

        # Create gates
        print(f"\n[1/4] Creating {gate_count} gates...")
        for config in gate_configs:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                gate_peers=get_gate_peer_tcp_addrs(gate_configs, config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, config["udp"]),
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
            )
            gates.append(gate)
            print(f"  Created {config['name']}")

        # Create managers for each DC
        print(f"\n[2/4] Creating managers for {len(dc_ids)} datacenters...")
        for dc_id, configs in dc_manager_configs.items():
            print(f"  {dc_id}:")
            for config in configs:
                manager = ManagerServer(
                    host='127.0.0.1',
                    tcp_port=config["tcp"],
                    udp_port=config["udp"],
                    env=Env(
                        MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                        MERCURY_SYNC_LOG_LEVEL="error",
                    ),
                    dc_id=dc_id,
                    manager_peers=get_manager_peer_tcp_addrs(configs, config["tcp"]),
                    manager_udp_peers=get_manager_peer_udp_addrs(configs, config["udp"]),
                    gate_addrs=get_all_gate_tcp_addrs(gate_configs),
                    gate_udp_addrs=get_all_gate_udp_addrs(gate_configs),
                )
                managers.append(manager)
                print(f"    Created {config['name']}")

        # Start gates
        print(f"\n[3/4] Starting all nodes...")
        await asyncio.gather(*[gate.start() for gate in gates])
        print(f"  Started {gate_count} gates")

        await asyncio.sleep(stabilization_time // 3)

        await asyncio.gather(*[manager.start() for manager in managers])
        print(f"  Started {total_managers} managers")

        print(f"  Waiting for registration ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Verify multi-DC discovery
        print(f"\n[4/4] Verifying multi-DC discovery...")
        discovery_ok = True

        for i, gate in enumerate(gates):
            config = gate_configs[i]
            print(f"\n  {config['name']} per-DC discovery:")

            for dc_id in dc_ids:
                dc_discovery = gate._dc_manager_discovery.get(dc_id)
                if dc_discovery is None:
                    print(f"    {dc_id}: NOT INITIALIZED [FAIL]")
                    discovery_ok = False
                    continue

                discovery_count = dc_discovery.peer_count
                expected = managers_per_dc
                status = "PASS" if discovery_count >= expected else "FAIL"
                print(f"    {dc_id}: {discovery_count}/{expected} managers [{status}]")

                if discovery_count < expected:
                    discovery_ok = False

        # Summary
        print(f"\n{'=' * 70}")
        result = "PASSED" if discovery_ok else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Datacenters: {dc_ids}")
        print(f"  Managers per DC: {managers_per_dc}")
        print(f"  Multi-DC discovery: {'PASS' if discovery_ok else 'FAIL'}")
        print(f"{'=' * 70}")

        return discovery_ok

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
        for gate in gates:
            try:
                await gate.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Test: Gate-Manager Discovery - Manager Selection
# ==========================================================================

async def scenario_gate_manager_selection(
    gate_count: int,
    manager_count: int,
) -> bool:
    """
    Test that gates correctly select managers using DiscoveryService.

    Validates:
    - Manager selection returns valid addresses
    - Selection is deterministic for same key
    - Latency feedback is recorded correctly
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Gate-Manager Selection - {gate_count} Gates, {manager_count} Managers")
    print(f"{'=' * 70}")

    dc_id = "DC-TEST"
    gate_configs = generate_gate_configs(gate_count)
    manager_configs = generate_manager_configs(manager_count)

    gates: list[GateServer] = []
    managers: list[ManagerServer] = []
    stabilization_time = 20 + (gate_count + manager_count) * 2

    try:
        # Create infrastructure
        print(f"\n[1/4] Creating infrastructure...")
        datacenter_managers = {dc_id: get_all_manager_tcp_addrs(manager_configs)}
        datacenter_manager_udp = {dc_id: get_all_manager_udp_addrs(manager_configs)}

        for config in gate_configs:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                gate_peers=get_gate_peer_tcp_addrs(gate_configs, config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, config["udp"]),
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
            )
            gates.append(gate)

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
                gate_addrs=get_all_gate_tcp_addrs(gate_configs),
                gate_udp_addrs=get_all_gate_udp_addrs(gate_configs),
            )
            managers.append(manager)

        print(f"  Created {gate_count} gates and {manager_count} managers")

        # Start all nodes
        print(f"\n[2/4] Starting nodes...")
        await asyncio.gather(*[gate.start() for gate in gates])
        await asyncio.sleep(stabilization_time // 3)
        await asyncio.gather(*[manager.start() for manager in managers])

        print(f"  Waiting for registration ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Test manager selection
        print(f"\n[3/4] Testing manager selection...")
        selection_ok = True

        for i, gate in enumerate(gates):
            config = gate_configs[i]
            print(f"\n  {config['name']} selection tests:")

            dc_discovery = gate._dc_manager_discovery.get(dc_id)
            if dc_discovery is None:
                print(f"    DC discovery not initialized [FAIL]")
                selection_ok = False
                continue

            # Test selection for multiple keys
            test_keys = ["job-1", "job-2", "job-3"]
            for key in test_keys:
                selected = dc_discovery.select_peer(key)
                if selected is not None:
                    print(f"    select('{key}'): {selected.host}:{selected.port} [PASS]")
                else:
                    print(f"    select('{key}'): None [FAIL]")
                    selection_ok = False

            # Test selection determinism
            key = "determinism-test"
            first_selection = dc_discovery.select_peer(key)
            second_selection = dc_discovery.select_peer(key)

            if first_selection and second_selection:
                same = (first_selection.peer_id == second_selection.peer_id)
                status = "PASS" if same else "FAIL"
                print(f"    Deterministic selection: {status}")
                if not same:
                    selection_ok = False

        # Test latency feedback
        print(f"\n[4/4] Testing latency feedback...")
        feedback_ok = True

        for i, gate in enumerate(gates):
            config = gate_configs[i]
            dc_discovery = gate._dc_manager_discovery.get(dc_id)
            if dc_discovery is None:
                continue

            all_peers = dc_discovery.get_all_peers()
            if all_peers:
                test_peer = all_peers[0]

                # Record success with latency
                dc_discovery.record_success(test_peer.peer_id, 10.0)
                dc_discovery.record_success(test_peer.peer_id, 15.0)

                # Record failure
                dc_discovery.record_failure(test_peer.peer_id)

                # Check effective latency
                effective = dc_discovery.get_effective_latency(test_peer.peer_id)
                if effective > 0:
                    print(f"  {config['name']} latency feedback: effective={effective:.1f}ms [PASS]")
                else:
                    print(f"  {config['name']} latency feedback: not recorded [FAIL]")
                    feedback_ok = False

        # Summary
        print(f"\n{'=' * 70}")
        all_passed = selection_ok and feedback_ok
        result = "PASSED" if all_passed else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Manager selection: {'PASS' if selection_ok else 'FAIL'}")
        print(f"  Latency feedback: {'PASS' if feedback_ok else 'FAIL'}")
        print(f"{'=' * 70}")

        return all_passed

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
        for gate in gates:
            try:
                await gate.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Main Test Runner
# ==========================================================================

async def run_all_tests():
    """Run all gate-manager discovery tests."""
    results = {}

    print("\n" + "=" * 70)
    print("GATE-MANAGER DISCOVERY INTEGRATION TESTS")
    print("=" * 70)
    print("\nThis test suite validates:")
    print("  1. Gates discover managers via per-DC DiscoveryService")
    print("  2. Manager registration is tracked in discovery")
    print("  3. Failed managers are detected and removed")
    print("  4. Recovered managers are re-discovered")
    print("  5. Multi-datacenter discovery works correctly")
    print("  6. Manager selection and latency feedback work correctly")

    # Basic discovery tests
    print("\n--- Basic Discovery Tests ---")
    for gates, managers in [(2, 3), (3, 3)]:
        result = await scenario_gate_manager_discovery_basic(gates, managers)
        results[f"basic_{gates}g_{managers}m"] = result

    # Manager selection tests
    print("\n--- Manager Selection Tests ---")
    result = await scenario_gate_manager_selection(2, 3)
    results["selection_2g_3m"] = result

    # Multi-DC tests
    print("\n--- Multi-Datacenter Tests ---")
    result = await scenario_gate_manager_discovery_multi_dc(2, 2)
    results["multi_dc_2g_2m_per_dc"] = result

    # Failure/recovery tests
    print("\n--- Failure/Recovery Tests ---")
    result = await scenario_gate_manager_discovery_failure_recovery(2, 3)
    results["failure_recovery_2g_3m"] = result

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
