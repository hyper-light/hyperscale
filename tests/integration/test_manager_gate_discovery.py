#!/usr/bin/env python3
"""
Manager-Gate Discovery Integration Tests (AD-28).

Tests that managers and gates correctly discover each other using the
DiscoveryService with adaptive EWMA-based selection across multiple datacenters.

Test scenarios:
1. Manager-gate discovery for varying cluster sizes and DC counts
2. Manager-gate discovery failure and recovery
3. Cross-datacenter discovery and locality awareness

This validates:
- Gates discover managers in multiple datacenters
- Managers register with gates successfully
- Per-DC manager discovery tracking
- Failed nodes are detected and removed
- Recovery allows nodes to rejoin discovery
"""

import asyncio
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.nodes.gate import GateServer
from hyperscale.distributed_rewrite.nodes.manager import ManagerServer
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


def generate_manager_configs_for_dc(
    dc_id: str,
    count: int,
    base_tcp_port: int,
) -> list[dict]:
    """Generate manager configurations for a given DC."""
    configs = []
    for i in range(count):
        configs.append({
            "name": f"{dc_id} Manager {i + 1}",
            "dc_id": dc_id,
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


def get_all_gate_tcp_addrs(configs: list[dict]) -> list[tuple[str, int]]:
    """Get TCP addresses of all gates."""
    return [('127.0.0.1', cfg['tcp']) for cfg in configs]


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


def get_dc_manager_tcp_addrs(configs: list[dict]) -> list[tuple[str, int]]:
    """Get TCP addresses of all managers in a DC."""
    return [('127.0.0.1', cfg['tcp']) for cfg in configs]


def get_dc_manager_udp_addrs(configs: list[dict]) -> list[tuple[str, int]]:
    """Get UDP addresses of all managers in a DC."""
    return [('127.0.0.1', cfg['udp']) for cfg in configs]


# ==========================================================================
# Test: Manager-Gate Discovery - Single DC
# ==========================================================================

async def test_manager_gate_discovery_single_dc(
    gate_count: int,
    manager_count: int,
) -> bool:
    """
    Test manager-gate discovery in a single datacenter.

    Validates:
    - Gates start and discover managers
    - Managers register with gates
    - Per-DC discovery service tracks managers
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager-Gate Discovery - {gate_count} Gates, {manager_count} Managers (1 DC)")
    print(f"{'=' * 70}")

    dc_id = "DC-TEST"
    gate_configs = generate_gate_configs(gate_count)
    manager_configs = generate_manager_configs_for_dc(dc_id, manager_count, base_tcp_port=9000)

    gates: list[GateServer] = []
    managers: list[ManagerServer] = []
    stabilization_time = 15 + (gate_count + manager_count) * 2

    try:
        # Create gates
        print(f"\n[1/5] Creating {gate_count} gates...")
        datacenter_managers = {dc_id: get_dc_manager_tcp_addrs(manager_configs)}
        datacenter_manager_udp = {dc_id: get_dc_manager_udp_addrs(manager_configs)}

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
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
                gate_peers=get_gate_peer_tcp_addrs(gate_configs, config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, config["udp"]),
            )
            gates.append(gate)
            print(f"  Created {config['name']}")

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
            )
            managers.append(manager)
            print(f"  Created {config['name']}")

        # Start gates first
        print(f"\n[3/5] Starting gates...")
        start_tasks = [gate.start() for gate in gates]
        await asyncio.gather(*start_tasks)
        print(f"  All gates started")

        # Start managers
        print(f"\n[4/5] Starting managers...")
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)
        print(f"  All managers started")

        # Wait for discovery
        print(f"\n[5/5] Waiting for discovery ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Verify gate discovery of managers
        print(f"\n  Gate Discovery Results:")
        gates_discovery_ok = True

        for i, gate in enumerate(gates):
            dc_discovery = gate._dc_manager_discovery.get(dc_id)
            if dc_discovery:
                manager_peer_count = dc_discovery.peer_count
                managers_ok = manager_peer_count >= manager_count
                status = "PASS" if managers_ok else "FAIL"
                print(f"    {gate_configs[i]['name']}: {manager_peer_count}/{manager_count} managers in {dc_id} [{status}]")
                if not managers_ok:
                    gates_discovery_ok = False
            else:
                print(f"    {gate_configs[i]['name']}: No discovery for {dc_id} [FAIL]")
                gates_discovery_ok = False

        # Verify manager registration with gates
        print(f"\n  Manager Gate Registration:")
        managers_registered_ok = True

        for i, manager in enumerate(managers):
            registered_gates = len(manager._registered_with_gates)
            gates_ok = registered_gates >= 1  # Should register with at least one gate
            status = "PASS" if gates_ok else "FAIL"
            print(f"    {manager_configs[i]['name']}: registered with {registered_gates} gates [{status}]")
            if not gates_ok:
                managers_registered_ok = False

        # Summary
        print(f"\n{'=' * 70}")
        all_passed = gates_discovery_ok and managers_registered_ok
        result = "PASSED" if all_passed else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Gates discovered managers: {'PASS' if gates_discovery_ok else 'FAIL'}")
        print(f"  Managers registered with gates: {'PASS' if managers_registered_ok else 'FAIL'}")
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
            except Exception:
                pass

        for i, gate in enumerate(gates):
            try:
                await gate.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Test: Manager-Gate Discovery - Multi-DC
# ==========================================================================

async def test_manager_gate_discovery_multi_dc(
    gate_count: int,
    managers_per_dc: int,
    dc_count: int,
) -> bool:
    """
    Test manager-gate discovery across multiple datacenters.

    Validates:
    - Gates discover managers in multiple DCs
    - Per-DC discovery services track managers correctly
    - Cross-DC awareness works properly
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager-Gate Discovery - {gate_count} Gates, {managers_per_dc} Managers/DC, {dc_count} DCs")
    print(f"{'=' * 70}")

    gate_configs = generate_gate_configs(gate_count)

    # Generate manager configs per DC
    dc_ids = [f"DC-{i + 1}" for i in range(dc_count)]
    dc_manager_configs: dict[str, list[dict]] = {}

    for dc_idx, dc_id in enumerate(dc_ids):
        base_port = 9000 + (dc_idx * 100)  # Offset ports per DC
        dc_manager_configs[dc_id] = generate_manager_configs_for_dc(
            dc_id,
            managers_per_dc,
            base_tcp_port=base_port,
        )

    gates: list[GateServer] = []
    all_managers: list[ManagerServer] = []
    stabilization_time = 20 + (gate_count + managers_per_dc * dc_count) * 2

    try:
        # Create gates
        print(f"\n[1/5] Creating {gate_count} gates...")
        datacenter_managers = {
            dc_id: get_dc_manager_tcp_addrs(configs)
            for dc_id, configs in dc_manager_configs.items()
        }
        datacenter_manager_udp = {
            dc_id: get_dc_manager_udp_addrs(configs)
            for dc_id, configs in dc_manager_configs.items()
        }

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
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
                gate_peers=get_gate_peer_tcp_addrs(gate_configs, config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, config["udp"]),
            )
            gates.append(gate)
            print(f"  Created {config['name']}")

        # Create managers for each DC
        print(f"\n[2/5] Creating managers ({managers_per_dc} per DC)...")
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
                )
                all_managers.append(manager)
                print(f"    Created {config['name']}")

        # Start gates first
        print(f"\n[3/5] Starting gates...")
        start_tasks = [gate.start() for gate in gates]
        await asyncio.gather(*start_tasks)
        print(f"  All gates started")

        # Start all managers
        print(f"\n[4/5] Starting managers...")
        start_tasks = [manager.start() for manager in all_managers]
        await asyncio.gather(*start_tasks)
        print(f"  All managers started")

        # Wait for discovery
        print(f"\n[5/5] Waiting for discovery ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Verify per-DC discovery
        print(f"\n  Gate Per-DC Discovery Results:")
        per_dc_discovery_ok = True

        for i, gate in enumerate(gates):
            print(f"    {gate_configs[i]['name']}:")
            for dc_id in dc_ids:
                dc_discovery = gate._dc_manager_discovery.get(dc_id)
                if dc_discovery:
                    manager_peer_count = dc_discovery.peer_count
                    managers_ok = manager_peer_count >= managers_per_dc
                    status = "PASS" if managers_ok else "FAIL"
                    print(f"      {dc_id}: {manager_peer_count}/{managers_per_dc} managers [{status}]")
                    if not managers_ok:
                        per_dc_discovery_ok = False
                else:
                    print(f"      {dc_id}: No discovery [FAIL]")
                    per_dc_discovery_ok = False

        # Summary
        print(f"\n{'=' * 70}")
        result = "PASSED" if per_dc_discovery_ok else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Configuration: {gate_count} gates, {managers_per_dc} managers/DC, {dc_count} DCs")
        print(f"  Total managers: {managers_per_dc * dc_count}")
        print(f"  Per-DC discovery: {'PASS' if per_dc_discovery_ok else 'FAIL'}")
        print(f"{'=' * 70}")

        return per_dc_discovery_ok

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        print("\nCleaning up...")
        for manager in all_managers:
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
# Test: Manager-Gate Discovery - Failure and Recovery
# ==========================================================================

async def test_manager_gate_discovery_failure_recovery(
    gate_count: int,
    manager_count: int,
) -> bool:
    """
    Test manager-gate discovery handles failure and recovery.

    Validates:
    - Gates detect manager failure
    - Failed managers are removed from per-DC discovery
    - Recovered managers are re-added
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager-Gate Discovery Failure/Recovery - {gate_count} Gates, {manager_count} Managers")
    print(f"{'=' * 70}")

    dc_id = "DC-TEST"
    gate_configs = generate_gate_configs(gate_count)
    manager_configs = generate_manager_configs_for_dc(dc_id, manager_count, base_tcp_port=9000)

    gates: list[GateServer] = []
    managers: list[ManagerServer] = []
    stabilization_time = 15 + (gate_count + manager_count) * 2
    failure_detection_time = 15
    recovery_time = 15

    try:
        # Create and start infrastructure
        print(f"\n[1/8] Creating infrastructure...")
        datacenter_managers = {dc_id: get_dc_manager_tcp_addrs(manager_configs)}
        datacenter_manager_udp = {dc_id: get_dc_manager_udp_addrs(manager_configs)}

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
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
                gate_peers=get_gate_peer_tcp_addrs(gate_configs, config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(gate_configs, config["udp"]),
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
            )
            managers.append(manager)

        print(f"  Created {gate_count} gates and {manager_count} managers")

        print(f"\n[2/8] Starting gates...")
        await asyncio.gather(*[gate.start() for gate in gates])

        print(f"\n[3/8] Starting managers...")
        await asyncio.gather(*[manager.start() for manager in managers])

        print(f"\n[4/8] Waiting for initial discovery ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Check initial state
        initial_discovery_ok = True
        for gate in gates:
            dc_discovery = gate._dc_manager_discovery.get(dc_id)
            if not dc_discovery or dc_discovery.peer_count < manager_count:
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
            if dc_discovery:
                peer_count = dc_discovery.peer_count
                detected = peer_count <= expected_after_failure
                status = "DETECTED" if detected else "NOT DETECTED"
                print(f"  {gate_configs[i]['name']}: {peer_count} managers [{status}]")
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
        )
        managers[failed_idx] = recovered_manager
        await recovered_manager.start()

        print(f"\n[8/8] Waiting for recovery detection ({recovery_time}s)...")
        await asyncio.sleep(recovery_time)

        # Check recovery
        recovery_detected = True
        for i, gate in enumerate(gates):
            dc_discovery = gate._dc_manager_discovery.get(dc_id)
            if dc_discovery:
                peer_count = dc_discovery.peer_count
                recovered = peer_count >= manager_count
                status = "RECOVERED" if recovered else "NOT RECOVERED"
                print(f"  {gate_configs[i]['name']}: {peer_count} managers [{status}]")
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
# Main Test Runner
# ==========================================================================

async def run_all_tests():
    """Run all manager-gate discovery tests."""
    results = {}

    print("\n" + "=" * 70)
    print("MANAGER-GATE DISCOVERY INTEGRATION TESTS")
    print("=" * 70)
    print("\nThis test suite validates:")
    print("  1. Gates discover managers in single and multiple datacenters")
    print("  2. Per-DC discovery services track managers correctly")
    print("  3. Failed nodes are detected and removed")
    print("  4. Recovered nodes are re-discovered")

    # Single DC tests
    print("\n--- Single DC Tests ---")
    for gates, managers in [(2, 2), (3, 3), (3, 5)]:
        result = await test_manager_gate_discovery_single_dc(gates, managers)
        results[f"single_dc_{gates}g_{managers}m"] = result

    # Multi-DC tests
    print("\n--- Multi-DC Tests ---")
    for gates, managers_per_dc, dcs in [(2, 2, 2), (3, 3, 2), (3, 2, 3)]:
        result = await test_manager_gate_discovery_multi_dc(gates, managers_per_dc, dcs)
        results[f"multi_dc_{gates}g_{managers_per_dc}m_{dcs}dc"] = result

    # Failure/recovery tests
    print("\n--- Failure/Recovery Tests ---")
    for gates, managers in [(2, 3), (3, 3)]:
        result = await test_manager_gate_discovery_failure_recovery(gates, managers)
        results[f"failure_recovery_{gates}g_{managers}m"] = result

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
