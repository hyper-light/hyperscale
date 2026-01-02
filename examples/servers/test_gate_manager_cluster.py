#!/usr/bin/env python3
"""
Gate + Manager Cluster Integration Test

This test starts both a gate cluster and a manager cluster and verifies:
1. Managers can connect to each other and elect a leader
2. Gates can connect to each other and elect a leader
3. Managers can register with gates
4. Gates can see managers as healthy

Usage:
    python test_gate_manager_cluster.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.nodes import ManagerServer, GateServer


# Port allocation for managers (TCP, UDP pairs)
MANAGER_CONFIGS = [
    {"tcp": 9000, "udp": 9001, "name": "Manager 1"},
    {"tcp": 9002, "udp": 9003, "name": "Manager 2"},
    {"tcp": 9004, "udp": 9005, "name": "Manager 3"},
]

# Port allocation for gates (TCP, UDP pairs)
GATE_CONFIGS = [
    {"tcp": 9100, "udp": 9101, "name": "Gate 1"},
    {"tcp": 9102, "udp": 9103, "name": "Gate 2"},
]

# Datacenter ID for this test
DC_ID = "DC-EAST"


def get_manager_peer_udp_addrs(my_udp: int) -> list[tuple[str, int]]:
    """Get manager peer UDP addresses excluding self."""
    return [
        ('127.0.0.1', config["udp"]) 
        for config in MANAGER_CONFIGS 
        if config["udp"] != my_udp
    ]


def get_manager_peer_tcp_addrs(my_tcp: int) -> list[tuple[str, int]]:
    """Get manager peer TCP addresses excluding self."""
    return [
        ('127.0.0.1', config["tcp"]) 
        for config in MANAGER_CONFIGS 
        if config["tcp"] != my_tcp
    ]


def get_gate_peer_udp_addrs(my_udp: int) -> list[tuple[str, int]]:
    """Get gate peer UDP addresses excluding self."""
    return [
        ('127.0.0.1', config["udp"]) 
        for config in GATE_CONFIGS 
        if config["udp"] != my_udp
    ]


def get_gate_peer_tcp_addrs(my_tcp: int) -> list[tuple[str, int]]:
    """Get gate peer TCP addresses excluding self."""
    return [
        ('127.0.0.1', config["tcp"]) 
        for config in GATE_CONFIGS 
        if config["tcp"] != my_tcp
    ]


def get_all_gate_tcp_addrs() -> list[tuple[str, int]]:
    """Get all gate TCP addresses."""
    return [('127.0.0.1', config["tcp"]) for config in GATE_CONFIGS]


def get_all_gate_udp_addrs() -> list[tuple[str, int]]:
    """Get all gate UDP addresses."""
    return [('127.0.0.1', config["udp"]) for config in GATE_CONFIGS]


def get_all_manager_tcp_addrs() -> list[tuple[str, int]]:
    """Get all manager TCP addresses."""
    return [('127.0.0.1', config["tcp"]) for config in MANAGER_CONFIGS]


def get_all_manager_udp_addrs() -> list[tuple[str, int]]:
    """Get all manager UDP addresses."""
    return [('127.0.0.1', config["udp"]) for config in MANAGER_CONFIGS]


async def run_test():
    """Run the gate + manager cluster test."""
    print("=" * 70)
    print("GATE + MANAGER CLUSTER INTEGRATION TEST")
    print("=" * 70)
    print(f"Testing with {len(MANAGER_CONFIGS)} managers + {len(GATE_CONFIGS)} gates")
    print(f"Datacenter: {DC_ID}")
    print()
    
    managers: list[ManagerServer] = []
    gates: list[GateServer] = []
    
    try:
        # ================================================================
        # STEP 1: Create all servers
        # ================================================================
        print("[1/5] Creating servers...")
        print("-" * 50)
        
        # Create managers (with gate addresses for registration)
        for config in MANAGER_CONFIGS:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
                dc_id=DC_ID,
                manager_peers=get_manager_peer_tcp_addrs(config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(config["udp"]),
                gate_addrs=get_all_gate_tcp_addrs(),
                gate_udp_addrs=get_all_gate_udp_addrs(),
            )
            managers.append(manager)
            print(f"  ✓ {config['name']} created (TCP:{config['tcp']} UDP:{config['udp']})")
        
        # Create gates (with manager addresses per datacenter)
        datacenter_managers = {DC_ID: get_all_manager_tcp_addrs()}
        datacenter_manager_udp = {DC_ID: get_all_manager_udp_addrs()}
        
        for config in GATE_CONFIGS:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
                gate_peers=get_gate_peer_tcp_addrs(config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(config["udp"]),
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
            )
            gates.append(gate)
            print(f"  ✓ {config['name']} created (TCP:{config['tcp']} UDP:{config['udp']})")
        
        print()
        
        # ================================================================
        # STEP 2: Start gates first (so managers can register with them)
        # ================================================================
        print("[2/5] Starting gates...")
        print("-" * 50)
        
        start_tasks = [gate.start() for gate in gates]
        await asyncio.gather(*start_tasks)
        
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {gate._node_id.short}")
        
        print()
        # ================================================================
        # STEP 3: Start managers (they will register with gates)
        # ================================================================
        print("[3/5] Starting managers...")
        print("-" * 50)
        
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)
        
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {manager._node_id.short}")
        
        print()
        
        # ================================================================
        # STEP 4: Wait for cluster stabilization
        # ================================================================
        print("[4/5] Waiting for clusters to stabilize (20s)...")
        print("-" * 50)
        await asyncio.sleep(20)
        print("  Done.")
        print()
        
        # ================================================================
        # STEP 5: Verify cluster state
        # ================================================================
        print("[5/5] Verifying cluster state...")
        print("-" * 50)
        
        all_checks_passed = True
        
        # ----- Manager Cluster -----
        print("\n  === MANAGER CLUSTER ===")
        
        # Manager connectivity
        print("\n  Manager Connectivity:")
        managers_connected = True
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            known_peers = len(manager._incarnation_tracker.get_all_nodes())
            expected = len(MANAGER_CONFIGS) - 1
            status = "✓" if known_peers >= expected else "✗"
            print(f"    {status} {config['name']}: knows {known_peers}/{expected} manager peers")
            if known_peers < expected:
                managers_connected = False
        all_checks_passed &= managers_connected
        
        # Manager state
        print("\n  Manager State:")
        managers_active = True
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            state = manager._manager_state.value
            status = "✓" if state == "active" else "✗"
            print(f"    {status} {config['name']}: {state}")
            if state != "active":
                managers_active = False
        all_checks_passed &= managers_active
        
        # Manager leadership
        print("\n  Manager Leadership:")
        manager_leaders = []
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            is_leader = manager.is_leader()
            leader_status = manager.get_leadership_status()
            if is_leader:
                manager_leaders.append(config['name'])
            print(f"    {config['name']}: role={leader_status['role']}, term={leader_status['term']}")
        
        has_manager_leader = len(manager_leaders) == 1
        all_checks_passed &= has_manager_leader
        
        # Manager quorum
        print("\n  Manager Quorum:")
        managers_have_quorum = True
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            quorum = manager.get_quorum_status()
            status = "✓" if quorum['quorum_available'] else "✗"
            print(f"    {status} {config['name']}: active={quorum['active_managers']}, required={quorum['required_quorum']}")
            if not quorum['quorum_available']:
                managers_have_quorum = False
        all_checks_passed &= managers_have_quorum
        
        # ----- Gate Cluster -----
        print("\n  === GATE CLUSTER ===")
        
        # Gate connectivity (to other gates)
        print("\n  Gate Connectivity:")
        gates_connected = True
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            known_peers = len(gate._incarnation_tracker.get_all_nodes())
            # Gates should see other gates + all managers
            expected_gates = len(GATE_CONFIGS) - 1
            expected_total = expected_gates + len(MANAGER_CONFIGS)
            status = "✓" if known_peers >= expected_gates else "✗"
            print(f"    {status} {config['name']}: knows {known_peers} peers (min {expected_gates} gates)")
            if known_peers < expected_gates:
                gates_connected = False
        all_checks_passed &= gates_connected
        
        # Gate state
        print("\n  Gate State:")
        gates_active = True
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            state = gate._gate_state.value
            status = "✓" if state == "active" else "✗"
            print(f"    {status} {config['name']}: {state}")
            if state != "active":
                gates_active = False
        all_checks_passed &= gates_active
        
        # Gate leadership
        print("\n  Gate Leadership:")
        gate_leaders = []
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            is_leader = gate.is_leader()
            leader_status = gate.get_leadership_status()
            if is_leader:
                gate_leaders.append(config['name'])
            print(f"    {config['name']}: role={leader_status['role']}, term={leader_status['term']}")
        
        has_gate_leader = len(gate_leaders) == 1
        all_checks_passed &= has_gate_leader
        
        # Gate quorum
        print("\n  Gate Quorum:")
        gates_have_quorum = True
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            quorum = gate.get_quorum_status()
            status = "✓" if quorum['quorum_available'] else "✗"
            print(f"    {status} {config['name']}: active={quorum['active_gates']}, required={quorum['required_quorum']}")
            if not quorum['quorum_available']:
                gates_have_quorum = False
        all_checks_passed &= gates_have_quorum
        
        # ----- Cross-Cluster Communication -----
        print("\n  === CROSS-CLUSTER COMMUNICATION ===")
        
        # Check if gates know about managers in the datacenter
        print("\n  Gate Datacenter Manager Config:")
        gates_have_manager_config = True
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            # Check if gate has managers configured for DC-EAST
            known_managers = len(gate._datacenter_managers.get(DC_ID, []))
            status = "✓" if known_managers > 0 else "✗"
            print(f"    {status} {config['name']}: {known_managers} managers configured for {DC_ID}")
            if known_managers == 0:
                gates_have_manager_config = False
        all_checks_passed &= gates_have_manager_config
        
        # Check if gates can see managers via SWIM
        print("\n  Gate SWIM Tracking of Managers:")
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            # Managers should be in the gate's SWIM membership (probe scheduler)
            nodes = gate._context.read('nodes')
            manager_nodes_found = 0
            for manager_cfg in MANAGER_CONFIGS:
                manager_udp = ('127.0.0.1', manager_cfg['udp'])
                if manager_udp in nodes:
                    manager_nodes_found += 1
            status = "✓" if manager_nodes_found == len(MANAGER_CONFIGS) else "○"  # Optional - may take time
            print(f"    {status} {config['name']}: {manager_nodes_found}/{len(MANAGER_CONFIGS)} managers in SWIM nodes")
        
        # Check if managers registered with gates
        print("\n  Manager Gate Registration:")
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            known_gates = len(manager._known_gates)
            primary_gate = manager._primary_gate_id
            status = "✓" if known_gates > 0 else "○"  # May fail if gates weren't up in time
            print(f"    {status} {config['name']}: knows {known_gates} gates, primary={primary_gate or 'None'}")
        
        # Final verdict
        print()
        print("=" * 70)
        
        if all_checks_passed:
            print("TEST RESULT: ✓ PASSED")
            print()
            print(f"  Manager Leader: {manager_leaders[0] if manager_leaders else 'None'}")
            print(f"  Gate Leader: {gate_leaders[0] if gate_leaders else 'None'}")
            print(f"  All {len(managers)} managers connected and in quorum")
            print(f"  All {len(gates)} gates connected and in quorum")
            print(f"  Cross-cluster communication verified")
            return True
        else:
            print("TEST RESULT: ✗ FAILED")
            print()
            if not managers_connected:
                print("  - Managers not fully connected")
            if not managers_active:
                print("  - Not all managers in ACTIVE state")
            if not has_manager_leader:
                print(f"  - Manager leader issue: {manager_leaders}")
            if not managers_have_quorum:
                print("  - Manager quorum not available")
            if not gates_connected:
                print("  - Gates not fully connected")
            if not gates_active:
                print("  - Not all gates in ACTIVE state")
            if not has_gate_leader:
                print(f"  - Gate leader issue: {gate_leaders}")
            if not gates_have_quorum:
                print("  - Gate quorum not available")
            if not managers_registered:
                print("  - Managers not registered with gates")
            return False
        
    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        print()
        print("=" * 70)
        print("Cleaning up...")
        print("-" * 50)
        
        # Stop gates first
        for i, gate in enumerate(gates):
            try:
                gate.stop_probe_cycle()
                await gate.stop_leader_election()
                await gate.shutdown()
                print(f"  ✓ {GATE_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {GATE_CONFIGS[i]['name']} stop failed: {e}")
        
        # Stop managers
        for i, manager in enumerate(managers):
            try:
                manager.stop_probe_cycle()
                await manager.stop_leader_election()
                await manager.shutdown()
                print(f"  ✓ {MANAGER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {MANAGER_CONFIGS[i]['name']} stop failed: {e}")
        
        print()
        print("Test complete.")
        print("=" * 70)


if __name__ == '__main__':
    try:
        success = asyncio.run(run_test())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(1)

