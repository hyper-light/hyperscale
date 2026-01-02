#!/usr/bin/env python3
"""
Gate Cluster Integration Test

This test starts multiple gates and verifies they can:
1. Start successfully
2. Connect to each other via SWIM
3. Elect a leader
4. Form a quorum

Usage:
    python test_gate_cluster.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.nodes import GateServer


# Port allocation for gates (TCP, UDP pairs)
GATE_CONFIGS = [
    {"tcp": 9100, "udp": 9101, "name": "Gate 1"},
    {"tcp": 9102, "udp": 9103, "name": "Gate 2"},
    {"tcp": 9104, "udp": 9105, "name": "Gate 3"},
]

# Datacenter configuration (gates need to know about managers per DC)
# For this test, we'll use empty datacenter configs since we're just
# testing gate-to-gate communication
DATACENTER_MANAGERS = {}
DATACENTER_MANAGER_UDP = {}


def get_peer_udp_addrs(my_udp: int) -> list[tuple[str, int]]:
    """Get peer UDP addresses excluding self."""
    return [
        ('127.0.0.1', config["udp"]) 
        for config in GATE_CONFIGS 
        if config["udp"] != my_udp
    ]


def get_peer_tcp_addrs(my_tcp: int) -> list[tuple[str, int]]:
    """Get peer TCP addresses excluding self."""
    return [
        ('127.0.0.1', config["tcp"]) 
        for config in GATE_CONFIGS 
        if config["tcp"] != my_tcp
    ]


async def run_test():
    """Run the gate cluster test."""
    print("=" * 70)
    print("GATE CLUSTER INTEGRATION TEST")
    print("=" * 70)
    print(f"Testing with {len(GATE_CONFIGS)} gates")
    print()
    
    gates: list[GateServer] = []
    
    try:
        # Step 1: Create all gate servers (don't start yet)
        print("[1/4] Creating gate servers...")
        print("-" * 50)
        
        for config in GATE_CONFIGS:
            tcp_peers = get_peer_tcp_addrs(config["tcp"])
            udp_peers = get_peer_udp_addrs(config["udp"])
            
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
                gate_peers=tcp_peers,
                gate_udp_peers=udp_peers,
                datacenter_managers=DATACENTER_MANAGERS,
                datacenter_manager_udp=DATACENTER_MANAGER_UDP,
            )
            gates.append(gate)
            print(f"  ✓ {config['name']} created (TCP:{config['tcp']} UDP:{config['udp']})")
        
        print()
        
        # Step 2: Start all gates concurrently
        print("[2/4] Starting gates (uses full start() method)...")
        print("-" * 50)
        
        # Start each gate - this does:
        # - start_server()
        # - join_cluster() for each peer
        # - start_probe_cycle()
        # - start_leader_election()
        # - _complete_startup_sync() -> transitions to ACTIVE
        start_tasks = [gate.start() for gate in gates]
        await asyncio.gather(*start_tasks)
        
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {gate._node_id.short}")
        
        print()
        
        # Step 3: Wait for cluster to stabilize
        # Leader election: pre-vote(2s) + election(5-7s) = 7-9s per attempt
        # If first attempt splits votes, need retry with higher term
        print("[3/4] Waiting for cluster to stabilize (18s for 2 election cycles)...")
        print("-" * 50)
        await asyncio.sleep(18)
        print("  Done.")
        print()
        
        # Step 4: Verify cluster state
        print("[4/4] Verifying cluster state...")
        print("-" * 50)
        
        # Check connectivity
        print("\n  Connectivity (SWIM nodes dict):")
        all_connected = True
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            known_peers = len(gate._incarnation_tracker.get_all_nodes())
            nodes_dict = gate._context.read('nodes')
            nodes_count = len(nodes_dict) if nodes_dict else 0
            expected = len(GATE_CONFIGS) - 1
            status = "✓" if known_peers >= expected else "✗"
            print(f"    {status} {config['name']}: incarnation_tracker={known_peers}, "
                  f"nodes_dict={nodes_count} (need {expected})")
            if known_peers < expected:
                all_connected = False
        
        # Check gate state (enum uses lowercase values)
        print("\n  Gate State:")
        all_active = True
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            state = gate._gate_state.value
            status = "✓" if state == "active" else "✗"
            print(f"    {status} {config['name']}: {state}")
            if state != "active":
                all_active = False
        
        # Check leadership
        print("\n  Leadership:")
        leaders = []
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            is_leader = gate.is_leader()
            leader_addr = gate.get_current_leader()
            status = gate.get_leadership_status()
            
            if is_leader:
                leaders.append(config['name'])
            
            leader_str = f"{leader_addr}" if leader_addr else "None"
            print(f"    {config['name']}: role={status['role']}, term={status['term']}, "
                  f"sees={leader_str}, eligible={status['eligible']}")
        
        # Check quorum
        print("\n  Quorum:")
        all_have_quorum = True
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            quorum = gate.get_quorum_status()
            status = "✓" if quorum['quorum_available'] else "✗"
            print(f"    {status} {config['name']}: active={quorum['active_gates']}, "
                  f"required={quorum['required_quorum']}, available={quorum['quorum_available']}")
            if not quorum['quorum_available']:
                all_have_quorum = False
        
        # Final verdict
        print()
        print("=" * 70)
        
        has_single_leader = len(leaders) == 1
        
        if has_single_leader and all_have_quorum and all_connected and all_active:
            print("TEST RESULT: ✓ PASSED")
            print()
            print(f"  Leader: {leaders[0]}")
            print(f"  All {len(gates)} gates connected")
            print(f"  All gates in ACTIVE state")
            print(f"  Quorum available on all gates")
            return True
        else:
            print("TEST RESULT: ✗ FAILED")
            print()
            if not all_connected:
                print("  - Not all gates fully connected")
            if not all_active:
                print("  - Not all gates in ACTIVE state")
            if len(leaders) == 0:
                print("  - No leader elected")
            elif len(leaders) > 1:
                print(f"  - Multiple leaders: {leaders}")
            if not all_have_quorum:
                print("  - Quorum not available on all gates")
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
        
        # Stop gates
        for i, gate in enumerate(gates):
            try:
                await gate.graceful_shutdown()
                print(f"  ✓ {GATE_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {GATE_CONFIGS[i]['name']} stop failed: {e}")
        
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

