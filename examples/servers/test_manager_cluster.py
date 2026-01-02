#!/usr/bin/env python3
"""
Manager Cluster Integration Test

This test starts all 5 managers and verifies they can:
1. Start successfully
2. Connect to each other via SWIM
3. Elect a leader
4. Form a quorum

Usage:
    python test_manager_cluster.py

Expected output:
- All 5 managers start
- They discover each other via SWIM
- One becomes leader
- Quorum is established
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.nodes import ManagerServer


# Port allocation for managers
MANAGER_CONFIGS = [
    {"tcp": 9000, "udp": 9001, "name": "Manager 1"},
    {"tcp": 9002, "udp": 9003, "name": "Manager 2"},
    {"tcp": 9004, "udp": 9005, "name": "Manager 3"},
    {"tcp": 9006, "udp": 9007, "name": "Manager 4"},
    {"tcp": 9008, "udp": 9009, "name": "Manager 5"},
]


def get_peer_addrs(my_tcp: int, my_udp: int) -> tuple[list, list]:
    """Get peer TCP and UDP addresses excluding self."""
    tcp_peers = []
    udp_peers = []
    for config in MANAGER_CONFIGS:
        if config["tcp"] != my_tcp:
            tcp_peers.append(('127.0.0.1', config["tcp"]))
            udp_peers.append(('127.0.0.1', config["udp"]))
    return tcp_peers, udp_peers


async def create_manager(config: dict) -> ManagerServer:
    """Create a manager server with the given config."""
    tcp_peers, udp_peers = get_peer_addrs(config["tcp"], config["udp"])
    
    server = ManagerServer(
        host='127.0.0.1',
        tcp_port=config["tcp"],
        udp_port=config["udp"],
        env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
        dc_id='DC-EAST',
        manager_peers=tcp_peers,
        manager_udp_peers=udp_peers,
    )
    
    return server


async def run_test():
    """Run the manager cluster test."""
    print("=" * 70)
    print("MANAGER CLUSTER INTEGRATION TEST")
    print("=" * 70)
    print()
    
    managers: list[ManagerServer] = []
    
    try:
        # Step 1: Create and start all managers
        print("[1/5] Starting all managers...")
        print("-" * 50)
        
        for config in MANAGER_CONFIGS:
            manager = await create_manager(config)
            await manager.start()
            managers.append(manager)
            print(f"  ✓ {config['name']} started on TCP:{config['tcp']} UDP:{config['udp']}")
            print(f"    Node ID: {manager._node_id.short}")
        
        print()
        
        # Step 2: Wait for SWIM to propagate membership
        print("[2/5] Waiting for SWIM membership propagation...")
        print("-" * 50)
        
        await asyncio.sleep(5)  # Give SWIM time to exchange probes
        
        # Step 3: Verify connectivity
        print()
        print("[3/5] Verifying SWIM connectivity...")
        print("-" * 50)
        
        all_connected = True
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            # Check how many peers this manager knows about
            known_peers = len(manager._incarnation_tracker.get_all_nodes())
            expected_peers = len(MANAGER_CONFIGS) - 1  # All except self
            
            status = "✓" if known_peers >= expected_peers else "✗"
            print(f"  {status} {config['name']}: knows {known_peers}/{expected_peers} peers")
            
            if known_peers < expected_peers:
                all_connected = False
        
        if not all_connected:
            print()
            print("  ⚠ Not all managers fully connected yet, waiting longer...")
            await asyncio.sleep(5)
        
        print()
        
        # Step 4: Verify leader election
        print("[4/5] Verifying leader election...")
        print("-" * 50)
        
        # Wait for leader election to settle
        await asyncio.sleep(3)
        
        leaders = []
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            is_leader = manager.is_leader()
            current_leader = manager.get_current_leader()
            state = manager._manager_state.value
            
            if is_leader:
                leaders.append(config['name'])
            
            leader_str = current_leader if current_leader else "None"
            print(f"  {config['name']}: state={state}, is_leader={is_leader}, sees_leader={leader_str}")
        
        print()
        if len(leaders) == 1:
            print(f"  ✓ Single leader elected: {leaders[0]}")
        elif len(leaders) == 0:
            print("  ⚠ No leader elected yet (may still be in election)")
        else:
            print(f"  ✗ Multiple leaders detected: {leaders} (split brain!)")
        
        print()
        
        # Step 5: Verify quorum
        print("[5/5] Verifying quorum status...")
        print("-" * 50)
        
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            quorum = manager.get_quorum_status()
            
            status = "✓" if quorum['quorum_available'] else "✗"
            print(f"  {status} {config['name']}: active={quorum['active_managers']}, "
                  f"required={quorum['required_quorum']}, available={quorum['quorum_available']}")
        
        print()
        print("=" * 70)
        
        # Final verdict
        # Check final state
        final_leaders = [m for m in managers if m.is_leader()]
        final_quorum = all(m.get_quorum_status()['quorum_available'] for m in managers)
        final_connected = all(
            len(m._incarnation_tracker.get_all_nodes()) >= len(MANAGER_CONFIGS) - 1
            for m in managers
        )
        
        print()
        if len(final_leaders) == 1 and final_quorum and final_connected:
            print("TEST RESULT: ✓ PASSED")
            print()
            print("All managers:")
            print("  - Connected to each other via SWIM")
            print("  - Elected a single leader")
            print("  - Have quorum available")
            return True
        else:
            print("TEST RESULT: ✗ FAILED")
            print()
            if len(final_leaders) != 1:
                print(f"  - Leader election issue: {len(final_leaders)} leaders")
            if not final_quorum:
                print("  - Quorum not available on all managers")
            if not final_connected:
                print("  - Not all managers fully connected")
            return False
        
    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup: stop all managers
        print()
        print("=" * 70)
        print("Cleaning up...")
        print("-" * 50)
        
        for i, manager in enumerate(managers):
            try:
                await manager.stop()
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

