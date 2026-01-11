#!/usr/bin/env python3
"""
Worker + Manager Cluster Integration Test.

Tests that workers can:
1. Connect to a manager cluster
2. Register successfully
3. Be tracked by all managers (via cross-manager sync)
4. Receive the full list of all managers

This validates the worker <-> manager registration flow and
cross-manager worker discovery synchronization.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.models import ManagerState
from hyperscale.logging.config.logging_config import LoggingConfig

# Initialize logging directory (required for server pool)
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())



# ==========================================================================
# Configuration
# ==========================================================================

DC_ID = "DC-EAST"

# Manager configuration - 3 managers for quorum
MANAGER_CONFIGS = [
    {"name": "Manager 1", "tcp": 9000, "udp": 9001},
    {"name": "Manager 2", "tcp": 9002, "udp": 9003},
    {"name": "Manager 3", "tcp": 9004, "udp": 9005},
]

# Worker configuration - 4 workers
WORKER_CONFIGS = [
    {"name": "Worker 1", "tcp": 9200, "udp": 9250, "cores": 4},
    {"name": "Worker 2", "tcp": 9300, "udp": 9350, "cores": 4},
    {"name": "Worker 3", "tcp": 9400, "udp": 9450, "cores": 4},
    {"name": "Worker 4", "tcp": 9500, "udp": 9550, "cores": 4},
]

STABILIZATION_TIME = 15  # seconds to wait for cluster stabilization


def get_manager_peer_tcp_addrs(exclude_port: int) -> list[tuple[str, int]]:
    """Get TCP addresses of all managers except the one with exclude_port."""
    return [
        ('127.0.0.1', cfg['tcp'])
        for cfg in MANAGER_CONFIGS
        if cfg['tcp'] != exclude_port
    ]


def get_manager_peer_udp_addrs(exclude_port: int) -> list[tuple[str, int]]:
    """Get UDP addresses of all managers except the one with exclude_port."""
    return [
        ('127.0.0.1', cfg['udp'])
        for cfg in MANAGER_CONFIGS
        if cfg['udp'] != exclude_port
    ]


def get_all_manager_tcp_addrs() -> list[tuple[str, int]]:
    """Get TCP addresses of all managers."""
    return [('127.0.0.1', cfg['tcp']) for cfg in MANAGER_CONFIGS]


async def run_test():
    """Run the worker + manager cluster integration test."""
    
    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    
    try:
        # ==============================================================
        # STEP 1: Create servers
        # ==============================================================
        print("[1/6] Creating servers...")
        print("-" * 50)
        
        # Create managers
        for config in MANAGER_CONFIGS:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s', MERCURY_SYNC_LOG_LEVEL='error'),
                dc_id=DC_ID,
                manager_peers=get_manager_peer_tcp_addrs(config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(config["udp"]),
            )
            managers.append(manager)
            print(f"  ✓ {config['name']} created (TCP:{config['tcp']} UDP:{config['udp']})")
        
        # Create workers with seed managers
        seed_managers = get_all_manager_tcp_addrs()
        
        for config in WORKER_CONFIGS:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s', MERCURY_SYNC_LOG_LEVEL='error'),
                dc_id=DC_ID,
                total_cores=config["cores"],
                seed_managers=seed_managers,
            )
            workers.append(worker)
            print(f"  ✓ {config['name']} created (TCP:{config['tcp']} UDP:{config['udp']}, {config['cores']} cores)")
        
        print()
        
        # ==============================================================
        # STEP 2: Start managers first
        # ==============================================================
        print("[2/6] Starting managers...")
        print("-" * 50)
        
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)
        
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {manager._node_id.short}")
        
        print()
        
        # ==============================================================
        # STEP 3: Wait for manager cluster to stabilize
        # ==============================================================
        print("[3/6] Waiting for manager cluster to stabilize (15s)...")
        print("-" * 50)
        await asyncio.sleep(15)
        print("  Done.")
        print()
        
        # ==============================================================
        # STEP 4: Start workers (they will register with managers)
        # ==============================================================
        print("[4/6] Starting workers...")
        print("-" * 50)
        
        start_tasks = [worker.start() for worker in workers]
        await asyncio.gather(*start_tasks)
        
        for i, worker in enumerate(workers):
            config = WORKER_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {worker._node_id.short}")
        
        print()
        
        # ==============================================================
        # STEP 5: Wait for registration and sync
        # ==============================================================
        print(f"[5/6] Waiting for registration and sync ({STABILIZATION_TIME}s)...")
        print("-" * 50)
        await asyncio.sleep(STABILIZATION_TIME)
        print("  Done.")
        print()
        
        # ==============================================================
        # STEP 6: Verify cluster state
        # ==============================================================
        print("[6/6] Verifying cluster state...")
        print("-" * 50)
        
        all_checks_passed = True
        
        # ----- Manager Cluster Health -----
        print("\n  === MANAGER CLUSTER ===")
        
        print("\n  Manager Connectivity:")
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            nodes = manager._context.read('nodes')
            peer_count = len([n for n in nodes.keys() if n != ('127.0.0.1', config['udp'])])
            expected_peers = len(MANAGER_CONFIGS) - 1
            status = "✓" if peer_count >= expected_peers else "✗"
            print(f"    {status} {config['name']}: knows {peer_count}/{expected_peers} manager peers")
            if peer_count < expected_peers:
                all_checks_passed = False
        
        print("\n  Manager State:")
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            state = manager._manager_state
            status = "✓" if state == ManagerState.ACTIVE else "✗"
            print(f"    {status} {config['name']}: {state.value}")
            if state != ManagerState.ACTIVE:
                all_checks_passed = False
        
        print("\n  Manager Leadership:")
        leader_count = 0
        leader_name = None
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            is_leader = manager.is_leader()
            role = "leader" if is_leader else "follower"
            term = manager._leader_election.state.current_term
            print(f"    {config['name']}: role={role}, term={term}")
            if is_leader:
                leader_count += 1
                leader_name = config['name']
        
        if leader_count != 1:
            print(f"    ✗ Expected exactly 1 leader, got {leader_count}")
            all_checks_passed = False
        
        # ----- Worker Registration -----
        print("\n  === WORKER REGISTRATION ===")
        
        print("\n  Workers Tracked by Managers:")
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            worker_count = len(manager._workers)
            expected_workers = len(WORKER_CONFIGS)
            status = "✓" if worker_count >= expected_workers else "✗"
            print(f"    {status} {config['name']}: tracks {worker_count}/{expected_workers} workers")
            if worker_count < expected_workers:
                all_checks_passed = False
        
        print("\n  Worker Details per Manager:")
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            print(f"    {config['name']}:")
            for worker_id, registration in manager._workers.items():
                short_id = worker_id.split('-')[-1][:8] if '-' in worker_id else worker_id[:8]
                cores = registration.total_cores
                print(f"      - {short_id}... ({cores} cores)")
        
        # ----- Worker Manager Discovery -----
        print("\n  === WORKER MANAGER DISCOVERY ===")
        
        print("\n  Workers Know All Managers:")
        for i, worker in enumerate(workers):
            config = WORKER_CONFIGS[i]
            known_managers = len(worker._known_managers)
            expected_managers = len(MANAGER_CONFIGS)
            status = "✓" if known_managers >= expected_managers else "✗"
            print(f"    {status} {config['name']}: knows {known_managers}/{expected_managers} managers")
            if known_managers < expected_managers:
                all_checks_passed = False
        
        print("\n  Worker Primary Manager:")
        for i, worker in enumerate(workers):
            config = WORKER_CONFIGS[i]
            primary = worker._primary_manager_id
            has_primary = "✓" if primary else "✗"
            primary_short = primary.split('-')[-1][:8] if primary and '-' in primary else (primary[:8] if primary else "None")
            print(f"    {has_primary} {config['name']}: primary={primary_short}...")
            if not primary:
                all_checks_passed = False
        
        # ----- Cross-Manager Sync Verification -----
        print("\n  === CROSS-MANAGER WORKER SYNC ===")
        
        # Collect all unique worker IDs across all managers
        all_worker_ids: set[str] = set()
        for manager in managers:
            all_worker_ids.update(manager._workers.keys())
        
        print(f"\n  Total unique workers discovered: {len(all_worker_ids)}")
        
        # Check if all managers have all workers
        sync_complete = True
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            manager_worker_ids = set(manager._workers.keys())
            missing = all_worker_ids - manager_worker_ids
            if missing:
                print(f"    ✗ {config['name']}: missing {len(missing)} workers")
                sync_complete = False
            else:
                print(f"    ✓ {config['name']}: has all {len(all_worker_ids)} workers")
        
        if not sync_complete:
            all_checks_passed = False
        
        # ==============================================================
        # Results
        # ==============================================================
        print()
        print("=" * 70)
        
        if all_checks_passed:
            print("TEST RESULT: ✓ PASSED")
            print()
            print(f"  Manager Leader: {leader_name}")
            print(f"  All {len(managers)} managers connected and tracking workers")
            print(f"  All {len(workers)} workers registered and discovered managers")
            print(f"  Cross-manager worker sync verified")
        else:
            print("TEST RESULT: ✗ FAILED")
            print()
            print("  Some checks did not pass. See details above.")
        
        print()
        print("=" * 70)
        
        return all_checks_passed
        
    except Exception as e:
        import traceback
        print(f"\n✗ Test failed with exception: {e}")
        traceback.print_exc()
        return False
        
    finally:
        # ==============================================================
        # Cleanup
        # ==============================================================
        print("Cleaning up...")
        print("-" * 50)
        
        # Stop workers first
        for i, worker in enumerate(workers):
            try:
                await worker.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  ✓ {WORKER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {WORKER_CONFIGS[i]['name']} stop failed: {e}")
        
        # Then stop managers
        for i, manager in enumerate(managers):
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  ✓ {MANAGER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {MANAGER_CONFIGS[i]['name']} stop failed: {e}")
        
        print()
        print("Test complete.")
        print("=" * 70)


def main():
    print("=" * 70)
    print("WORKER + MANAGER CLUSTER INTEGRATION TEST")
    print("=" * 70)
    print(f"Testing with {len(MANAGER_CONFIGS)} managers + {len(WORKER_CONFIGS)} workers")
    print(f"Datacenter: {DC_ID}")
    print()
    
    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

