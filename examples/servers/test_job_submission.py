#!/usr/bin/env python3
"""
Job Submission Integration Test.

Tests that:
1. A manager cluster starts and elects a leader
2. Workers register with managers
3. A client can submit a job to the leader manager
4. The manager receives and accepts the job
5. The manager attempts to provision workflows to workers

This is an end-to-end test of the job submission flow.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.graph import Workflow, step
from hyperscale.distributed_rewrite.nodes.manager import ManagerServer
from hyperscale.distributed_rewrite.nodes.worker import WorkerServer
from hyperscale.distributed_rewrite.nodes.client import HyperscaleClient
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.distributed_rewrite.models import ManagerState, JobStatus


# ==========================================================================
# Test Workflow - Simple class that can be pickled
# ==========================================================================

class TestWorkflow:
    """Simple test workflow that does nothing (no Workflow inheritance for simpler pickle)."""
    name = "test_workflow"
    vus = 1
    duration = "5s"
    
    async def run(self) -> None:
        """A simple run method."""
        pass


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

# Worker configuration - 2 workers
WORKER_CONFIGS = [
    {"name": "Worker 1", "tcp": 9200, "udp": 9201, "cores": 4},
    {"name": "Worker 2", "tcp": 9202, "udp": 9203, "cores": 4},
]

# Client configuration
CLIENT_CONFIG = {"tcp": 9300}

STABILIZATION_TIME = 10  # seconds to wait for cluster stabilization


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
    """Run the job submission integration test."""
    
    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    client: HyperscaleClient | None = None
    
    try:
        # ==============================================================
        # STEP 1: Create and start managers
        # ==============================================================
        print("[1/7] Creating and starting managers...")
        print("-" * 50)
        
        for config in MANAGER_CONFIGS:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
                dc_id=DC_ID,
                manager_peers=get_manager_peer_tcp_addrs(config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(config["udp"]),
            )
            managers.append(manager)
        
        # Start all managers
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)
        
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {manager._node_id.short}")
        
        print()
        
        # ==============================================================
        # STEP 2: Wait for leader election
        # ==============================================================
        print("[2/7] Waiting for leader election (10s)...")
        print("-" * 50)
        await asyncio.sleep(10)
        
        # Find the leader
        leader_manager = None
        leader_addr = None
        for i, manager in enumerate(managers):
            if manager.is_leader():
                leader_manager = manager
                leader_addr = ('127.0.0.1', MANAGER_CONFIGS[i]['tcp'])
                print(f"  ✓ Leader elected: {MANAGER_CONFIGS[i]['name']}")
                break
        
        if not leader_manager:
            print("  ✗ No leader elected!")
            return False
        
        print()
        
        # ==============================================================
        # STEP 3: Create and start workers
        # ==============================================================
        print("[3/7] Creating and starting workers...")
        print("-" * 50)
        
        seed_managers = get_all_manager_tcp_addrs()
        
        for config in WORKER_CONFIGS:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
                dc_id=DC_ID,
                total_cores=config["cores"],
                seed_managers=seed_managers,
            )
            workers.append(worker)
        
        # Start all workers
        start_tasks = [worker.start() for worker in workers]
        await asyncio.gather(*start_tasks)
        
        for i, worker in enumerate(workers):
            config = WORKER_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {worker._node_id.short}")
        
        print()
        
        # ==============================================================
        # STEP 4: Wait for workers to register
        # ==============================================================
        print(f"[4/7] Waiting for worker registration ({STABILIZATION_TIME}s)...")
        print("-" * 50)
        await asyncio.sleep(STABILIZATION_TIME)
        
        # Verify workers are registered with the leader
        registered_workers = len(leader_manager._workers)
        expected_workers = len(WORKER_CONFIGS)
        if registered_workers >= expected_workers:
            print(f"  ✓ {registered_workers}/{expected_workers} workers registered with leader")
        else:
            print(f"  ✗ Only {registered_workers}/{expected_workers} workers registered")
            return False
        
        print()
        
        # ==============================================================
        # STEP 5: Create client and submit job
        # ==============================================================
        print("[5/7] Creating client and submitting job...")
        print("-" * 50)
        
        client = HyperscaleClient(
            host='127.0.0.1',
            port=CLIENT_CONFIG["tcp"],
            env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='5s'),
            managers=[leader_addr],  # Submit directly to leader
        )
        await client.start()
        print(f"  ✓ Client started on port {CLIENT_CONFIG['tcp']}")
        
        # Track status updates
        status_updates = []
        def on_status_update(push):
            status_updates.append(push)
            print(f"    [Push] Job {push.job_id}: {push.status} - {push.message}")
        
        # Submit job
        try:
            job_id = await client.submit_job(
                workflows=[TestWorkflow],
                vus=1,
                timeout_seconds=30.0,
                on_status_update=on_status_update,
            )
            print(f"  ✓ Job submitted: {job_id}")
        except Exception as e:
            print(f"  ✗ Job submission failed: {e}")
            return False
        
        print()
        
        # ==============================================================
        # STEP 6: Verify job was received by manager
        # ==============================================================
        print("[6/7] Verifying job reception...")
        print("-" * 50)
        
        # Check if leader has the job
        if job_id in leader_manager._jobs:
            job = leader_manager._jobs[job_id]
            print(f"  ✓ Job found in leader's job tracker")
            print(f"    - Job ID: {job.job_id}")
            print(f"    - Status: {job.status}")
            print(f"    - Datacenter: {job.datacenter}")
        else:
            print(f"  ✗ Job {job_id} not found in leader's job tracker")
            print(f"    Available jobs: {list(leader_manager._jobs.keys())}")
            return False
        
        print()
        
        # ==============================================================
        # STEP 7: Wait a bit and check job progress
        # ==============================================================
        print("[7/7] Checking job progress (5s)...")
        print("-" * 50)
        await asyncio.sleep(5)
        
        # Check job status
        job = leader_manager._jobs.get(job_id)
        if job:
            print(f"  Job Status: {job.status}")
            print(f"  Workflows: {len(job.workflows)}")
            
            # Check if any workflows were dispatched
            dispatched = len(leader_manager._workflow_assignments)
            print(f"  Workflow assignments: {dispatched}")
            
            for wf_id, worker_id in leader_manager._workflow_assignments.items():
                if wf_id.startswith(job_id):
                    print(f"    - {wf_id[:20]}... -> {worker_id[:20]}...")
        
        # Check client's view
        client_job = client.get_job_status(job_id)
        if client_job:
            print(f"  Client job status: {client_job.status}")
        
        print(f"  Status updates received: {len(status_updates)}")
        
        print()
        
        # ==============================================================
        # Final Results
        # ==============================================================
        print("=" * 70)
        print("TEST RESULT: ✓ PASSED")
        print()
        print("  Job submission flow verified:")
        print(f"  - Manager cluster: {len(managers)} managers, leader elected")
        print(f"  - Workers registered: {registered_workers}")
        print(f"  - Job submitted: {job_id}")
        print(f"  - Job received by leader: Yes")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        import traceback
        print(f"\n✗ Test failed with exception: {e}")
        traceback.print_exc()
        return False
        
    finally:
        # ==============================================================
        # Cleanup
        # ==============================================================
        print()
        print("Cleaning up...")
        print("-" * 50)
        
        # Stop client
        if client:
            try:
                await client.stop()
                print("  ✓ Client stopped")
            except Exception as e:
                print(f"  ✗ Client stop failed: {e}")
        
        # Stop workers
        for i, worker in enumerate(workers):
            try:
                await worker.graceful_shutdown()
                print(f"  ✓ {WORKER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {WORKER_CONFIGS[i]['name']} stop failed: {e}")
        
        # Stop managers
        for i, manager in enumerate(managers):
            try:
                await manager.graceful_shutdown()
                print(f"  ✓ {MANAGER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {MANAGER_CONFIGS[i]['name']} stop failed: {e}")
        
        print()
        print("Test complete.")
        print("=" * 70)


def main():
    print("=" * 70)
    print("JOB SUBMISSION INTEGRATION TEST")
    print("=" * 70)
    print(f"Testing with {len(MANAGER_CONFIGS)} managers + {len(WORKER_CONFIGS)} workers")
    print(f"Datacenter: {DC_ID}")
    print()
    
    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

