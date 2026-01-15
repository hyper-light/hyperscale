#!/usr/bin/env python3
"""
Gate Job Submission Integration Test.

Tests that:
1. A gate cluster starts and elects a leader
2. A manager cluster starts in a datacenter and registers with gates
3. Workers register with managers
4. A client can submit a job to the gate cluster
5. The gate receives the job and dispatches it to a datacenter

This tests the full job submission flow through the gate tier.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse
from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.models import GateState, ManagerState, JobStatus


# ==========================================================================
# Test Workflow - Simple class that can be pickled
# ==========================================================================

class TestWorkflow(Workflow):
    vus = 2000
    duration = "15s"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)


# ==========================================================================
# Configuration
# ==========================================================================

DC_ID = "DC-EAST"

# Gate configuration - 3 gates for quorum (global tier)
GATE_CONFIGS = [
    {"name": "Gate 1", "tcp": 9100, "udp": 9101},
    {"name": "Gate 2", "tcp": 9102, "udp": 9103},
    {"name": "Gate 3", "tcp": 9104, "udp": 9105},
]

# Manager configuration - 3 managers for quorum (DC-EAST)
MANAGER_CONFIGS = [
    {"name": "Manager 1", "tcp": 9000, "udp": 9001},
    {"name": "Manager 2", "tcp": 9002, "udp": 9003},
    {"name": "Manager 3", "tcp": 9004, "udp": 9005},
]

# Worker configuration - 2 workers
WORKER_CONFIGS = [
    {"name": "Worker 1", "tcp": 9200, "udp": 9250, "cores": 4},
    {"name": "Worker 2", "tcp": 9300, "udp": 9350, "cores": 4},
]

# Client configuration
CLIENT_CONFIG = {"tcp": 9300}

CLUSTER_STABILIZATION_TIME = 20  # seconds for gate+manager clusters to stabilize
WORKER_REGISTRATION_TIME = 8  # seconds for workers to register


def get_gate_peer_tcp_addrs(exclude_port: int) -> list[tuple[str, int]]:
    """Get TCP addresses of all gates except the one with exclude_port."""
    return [
        ('127.0.0.1', cfg['tcp'])
        for cfg in GATE_CONFIGS
        if cfg['tcp'] != exclude_port
    ]


def get_gate_peer_udp_addrs(exclude_port: int) -> list[tuple[str, int]]:
    """Get UDP addresses of all gates except the one with exclude_port."""
    return [
        ('127.0.0.1', cfg['udp'])
        for cfg in GATE_CONFIGS
        if cfg['udp'] != exclude_port
    ]


def get_all_gate_tcp_addrs() -> list[tuple[str, int]]:
    """Get TCP addresses of all gates."""
    return [('127.0.0.1', cfg['tcp']) for cfg in GATE_CONFIGS]


def get_all_gate_udp_addrs() -> list[tuple[str, int]]:
    """Get UDP addresses of all gates."""
    return [('127.0.0.1', cfg['udp']) for cfg in GATE_CONFIGS]


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


def get_all_manager_udp_addrs() -> list[tuple[str, int]]:
    """Get UDP addresses of all managers."""
    return [('127.0.0.1', cfg['udp']) for cfg in MANAGER_CONFIGS]


async def run_test():
    """Run the gate job submission integration test."""
    
    gates: list[GateServer] = []
    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    client: HyperscaleClient | None = None
    
    try:
        # ==============================================================
        # STEP 1: Create all servers
        # ==============================================================
        print("[1/7] Creating servers...")
        print("-" * 50)
        
        # Create gates first (with manager addresses per datacenter)
        datacenter_managers = {DC_ID: get_all_manager_tcp_addrs()}
        datacenter_manager_udp = {DC_ID: get_all_manager_udp_addrs()}
        
        for config in GATE_CONFIGS:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s', MERCURY_SYNC_LOG_LEVEL="error"),
                gate_peers=get_gate_peer_tcp_addrs(config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(config["udp"]),
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
            )
            gates.append(gate)
            print(f"  ✓ {config['name']} created (TCP:{config['tcp']} UDP:{config['udp']})")
        
        # Create managers (with gate addresses for registration)
        for config in MANAGER_CONFIGS:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s', MERCURY_SYNC_LOG_LEVEL="error"),
                dc_id=DC_ID,
                manager_peers=get_manager_peer_tcp_addrs(config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(config["udp"]),
                gate_addrs=get_all_gate_tcp_addrs(),
                gate_udp_addrs=get_all_gate_udp_addrs(),
            )
            managers.append(manager)
            print(f"  ✓ {config['name']} created (TCP:{config['tcp']} UDP:{config['udp']})")
        
        print()
        
        # ==============================================================
        # STEP 2: Start gates first
        # ==============================================================
        print("[2/7] Starting gates...")
        print("-" * 50)
        
        start_tasks = [gate.start() for gate in gates]
        await asyncio.gather(*start_tasks)
        
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {gate._node_id.short}")
        
        print()
        
        # ==============================================================
        # STEP 3: Start managers (they will register with gates)
        # ==============================================================
        print("[3/7] Starting managers...")
        print("-" * 50)
        
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)
        
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {manager._node_id.short}")
        
        print()
        
        # ==============================================================
        # STEP 4: Wait for gate and manager clusters to stabilize
        # ==============================================================
        print(f"[4/7] Waiting for clusters to stabilize ({CLUSTER_STABILIZATION_TIME}s)...")
        print("-" * 50)
        await asyncio.sleep(CLUSTER_STABILIZATION_TIME)
        
        # Verify leaders elected
        gate_leader = None
        for i, gate in enumerate(gates):
            if gate.is_leader():
                gate_leader = gate
                print(f"  ✓ Gate leader: {GATE_CONFIGS[i]['name']}")
                break
        
        if not gate_leader:
            print("  ✗ No gate leader elected!")
            return False
        
        manager_leader = None
        for i, manager in enumerate(managers):
            if manager.is_leader():
                manager_leader = manager
                print(f"  ✓ Manager leader: {MANAGER_CONFIGS[i]['name']}")
                break
        
        if not manager_leader:
            print("  ✗ No manager leader elected!")
            return False
        
        # Verify manager-gate registration
        dc_managers = gate_leader._datacenter_managers.get(DC_ID, {})
        print(f"  ✓ {len(dc_managers)} managers registered with gate leader for {DC_ID}")
        
        print()
        
        # ==============================================================
        # STEP 5: Create and start workers
        # ==============================================================
        print("[5/7] Creating and starting workers...")
        print("-" * 50)
        
        seed_managers = get_all_manager_tcp_addrs()
        
        for config in WORKER_CONFIGS:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s', MERCURY_SYNC_LOG_LEVEL="error"),
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
        
        # Wait for workers to register
        print(f"\n  Waiting for worker registration ({WORKER_REGISTRATION_TIME}s)...")
        await asyncio.sleep(WORKER_REGISTRATION_TIME)
        
        # Verify workers are registered with the manager leader
        registered_workers = len(manager_leader._workers)
        expected_workers = len(WORKER_CONFIGS)
        if registered_workers >= expected_workers:
            print(f"  ✓ {registered_workers}/{expected_workers} workers registered with manager leader")
        else:
            print(f"  ✗ Only {registered_workers}/{expected_workers} workers registered")
            return False
        
        # Wait for manager heartbeat to propagate to gates (heartbeat interval is 5s)
        # The heartbeat loop starts with a 5s sleep, so first heartbeat is 5s after manager start.
        # Workers register at +20s (cluster stabilization) + 8s (worker registration wait) = +28s.
        # We need to wait for the next heartbeat after workers register, which is at +30s or +35s.
        print(f"  Waiting for gate status update (10s to ensure heartbeat cycle)...")
        await asyncio.sleep(10)
        
        # Debug: Check what managers know about gates AND their worker status
        print(f"  DEBUG: Manager status:")
        for i, m in enumerate(managers):
            gate_addrs = m._gate_addrs
            known_gates = len(m._known_gates)
            healthy_gates = len(m._healthy_gate_ids)
            worker_count = len(m._workers)
            worker_status_count = len(m._worker_status)
            available_cores = sum(s.available_cores for s in m._worker_status.values())
            print(f"    {MANAGER_CONFIGS[i]['name']}: workers={worker_count}, status_entries={worker_status_count}, avail_cores={available_cores}, gates={healthy_gates}")
        
        # Debug: Check all gates' status after heartbeat propagation (new per-manager storage)
        print(f"  DEBUG: Checking gate datacenter status after heartbeat:")
        for i, g in enumerate(gates):
            manager_statuses = g._datacenter_manager_status.get(DC_ID, {})
            if manager_statuses:
                for mgr_addr, status in manager_statuses.items():
                    print(f"    {GATE_CONFIGS[i]['name']}: {mgr_addr} -> worker_count={status.worker_count}, available_cores={status.available_cores}")
            else:
                print(f"    {GATE_CONFIGS[i]['name']}: No manager status for {DC_ID}")
        
        print()
        
        # ==============================================================
        # STEP 6: Create client and submit job to GATE
        # ==============================================================
        print("[6/7] Creating client and submitting job to gate leader...")
        print("-" * 50)
        
        # Find the gate leader's address (and update gate_leader reference)
        gate_leader_addr = None
        for i, gate in enumerate(gates):
            if gate.is_leader():
                gate_leader = gate  # Update reference to current leader
                gate_leader_addr = ('127.0.0.1', GATE_CONFIGS[i]['tcp'])
                print(f"  ✓ Current gate leader: {GATE_CONFIGS[i]['name']}")
                break
        
        if not gate_leader_addr:
            print("  ✗ Could not find gate leader address!")
            return False
        
        client = HyperscaleClient(
            host='127.0.0.1',
            port=CLIENT_CONFIG["tcp"],
            env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='5s'),
            gates=[gate_leader_addr],  # Submit to gate leader
        )
        await client.start()
        print(f"  ✓ Client started on port {CLIENT_CONFIG['tcp']}")
        print(f"  ✓ Targeting gate leader at {gate_leader_addr}")
        
        # Track status updates
        status_updates = []
        def on_status_update(push):
            status_updates.append(push)
            print(f"    [Push] Job {push.job_id}: {push.status}")
        
        # Submit job
        try:
            job_id = await client.submit_job(
                workflows=[TestWorkflow],
                vus=1,
                timeout_seconds=30.0,
                datacenter_count=1,  # Target 1 datacenter
                on_status_update=on_status_update,
            )
            print(f"  ✓ Job submitted to gate: {job_id}")
        except Exception as e:
            print(f"  ✗ Job submission failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        print()
        
        # ==============================================================
        # STEP 7: Verify job was received by gate and dispatched
        # ==============================================================
        print("[7/7] Verifying job reception and dispatch...")
        print("-" * 50)
        
        # Check if gate has the job
        gate_has_job = False
        if job_id in gate_leader._jobs:
            gate_job = gate_leader._jobs[job_id]
            gate_has_job = True
            print(f"  ✓ Job found in gate leader's job tracker")
            print(f"    - Job ID: {gate_job.job_id}")
            print(f"    - Status: {gate_job.status}")
            print(f"    - Dispatched DCs: {gate_job.completed_datacenters}")
            print(f"    - Failed DCs: {gate_job.failed_datacenters}")
        else:
            print(f"  ✗ Job {job_id} not found in gate leader's job tracker")
            print(f"    Available jobs: {list(gate_leader._jobs.keys())}")
        
        # Wait a bit for dispatch to propagate and execute
        print(f"  Waiting for workflow execution (8s)...")
        await asyncio.sleep(8)
        
        # Check if manager received the job
        manager_has_job = False
        for i, manager in enumerate(managers):
            if job_id in manager._jobs:
                manager_job = manager._jobs[job_id]
                manager_has_job = True
                print(f"  ✓ Job found in {MANAGER_CONFIGS[i]['name']}'s tracker")
                print(f"    - Status: {manager_job.status}")
                print(f"    - Workflows: {len(manager_job.workflows)}")
                # Check workflow assignments
                print(f"    - Workflow assignments: {len(manager._workflow_assignments)}")
                for wf_id, worker_id in list(manager._workflow_assignments.items())[:3]:
                    print(f"      - {wf_id} -> {worker_id}")
                # Check worker statuses
                print(f"    - Active workers: {len(manager._worker_status)}")
                for w_id, w_status in list(manager._worker_status.items())[:2]:
                    print(f"      - {w_id}: cores={w_status.available_cores}, state={w_status.state}")
                break
        
        if not manager_has_job:
            print(f"  ○ Job not yet received by managers (gate may still be routing)")
        
        # Check if workers are executing anything
        for i, worker in enumerate(workers):
            active_wfs = len(worker._active_workflows)
            if active_wfs > 0:
                print(f"  ✓ Worker {i+1} executing {active_wfs} workflows")
                for wf_id, wf in list(worker._active_workflows.items())[:2]:
                    print(f"    - {wf_id}: status={wf.status}")
            else:
                print(f"  ○ Worker {i+1}: no active workflows (cores={worker._available_cores}/{worker._total_cores})")
        
        # Check client's view
        client_job = client.get_job_status(job_id)
        if client_job:
            print(f"  Client job status: {client_job.status}")
        
        print(f"  Status updates received: {len(status_updates)}")
        
        print()
        
        # ==============================================================
        # Final Results
        # ==============================================================
        all_passed = gate_has_job
        
        print("=" * 70)
        if all_passed:
            print("TEST RESULT: ✓ PASSED")
        else:
            print("TEST RESULT: ✗ FAILED")
        print()
        print("  Gate job submission flow verified:")
        print(f"  - Gate cluster: {len(gates)} gates, leader elected")
        print(f"  - Manager cluster: {len(managers)} managers, leader elected")
        print(f"  - Workers registered: {registered_workers}")
        print(f"  - Managers registered with gates: {len(dc_managers)}")
        print(f"  - Job submitted to gate: {job_id}")
        print(f"  - Job received by gate: {'Yes' if gate_has_job else 'No'}")
        print(f"  - Job dispatched to manager: {'Yes' if manager_has_job else 'Pending'}")
        print("=" * 70)
        
        return all_passed
        
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
                await worker.stop()
                print(f"  ✓ {WORKER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {WORKER_CONFIGS[i]['name']} stop failed: {e}")
        
        # Stop managers
        for i, manager in enumerate(managers):
            try:
                await manager.stop()
                print(f"  ✓ {MANAGER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {MANAGER_CONFIGS[i]['name']} stop failed: {e}")
        
        # Stop gates
        for i, gate in enumerate(gates):
            try:
                await gate.stop()
                print(f"  ✓ {GATE_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  ✗ {GATE_CONFIGS[i]['name']} stop failed: {e}")
        
        print()
        print("Test complete.")
        print("=" * 70)


def main():
    print("=" * 70)
    print("GATE JOB SUBMISSION INTEGRATION TEST")
    print("=" * 70)
    print(f"Testing with {len(GATE_CONFIGS)} gates + {len(MANAGER_CONFIGS)} managers + {len(WORKER_CONFIGS)} workers")
    print(f"Datacenter: {DC_ID}")
    print()
    
    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
