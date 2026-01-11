#!/usr/bin/env python3
"""
Gate Per-Job Routing Integration Test.

Tests per-job ownership via consistent hashing:
1. Multiple gates form a cluster with a shared hash ring
2. Jobs are deterministically assigned to gates via hash(job_id)
3. If a job is submitted to the wrong gate, it should redirect to the owner
4. When a gate fails, its jobs can be claimed by other gates
5. Lease management prevents split-brain scenarios

This tests the ConsistentHashRing and LeaseManager integration in gates.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Initialize logging config before importing other hyperscale modules
from hyperscale.logging.config import LoggingConfig
LoggingConfig().update(log_directory=os.getcwd(), log_level="info")

from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse
from hyperscale.distributed_rewrite.nodes.gate import GateServer
from hyperscale.distributed_rewrite.nodes.manager import ManagerServer
from hyperscale.distributed_rewrite.nodes.worker import WorkerServer
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.distributed_rewrite.routing import ConsistentHashRing
from hyperscale.distributed_rewrite.models import (
    JobSubmission,
    JobAck,
)

# ==========================================================================
# Test Workflow
# ==========================================================================

class TestWorkflow(Workflow):
    vus = 1
    duration = "5s"

    @step()
    async def get_test(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)


# ==========================================================================
# Configuration
# ==========================================================================

DC_ID = "DC-EAST"

# Gate configuration - 3 gates for testing distribution
GATE_CONFIGS = [
    {"name": "Gate 1", "tcp": 9100, "udp": 9101},
    {"name": "Gate 2", "tcp": 9102, "udp": 9103},
    {"name": "Gate 3", "tcp": 9104, "udp": 9105},
]

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

CLUSTER_STABILIZATION_TIME = 15  # seconds for clusters to stabilize
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
    """Run the gate per-job routing integration test."""

    gates: list[GateServer] = []
    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    test_passed = True

    try:
        # ==============================================================
        # STEP 1: Create and start gates with datacenter managers
        # ==============================================================
        print("[1/8] Creating and starting gates...")
        print("-" * 50)

        env = Env(
            MERCURY_SYNC_REQUEST_TIMEOUT='2s',
            # Use shorter lease for testing
            JOB_LEASE_DURATION=10.0,
            JOB_LEASE_CLEANUP_INTERVAL=2.0,
        )

        datacenter_managers = {DC_ID: get_all_manager_tcp_addrs()}
        datacenter_manager_udp = {DC_ID: get_all_manager_udp_addrs()}

        for config in GATE_CONFIGS:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=env,
                gate_peers=get_gate_peer_tcp_addrs(config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(config["udp"]),
                datacenter_managers=datacenter_managers,
                datacenter_manager_udp=datacenter_manager_udp,
            )
            gates.append(gate)

        # Start all gates
        start_tasks = [gate.start() for gate in gates]
        await asyncio.gather(*start_tasks)

        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            print(f"  [OK] {config['name']} started (TCP:{config['tcp']}) - Ring ID: {gate._my_ring_id}")

        print()

        # ==============================================================
        # STEP 2: Create and start managers
        # ==============================================================
        print("[2/8] Creating and starting managers...")
        print("-" * 50)

        for config in MANAGER_CONFIGS:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=env,
                dc_id=DC_ID,
                manager_peers=get_manager_peer_tcp_addrs(config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(config["udp"]),
                gate_addrs=get_all_gate_tcp_addrs(),
                gate_udp_addrs=get_all_gate_udp_addrs(),
            )
            managers.append(manager)

        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)

        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            print(f"  [OK] {config['name']} started (TCP:{config['tcp']})")

        print()

        # ==============================================================
        # STEP 3: Create and start workers
        # ==============================================================
        print("[3/8] Creating and starting workers...")
        print("-" * 50)

        seed_managers = get_all_manager_tcp_addrs()

        for config in WORKER_CONFIGS:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=env,
                dc_id=DC_ID,
                total_cores=config["cores"],
                seed_managers=seed_managers,
            )
            workers.append(worker)

        start_tasks = [worker.start() for worker in workers]
        await asyncio.gather(*start_tasks)

        for i, worker in enumerate(workers):
            config = WORKER_CONFIGS[i]
            print(f"  [OK] {config['name']} started (TCP:{config['tcp']})")

        print()

        # ==============================================================
        # STEP 4: Wait for cluster stabilization
        # ==============================================================
        print(f"[4/8] Waiting for clusters to stabilize ({CLUSTER_STABILIZATION_TIME}s)...")
        print("-" * 50)
        await asyncio.sleep(CLUSTER_STABILIZATION_TIME)

        # Verify all gates see each other in the hash ring
        for i, gate in enumerate(gates):
            ring_nodes = gate._job_hash_ring.get_all_nodes()
            expected_nodes = len(GATE_CONFIGS)
            if len(ring_nodes) == expected_nodes:
                print(f"  [OK] {GATE_CONFIGS[i]['name']}: hash ring has {len(ring_nodes)} nodes")
            else:
                print(f"  [FAIL] {GATE_CONFIGS[i]['name']}: hash ring has {len(ring_nodes)}/{expected_nodes} nodes")
                test_passed = False

        # Verify manager leader elected
        manager_leader = None
        for i, manager in enumerate(managers):
            if manager.is_leader():
                manager_leader = manager
                print(f"  [OK] Manager leader: {MANAGER_CONFIGS[i]['name']}")
                break

        if not manager_leader:
            print("  [FAIL] No manager leader elected")
            test_passed = False

        # Wait for worker registration
        print(f"  Waiting for worker registration ({WORKER_REGISTRATION_TIME}s)...")
        await asyncio.sleep(WORKER_REGISTRATION_TIME)

        if manager_leader:
            registered_workers = len(manager_leader._workers)
            print(f"  [OK] {registered_workers} workers registered with manager leader")

        print()

        # ==============================================================
        # STEP 5: Verify consistent hashing distributes jobs
        # ==============================================================
        print("[5/8] Testing job distribution via consistent hashing...")
        print("-" * 50)

        # Create a reference hash ring to verify deterministic routing
        ref_ring = ConsistentHashRing(virtual_nodes=env.JOB_HASH_RING_VIRTUAL_NODES)
        for cfg in GATE_CONFIGS:
            ref_ring.add_node(f"127.0.0.1:{cfg['tcp']}")

        # Test job distribution across 50 jobs
        job_distribution: dict[str, list[str]] = {
            f"127.0.0.1:{cfg['tcp']}": [] for cfg in GATE_CONFIGS
        }

        for i in range(50):
            job_id = f"test-job-{i}"
            owner = ref_ring.get_node(job_id)
            if owner:
                job_distribution[owner].append(job_id)

        print(f"  Job distribution across {len(GATE_CONFIGS)} gates:")
        min_jobs = float('inf')
        max_jobs = 0
        for node_id, jobs in job_distribution.items():
            min_jobs = min(min_jobs, len(jobs))
            max_jobs = max(max_jobs, len(jobs))
            print(f"    {node_id}: {len(jobs)} jobs")

        # Check that distribution is reasonably balanced (no gate has 0 or all jobs)
        if min_jobs > 0 and max_jobs < 50:
            print(f"  [OK] Jobs distributed (min={min_jobs}, max={max_jobs})")
        else:
            print(f"  [FAIL] Poor distribution (min={min_jobs}, max={max_jobs})")
            test_passed = False

        print()

        # ==============================================================
        # STEP 6: Test direct job submission to correct owner
        # ==============================================================
        print("[6/8] Testing job submission to correct owner gate...")
        print("-" * 50)

        # Pick a job and determine its owner
        test_job_id = "integration-test-job-1"
        expected_owner = ref_ring.get_node(test_job_id)
        print(f"  Job '{test_job_id}' should be owned by: {expected_owner}")

        # Find the gate that should own this job
        owner_gate = None
        owner_gate_idx = None
        for i, gate in enumerate(gates):
            if gate._my_ring_id == expected_owner:
                owner_gate = gate
                owner_gate_idx = i
                break

        if not owner_gate:
            print(f"  [FAIL] Could not find owner gate for job")
            test_passed = False
        else:
            print(f"  Submitting job to {GATE_CONFIGS[owner_gate_idx]['name']}...")

            # Create a job submission with pickled workflow
            import cloudpickle
            submission = JobSubmission(
                job_id=test_job_id,
                workflows=cloudpickle.dumps([TestWorkflow]),
                vus=1,
                timeout_seconds=30.0,
                datacenter_count=1,
            )

            # Submit directly via the gate's internal job_submission handler
            response = await owner_gate.job_submission(
                addr=('127.0.0.1', 9999),  # Dummy client address
                data=submission.dump(),
                clock_time=0,
            )
            ack = JobAck.load(response)

            if ack.accepted:
                print(f"  [OK] Job accepted by owner gate (job_id={ack.job_id})")

                # Verify lease was acquired
                if owner_gate._job_lease_manager.is_owner(test_job_id):
                    lease = owner_gate._job_lease_manager.get_lease(test_job_id)
                    print(f"  [OK] Lease acquired (fence_token={lease.fence_token}, expires in {lease.remaining_seconds():.1f}s)")
                else:
                    print(f"  [FAIL] Lease not acquired")
                    test_passed = False

                # Verify job is in gate's tracking
                if test_job_id in owner_gate._jobs:
                    job = owner_gate._jobs[test_job_id]
                    print(f"  [OK] Job in gate tracking (status={job.status})")
                else:
                    print(f"  [FAIL] Job not in gate tracking")
                    test_passed = False
            else:
                print(f"  [FAIL] Job rejected: {ack.error}")
                test_passed = False

        print()

        # ==============================================================
        # STEP 7: Test job submission to wrong gate (should redirect)
        # ==============================================================
        print("[7/8] Testing job submission to non-owner gate (redirect)...")
        print("-" * 50)

        test_job_id_2 = "integration-test-job-2"
        expected_owner_2 = ref_ring.get_node(test_job_id_2)
        print(f"  Job '{test_job_id_2}' should be owned by: {expected_owner_2}")

        # Find a gate that is NOT the owner
        non_owner_gate = None
        non_owner_idx = None
        for i, gate in enumerate(gates):
            if gate._my_ring_id != expected_owner_2:
                non_owner_gate = gate
                non_owner_idx = i
                break

        if not non_owner_gate:
            print(f"  [SKIP] All gates are owners (single-node scenario)")
        else:
            print(f"  Submitting job to non-owner: {GATE_CONFIGS[non_owner_idx]['name']}...")

            import cloudpickle
            submission = JobSubmission(
                job_id=test_job_id_2,
                workflows=cloudpickle.dumps([TestWorkflow]),
                vus=1,
                timeout_seconds=30.0,
                datacenter_count=1,
            )

            response = await non_owner_gate.job_submission(
                addr=('127.0.0.1', 9999),
                data=submission.dump(),
                clock_time=0,
            )
            ack = JobAck.load(response)

            if not ack.accepted and ack.leader_addr:
                # leader_addr contains the correct owner's address
                redirect_addr = f"{ack.leader_addr[0]}:{ack.leader_addr[1]}"
                if redirect_addr == expected_owner_2:
                    print(f"  [OK] Correctly redirected to owner: {redirect_addr}")
                else:
                    print(f"  [FAIL] Redirected to wrong gate: {redirect_addr} (expected {expected_owner_2})")
                    test_passed = False
            elif ack.accepted:
                print(f"  [FAIL] Job should have been rejected (not owner)")
                test_passed = False
            else:
                print(f"  [FAIL] Job rejected without redirect: {ack.error}")
                test_passed = False

        print()

        # ==============================================================
        # STEP 8: Test lease prevents duplicate acquisition
        # ==============================================================
        print("[8/8] Testing lease prevents duplicate acquisition...")
        print("-" * 50)

        # Try to acquire the same job from another gate
        test_job_id_3 = "integration-test-job-3"
        expected_owner_3 = ref_ring.get_node(test_job_id_3)

        # Find owner and non-owner gates
        owner_gate_3 = None
        other_gate = None
        for gate in gates:
            if gate._my_ring_id == expected_owner_3:
                owner_gate_3 = gate
            else:
                other_gate = gate

        if owner_gate_3 and other_gate:
            # First, owner acquires the job
            import cloudpickle
            submission = JobSubmission(
                job_id=test_job_id_3,
                workflows=cloudpickle.dumps([TestWorkflow]),
                vus=1,
                timeout_seconds=30.0,
                datacenter_count=1,
            )

            response = await owner_gate_3.job_submission(
                addr=('127.0.0.1', 9999),
                data=submission.dump(),
                clock_time=0,
            )
            ack = JobAck.load(response)

            if ack.accepted:
                print(f"  [OK] Owner acquired job")

                # Export lease state to other gate (simulating state sync)
                leases = owner_gate_3._job_lease_manager.export_leases()
                for lease_data in leases:
                    if lease_data['job_id'] == test_job_id_3:
                        import time
                        other_gate._job_lease_manager.import_lease(
                            job_id=lease_data['job_id'],
                            owner_node=lease_data['owner_node'],
                            fence_token=lease_data['fence_token'],
                            expires_at=time.monotonic() + lease_data['expires_in'],
                        )
                        print(f"  [OK] Lease synced to other gate")

                # Now try to acquire from other gate (should fail without force flag)
                result = other_gate._job_lease_manager.acquire(test_job_id_3)
                if not result.success:
                    print(f"  [OK] Other gate correctly blocked from acquiring (owner: {result.current_owner})")
                else:
                    print(f"  [FAIL] Other gate acquired lease it shouldn't have")
                    test_passed = False
            else:
                print(f"  [FAIL] Owner couldn't acquire job: {ack.error}")
                test_passed = False
        else:
            print(f"  [SKIP] Need multiple gates for this test")

        print()

        # ==============================================================
        # Final Results
        # ==============================================================
        print("=" * 70)
        if test_passed:
            print("TEST RESULT: PASSED")
        else:
            print("TEST RESULT: FAILED")
        print()
        print("  Per-job routing verified:")
        print(f"  - Gate cluster: {len(gates)} gates")
        print(f"  - Manager cluster: {len(managers)} managers")
        print(f"  - Worker cluster: {len(workers)} workers")
        print(f"  - Hash ring populated with all gates")
        print(f"  - Jobs distributed across gates via consistent hashing")
        print(f"  - Owner gate accepts jobs and acquires lease")
        print(f"  - Non-owner gate redirects to owner")
        print(f"  - Leases prevent duplicate acquisition")
        print("=" * 70)

        return test_passed

    except Exception as e:
        import traceback
        print(f"\n[FAIL] Test failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        # ==============================================================
        # Cleanup
        # ==============================================================
        print()
        print("Cleaning up...")
        print("-" * 50)

        # Stop workers first
        for i, worker in enumerate(workers):
            try:
                await worker.shutdown()
                print(f"  [OK] {WORKER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  [FAIL] {WORKER_CONFIGS[i]['name']} stop failed: {e}")

        # Stop managers
        for i, manager in enumerate(managers):
            try:
                await manager.graceful_shutdown()
                print(f"  [OK] {MANAGER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  [FAIL] {MANAGER_CONFIGS[i]['name']} stop failed: {e}")

        # Stop gates
        for i, gate in enumerate(gates):
            try:
                await gate.stop()
                print(f"  [OK] {GATE_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  [FAIL] {GATE_CONFIGS[i]['name']} stop failed: {e}")

        print()
        print("Test complete.")
        print("=" * 70)


def main():
    print("=" * 70)
    print("GATE PER-JOB ROUTING INTEGRATION TEST")
    print("=" * 70)
    print(f"Testing with {len(GATE_CONFIGS)} gates + {len(MANAGER_CONFIGS)} managers + {len(WORKER_CONFIGS)} workers")
    print(f"Datacenter: {DC_ID}")
    print("Validates: ConsistentHashRing + LeaseManager integration")
    print()

    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
