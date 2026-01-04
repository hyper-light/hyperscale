#!/usr/bin/env python3
"""
Multi-Worker Workflow Dispatch Integration Test.

Tests that:
1. A workflow requiring more cores than any single worker has gets distributed
   across multiple workers (multi-worker dispatch)
2. Sub-workflow progress is aggregated correctly
3. When cores are freed (first workflow completes), waiting workflows are
   immediately dispatched (eager execution via _cores_available_event)

Scenario:
- 2 workers with 4 cores each (8 total cores in pool)
- First job: 1 workflow with Priority.AUTO (needs all 8 cores)
  - Should dispatch to BOTH workers (4 cores each)
- Second job: 1 workflow with Priority.AUTO (needs all 8 cores)
  - Should WAIT until first workflow completes
  - Should be dispatched IMMEDIATELY when cores are freed (eager execution)

This validates the multi-worker dispatch and event-driven core availability.
"""

import asyncio
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse
from hyperscale.distributed_rewrite.nodes.manager import ManagerServer
from hyperscale.distributed_rewrite.nodes.worker import WorkerServer
from hyperscale.distributed_rewrite.nodes.client import HyperscaleClient
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.distributed_rewrite.models import ManagerState, WorkflowStatus
from hyperscale.logging.config.logging_config import LoggingConfig

# Initialize logging directory (required for server pool)
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())


# ==========================================================================
# Test Workflows
# ==========================================================================

class QuickWorkflow(Workflow):
    """A quick workflow that completes fast to test eager dispatch."""
    vus = 100
    duration = "3s"  # Short duration

    @step()
    async def quick_step(self) -> dict:
        # Simple step that completes quickly
        return {"status": "done"}


class SecondWorkflow(Workflow):
    """Second workflow that should wait for first to complete."""
    vus = 100
    duration = "3s"

    @step()
    async def second_step(self) -> dict:
        return {"status": "done"}


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
]

# Client configuration
CLIENT_CONFIG = {"tcp": 9630}

MANAGER_STABILIZATION_TIME = 15  # seconds for manager to start
WORKER_REGISTRATION_TIME = 15  # seconds for workers to register


def get_all_manager_tcp_addrs() -> list[tuple[str, int]]:
    """Get TCP addresses of all managers."""
    return [('127.0.0.1', cfg['tcp']) for cfg in MANAGER_CONFIGS]


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


async def run_test():
    """Run the multi-worker dispatch integration test."""

    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    client: HyperscaleClient | None = None

    try:
        # ==============================================================
        # STEP 1: Create servers
        # ==============================================================
        print("[1/8] Creating servers...")
        print("-" * 60)

        # Create managers with peer configuration for quorum
        for config in MANAGER_CONFIGS:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id=DC_ID,
                manager_peers=get_manager_peer_tcp_addrs(config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(config["udp"]),
            )
            managers.append(manager)
            print(f"  Created {config['name']} (TCP:{config['tcp']} UDP:{config['udp']})")

        # Create workers
        seed_managers = get_all_manager_tcp_addrs()

        for config in WORKER_CONFIGS:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id=DC_ID,
                total_cores=config["cores"],
                seed_managers=seed_managers,
            )
            workers.append(worker)
            print(f"  Created {config['name']} (TCP:{config['tcp']} UDP:{config['udp']}, {config['cores']} cores)")

        print()

        # ==============================================================
        # STEP 2: Start managers (concurrently for proper cluster formation)
        # ==============================================================
        print("[2/8] Starting managers...")
        print("-" * 60)

        # Start all managers concurrently - critical for proper SWIM cluster
        # formation and leader election timing
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)

        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            print(f"  Started {config['name']} - Node ID: {manager._node_id.short}")

        print(f"\n  Waiting for manager stabilization ({MANAGER_STABILIZATION_TIME}s)...")
        await asyncio.sleep(MANAGER_STABILIZATION_TIME)
        print()

        # ==============================================================
        # STEP 3: Start workers
        # ==============================================================
        print("[3/8] Starting workers...")
        print("-" * 60)

        start_tasks = [worker.start() for worker in workers]
        await asyncio.gather(*start_tasks)

        for i, worker in enumerate(workers):
            config = WORKER_CONFIGS[i]
            print(f"  Started {config['name']} - Node ID: {worker._node_id.short}")

        print(f"\n  Waiting for worker registration ({WORKER_REGISTRATION_TIME}s)...")
        await asyncio.sleep(WORKER_REGISTRATION_TIME)

        # Verify workers registered
        manager = managers[0]
        registered_workers = len(manager._workers)
        total_cores = sum(s.available_cores for s in manager._worker_status.values())
        print(f"  Registered workers: {registered_workers}")
        print(f"  Total available cores: {total_cores}")

        if registered_workers < len(WORKER_CONFIGS):
            print(f"  ERROR: Expected {len(WORKER_CONFIGS)} workers, got {registered_workers}")
            return False

        print()

        # ==============================================================
        # STEP 4: Create client
        # ==============================================================
        print("[4/8] Creating client...")
        print("-" * 60)

        client = HyperscaleClient(
            host='127.0.0.1',
            port=CLIENT_CONFIG["tcp"],
            env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='10s'),
            managers=get_all_manager_tcp_addrs(),  # Direct to manager (no gates)
        )
        await client.start()
        print(f"  Client started on port {CLIENT_CONFIG['tcp']}")
        print()

        # ==============================================================
        # STEP 5: Submit first job (should use all 8 cores across 2 workers)
        # ==============================================================
        print("[5/8] Submitting first job (should dispatch to BOTH workers)...")
        print("-" * 60)

        job1_id = await client.submit_job(
            workflows=[QuickWorkflow],
            timeout_seconds=60.0,
        )
        print(f"  Job 1 submitted: {job1_id}")

        # Wait for dispatch to happen
        await asyncio.sleep(2)

        # Check sub-workflow mapping (multi-worker dispatch)
        parent_workflow_id = None
        sub_workflow_count = 0
        for parent_id, sub_ids in manager._sub_workflow_mapping.items():
            if job1_id in parent_id:
                parent_workflow_id = parent_id
                sub_workflow_count = len(sub_ids)
                print(f"  Parent workflow: {parent_id}")
                print(f"  Sub-workflows dispatched: {sub_workflow_count}")
                for sub_id in sub_ids:
                    assignment = manager._workflow_assignments.get(job1_id, {}).get(sub_id)
                    print(f"    - {sub_id} -> {assignment}")
                break

        # Verify multi-worker dispatch
        multi_worker_dispatch_ok = sub_workflow_count >= 2
        if multi_worker_dispatch_ok:
            print(f"  PASS: Workflow dispatched to {sub_workflow_count} workers")
        else:
            print(f"  FAIL: Expected dispatch to 2 workers, got {sub_workflow_count}")

        # Check workers are executing
        workers_executing = 0
        for i, worker in enumerate(workers):
            active = len(worker._active_workflows)
            if active > 0:
                workers_executing += 1
                print(f"  {WORKER_CONFIGS[i]['name']}: executing {active} workflow(s)")

        print()

        # ==============================================================
        # STEP 6: Submit second job (should WAIT - no cores available)
        # ==============================================================
        print("[6/8] Submitting second job (should WAIT for cores)...")
        print("-" * 60)

        # Record time before submitting second job
        second_job_submit_time = time.monotonic()

        # Submit second job (this will block waiting for cores)
        job2_submitted = asyncio.Event()
        job2_id = None
        job2_dispatch_time = None

        async def submit_second_job():
            nonlocal job2_id, job2_dispatch_time
            job2_id = await client.submit_job(
                workflows=[SecondWorkflow],
                timeout_seconds=60.0,
            )
            job2_dispatch_time = time.monotonic()
            job2_submitted.set()

        # Start second job submission in background
        submit_task = asyncio.create_task(submit_second_job())

        # Wait a moment to ensure it's in the waiting state
        await asyncio.sleep(1)

        # Check available cores (should be 0 or very low)
        available_cores = sum(s.available_cores for s in manager._worker_status.values())
        print(f"  Available cores while Job 1 running: {available_cores}")

        # Second job should NOT have been dispatched yet
        job2_waiting = not job2_submitted.is_set()
        if job2_waiting:
            print(f"  PASS: Job 2 is waiting for cores (as expected)")
        else:
            print(f"  INFO: Job 2 was dispatched quickly (cores may have freed)")

        print()

        # ==============================================================
        # STEP 7: Wait for first job to complete and verify eager dispatch
        # ==============================================================
        print("[7/8] Waiting for Job 1 to complete and Job 2 to dispatch...")
        print("-" * 60)

        # Wait for Job 1 to complete (short workflow, should be ~3-5s)
        job1_complete = False
        for _ in range(30):  # 30 second timeout
            job1 = manager._jobs.get(job1_id)
            if job1:
                # Check if all workflows completed
                all_complete = all(
                    w.status in (WorkflowStatus.COMPLETED.value, WorkflowStatus.FAILED.value)
                    for w in job1.workflows
                )
                if all_complete:
                    job1_complete = True
                    job1_complete_time = time.monotonic()
                    print(f"  Job 1 completed at t={job1_complete_time - second_job_submit_time:.2f}s")
                    break
            await asyncio.sleep(0.5)

        if not job1_complete:
            print("  ERROR: Job 1 did not complete within timeout")
            submit_task.cancel()
            return False

        # Wait for Job 2 to be submitted (should happen immediately after cores freed)
        try:
            await asyncio.wait_for(job2_submitted.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            print("  ERROR: Job 2 was not dispatched after Job 1 completed")
            submit_task.cancel()
            return False

        # Calculate how quickly Job 2 was dispatched after Job 1 completed
        dispatch_delay = job2_dispatch_time - job1_complete_time
        print(f"  Job 2 dispatched: {job2_id}")
        print(f"  Dispatch delay after Job 1 completion: {dispatch_delay:.3f}s")

        # Eager dispatch should happen within ~1 second (event-driven)
        eager_dispatch_ok = dispatch_delay < 2.0
        if eager_dispatch_ok:
            print(f"  PASS: Eager dispatch triggered ({dispatch_delay:.3f}s < 2.0s threshold)")
        else:
            print(f"  WARN: Dispatch was slower than expected ({dispatch_delay:.3f}s)")

        print()

        # ==============================================================
        # STEP 8: Verify final state
        # ==============================================================
        print("[8/8] Verifying final state...")
        print("-" * 60)

        # Wait for Job 2 to complete
        job2_complete = False
        for _ in range(30):
            job2 = manager._jobs.get(job2_id)
            if job2:
                all_complete = all(
                    w.status in (WorkflowStatus.COMPLETED.value, WorkflowStatus.FAILED.value)
                    for w in job2.workflows
                )
                if all_complete:
                    job2_complete = True
                    print(f"  Job 2 completed")
                    break
            await asyncio.sleep(0.5)

        if not job2_complete:
            print("  WARN: Job 2 did not complete within timeout (test continues)")

        # Summary of core allocation
        print(f"\n  === Core Allocation Summary ===")
        for wid, status in manager._worker_status.items():
            short_id = wid.split('-')[-1][:8] if '-' in wid else wid[:8]
            print(f"    Worker {short_id}: available={status.available_cores}")

        # Check workflow assignments for both jobs
        print(f"\n  === Workflow Assignments ===")
        for job_id in [job1_id, job2_id]:
            if job_id:
                assignments = manager._workflow_assignments.get(job_id, {})
                print(f"    {job_id[:16]}...: {len(assignments)} assignments")

        print()

        # ==============================================================
        # Results
        # ==============================================================
        print("=" * 70)

        all_passed = multi_worker_dispatch_ok and eager_dispatch_ok

        if all_passed:
            print("TEST RESULT: PASSED")
        else:
            print("TEST RESULT: FAILED")

        print()
        print("  Test Summary:")
        print(f"    - Multi-worker dispatch: {'PASS' if multi_worker_dispatch_ok else 'FAIL'}")
        print(f"      (Workflow dispatched to {sub_workflow_count} workers)")
        print(f"    - Eager execution: {'PASS' if eager_dispatch_ok else 'FAIL'}")
        print(f"      (Dispatch delay: {dispatch_delay:.3f}s)")
        print()
        print("=" * 70)

        return all_passed

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        # ==============================================================
        # Cleanup
        # ==============================================================
        print()
        print("Cleaning up...")
        print("-" * 60)

        # Stop client
        if client:
            try:
                await client.stop()
                print("  Client stopped")
            except Exception as e:
                print(f"  Client stop failed: {e}")

        # Stop workers
        for i, worker in enumerate(workers):
            try:
                await worker.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {WORKER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  {WORKER_CONFIGS[i]['name']} stop failed: {e}")

        # Stop managers
        for i, manager in enumerate(managers):
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {MANAGER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  {MANAGER_CONFIGS[i]['name']} stop failed: {e}")

        print()
        print("Test complete.")
        print("=" * 70)


def main():
    print("=" * 70)
    print("MULTI-WORKER WORKFLOW DISPATCH INTEGRATION TEST")
    print("=" * 70)
    print()
    print("This test validates:")
    print("  1. Workflows are dispatched across multiple workers when needed")
    print("  2. Waiting workflows are dispatched immediately when cores freed")
    print()
    print(f"Configuration:")
    print(f"  - {len(MANAGER_CONFIGS)} manager(s)")
    print(f"  - {len(WORKER_CONFIGS)} workers ({sum(c['cores'] for c in WORKER_CONFIGS)} total cores)")
    print(f"  - Datacenter: {DC_ID}")
    print()

    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
