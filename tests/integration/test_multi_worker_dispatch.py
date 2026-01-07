#!/usr/bin/env python3
"""
Multi-Worker Workflow Dispatch Integration Test.

Tests workflow dependency execution and core allocation:

1. TestWorkflow and TestWorkflowTwo execute concurrently, each getting half
   the available cores (4 cores each on 2 workers with 4 cores each)

2. NonTestWorkflow depends on TestWorkflowTwo - should be enqueued until
   TestWorkflowTwo completes, then get assigned to freed cores

3. NonTestWorkflowTwo depends on BOTH TestWorkflow and TestWorkflowTwo -
   should remain enqueued until both complete

This validates:
- Dependency-based workflow scheduling
- Core allocation (test workflows split cores evenly)
- Enqueued/pending state for dependent workflows
- Eager dispatch when dependencies complete
- Aggregate workflow results pushed to client (WorkflowResultPush)
- Stats updates pushed to client (JobStatusPush)
- Job's workflow_results dict populated with all workflow results
"""

import asyncio
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.graph import Workflow, step, depends
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

class TestWorkflow(Workflow):
    vus = 2000
    duration = "20s"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)

class TestWorkflowTwo(Workflow):
    vus = 500
    duration = "5s"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)

@depends('TestWorkflowTwo')
class NonTestWorkflow(Workflow):
    """Second workflow that should wait for first to complete."""
    vus = 100
    duration = "3s"

    @step()
    async def second_step(self) -> dict:
        return {"status": "done"}

@depends('TestWorkflow', 'TestWorkflowTwo')
class NonTestWorkflowTwo(Workflow):
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

    # Counters for tracking push notifications
    status_updates_received = 0
    workflow_results_received: dict[str, str] = {}  # workflow_name -> status

    def on_status_update(push):
        """Callback for status updates (stats pushes)."""
        nonlocal status_updates_received
        status_updates_received += 1

    def on_workflow_result(push):
        """Callback for workflow completion results."""
        nonlocal workflow_results_received
        workflow_results_received[push.workflow_name] = push.status

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
                    WORKER_MAX_CORES=config["cores"],
                ),
                dc_id=DC_ID,
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
        for idx, manager in enumerate(managers):
            registered_workers = len(manager._workers)
            registered_managers = len(manager._get_active_manager_peer_addrs())
            total_cores = manager._get_total_available_cores()
            print(f'  Registered managers for manager {idx}: {registered_managers}')
            print(f"  Registered workers for manager {idx}: {registered_workers}")
            print(f"  Total available cores for manager {idx}: {total_cores}")


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
        # STEP 5: Submit job with all workflows
        # ==============================================================
        print("[5/10] Submitting job with all 4 workflows...")
        print("-" * 60)

        job_id = await client.submit_job(
            workflows=[([], TestWorkflow()), ([], TestWorkflowTwo()), (["TestWorkflowTwo"],NonTestWorkflow()), (["TestWorkflow", "TestWorkflowTwo"], NonTestWorkflowTwo())],
            timeout_seconds=120.0,
            on_status_update=on_status_update,
            on_workflow_result=on_workflow_result,
        )
        print(f"  Job submitted: {job_id}")

        # Wait a moment for dispatch to begin
        await asyncio.sleep(2)

        # ==============================================================
        # STEP 6: Verify initial state - test workflows running, dependent workflows pending
        # ==============================================================
        print()
        print("[6/10] Verifying initial workflow state...")
        print("-" * 60)

        all_workflow_names = ['TestWorkflow', 'TestWorkflowTwo', 'NonTestWorkflow', 'NonTestWorkflowTwo']

        # Helper to get workflow status by name
        def get_workflow_by_name(results: dict, name: str):
            for dc_id, workflows in results.items():
                for wf in workflows:
                    if wf.workflow_name == name:
                        return wf
            return None

        # Query initial state
        results = await client.query_workflows(all_workflow_names, job_id=job_id)
        print(f"  Query returned {sum(len(wfs) for wfs in results.values())} workflows")

        test_wf = get_workflow_by_name(results, 'TestWorkflow')
        test_wf_two = get_workflow_by_name(results, 'TestWorkflowTwo')
        non_test_wf = get_workflow_by_name(results, 'NonTestWorkflow')
        non_test_wf_two = get_workflow_by_name(results, 'NonTestWorkflowTwo')

        # Verify test workflows are running/assigned
        test_wf_ok = test_wf and test_wf.status in ('running', 'assigned')
        test_wf_two_ok = test_wf_two and test_wf_two.status in ('running', 'assigned')
        print(f"  TestWorkflow: status={test_wf.status if test_wf else 'NOT FOUND'}, "
              f"cores={test_wf.provisioned_cores if test_wf else 0}, "
              f"workers={len(test_wf.assigned_workers) if test_wf else 0}")
        print(f"  TestWorkflowTwo: status={test_wf_two.status if test_wf_two else 'NOT FOUND'}, "
              f"cores={test_wf_two.provisioned_cores if test_wf_two else 0}, "
              f"workers={len(test_wf_two.assigned_workers) if test_wf_two else 0}")

        # Verify dependent workflows are pending/enqueued
        non_test_pending = non_test_wf and non_test_wf.status == 'pending'
        non_test_two_pending = non_test_wf_two and non_test_wf_two.status == 'pending'
        print(f"  NonTestWorkflow: status={non_test_wf.status if non_test_wf else 'NOT FOUND'}, "
              f"is_enqueued={non_test_wf.is_enqueued if non_test_wf else False}")
        print(f"  NonTestWorkflowTwo: status={non_test_wf_two.status if non_test_wf_two else 'NOT FOUND'}, "
              f"is_enqueued={non_test_wf_two.is_enqueued if non_test_wf_two else False}")

        initial_state_ok = test_wf_ok and test_wf_two_ok and non_test_pending and non_test_two_pending
        print(f"\n  Initial state verification: {'PASS' if initial_state_ok else 'FAIL'}")

        # ==============================================================
        # STEP 7: Poll for TestWorkflowTwo to complete
        # ==============================================================
        print()
        print("[7/10] Waiting for TestWorkflowTwo to complete...")
        print("-" * 60)

        test_wf_two_completed = False
        for i in range(60):  # 60 second timeout
            results = await client.query_workflows(['TestWorkflowTwo'], job_id=job_id)
            test_wf_two = get_workflow_by_name(results, 'TestWorkflowTwo')

            if test_wf_two and test_wf_two.status == 'completed':
                test_wf_two_completed = True
                print(f"  TestWorkflowTwo completed after {i+1}s")
                break

            # While waiting, verify dependent workflows remain pending
            dep_results = await client.query_workflows(['NonTestWorkflow', 'NonTestWorkflowTwo'], job_id=job_id)
            non_test_wf = get_workflow_by_name(dep_results, 'NonTestWorkflow')
            non_test_wf_two = get_workflow_by_name(dep_results, 'NonTestWorkflowTwo')

            if i % 5 == 0:  # Log every 5 seconds
                print(f"  [{i}s] TestWorkflowTwo: {test_wf_two.status if test_wf_two else 'NOT FOUND'}, "
                      f"NonTestWorkflow: {non_test_wf.status if non_test_wf else 'NOT FOUND'}, "
                      f"NonTestWorkflowTwo: {non_test_wf_two.status if non_test_wf_two else 'NOT FOUND'}")

            await asyncio.sleep(1)

        if not test_wf_two_completed:
            print("  ERROR: TestWorkflowTwo did not complete in time")
            return False

        # ==============================================================
        # STEP 8: Verify TestWorkflow still running, NonTestWorkflow assigned,
        #         NonTestWorkflowTwo still pending
        # ==============================================================
        print()
        print("[8/10] Verifying state after TestWorkflowTwo completed...")
        print("-" * 60)

        # Small delay for dispatch to happen
        await asyncio.sleep(1)

        results = await client.query_workflows(all_workflow_names, job_id=job_id)

        test_wf = get_workflow_by_name(results, 'TestWorkflow')
        non_test_wf = get_workflow_by_name(results, 'NonTestWorkflow')
        non_test_wf_two = get_workflow_by_name(results, 'NonTestWorkflowTwo')

        # TestWorkflow should still be running (longer duration)
        test_wf_still_running = test_wf and test_wf.status in ('running', 'assigned')
        print(f"  TestWorkflow: status={test_wf.status if test_wf else 'NOT FOUND'} "
              f"(expected: running/assigned) {'PASS' if test_wf_still_running else 'FAIL'}")

        # NonTestWorkflow should now be assigned/running (dependency on TestWorkflowTwo met)
        non_test_assigned = non_test_wf and non_test_wf.status in ('running', 'assigned', 'completed')
        print(f"  NonTestWorkflow: status={non_test_wf.status if non_test_wf else 'NOT FOUND'}, "
              f"workers={non_test_wf.assigned_workers if non_test_wf else []} "
              f"(expected: running/assigned) {'PASS' if non_test_assigned else 'FAIL'}")

        # NonTestWorkflowTwo should still be pending (needs both TestWorkflow AND TestWorkflowTwo)
        non_test_two_still_pending = non_test_wf_two and non_test_wf_two.status == 'pending'
        print(f"  NonTestWorkflowTwo: status={non_test_wf_two.status if non_test_wf_two else 'NOT FOUND'} "
              f"(expected: pending) {'PASS' if non_test_two_still_pending else 'FAIL'}")

        step8_ok = test_wf_still_running and non_test_assigned and non_test_two_still_pending
        print(f"\n  Post-TestWorkflowTwo state: {'PASS' if step8_ok else 'FAIL'}")

        # ==============================================================
        # STEP 9: Wait for TestWorkflow to complete, verify NonTestWorkflowTwo gets assigned
        # ==============================================================
        print()
        print("[9/10] Waiting for TestWorkflow to complete...")
        print("-" * 60)

        test_wf_completed = False
        for i in range(60):  # 60 second timeout
            results = await client.query_workflows(['TestWorkflow'], job_id=job_id)
            test_wf = get_workflow_by_name(results, 'TestWorkflow')

            if test_wf and test_wf.status == 'completed':
                test_wf_completed = True
                print(f"  TestWorkflow completed after {i+1}s")
                break

            if i % 5 == 0:
                print(f"  [{i}s] TestWorkflow: {test_wf.status if test_wf else 'NOT FOUND'}")

            await asyncio.sleep(1)

        if not test_wf_completed:
            print("  ERROR: TestWorkflow did not complete in time")
            return False

        # Small delay for dispatch
        await asyncio.sleep(1)

        # Verify NonTestWorkflowTwo is now assigned
        results = await client.query_workflows(['NonTestWorkflowTwo'], job_id=job_id)
        non_test_wf_two = get_workflow_by_name(results, 'NonTestWorkflowTwo')

        non_test_two_assigned = non_test_wf_two and non_test_wf_two.status in ('running', 'assigned', 'completed')
        print(f"  NonTestWorkflowTwo: status={non_test_wf_two.status if non_test_wf_two else 'NOT FOUND'}, "
              f"workers={non_test_wf_two.assigned_workers if non_test_wf_two else []} "
              f"(expected: running/assigned) {'PASS' if non_test_two_assigned else 'FAIL'}")

        # ==============================================================
        # STEP 10: Wait for all remaining workflows to complete
        # ==============================================================
        print()
        print("[10/10] Waiting for NonTestWorkflow and NonTestWorkflowTwo to complete...")
        print("-" * 60)

        all_complete = False
        for i in range(60):
            results = await client.query_workflows(['NonTestWorkflow', 'NonTestWorkflowTwo'], job_id=job_id)
            non_test_wf = get_workflow_by_name(results, 'NonTestWorkflow')
            non_test_wf_two = get_workflow_by_name(results, 'NonTestWorkflowTwo')

            non_test_done = non_test_wf and non_test_wf.status == 'completed'
            non_test_two_done = non_test_wf_two and non_test_wf_two.status == 'completed'

            if non_test_done and non_test_two_done:
                all_complete = True
                print(f"  All workflows completed after {i+1}s")
                break

            if i % 5 == 0:
                print(f"  [{i}s] NonTestWorkflow: {non_test_wf.status if non_test_wf else 'NOT FOUND'}, "
                      f"NonTestWorkflowTwo: {non_test_wf_two.status if non_test_wf_two else 'NOT FOUND'}")

            await asyncio.sleep(1)

        if not all_complete:
            print("  WARNING: Not all workflows completed in time")

        # ==============================================================
        # STEP 11: Verify aggregate results and stats updates
        # ==============================================================
        print()
        print("[11/11] Verifying aggregate results and stats updates...")
        print("-" * 60)

        # Give a moment for any final push notifications
        await asyncio.sleep(1)

        # Check workflow results received via callback
        expected_workflows = {'TestWorkflow', 'TestWorkflowTwo', 'NonTestWorkflow', 'NonTestWorkflowTwo'}
        received_workflows = set(workflow_results_received.keys())

        workflow_results_ok = received_workflows == expected_workflows
        print(f"  Workflow results received: {len(workflow_results_received)}/4")
        for workflow_name, status in sorted(workflow_results_received.items()):
            print(f"    - {workflow_name}: {status}")

        if not workflow_results_ok:
            missing = expected_workflows - received_workflows
            extra = received_workflows - expected_workflows
            if missing:
                print(f"  Missing workflow results: {missing}")
            if extra:
                print(f"  Unexpected workflow results: {extra}")

        print(f"  Workflow results verification: {'PASS' if workflow_results_ok else 'FAIL'}")

        # Check stats updates received
        stats_updates_ok = status_updates_received > 0
        print(f"\n  Status updates received: {status_updates_received}")
        print(f"  Stats updates verification (>0): {'PASS' if stats_updates_ok else 'FAIL'}")

        # Also check the job result's workflow_results dict
        job_result = client.get_job_status(job_id)
        job_workflow_results_ok = False
        if job_result:
            job_workflow_results = set(job_result.workflow_results.keys())
            # workflow_results is keyed by workflow_id, not name, so check count
            job_workflow_results_ok = len(job_result.workflow_results) == 4
            print(f"\n  Job result workflow_results count: {len(job_result.workflow_results)}/4")
            for workflow_id, wf_result in sorted(job_result.workflow_results.items()):
                print(f"    - {wf_result.workflow_name} ({workflow_id}): {wf_result.status}")
            print(f"  Job workflow_results verification: {'PASS' if job_workflow_results_ok else 'FAIL'}")

        # ==============================================================
        # Final Results
        # ==============================================================
        print()
        print("=" * 70)
        all_passed = (
            initial_state_ok and
            step8_ok and
            non_test_two_assigned and
            all_complete and
            workflow_results_ok and
            stats_updates_ok and
            job_workflow_results_ok
        )

        if all_passed:
            print("TEST RESULT: PASSED")
        else:
            print("TEST RESULT: FAILED")

        print()
        print("  Test Summary:")
        print(f"    - Initial state (test wfs running, deps pending): {'PASS' if initial_state_ok else 'FAIL'}")
        print(f"    - After TestWorkflowTwo done (NonTestWorkflow assigned): {'PASS' if step8_ok else 'FAIL'}")
        print(f"    - After TestWorkflow done (NonTestWorkflowTwo assigned): {'PASS' if non_test_two_assigned else 'FAIL'}")
        print(f"    - All workflows completed: {'PASS' if all_complete else 'FAIL'}")
        print(f"    - Workflow results pushed to client (4/4): {'PASS' if workflow_results_ok else 'FAIL'}")
        print(f"    - Stats updates received (>0): {'PASS' if stats_updates_ok else 'FAIL'}")
        print(f"    - Job workflow_results populated: {'PASS' if job_workflow_results_ok else 'FAIL'}")
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
    print("WORKFLOW DEPENDENCY & CORE ALLOCATION TEST")
    print("=" * 70)
    print()
    print("This test validates:")
    print("  1. TestWorkflow and TestWorkflowTwo run concurrently (split cores)")
    print("  2. NonTestWorkflow (depends on TestWorkflowTwo) waits, then runs")
    print("  3. NonTestWorkflowTwo (depends on BOTH) waits for both to complete")
    print("  4. Dependency-based scheduling triggers eager dispatch")
    print("  5. Workflow results are pushed to client for each completed workflow")
    print("  6. Stats updates are pushed to client (>0 received)")
    print("  7. Job's workflow_results dict is populated with all 4 workflow results")
    print()
    print("Workflow dependencies:")
    print("  - TestWorkflow: no dependencies")
    print("  - TestWorkflowTwo: no dependencies")
    print("  - NonTestWorkflow: depends on TestWorkflowTwo")
    print("  - NonTestWorkflowTwo: depends on TestWorkflow AND TestWorkflowTwo")
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
