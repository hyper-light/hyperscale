#!/usr/bin/env python3
"""
Gate Cross-Datacenter Dispatch Integration Test.

Tests workflow execution across two datacenters coordinated by a Gate:

1. Gate receives job submission from client
2. Gate dispatches to managers in two datacenters (DC-EAST, DC-WEST)
3. Each datacenter has 3 managers (for quorum) and 4 workers (2 cores each)
4. TestWorkflow and TestWorkflowTwo execute concurrently across both DCs
5. Dependent workflows (NonTestWorkflow, NonTestWorkflowTwo) wait for dependencies
6. Gate aggregates results from both DCs and pushes to client

This validates:
- Gate job submission and dispatch to multiple DCs
- Cross-DC workflow coordination
- Manager quorum formation per DC
- Worker registration and core allocation
- Windowed stats aggregation across DCs
- Aggregate workflow results pushed to client
"""

import asyncio
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.graph import Workflow, step, depends
from hyperscale.testing import URL, HTTPResponse
from hyperscale.distributed_rewrite.nodes.gate import GateServer
from hyperscale.distributed_rewrite.nodes.manager import ManagerServer
from hyperscale.distributed_rewrite.nodes.worker import WorkerServer
from hyperscale.distributed_rewrite.nodes.client import HyperscaleClient
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.distributed_rewrite.models import ManagerState, WorkflowStatus
from hyperscale.distributed_rewrite.jobs import WindowedStatsPush
from hyperscale.logging.config.logging_config import LoggingConfig

# Initialize logging directory (required for server pool)
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())


# ==========================================================================
# Test Workflows
# ==========================================================================

class TestWorkflow(Workflow):
    vus: int = 2000
    duration: str = "20s"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)

class TestWorkflowTwo(Workflow):
    vus: int = 500
    duration: str = "5s"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)

@depends('TestWorkflowTwo')
class NonTestWorkflow(Workflow):
    """Second workflow that should wait for first to complete."""
    vus: int = 100
    duration: str = "3s"

    @step()
    async def second_step(self) -> dict:
        return {"status": "done"}

@depends('TestWorkflow', 'TestWorkflowTwo')
class NonTestWorkflowTwo(Workflow):
    """Second workflow that should wait for first to complete."""
    vus: int = 100
    duration: str = "3s"

    @step()
    async def second_step(self) -> dict:
        return {"status": "done"}


# ==========================================================================
# Configuration
# ==========================================================================

# Datacenter IDs
DC_EAST = "DC-EAST"
DC_WEST = "DC-WEST"

# Gate configuration - 3 gates for quorum
GATE_CONFIGS = [
    {"name": "Gate 1", "tcp": 8000, "udp": 8001},
    {"name": "Gate 2", "tcp": 8002, "udp": 8003},
    {"name": "Gate 3", "tcp": 8004, "udp": 8005},
]

# Manager configuration per DC - 3 managers each for quorum
# DC-EAST managers: ports 9000-9005
# DC-WEST managers: ports 9100-9105
DC_EAST_MANAGER_CONFIGS = [
    {"name": "DC-EAST Manager 1", "tcp": 9000, "udp": 9001},
    {"name": "DC-EAST Manager 2", "tcp": 9002, "udp": 9003},
    {"name": "DC-EAST Manager 3", "tcp": 9004, "udp": 9005},
]

DC_WEST_MANAGER_CONFIGS = [
    {"name": "DC-WEST Manager 1", "tcp": 9100, "udp": 9101},
    {"name": "DC-WEST Manager 2", "tcp": 9102, "udp": 9103},
    {"name": "DC-WEST Manager 3", "tcp": 9104, "udp": 9105},
]

# Worker configuration per DC - 4 workers each with 2 cores
# DC-EAST workers: TCP ports 9200, 9250, 9300, 9350 (stride 50)
# DC-WEST workers: TCP ports 9400, 9450, 9500, 9550 (stride 50)
DC_EAST_WORKER_CONFIGS = [
    {"name": "DC-EAST Worker 1", "tcp": 9200, "udp": 9210, "cores": 2},
    {"name": "DC-EAST Worker 2", "tcp": 9250, "udp": 9260, "cores": 2},
    {"name": "DC-EAST Worker 3", "tcp": 9300, "udp": 9310, "cores": 2},
    {"name": "DC-EAST Worker 4", "tcp": 9350, "udp": 9360, "cores": 2},
]

DC_WEST_WORKER_CONFIGS = [
    {"name": "DC-WEST Worker 1", "tcp": 9400, "udp": 9410, "cores": 2},
    {"name": "DC-WEST Worker 2", "tcp": 9450, "udp": 9460, "cores": 2},
    {"name": "DC-WEST Worker 3", "tcp": 9500, "udp": 9510, "cores": 2},
    {"name": "DC-WEST Worker 4", "tcp": 9550, "udp": 9560, "cores": 2},
]

# Client configuration
CLIENT_CONFIG = {"tcp": 9630}

MANAGER_STABILIZATION_TIME = 15  # seconds for managers to stabilize
WORKER_REGISTRATION_TIME = 15  # seconds for workers to register
GATE_STABILIZATION_TIME = 15  # seconds for gates to form cluster and discover DCs


def get_dc_manager_tcp_addrs(dc_configs: list[dict]) -> list[tuple[str, int]]:
    """Get TCP addresses of all managers in a DC."""
    return [('127.0.0.1', cfg['tcp']) for cfg in dc_configs]


def get_dc_manager_udp_addrs(dc_configs: list[dict]) -> list[tuple[str, int]]:
    """Get UDP addresses of all managers in a DC."""
    return [('127.0.0.1', cfg['udp']) for cfg in dc_configs]


def get_manager_peer_tcp_addrs(dc_configs: list[dict], exclude_port: int) -> list[tuple[str, int]]:
    """Get TCP addresses of all managers except the one with exclude_port."""
    return [
        ('127.0.0.1', cfg['tcp'])
        for cfg in dc_configs
        if cfg['tcp'] != exclude_port
    ]


def get_manager_peer_udp_addrs(dc_configs: list[dict], exclude_port: int) -> list[tuple[str, int]]:
    """Get UDP addresses of all managers except the one with exclude_port."""
    return [
        ('127.0.0.1', cfg['udp'])
        for cfg in dc_configs
        if cfg['udp'] != exclude_port
    ]


def get_all_gate_tcp_addrs() -> list[tuple[str, int]]:
    """Get TCP addresses of all gates."""
    return [('127.0.0.1', cfg['tcp']) for cfg in GATE_CONFIGS]


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


async def run_test():
    """Run the Gate cross-DC dispatch integration test."""

    gates: list[GateServer] = []
    dc_east_managers: list[ManagerServer] = []
    dc_west_managers: list[ManagerServer] = []
    dc_east_workers: list[WorkerServer] = []
    dc_west_workers: list[WorkerServer] = []
    client: HyperscaleClient | None = None

    # Container for tracking push notifications (avoids nonlocal anti-pattern)
    counters: dict[str, int | dict] = {
        'status_updates': 0,
        'progress_updates': 0,
        'workflow_results': {},  # workflow_name -> status
        'workflow_progress_counts': {},  # workflow_name -> update count
    }

    def on_status_update(push):
        """Callback for critical status updates (job status changes)."""
        counters['status_updates'] += 1

    def on_progress_update(push: WindowedStatsPush):
        """Callback for streaming windowed stats updates."""
        counters['progress_updates'] += 1
        # Track per-workflow progress updates
        workflow_name = push.workflow_name
        if workflow_name:
            progress_counts = counters['workflow_progress_counts']
            progress_counts[workflow_name] = progress_counts.get(workflow_name, 0) + 1

    def on_workflow_result(push):
        """Callback for workflow completion results."""
        counters['workflow_results'][push.workflow_name] = push.status

    try:
        # ==============================================================
        # STEP 1: Create servers
        # ==============================================================
        print("[1/9] Creating servers...")
        print("-" * 60)

        # Create DC-EAST managers
        print("  DC-EAST Managers:")
        for config in DC_EAST_MANAGER_CONFIGS:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id=DC_EAST,
                manager_peers=get_manager_peer_tcp_addrs(DC_EAST_MANAGER_CONFIGS, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(DC_EAST_MANAGER_CONFIGS, config["udp"]),
                gate_addrs=get_all_gate_tcp_addrs(),
            )
            dc_east_managers.append(manager)
            print(f"    Created {config['name']} (TCP:{config['tcp']} UDP:{config['udp']})")

        # Create DC-WEST managers
        print("  DC-WEST Managers:")
        for config in DC_WEST_MANAGER_CONFIGS:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id=DC_WEST,
                manager_peers=get_manager_peer_tcp_addrs(DC_WEST_MANAGER_CONFIGS, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(DC_WEST_MANAGER_CONFIGS, config["udp"]),
                gate_addrs=get_all_gate_tcp_addrs(),
            )
            dc_west_managers.append(manager)
            print(f"    Created {config['name']} (TCP:{config['tcp']} UDP:{config['udp']})")

        # Create DC-EAST workers
        print("  DC-EAST Workers:")
        dc_east_seed_managers = get_dc_manager_tcp_addrs(DC_EAST_MANAGER_CONFIGS)
        for config in DC_EAST_WORKER_CONFIGS:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                    WORKER_MAX_CORES=config["cores"],
                ),
                dc_id=DC_EAST,
                seed_managers=dc_east_seed_managers,
            )
            dc_east_workers.append(worker)
            print(f"    Created {config['name']} (TCP:{config['tcp']} UDP:{config['udp']}, {config['cores']} cores)")

        # Create DC-WEST workers
        print("  DC-WEST Workers:")
        dc_west_seed_managers = get_dc_manager_tcp_addrs(DC_WEST_MANAGER_CONFIGS)
        for config in DC_WEST_WORKER_CONFIGS:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                    WORKER_MAX_CORES=config["cores"],
                ),
                dc_id=DC_WEST,
                seed_managers=dc_west_seed_managers,
            )
            dc_west_workers.append(worker)
            print(f"    Created {config['name']} (TCP:{config['tcp']} UDP:{config['udp']}, {config['cores']} cores)")

        # Create Gates (3-gate cluster for quorum)
        print("  Gates:")
        for config in GATE_CONFIGS:
            gate = GateServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id="global",
                datacenter_managers={
                    DC_EAST: get_dc_manager_tcp_addrs(DC_EAST_MANAGER_CONFIGS),
                    DC_WEST: get_dc_manager_tcp_addrs(DC_WEST_MANAGER_CONFIGS),
                },
                datacenter_manager_udp={
                    DC_EAST: get_dc_manager_udp_addrs(DC_EAST_MANAGER_CONFIGS),
                    DC_WEST: get_dc_manager_udp_addrs(DC_WEST_MANAGER_CONFIGS),
                },
                gate_peers=get_gate_peer_tcp_addrs(config["tcp"]),
                gate_udp_peers=get_gate_peer_udp_addrs(config["udp"]),
            )
            gates.append(gate)
            print(f"    Created {config['name']} (TCP:{config['tcp']} UDP:{config['udp']})")
        print()

        # ==============================================================
        # STEP 2: Start Gates first (so managers can register)
        # ==============================================================
        print("[2/9] Starting Gates...")
        print("-" * 60)

        # Start all gates concurrently for proper cluster formation
        start_tasks = [gate.start() for gate in gates]
        await asyncio.gather(*start_tasks)

        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            print(f"  Started {config['name']} - Node ID: {gate._node_id.short}")

        print(f"\n  Waiting for gate cluster stabilization ({GATE_STABILIZATION_TIME}s)...")
        await asyncio.sleep(GATE_STABILIZATION_TIME)
        print()

        # ==============================================================
        # STEP 3: Start managers (concurrently per DC)
        # ==============================================================
        print("[3/9] Starting managers...")
        print("-" * 60)

        # Start all managers concurrently
        all_managers = dc_east_managers + dc_west_managers
        start_tasks = [manager.start() for manager in all_managers]
        await asyncio.gather(*start_tasks)

        print("  DC-EAST Managers:")
        for i, manager in enumerate(dc_east_managers):
            config = DC_EAST_MANAGER_CONFIGS[i]
            print(f"    Started {config['name']} - Node ID: {manager._node_id.short}")

        print("  DC-WEST Managers:")
        for i, manager in enumerate(dc_west_managers):
            config = DC_WEST_MANAGER_CONFIGS[i]
            print(f"    Started {config['name']} - Node ID: {manager._node_id.short}")

        print(f"\n  Waiting for manager stabilization ({MANAGER_STABILIZATION_TIME}s)...")
        await asyncio.sleep(MANAGER_STABILIZATION_TIME)
        print()

        # ==============================================================
        # STEP 4: Start workers
        # ==============================================================
        print("[4/9] Starting workers...")
        print("-" * 60)

        # Start all workers concurrently
        all_workers = dc_east_workers + dc_west_workers
        start_tasks = [worker.start() for worker in all_workers]
        await asyncio.gather(*start_tasks)

        print("  DC-EAST Workers:")
        for i, worker in enumerate(dc_east_workers):
            config = DC_EAST_WORKER_CONFIGS[i]
            print(f"    Started {config['name']} - Node ID: {worker._node_id.short}")

        print("  DC-WEST Workers:")
        for i, worker in enumerate(dc_west_workers):
            config = DC_WEST_WORKER_CONFIGS[i]
            print(f"    Started {config['name']} - Node ID: {worker._node_id.short}")

        print(f"\n  Waiting for worker registration ({WORKER_REGISTRATION_TIME}s)...")
        await asyncio.sleep(WORKER_REGISTRATION_TIME)

        # Verify workers registered in each DC
        print("\n  DC-EAST Registration:")
        for idx, manager in enumerate(dc_east_managers):
            total_cores = manager._get_total_available_cores()
            registered_managers = len(manager._get_active_manager_peer_addrs())
            print(f"    Manager {idx}: {registered_managers} peers, {total_cores} available cores")

        print("  DC-WEST Registration:")
        for idx, manager in enumerate(dc_west_managers):
            total_cores = manager._get_total_available_cores()
            registered_managers = len(manager._get_active_manager_peer_addrs())
            print(f"    Manager {idx}: {registered_managers} peers, {total_cores} available cores")

        # Check gates' view of datacenters
        print("\n  Gate Cluster Datacenter View:")
        for i, gate in enumerate(gates):
            config = GATE_CONFIGS[i]
            print(f"    {config['name']}:")
            for dc_id in [DC_EAST, DC_WEST]:
                manager_count = len(gate._datacenter_managers.get(dc_id, []))
                print(f"      {dc_id}: {manager_count} managers configured")

        print()

        # ==============================================================
        # STEP 5: Create client
        # ==============================================================
        print("[5/9] Creating client...")
        print("-" * 60)

        client = HyperscaleClient(
            host='127.0.0.1',
            port=CLIENT_CONFIG["tcp"],
            env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='10s'),
            gates=get_all_gate_tcp_addrs(),  # Connect to all gates
        )
        await client.start()
        print(f"  Client started on port {CLIENT_CONFIG['tcp']}")
        print()

        # ==============================================================
        # STEP 6: Submit job with all workflows
        # ==============================================================
        print("[6/9] Submitting job with all 4 workflows via Gate...")
        print("-" * 60)

        job_id = await client.submit_job(
            workflows=[([], TestWorkflow()), ([], TestWorkflowTwo()), (["TestWorkflowTwo"], NonTestWorkflow()), (["TestWorkflow", "TestWorkflowTwo"], NonTestWorkflowTwo())],
            timeout_seconds=120.0,
            datacenter_count=2,  # Request both DCs
            on_status_update=on_status_update,
            on_workflow_result=on_workflow_result,
            on_progress_update=on_progress_update,
        )
        print(f"  Job submitted: {job_id}")

        # Wait a moment for dispatch to begin
        await asyncio.sleep(3)

        # ==============================================================
        # STEP 7: Verify initial state
        # ==============================================================
        print()
        print("[7/9] Verifying initial workflow state...")
        print("-" * 60)

        all_workflow_names = ['TestWorkflow', 'TestWorkflowTwo', 'NonTestWorkflow', 'NonTestWorkflowTwo']

        # Helper to get workflow status by name (may be in multiple DCs)
        def get_workflows_by_name(results: dict, name: str) -> list:
            workflows = []
            for dc_id, dc_workflows in results.items():
                for wf in dc_workflows:
                    if wf.workflow_name == name:
                        workflows.append((dc_id, wf))
            return workflows

        # Query initial state via gate
        results = await client.query_workflows_via_gate(all_workflow_names, job_id=job_id)
        total_workflows = sum(len(wfs) for wfs in results.values())
        print(f"  Query returned {total_workflows} workflow entries across {len(results)} DCs")

        # Check test workflows are running
        test_wf_entries = get_workflows_by_name(results, 'TestWorkflow')
        test_wf_two_entries = get_workflows_by_name(results, 'TestWorkflowTwo')
        non_test_wf_entries = get_workflows_by_name(results, 'NonTestWorkflow')
        non_test_wf_two_entries = get_workflows_by_name(results, 'NonTestWorkflowTwo')

        print(f"\n  TestWorkflow: {len(test_wf_entries)} entries")
        for dc_id, wf in test_wf_entries:
            print(f"    [{dc_id}] status={wf.status}, cores={wf.provisioned_cores}")

        print(f"  TestWorkflowTwo: {len(test_wf_two_entries)} entries")
        for dc_id, wf in test_wf_two_entries:
            print(f"    [{dc_id}] status={wf.status}, cores={wf.provisioned_cores}")

        print(f"  NonTestWorkflow: {len(non_test_wf_entries)} entries")
        for dc_id, wf in non_test_wf_entries:
            print(f"    [{dc_id}] status={wf.status}, is_enqueued={wf.is_enqueued}")

        print(f"  NonTestWorkflowTwo: {len(non_test_wf_two_entries)} entries")
        for dc_id, wf in non_test_wf_two_entries:
            print(f"    [{dc_id}] status={wf.status}, is_enqueued={wf.is_enqueued}")

        # Verify test workflows are running/assigned in at least one DC
        test_wf_running = any(
            wf.status in ('running', 'assigned')
            for _, wf in test_wf_entries
        )
        test_wf_two_running = any(
            wf.status in ('running', 'assigned')
            for _, wf in test_wf_two_entries
        )
        initial_state_ok = test_wf_running and test_wf_two_running
        print(f"\n  Initial state verification: {'PASS' if initial_state_ok else 'FAIL'}")

        # ==============================================================
        # STEP 8: Wait for all workflows to complete
        # ==============================================================
        print()
        print("[8/9] Waiting for all workflows to complete...")
        print("-" * 60)

        timeout = 90  # seconds
        poll_interval = 5
        start_time = time.time()
        all_complete = False

        while time.time() - start_time < timeout:
            results = await client.query_workflows_via_gate(all_workflow_names, job_id=job_id)

            # Check if all workflows are complete in at least one DC
            completed_workflows = set()
            for dc_id, dc_workflows in results.items():
                for wf in dc_workflows:
                    if wf.status == 'completed':
                        completed_workflows.add(wf.workflow_name)

            elapsed = int(time.time() - start_time)
            print(f"  [{elapsed}s] Completed: {sorted(completed_workflows)}")

            if completed_workflows == set(all_workflow_names):
                all_complete = True
                print(f"  All workflows completed after {elapsed}s")
                break

            await asyncio.sleep(poll_interval)

        if not all_complete:
            print(f"  TIMEOUT: Not all workflows completed within {timeout}s")

        # ==============================================================
        # STEP 9: Verify results and stats
        # ==============================================================
        print()
        print("[9/9] Verifying aggregate results and stats updates...")
        print("-" * 60)

        # Give a moment for any final push notifications
        await asyncio.sleep(2)

        # Check workflow results received via callback
        expected_workflows = {'TestWorkflow', 'TestWorkflowTwo', 'NonTestWorkflow', 'NonTestWorkflowTwo'}
        workflow_results_received = counters['workflow_results']
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

        # Check streaming progress updates received (windowed stats)
        progress_updates_received = counters['progress_updates']
        progress_updates_ok = progress_updates_received > 0
        print(f"\n  Progress updates received (windowed stats): {progress_updates_received}")
        print(f"  Progress updates verification (>0): {'PASS' if progress_updates_ok else 'FAIL'}")

        # Check per-workflow progress updates
        workflow_progress_counts = counters['workflow_progress_counts']
        test_workflow_progress_ok = (
            workflow_progress_counts.get('TestWorkflow', 0) > 0 and
            workflow_progress_counts.get('TestWorkflowTwo', 0) > 0
        )
        print(f"\n  Per-workflow progress updates:")
        for workflow_name in all_workflow_names:
            count = workflow_progress_counts.get(workflow_name, 0)
            print(f"    - {workflow_name}: {count} updates")
        print(f"  Test workflow progress verification (both > 0): {'PASS' if test_workflow_progress_ok else 'FAIL'}")

        # Check job result
        job_result = client.get_job_status(job_id)
        job_workflow_results_ok = False
        if job_result:
            job_workflow_results_ok = len(job_result.workflow_results) == 4
            print(f"\n  Job result workflow_results count: {len(job_result.workflow_results)}/4")
            for workflow_id, result in job_result.workflow_results.items():
                # Extract workflow name from workflow_id
                print(f"    - {workflow_id}: {result.status if hasattr(result, 'status') else result}")
        else:
            print("\n  Job result: Not available")

        print(f"  Job workflow_results verification: {'PASS' if job_workflow_results_ok else 'FAIL'}")

        # ==============================================================
        # Final Result
        # ==============================================================
        print()
        print("=" * 70)
        all_passed = (
            initial_state_ok and
            all_complete and
            workflow_results_ok and
            progress_updates_ok and
            test_workflow_progress_ok and
            job_workflow_results_ok
        )

        if all_passed:
            print("TEST RESULT: PASSED")
        else:
            print("TEST RESULT: FAILED")

        print()
        print("  Test Summary:")
        print(f"    - Initial state (test workflows running): {'PASS' if initial_state_ok else 'FAIL'}")
        print(f"    - All workflows completed: {'PASS' if all_complete else 'FAIL'}")
        print(f"    - Workflow results pushed to client (4/4): {'PASS' if workflow_results_ok else 'FAIL'}")
        print(f"    - Progress updates received (>0): {'PASS' if progress_updates_ok else 'FAIL'}")
        print(f"    - Test workflow progress stats (both > 0): {'PASS' if test_workflow_progress_ok else 'FAIL'}")
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

        # Stop DC-EAST workers
        for i, worker in enumerate(dc_east_workers):
            try:
                await worker.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {DC_EAST_WORKER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  {DC_EAST_WORKER_CONFIGS[i]['name']} stop failed: {e}")

        # Stop DC-WEST workers
        for i, worker in enumerate(dc_west_workers):
            try:
                await worker.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {DC_WEST_WORKER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  {DC_WEST_WORKER_CONFIGS[i]['name']} stop failed: {e}")

        # Stop DC-EAST managers
        for i, manager in enumerate(dc_east_managers):
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {DC_EAST_MANAGER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  {DC_EAST_MANAGER_CONFIGS[i]['name']} stop failed: {e}")

        # Stop DC-WEST managers
        for i, manager in enumerate(dc_west_managers):
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {DC_WEST_MANAGER_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  {DC_WEST_MANAGER_CONFIGS[i]['name']} stop failed: {e}")

        # Stop gates
        for i, gate in enumerate(gates):
            try:
                await gate.stop(drain_timeout=0.5, broadcast_leave=False)
                print(f"  {GATE_CONFIGS[i]['name']} stopped")
            except Exception as e:
                print(f"  {GATE_CONFIGS[i]['name']} stop failed: {e}")

        print()
        print("Test complete.")
        print("=" * 70)


def main():
    print("=" * 70)
    print("GATE CROSS-DATACENTER DISPATCH TEST")
    print("=" * 70)
    print()
    print("This test validates:")
    print("  1. Gate cluster (3 gates) accepts job submission from client")
    print("  2. Gates dispatch to managers in two datacenters")
    print("  3. Each DC has 3 managers (quorum) and 4 workers (2 cores each)")
    print("  4. TestWorkflow and TestWorkflowTwo run concurrently")
    print("  5. Dependent workflows wait for dependencies to complete")
    print("  6. Gate aggregates windowed stats from both DCs")
    print("  7. Workflow results are pushed to client")
    print("  8. Job's workflow_results dict is populated")
    print()
    print("Workflow dependencies:")
    print("  - TestWorkflow: no dependencies")
    print("  - TestWorkflowTwo: no dependencies")
    print("  - NonTestWorkflow: depends on TestWorkflowTwo")
    print("  - NonTestWorkflowTwo: depends on TestWorkflow AND TestWorkflowTwo")
    print()
    print(f"Configuration:")
    print(f"  - 3 Gates (quorum cluster)")
    print(f"  - 2 Datacenters: {DC_EAST}, {DC_WEST}")
    print(f"  - 3 managers per DC (6 total)")
    print(f"  - 4 workers per DC with 2 cores each (8 cores per DC, 16 total)")
    print()

    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
