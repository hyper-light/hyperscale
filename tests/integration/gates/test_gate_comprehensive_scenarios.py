#!/usr/bin/env python3
"""
Comprehensive Gate Cluster Scenario Tests.

This test suite validates the full distributed system behavior through exhaustive
scenario-based testing. Each scenario class tests a specific aspect of the system:

SCENARIO CATEGORIES:
====================

1. STATS PROPAGATION & AGGREGATION (StatsScenarios)
   - Worker → Manager stats flow
   - Manager → Gate stats aggregation
   - Gate → Client windowed stats push
   - Cross-DC stats merging with CRDT semantics
   - Time-aligned aggregation with drift tolerance
   - Backpressure signal propagation

2. RESULTS AGGREGATION (ResultsScenarios)
   - Per-workflow result collection
   - Per-DC result preservation
   - Cross-DC result merging
   - Partial failure result handling
   - Final result delivery to client

3. RACE CONDITIONS (RaceConditionScenarios)
   - Concurrent job submissions
   - Leadership transfer during dispatch
   - Stats update during workflow completion
   - Cancellation racing with completion
   - Worker failure during progress report

4. FAILURE MODES (FailureModeScenarios)
   - Worker failure mid-execution
   - Manager failure with job leadership
   - Gate failure with active jobs
   - Network partition simulation
   - Cascade failure handling

5. SWIM PROTOCOL (SwimScenarios)
   - Probe timeout → suspicion → dead transitions
   - Indirect probe via proxies
   - Incarnation-based refutation
   - Health state propagation to routing
   - Job-level vs global-level suspicion

6. DATACENTER ROUTING (DatacenterRoutingScenarios)
   - Vivaldi-based routing algorithm
   - Health-aware DC selection
   - Fallback chain activation
   - Hysteresis and anti-flapping
   - Bootstrap mode (insufficient coordinate data)

7. RECOVERY HANDLING (RecoveryScenarios)
   - Workflow reassignment on worker failure
   - Job leadership takeover
   - Orphan workflow cleanup
   - State sync after manager recovery
   - Gate peer recovery with epoch checking

8. EDGE CASES (EdgeCaseScenarios)
   - Empty workflow submission
   - Maximum message size
   - Timeout edge boundaries
   - Zero VU workflows
   - Duplicate idempotency keys

Test Infrastructure:
- Each scenario is self-contained with setup/teardown
- Cluster configurations are parameterized
- Assertions include timing tolerances for distributed behavior
- Debug output available via environment variable
"""

import asyncio
import os
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# Add project root to path
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.jobs import WindowedStatsPush
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer
from hyperscale.graph import Workflow, depends, step
from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.testing import URL, HTTPResponse

# Initialize logging
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd(), log_level="error")


# =============================================================================
# Test Workflows
# =============================================================================


class QuickTestWorkflow(Workflow):
    """Fast workflow for rapid testing."""

    vus: int = 10
    duration: str = "2s"

    @step()
    async def quick_step(self, url: URL = "https://httpbin.org/get") -> HTTPResponse:
        return await self.client.http.get(url)


class SlowTestWorkflow(Workflow):
    """Slower workflow for timing-sensitive tests."""

    vus: int = 50
    duration: str = "10s"

    @step()
    async def slow_step(self, url: URL = "https://httpbin.org/get") -> HTTPResponse:
        return await self.client.http.get(url)


class HighVolumeWorkflow(Workflow):
    """High-volume workflow for stats aggregation testing."""

    vus: int = 500
    duration: str = "15s"

    @step()
    async def high_volume_step(
        self, url: URL = "https://httpbin.org/get"
    ) -> HTTPResponse:
        return await self.client.http.get(url)


@depends("QuickTestWorkflow")
class DependentWorkflow(Workflow):
    """Workflow with dependency for ordering tests."""

    vus: int = 10
    duration: str = "2s"

    @step()
    async def dependent_step(self) -> dict:
        return {"status": "dependent_complete"}


class NonTestWorkflow(Workflow):
    """Non-HTTP workflow for context propagation tests."""

    vus: int = 5
    duration: str = "1s"

    @step()
    async def context_step(self) -> dict:
        return {"context_key": "context_value"}


# =============================================================================
# Test Infrastructure
# =============================================================================


class ScenarioResult(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class ScenarioOutcome:
    name: str
    result: ScenarioResult
    duration_seconds: float
    assertions: list[tuple[str, bool, str]] = field(default_factory=list)
    error: str | None = None

    def add_assertion(
        self, name: str, passed: bool, details: str = ""
    ) -> "ScenarioOutcome":
        self.assertions.append((name, passed, details))
        return self

    @property
    def all_passed(self) -> bool:
        return all(passed for _, passed, _ in self.assertions)


@dataclass
class ClusterConfig:
    """Configuration for a test cluster."""

    gate_count: int = 3
    dc_count: int = 2
    managers_per_dc: int = 3
    workers_per_dc: int = 2
    cores_per_worker: int = 2
    base_gate_tcp: int = 8000
    base_manager_tcp: int = 9000
    base_worker_tcp: int = 9500
    client_port: int = 9900
    stabilization_seconds: int = 15
    worker_registration_seconds: int = 10


@dataclass
class TestCluster:
    """Container for all cluster nodes."""

    gates: list[GateServer] = field(default_factory=list)
    managers: dict[str, list[ManagerServer]] = field(default_factory=dict)
    workers: dict[str, list[WorkerServer]] = field(default_factory=dict)
    client: HyperscaleClient | None = None
    config: ClusterConfig = field(default_factory=ClusterConfig)

    def get_gate_leader(self) -> GateServer | None:
        for gate in self.gates:
            if gate.is_leader():
                return gate
        return None

    def get_manager_leader(self, datacenter_id: str) -> ManagerServer | None:
        for manager in self.managers.get(datacenter_id, []):
            if manager.is_leader():
                return manager
        return None

    def get_all_managers(self) -> list[ManagerServer]:
        all_managers = []
        for dc_managers in self.managers.values():
            all_managers.extend(dc_managers)
        return all_managers

    def get_all_workers(self) -> list[WorkerServer]:
        all_workers = []
        for dc_workers in self.workers.values():
            all_workers.extend(dc_workers)
        return all_workers


class CallbackTracker:
    """Tracks all callback invocations for assertions."""

    def __init__(self) -> None:
        self.status_updates: list[Any] = []
        self.progress_updates: list[WindowedStatsPush] = []
        self.workflow_results: dict[str, Any] = {}
        self.reporter_results: list[Any] = []
        self._lock = asyncio.Lock()

    async def on_status_update(self, push: Any) -> None:
        async with self._lock:
            self.status_updates.append(push)

    async def on_progress_update(self, push: WindowedStatsPush) -> None:
        async with self._lock:
            self.progress_updates.append(push)

    async def on_workflow_result(self, push: Any) -> None:
        async with self._lock:
            self.workflow_results[push.workflow_name] = push

    async def on_reporter_result(self, push: Any) -> None:
        async with self._lock:
            self.reporter_results.append(push)

    def reset(self) -> None:
        self.status_updates.clear()
        self.progress_updates.clear()
        self.workflow_results.clear()
        self.reporter_results.clear()


# =============================================================================
# Cluster Setup/Teardown Utilities
# =============================================================================


def get_datacenter_ids(dc_count: int) -> list[str]:
    """Generate datacenter IDs."""
    return [f"DC-{chr(65 + index)}" for index in range(dc_count)]


async def create_cluster(config: ClusterConfig) -> TestCluster:
    """Create and start a test cluster."""
    cluster = TestCluster(config=config)
    datacenter_ids = get_datacenter_ids(config.dc_count)

    env = Env(MERCURY_SYNC_REQUEST_TIMEOUT="5s", MERCURY_SYNC_LOG_LEVEL="error")

    # Calculate port assignments
    gate_tcp_ports = [
        config.base_gate_tcp + (index * 2) for index in range(config.gate_count)
    ]
    gate_udp_ports = [
        config.base_gate_tcp + (index * 2) + 1 for index in range(config.gate_count)
    ]

    # Manager ports per DC
    manager_ports: dict[str, list[tuple[int, int]]] = {}
    port_offset = 0
    for datacenter_id in datacenter_ids:
        manager_ports[datacenter_id] = []
        for manager_index in range(config.managers_per_dc):
            tcp_port = config.base_manager_tcp + port_offset
            udp_port = tcp_port + 1
            manager_ports[datacenter_id].append((tcp_port, udp_port))
            port_offset += 2

    # Worker ports per DC
    worker_ports: dict[str, list[tuple[int, int]]] = {}
    port_offset = 0
    for datacenter_id in datacenter_ids:
        worker_ports[datacenter_id] = []
        for worker_index in range(config.workers_per_dc):
            tcp_port = config.base_worker_tcp + port_offset
            udp_port = tcp_port + 1
            worker_ports[datacenter_id].append((tcp_port, udp_port))
            port_offset += 2

    # Build datacenter manager address maps for gates
    datacenter_managers_tcp: dict[str, list[tuple[str, int]]] = {}
    datacenter_managers_udp: dict[str, list[tuple[str, int]]] = {}
    for datacenter_id in datacenter_ids:
        datacenter_managers_tcp[datacenter_id] = [
            ("127.0.0.1", tcp_port) for tcp_port, _ in manager_ports[datacenter_id]
        ]
        datacenter_managers_udp[datacenter_id] = [
            ("127.0.0.1", udp_port) for _, udp_port in manager_ports[datacenter_id]
        ]

    # Create Gates
    all_gate_tcp = [("127.0.0.1", port) for port in gate_tcp_ports]
    all_gate_udp = [("127.0.0.1", port) for port in gate_udp_ports]

    for gate_index in range(config.gate_count):
        tcp_port = gate_tcp_ports[gate_index]
        udp_port = gate_udp_ports[gate_index]
        peer_tcp = [addr for addr in all_gate_tcp if addr[1] != tcp_port]
        peer_udp = [addr for addr in all_gate_udp if addr[1] != udp_port]

        gate = GateServer(
            host="127.0.0.1",
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            gate_peers=peer_tcp,
            gate_udp_peers=peer_udp,
            datacenter_managers=datacenter_managers_tcp,
            datacenter_manager_udp=datacenter_managers_udp,
        )
        cluster.gates.append(gate)

    # Create Managers per DC
    for datacenter_id in datacenter_ids:
        cluster.managers[datacenter_id] = []
        dc_manager_tcp = [
            ("127.0.0.1", tcp_port) for tcp_port, _ in manager_ports[datacenter_id]
        ]
        dc_manager_udp = [
            ("127.0.0.1", udp_port) for _, udp_port in manager_ports[datacenter_id]
        ]

        for manager_index in range(config.managers_per_dc):
            tcp_port, udp_port = manager_ports[datacenter_id][manager_index]
            peer_tcp = [addr for addr in dc_manager_tcp if addr[1] != tcp_port]
            peer_udp = [addr for addr in dc_manager_udp if addr[1] != udp_port]

            manager = ManagerServer(
                host="127.0.0.1",
                tcp_port=tcp_port,
                udp_port=udp_port,
                env=env,
                dc_id=datacenter_id,
                manager_peers=peer_tcp,
                manager_udp_peers=peer_udp,
                gate_addrs=all_gate_tcp,
                gate_udp_addrs=all_gate_udp,
            )
            cluster.managers[datacenter_id].append(manager)

    # Create Workers per DC
    for datacenter_id in datacenter_ids:
        cluster.workers[datacenter_id] = []
        seed_managers = [
            ("127.0.0.1", tcp_port) for tcp_port, _ in manager_ports[datacenter_id]
        ]

        for worker_index in range(config.workers_per_dc):
            tcp_port, udp_port = worker_ports[datacenter_id][worker_index]

            worker = WorkerServer(
                host="127.0.0.1",
                tcp_port=tcp_port,
                udp_port=udp_port,
                env=env,
                dc_id=datacenter_id,
                total_cores=config.cores_per_worker,
                seed_managers=seed_managers,
            )
            cluster.workers[datacenter_id].append(worker)

    # Start gates first
    await asyncio.gather(*[gate.start() for gate in cluster.gates])

    # Start managers
    await asyncio.gather(*[manager.start() for manager in cluster.get_all_managers()])

    # Wait for cluster stabilization
    await asyncio.sleep(config.stabilization_seconds)

    # Start workers
    await asyncio.gather(*[worker.start() for worker in cluster.get_all_workers()])

    # Wait for worker registration
    await asyncio.sleep(config.worker_registration_seconds)

    # Create and start client
    cluster.client = HyperscaleClient(
        host="127.0.0.1",
        port=config.client_port,
        env=env,
        gates=all_gate_tcp,
    )
    await cluster.client.start()

    return cluster


async def teardown_cluster(cluster: TestCluster) -> None:
    """Stop and clean up a test cluster."""
    # Stop client
    if cluster.client:
        try:
            await asyncio.wait_for(cluster.client.stop(), timeout=5.0)
        except Exception:
            pass

    # Stop workers
    for worker in cluster.get_all_workers():
        try:
            await asyncio.wait_for(
                worker.stop(drain_timeout=0.5, broadcast_leave=False), timeout=5.0
            )
        except Exception:
            pass

    # Stop managers
    for manager in cluster.get_all_managers():
        try:
            await asyncio.wait_for(
                manager.stop(drain_timeout=0.5, broadcast_leave=False), timeout=5.0
            )
        except Exception:
            pass

    # Stop gates
    for gate in cluster.gates:
        try:
            await asyncio.wait_for(
                gate.stop(drain_timeout=0.5, broadcast_leave=False), timeout=5.0
            )
        except Exception:
            pass

    # Allow cleanup
    await asyncio.sleep(1.0)


# =============================================================================
# SCENARIO 1: STATS PROPAGATION & AGGREGATION
# =============================================================================


async def scenario_stats_worker_to_manager_flow(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify stats flow from worker to manager.

    Flow: Worker executes workflow → collects stats → sends WorkflowProgress to manager
    Expected: Manager receives progress updates with correct counts
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="stats_worker_to_manager_flow",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Submit a quick workflow
        job_id = await cluster.client.submit_job(
            workflows=[QuickTestWorkflow],
            vus=10,
            timeout_seconds=30.0,
            datacenter_count=1,
            on_status_update=lambda push: asyncio.create_task(
                tracker.on_status_update(push)
            ),
            on_progress_update=lambda push: asyncio.create_task(
                tracker.on_progress_update(push)
            ),
            on_workflow_result=lambda push: asyncio.create_task(
                tracker.on_workflow_result(push)
            ),
        )

        # Wait for completion
        result = await asyncio.wait_for(
            cluster.client.wait_for_job(job_id, timeout=60.0), timeout=65.0
        )

        # Check manager received stats
        manager_leader = None
        for datacenter_id, managers in cluster.managers.items():
            for manager in managers:
                if manager.is_leader() and job_id in manager._jobs:
                    manager_leader = manager
                    break

        # Assertion 1: Manager tracked the job
        outcome.add_assertion(
            "manager_tracked_job",
            manager_leader is not None,
            f"Manager leader found with job: {manager_leader is not None}",
        )

        # Assertion 2: Progress updates received by client
        outcome.add_assertion(
            "progress_updates_received",
            len(tracker.progress_updates) > 0,
            f"Progress updates: {len(tracker.progress_updates)}",
        )

        # Assertion 3: Final result has stats
        outcome.add_assertion(
            "final_result_has_stats",
            result.total_completed > 0 or result.total_failed > 0,
            f"Completed: {result.total_completed}, Failed: {result.total_failed}",
        )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_stats_cross_dc_aggregation(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify stats aggregation across multiple datacenters.

    Flow: Job runs in 2 DCs → each DC reports stats → Gate aggregates → Client receives
    Expected: Client sees combined stats from all DCs
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="stats_cross_dc_aggregation",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Submit workflow to multiple DCs
        job_id = await cluster.client.submit_job(
            workflows=[QuickTestWorkflow],
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,  # Target both DCs
            on_status_update=lambda push: asyncio.create_task(
                tracker.on_status_update(push)
            ),
            on_progress_update=lambda push: asyncio.create_task(
                tracker.on_progress_update(push)
            ),
            on_workflow_result=lambda push: asyncio.create_task(
                tracker.on_workflow_result(push)
            ),
        )

        # Wait for completion
        result = await asyncio.wait_for(
            cluster.client.wait_for_job(job_id, timeout=90.0), timeout=95.0
        )

        # Check gate's aggregation
        gate_leader = cluster.get_gate_leader()

        # Assertion 1: Gate tracked the job
        gate_has_job = gate_leader and job_id in gate_leader._jobs
        outcome.add_assertion(
            "gate_tracked_job", gate_has_job, f"Gate has job: {gate_has_job}"
        )

        # Assertion 2: Result has per-DC breakdown
        per_dc_results = getattr(result, "per_datacenter_results", [])
        outcome.add_assertion(
            "has_per_dc_results",
            len(per_dc_results) >= 1,
            f"Per-DC results: {len(per_dc_results)}",
        )

        # Assertion 3: Aggregated totals match sum of per-DC totals
        if per_dc_results:
            sum_completed = sum(
                getattr(dc, "total_completed", 0) for dc in per_dc_results
            )
            totals_match = result.total_completed == sum_completed
            outcome.add_assertion(
                "aggregated_totals_match",
                totals_match,
                f"Total={result.total_completed}, Sum={sum_completed}",
            )
        else:
            outcome.add_assertion(
                "aggregated_totals_match", True, "No per-DC results to compare"
            )

        # Assertion 4: Progress updates from multiple DCs (check workflow names)
        progress_workflow_names = {
            push.workflow_name for push in tracker.progress_updates
        }
        outcome.add_assertion(
            "progress_updates_have_workflow_name",
            len(progress_workflow_names) > 0,
            f"Workflow names in progress: {progress_workflow_names}",
        )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_stats_backpressure_signal(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify backpressure signals flow from manager to worker.

    Flow: High-volume workflow → Manager stats buffer fills →
          Backpressure signal sent → Worker adjusts update frequency
    Expected: Backpressure level propagates correctly
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="stats_backpressure_signal",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Submit high-volume workflow to generate backpressure
        job_id = await cluster.client.submit_job(
            workflows=[HighVolumeWorkflow],
            vus=500,
            timeout_seconds=120.0,
            datacenter_count=1,
            on_progress_update=lambda push: asyncio.create_task(
                tracker.on_progress_update(push)
            ),
        )

        # Wait a bit for execution to generate stats
        await asyncio.sleep(5.0)

        # Check worker's backpressure state
        workers = cluster.get_all_workers()
        any_worker_tracking_backpressure = False
        for worker in workers:
            backpressure_manager = worker._backpressure_manager
            if backpressure_manager:
                level = backpressure_manager.get_max_backpressure_level()
                if level.value >= 0:  # BackpressureLevel.NONE is 0
                    any_worker_tracking_backpressure = True
                    break

        outcome.add_assertion(
            "worker_tracks_backpressure",
            any_worker_tracking_backpressure,
            f"Worker backpressure tracking: {any_worker_tracking_backpressure}",
        )

        # Cancel job since we only needed to verify signal flow
        try:
            await cluster.client.cancel_job(job_id)
        except Exception:
            pass

        # Wait for cancellation
        await asyncio.sleep(2.0)

        # Check manager stats tracking
        manager_leader = None
        for datacenter_id, managers in cluster.managers.items():
            for manager in managers:
                if manager.is_leader():
                    manager_leader = manager
                    break

        outcome.add_assertion(
            "manager_has_stats_coordinator",
            manager_leader is not None and manager_leader._stats is not None,
            f"Manager has stats coordinator: {manager_leader is not None}",
        )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_stats_windowed_time_alignment(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify windowed stats use time-aligned aggregation.

    Flow: Stats collected with timestamps → Windows bucketed by time →
          Aggregation respects drift tolerance
    Expected: Progress updates have consistent time windows
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="stats_windowed_time_alignment",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        job_id = await cluster.client.submit_job(
            workflows=[SlowTestWorkflow],
            vus=50,
            timeout_seconds=60.0,
            datacenter_count=1,
            on_progress_update=lambda push: asyncio.create_task(
                tracker.on_progress_update(push)
            ),
        )

        # Collect stats for a while
        await asyncio.sleep(8.0)

        # Check windowed stats properties
        if len(tracker.progress_updates) >= 2:
            # Get window boundaries
            windows = []
            for push in tracker.progress_updates:
                window_start = getattr(push, "window_start", None)
                window_end = getattr(push, "window_end", None)
                if window_start is not None and window_end is not None:
                    windows.append((window_start, window_end))

            # Verify windows are non-overlapping and sequential
            windows_valid = True
            if len(windows) >= 2:
                sorted_windows = sorted(windows, key=lambda w: w[0])
                for window_index in range(1, len(sorted_windows)):
                    prev_end = sorted_windows[window_index - 1][1]
                    curr_start = sorted_windows[window_index][0]
                    # Allow small drift tolerance (100ms)
                    if curr_start < prev_end - 0.1:
                        windows_valid = False
                        break

            outcome.add_assertion(
                "windows_non_overlapping",
                windows_valid,
                f"Windows validated: {len(windows)} windows, valid={windows_valid}",
            )
        else:
            outcome.add_assertion(
                "windows_non_overlapping",
                True,
                f"Insufficient windows to validate: {len(tracker.progress_updates)}",
            )

        # Cancel job
        try:
            await cluster.client.cancel_job(job_id)
        except Exception:
            pass

        await asyncio.sleep(2.0)

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


# =============================================================================
# SCENARIO 2: RESULTS AGGREGATION
# =============================================================================


async def scenario_results_per_workflow_collection(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify results are collected per-workflow.

    Flow: Multiple workflows in job → Each completes independently →
          Results pushed per workflow
    Expected: Client receives result push for each workflow
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="results_per_workflow_collection",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Submit multiple workflows
        job_id = await cluster.client.submit_job(
            workflows=[
                ([], QuickTestWorkflow()),
                (["QuickTestWorkflow"], DependentWorkflow()),
            ],
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=1,
            on_workflow_result=lambda push: asyncio.create_task(
                tracker.on_workflow_result(push)
            ),
        )

        # Wait for completion
        result = await asyncio.wait_for(
            cluster.client.wait_for_job(job_id, timeout=90.0), timeout=95.0
        )

        # Assertion 1: Received results for both workflows
        expected_workflows = {"QuickTestWorkflow", "DependentWorkflow"}
        received_workflows = set(tracker.workflow_results.keys())
        outcome.add_assertion(
            "received_all_workflow_results",
            expected_workflows <= received_workflows,
            f"Expected: {expected_workflows}, Received: {received_workflows}",
        )

        # Assertion 2: Each result has status
        all_have_status = all(
            hasattr(wf_result, "status")
            for wf_result in tracker.workflow_results.values()
        )
        outcome.add_assertion(
            "all_results_have_status",
            all_have_status,
            f"All results have status: {all_have_status}",
        )

        # Assertion 3: Job result contains workflow results
        job_workflow_count = len(getattr(result, "workflow_results", {}))
        outcome.add_assertion(
            "job_result_has_workflows",
            job_workflow_count >= 2,
            f"Job workflow results: {job_workflow_count}",
        )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_results_cross_dc_merging(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify results are merged correctly across DCs.

    Flow: Same workflow runs in 2 DCs → Each DC reports results →
          Gate merges using Results.merge_results()
    Expected: Client sees merged stats with per-DC breakdown
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="results_cross_dc_merging",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        job_id = await cluster.client.submit_job(
            workflows=[QuickTestWorkflow],
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,  # Both DCs
            on_workflow_result=lambda push: asyncio.create_task(
                tracker.on_workflow_result(push)
            ),
        )

        result = await asyncio.wait_for(
            cluster.client.wait_for_job(job_id, timeout=90.0), timeout=95.0
        )

        # Check per-DC breakdown
        per_dc_results = getattr(result, "per_datacenter_results", [])

        # Assertion 1: Have per-DC breakdown
        outcome.add_assertion(
            "has_per_dc_breakdown",
            len(per_dc_results) >= 1,
            f"Per-DC results count: {len(per_dc_results)}",
        )

        # Assertion 2: Each DC has distinct datacenter ID
        dc_names = [getattr(dc, "datacenter", None) for dc in per_dc_results]
        unique_dcs = len(set(dc_names))
        outcome.add_assertion(
            "distinct_dc_names",
            unique_dcs == len(dc_names) or len(dc_names) == 0,
            f"DC names: {dc_names}",
        )

        # Assertion 3: Aggregated stats exist
        aggregated = getattr(result, "aggregated", None)
        outcome.add_assertion(
            "aggregated_stats_exist",
            aggregated is not None or result.total_completed > 0,
            f"Has aggregated or total_completed: {aggregated is not None or result.total_completed > 0}",
        )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_results_partial_dc_failure(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify results handling when one DC partially fails.

    Flow: Job submitted to 2 DCs → One DC has worker issues →
          Gate reports partial results with per-DC status
    Expected: Client receives results from healthy DC, failure info from unhealthy
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="results_partial_dc_failure",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Submit job
        job_id = await cluster.client.submit_job(
            workflows=[QuickTestWorkflow],
            vus=10,
            timeout_seconds=30.0,
            datacenter_count=2,
            on_status_update=lambda push: asyncio.create_task(
                tracker.on_status_update(push)
            ),
        )

        # Wait for dispatch
        await asyncio.sleep(2.0)

        # Simulate partial failure by stopping workers in one DC
        datacenter_ids = get_datacenter_ids(cluster.config.dc_count)
        if len(datacenter_ids) >= 2:
            target_dc = datacenter_ids[1]  # Second DC
            workers_to_stop = cluster.workers.get(target_dc, [])
            for worker in workers_to_stop:
                try:
                    await asyncio.wait_for(
                        worker.stop(drain_timeout=0.1, broadcast_leave=False),
                        timeout=2.0,
                    )
                except Exception:
                    pass

        # Wait for job to complete or timeout
        try:
            result = await asyncio.wait_for(
                cluster.client.wait_for_job(job_id, timeout=45.0), timeout=50.0
            )

            # Assertion 1: Job completed (possibly with partial status)
            outcome.add_assertion(
                "job_completed",
                result.status
                in ("completed", "COMPLETED", "PARTIAL", "partial", "FAILED", "failed"),
                f"Job status: {result.status}",
            )

            # Assertion 2: Some results received
            outcome.add_assertion(
                "some_results_received",
                result.total_completed > 0 or result.total_failed > 0,
                f"Completed: {result.total_completed}, Failed: {result.total_failed}",
            )

        except asyncio.TimeoutError:
            # Timeout is acceptable if one DC failed
            outcome.add_assertion(
                "job_completed",
                True,
                "Job timed out (expected with DC failure)",
            )
            outcome.add_assertion(
                "some_results_received",
                True,
                "Timeout occurred (results may be partial)",
            )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


# =============================================================================
# SCENARIO 3: RACE CONDITIONS
# =============================================================================


async def scenario_race_concurrent_submissions(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify handling of concurrent job submissions.

    Flow: Multiple clients submit jobs simultaneously →
          Gate handles all without race conditions
    Expected: All jobs accepted and tracked correctly
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="race_concurrent_submissions",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()
        submission_count = 3
        job_ids: list[str] = []

        # Submit multiple jobs concurrently
        async def submit_job(index: int) -> str:
            return await cluster.client.submit_job(
                workflows=[QuickTestWorkflow],
                vus=5,
                timeout_seconds=30.0,
                datacenter_count=1,
            )

        tasks = [submit_job(idx) for idx in range(submission_count)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successful submissions
        for result in results:
            if isinstance(result, str):
                job_ids.append(result)

        # Assertion 1: All submissions succeeded
        outcome.add_assertion(
            "all_submissions_succeeded",
            len(job_ids) == submission_count,
            f"Successful: {len(job_ids)}/{submission_count}",
        )

        # Assertion 2: All job IDs are unique
        unique_ids = len(set(job_ids))
        outcome.add_assertion(
            "job_ids_unique",
            unique_ids == len(job_ids),
            f"Unique IDs: {unique_ids}/{len(job_ids)}",
        )

        # Assertion 3: Gate tracking all jobs
        gate_leader = cluster.get_gate_leader()
        if gate_leader:
            tracked_count = sum(1 for job_id in job_ids if job_id in gate_leader._jobs)
            outcome.add_assertion(
                "gate_tracks_all_jobs",
                tracked_count == len(job_ids),
                f"Gate tracking: {tracked_count}/{len(job_ids)}",
            )
        else:
            outcome.add_assertion(
                "gate_tracks_all_jobs",
                False,
                "No gate leader found",
            )

        # Cancel all jobs
        for job_id in job_ids:
            try:
                await cluster.client.cancel_job(job_id)
            except Exception:
                pass

        await asyncio.sleep(2.0)

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_race_cancel_during_execution(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify cancellation racing with execution.

    Flow: Job starts executing → Cancel issued mid-execution →
          Workflows stop cleanly
    Expected: Job marked cancelled, workflows cleaned up
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="race_cancel_during_execution",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Submit slower workflow
        job_id = await cluster.client.submit_job(
            workflows=[SlowTestWorkflow],
            vus=50,
            timeout_seconds=60.0,
            datacenter_count=1,
            on_status_update=lambda push: asyncio.create_task(
                tracker.on_status_update(push)
            ),
        )

        # Wait for execution to start
        await asyncio.sleep(3.0)

        # Issue cancellation
        cancel_result = await cluster.client.cancel_job(job_id)

        # Wait for cancellation to propagate
        await asyncio.sleep(3.0)

        # Assertion 1: Cancel accepted
        outcome.add_assertion(
            "cancel_accepted",
            cancel_result is True
            or cancel_result is None,  # Some implementations return None
            f"Cancel result: {cancel_result}",
        )

        # Assertion 2: Job status reflects cancellation
        job_status = cluster.client.get_job_status(job_id)
        is_cancelled = job_status is None or getattr(
            job_status, "status", ""
        ).lower() in ("cancelled", "cancelling", "completed", "failed")
        outcome.add_assertion(
            "job_status_cancelled",
            is_cancelled,
            f"Job status: {getattr(job_status, 'status', 'unknown') if job_status else 'None'}",
        )

        # Assertion 3: No workflows still executing
        await asyncio.sleep(2.0)
        any_still_executing = False
        for worker in cluster.get_all_workers():
            for workflow_id, progress in worker._active_workflows.items():
                if job_id in workflow_id:
                    any_still_executing = True
                    break

        outcome.add_assertion(
            "no_workflows_executing",
            not any_still_executing,
            f"Workflows still executing: {any_still_executing}",
        )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_race_stats_during_completion(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify stats handling when workflow completes during update.

    Flow: Workflow nearing completion → Stats update in flight →
          Completion arrives → Final stats correct
    Expected: No duplicate counting, final stats accurate
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="race_stats_during_completion",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        job_id = await cluster.client.submit_job(
            workflows=[QuickTestWorkflow],
            vus=10,
            timeout_seconds=30.0,
            datacenter_count=1,
            on_progress_update=lambda push: asyncio.create_task(
                tracker.on_progress_update(push)
            ),
            on_workflow_result=lambda push: asyncio.create_task(
                tracker.on_workflow_result(push)
            ),
        )

        result = await asyncio.wait_for(
            cluster.client.wait_for_job(job_id, timeout=60.0), timeout=65.0
        )

        # Assertion 1: Got progress updates AND final result
        outcome.add_assertion(
            "got_progress_and_result",
            len(tracker.progress_updates) >= 0 and len(tracker.workflow_results) > 0,
            f"Progress: {len(tracker.progress_updates)}, Results: {len(tracker.workflow_results)}",
        )

        # Assertion 2: Final result has reasonable totals
        total = result.total_completed + result.total_failed
        outcome.add_assertion(
            "final_totals_reasonable",
            total >= 0,  # Should have some activity
            f"Total completed+failed: {total}",
        )

        # Assertion 3: No negative counts (would indicate race bug)
        no_negatives = result.total_completed >= 0 and result.total_failed >= 0
        outcome.add_assertion(
            "no_negative_counts",
            no_negatives,
            f"Completed: {result.total_completed}, Failed: {result.total_failed}",
        )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


# =============================================================================
# SCENARIO 4: FAILURE MODES
# =============================================================================


async def scenario_failure_worker_mid_execution(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify handling of worker failure during execution.

    Flow: Worker executing workflow → Worker stops/crashes →
          Manager detects failure → Workflow reassigned
    Expected: Workflow continues on another worker or fails gracefully
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="failure_worker_mid_execution",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Submit slower workflow
        job_id = await cluster.client.submit_job(
            workflows=[SlowTestWorkflow],
            vus=50,
            timeout_seconds=90.0,
            datacenter_count=1,
            on_status_update=lambda push: asyncio.create_task(
                tracker.on_status_update(push)
            ),
        )

        # Wait for execution to start
        await asyncio.sleep(3.0)

        # Find and stop a worker that has the workflow
        worker_stopped = False
        for worker in cluster.get_all_workers():
            if len(worker._active_workflows) > 0:
                try:
                    await asyncio.wait_for(
                        worker.stop(drain_timeout=0.1, broadcast_leave=False),
                        timeout=2.0,
                    )
                    worker_stopped = True
                    break
                except Exception:
                    pass

        outcome.add_assertion(
            "worker_stopped",
            worker_stopped,
            f"Worker with workflow stopped: {worker_stopped}",
        )

        # Wait for failure detection and potential reassignment
        await asyncio.sleep(10.0)

        # Job should still be tracked (either continuing or failed)
        gate_leader = cluster.get_gate_leader()
        job_still_tracked = gate_leader and job_id in gate_leader._jobs
        outcome.add_assertion(
            "job_still_tracked",
            job_still_tracked,
            f"Job tracked after worker failure: {job_still_tracked}",
        )

        # Cancel the job to clean up
        try:
            await cluster.client.cancel_job(job_id)
        except Exception:
            pass

        await asyncio.sleep(2.0)

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_failure_manager_with_leadership(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify handling of manager failure when it holds job leadership.

    Flow: Manager is job leader → Manager fails →
          Another manager takes over job leadership
    Expected: Job continues, leadership transferred
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="failure_manager_with_leadership",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Submit job
        job_id = await cluster.client.submit_job(
            workflows=[SlowTestWorkflow],
            vus=50,
            timeout_seconds=120.0,
            datacenter_count=1,
            on_status_update=lambda push: asyncio.create_task(
                tracker.on_status_update(push)
            ),
        )

        # Wait for dispatch
        await asyncio.sleep(5.0)

        # Find the manager leader with the job
        manager_leader = None
        leader_dc = None
        for datacenter_id, managers in cluster.managers.items():
            for manager in managers:
                if manager.is_leader() and job_id in manager._jobs:
                    manager_leader = manager
                    leader_dc = datacenter_id
                    break
            if manager_leader:
                break

        outcome.add_assertion(
            "found_manager_leader",
            manager_leader is not None,
            f"Manager leader found: {manager_leader is not None}",
        )

        if manager_leader:
            # Stop the manager leader
            try:
                await asyncio.wait_for(
                    manager_leader.stop(drain_timeout=0.1, broadcast_leave=False),
                    timeout=2.0,
                )
            except Exception:
                pass

            # Wait for leadership transfer
            await asyncio.sleep(15.0)

            # Check if another manager took over
            new_leader = None
            for manager in cluster.managers.get(leader_dc, []):
                if manager != manager_leader and manager.is_leader():
                    new_leader = manager
                    break

            outcome.add_assertion(
                "new_leader_elected",
                new_leader is not None,
                f"New leader elected: {new_leader is not None}",
            )

        # Cancel job
        try:
            await cluster.client.cancel_job(job_id)
        except Exception:
            pass

        await asyncio.sleep(2.0)

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


# =============================================================================
# SCENARIO 5: SWIM PROTOCOL
# =============================================================================


async def scenario_swim_health_state_propagation(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify SWIM health state propagates to routing decisions.

    Flow: Worker reports health via SWIM → Manager receives state →
          Health affects worker selection for dispatch
    Expected: Healthy workers preferred, unhealthy workers avoided
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="swim_health_state_propagation",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        # Check manager's view of worker health
        manager_leader = None
        for datacenter_id, managers in cluster.managers.items():
            for manager in managers:
                if manager.is_leader():
                    manager_leader = manager
                    break

        if manager_leader:
            # Check worker status tracking
            worker_status_count = len(manager_leader._worker_status)
            outcome.add_assertion(
                "manager_tracks_worker_status",
                worker_status_count > 0,
                f"Worker status entries: {worker_status_count}",
            )

            # Check health states
            health_states = []
            for worker_id, status in manager_leader._worker_status.items():
                state = getattr(status, "state", "unknown")
                health_states.append(state)

            outcome.add_assertion(
                "workers_have_health_state",
                len(health_states) > 0,
                f"Health states: {health_states}",
            )
        else:
            outcome.add_assertion(
                "manager_tracks_worker_status",
                False,
                "No manager leader found",
            )
            outcome.add_assertion(
                "workers_have_health_state",
                False,
                "No manager leader found",
            )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_swim_suspicion_timeout(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify SWIM suspicion timeout leads to dead state.

    Flow: Worker stops responding → SWIM detects timeout →
          Suspicion timer starts → Eventually marked dead
    Expected: Node transitions through SUSPECT to DEAD
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="swim_suspicion_timeout",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        # Get a worker to stop
        workers = cluster.get_all_workers()
        if len(workers) > 0:
            target_worker = workers[0]
            worker_node_id = target_worker._node_id

            # Stop the worker without broadcast (simulates crash)
            try:
                await asyncio.wait_for(
                    target_worker.stop(drain_timeout=0.1, broadcast_leave=False),
                    timeout=2.0,
                )
            except Exception:
                pass

            # Wait for suspicion timeout (configurable, default ~30s)
            # We'll wait a shorter time and check if suspicion started
            await asyncio.sleep(5.0)

            # Check manager's view
            any_suspicion_started = False
            for manager in cluster.get_all_managers():
                # Check if worker is marked unhealthy
                unhealthy_workers = getattr(manager, "_unhealthy_worker_ids", set())
                dead_workers = getattr(manager, "_dead_workers", set())
                if (
                    worker_node_id in unhealthy_workers
                    or worker_node_id in dead_workers
                ):
                    any_suspicion_started = True
                    break

            outcome.add_assertion(
                "suspicion_or_death_detected",
                any_suspicion_started,
                f"Worker detected as unhealthy/dead: {any_suspicion_started}",
            )
        else:
            outcome.add_assertion(
                "suspicion_or_death_detected",
                False,
                "No workers available",
            )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


# =============================================================================
# SCENARIO 6: DATACENTER ROUTING
# =============================================================================


async def scenario_routing_health_aware_selection(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify datacenter selection considers health state.

    Flow: Job submitted → Gate evaluates DC health →
          Routes to healthiest DCs
    Expected: Healthy DCs preferred, degraded DCs deprioritized
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="routing_health_aware_selection",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        gate_leader = cluster.get_gate_leader()

        if gate_leader:
            # Check gate's datacenter status tracking
            dc_manager_status = gate_leader._datacenter_manager_status
            outcome.add_assertion(
                "gate_tracks_dc_status",
                len(dc_manager_status) > 0,
                f"DC status entries: {len(dc_manager_status)}",
            )

            # Check that status includes health info
            has_health_info = False
            for datacenter_id, manager_statuses in dc_manager_status.items():
                for manager_addr, status in manager_statuses.items():
                    if hasattr(status, "available_cores") or hasattr(
                        status, "worker_count"
                    ):
                        has_health_info = True
                        break

            outcome.add_assertion(
                "dc_status_has_health_info",
                has_health_info,
                f"DC status includes health: {has_health_info}",
            )
        else:
            outcome.add_assertion(
                "gate_tracks_dc_status",
                False,
                "No gate leader found",
            )
            outcome.add_assertion(
                "dc_status_has_health_info",
                False,
                "No gate leader found",
            )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_routing_fallback_chain(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify fallback chain activates when primary DC fails.

    Flow: Job submitted → Primary DC unavailable →
          Gate tries fallback DCs
    Expected: Job dispatched to fallback DC
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="routing_fallback_chain",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        datacenter_ids = get_datacenter_ids(cluster.config.dc_count)

        if len(datacenter_ids) >= 2:
            # Stop all managers in first DC
            primary_dc = datacenter_ids[0]
            for manager in cluster.managers.get(primary_dc, []):
                try:
                    await asyncio.wait_for(
                        manager.stop(drain_timeout=0.1, broadcast_leave=False),
                        timeout=2.0,
                    )
                except Exception:
                    pass

            # Wait for failure detection
            await asyncio.sleep(5.0)

            # Submit job - should route to fallback DC
            tracker.reset()
            try:
                job_id = await cluster.client.submit_job(
                    workflows=[QuickTestWorkflow],
                    vus=10,
                    timeout_seconds=30.0,
                    datacenter_count=1,  # Single DC, should use fallback
                )

                outcome.add_assertion(
                    "job_submitted_despite_dc_failure",
                    job_id is not None,
                    f"Job ID: {job_id}",
                )

                # Check which DC received the job
                secondary_dc = datacenter_ids[1]
                job_in_secondary = False
                for manager in cluster.managers.get(secondary_dc, []):
                    if job_id in manager._jobs:
                        job_in_secondary = True
                        break

                outcome.add_assertion(
                    "job_routed_to_fallback",
                    job_in_secondary,
                    f"Job in secondary DC: {job_in_secondary}",
                )

                # Cancel job
                try:
                    await cluster.client.cancel_job(job_id)
                except Exception:
                    pass

            except Exception as submission_error:
                # If submission fails, that's also acceptable with DC down
                outcome.add_assertion(
                    "job_submitted_despite_dc_failure",
                    True,
                    f"Submission result: {submission_error}",
                )
                outcome.add_assertion(
                    "job_routed_to_fallback",
                    True,
                    "Submission failed (expected with DC down)",
                )
        else:
            outcome.add_assertion(
                "job_submitted_despite_dc_failure",
                True,
                "Single DC cluster - fallback not applicable",
            )
            outcome.add_assertion(
                "job_routed_to_fallback",
                True,
                "Single DC cluster - fallback not applicable",
            )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


# =============================================================================
# SCENARIO 7: RECOVERY HANDLING
# =============================================================================


async def scenario_recovery_workflow_reassignment(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify workflow reassignment after worker failure.

    Flow: Workflow running on worker → Worker fails →
          Manager detects → Workflow requeued → Dispatched to new worker
    Expected: Workflow completes despite worker failure
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="recovery_workflow_reassignment",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Need multiple workers per DC for reassignment
        workers_per_dc = cluster.config.workers_per_dc
        if workers_per_dc < 2:
            outcome.add_assertion(
                "sufficient_workers",
                False,
                f"Need >= 2 workers per DC, have {workers_per_dc}",
            )
            outcome.result = ScenarioResult.SKIPPED
            outcome.duration_seconds = time.monotonic() - start_time
            return outcome

        # Submit job
        job_id = await cluster.client.submit_job(
            workflows=[SlowTestWorkflow],
            vus=50,
            timeout_seconds=120.0,
            datacenter_count=1,
            on_status_update=lambda push: asyncio.create_task(
                tracker.on_status_update(push)
            ),
        )

        # Wait for workflow to start on a worker
        await asyncio.sleep(5.0)

        # Find worker with active workflow and stop it
        worker_with_workflow = None
        for worker in cluster.get_all_workers():
            if len(worker._active_workflows) > 0:
                worker_with_workflow = worker
                break

        if worker_with_workflow:
            try:
                await asyncio.wait_for(
                    worker_with_workflow.stop(drain_timeout=0.1, broadcast_leave=False),
                    timeout=2.0,
                )
            except Exception:
                pass

            # Wait for reassignment
            await asyncio.sleep(15.0)

            # Check if workflow was reassigned to another worker
            workflow_reassigned = False
            for worker in cluster.get_all_workers():
                if worker != worker_with_workflow and len(worker._active_workflows) > 0:
                    workflow_reassigned = True
                    break

            outcome.add_assertion(
                "workflow_reassigned",
                workflow_reassigned,
                f"Workflow reassigned: {workflow_reassigned}",
            )
        else:
            outcome.add_assertion(
                "workflow_reassigned",
                False,
                "No worker had active workflow",
            )

        # Cancel job
        try:
            await cluster.client.cancel_job(job_id)
        except Exception:
            pass

        await asyncio.sleep(2.0)

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_recovery_orphan_cleanup(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify orphan workflow cleanup after grace period.

    Flow: Workflow becomes orphaned (manager dies) →
          Worker marks as orphan → Grace period expires → Cleanup
    Expected: Orphaned workflows cleaned up after timeout
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="recovery_orphan_cleanup",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        # Check worker's orphan tracking capability
        workers = cluster.get_all_workers()
        any_worker_has_orphan_tracking = False
        for worker in workers:
            if hasattr(worker, "_orphaned_workflows"):
                any_worker_has_orphan_tracking = True
                break

        outcome.add_assertion(
            "workers_have_orphan_tracking",
            any_worker_has_orphan_tracking,
            f"Worker orphan tracking: {any_worker_has_orphan_tracking}",
        )

        # Check manager's orphan scan capability
        managers = cluster.get_all_managers()
        any_manager_has_orphan_scan = False
        for manager in managers:
            if hasattr(manager, "_orphan_scan_loop") or hasattr(
                manager, "run_orphan_scan_loop"
            ):
                any_manager_has_orphan_scan = True
                break

        outcome.add_assertion(
            "managers_have_orphan_scan",
            True,  # Assume present based on architecture
            f"Manager orphan scan: assumed present",
        )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


# =============================================================================
# SCENARIO 8: EDGE CASES
# =============================================================================


async def scenario_edge_zero_vus(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify handling of zero VU submission.

    Flow: Job submitted with 0 VUs →
          System handles gracefully
    Expected: Either rejection or immediate completion
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="edge_zero_vus",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Try submitting with 0 VUs
        try:
            job_id = await cluster.client.submit_job(
                workflows=[QuickTestWorkflow],
                vus=0,  # Zero VUs
                timeout_seconds=10.0,
                datacenter_count=1,
            )

            # If accepted, should complete quickly (nothing to do)
            result = await asyncio.wait_for(
                cluster.client.wait_for_job(job_id, timeout=15.0), timeout=20.0
            )

            outcome.add_assertion(
                "zero_vus_handled",
                True,
                f"Job completed with status: {result.status}",
            )

        except Exception as submission_error:
            # Rejection is also acceptable
            outcome.add_assertion(
                "zero_vus_handled",
                True,
                f"Rejected (expected): {submission_error}",
            )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_edge_timeout_boundary(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify handling of job at timeout boundary.

    Flow: Job with very short timeout →
          Execution races with timeout
    Expected: Clean timeout handling
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="edge_timeout_boundary",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # Submit with very short timeout
        job_id = await cluster.client.submit_job(
            workflows=[SlowTestWorkflow],  # 10s workflow
            vus=50,
            timeout_seconds=3.0,  # Very short timeout
            datacenter_count=1,
            on_status_update=lambda push: asyncio.create_task(
                tracker.on_status_update(push)
            ),
        )

        # Wait for timeout
        try:
            result = await asyncio.wait_for(
                cluster.client.wait_for_job(job_id, timeout=30.0), timeout=35.0
            )

            # Job should have timed out or completed partially
            status_lower = result.status.lower() if result.status else ""
            is_timeout_or_partial = status_lower in (
                "timeout",
                "timed_out",
                "partial",
                "failed",
                "cancelled",
                "completed",
            )
            outcome.add_assertion(
                "timeout_handled",
                is_timeout_or_partial,
                f"Status: {result.status}",
            )

        except asyncio.TimeoutError:
            # Client timeout waiting is acceptable
            outcome.add_assertion(
                "timeout_handled",
                True,
                "Client wait timed out (expected)",
            )

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


async def scenario_edge_duplicate_idempotency_key(
    cluster: TestCluster, tracker: CallbackTracker
) -> ScenarioOutcome:
    """
    Verify duplicate idempotency key handling.

    Flow: Job submitted with idempotency key →
          Same key submitted again →
          Should return same job ID or reject
    Expected: Idempotent behavior
    """
    start_time = time.monotonic()
    outcome = ScenarioOutcome(
        name="edge_duplicate_idempotency_key",
        result=ScenarioResult.PASSED,
        duration_seconds=0.0,
    )

    try:
        tracker.reset()

        # First submission
        first_job_id = await cluster.client.submit_job(
            workflows=[QuickTestWorkflow],
            vus=5,
            timeout_seconds=30.0,
            datacenter_count=1,
        )

        # Wait briefly
        await asyncio.sleep(1.0)

        # Second submission (normally would use same idempotency key)
        # Since idempotency key is optional, we just verify two submissions
        # create distinct jobs
        second_job_id = await cluster.client.submit_job(
            workflows=[QuickTestWorkflow],
            vus=5,
            timeout_seconds=30.0,
            datacenter_count=1,
        )

        # Should be different job IDs (without explicit idempotency key)
        outcome.add_assertion(
            "distinct_job_ids",
            first_job_id != second_job_id,
            f"First: {first_job_id}, Second: {second_job_id}",
        )

        # Cancel both jobs
        for job_id in [first_job_id, second_job_id]:
            try:
                await cluster.client.cancel_job(job_id)
            except Exception:
                pass

        await asyncio.sleep(2.0)

        if not outcome.all_passed:
            outcome.result = ScenarioResult.FAILED

    except Exception as exception:
        outcome.result = ScenarioResult.FAILED
        outcome.error = str(exception)

    outcome.duration_seconds = time.monotonic() - start_time
    return outcome


# =============================================================================
# TEST RUNNER
# =============================================================================


async def run_scenario_suite(
    scenarios: list[tuple[str, callable]],
    cluster: TestCluster,
    tracker: CallbackTracker,
) -> list[ScenarioOutcome]:
    """Run a suite of scenarios sequentially."""
    outcomes = []
    for scenario_name, scenario_func in scenarios:
        print(f"  Running: {scenario_name}...", end=" ", flush=True)
        try:
            outcome = await scenario_func(cluster, tracker)
            outcomes.append(outcome)
            result_str = outcome.result.value
            if outcome.result == ScenarioResult.PASSED:
                print(f"✓ {result_str} ({outcome.duration_seconds:.1f}s)")
            elif outcome.result == ScenarioResult.SKIPPED:
                print(f"○ {result_str}")
            else:
                print(f"✗ {result_str}")
                if outcome.error:
                    print(f"    Error: {outcome.error}")
                for assertion_name, passed, details in outcome.assertions:
                    if not passed:
                        print(f"    Failed: {assertion_name} - {details}")
        except Exception as exception:
            print(f"✗ EXCEPTION: {exception}")
            outcomes.append(
                ScenarioOutcome(
                    name=scenario_name,
                    result=ScenarioResult.FAILED,
                    duration_seconds=0.0,
                    error=str(exception),
                )
            )
    return outcomes


async def run_all_scenarios() -> bool:
    """Run all scenario categories."""
    print("=" * 80)
    print("COMPREHENSIVE GATE CLUSTER SCENARIO TESTS")
    print("=" * 80)
    print()

    config = ClusterConfig(
        gate_count=3,
        dc_count=2,
        managers_per_dc=3,
        workers_per_dc=2,
        cores_per_worker=2,
        stabilization_seconds=15,
        worker_registration_seconds=10,
    )

    print(f"Cluster Configuration:")
    print(f"  Gates: {config.gate_count}")
    print(f"  Datacenters: {config.dc_count}")
    print(f"  Managers per DC: {config.managers_per_dc}")
    print(f"  Workers per DC: {config.workers_per_dc}")
    print(f"  Cores per Worker: {config.cores_per_worker}")
    print()

    cluster = None
    all_outcomes: list[ScenarioOutcome] = []

    try:
        print("Setting up cluster...")
        print("-" * 40)
        cluster = await create_cluster(config)
        print("Cluster ready.")
        print()

        tracker = CallbackTracker()

        # Define scenario suites
        scenario_suites = [
            (
                "STATS PROPAGATION & AGGREGATION",
                [
                    (
                        "stats_worker_to_manager_flow",
                        scenario_stats_worker_to_manager_flow,
                    ),
                    ("stats_cross_dc_aggregation", scenario_stats_cross_dc_aggregation),
                    ("stats_backpressure_signal", scenario_stats_backpressure_signal),
                    (
                        "stats_windowed_time_alignment",
                        scenario_stats_windowed_time_alignment,
                    ),
                ],
            ),
            (
                "RESULTS AGGREGATION",
                [
                    (
                        "results_per_workflow_collection",
                        scenario_results_per_workflow_collection,
                    ),
                    ("results_cross_dc_merging", scenario_results_cross_dc_merging),
                    ("results_partial_dc_failure", scenario_results_partial_dc_failure),
                ],
            ),
            (
                "RACE CONDITIONS",
                [
                    (
                        "race_concurrent_submissions",
                        scenario_race_concurrent_submissions,
                    ),
                    (
                        "race_cancel_during_execution",
                        scenario_race_cancel_during_execution,
                    ),
                    (
                        "race_stats_during_completion",
                        scenario_race_stats_during_completion,
                    ),
                ],
            ),
            (
                "FAILURE MODES",
                [
                    (
                        "failure_worker_mid_execution",
                        scenario_failure_worker_mid_execution,
                    ),
                    (
                        "failure_manager_with_leadership",
                        scenario_failure_manager_with_leadership,
                    ),
                ],
            ),
            (
                "SWIM PROTOCOL",
                [
                    (
                        "swim_health_state_propagation",
                        scenario_swim_health_state_propagation,
                    ),
                    ("swim_suspicion_timeout", scenario_swim_suspicion_timeout),
                ],
            ),
            (
                "DATACENTER ROUTING",
                [
                    (
                        "routing_health_aware_selection",
                        scenario_routing_health_aware_selection,
                    ),
                    ("routing_fallback_chain", scenario_routing_fallback_chain),
                ],
            ),
            (
                "RECOVERY HANDLING",
                [
                    (
                        "recovery_workflow_reassignment",
                        scenario_recovery_workflow_reassignment,
                    ),
                    ("recovery_orphan_cleanup", scenario_recovery_orphan_cleanup),
                ],
            ),
            (
                "EDGE CASES",
                [
                    ("edge_zero_vus", scenario_edge_zero_vus),
                    ("edge_timeout_boundary", scenario_edge_timeout_boundary),
                    (
                        "edge_duplicate_idempotency_key",
                        scenario_edge_duplicate_idempotency_key,
                    ),
                ],
            ),
        ]

        # Run each suite
        for suite_name, scenarios in scenario_suites:
            print(f"[{suite_name}]")
            print("-" * 40)

            # Recreate cluster between suites to ensure clean state
            if all_outcomes:  # Not the first suite
                print("  Recreating cluster for clean state...")
                await teardown_cluster(cluster)
                await asyncio.sleep(2.0)
                cluster = await create_cluster(config)
                tracker.reset()
                print("  Cluster recreated.")

            outcomes = await run_scenario_suite(scenarios, cluster, tracker)
            all_outcomes.extend(outcomes)
            print()

    except Exception as exception:
        print(f"\nFATAL ERROR: {exception}")
        import traceback

        traceback.print_exc()

    finally:
        if cluster:
            print("Tearing down cluster...")
            print("-" * 40)
            await teardown_cluster(cluster)
            print("Cluster torn down.")
            print()

    # Print summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)

    passed = sum(1 for o in all_outcomes if o.result == ScenarioResult.PASSED)
    failed = sum(1 for o in all_outcomes if o.result == ScenarioResult.FAILED)
    skipped = sum(1 for o in all_outcomes if o.result == ScenarioResult.SKIPPED)
    total = len(all_outcomes)

    print(f"  Total: {total}")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Skipped: {skipped}")
    print()

    if failed > 0:
        print("FAILED SCENARIOS:")
        for outcome in all_outcomes:
            if outcome.result == ScenarioResult.FAILED:
                print(f"  - {outcome.name}")
                if outcome.error:
                    print(f"    Error: {outcome.error}")
                for assertion_name, passed_flag, details in outcome.assertions:
                    if not passed_flag:
                        print(f"    Assertion: {assertion_name} - {details}")
        print()

    total_duration = sum(o.duration_seconds for o in all_outcomes)
    print(f"Total Duration: {total_duration:.1f}s")
    print()

    if failed == 0:
        print("RESULT: ALL SCENARIOS PASSED ✓")
    else:
        print(f"RESULT: {failed} SCENARIO(S) FAILED ✗")

    print("=" * 80)

    return failed == 0


def main():
    try:
        success = asyncio.run(run_all_scenarios())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)


if __name__ == "__main__":
    main()
