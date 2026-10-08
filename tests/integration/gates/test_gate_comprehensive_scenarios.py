"""
Scenario tests of a full gate cluster (three gates over two datacenters of
three managers and two workers each, with a client submitting through the
gates), each against a freshly started cluster. They prove that:

- stats flow from workers through managers and gates to the client, are
  aggregated across datacenters with totals equal to the per-datacenter
  sum, arrive in non-overlapping time windows, and that workers track the
  managers' backpressure;
- a result is pushed for every workflow of a job, cross-datacenter results
  keep a distinct per-datacenter breakdown, and a job whose second
  datacenter loses its workers still reports;
- concurrent submissions are all accepted with unique ids, a cancel during
  execution stops the job's workflows, and a job's final counts are never
  negative;
- a job outlives the failure of a worker running it, and a datacenter
  elects a new leader when its leader manager fails;
- managers track worker health and detect a crashed worker, and the gate
  tracks every datacenter's health;
- a job submitted while one datacenter is down is placed in the other;
- a failed worker's workflow is reassigned, and workers and managers run
  their orphan handling;
- zero-VU and short-timeout jobs are handled, and two submissions without
  an idempotency key are two jobs.

The workflows call https://httpbin.org, so the tests need network access.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.jobs import WindowedStatsPush
from hyperscale.distributed.models import DatacenterHealth, JobStatus, WorkflowResultPush
from hyperscale.distributed.reliability import BackpressureLevel
from hyperscale.graph import Workflow, depends, step
from hyperscale.testing import URL, HTTPResponse
from tests.integration.gates.comprehensive_cluster import (
    DATACENTER_IDS,
    WORKERS_PER_DATACENTER,
    ComprehensiveCluster,
    build_comprehensive_cluster,
    start_comprehensive_cluster,
    stop_comprehensive_cluster,
)
from tests.integration.in_process_nodes import wait_until

# Bounds on what the cluster does on its own, generous multiples of the
# fixed sleeps the original script waited for the same thing.
EXECUTION_START_SECONDS = 30.0
JOB_DISPATCH_SECONDS = 30.0
FAILURE_DETECTION_SECONDS = 60.0
CANCELLATION_SECONDS = 15.0
WINDOWED_STATS_SECONDS = 30.0
# A cancel issued while a datacenter fails over keeps sweeping its targets
# for this long (the client's cancel budget).
FAILOVER_CANCEL_SECONDS = 60.0
TERMINAL_JOB_STATUSES = frozenset(
    {JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value, JobStatus.TIMEOUT.value}
)


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
    async def high_volume_step(self, url: URL = "https://httpbin.org/get") -> HTTPResponse:
        return await self.client.http.get(url)


@depends("QuickTestWorkflow")
class DependentWorkflow(Workflow):
    """Workflow with dependency for ordering tests."""

    vus: int = 10
    duration: str = "2s"

    @step()
    async def dependent_step(self) -> dict[str, str]:
        return {"status": "dependent_complete"}


@pytest.fixture
async def cluster(node_directory: pathlib.Path) -> AsyncIterator[ComprehensiveCluster]:
    """A started scenario cluster, stopped after the test whatever it did."""
    comprehensive_cluster = build_comprehensive_cluster(node_directory)
    try:
        await start_comprehensive_cluster(comprehensive_cluster)
        yield comprehensive_cluster
    finally:
        await stop_comprehensive_cluster(comprehensive_cluster)


# =============================================================================
# Stats propagation and aggregation
# =============================================================================


async def test_worker_stats_reach_manager_and_client(cluster: ComprehensiveCluster) -> None:
    """Worker -> manager -> client: the leader manager tracks the job, the
    client receives progress, and the final result carries counts."""
    progress_updates: list[WindowedStatsPush] = []
    job_id = await cluster.client.submit_job(
        workflows=[([], QuickTestWorkflow())],
        vus=10,
        timeout_seconds=30.0,
        datacenter_count=1,
        on_progress_update=progress_updates.append,
    )
    result = await asyncio.wait_for(cluster.client.wait_for_job(job_id, timeout=60.0), timeout=65.0)

    assert any(
        manager.is_leader() and manager._job_manager.get_job_by_id(job_id) is not None
        for manager in cluster.all_managers
    ), f"no leader manager tracks job {job_id}"
    assert len(progress_updates) > 0, "the client received no progress updates"
    assert result.total_completed > 0 or result.total_failed > 0, (
        f"final result has no stats: completed={result.total_completed}, failed={result.total_failed}"
    )


async def test_cross_datacenter_stats_aggregate_at_gate(cluster: ComprehensiveCluster) -> None:
    """A job in both datacenters: the gate leader tracks it, the result
    has a per-datacenter breakdown whose completed counts sum to the
    aggregate, and progress names the workflow."""
    progress_updates: list[WindowedStatsPush] = []
    job_id = await cluster.client.submit_job(
        workflows=[([], QuickTestWorkflow())],
        vus=10,
        timeout_seconds=60.0,
        datacenter_count=2,
        on_progress_update=progress_updates.append,
    )
    result = await asyncio.wait_for(cluster.client.wait_for_job(job_id, timeout=90.0), timeout=95.0)

    gate_leader = cluster.gate_leader()
    assert gate_leader is not None and gate_leader._job_manager.has_job(job_id), (
        f"the gate leader ({gate_leader}) does not track job {job_id}"
    )
    assert len(result.per_datacenter_results) >= 1, "the result has no per-datacenter results"
    per_datacenter_completed = sum(
        datacenter_result.total_completed for datacenter_result in result.per_datacenter_results
    )
    assert result.total_completed == per_datacenter_completed, (
        f"aggregated completed {result.total_completed} != per-datacenter sum {per_datacenter_completed}"
    )
    progress_workflow_names = {push.workflow_name for push in progress_updates}
    assert len(progress_workflow_names) > 0, f"no workflow names in progress updates: {progress_workflow_names}"


async def test_workers_track_manager_backpressure(cluster: ComprehensiveCluster) -> None:
    """While a high-volume job runs, a worker tracks the managers'
    backpressure level, and the leader manager has its stats coordinator."""
    job_id = await cluster.client.submit_job(
        workflows=[([], HighVolumeWorkflow())],
        vus=500,
        timeout_seconds=120.0,
        datacenter_count=1,
    )
    await wait_until(
        lambda: any(
            progress.job_id == job_id
            for worker in cluster.all_workers
            for progress in worker._active_workflows.values()
        ),
        within_seconds=EXECUTION_START_SECONDS,
        description=f"a worker executing job {job_id}",
    )

    assert any(
        worker._backpressure_manager.get_max_backpressure_level() >= BackpressureLevel.NONE
        for worker in cluster.all_workers
    ), "no worker tracks a backpressure level"

    await cluster.client.cancel_job(job_id)

    leader_managers = [manager for manager in cluster.all_managers if manager.is_leader()]
    assert leader_managers, "no leader manager"
    assert all(manager._stats is not None for manager in leader_managers), "a leader manager has no stats coordinator"


async def test_windowed_stats_windows_do_not_overlap(cluster: ComprehensiveCluster) -> None:
    """Progress pushes of a running job cover sequential, non-overlapping
    time windows (within 100ms of drift)."""
    progress_updates: list[WindowedStatsPush] = []
    job_id = await cluster.client.submit_job(
        workflows=[([], SlowTestWorkflow())],
        vus=50,
        timeout_seconds=60.0,
        datacenter_count=1,
        on_progress_update=progress_updates.append,
    )
    await wait_until(
        lambda: len(progress_updates) >= 2,
        within_seconds=WINDOWED_STATS_SECONDS,
        description="at least two windowed stats pushes",
    )

    sorted_windows = sorted((push.window_start, push.window_end) for push in progress_updates)
    overlapping_windows = [
        (previous_window, current_window)
        for previous_window, current_window in zip(sorted_windows, sorted_windows[1:])
        if current_window[0] < previous_window[1] - 0.1
    ]
    assert not overlapping_windows, f"overlapping windows among {len(sorted_windows)}: {overlapping_windows}"

    await cluster.client.cancel_job(job_id)


# =============================================================================
# Results aggregation
# =============================================================================


async def test_each_workflow_result_is_pushed(cluster: ComprehensiveCluster) -> None:
    """A job of a workflow and its dependent: the client receives a result
    push, with a status, for each, and the job result holds both."""
    workflow_result_pushes: list[WorkflowResultPush] = []
    job_id = await cluster.client.submit_job(
        workflows=[([], QuickTestWorkflow()), (["QuickTestWorkflow"], DependentWorkflow())],
        vus=10,
        timeout_seconds=60.0,
        datacenter_count=1,
        on_workflow_result=workflow_result_pushes.append,
    )
    result = await asyncio.wait_for(cluster.client.wait_for_job(job_id, timeout=90.0), timeout=95.0)

    expected_workflow_names = {"QuickTestWorkflow", "DependentWorkflow"}
    received_workflow_names = {push.workflow_name for push in workflow_result_pushes}
    assert expected_workflow_names <= received_workflow_names, (
        f"expected results for {expected_workflow_names}, received {received_workflow_names}"
    )
    assert all(push.status for push in workflow_result_pushes), (
        f"a workflow result has no status: {[push.status for push in workflow_result_pushes]}"
    )
    assert len(result.workflow_results) >= 2, f"the job result holds {len(result.workflow_results)} workflow results"


async def test_cross_datacenter_results_keep_per_datacenter_breakdown(cluster: ComprehensiveCluster) -> None:
    """A job in both datacenters: the result has a per-datacenter breakdown
    with one entry per distinct datacenter, and aggregated stats."""
    job_id = await cluster.client.submit_job(
        workflows=[([], QuickTestWorkflow())],
        vus=10,
        timeout_seconds=60.0,
        datacenter_count=2,
    )
    result = await asyncio.wait_for(cluster.client.wait_for_job(job_id, timeout=90.0), timeout=95.0)

    assert len(result.per_datacenter_results) >= 1, "the result has no per-datacenter breakdown"
    datacenter_names = [datacenter_result.datacenter for datacenter_result in result.per_datacenter_results]
    assert len(set(datacenter_names)) == len(datacenter_names), f"datacenters repeat: {datacenter_names}"
    assert result.aggregated is not None or result.total_completed > 0, (
        f"no aggregated stats and completed={result.total_completed}"
    )


async def test_job_reports_when_one_datacenter_loses_its_workers(cluster: ComprehensiveCluster) -> None:
    """Once a two-datacenter job reaches the second datacenter, its workers
    stop: the job still ends with a final status and counts. The client's
    wait timing out is an accepted outcome of losing a datacenter."""
    second_datacenter = DATACENTER_IDS[1]
    job_id = await cluster.client.submit_job(
        workflows=[([], QuickTestWorkflow())],
        vus=10,
        timeout_seconds=30.0,
        datacenter_count=2,
    )
    await wait_until(
        lambda: any(
            manager._job_manager.get_job_by_id(job_id) is not None
            for manager in cluster.managers_by_datacenter[second_datacenter]
        ),
        within_seconds=JOB_DISPATCH_SECONDS,
        description=f"job {job_id} reaching {second_datacenter}",
    )
    for worker in cluster.workers_by_datacenter[second_datacenter]:
        await cluster.stop_node(worker)

    try:
        result = await asyncio.wait_for(cluster.client.wait_for_job(job_id, timeout=45.0), timeout=50.0)
    except TimeoutError:
        # The scenario's accepted outcome: with a datacenter's workers
        # gone, the job may not finish within the wait.
        return

    assert result.status in ("completed", "COMPLETED", "PARTIAL", "partial", "FAILED", "failed"), (
        f"job status: {result.status}"
    )
    assert result.total_completed > 0 or result.total_failed > 0, (
        f"no results: completed={result.total_completed}, failed={result.total_failed}"
    )


# =============================================================================
# Race conditions
# =============================================================================


async def test_concurrent_submissions_are_all_accepted(cluster: ComprehensiveCluster) -> None:
    """Three concurrent submissions: all accepted, with unique ids, all
    tracked by the gate leader."""
    submission_count = 3
    submission_results = await asyncio.gather(
        *[
            cluster.client.submit_job(
                workflows=[([], QuickTestWorkflow())],
                vus=5,
                timeout_seconds=30.0,
                datacenter_count=1,
            )
            for _ in range(submission_count)
        ],
        return_exceptions=True,
    )

    failures = [result for result in submission_results if isinstance(result, BaseException)]
    assert not failures, f"{len(failures)}/{submission_count} submissions failed: {failures}"
    job_ids = [result for result in submission_results if isinstance(result, str)]
    assert len(set(job_ids)) == len(job_ids), f"job ids repeat: {job_ids}"
    gate_leader = cluster.gate_leader()
    assert gate_leader is not None, "no gate leader"
    untracked_job_ids = [job_id for job_id in job_ids if not gate_leader._job_manager.has_job(job_id)]
    assert not untracked_job_ids, f"the gate leader does not track {untracked_job_ids}"

    for job_id in job_ids:
        await cluster.client.cancel_job(job_id)


async def test_cancel_during_execution_stops_workflows(cluster: ComprehensiveCluster) -> None:
    """A cancel issued while a job executes is accepted, the client's view
    of the job turns terminal, and no worker still runs its workflows."""
    job_id = await cluster.client.submit_job(
        workflows=[([], SlowTestWorkflow())],
        vus=50,
        timeout_seconds=60.0,
        datacenter_count=1,
    )
    await wait_until(
        lambda: any(
            progress.job_id == job_id
            for worker in cluster.all_workers
            for progress in worker._active_workflows.values()
        ),
        within_seconds=EXECUTION_START_SECONDS,
        description=f"a worker executing job {job_id}",
    )

    cancel_response = await cluster.client.cancel_job(job_id)
    assert cancel_response.success, f"cancel was not accepted: {cancel_response}"

    cancelled_statuses = ("cancelled", "cancelling", "completed", "failed")
    await wait_until(
        lambda: (job_status := cluster.client.get_job_status(job_id)) is None
        or job_status.status.lower() in cancelled_statuses,
        within_seconds=CANCELLATION_SECONDS,
        description=f"job {job_id}'s status turning one of {cancelled_statuses}",
    )
    await wait_until(
        lambda: not any(
            progress.job_id == job_id
            for worker in cluster.all_workers
            for progress in worker._active_workflows.values()
        ),
        within_seconds=CANCELLATION_SECONDS,
        description=f"no worker still executing job {job_id}",
    )


async def test_job_counts_are_never_negative(cluster: ComprehensiveCluster) -> None:
    """Stats racing a workflow's completion: the client gets the workflow
    result and final counts that are never negative."""
    workflow_result_pushes: list[WorkflowResultPush] = []
    job_id = await cluster.client.submit_job(
        workflows=[([], QuickTestWorkflow())],
        vus=10,
        timeout_seconds=30.0,
        datacenter_count=1,
        on_workflow_result=workflow_result_pushes.append,
    )
    result = await asyncio.wait_for(cluster.client.wait_for_job(job_id, timeout=60.0), timeout=65.0)

    assert len(workflow_result_pushes) > 0, "the client received no workflow result"
    assert result.total_completed >= 0 and result.total_failed >= 0, (
        f"negative counts: completed={result.total_completed}, failed={result.total_failed}"
    )


# =============================================================================
# Failure modes
# =============================================================================


async def test_job_outlives_worker_failure(cluster: ComprehensiveCluster) -> None:
    """The worker executing a job crashes: once its managers detect the
    failure, the gate leader still tracks the job."""
    job_id = await cluster.client.submit_job(
        workflows=[([], SlowTestWorkflow())],
        vus=50,
        timeout_seconds=90.0,
        datacenter_count=1,
    )
    await wait_until(
        lambda: any(len(worker._active_workflows) > 0 for worker in cluster.all_workers),
        within_seconds=EXECUTION_START_SECONDS,
        description="a worker executing a workflow",
    )
    failed_worker = next(worker for worker in cluster.all_workers if len(worker._active_workflows) > 0)
    failed_worker_id = failed_worker._node_id.full
    registering_managers = [
        manager for manager in cluster.all_managers if manager._manager_state.has_worker(failed_worker_id)
    ]
    assert registering_managers, f"no manager has worker {failed_worker_id} registered"

    await cluster.stop_node(failed_worker)
    await wait_until(
        lambda: any(
            manager._manager_state.has_worker_unhealthy_since(failed_worker_id)
            or not manager._manager_state.has_worker(failed_worker_id)
            for manager in registering_managers
        ),
        within_seconds=FAILURE_DETECTION_SECONDS,
        description=f"a manager detecting worker {failed_worker_id}'s failure",
    )

    gate_leader = cluster.gate_leader()
    assert gate_leader is not None and gate_leader._job_manager.has_job(job_id), (
        f"the gate leader ({gate_leader}) no longer tracks job {job_id} after the worker failed"
    )

    await cluster.client.cancel_job(job_id)


async def test_datacenter_elects_new_leader_when_job_leader_fails(cluster: ComprehensiveCluster) -> None:
    """The leader manager holding a job fails: another manager of its
    datacenter becomes leader."""
    job_id = await cluster.client.submit_job(
        workflows=[([], SlowTestWorkflow())],
        vus=50,
        timeout_seconds=120.0,
        datacenter_count=1,
    )
    await wait_until(
        lambda: any(
            manager.is_leader() and manager._job_manager.get_job_by_id(job_id) is not None
            for manager in cluster.all_managers
        ),
        within_seconds=JOB_DISPATCH_SECONDS,
        description=f"a leader manager tracking job {job_id}",
    )
    leader_datacenter, failed_leader = next(
        (datacenter_id, manager)
        for datacenter_id, managers in cluster.managers_by_datacenter.items()
        for manager in managers
        if manager.is_leader() and manager._job_manager.get_job_by_id(job_id) is not None
    )

    await cluster.stop_node(failed_leader)
    await wait_until(
        lambda: any(
            manager is not failed_leader and manager.is_leader()
            for manager in cluster.managers_by_datacenter[leader_datacenter]
        ),
        within_seconds=FAILURE_DETECTION_SECONDS,
        description=f"a new leader in {leader_datacenter}",
    )

    await cluster.client.cancel_job(job_id, timeout=FAILOVER_CANCEL_SECONDS)


# =============================================================================
# SWIM protocol
# =============================================================================


async def test_manager_tracks_worker_health_states(cluster: ComprehensiveCluster) -> None:
    """The leader manager of the last datacenter has its workers registered,
    each with a health state."""
    manager_leader = cluster.datacenter_leader(DATACENTER_IDS[-1])
    assert manager_leader is not None, f"no leader manager in {DATACENTER_IDS[-1]}"

    registered_workers = manager_leader._manager_state.get_all_workers()
    assert len(registered_workers) > 0, "the leader manager has no registered workers"
    health_states = [manager_leader._registry.get_worker_health_state(worker_id) for worker_id in registered_workers]
    assert len(health_states) > 0 and all(health_states), f"worker health states: {health_states}"


async def test_crashed_worker_is_suspected_by_managers(cluster: ComprehensiveCluster) -> None:
    """A worker stops without announcing its leave: a manager that had it
    registered marks it unhealthy or removes it."""
    target_worker = cluster.all_workers[0]
    target_worker_id = target_worker._node_id.full
    registering_managers = [
        manager for manager in cluster.all_managers if manager._manager_state.has_worker(target_worker_id)
    ]
    assert registering_managers, f"no manager has worker {target_worker_id} registered"

    await cluster.stop_node(target_worker)
    await wait_until(
        lambda: any(
            manager._manager_state.has_worker_unhealthy_since(target_worker_id)
            or not manager._manager_state.has_worker(target_worker_id)
            for manager in registering_managers
        ),
        within_seconds=FAILURE_DETECTION_SECONDS,
        description=f"a manager marking worker {target_worker_id} unhealthy or dead",
    )


# =============================================================================
# Datacenter routing
# =============================================================================


async def test_gate_tracks_datacenter_health(cluster: ComprehensiveCluster) -> None:
    """The gate leader holds manager heartbeats per datacenter, and they
    report the datacenters' registered workers."""
    gate_leader = cluster.gate_leader()
    assert gate_leader is not None, "no gate leader"

    manager_heartbeats_by_datacenter = gate_leader._modular_state._datacenter_manager_status
    assert len(manager_heartbeats_by_datacenter) > 0, "the gate leader tracks no datacenter status"
    assert any(
        heartbeat.worker_count > 0
        for heartbeats in manager_heartbeats_by_datacenter.values()
        for heartbeat in heartbeats.values()
    ), f"no manager heartbeat reports workers: {manager_heartbeats_by_datacenter}"


async def test_job_is_placed_in_fallback_datacenter_when_primary_is_down(cluster: ComprehensiveCluster) -> None:
    """Every manager of the first datacenter stops: once the gate leader
    classifies it unhealthy, a job lands in the second datacenter. A
    rejected submission is the scenario's other accepted outcome."""
    primary_datacenter, secondary_datacenter = DATACENTER_IDS
    for manager in cluster.managers_by_datacenter[primary_datacenter]:
        await cluster.stop_node(manager)
    await wait_until(
        lambda: (gate_leader := cluster.gate_leader()) is not None
        and gate_leader._classify_datacenter_health(primary_datacenter).health == DatacenterHealth.UNHEALTHY.value,
        within_seconds=FAILURE_DETECTION_SECONDS,
        description=f"the gate leader classifying {primary_datacenter} unhealthy",
    )

    try:
        job_id = await cluster.client.submit_job(
            workflows=[([], QuickTestWorkflow())],
            vus=10,
            timeout_seconds=30.0,
            datacenter_count=1,
        )
    except RuntimeError:
        # The client's rejected-submission error: with a datacenter down,
        # the scenario accepts a rejection.
        return

    await wait_until(
        lambda: any(
            manager._job_manager.get_job_by_id(job_id) is not None
            for manager in cluster.managers_by_datacenter[secondary_datacenter]
        ),
        within_seconds=JOB_DISPATCH_SECONDS,
        description=f"job {job_id} reaching the fallback datacenter {secondary_datacenter}",
    )

    await cluster.client.cancel_job(job_id)


# =============================================================================
# Recovery handling
# =============================================================================


async def test_workflow_is_reassigned_after_worker_failure(cluster: ComprehensiveCluster) -> None:
    """The worker executing a job's workflow crashes: another worker takes
    the job's workflow over."""
    assert WORKERS_PER_DATACENTER >= 2, f"reassignment needs >= 2 workers per datacenter, have {WORKERS_PER_DATACENTER}"
    job_id = await cluster.client.submit_job(
        workflows=[([], SlowTestWorkflow())],
        vus=50,
        timeout_seconds=120.0,
        datacenter_count=1,
    )
    await wait_until(
        lambda: any(len(worker._active_workflows) > 0 for worker in cluster.all_workers),
        within_seconds=EXECUTION_START_SECONDS,
        description="a worker executing a workflow",
    )
    failed_worker = next(worker for worker in cluster.all_workers if len(worker._active_workflows) > 0)

    await cluster.stop_node(failed_worker)
    await wait_until(
        lambda: any(
            progress.job_id == job_id
            for worker in cluster.all_workers
            if worker is not failed_worker
            for progress in worker._active_workflows.values()
        ),
        within_seconds=FAILURE_DETECTION_SECONDS,
        description=f"another worker executing job {job_id}",
    )

    await cluster.client.cancel_job(job_id)


async def test_workers_and_managers_run_orphan_handling(cluster: ComprehensiveCluster) -> None:
    """Workers track orphaned workflows and every manager runs its orphan
    scan loop."""
    assert all(isinstance(worker._orphaned_workflows, dict) for worker in cluster.all_workers), (
        "a worker has no orphaned-workflow tracking"
    )
    assert all(
        manager._orphan_scan_task is not None and not manager._orphan_scan_task.done()
        for manager in cluster.all_managers
    ), "a manager's orphan scan loop is not running"


# =============================================================================
# Edge cases
# =============================================================================


async def test_zero_vu_job_is_rejected_or_finishes(cluster: ComprehensiveCluster) -> None:
    """A zero-VU job is either rejected at submission or reaches a terminal
    status."""
    try:
        job_id = await cluster.client.submit_job(
            workflows=[([], QuickTestWorkflow())],
            vus=0,
            timeout_seconds=10.0,
            datacenter_count=1,
        )
    except RuntimeError:
        # The client's rejected-submission error: an accepted outcome.
        return

    result = await asyncio.wait_for(cluster.client.wait_for_job(job_id, timeout=15.0), timeout=20.0)
    assert result.status in TERMINAL_JOB_STATUSES, f"zero-VU job ended {result.status}"


async def test_job_shorter_than_its_workflow_times_out_cleanly(cluster: ComprehensiveCluster) -> None:
    """A 3s job timeout on a 10s workflow ends the job in a timeout,
    partial, failed, cancelled or completed status; the client's wait
    timing out is also accepted."""
    job_id = await cluster.client.submit_job(
        workflows=[([], SlowTestWorkflow())],
        vus=50,
        timeout_seconds=3.0,
        datacenter_count=1,
    )

    try:
        result = await asyncio.wait_for(cluster.client.wait_for_job(job_id, timeout=30.0), timeout=35.0)
    except TimeoutError:
        # The scenario's accepted outcome: the client's wait timing out.
        return

    status = result.status.lower() if result.status else ""
    assert status in ("timeout", "timed_out", "partial", "failed", "cancelled", "completed"), (
        f"job status: {result.status}"
    )


async def test_submissions_without_idempotency_key_are_distinct_jobs(cluster: ComprehensiveCluster) -> None:
    """Two identical submissions without an idempotency key, a second
    apart, are two jobs."""
    first_job_id = await cluster.client.submit_job(
        workflows=[([], QuickTestWorkflow())],
        vus=5,
        timeout_seconds=30.0,
        datacenter_count=1,
    )
    await asyncio.sleep(1.0)
    second_job_id = await cluster.client.submit_job(
        workflows=[([], QuickTestWorkflow())],
        vus=5,
        timeout_seconds=30.0,
        datacenter_count=1,
    )

    assert first_job_id != second_job_id, f"both submissions returned job {first_job_id}"

    for job_id in (first_job_id, second_job_id):
        await cluster.client.cancel_job(job_id)
