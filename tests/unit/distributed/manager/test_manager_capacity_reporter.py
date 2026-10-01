"""
AD-43: manager heartbeats report real backlog and remaining work.

``ManagerHeartbeat.pending_workflow_count`` / ``pending_duration_seconds``
/ ``active_remaining_seconds`` were declared and never set, so every
heartbeat shipped zeros into the gate's wait estimation and spillover
saw no backlog anywhere. ``ManagerCapacityReporter`` derives them from a
REAL ``JobManager`` (running sub-workflows) and the dispatcher's pending
map via the existing ``ExecutionTimeEstimator``.

Pinned here: running work counts its remaining duration; terminal work
counts nothing; only jobs this manager leads count (peers hydrate remote
jobs, and gates SUM across a datacenter's managers); exhausted pending
entries (cascade-failed / retry-exhausted, kept until job cleanup) are
not backlog.
"""

from types import SimpleNamespace

import pytest

from hyperscale.core.graph import Workflow
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.models import JobSubmission
from hyperscale.distributed.models.distributed import WorkflowStatus
from hyperscale.distributed.models.jobs import PendingWorkflow
from hyperscale.distributed.nodes.manager.capacity_reporter import (
    ManagerCapacityReporter,
)

MANAGER_ID = "manager-self"
WORKFLOW_DURATION_SECONDS = 120.0


class TwoMinuteWorkflow(Workflow):
    duration = "2m"
    timeout = "30s"


async def _running_job(job_manager: JobManager, job_id: str, leader_node_id: str):
    job = await job_manager.create_job(
        JobSubmission(job_id=job_id, workflows=b"", vus=1, timeout_seconds=600)
    )
    job.leader_node_id = leader_node_id
    workflow = await job_manager.register_workflow(
        job_id, "wf-1", "TwoMinuteWorkflow", TwoMinuteWorkflow()
    )
    await job_manager.register_sub_workflow(job_id, "wf-1", "worker-a", 4)
    await job_manager.update_workflow_status(job_id, workflow.token, WorkflowStatus.RUNNING)
    return workflow


def _pending(workflow_id: str, *, dispatched: bool = False, exhausted: bool = False) -> PendingWorkflow:
    entry = PendingWorkflow(
        job_id="job-mine",
        workflow_id=workflow_id,
        workflow_name="TwoMinuteWorkflow",
        workflow=TwoMinuteWorkflow(),
        vus=1,
        priority=None,
        is_test=True,
        dependencies=set(),
    )
    entry.dispatched = dispatched
    if exhausted:
        entry.dispatch_attempts = entry.max_dispatch_attempts
    return entry


def _reporter(job_manager: JobManager, pending_map: dict[str, PendingWorkflow]) -> ManagerCapacityReporter:
    dispatcher_view = SimpleNamespace(get_pending_workflows=lambda: pending_map)
    return ManagerCapacityReporter(
        job_manager=job_manager,
        get_workflow_dispatcher=lambda: dispatcher_view,
        get_total_cores=lambda: 8,
        node_id=MANAGER_ID,
    )


@pytest.mark.asyncio
async def test_running_work_reports_its_remaining_duration() -> None:
    job_manager = JobManager(datacenter="dc-a", manager_id=MANAGER_ID)
    await _running_job(job_manager, "job-mine", MANAGER_ID)

    _, _, active_remaining = _reporter(job_manager, {}).snapshot()

    assert 0.0 < active_remaining <= WORKFLOW_DURATION_SECONDS


@pytest.mark.asyncio
async def test_terminal_work_reports_nothing() -> None:
    job_manager = JobManager(datacenter="dc-a", manager_id=MANAGER_ID)
    workflow = await _running_job(job_manager, "job-mine", MANAGER_ID)
    await job_manager.update_workflow_status("job-mine", workflow.token, WorkflowStatus.COMPLETED)

    _, _, active_remaining = _reporter(job_manager, {}).snapshot()

    assert active_remaining == 0.0


@pytest.mark.asyncio
async def test_only_jobs_this_manager_leads_are_counted() -> None:
    job_manager = JobManager(datacenter="dc-a", manager_id=MANAGER_ID)
    await _running_job(job_manager, "job-peer", "manager-peer")

    _, _, active_remaining = _reporter(job_manager, {}).snapshot()

    assert active_remaining == 0.0


@pytest.mark.asyncio
async def test_backlog_counts_only_work_that_will_still_run() -> None:
    job_manager = JobManager(datacenter="dc-a", manager_id=MANAGER_ID)
    pending_map = {
        "awaiting": _pending("wf-2"),
        "exhausted": _pending("wf-3", exhausted=True),
        "dispatched": _pending("wf-4", dispatched=True),
    }

    pending_count, pending_duration, _ = _reporter(job_manager, pending_map).snapshot()

    assert pending_count == 1
    assert pending_duration == WORKFLOW_DURATION_SECONDS


@pytest.mark.asyncio
async def test_no_dispatcher_yet_reports_no_backlog() -> None:
    job_manager = JobManager(datacenter="dc-a", manager_id=MANAGER_ID)
    reporter = ManagerCapacityReporter(
        job_manager=job_manager,
        get_workflow_dispatcher=lambda: None,
        get_total_cores=lambda: 8,
        node_id=MANAGER_ID,
    )

    assert reporter.snapshot() == (0, 0.0, 0.0)
