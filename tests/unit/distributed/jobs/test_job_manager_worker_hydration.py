"""
A job leader learns the workflows a worker reports running (AD-54) --
including one its JobManager never registered, which hydrated with no
``is_test`` and raised ``KeyError`` from ``hydrate_remote_job_state``.

``is_test`` decides whether a workflow's per-core results merge as
load-test stats; only the workflow's definition knows it. A registered
workflow keeps what its definition said; one the job never registered keeps
its results unmerged, which loses nothing.
"""

from __future__ import annotations

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.models import JobSubmission, WorkflowProgress
from hyperscale.distributed.runtime.real_clock import RealClock

JOB_ID = "job-hydrated-from-worker"
WORKFLOW_ID = "workflow-reported-by-worker"
WORKER_ID = "worker-1"


def make_job_manager() -> JobManager:
    return JobManager(
        datacenter="dc-1",
        manager_id="manager-1",
        clock=RealClock(),
        max_budgeted_retries=Env().RETRY_BUDGET_PER_WORKFLOW_MAX,
    )


def reported_progress(job_manager: JobManager) -> WorkflowProgress:
    sub_workflow_token = job_manager.create_sub_workflow_token(JOB_ID, WORKFLOW_ID, WORKER_ID)
    return WorkflowProgress(
        job_id=JOB_ID,
        workflow_id=str(sub_workflow_token),
        workflow_name="ReportedWorkflow",
        status="running",
        completed_count=0,
        failed_count=0,
        rate_per_second=0.0,
        elapsed_seconds=0.0,
    )


async def hydrate(job_manager: JobManager):
    return await job_manager.hydrate_worker_active_workflow(
        progress=reported_progress(job_manager),
        worker_id=WORKER_ID,
        leader_node_id="manager-1",
        leader_addr=("127.0.0.1", 9000),
        fencing_token=1,
        callback_addr=None,
    )


@pytest.mark.asyncio
async def test_a_workflow_the_job_never_registered_is_learned_unmerged() -> None:
    job_manager = make_job_manager()

    job = await hydrate(job_manager)

    workflow_token = str(job_manager.create_workflow_token(JOB_ID, WORKFLOW_ID))
    assert job is not None
    assert job.workflows[workflow_token].is_test is False


@pytest.mark.asyncio
async def test_a_registered_test_workflow_keeps_its_classification() -> None:
    job_manager = make_job_manager()
    await job_manager.create_job(
        JobSubmission(job_id=JOB_ID, workflows=b"", vus=1, timeout_seconds=60.0)
    )
    await job_manager.register_workflow(
        JOB_ID,
        WORKFLOW_ID,
        "ReportedWorkflow",
        dependency_workflow_ids=frozenset(),
        is_test=True,
    )

    job = await hydrate(job_manager)

    workflow_token = str(job_manager.create_workflow_token(JOB_ID, WORKFLOW_ID))
    assert job is not None
    assert job.workflows[workflow_token].is_test is True
