"""
A worker's per-job death (AD-30) reassigns only what still needs a worker.

The per-job path picked every sub-workflow on the dead worker that had no
result -- including superseded ones and ones whose parent had already
finished (a multi-core workflow completes on its surviving sub). The
requeue that followed dispatched a finished workflow again, running its
load twice. It now uses the global death's filter, scoped to the job.

* an unfinished sub of an unfinished parent is reassigned;
* a sub whose parent already finished is not;
* a superseded sub is not;
* a manager that does not lead the job reassigns nothing -- its leader
  decides the job's retries.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.models.jobs import JobInfo, SubWorkflowInfo, WorkflowInfo
from hyperscale.distributed.models import WorkflowStatus
from hyperscale.distributed.nodes.manager.server import ManagerServer

JOB_ID = "job-1"
DEAD_WORKER = "worker-dead"
LIVE_WORKER = "worker-live"


def make_job_manager(parent_status: WorkflowStatus, superseded: bool) -> JobManager:
    job_manager = JobManager(
        datacenter="dc-1",
        manager_id="manager-1",
        clock=RealClock(),
        max_budgeted_retries=Env().RETRY_BUDGET_PER_WORKFLOW_MAX,
    )
    job_token = job_manager.create_job_token(JOB_ID)
    workflow_token = job_manager.create_workflow_token(JOB_ID, "wf-1")
    job = JobInfo(token=job_token, submission=None, workflows_total=1)
    job.workflows[str(workflow_token)] = WorkflowInfo(
        token=workflow_token,
        name="Workflow",
        status=parent_status,
        terminal_pushed=parent_status == WorkflowStatus.COMPLETED,
    )
    for worker_id, sub_superseded in ((DEAD_WORKER, superseded), (LIVE_WORKER, False)):
        sub_token = job_manager.create_sub_workflow_token(JOB_ID, "wf-1", worker_id)
        job.sub_workflows[str(sub_token)] = SubWorkflowInfo(
            token=sub_token,
            parent_token=workflow_token,
            cores_allocated=1,
            superseded=sub_superseded,
        )
    job_manager._jobs[str(job_token)] = job
    return job_manager


async def reassigned_subs(job_manager: JobManager, leads_job: bool = True) -> list[str]:
    reassigned: list[str] = []
    manager = object.__new__(ManagerServer)
    manager._job_manager = job_manager
    manager._workflow_dispatcher = SimpleNamespace()
    manager._leases = SimpleNamespace(is_job_leader=lambda job_id: leads_job)
    manager._systemic_eviction_hold = False

    async def apply_workflow_reassignment_state(**reassignment) -> None:
        reassigned.append(reassignment["sub_workflow_token"])

    manager._apply_workflow_reassignment_state = apply_workflow_reassignment_state
    await manager._handle_worker_dead_for_job_reassignment(JOB_ID, DEAD_WORKER)
    return reassigned


@pytest.mark.asyncio
async def test_an_unfinished_sub_of_an_unfinished_parent_is_reassigned() -> None:
    job_manager = make_job_manager(WorkflowStatus.RUNNING, superseded=False)

    reassigned = await reassigned_subs(job_manager)

    dead_sub = str(job_manager.create_sub_workflow_token(JOB_ID, "wf-1", DEAD_WORKER))
    assert reassigned == [dead_sub]


@pytest.mark.asyncio
async def test_a_sub_whose_parent_already_finished_is_not_reassigned() -> None:
    job_manager = make_job_manager(WorkflowStatus.COMPLETED, superseded=False)

    assert await reassigned_subs(job_manager) == []


@pytest.mark.asyncio
async def test_a_superseded_sub_is_not_reassigned() -> None:
    job_manager = make_job_manager(WorkflowStatus.RUNNING, superseded=True)

    assert await reassigned_subs(job_manager) == []


@pytest.mark.asyncio
async def test_a_follower_reassigns_nothing() -> None:
    job_manager = make_job_manager(WorkflowStatus.RUNNING, superseded=False)

    assert await reassigned_subs(job_manager, leads_job=False) == []
