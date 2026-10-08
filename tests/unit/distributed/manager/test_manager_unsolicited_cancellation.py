"""
A workflow its manager or worker stopped, outside a job cancel, fails.

When every sub-workflow of a workflow came back CANCELLED, the manager
marked nothing: the job's completed + failed count never reached its
total, and the job stood until AD-34 declared a misleading timeout. That
is how an AD-41 resource kill ends -- the kill rides the cancel path, and
the worker reports the workflow CANCELLED.

* a CANCELLED aggregate of a workflow nothing was cancelling fails it with
  the recorded cancellation's reason (an AD-41 kill of its sub);
* without a recorded reason it fails with the workers' error;
* a CANCELLING workflow (a cancellation turns it CANCELLING before it sends
  any cancel) whose subs all came back cancelled is cancelled -- neither
  failed nor completed.

The real-process proof is test_over_budget_workflow_is_killed_before_it_would_finish
(tests/integration/cli/test_cli_resource_guard.py).
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import CancelledWorkflowInfo, WorkflowStatus
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.workflow import WorkflowState

JOB_ID = "job-1"
SUB_WORKFLOW_TOKEN = "dc-1:manager-1:job-1:wf-1:worker-1"
KILL_REASON = "resource budget exceeded: cpu_exceeded"


class RecordingJobManager:
    def __init__(self, workflow_state: WorkflowState) -> None:
        self.failed: list[tuple[str, str]] = []
        self.completed: list[str] = []
        self.cancelled: list[str] = []
        self.workflow_lifecycle = SimpleNamespace(
            get_state=lambda job_id, workflow_id: workflow_state
        )

    async def finish_workflow_cancellation(self, job_id: str, workflow_id: str) -> bool:
        self.cancelled.append(workflow_id)
        return True

    async def aggregate_parent_workflow_outcome(self, workflow_id: str):
        return WorkflowStatus.CANCELLED.value, "Cancelled", []

    async def mark_workflow_completed(self, token) -> None:
        self.completed.append(str(token))

    async def mark_workflow_failed(self, token, error: str) -> bool:
        self.failed.append((str(token), error))
        return True


def make_manager(
    workflow_is_cancelling: bool,
    cancellation: CancelledWorkflowInfo | None,
) -> tuple[ManagerServer, RecordingJobManager]:
    manager = object.__new__(ManagerServer)
    job_manager = RecordingJobManager(
        WorkflowState.CANCELLING if workflow_is_cancelling else WorkflowState.RUNNING
    )
    manager._job_manager = job_manager
    manager._manager_state = SimpleNamespace(
        get_cancelled_workflow=lambda job_id, workflow_id: (
            cancellation if (job_id, workflow_id) == (JOB_ID, SUB_WORKFLOW_TOKEN) else None
        ),
    )

    async def push_result(*args, **kwargs) -> None:
        return None

    manager._push_workflow_result_to_client = push_result
    return manager, job_manager


def final_result() -> SimpleNamespace:
    return SimpleNamespace(job_id=JOB_ID, workflow_id=SUB_WORKFLOW_TOKEN, status="cancelled")


async def handle(manager: ManagerServer) -> None:
    await manager._handle_parent_workflow_completion(final_result(), True, True)


@pytest.mark.asyncio
async def test_an_unsolicited_cancellation_fails_with_its_recorded_reason() -> None:
    manager, job_manager = make_manager(
        workflow_is_cancelling=False,
        cancellation=CancelledWorkflowInfo(
            job_id=JOB_ID,
            workflow_id=SUB_WORKFLOW_TOKEN,
            cancelled_at=1.0,
            reason=KILL_REASON,
        ),
    )

    await handle(manager)

    assert [error for _, error in job_manager.failed] == [f"cancelled before completing: {KILL_REASON}"]


@pytest.mark.asyncio
async def test_without_a_recorded_reason_it_fails_with_the_workers_error() -> None:
    manager, job_manager = make_manager(workflow_is_cancelling=False, cancellation=None)

    await handle(manager)

    assert [error for _, error in job_manager.failed] == ["Cancelled"]


@pytest.mark.asyncio
async def test_a_cancelling_workflow_whose_subs_came_back_cancelled_is_cancelled() -> None:
    manager, job_manager = make_manager(workflow_is_cancelling=True, cancellation=None)

    await handle(manager)

    assert job_manager.cancelled == ["wf-1"]
    assert job_manager.failed == []
    assert job_manager.completed == []
