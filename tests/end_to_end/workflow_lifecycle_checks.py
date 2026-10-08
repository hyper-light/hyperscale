"""
AD-54 / AD-44 invariants the end-to-end sections check on a manager after
their scenario ran -- whatever the scenario did, these hold:

* no workflow transition was refused: every move any workflow made was an
  edge of the AD-54 table;
* every workflow the manager still tracks has a lifecycle record, and
  reads the status that record's state projects to;
* no AD-44 retry budget outlives its job.
"""

from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.workflow import WORKFLOW_STATUS_BY_WORKFLOW_STATE


def assert_workflow_lifecycle_sound(manager: ManagerServer, scenario: str) -> None:
    lifecycle = manager._job_manager.workflow_lifecycle
    assert lifecycle.rejected_transition_count == 0, (
        f"{scenario}: {lifecycle.rejected_transition_count} workflow transition(s) "
        "refused -- a workflow moved off the AD-54 table"
    )
    for job in manager._job_manager.iter_jobs():
        for workflow_info in job.workflows.values():
            workflow_id = workflow_info.token.workflow_id or ""
            state = lifecycle.get_state(job.job_id, workflow_id)
            assert state is not None, (
                f"{scenario}: workflow {workflow_id} of job {job.job_id} has no lifecycle record"
            )
            assert WORKFLOW_STATUS_BY_WORKFLOW_STATE[state] == workflow_info.status, (
                f"{scenario}: workflow {workflow_id} reads {workflow_info.status.value} "
                f"but its lifecycle is {state.value}"
            )


def assert_retry_budgets_released(manager: ManagerServer, scenario: str) -> None:
    tracked_job_ids = {job.job_id for job in manager._job_manager.iter_jobs()}
    leaked_budget_job_ids = set(manager._retry_budget_manager._budgets) - tracked_job_ids
    assert not leaked_budget_job_ids, (
        f"{scenario}: retry budgets outlived their jobs: {sorted(leaked_budget_job_ids)}"
    )
