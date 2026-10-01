"""
AD-43 capacity inputs for the manager heartbeat.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.capacity.active_dispatch import ActiveDispatch
from hyperscale.distributed.capacity.execution_time_estimator import (
    ExecutionTimeEstimator,
)
from hyperscale.distributed.models.distributed import WorkflowStatus
from hyperscale.distributed.taskex.util.time_parser import TimeParser

if TYPE_CHECKING:
    from hyperscale.distributed.jobs.job_manager import JobManager
    from hyperscale.distributed.jobs.workflow_dispatcher import WorkflowDispatcher
    from hyperscale.distributed.models.jobs import JobInfo


EXECUTING_WORKFLOW_STATUSES: frozenset[WorkflowStatus] = frozenset(
    {WorkflowStatus.ASSIGNED, WorkflowStatus.RUNNING}
)


class ManagerCapacityReporter:
    """Computes this manager's AD-43 heartbeat capacity fields.

    ``pending_workflow_count`` / ``pending_duration_seconds`` cover work
    queued here that will still run; ``active_remaining_seconds`` covers
    sub-workflows executing on this manager's workers. Gates sum these
    across a datacenter's managers, so only jobs THIS manager leads are
    counted — peers hydrate remote jobs for failover, and counting those
    too would double the datacenter's backlog.
    """

    def __init__(
        self,
        job_manager: JobManager,
        get_workflow_dispatcher: Callable[[], WorkflowDispatcher | None],
        get_total_cores: Callable[[], int],
        node_id: str,
    ) -> None:
        self._job_manager = job_manager
        self._get_workflow_dispatcher = get_workflow_dispatcher
        self._get_total_cores = get_total_cores
        self._node_id = node_id

    def snapshot(self) -> tuple[int, float, float]:
        """Return ``(pending_count, pending_duration_seconds, active_remaining_seconds)``."""
        dispatcher = self._get_workflow_dispatcher()
        pending_workflows = dispatcher.get_pending_workflows() if dispatcher else {}

        estimator = ExecutionTimeEstimator(
            active_dispatches=self._active_dispatches(),
            pending_workflows=pending_workflows,
            total_cores=self._get_total_cores(),
        )
        pending_count = sum(
            1 for pending in pending_workflows.values() if pending.is_awaiting_dispatch
        )
        return (
            pending_count,
            estimator.get_pending_duration_sum(),
            estimator.get_active_remaining_sum(),
        )

    def _active_dispatches(self) -> dict[str, ActiveDispatch]:
        return {
            sub_workflow_token: dispatch
            for job in self._job_manager.iter_jobs()
            if job.leader_node_id == self._node_id
            for sub_workflow_token, dispatch in self._job_active_dispatches(job)
        }

    @staticmethod
    def _job_active_dispatches(job: JobInfo) -> list[tuple[str, ActiveDispatch]]:
        active: list[tuple[str, ActiveDispatch]] = []
        for sub_workflow_token, sub_workflow in job.sub_workflows.items():
            if sub_workflow.result is not None or sub_workflow.superseded:
                continue

            parent = job.workflows.get(str(sub_workflow.parent_token))
            if parent is None or parent.workflow is None:
                continue
            if parent.status not in EXECUTING_WORKFLOW_STATUSES:
                continue

            active.append(
                (
                    sub_workflow_token,
                    ActiveDispatch(
                        workflow_id=parent.token.workflow_id or "",
                        job_id=job.job_id,
                        worker_id=sub_workflow.token.worker_id or "",
                        cores_allocated=sub_workflow.cores_allocated,
                        dispatched_at=sub_workflow.dispatched_at,
                        duration_seconds=TimeParser(parent.workflow.duration).time,
                        timeout_seconds=TimeParser(parent.workflow.timeout).time,
                    ),
                )
            )
        return active
