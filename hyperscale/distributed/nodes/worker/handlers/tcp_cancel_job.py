"""
Job-scoped cancellation TCP handler for worker.

Handles ``CancelJobWorkflowsRequest`` from a manager that wants every
workflow on this worker for a given ``job_id`` cancelled, without
needing to know the per-workflow ids in advance. Used by the
takeover-side cancel path on a new DC leader whose post-failover
``job.workflows`` map didn't fully repopulate from peer/worker state
sync — the new leader broadcasts this request to every worker it
knows about and the workers report back which workflows they actually
cancelled.

Replaces the prior ``manager/server.py`` shortcut that fired the
``job_cancellation_complete`` push speculatively when its local
``workflows_to_cancel`` was empty, leaving worker-side workflows
running indefinitely under the surface.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    CancelJobWorkflowsRequest,
    CancelJobWorkflowsResponse,
    WorkflowProgress,
    WorkflowStatus,
)
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerInfo

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.worker.server import WorkerServer


class CancelJobWorkflowsHandler:
    """
    Handler for job-scoped workflow cancellation requests from managers.

    Iterates ``_active_workflows`` for any sub-workflow whose
    ``progress.job_id`` matches the request, cancels each via the
    existing ``_cancel_workflow`` path, and returns the set of
    sub-workflow token strings the worker actually cancelled.

    Workflows already in a terminal state (CANCELLED / COMPLETED /
    FAILED) are skipped — the per-workflow ``cancel_workflow``
    handler's ``already_completed`` short-circuit makes the cancel
    request idempotent in that case anyway, but skipping here avoids
    an unnecessary ``cancel_workflow`` task per terminal workflow.
    """

    def __init__(self, server: "WorkerServer") -> None:
        self._server: "WorkerServer" = server

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        try:
            request = CancelJobWorkflowsRequest.load(data)
        except Exception as load_error:
            return await self._load_failure_response(addr, load_error)

        matching_workflow_ids = self._matching_workflow_ids(request.job_id)

        cancelled_ids: list[str] = []
        errors: list[str] = []
        for workflow_id in matching_workflow_ids:
            await self._cancel_matching_workflow(workflow_id, cancelled_ids, errors)

        await self._log_cancelled(request, cancelled_ids)

        return CancelJobWorkflowsResponse(
            job_id=request.job_id,
            worker_id=self._server._node_id.full,
            cancelled_workflow_ids=cancelled_ids,
            errors=errors,
        ).dump()

    async def _load_failure_response(
        self,
        addr: tuple[str, int],
        load_error: Exception,
    ) -> bytes:
        """Log an unparseable request and answer with its load error."""
        await self._server._udp_logger.log(
            ServerError(
                message=(
                    "Failed to load CancelJobWorkflowsRequest from "
                    f"{addr}: {load_error}"
                ),
                node_host=self._server._host,
                node_port=self._server._tcp_port,
                node_id=self._server._node_id.short,
            )
        )
        return CancelJobWorkflowsResponse(
            job_id="",
            worker_id=self._server._node_id.full,
            cancelled_workflow_ids=[],
            errors=[str(load_error)],
        ).dump()

    def _matching_workflow_ids(self, job_id: str) -> list[str]:
        """Snapshot the job's non-terminal active workflow ids."""
        # Snapshot the active-workflow keys before iterating —
        # ``_cancel_workflow`` mutates ``_active_workflows`` for any
        # workflow whose status field gets updated to CANCELLED while
        # the loop runs, so a live iteration would either skip
        # workflows or trip a ``dict changed size during iteration``
        # error. Filtering by ``progress.job_id == request.job_id``
        # at snapshot time is the canonical way to perform job-scoped
        # iteration safely.
        terminal_statuses = (
            WorkflowStatus.CANCELLED.value,
            WorkflowStatus.COMPLETED.value,
            WorkflowStatus.FAILED.value,
        )
        return [
            workflow_id
            for workflow_id, progress in list(
                self._server._active_workflows.items()
            )
            if self._is_cancellable_for_job(progress, job_id, terminal_statuses)
        ]

    @staticmethod
    def _is_cancellable_for_job(
        progress: "WorkflowProgress | None",
        job_id: str,
        terminal_statuses: tuple[str, str, str],
    ) -> bool:
        """Whether a tracked workflow belongs to the job and is not terminal."""
        return (
            progress is not None
            and progress.job_id == job_id
            and progress.status not in terminal_statuses
        )

    async def _cancel_matching_workflow(
        self,
        workflow_id: str,
        cancelled_ids: list[str],
        errors: list[str],
    ) -> None:
        """Cancel one workflow, recording its id when cancelled and any errors."""
        try:
            cancelled, cancel_errors = await self._server._cancel_workflow(
                workflow_id, "manager_job_cancel_request"
            )
        except Exception as cancel_error:
            errors.append(
                f"Workflow {workflow_id[:8]}...: {cancel_error}"
            )
            return

        if cancelled:
            cancelled_ids.append(workflow_id)
        self._extend_workflow_errors(workflow_id, cancel_errors, errors)

    @staticmethod
    def _extend_workflow_errors(
        workflow_id: str,
        cancel_errors: list[str],
        errors: list[str],
    ) -> None:
        """Append a workflow's cancel errors, each prefixed with its short id."""
        if cancel_errors:
            errors.extend(
                f"Workflow {workflow_id[:8]}...: {err}"
                for err in cancel_errors
            )

    async def _log_cancelled(
        self,
        request: CancelJobWorkflowsRequest,
        cancelled_ids: list[str],
    ) -> None:
        """Log how many of the job's workflows were cancelled, if any."""
        if cancelled_ids:
            await self._server._udp_logger.log(
                ServerInfo(
                    message=(
                        f"Cancelled {len(cancelled_ids)} workflow(s) for job "
                        f"{request.job_id[:8]}... via "
                        "CancelJobWorkflowsRequest"
                    ),
                    node_host=self._server._host,
                    node_port=self._server._tcp_port,
                    node_id=self._server._node_id.short,
                )
            )
