"""
TCP handler for workflow result push notifications.

Handles WorkflowResultPush messages with aggregated workflow completion results.
"""

import time

from hyperscale.distributed_rewrite.models import (
    WorkflowResultPush,
    ClientWorkflowResult,
    ClientWorkflowDCResult,
)
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.logging import Logger


class WorkflowResultPushHandler:
    """
    Handle workflow result push from manager or gate.

    Called when a workflow completes with aggregated results.
    Updates the job's workflow_results for immediate access.

    For multi-DC jobs (via gates), includes per_dc_results with per-datacenter breakdown.
    For single-DC jobs (direct from manager), per_dc_results will be empty.
    """

    def __init__(
        self,
        state: ClientState,
        logger: Logger,
        reporting_manager=None,  # Will be injected later
    ) -> None:
        self._state = state
        self._logger = logger
        self._reporting_manager = reporting_manager

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process workflow result push.

        Args:
            addr: Source address (gate/manager)
            data: Serialized WorkflowResultPush message
            clock_time: Logical clock time

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            push = WorkflowResultPush.load(data)

            job = self._state._jobs.get(push.job_id)
            if job:
                # Extract aggregated stats (should be single item list for client-bound)
                stats = push.results[0] if push.results else None

                # Convert per-DC results from message format to client format
                per_dc_results: list[ClientWorkflowDCResult] = []
                for dc_result in push.per_dc_results:
                    per_dc_results.append(
                        ClientWorkflowDCResult(
                            datacenter=dc_result.datacenter,
                            status=dc_result.status,
                            stats=dc_result.stats,
                            error=dc_result.error,
                            elapsed_seconds=dc_result.elapsed_seconds,
                        )
                    )

                # Use push.completed_at if provided, otherwise use current time
                completed_at = push.completed_at if push.completed_at > 0 else time.time()

                job.workflow_results[push.workflow_id] = ClientWorkflowResult(
                    workflow_id=push.workflow_id,
                    workflow_name=push.workflow_name,
                    status=push.status,
                    stats=stats,
                    error=push.error,
                    elapsed_seconds=push.elapsed_seconds,
                    completed_at=completed_at,
                    per_dc_results=per_dc_results,
                )

            # Call user callback if registered
            callback = self._state._workflow_callbacks.get(push.job_id)
            if callback:
                try:
                    callback(push)
                except Exception:
                    pass  # Don't let callback errors break the handler

            # Submit to local file-based reporters (aggregated stats only, not per-DC)
            if stats and self._reporting_manager:
                await self._reporting_manager.submit_to_local_reporters(
                    push.job_id, push.workflow_name, stats
                )

            return b'ok'

        except Exception:
            return b'error'
