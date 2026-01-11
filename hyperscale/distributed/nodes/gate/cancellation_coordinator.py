"""
Gate cancellation coordination module (AD-20).

Coordinates job and workflow cancellation across datacenters.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    CancelJob,
    CancelAck,
    JobCancelRequest,
    JobCancelResponse,
    JobCancellationComplete,
)

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.logging import Logger
    from hyperscale.taskex import TaskRunner


class GateCancellationCoordinator:
    """
    Coordinates job cancellation across datacenters.

    Responsibilities:
    - Handle cancel requests from clients
    - Forward cancellation to DC managers
    - Track cancellation completion
    - Aggregate cancellation results
    """

    def __init__(
        self,
        state: "GateRuntimeState",
        logger: "Logger",
        task_runner: "TaskRunner",
        get_job_target_dcs: callable,
        get_dc_manager_addr: callable,
        send_tcp: callable,
        is_job_leader: callable,
    ) -> None:
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._get_job_target_dcs = get_job_target_dcs
        self._get_dc_manager_addr = get_dc_manager_addr
        self._send_tcp = send_tcp
        self._is_job_leader = is_job_leader

    async def cancel_job(
        self,
        job_id: str,
        reason: str = "user_requested",
    ) -> JobCancelResponse:
        """
        Cancel a job across all target datacenters.

        Args:
            job_id: Job identifier
            reason: Cancellation reason

        Returns:
            JobCancelResponse with status
        """
        # Check if we're the job leader
        if not self._is_job_leader(job_id):
            return JobCancelResponse(
                job_id=job_id,
                success=False,
                error="Not job leader - redirect to leader gate",
            )

        # Get target DCs for this job
        target_dcs = self._get_job_target_dcs(job_id)
        if not target_dcs:
            return JobCancelResponse(
                job_id=job_id,
                success=False,
                error="Job not found or no target DCs",
            )

        # Initialize cancellation tracking
        event = self._state.initialize_cancellation(job_id)

        # Send cancellation to each DC
        cancel_tasks = []
        for dc_id in target_dcs:
            task = self._task_runner.run(
                self._cancel_job_in_dc,
                job_id,
                dc_id,
                reason,
            )
            cancel_tasks.append(task)

        # Wait for all DCs to respond (with timeout)
        try:
            await asyncio.wait_for(event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            self._state.add_cancellation_error(job_id, "Timeout waiting for DC responses")

        # Get results
        errors = self._state.get_cancellation_errors(job_id)
        success = len(errors) == 0

        # Cleanup
        self._state.cleanup_cancellation(job_id)

        return JobCancelResponse(
            job_id=job_id,
            success=success,
            error="; ".join(errors) if errors else None,
        )

    async def _cancel_job_in_dc(
        self,
        job_id: str,
        dc_id: str,
        reason: str,
    ) -> None:
        """
        Send cancellation request to a specific datacenter.

        Args:
            job_id: Job identifier
            dc_id: Datacenter identifier
            reason: Cancellation reason
        """
        try:
            manager_addr = self._get_dc_manager_addr(job_id, dc_id)
            if not manager_addr:
                self._state.add_cancellation_error(
                    job_id, f"No manager found for DC {dc_id}"
                )
                return

            cancel_msg = CancelJob(
                job_id=job_id,
                reason=reason,
            )

            response, _ = await self._send_tcp(
                manager_addr,
                "cancel_job",
                cancel_msg.dump(),
                timeout=10.0,
            )

            if response and not isinstance(response, Exception):
                ack = CancelAck.load(response)
                if not ack.cancelled:
                    self._state.add_cancellation_error(
                        job_id, f"DC {dc_id} rejected: {ack.error}"
                    )
            else:
                self._state.add_cancellation_error(
                    job_id, f"No response from DC {dc_id}"
                )

        except Exception as e:
            self._state.add_cancellation_error(
                job_id, f"Error cancelling in DC {dc_id}: {str(e)}"
            )

    def handle_cancellation_complete(
        self,
        job_id: str,
        dc_id: str,
        success: bool,
        workflows_cancelled: int,
        errors: list[str],
    ) -> None:
        """
        Handle cancellation completion notification from a manager.

        Args:
            job_id: Job identifier
            dc_id: Datacenter that completed cancellation
            success: Whether cancellation succeeded
            workflows_cancelled: Number of workflows cancelled
            errors: Any errors encountered
        """
        # Record errors if any
        for error in errors:
            self._state.add_cancellation_error(job_id, f"DC {dc_id}: {error}")

        # Check if all DCs have reported
        # This is tracked by counting completed DCs
        event = self._state.get_cancellation_event(job_id)
        if event:
            # Signal completion (the cancel_job method will check all DCs)
            event.set()


__all__ = ["GateCancellationCoordinator"]
