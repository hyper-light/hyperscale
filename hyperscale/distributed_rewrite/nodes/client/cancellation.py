"""
Job cancellation for HyperscaleClient.

Handles job cancellation with retry logic, leader redirection, and completion tracking.
"""

import asyncio
import time

from hyperscale.core.jobs.models import JobStatus
from hyperscale.distributed_rewrite.models import (
    JobCancelRequest,
    JobCancelResponse,
)
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.distributed_rewrite.nodes.client.config import ClientConfig, TRANSIENT_ERRORS
from hyperscale.logging.hyperscale_logger import Logger


class ClientCancellationManager:
    """
    Manages job cancellation with retry logic and completion tracking.

    Cancellation flow:
    1. Build JobCancelRequest with job_id and reason
    2. Get targets prioritizing the server that accepted the job
    3. Retry loop with exponential backoff:
       - Cycle through all targets (gates/managers)
       - Follow leader redirects (up to max_redirects)
       - Detect transient errors and retry
       - Permanent rejection fails immediately
    4. On success: update job status to CANCELLED
    5. Handle already_cancelled/already_completed responses
    6. await_job_cancellation() waits for CancellationComplete push notification
    """

    def __init__(
        self,
        state: ClientState,
        config: ClientConfig,
        logger: Logger,
        targets,  # ClientTargetSelector
        tracker,  # ClientJobTracker
        send_tcp_func,  # Callable for sending TCP messages
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._targets = targets
        self._tracker = tracker
        self._send_tcp = send_tcp_func

    async def cancel_job(
        self,
        job_id: str,
        reason: str = "",
        max_redirects: int = 3,
        max_retries: int = 3,
        retry_base_delay: float = 0.5,
        timeout: float = 10.0,
    ) -> JobCancelResponse:
        """
        Cancel a running job.

        Sends a cancellation request to the gate/manager that owns the job.
        The cancellation propagates to all datacenters and workers executing
        workflows for this job.

        Args:
            job_id: Job identifier to cancel.
            reason: Optional reason for cancellation.
            max_redirects: Maximum leader redirects to follow.
            max_retries: Maximum retries for transient errors.
            retry_base_delay: Base delay for exponential backoff (seconds).
            timeout: Request timeout in seconds.

        Returns:
            JobCancelResponse with cancellation result.

        Raises:
            RuntimeError: If no gates/managers configured or cancellation fails.
            KeyError: If job not found (never submitted through this client).
        """
        # Build request
        request = JobCancelRequest(
            job_id=job_id,
            requester_id=f"client-{self._config.host}:{self._config.tcp_port}",
            timestamp=time.time(),
            fence_token=0,  # Client doesn't track fence tokens
            reason=reason,
        )

        # Determine targets - prefer the manager/gate that accepted the job
        all_targets = self._targets.get_targets_for_job(job_id)
        if not all_targets:
            raise RuntimeError("No managers or gates configured")

        last_error: str | None = None

        # Retry loop with exponential backoff
        for retry in range(max_retries + 1):
            target_idx = retry % len(all_targets)
            target = all_targets[target_idx]

            # Try with leader redirect handling
            result = await self._cancel_with_redirects(
                job_id, target, request, max_redirects, timeout
            )

            if result == "success":
                return self._state._jobs[job_id]  # Return updated job result
            elif isinstance(result, JobCancelResponse):
                # Success (already cancelled/completed) or permanent error handled
                return result
            else:
                # Transient error - retry
                last_error = result

            # Wait before retry with exponential backoff
            if retry < max_retries:
                delay = retry_base_delay * (2 ** retry)
                await asyncio.sleep(delay)

        # All retries exhausted
        raise RuntimeError(
            f"Job cancellation failed after {max_retries} retries: {last_error}"
        )

    async def _cancel_with_redirects(
        self,
        job_id: str,
        target: tuple[str, int],
        request: JobCancelRequest,
        max_redirects: int,
        timeout: float,
    ) -> str | JobCancelResponse:
        """
        Cancel with leader redirect handling.

        Args:
            job_id: Job identifier
            target: Initial target (host, port)
            request: JobCancelRequest message
            max_redirects: Maximum redirects to follow
            timeout: Request timeout

        Returns:
            "success", JobCancelResponse, or error message (transient)
        """
        redirects = 0
        while redirects <= max_redirects:
            response_data, _ = await self._send_tcp(
                target,
                "cancel_job",
                request.dump(),
                timeout=timeout,
            )

            if isinstance(response_data, Exception):
                return str(response_data)  # Transient error

            if response_data == b'error':
                return "Server returned error"  # Transient error

            response = JobCancelResponse.load(response_data)

            if response.success:
                self._tracker.update_job_status(job_id, JobStatus.CANCELLED.value)
                return "success"

            # Check for already completed/cancelled (not an error)
            if response.already_cancelled:
                self._tracker.update_job_status(job_id, JobStatus.CANCELLED.value)
                return response
            if response.already_completed:
                self._tracker.update_job_status(job_id, JobStatus.COMPLETED.value)
                return response

            # Check for leader redirect
            if response.leader_addr and redirects < max_redirects:
                target = tuple(response.leader_addr)
                redirects += 1
                continue

            # Check for transient error
            if response.error and self._is_transient_error(response.error):
                return response.error  # Transient error

            # Permanent error
            raise RuntimeError(f"Job cancellation failed: {response.error}")

        return "max_redirects_exceeded"

    async def await_job_cancellation(
        self,
        job_id: str,
        timeout: float | None = None,
    ) -> tuple[bool, list[str]]:
        """
        Wait for job cancellation to complete.

        This method blocks until the job cancellation is fully complete and the
        push notification is received from the manager/gate, or until timeout.

        Args:
            job_id: The job ID to wait for cancellation completion
            timeout: Optional timeout in seconds. None means wait indefinitely.

        Returns:
            Tuple of (success, errors):
            - success: True if all workflows were cancelled successfully
            - errors: List of error messages from workflows that failed to cancel
        """
        # Create event if not exists (in case called before cancel_job)
        if job_id not in self._state._cancellation_events:
            self._state.initialize_cancellation_tracking(job_id)

        event = self._state._cancellation_events[job_id]

        try:
            if timeout is not None:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            else:
                await event.wait()
        except asyncio.TimeoutError:
            return (False, [f"Timeout waiting for cancellation completion after {timeout}s"])

        # Get the results
        success = self._state._cancellation_success.get(job_id, False)
        errors = self._state._cancellation_errors.get(job_id, [])

        # Cleanup tracking structures
        self._state._cancellation_events.pop(job_id, None)
        self._state._cancellation_success.pop(job_id, None)
        self._state._cancellation_errors.pop(job_id, None)

        return (success, errors)

    def _is_transient_error(self, error: str) -> bool:
        """
        Check if an error is transient and should be retried.

        Args:
            error: Error message

        Returns:
            True if error matches TRANSIENT_ERRORS patterns
        """
        error_lower = error.lower()
        return any(te in error_lower for te in TRANSIENT_ERRORS)
