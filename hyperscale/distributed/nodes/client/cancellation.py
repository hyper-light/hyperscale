"""
Job cancellation for HyperscaleClient.

Handles job cancellation with retry logic, leader redirection, and completion tracking.
"""

import asyncio
import random
import time

from hyperscale.distributed.models import (
    JobCancelRequest,
    JobCancelResponse,
    JobStatus,
    RateLimitResponse,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.config import ClientConfig, TRANSIENT_ERRORS
from hyperscale.logging import Logger


class ClientCancellationManager:
    """
    Manages job cancellation with retry logic and completion tracking.

    Cancellation flow:
    1. Build JobCancelRequest with job_id and reason
    2. Get targets prioritizing the server that accepted the job
    3. Retry loop with exponential backoff:
       - Cycle through all targets (gates/managers)
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

    async def _apply_retry_delay(
        self,
        retry: int,
        max_retries: int,
        base_delay: float,
    ) -> None:
        """Apply exponential backoff with jitter (AD-21) before retry."""
        if retry < max_retries:
            calculated_delay = base_delay * (2 ** retry)
            jittered_delay = calculated_delay * (0.5 + random.random())
            await asyncio.sleep(jittered_delay)

    def _handle_successful_response(
        self,
        job_id: str,
        response: JobCancelResponse,
    ) -> JobCancelResponse | None:
        """Handle successful or already-completed responses. Returns response if handled."""
        if response.success:
            self._tracker.update_job_status(job_id, JobStatus.CANCELLED.value)
            return response
        if response.already_cancelled:
            self._tracker.update_job_status(job_id, JobStatus.CANCELLED.value)
            return response
        if response.already_completed:
            self._tracker.update_job_status(job_id, JobStatus.COMPLETED.value)
            return response
        return None

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

        Sends a cancellation request to the gate/manager that owns the
        job. The cancellation propagates to all datacenters and workers
        executing workflows for this job.

        Strategy:

        1. **Targets:** ``ClientTargetSelector.get_targets_for_job`` is
           consulted; the job-specific target (the server that
           accepted submission) is tried first. The full target list is
           the failover fleet.

        2. **Per-target attempt:**

           a. Send the request.
           b. If the server response carries ``leader_addr``, swap to
              that leader as the new target and retry under the
              ``max_redirects`` budget. This is the canonical "non-
              leader received the cancel" path — every honest non-
              leader manager populates ``leader_addr`` (see
              ``ManagerServer.cancel_job`` handler).
           c. If the response is success / already_cancelled /
              already_completed, return immediately.
           d. If the error is transient (connection refused, timeout,
              rate-limit, "not leader" without an addr, etc.), back off
              and try the *next available target*. We never re-hit a
              target that already returned a transient error in the
              same call — that prevents the prior round-robin pattern
              from cycling through dead/refusing endpoints.

        3. **Termination:** the loop exhausts when either max_redirects
           and max_retries are both spent, or every target has been
           tried without a definitive answer. The last transient error
           message is included in the raised ``RuntimeError`` so the
           caller can diagnose.

        Args:
            job_id: Job identifier to cancel.
            reason: Optional reason for cancellation.
            max_redirects: Maximum leader redirects to follow within a
                single attempt sequence (per AD-20).
            max_retries: Maximum retries against alternate targets for
                transient errors.
            retry_base_delay: Base delay for exponential backoff
                (seconds).
            timeout: Request timeout in seconds.

        Returns:
            JobCancelResponse with cancellation result.

        Raises:
            RuntimeError: If no gates/managers configured or
                cancellation does not converge within budget.
            KeyError: If job not found (never submitted through this
                client).
        """
        # Initialize cancellation tracking BEFORE sending the request.
        # The manager → client ``job_cancellation_complete`` push can
        # arrive within milliseconds of the request returning (it's
        # sent from a TaskRunner background task the moment all
        # worker completions land), and the inbound TCP handler must
        # find the event in ``_cancellation_events`` to signal it.
        # If we defer initialization to ``await_job_cancellation``,
        # the notification can land first and silently no-op — the
        # subsequent ``await`` then blocks on an event that will
        # never be set. The fix is structural: ensure the event
        # exists before the request goes out. This mirrors the
        # ``seed-pending-before-send`` discipline the manager-side
        # cancellation flow uses for the same class of race.
        we_initialized_tracking = (
            job_id not in self._state._cancellation_events
        )
        if we_initialized_tracking:
            self._state.initialize_cancellation_tracking(job_id)

        try:
            request = JobCancelRequest(
                job_id=job_id,
                requester_id=f"client-{self._config.host}:{self._config.tcp_port}",
                timestamp=time.time(),
                fence_token=0,
                reason=reason,
            )

            configured_targets = self._targets.get_targets_for_job(job_id)
            if not configured_targets:
                raise RuntimeError("No managers or gates configured")

            # Pending = the failover fleet; tried = targets that have
            # returned a transient error already this call (so we don't
            # cycle back into them). The job-specific target is at
            # index 0 by ``get_targets_for_job`` contract; preserve order.
            pending: list[tuple[str, int]] = list(configured_targets)
            tried: set[tuple[str, int]] = set()

            last_error: str | None = None
            retries_used = 0

            while pending and retries_used <= max_retries:
                target = pending.pop(0)
                tried.add(target)

                result = await self._attempt_with_redirects(
                    initial_target=target,
                    request=request,
                    job_id=job_id,
                    timeout=timeout,
                    max_redirects=max_redirects,
                    tried=tried,
                )

                if isinstance(result, JobCancelResponse):
                    return result

                # ``result`` is a transient error string — back off then
                # fall over to the next pending target.
                last_error = result
                await self._apply_retry_delay(
                    retries_used, max_retries, retry_base_delay
                )
                retries_used += 1

            raise RuntimeError(
                f"Job cancellation failed after {retries_used} retries "
                f"across {len(tried)} target(s): {last_error}"
            )
        except BaseException:
            # Drop the tracking we installed if the request itself
            # failed — the caller will not reach ``await_job_cancellation``
            # to clean it up. Only clean what *we* set up; if the
            # tracker was pre-existing (concurrent caller), leave it
            # for that other caller to manage.
            if we_initialized_tracking:
                self._state._cancellation_events.pop(job_id, None)
                self._state._cancellation_success.pop(job_id, None)
                self._state._cancellation_errors.pop(job_id, None)
            raise

    async def _attempt_with_redirects(
        self,
        *,
        initial_target: tuple[str, int],
        request: JobCancelRequest,
        job_id: str,
        timeout: float,
        max_redirects: int,
        tried: set[tuple[str, int]],
    ) -> JobCancelResponse | str:
        """Send to ``initial_target``, follow leader redirects up to
        ``max_redirects``, return the response or a transient-error
        string.

        Permanent rejection (a JobCancelResponse with ``success=False``,
        no ``leader_addr``, and a non-transient ``error``) raises
        ``RuntimeError`` — the canonical "this is not retryable"
        signal.

        ``tried`` is mutated as the redirect chain visits new
        targets so the outer loop never falls back into a target the
        redirect path already consumed.
        """
        target = initial_target
        redirects_used = 0

        while True:
            response_data, _ = await self._send_tcp(
                target, "cancel_job", request.dump(), timeout=timeout
            )

            if isinstance(response_data, Exception):
                # Network-level failure — connection refused,
                # timeout, etc. Treat as transient; outer loop falls
                # over to the next target.
                return f"{type(response_data).__name__}: {response_data}"

            if response_data == b"error":
                return "Server returned error"

            rate_limit_delay = self._check_rate_limit(response_data)
            if rate_limit_delay is not None:
                # Honor the server's retry_after; treat as transient.
                await asyncio.sleep(rate_limit_delay)
                return "Rate limited"

            response = JobCancelResponse.load(response_data)
            handled = self._handle_successful_response(job_id, response)
            if handled:
                return handled

            # Leader redirect — follow it within budget.
            if response.leader_addr and redirects_used < max_redirects:
                redirect_target = tuple(response.leader_addr)
                if redirect_target in tried:
                    # We've already tried this leader; treat the
                    # redirect as a transient (the cluster is still
                    # converging on a leader). Outer loop will fall
                    # over.
                    return (
                        f"redirect cycles to already-tried target "
                        f"{redirect_target}"
                    )
                tried.add(redirect_target)
                target = redirect_target
                redirects_used += 1
                continue

            # No more redirects available — classify the error.
            if response.error and self._is_transient_error(response.error):
                return response.error

            # Permanent rejection: surface it.
            raise RuntimeError(f"Job cancellation failed: {response.error}")

    def _check_rate_limit(self, response_data: bytes) -> float | None:
        """Check if response is rate limiting. Returns delay if so, None otherwise."""
        try:
            rate_limit = RateLimitResponse.load(response_data)
            return rate_limit.retry_after_seconds
        except Exception:
            return None

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
        # Create event if not exists. ``cancel_job`` already
        # pre-installs tracking in the common case so the inbound
        # ``job_cancellation_complete`` handler always finds an event
        # to signal; this branch covers the "await without prior
        # cancel" case (e.g. a second awaiter or external job_id
        # passed in).
        if job_id not in self._state._cancellation_events:
            self._state.initialize_cancellation_tracking(job_id)

        event = self._state._cancellation_events[job_id]

        try:
            # ``event.wait()`` is the slow path; if the notification
            # has already arrived between ``cancel_job`` returning and
            # this call, ``event.is_set()`` is already True and
            # ``wait()`` returns immediately. The handler writes
            # success/errors *before* it sets the event, so any
            # caller that observes the event set is guaranteed to
            # see the consistent post-notification state below.
            try:
                if timeout is not None:
                    await asyncio.wait_for(event.wait(), timeout=timeout)
                else:
                    await event.wait()
            except asyncio.TimeoutError:
                return (
                    False,
                    [f"Timeout waiting for cancellation completion after {timeout}s"],
                )

            return (
                self._state._cancellation_success.get(job_id, False),
                self._state._cancellation_errors.get(job_id, []),
            )
        finally:
            # Cleanup on every exit path — success, timeout, or
            # exception. Without ``finally`` a cancellation that
            # never completes (e.g. cancel_job got a response but the
            # asynchronous completion push was lost) would leave the
            # tracker entries dangling for the lifetime of the client.
            self._state._cancellation_events.pop(job_id, None)
            self._state._cancellation_success.pop(job_id, None)
            self._state._cancellation_errors.pop(job_id, None)

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
