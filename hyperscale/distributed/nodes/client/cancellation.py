"""
Job cancellation for HyperscaleClient.

Handles job cancellation with retry logic, leader redirection, and completion tracking.
"""

import asyncio

from hyperscale.distributed.models import (
    JobCancelRequest,
    JobCancelResponse,
    JobStatus,
    RateLimitResponse,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.config import TRANSIENT_ERRORS
from hyperscale.distributed.nodes.client.models.client_config import ClientConfig
from hyperscale.logging import Logger

from hyperscale.distributed.runtime import Clock, RealClock, Random, RealRandom


_DEFAULT_CLOCK: Clock = RealClock()
_DEFAULT_RANDOM: Random = RealRandom()

# Per-send timeout for a single cancel round-trip. Kept well under the
# caller's total cancellation budget so one slow hop — e.g. a gate
# fanning a cancel out to a partly-dead DC mid-failover — cannot
# swallow the whole budget; the budget is spent across many quick
# fleet sweeps instead. Must exceed the gate's own fail-fast
# DC-forward ceiling so a healthy gate round-trip is never cut off
# (see ``GateCancellationHandler`` forward retry config).
_CANCEL_PER_SEND_TIMEOUT_SECONDS = 12.0


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

    def _handle_successful_response(
        self,
        job_id: str,
        response: JobCancelResponse,
    ) -> JobCancelResponse | None:
        """Handle successful or already-completed responses. Returns response if handled.

        For ``success`` (the canonical cancel-mid-flight path) the manager
        sends an asynchronous ``job_cancellation_complete`` push after all
        workers report in, which signals the cancellation event via
        ``CancellationCompleteHandler``. For the two terminal-state
        short-circuit branches — ``already_cancelled`` and
        ``already_completed`` — no async push is ever sent, so any
        caller awaiting ``await_job_cancellation`` would hang for the
        full timeout. Synthesize the completion state in-place and
        signal the event so the await returns immediately.
        """
        if response.success:
            self._tracker.update_job_status(job_id, JobStatus.CANCELLED.value)
            return response
        return self._handle_terminal_response(job_id, response)

    def _handle_terminal_response(
        self,
        job_id: str,
        response: JobCancelResponse,
    ) -> JobCancelResponse | None:
        """Synthesize completion for an already-cancelled or already-completed response; None for others."""
        if response.already_cancelled:
            self._tracker.update_job_status(job_id, JobStatus.CANCELLED.value)
            self._signal_terminal_cancellation(job_id, success=True, errors=[])
            return response
        if response.already_completed:
            self._tracker.update_job_status(job_id, JobStatus.COMPLETED.value)
            self._signal_terminal_cancellation(
                job_id,
                success=False,
                errors=["Job already completed"],
            )
            return response
        return None

    def _signal_terminal_cancellation(
        self,
        job_id: str,
        *,
        success: bool,
        errors: list[str],
    ) -> None:
        """Mark a cancellation as resolved without an async manager push.

        The manager's ``cancel_job`` handler only emits the asynchronous
        ``job_cancellation_complete`` push when it transitions the job
        through the cancellation pipeline. For terminal-state responses
        (``already_cancelled`` / ``already_completed``) the synchronous
        response is the only signal the client ever receives; without
        this signal, ``await_job_cancellation`` would block until its
        timeout fires. Writing ``_cancellation_success`` /
        ``_cancellation_errors`` before ``Event.set`` preserves the same
        ordering invariant the inbound push handler relies on, so
        either signal path produces a consistent post-event state.
        """
        self._state._cancellation_success[job_id] = success
        self._state._cancellation_errors[job_id] = errors
        event = self._state._cancellation_events.get(job_id)
        if event is not None:
            event.set()

    async def cancel_job(
        self,
        job_id: str,
        reason: str = "",
        max_redirects: int = 3,
        max_retries: int = 3,
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
            return await self._cancel_within_budget(
                job_id,
                reason,
                max_redirects,
                self._config.retry_base_delay_seconds,
                timeout,
            )
        except BaseException:
            self._drop_tracking_we_installed(job_id, we_initialized_tracking)
            raise

    def _drop_tracking_we_installed(self, job_id: str, we_initialized_tracking: bool) -> None:
        """Drop the cancellation tracking this call installed, after its request failed."""
        # Drop the tracking we installed if the request itself
        # failed — the caller will not reach ``await_job_cancellation``
        # to clean it up. Only clean what *we* set up; if the
        # tracker was pre-existing (concurrent caller), leave it
        # for that other caller to manage.
        if we_initialized_tracking:
            self._state._cancellation_events.pop(job_id, None)
            self._state._cancellation_success.pop(job_id, None)
            self._state._cancellation_errors.pop(job_id, None)

    async def _cancel_within_budget(
        self,
        job_id: str,
        reason: str,
        max_redirects: int,
        retry_base_delay: float,
        timeout: float,
    ) -> JobCancelResponse:
        """Sweep the job's targets until one answers definitively or the time budget is spent."""
        request = JobCancelRequest(
            job_id=job_id,
            requester_id=f"client-{self._config.host}:{self._config.tcp_port}",
            timestamp=_DEFAULT_CLOCK.time(),
            fence_token=0,
            reason=reason,
            # Piggyback the client's callback address so the
            # receiving manager can seed its ``_job_callbacks``
            # entry when it was lost across a leader failover —
            # otherwise ``_push_cancellation_complete_to_origin``
            # silently no-ops and the client hangs waiting for a
            # push that will never fire.
            callback_addr=self._targets.get_callback_addr(),
        )

        configured_targets = self._cancel_targets(job_id)

        # Managers/gates this attempt has demonstrably failed to
        # reach (connection refused / timeout). Threaded into every
        # subsequent request so a freshly-elected DC leader can
        # treat a cached job-leader in this set as genuinely dead
        # and take over immediately, rather than redirecting the
        # client back to an address it already proved unreachable
        # while its own SWIM failure detector is still catching up.
        # Accumulated across the whole budget: a node proven dead
        # stays dead for the duration of this cancel.
        unreachable: set[tuple[str, int]] = set()

        # Retry is bounded by a TIME budget, not a fixed count. A
        # cancel issued mid-failover must keep sweeping the fleet
        # until the DC's manager leadership reconverges and a target
        # confirms — which can take a full SWIM/Raft failover
        # convergence (tens of seconds), far longer than a fixed
        # ``max_retries`` of quick round-trips would span. The
        # per-send timeout is kept modest (bounded well under the
        # total budget) so one slow hop — e.g. a gate fanning out to
        # a partly-dead DC — cannot swallow the whole budget; the
        # budget is spent across many quick sweeps instead.
        per_send_timeout = min(timeout, _CANCEL_PER_SEND_TIMEOUT_SECONDS)
        deadline = _DEFAULT_CLOCK.monotonic() + timeout

        response, last_error, sweep = await self._sweep_until_deadline(
            configured_targets,
            request,
            job_id,
            per_send_timeout,
            max_redirects,
            unreachable,
            deadline,
            retry_base_delay,
        )
        if response is not None:
            return response

        raise RuntimeError(
            f"Job cancellation failed within {timeout:.1f}s budget "
            f"({sweep + 1} sweep(s) over {len(configured_targets)} "
            f"target(s)): {last_error}"
        )

    def _cancel_targets(self, job_id: str) -> list[tuple[str, int]]:
        """The job's cancel targets, its accepting server first; raises when none are configured."""
        configured_targets = self._targets.get_targets_for_job(job_id)
        if not configured_targets:
            raise RuntimeError("No managers or gates configured")
        return configured_targets

    async def _sweep_until_deadline(
        self,
        configured_targets: list[tuple[str, int]],
        request: JobCancelRequest,
        job_id: str,
        per_send_timeout: float,
        max_redirects: int,
        unreachable: set[tuple[str, int]],
        deadline: float,
        retry_base_delay: float,
    ) -> tuple[JobCancelResponse | None, str | None, int]:
        """Sweep the fleet, backing off between sweeps: the definitive response, the last error, the sweep."""
        last_error: str | None = None
        sweep = 0

        while True:
            response, last_error, keep_sweeping = await self._sweep_then_back_off(
                configured_targets,
                request,
                job_id,
                per_send_timeout,
                max_redirects,
                unreachable,
                last_error,
                deadline,
                retry_base_delay,
                sweep,
            )
            if not keep_sweeping:
                return (response, last_error, sweep)
            sweep += 1

    async def _sweep_then_back_off(
        self,
        configured_targets: list[tuple[str, int]],
        request: JobCancelRequest,
        job_id: str,
        per_send_timeout: float,
        max_redirects: int,
        unreachable: set[tuple[str, int]],
        last_error: str | None,
        deadline: float,
        retry_base_delay: float,
        sweep: int,
    ) -> tuple[JobCancelResponse | None, str | None, bool]:
        """One sweep of the fleet, then a back-off: the response, the last error, whether to sweep again."""
        outcome = await self._sweep_targets(
            configured_targets,
            request,
            job_id,
            per_send_timeout,
            max_redirects,
            unreachable,
            last_error,
        )
        if isinstance(outcome, JobCancelResponse):
            return (outcome, last_error, False)

        return (None, outcome, await self._back_off_before_next_sweep(deadline, retry_base_delay, sweep))

    async def _sweep_targets(
        self,
        configured_targets: list[tuple[str, int]],
        request: JobCancelRequest,
        job_id: str,
        per_send_timeout: float,
        max_redirects: int,
        unreachable: set[tuple[str, int]],
        last_error: str | None,
    ) -> JobCancelResponse | str | None:
        """Try each configured target once: the first definitive response, else the last transient error."""
        # Each sweep re-tries the full fleet from scratch:
        # ``tried`` resets so redirect hints can be followed
        # afresh as leadership converges, while ``unreachable``
        # persists as accumulated ground truth.
        pending: list[tuple[str, int]] = list(configured_targets)
        tried: set[tuple[str, int]] = set()

        while pending:
            target = pending.pop(0)
            tried.add(target)

            result = await self._attempt_with_redirects(
                initial_target=target,
                request=request,
                job_id=job_id,
                timeout=per_send_timeout,
                max_redirects=max_redirects,
                tried=tried,
                unreachable=unreachable,
            )

            if isinstance(result, JobCancelResponse):
                return result

            # ``result`` is a transient error string — record it
            # and fall over to the next target in this sweep.
            last_error = result

        return last_error

    async def _back_off_before_next_sweep(
        self,
        deadline: float,
        retry_base_delay: float,
        sweep: int,
    ) -> bool:
        """Back off before the next sweep; False once the budget is spent."""
        # A full sweep produced only transient errors. Stop if
        # the budget is spent; otherwise back off (capped,
        # jittered, and never past the deadline) and sweep again.
        remaining = deadline - _DEFAULT_CLOCK.monotonic()
        if remaining <= 0:
            return False
        base = min(retry_base_delay * (2 ** min(sweep, 4)), 5.0)
        delay = base * (0.5 + _DEFAULT_RANDOM.random())
        await _DEFAULT_CLOCK.sleep(min(delay, remaining))
        return True

    async def _attempt_with_redirects(
        self,
        *,
        initial_target: tuple[str, int],
        request: JobCancelRequest,
        job_id: str,
        timeout: float,
        max_redirects: int,
        tried: set[tuple[str, int]],
        unreachable: set[tuple[str, int]],
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

        ``unreachable`` accumulates every target this attempt fails to
        reach at the network level. It is stamped onto the request
        before each send so the *next* manager the request lands on
        learns which peers the client has already proved dead — the
        signal a freshly-elected DC leader needs to take over job
        leadership from a killed prior leader without waiting for its
        own SWIM detector.
        """
        target = initial_target
        redirects_used = 0

        while True:
            outcome, redirect_target = await self._attempt_target(
                target,
                request,
                job_id,
                timeout,
                tried,
                unreachable,
                redirects_used < max_redirects,
            )
            if redirect_target is None:
                return outcome
            target = redirect_target
            redirects_used += 1

    async def _attempt_target(
        self,
        target: tuple[str, int],
        request: JobCancelRequest,
        job_id: str,
        timeout: float,
        tried: set[tuple[str, int]],
        unreachable: set[tuple[str, int]],
        may_redirect: bool,
    ) -> tuple[JobCancelResponse | str | None, tuple[str, int] | None]:
        """Send the cancel to one target: its outcome, or the leader to redirect to."""
        # Stamp the latest reachability ground truth onto the
        # request so whichever manager processes it sees every
        # peer the client has already failed to reach.
        request.unreachable_addrs = list(unreachable)
        response_data, _ = await self._send_tcp(
            target, "cancel_job", request.dump(), timeout=timeout
        )

        if isinstance(response_data, Exception):
            # Network-level failure — connection refused,
            # timeout, etc. Treat as transient; outer loop falls
            # over to the next target. Record the dead target so
            # subsequent requests carry it in ``unreachable_addrs``.
            unreachable.add(target)
            return (f"{type(response_data).__name__}: {response_data}", None)

        return await self._classify_cancel_response(job_id, response_data, tried, may_redirect)

    async def _classify_cancel_response(
        self,
        job_id: str,
        response_data: bytes,
        tried: set[tuple[str, int]],
        may_redirect: bool,
    ) -> tuple[JobCancelResponse | str | None, tuple[str, int] | None]:
        """Classify a target's reply: server error, rate limit, or a cancel response."""
        if response_data == b"error":
            return ("Server returned error", None)

        rate_limit_delay = self._check_rate_limit(response_data)
        if rate_limit_delay is not None:
            # Honor the server's retry_after; treat as transient.
            await _DEFAULT_CLOCK.sleep(rate_limit_delay)
            return ("Rate limited", None)

        return self._cancel_response_outcome(job_id, JobCancelResponse.load(response_data), tried, may_redirect)

    def _cancel_response_outcome(
        self,
        job_id: str,
        response: JobCancelResponse,
        tried: set[tuple[str, int]],
        may_redirect: bool,
    ) -> tuple[JobCancelResponse | str | None, tuple[str, int] | None]:
        """A handled response as the outcome; otherwise a redirect or the rejection's classification."""
        handled = self._handle_successful_response(job_id, response)
        if handled:
            return (handled, None)

        return self._unhandled_cancel_outcome(response, tried, may_redirect)

    def _unhandled_cancel_outcome(
        self,
        response: JobCancelResponse,
        tried: set[tuple[str, int]],
        may_redirect: bool,
    ) -> tuple[str | None, tuple[str, int] | None]:
        """Follow the response's leader redirect within budget, else classify its error."""
        # Leader redirect — follow it within budget.
        if response.leader_addr and may_redirect:
            return self._follow_cancel_redirect(response, tried)

        return (self._cancel_rejection_outcome(response), None)

    @staticmethod
    def _follow_cancel_redirect(
        response: JobCancelResponse,
        tried: set[tuple[str, int]],
    ) -> tuple[str | None, tuple[str, int] | None]:
        """The leader to redirect to, or a transient error when it was already tried."""
        redirect_target = tuple(response.leader_addr)
        if redirect_target in tried:
            # We've already tried this leader; treat the
            # redirect as a transient (the cluster is still
            # converging on a leader). Outer loop will fall
            # over.
            return (
                (
                    f"redirect cycles to already-tried target "
                    f"{redirect_target}"
                ),
                None,
            )
        tried.add(redirect_target)
        return (None, redirect_target)

    def _cancel_rejection_outcome(self, response: JobCancelResponse) -> str:
        """A transient rejection's error; a permanent rejection raises."""
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
                await self._wait_for_cancellation_event(event, timeout)
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

    async def _wait_for_cancellation_event(self, event: asyncio.Event, timeout: float | None) -> None:
        """Wait on the cancellation event, bounded by the timeout when one is given."""
        if timeout is not None:
            await _DEFAULT_CLOCK.wait_for(event.wait(), timeout=timeout)
        else:
            await event.wait()

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
