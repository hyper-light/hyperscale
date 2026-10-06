"""
TCP handlers for job and workflow cancellation operations.

Handles cancellation requests:
- Job cancellation from clients
- Single workflow cancellation
- Cancellation completion notifications
"""

import asyncio
import functools
from typing import TYPE_CHECKING, Awaitable, Callable

from hyperscale.distributed.models import (
    CancelAck,
    CancelJob,
    GlobalJobStatus,
    JobCancelRequest,
    JobCancelResponse,
    JobCancellationComplete,
    JobStatus,
    SingleWorkflowCancelRequest,
    SingleWorkflowCancelResponse,
    WorkflowCancellationStatus,
)
from hyperscale.distributed.models import RateLimitResponse
from hyperscale.distributed.reliability import (
    JitterStrategy,
    RetryConfig,
    RetryExecutor,
)
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerError,
    ServerInfo,
)

from hyperscale.distributed.nodes.gate.state import GateRuntimeState

# Prefix stamped on a cancel response when no DC confirmed the cancel,
# so the client classifies the failure as retryable. The substring
# ``leader transition`` is registered in
# ``hyperscale.distributed.nodes.client.config.TRANSIENT_ERRORS``; the
# two must stay in sync — changing this marker requires updating that
# set.
_CANCEL_RETRYABLE_MARKER = "cancellation pending leader transition"

# How far each datacenter's answer to a single-workflow cancel carries the
# gate's aggregate: a workflow still being stopped in any datacenter is not
# cancelled yet, so CANCELLING outranks every finished answer; a datacenter
# where it had already finished outranks one that never had it.
_SINGLE_WORKFLOW_CANCEL_STATUS_RANK: dict[str, int] = {
    status.value: rank
    for rank, status in enumerate(
        (
            WorkflowCancellationStatus.NOT_FOUND,
            WorkflowCancellationStatus.ALREADY_COMPLETED,
            WorkflowCancellationStatus.ALREADY_CANCELLED,
            WorkflowCancellationStatus.PENDING_CANCELLED,
            WorkflowCancellationStatus.CANCELLED,
            WorkflowCancellationStatus.CANCELLING,
        )
    )
}

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.jobs.gates import GateJobManager
    from hyperscale.distributed.taskex import TaskRunner


class GateCancellationHandler:
    """
    Handles job and workflow cancellation operations.

    Provides TCP handler methods for cancellation requests from clients
    and completion notifications from managers.
    """

    def __init__(
        self,
        state: GateRuntimeState,
        logger: Logger,
        task_runner: "TaskRunner",
        job_manager: "GateJobManager",
        datacenter_managers: dict[str, list[tuple[str, int]]],
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        check_rate_limit: Callable[[str, str], tuple[bool, float]],
        send_tcp: Callable,
        record_cancellation: Callable[
            [str, str, str, list[tuple[str, int]]], Awaitable[None]
        ],
        client_push_timeout_seconds: float,
        manager_request_timeout_seconds: float,
    ) -> None:
        """
        Initialize the cancellation handler.

        Args:
            state: Runtime state container
            logger: Async logger instance
            task_runner: Background task executor
            job_manager: Job management service
            datacenter_managers: DC -> manager addresses mapping
            get_node_id: Callback to get this gate's node ID
            get_host: Callback to get this gate's host
            get_tcp_port: Callback to get this gate's TCP port
            check_rate_limit: Callback to check rate limit
            send_tcp: Callback to send TCP messages
            record_cancellation: Durably records a confirmed cancel as
                (job_id, reason, requester_id, [(datacenter, cancelled)])
                — AD-38 JobCancellationRequested + one JobCancellationAcked
                per confirming datacenter
        """
        self._state: GateRuntimeState = state
        self._logger: Logger = logger
        self._task_runner: "TaskRunner" = task_runner
        self._job_manager: "GateJobManager" = job_manager
        self._datacenter_managers: dict[str, list[tuple[str, int]]] = (
            datacenter_managers
        )
        self._get_node_id: Callable[[], "NodeId"] = get_node_id
        self._get_host: Callable[[], str] = get_host
        self._get_tcp_port: Callable[[], int] = get_tcp_port
        self._record_cancellation: Callable[
            [str, str, str, list[tuple[str, int]]], Awaitable[None]
        ] = record_cancellation
        self._check_rate_limit: Callable[[str, str], tuple[bool, float]] = (
            check_rate_limit
        )
        self._send_tcp: Callable = send_tcp
        self._client_push_timeout_seconds: float = client_push_timeout_seconds
        self._manager_request_timeout_seconds: float = manager_request_timeout_seconds

    def _build_cancel_response(
        self,
        use_ad20: bool,
        job_id: str,
        success: bool,
        error: str | None = None,
        cancelled_count: int = 0,
        already_cancelled: bool = False,
        already_completed: bool = False,
    ) -> bytes:
        """Build cancel response in appropriate format (AD-20 or legacy)."""
        if use_ad20:
            return JobCancelResponse(
                job_id=job_id,
                success=success,
                error=error,
                cancelled_workflow_count=cancelled_count,
                already_cancelled=already_cancelled,
                already_completed=already_completed,
            ).dump()
        return CancelAck(
            job_id=job_id,
            cancelled=success,
            error=error,
            workflows_cancelled=cancelled_count,
        ).dump()

    def _is_ad20_cancel_request(self, data: bytes) -> bool:
        """Check if cancel request data is AD-20 format."""
        try:
            JobCancelRequest.load(data)
            return True
        except Exception:
            return False

    def _cancellation_target_datacenters(self, job_id: str) -> list[str]:
        """The datacenters a cancel must reach: every DC the job was
        dispatched to, whatever its current health -- a DC that is
        momentarily unhealthy may still be running the job, and a DC
        the job never ran in has nothing to cancel. A job with no
        recorded targets falls back to every known DC."""
        target_datacenters = self._job_manager.get_target_dcs(job_id)
        return sorted(target_datacenters or self._datacenter_managers.keys())

    async def handle_cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle job cancellation from client (AD-20).

        Supports both legacy CancelJob and new JobCancelRequest formats.
        Uses retry logic with exponential backoff when forwarding to managers.

        Args:
            addr: Client address
            data: Serialized cancel request
            handle_exception: Callback for exception handling

        Returns:
            Serialized cancel response
        """
        try:
            return await self._cancel_job_for_client(addr, data)

        except Exception as error:
            await handle_exception(error, "cancel_job")
            is_ad20 = self._is_ad20_cancel_request(data)
            return self._build_cancel_response(
                is_ad20, "unknown", success=False, error=str(error)
            )

    async def _cancel_job_for_client(self, addr: tuple[str, int], data: bytes) -> bytes:
        """Cancel a job the client's rate limit admits, unless the cancel is
        refused or the job already ended."""
        client_id = f"{addr[0]}:{addr[1]}"
        allowed, retry_after = await self._check_rate_limit(client_id, "cancel")
        if not allowed:
            return RateLimitResponse(
                operation="cancel",
                retry_after_seconds=retry_after,
            ).dump()

        job_id, fence_token, requester_id, reason, timestamp, use_ad20 = self._parse_cancel_request(addr, data)

        job = self._job_manager.get_job(job_id)
        if (refusal := self._cancel_refusal(job, job_id, fence_token, use_ad20)) is not None:
            return refusal

        return await self._cancel_job_across_datacenters(
            job, job_id, fence_token, requester_id, reason, timestamp, use_ad20
        )

    @staticmethod
    def _parse_cancel_request(
        addr: tuple[str, int],
        data: bytes,
    ) -> tuple[str, int, str, str, float, bool]:
        """Read an AD-20 ``JobCancelRequest``, else a legacy ``CancelJob``:
        (job id, fence token, requester, reason, timestamp, is AD-20)."""
        try:
            cancel_request = JobCancelRequest.load(data)
            return (
                cancel_request.job_id,
                cancel_request.fence_token,
                cancel_request.requester_id,
                cancel_request.reason,
                cancel_request.timestamp,
                True,
            )
        except Exception:
            cancel = CancelJob.load(data)
            return (
                cancel.job_id,
                cancel.fence_token,
                f"{addr[0]}:{addr[1]}",
                cancel.reason,
                0.0,
                False,
            )

    def _cancel_refusal(
        self,
        job: GlobalJobStatus | None,
        job_id: str,
        fence_token: int,
        use_ad20: bool,
    ) -> bytes | None:
        """The answer to a cancel of an unknown job or under a stale fence
        token, or of a job already ended; None to cancel it."""
        if not job:
            return self._build_cancel_response(
                use_ad20, job_id, success=False, error="Job not found"
            )

        if self._fence_token_mismatch(job, fence_token):
            error_msg = f"Fence token mismatch: expected {job.fence_token}, got {fence_token}"
            return self._build_cancel_response(
                use_ad20, job_id, success=False, error=error_msg
            )

        return self._ended_job_cancel_answer(job, job_id, use_ad20)

    @staticmethod
    def _fence_token_mismatch(job: GlobalJobStatus, fence_token: int) -> bool:
        """Whether the cancel carries a fence token other than the job's."""
        return (
            fence_token > 0
            and hasattr(job, "fence_token")
            and job.fence_token != fence_token
        )

    def _ended_job_cancel_answer(
        self,
        job: GlobalJobStatus,
        job_id: str,
        use_ad20: bool,
    ) -> bytes | None:
        """The answer to a cancel of a job already cancelled or completed."""
        if job.status == JobStatus.CANCELLED.value:
            return self._build_cancel_response(
                use_ad20, job_id, success=True, already_cancelled=True
            )

        if job.status == JobStatus.COMPLETED.value:
            return self._build_cancel_response(
                use_ad20,
                job_id,
                success=False,
                already_completed=True,
                error="Job already completed",
            )

        return None

    async def _cancel_job_across_datacenters(
        self,
        job: GlobalJobStatus,
        job_id: str,
        fence_token: int,
        requester_id: str,
        reason: str,
        timestamp: float,
        use_ad20: bool,
    ) -> bytes:
        """Forward the cancel to every target datacenter, mark the job
        CANCELLED once one confirmed, and answer the client."""
        # Fail-fast forward: try each manager once (no per-manager
        # connection retry) so a full DC sweep — even against a
        # partly-dead DC mid-failover — returns to the client well
        # inside the client's per-send timeout. Robustness against
        # the failover convergence window comes from the *client*
        # re-issuing the cancel across its total time budget (see
        # ``ClientCancellationManager.cancel_job``), not from the
        # gate blocking on internal retries. Retrying here instead
        # would stack per-manager backoff into a multi-tens-of-
        # seconds round-trip that the client would time out on,
        # which is exactly what stranded gate-routed cancels during
        # a manager-leader failover.
        retry_config = RetryConfig(
            max_attempts=1,
            base_delay=0.5,
            max_delay=5.0,
            jitter=JitterStrategy.FULL,
            retryable_exceptions=(ConnectionError, TimeoutError, OSError),
        )

        errors: list[str] = []
        confirmed_datacenters: list[tuple[str, int]] = []
        cancelled_workflows = 0
        for dc in self._cancellation_target_datacenters(job_id):
            cancelled_workflows += await self._cancel_job_in_datacenter(
                dc,
                use_ad20=use_ad20,
                job_id=job_id,
                requester_id=requester_id,
                fence_token=fence_token,
                reason=reason,
                timestamp=timestamp,
                retry_config=retry_config,
                errors=errors,
                confirmed_datacenters=confirmed_datacenters,
            )
        # A DC confirmed exactly when it is among the confirmed ones.
        any_dc_confirmed = bool(confirmed_datacenters)

        # Only mark the job CANCELLED locally when at least one DC
        # confirmed it actually cancelled (or the job was already
        # terminal there). Flipping the status to CANCELLED while
        # every DC forward failed — the prior behavior — reports a
        # false success to the client and desyncs the gate's view
        # from the managers that are still running the workflows.
        if any_dc_confirmed:
            await self._record_cancellation(
                job_id, reason, requester_id, confirmed_datacenters
            )
            job.status = JobStatus.CANCELLED.value
            await self._state.increment_state_version()

        return self._build_cancel_response(
            use_ad20,
            job_id,
            success=any_dc_confirmed,
            cancelled_count=cancelled_workflows,
            error=self._cancel_error_string(any_dc_confirmed, errors),
        )

    async def _cancel_job_in_datacenter(
        self,
        dc: str,
        *,
        use_ad20: bool,
        job_id: str,
        requester_id: str,
        fence_token: int,
        reason: str,
        timestamp: float,
        retry_config: RetryConfig,
        errors: list[str],
        confirmed_datacenters: list[tuple[str, int]],
    ) -> int:
        """Forward the cancel to one DC, recording whether it confirmed and
        its error; returns the workflows it cancelled."""
        managers = self._datacenter_managers.get(dc, [])
        dc_cancelled_count, dc_confirmed, dc_error = (
            await self._cancel_job_in_dc_with_redirects(
                dc=dc,
                managers=managers,
                use_ad20=use_ad20,
                job_id=job_id,
                requester_id=requester_id,
                fence_token=fence_token,
                reason=reason,
                timestamp=timestamp,
                retry_config=retry_config,
            )
        )
        if dc_confirmed:
            confirmed_datacenters.append((dc, dc_cancelled_count))
        if dc_error:
            errors.append(f"DC {dc}: {dc_error}")
        return dc_cancelled_count

    @staticmethod
    def _cancel_error_string(any_dc_confirmed: bool, errors: list[str]) -> str | None:
        """The error a cancel answers with."""
        # When no DC confirmed the cancel, the response is a
        # non-success that the client must be able to RETRY: during
        # a manager-leader failover every DC transiently returns
        # "not job leader" / unreachable until leadership
        # reconverges. Prefix the aggregate error with an
        # explicit transient marker so the client classifies it as
        # retryable (see ``TRANSIENT_ERRORS``) and re-issues the
        # cancel across its time budget instead of giving up. A
        # confirmed cancel carries the raw per-DC detail (which may
        # still include a non-fatal error from a *different* DC).
        if any_dc_confirmed:
            return "; ".join(errors) if errors else None
        return GateCancellationHandler._retryable_cancel_error(errors)

    @staticmethod
    def _retryable_cancel_error(errors: list[str]) -> str:
        """An unconfirmed cancel's error, marked retryable for the client."""
        detail = "; ".join(errors) if errors else "no DC confirmed"
        return f"{_CANCEL_RETRYABLE_MARKER}: {detail}"

    async def _cancel_job_in_dc_with_redirects(
        self,
        *,
        dc: str,
        managers: list[tuple[str, int]],
        use_ad20: bool,
        job_id: str,
        requester_id: str,
        fence_token: int,
        reason: str,
        timestamp: float,
        retry_config: RetryConfig,
        max_redirects: int = 3,
    ) -> tuple[int, bool, str | None]:
        """Forward a cancel to one DC, following the manager's leader
        redirects until a manager confirms cancellation.

        Returns ``(cancelled_count, confirmed, error)``. ``confirmed``
        is True when a manager reported the job cancelled, already
        cancelled, or already completed — i.e. a definitive terminal
        answer, not a redirect or a transport failure.

        Why this exists: the gate must reach the DC's *actual* job
        leader. A manager that isn't the job leader answers
        ``JobCancelResponse(success=False, leader_addr=X)``. The prior
        implementation accepted any parseable response as "DC done",
        so if the gate's cached manager list didn't happen to put the
        leader first — the common case right after a manager-leader
        failover — the cancel silently under-cancelled and the gate
        reported a false success. We now follow the redirect and try
        alternate managers, exactly like the client's own
        ``ClientCancellationManager._attempt_with_redirects``.

        Two pieces of context ride along on every forwarded
        ``JobCancelRequest``:

        * ``callback_addr`` = this gate's address. A manager that takes
          over job leadership after a failover may never have inherited
          the job's callback (the ``_broadcast_job_leadership`` fan-out
          is fire-and-forget and is readily dropped when a leader dies
          mid-send). Carrying the gate address lets the new leader push
          ``job_cancellation_complete`` back to us to forward to the
          client, so the client's ``await_job_cancellation`` unblocks.

        * ``unreachable_addrs`` = managers we've already failed to
          reach this attempt, so the manager's redirect resolver never
          bounces us to a peer we've proved dead.
        """
        build_cancel_data = functools.partial(
            self._build_forward_cancel_data,
            use_ad20=use_ad20,
            job_id=job_id,
            requester_id=requester_id,
            fence_token=fence_token,
            reason=reason,
            timestamp=timestamp,
            callback_addr=(self._get_host(), self._get_tcp_port()),
        )
        pending: list[tuple[str, int]] = list(managers)
        tried: set[tuple[str, int]] = set()
        unreachable: set[tuple[str, int]] = set()
        # The redirects followed, and the errors met, in order: the last
        # one is the DC's error when no manager confirms.
        followed_redirects: list[tuple[str, int]] = []
        manager_errors: list[str] = []

        while pending:
            if (
                confirmed_result := await self._try_next_manager(
                    dc,
                    pending,
                    tried,
                    unreachable,
                    followed_redirects,
                    manager_errors,
                    build_cancel_data,
                    retry_config,
                    max_redirects,
                )
            ) is not None:
                return confirmed_result

        return 0, False, self._last_cancel_error(manager_errors)

    @staticmethod
    def _last_cancel_error(manager_errors: list[str]) -> str:
        """The last error a DC's managers gave, or a generic one."""
        return (manager_errors[-1] if manager_errors else None) or "no manager confirmed cancellation"

    async def _try_next_manager(
        self,
        dc: str,
        pending: list[tuple[str, int]],
        tried: set[tuple[str, int]],
        unreachable: set[tuple[str, int]],
        followed_redirects: list[tuple[str, int]],
        manager_errors: list[str],
        build_cancel_data: Callable[..., bytes],
        retry_config: RetryConfig,
        max_redirects: int,
    ) -> tuple[int, bool, None] | None:
        """Ask the next queued manager not tried yet; the DC's result once
        it confirms, else None."""
        target = tuple(pending.pop(0))
        if target in tried:
            return None
        tried.add(target)

        return await self._ask_manager_to_cancel(
            dc,
            target,
            pending,
            tried,
            unreachable,
            followed_redirects,
            manager_errors,
            build_cancel_data,
            retry_config,
            max_redirects,
        )

    async def _ask_manager_to_cancel(
        self,
        dc: str,
        target: tuple[str, int],
        pending: list[tuple[str, int]],
        tried: set[tuple[str, int]],
        unreachable: set[tuple[str, int]],
        followed_redirects: list[tuple[str, int]],
        manager_errors: list[str],
        build_cancel_data: Callable[..., bytes],
        retry_config: RetryConfig,
        max_redirects: int,
    ) -> tuple[int, bool, None] | None:
        """Forward the cancel to one manager; the DC's result once it
        confirms, else None (an unreachable manager is recorded)."""
        cancel_data = build_cancel_data(unreachable_addrs=sorted(unreachable))

        retry_executor = RetryExecutor(retry_config)
        try:
            response = await retry_executor.execute(
                lambda addr=target, payload=cancel_data: self._forward_cancel(
                    addr, payload
                ),
                operation_name=f"cancel_job_dc_{dc}",
            )
        except Exception as error:
            # Exhausted connection retries — this manager is
            # unreachable. Record it so later requests in this DC
            # carry it in ``unreachable_addrs``, then fall over.
            unreachable.add(target)
            manager_errors.append(str(error))
            return None

        if not isinstance(response, bytes):
            manager_errors.append("no response from manager")
            return None

        return self._settle_manager_cancel_answer(
            response, pending, tried, followed_redirects, manager_errors, max_redirects
        )

    def _settle_manager_cancel_answer(
        self,
        response: bytes,
        pending: list[tuple[str, int]],
        tried: set[tuple[str, int]],
        followed_redirects: list[tuple[str, int]],
        manager_errors: list[str],
        max_redirects: int,
    ) -> tuple[int, bool, None] | None:
        """The DC's result on a confirmed cancel; else queue the redirect
        to follow first, or record the transient error."""
        confirmed, cancelled_count, leader_addr, transient_error = (
            self._interpret_manager_cancel_response(response)
        )
        if confirmed:
            return cancelled_count, True, None

        if self._should_follow_redirect(leader_addr, followed_redirects, max_redirects, tried):
            # Honor the manager's leader hint: try it next.
            pending.insert(0, tuple(leader_addr))
            followed_redirects.append(tuple(leader_addr))
            return None

        self._record_transient_cancel_error(transient_error, manager_errors)
        return None

    @staticmethod
    def _record_transient_cancel_error(transient_error: str | None, manager_errors: list[str]) -> None:
        """Record a manager's non-terminal error, when it gave one."""
        if transient_error:
            manager_errors.append(transient_error)

    @staticmethod
    def _should_follow_redirect(
        leader_addr: tuple[str, int] | None,
        followed_redirects: list[tuple[str, int]],
        max_redirects: int,
        tried: set[tuple[str, int]],
    ) -> bool:
        """Whether to follow a redirect: one was given, the budget allows
        it, and its manager was not tried yet."""
        return (
            leader_addr is not None
            and len(followed_redirects) < max_redirects
            and tuple(leader_addr) not in tried
        )

    def _build_forward_cancel_data(
        self,
        *,
        use_ad20: bool,
        job_id: str,
        requester_id: str,
        fence_token: int,
        reason: str,
        timestamp: float,
        callback_addr: tuple[str, int],
        unreachable_addrs: list[tuple[str, int]],
    ) -> bytes:
        """Serialize the manager-bound cancel request.

        AD-20 clients get a ``JobCancelRequest`` carrying the failover
        context (``callback_addr`` / ``unreachable_addrs``); legacy
        clients get the minimal ``CancelJob``, which has no fields for
        that context — legacy deployments simply forgo the
        failover-hardening, matching their pre-existing behavior.
        """
        if use_ad20:
            return JobCancelRequest(
                job_id=job_id,
                requester_id=requester_id,
                timestamp=timestamp,
                fence_token=fence_token,
                reason=reason,
                callback_addr=callback_addr,
                unreachable_addrs=unreachable_addrs,
            ).dump()
        return CancelJob(
            job_id=job_id,
            reason=reason,
            fence_token=fence_token,
        ).dump()

    # Per-manager forward timeout. Short so a full fail-fast DC sweep
    # (this timeout × manager count + a redirect hop or two) stays
    # inside the client's per-send budget; the client's total-budget
    # retry, not a long gate timeout, is what spans a failover.
    _FORWARD_TIMEOUT_SECONDS = 3.0

    async def _forward_cancel(
        self,
        manager_addr: tuple[str, int],
        cancel_data: bytes,
    ) -> bytes | None:
        """Send one cancel request to a manager and return the raw
        response bytes (or raise for the retry executor)."""
        response, _ = await self._send_tcp(
            manager_addr,
            "cancel_job",
            cancel_data,
            timeout=self._FORWARD_TIMEOUT_SECONDS,
        )
        # send_tcp returns transport errors rather than raising.
        if isinstance(response, Exception):
            raise response
        return response

    def _interpret_manager_cancel_response(
        self,
        response: bytes,
    ) -> tuple[bool, int, tuple[str, int] | None, str | None]:
        """Classify a manager's cancel response.

        Returns ``(confirmed, cancelled_count, leader_addr,
        transient_error)``:

        * ``confirmed`` — the manager gave a definitive terminal
          answer (cancelled / already cancelled / already completed).
        * ``leader_addr`` — a redirect hint to follow when not
          confirmed.
        * ``transient_error`` — a non-terminal error worth recording
          for diagnostics when neither confirmed nor redirected.

        The manager always answers with ``JobCancelResponse`` (its
        ``_build_cancel_response`` returns that shape regardless of the
        request format); the ``CancelAck`` fallback covers any legacy
        peer that still replies in the old shape. ``JobCancelResponse``
        is tried first because ``Message.load`` is deliberately lax
        about the concrete type.
        """
        try:
            return self._classify_job_cancel_response(JobCancelResponse.load(response))
        except Exception:
            return self._classify_cancel_ack(CancelAck.load(response))

    def _classify_job_cancel_response(
        self,
        parsed: JobCancelResponse,
    ) -> tuple[bool, int, tuple[str, int] | None, str | None]:
        """Classify an AD-20 ``JobCancelResponse``."""
        confirmed = self._cancel_confirmed(parsed)
        leader_addr = self._redirect_addr(parsed)
        transient_error = None if confirmed else parsed.error
        return (
            confirmed,
            parsed.cancelled_workflow_count,
            leader_addr,
            transient_error,
        )

    @staticmethod
    def _cancel_confirmed(parsed: JobCancelResponse) -> bool:
        """Whether the answer is terminal: cancelled, already cancelled or
        already completed."""
        return (
            parsed.success
            or parsed.already_cancelled
            or parsed.already_completed
        )

    @staticmethod
    def _redirect_addr(parsed: JobCancelResponse) -> tuple[str, int] | None:
        """The job leader a manager redirects to, if any."""
        return (
            tuple(parsed.leader_addr)
            if parsed.leader_addr is not None
            else None
        )

    @staticmethod
    def _classify_cancel_ack(ack: CancelAck) -> tuple[bool, int, tuple[str, int] | None, str | None]:
        """Classify a legacy ``CancelAck``."""
        return (
            ack.cancelled,
            ack.workflows_cancelled,
            None,
            None if ack.cancelled else ack.error,
        )

    async def handle_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        try:
            completion = JobCancellationComplete.load(data)
            job_id = completion.job_id

            await self._logger.log(
                ServerInfo(
                    message=f"Received job cancellation complete for {job_id[:8]}... "
                    f"(success={completion.success}, errors={len(completion.errors)})",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

            callback = self._job_manager.get_callback(job_id)
            if callback:
                self._task_runner.run(
                    self._push_cancellation_complete_to_client,
                    job_id,
                    completion,
                    callback,
                )

            return b"OK"

        except Exception as error:
            await handle_exception(error, "job_cancellation_complete")
            return b"ERROR"

    async def _push_cancellation_complete_to_client(
        self,
        job_id: str,
        completion: JobCancellationComplete,
        callback: tuple[str, int],
    ) -> None:
        """Push job cancellation completion to client callback."""
        try:
            # Action name aligned with the client's
            # @tcp.receive() handler (``job_cancellation_complete``).
            # A prior incarnation sent ``receive_job_cancellation_complete``
            # which silently mismatched the client's registered handler.
            response, _ = await self._send_tcp(
                callback,
                "job_cancellation_complete",
                completion.dump(),
                timeout=self._client_push_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response, Exception):
                raise response
        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Failed to push cancellation complete to client {callback}: {error}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

    async def handle_cancel_single_workflow(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle single workflow cancellation request from client (Section 6).

        Gates forward workflow cancellation requests to all datacenters
        that have the job, then aggregate responses.

        Args:
            addr: Client address
            data: Serialized SingleWorkflowCancelRequest
            handle_exception: Callback for exception handling

        Returns:
            Serialized SingleWorkflowCancelResponse
        """
        try:
            return await self._cancel_single_workflow(addr, data)

        except Exception as error:
            await handle_exception(error, "receive_cancel_single_workflow")
            return SingleWorkflowCancelResponse(
                job_id="unknown",
                workflow_id="unknown",
                request_id="unknown",
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=[str(error)],
            ).dump()

    async def _cancel_single_workflow(self, addr: tuple[str, int], data: bytes) -> bytes:
        """Cancel one workflow the client's rate limit admits (Section 6)."""
        request = SingleWorkflowCancelRequest.load(data)

        client_id = f"{addr[0]}:{addr[1]}"
        allowed, retry_after = await self._check_rate_limit(
            client_id, "cancel_workflow"
        )
        if not allowed:
            return RateLimitResponse(
                operation="cancel_workflow",
                retry_after_seconds=retry_after,
            ).dump()

        await self._logger.log(
            ServerInfo(
                message=f"Received workflow cancellation request for {request.workflow_id[:8]}... "
                f"(job {request.job_id[:8]}...)",
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            )
        )

        return await self._cancel_workflow_of_known_job(request)

    async def _cancel_workflow_of_known_job(self, request: SingleWorkflowCancelRequest) -> bytes:
        """Cancel the workflow in each datacenter its job was dispatched to;
        NOT_FOUND for an unknown job or one with no datacenter to ask."""
        job_info = self._job_manager.get_job(request.job_id)
        if not job_info:
            return SingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=["Job not found"],
            ).dump()

        target_dcs = self._single_workflow_cancel_targets(request.job_id)

        if not target_dcs:
            return SingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=["No datacenters available"],
            ).dump()

        return await self._aggregate_single_workflow_cancel(request, target_dcs)

    def _single_workflow_cancel_targets(self, job_id: str) -> list[str]:
        """The job's cancellation targets that have managers to ask."""
        # The datacenters the job was dispatched to (as a job cancel
        # reaches), each through its first manager that answers: a
        # manager that is not the job's leader forwards to it.
        return [
            dc_name
            for dc_name in self._cancellation_target_datacenters(job_id)
            if self._datacenter_managers.get(dc_name)
        ]

    async def _aggregate_single_workflow_cancel(
        self,
        request: SingleWorkflowCancelRequest,
        target_dcs: list[str],
    ) -> bytes:
        """Ask each target datacenter to cancel the workflow and aggregate
        the answers: the furthest-ranked status, every cancelled dependent
        and every error."""
        aggregated_dependents: list[str] = []
        aggregated_errors: list[str] = []
        final_status = WorkflowCancellationStatus.NOT_FOUND.value
        request_data = request.dump()

        for dc_name in target_dcs:
            final_status = await self._cancel_workflow_in_datacenter(
                dc_name, request_data, final_status, aggregated_dependents, aggregated_errors
            )

        return SingleWorkflowCancelResponse(
            job_id=request.job_id,
            workflow_id=request.workflow_id,
            request_id=request.request_id,
            status=final_status,
            cancelled_dependents=list(set(aggregated_dependents)),
            errors=aggregated_errors,
        ).dump()

    async def _cancel_workflow_in_datacenter(
        self,
        dc_name: str,
        request_data: bytes,
        final_status: str,
        aggregated_dependents: list[str],
        aggregated_errors: list[str],
    ) -> str:
        """Ask the datacenter's managers in turn until one answers, folding
        its answer in; returns the aggregate status. Unreachable managers'
        errors count only when none answered."""
        unreachable_manager_errors: list[str] = []
        for manager_addr in self._datacenter_managers[dc_name]:
            reached, response_data = await self._send_single_workflow_cancel(
                dc_name, manager_addr, request_data, unreachable_manager_errors
            )
            if not reached:
                continue

            final_status = self._merge_single_workflow_answer(
                response_data, final_status, aggregated_dependents, aggregated_errors
            )
            unreachable_manager_errors.clear()
            break

        aggregated_errors.extend(unreachable_manager_errors)
        return final_status

    async def _send_single_workflow_cancel(
        self,
        dc_name: str,
        manager_addr: tuple[str, int],
        request_data: bytes,
        unreachable_manager_errors: list[str],
    ) -> tuple[bool, bytes | None]:
        """Send the cancel to one manager; whether it was reached, and its
        answer. An unreachable manager's error is recorded."""
        try:
            response_data, _ = await self._send_tcp(
                manager_addr,
                "receive_cancel_single_workflow",
                request_data,
                timeout=self._manager_request_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response_data, Exception):
                raise response_data
        except Exception as error:
            unreachable_manager_errors.append(f"DC {dc_name} manager {manager_addr}: {error}")
            return False, None

        return True, response_data

    @staticmethod
    def _merge_single_workflow_answer(
        response_data: bytes | None,
        final_status: str,
        aggregated_dependents: list[str],
        aggregated_errors: list[str],
    ) -> str:
        """Fold a manager's answer into the aggregate; returns the status
        the aggregate carries now."""
        if not response_data:
            return final_status

        response = SingleWorkflowCancelResponse.load(response_data)
        aggregated_dependents.extend(response.cancelled_dependents)
        aggregated_errors.extend(response.errors)
        return (
            response.status
            if _SINGLE_WORKFLOW_CANCEL_STATUS_RANK[response.status]
            > _SINGLE_WORKFLOW_CANCEL_STATUS_RANK[final_status]
            else final_status
        )
