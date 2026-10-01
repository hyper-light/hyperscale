"""
TCP handlers for job and workflow cancellation operations.

Handles cancellation requests:
- Job cancellation from clients
- Single workflow cancellation
- Cancellation completion notifications
"""

import asyncio
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
        get_available_datacenters: Callable[[], list[str]],
        record_cancellation: Callable[
            [str, str, str, list[tuple[str, int]]], Awaitable[None]
        ],
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
            get_available_datacenters: Callback to get available DCs
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
        self._get_available_datacenters: Callable[[], list[str]] = (
            get_available_datacenters
        )

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
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit(client_id, "cancel")
            if not allowed:
                return RateLimitResponse(
                    operation="cancel",
                    retry_after_seconds=retry_after,
                ).dump()

            timestamp: float = 0.0
            try:
                cancel_request = JobCancelRequest.load(data)
                job_id = cancel_request.job_id
                fence_token = cancel_request.fence_token
                requester_id = cancel_request.requester_id
                reason = cancel_request.reason
                timestamp = cancel_request.timestamp
                use_ad20 = True
            except Exception:
                cancel = CancelJob.load(data)
                job_id = cancel.job_id
                fence_token = cancel.fence_token
                requester_id = f"{addr[0]}:{addr[1]}"
                reason = cancel.reason
                use_ad20 = False

            job = self._job_manager.get_job(job_id)
            if not job:
                return self._build_cancel_response(
                    use_ad20, job_id, success=False, error="Job not found"
                )

            if (
                fence_token > 0
                and hasattr(job, "fence_token")
                and job.fence_token != fence_token
            ):
                error_msg = f"Fence token mismatch: expected {job.fence_token}, got {fence_token}"
                return self._build_cancel_response(
                    use_ad20, job_id, success=False, error=error_msg
                )

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

            cancelled_workflows = 0
            errors: list[str] = []
            any_dc_confirmed = False
            confirmed_datacenters: list[tuple[str, int]] = []

            for dc in self._get_available_datacenters():
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
                cancelled_workflows += dc_cancelled_count
                any_dc_confirmed = any_dc_confirmed or dc_confirmed
                if dc_confirmed:
                    confirmed_datacenters.append((dc, dc_cancelled_count))
                if dc_error:
                    errors.append(f"DC {dc}: {dc_error}")

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
                error_str = "; ".join(errors) if errors else None
            else:
                detail = "; ".join(errors) if errors else "no DC confirmed"
                error_str = f"{_CANCEL_RETRYABLE_MARKER}: {detail}"
            return self._build_cancel_response(
                use_ad20,
                job_id,
                success=any_dc_confirmed,
                cancelled_count=cancelled_workflows,
                error=error_str,
            )

        except Exception as error:
            await handle_exception(error, "cancel_job")
            is_ad20 = self._is_ad20_cancel_request(data)
            return self._build_cancel_response(
                is_ad20, "unknown", success=False, error=str(error)
            )

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
        gate_callback = (self._get_host(), self._get_tcp_port())
        pending: list[tuple[str, int]] = list(managers)
        tried: set[tuple[str, int]] = set()
        unreachable: set[tuple[str, int]] = set()
        redirects_used = 0
        last_error: str | None = None

        while pending:
            target = tuple(pending.pop(0))
            if target in tried:
                continue
            tried.add(target)

            cancel_data = self._build_forward_cancel_data(
                use_ad20=use_ad20,
                job_id=job_id,
                requester_id=requester_id,
                fence_token=fence_token,
                reason=reason,
                timestamp=timestamp,
                callback_addr=gate_callback,
                unreachable_addrs=sorted(unreachable),
            )

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
                last_error = str(error)
                continue

            if not isinstance(response, bytes):
                last_error = "no response from manager"
                continue

            confirmed, cancelled_count, leader_addr, transient_error = (
                self._interpret_manager_cancel_response(response)
            )
            if confirmed:
                return cancelled_count, True, None

            if (
                leader_addr is not None
                and redirects_used < max_redirects
                and tuple(leader_addr) not in tried
            ):
                # Honor the manager's leader hint: try it next.
                pending.insert(0, tuple(leader_addr))
                redirects_used += 1
                continue

            if transient_error:
                last_error = transient_error

        return 0, False, last_error or "no manager confirmed cancellation"

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
            parsed = JobCancelResponse.load(response)
            confirmed = (
                parsed.success
                or parsed.already_cancelled
                or parsed.already_completed
            )
            leader_addr = (
                tuple(parsed.leader_addr)
                if parsed.leader_addr is not None
                else None
            )
            transient_error = None if confirmed else parsed.error
            return (
                confirmed,
                parsed.cancelled_workflow_count,
                leader_addr,
                transient_error,
            )
        except Exception:
            ack = CancelAck.load(response)
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

            if completion.errors:
                self._state._cancellation_errors[job_id].extend(completion.errors)

            event = self._state._cancellation_completion_events.get(job_id)
            if event:
                event.set()

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
            await self._send_tcp(
                callback,
                "job_cancellation_complete",
                completion.dump(),
                timeout=2.0,
            )
        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Failed to push cancellation complete to client {callback}: {error}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

        self._state._cancellation_completion_events.pop(job_id, None)
        self._state._cancellation_errors.pop(job_id, None)

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

            job_info = self._job_manager.get_job(request.job_id)
            if not job_info:
                return SingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=WorkflowCancellationStatus.NOT_FOUND.value,
                    errors=["Job not found"],
                ).dump()

            target_dcs: list[tuple[str, tuple[str, int]]] = []
            for dc_name, dc_managers in self._datacenter_managers.items():
                if dc_managers:
                    target_dcs.append((dc_name, dc_managers[0]))

            if not target_dcs:
                return SingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=WorkflowCancellationStatus.NOT_FOUND.value,
                    errors=["No datacenters available"],
                ).dump()

            aggregated_dependents: list[str] = []
            aggregated_errors: list[str] = []
            final_status = WorkflowCancellationStatus.NOT_FOUND.value

            for dc_name, dc_addr in target_dcs:
                try:
                    response_data, _ = await self._send_tcp(
                        dc_addr,
                        "receive_cancel_single_workflow",
                        request.dump(),
                        timeout=5.0,
                    )

                    if response_data:
                        response = SingleWorkflowCancelResponse.load(response_data)

                        aggregated_dependents.extend(response.cancelled_dependents)
                        aggregated_errors.extend(response.errors)

                        if (
                            response.status
                            == WorkflowCancellationStatus.CANCELLED.value
                        ):
                            final_status = WorkflowCancellationStatus.CANCELLED.value
                        elif (
                            response.status
                            == WorkflowCancellationStatus.PENDING_CANCELLED.value
                        ):
                            if (
                                final_status
                                == WorkflowCancellationStatus.NOT_FOUND.value
                            ):
                                final_status = (
                                    WorkflowCancellationStatus.PENDING_CANCELLED.value
                                )
                        elif (
                            response.status
                            == WorkflowCancellationStatus.ALREADY_CANCELLED.value
                        ):
                            if (
                                final_status
                                == WorkflowCancellationStatus.NOT_FOUND.value
                            ):
                                final_status = (
                                    WorkflowCancellationStatus.ALREADY_CANCELLED.value
                                )

                except Exception as error:
                    aggregated_errors.append(f"DC {dc_name}: {error}")

            return SingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=final_status,
                cancelled_dependents=list(set(aggregated_dependents)),
                errors=aggregated_errors,
            ).dump()

        except Exception as error:
            await handle_exception(error, "receive_cancel_single_workflow")
            return SingleWorkflowCancelResponse(
                job_id="unknown",
                workflow_id="unknown",
                request_id="unknown",
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=[str(error)],
            ).dump()
