"""
Manager job and workflow cancellation (AD-20).

The single implementation of the manager's cancellation protocol: the
client/gate cancel_job request (legacy CancelJob and AD-20
JobCancelRequest), job-leader fencing and redirection, cancelling
pending and running workflows, the worker-side completion push, single
workflow cancellation and the peer notifications -- with the AD-38
ledger records and AD-26 extension outcomes a cancellation produces.
The server's TCP handlers delegate here.
"""

import asyncio
from typing import TYPE_CHECKING, Awaitable, Callable

from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeKind
from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.models import (
    CancelJob,
    CancelJobWorkflowsRequest,
    CancelJobWorkflowsResponse,
    CancelledWorkflowInfo,
    JobCancellationComplete,
    JobCancelRequest,
    JobCancelResponse,
    JobInfo,
    JobStatus,
    RateLimitResponse,
    SingleWorkflowCancelRequest,
    SingleWorkflowCancelResponse,
    WorkflowCancellationComplete,
    WorkflowCancellationPeerNotification,
    WorkflowCancellationQuery,
    WorkflowCancellationResponse,
    WorkflowCancellationStatus,
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    WorkflowStatus,
)
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerInfo, ServerWarning

from .models import ParsedCancelRequest

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env
    from hyperscale.distributed.jobs import JobManager, WorkflowDispatcher
    from hyperscale.distributed.ledger.job_ledger import JobLedger
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.nodes.manager.leases import ManagerLeaseCoordinator
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.reliability.rate_limiting import ServerRateLimiter
    from hyperscale.distributed.runtime import Clock
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


class ManagerCancellationCoordinator:
    """Owns the manager's cancellation protocol (see the module docstring).

    Collaborators the cancellation touches are injected: the node's state,
    job manager, lease coordinator and transport; ``get_job_ledger`` and
    ``get_workflow_dispatcher`` resolve at call time (both are built in
    ``start()``, after this coordinator); server operations it relies on
    (cluster-leader takeover, terminal outcomes, ledger shortfall logging,
    leader resolution) are passed as callables.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        env: "Env",
        logger: "Logger",
        node_id: "NodeId",
        node_host: str,
        node_port: int,
        task_runner: "TaskRunner",
        clock: "Clock",
        job_manager: "JobManager",
        leases: "ManagerLeaseCoordinator",
        rate_limiter: "ServerRateLimiter",
        get_job_ledger: Callable[[], "JobLedger | None"],
        get_workflow_dispatcher: Callable[[], "WorkflowDispatcher | None"],
        is_cluster_leader: Callable[[], bool],
        send_tcp: Callable[..., Awaitable],
        send_to_worker: Callable[..., Awaitable],
        send_to_client: Callable[..., Awaitable],
        check_rate_limit_for_operation: Callable[[str, str], Awaitable[tuple[bool, float]]],
        take_over_job_leadership_as_cluster_leader: Callable[..., Awaitable[bool]],
        resolve_dc_leader_addr: Callable[..., "tuple[str, int] | None"],
        manager_tcp_addr_is_live: Callable[[tuple[str, int]], bool],
        emit_outcomes_for_terminal_job: Callable[..., None],
        discard_persisted_submission: Callable[..., Awaitable[None]],
        log_ledger_shortfall: Callable[..., Awaitable[None]],
    ) -> None:
        self._state = state
        self._config = config
        self._env = env
        self._logger = logger
        self._node_id = node_id
        self._node_host = node_host
        self._node_port = node_port
        self._task_runner = task_runner
        self._clock = clock
        self._job_manager = job_manager
        self._leases = leases
        self._rate_limiter = rate_limiter
        self._get_job_ledger = get_job_ledger
        self._get_workflow_dispatcher = get_workflow_dispatcher
        self._is_cluster_leader = is_cluster_leader
        self._send_tcp = send_tcp
        self._send_to_worker = send_to_worker
        self._send_to_client = send_to_client
        self._check_rate_limit_for_operation = check_rate_limit_for_operation
        self._take_over_job_leadership_as_cluster_leader = take_over_job_leadership_as_cluster_leader
        self._resolve_dc_leader_addr = resolve_dc_leader_addr
        self._manager_tcp_addr_is_live = manager_tcp_addr_is_live
        self._emit_outcomes_for_terminal_job = emit_outcomes_for_terminal_job
        self._discard_persisted_submission = discard_persisted_submission
        self._log_ledger_shortfall = log_ledger_shortfall

    def _build_cancel_response(
        self,
        job_id: str,
        success: bool,
        error: str | None = None,
        cancelled_count: int = 0,
        already_cancelled: bool = False,
        already_completed: bool = False,
        leader_addr: tuple[str, int] | None = None,
    ) -> bytes:
        """Build cancel response in AD-20 format."""
        return JobCancelResponse(
            job_id=job_id,
            success=success,
            error=error,
            cancelled_workflow_count=cancelled_count,
            already_cancelled=already_cancelled,
            already_completed=already_completed,
            leader_addr=leader_addr,
        ).dump()

    def _resolve_job_cancel_redirect_addr(
        self,
        job_id: str,
        client_unreachable_addrs: frozenset[tuple[str, int]] = frozenset(),
    ) -> tuple[str, int] | None:
        """Resolve a non-self, live redirect target for job cancellation.

        A new DC leader may receive a cancel before manager job-leader
        takeover commits locally. The cached ``get_job_leader_addr``
        still points at the dead prior leader; redirecting there sends
        the client to a manager already in its ``tried`` set, which the
        client surfaces as ``redirect cycles to already-tried target``.

        Filter every candidate through ``_manager_tcp_addr_is_live``
        AND the client's reported ``client_unreachable_addrs`` so we
        only redirect to peers *both* we and the client believe to be
        alive. The client's set is strictly fresher after a leader
        kill (it already failed to connect), so honoring it prevents
        the redirect-cycle even while our own SWIM view still lags.
        Returning ``None`` lets the client classify the response as
        transient and round-robin to a live target instead of chasing
        a known-dead leader address.
        """
        self_addr = (self._node_host, self._node_port)

        def is_redirectable(addr: tuple[str, int]) -> bool:
            return (
                addr != self_addr
                and addr not in client_unreachable_addrs
                and self._manager_tcp_addr_is_live(addr)
            )

        job_leader_addr = self._leases.get_job_leader_addr(job_id)
        if job_leader_addr is not None:
            job_leader_addr = tuple(job_leader_addr)
            if is_redirectable(job_leader_addr):
                return job_leader_addr

        dc_leader_addr = self._resolve_dc_leader_addr()
        if dc_leader_addr is not None and is_redirectable(tuple(dc_leader_addr)):
            return dc_leader_addr

        return None

    async def _push_cancellation_complete_to_origin(
        self,
        job_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """Push cancellation complete notification to origin gate/client."""
        callback_addr = self._state.get_job_callback(job_id)
        if not callback_addr:
            callback_addr = self._state.get_client_callback(job_id)

        if callback_addr:
            try:
                notification = JobCancellationComplete(
                    job_id=job_id,
                    success=success,
                    errors=errors,
                )
                await self._send_to_client(
                    callback_addr,
                    "job_cancellation_complete",
                    notification.dump(),
                )
            except Exception as error:
                await self._logger.log(
                    ServerWarning(
                        message=f"Failed to push cancellation complete to {callback_addr}: {error}",
                        node_host=self._node_host,
                        node_port=self._node_port,
                        node_id=self._node_id.short,
                    )
                )

    def _parse_cancel_request(
        self,
        data: bytes,
        addr: tuple[str, int],
    ) -> "ParsedCancelRequest":
        """Parse cancel request from either JobCancelRequest or
        legacy CancelJob format into a ``ParsedCancelRequest``.

        ``callback_addr`` is the client's push address when the
        JobCancelRequest carries it — used by the handler below to
        seed the local ``_job_callbacks`` entry if the entry was
        lost across a leader failover.

        ``unreachable_addrs`` is the set of manager TCP addresses the
        client has already proved unreachable this attempt. The
        handler treats any address in this set as not-live for its
        takeover / redirect decisions, closing the SWIM-lag window
        after a leader kill.
        """
        try:
            cancel_request = JobCancelRequest.load(data)
            return ParsedCancelRequest(
                job_id=cancel_request.job_id,
                fence_token=cancel_request.fence_token,
                requester_id=cancel_request.requester_id,
                timestamp=cancel_request.timestamp,
                reason=cancel_request.reason,
                callback_addr=(
                    tuple(cancel_request.callback_addr)
                    if cancel_request.callback_addr is not None
                    else None
                ),
                unreachable_addrs=frozenset(
                    tuple(unreachable)
                    for unreachable in (cancel_request.unreachable_addrs or [])
                ),
            )
        except Exception:
            # Normalize legacy CancelJob format to AD-20 fields
            cancel = CancelJob.load(data)
            return ParsedCancelRequest(
                job_id=cancel.job_id,
                fence_token=cancel.fence_token,
                requester_id=f"{addr[0]}:{addr[1]}",
                timestamp=self._clock.monotonic(),
                reason="Legacy cancel request",
                callback_addr=None,
                unreachable_addrs=frozenset(),
            )

    async def cancel_pending_workflows(
        self,
        job_id: str,
        timestamp: float,
        reason: str,
    ) -> list[str]:
        """Cancel and remove all pending workflows from the dispatch queue."""
        if not self._get_workflow_dispatcher():
            return []

        removed_pending = await self._get_workflow_dispatcher().cancel_pending_workflows(
            job_id
        )

        for workflow_id in removed_pending:
            self._state.set_cancelled_workflow(
                workflow_id,
                CancelledWorkflowInfo(
                    workflow_id=workflow_id,
                    job_id=job_id,
                    cancelled_at=timestamp,
                    reason=reason,
                ),
            )

        return removed_pending

    async def cancel_running_workflow_on_worker(
        self,
        job_id: str,
        workflow_id: str,
        worker_addr: tuple[str, int],
        requester_id: str,
        timestamp: float,
        reason: str,
    ) -> tuple[bool, str | None]:
        """Cancel a single running workflow on a worker. Returns (success, error_msg)."""
        try:
            cancel_data = WorkflowCancelRequest(
                job_id=job_id,
                workflow_id=workflow_id,
                requester_id=requester_id,
                timestamp=timestamp,
            ).dump()

            response = await self._send_to_worker(
                worker_addr,
                "cancel_workflow",
                cancel_data,
                timeout=self._env.CANCELLED_WORKFLOW_TIMEOUT,
            )

            if not isinstance(response, bytes):
                return False, "No response from worker"

            try:
                workflow_response = WorkflowCancelResponse.load(response)
                if workflow_response.success:
                    self._state.set_cancelled_workflow(
                        workflow_id,
                        CancelledWorkflowInfo(
                            workflow_id=workflow_id,
                            job_id=job_id,
                            cancelled_at=timestamp,
                            reason=reason,
                        ),
                    )

                    # Finalize the pending-cancellation tracker from
                    # this direct RPC ack instead of depending solely
                    # on the worker's fire-and-forget
                    # ``workflow_cancellation_complete`` push. The ack
                    # is definitive: ``success=True`` means the worker
                    # has terminally cancelled (``already_completed
                    # =False``) or already-terminated (``already
                    # _completed=True``) the workflow. The async push
                    # is fragile in exactly the scenario this handler
                    # runs under — a leader-failover-during-cancel
                    # leaves the network degraded (circuit breakers
                    # open toward the killed leader) and the worker's
                    # cached job-leader address stale, so the push is
                    # readily lost or delivered to a manager that
                    # isn't the job leader and can't forward it. When
                    # that happens the pending tracker never drains,
                    # the zero-pending branch that fires
                    # ``_push_cancellation_complete_to_origin`` never
                    # activates, and the client's
                    # ``await_job_cancellation`` times out — the
                    # residual failure ``test_cancel_during_leader
                    # _failover`` surfaces intermittently even after
                    # the ``already_completed`` and routing fixes.
                    #
                    # Finalizing here closes that window for BOTH
                    # cases (actively-cancelled and already-completed).
                    # ``_finalize_workflow_cancellation`` is idempotent
                    # — keyed on the same sub-workflow token string the
                    # tracker was seeded with — so if the worker's
                    # async push does arrive afterward it finds the
                    # entry already gone and no-ops. The push is thus
                    # demoted from sole-trigger to redundant backstop.
                    await self._finalize_workflow_cancellation(
                        job_id=job_id,
                        workflow_id=workflow_id,
                        success=True,
                        errors=[],
                    )
                    return True, None

                error_msg = (
                    workflow_response.error or "Worker reported cancellation failure"
                )
                return False, error_msg

            except Exception as parse_error:
                return False, f"Failed to parse worker response: {parse_error}"

        except Exception as send_error:
            return False, f"Failed to send cancellation to worker: {send_error}"

    def get_running_workflows_to_cancel(
        self,
        job: JobInfo,
        pending_cancelled: list[str],
    ) -> list[tuple[str, str, tuple[str, int]]]:
        """Get list of (workflow_id, worker_id, worker_addr) for running workflows to cancel."""
        workflows_to_cancel: list[tuple[str, str, tuple[str, int]]] = []

        # A workflow is "in-flight on a worker" — and therefore needs
        # a worker-side cancel push — once it is in ``ASSIGNED`` or
        # ``RUNNING`` state. ``ASSIGNED`` means the manager has
        # dispatched the workflow to a worker; the worker has the
        # sub-workflow token bound to it but may not have reported
        # the RUNNING transition back yet (that report can race
        # against the worker-side RUNNING state the client observes).
        # If we exclude ``ASSIGNED``, a cancel that arrives in that
        # window finds zero workflows to cancel, sends no
        # ``cancel_workflow`` to the worker, never seeds the
        # cancellation-pending tracker, and the client times out
        # waiting for the ``job_cancellation_complete`` push that
        # only fires when pending hits zero — even though the worker
        # is actively running the workflow.
        cancellable_statuses = (WorkflowStatus.ASSIGNED, WorkflowStatus.RUNNING)
        for workflow_id, workflow_info in job.workflows.items():
            if workflow_id in pending_cancelled:
                continue
            if workflow_info.status not in cancellable_statuses:
                continue

            for sub_workflow_token in workflow_info.sub_workflow_tokens:
                sub_workflow = job.sub_workflows.get(sub_workflow_token)
                if not (sub_workflow and sub_workflow.token.worker_id):
                    continue

                worker = self._state.get_worker(sub_workflow.token.worker_id)
                if worker:
                    worker_addr = (worker.node.host, worker.node.port)
                    # The dispatcher sends ``workflow_id=str(sub_token)``
                    # to the worker (see ``WorkflowDispatcher`` line ~679),
                    # so the worker stores the workflow in
                    # ``_active_workflows`` keyed by the sub-token string —
                    # *not* by the parent ``workflow_id``. Cancelling under
                    # the parent ``workflow_id`` makes the worker's cancel
                    # handler short-circuit on "workflow not found / already
                    # completed", silently returning success without
                    # actually cancelling and without scheduling the
                    # ``workflow_cancellation_complete`` push. The pending
                    # tracker on the manager then waits forever for a
                    # completion that will never arrive, and the client's
                    # ``await_job_cancellation`` times out.
                    workflows_to_cancel.append(
                        (
                            str(sub_workflow.token),
                            sub_workflow.token.worker_id,
                            worker_addr,
                        )
                    )

        return workflows_to_cancel

    async def cancel_running_workflows(
        self,
        job: JobInfo,
        pending_cancelled: list[str],
        requester_id: str,
        timestamp: float,
        reason: str,
        workflows_to_cancel: list[tuple[str, str, tuple[str, int]]] | None = None,
    ) -> tuple[list[str], dict[str, str]]:
        """Cancel all running workflows on workers. Returns (cancelled_list, errors_dict).

        ``workflows_to_cancel`` may be passed in by the caller when it
        has already computed the list (and seeded the pending tracker)
        — this lets the caller close the race where worker completions
        arrive before the manager's pending tracker is seeded.
        """
        running_cancelled: list[str] = []
        workflow_errors: dict[str, str] = {}

        if workflows_to_cancel is None:
            workflows_to_cancel = self.get_running_workflows_to_cancel(
                job, pending_cancelled
            )

        for workflow_id, worker_id, worker_addr in workflows_to_cancel:
            success, error_msg = await self.cancel_running_workflow_on_worker(
                job.job_id,
                workflow_id,
                worker_addr,
                requester_id,
                timestamp,
                reason,
            )

            if success:
                running_cancelled.append(workflow_id)
            elif error_msg:
                workflow_errors[workflow_id] = error_msg

        return running_cancelled, workflow_errors

    async def _broadcast_cancel_job_workflows_to_workers(
        self,
        *,
        job_id: str,
        requester_id: str,
        timestamp: float,
        reason: str,
    ) -> tuple[list[str], list[str]]:
        """Broadcast ``CancelJobWorkflowsRequest`` to every worker.

        Used when ``get_running_workflows_to_cancel`` returned empty
        — typically right after a leader-failover takeover whose
        post-takeover state-sync didn't fully populate
        ``job.workflows`` on the new leader. Each worker iterates its
        own ``_active_workflows`` for the requested ``job_id`` and
        cancels any matches, returning the sub-workflow token strings
        it actually cancelled.

        The manager seeds those returned ids into
        ``_cancellation_pending_workflows[job_id]`` so the canonical
        ``workflow_cancellation_complete`` zero-pending branch fires
        ``_push_cancellation_complete_to_origin`` when the workers'
        async completion pushes land. This restores the proper
        cancel contract — the client only sees success after the
        workers actually stop running the workflows — without
        requiring the new leader's local view to be perfectly
        consistent at the moment ``cancel_job`` arrived.

        Returns ``(cancelled_workflow_ids, broadcast_errors)``. The
        cancelled ids are returned for caller-side bookkeeping (the
        pending tracker is seeded inside this helper before
        returning, so callers don't need to seed again).
        """
        worker_entries = self._state.iter_workers()
        if not worker_entries:
            return [], []

        async def dispatch_to_worker(
            worker_id: str,
            worker,
        ) -> tuple[list[str], list[str]]:
            worker_addr = (worker.node.host, worker.node.port)
            request = CancelJobWorkflowsRequest(
                job_id=job_id,
                fence_token=self._leases.get_fence_token(job_id),
                requester_id=requester_id,
                timestamp=timestamp,
                reason=reason,
            )
            try:
                response = await self._send_to_worker(
                    worker_addr,
                    "cancel_job_workflows",
                    request.dump(),
                    timeout=self._env.CANCELLED_WORKFLOW_TIMEOUT,
                )
            except Exception as send_error:
                return [], [f"worker {worker_id[:8]}...: {send_error}"]

            if not isinstance(response, bytes) or not response:
                return [], [
                    f"worker {worker_id[:8]}...: no response from "
                    "cancel_job_workflows"
                ]

            try:
                decoded = CancelJobWorkflowsResponse.load(response)
            except Exception as decode_error:
                return [], [
                    f"worker {worker_id[:8]}...: decode response: "
                    f"{decode_error}"
                ]

            return list(decoded.cancelled_workflow_ids), list(decoded.errors)

        results = await asyncio.gather(
            *(
                dispatch_to_worker(worker_id, worker)
                for worker_id, worker in worker_entries
            ),
            return_exceptions=False,
        )

        cancelled_ids: list[str] = []
        errors: list[str] = []
        for per_worker_ids, per_worker_errors in results:
            cancelled_ids.extend(per_worker_ids)
            errors.extend(per_worker_errors)

        # Seed the pending tracker, then finalize each entry straight
        # from this synchronous ``CancelJobWorkflowsResponse``. The
        # response IS the worker's definitive confirmation that it
        # cancelled the workflow — exactly the same authority the
        # direct ``cancel_running_workflow_on_worker`` path relies on.
        # We deliberately do NOT wait for the worker's fire-and-forget
        # ``workflow_cancellation_complete`` push to drain the tracker:
        # under the degraded post-failover network this handler runs in
        # (circuit breakers open toward the killed leader, worker
        # job-leader caches stale) that push is readily lost or
        # delivered to a manager that isn't the job leader, and the
        # tracker would never reach zero — the client's
        # ``await_job_cancellation`` then times out.
        #
        # Seed-then-finalize (rather than skipping the tracker) keeps
        # the completion-event / error-aggregation machinery in
        # ``_finalize_workflow_cancellation`` on a single code path, and
        # is idempotent: if a worker's async push does arrive later it
        # finds the entry already gone and no-ops.
        for workflow_id in cancelled_ids:
            self._state.add_cancellation_pending_workflow(
                job_id, workflow_id
            )
        for workflow_id in cancelled_ids:
            await self._finalize_workflow_cancellation(
                job_id=job_id,
                workflow_id=workflow_id,
                success=True,
                errors=[],
            )

        return cancelled_ids, errors

    async def handle_cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle job cancellation request (AD-20).

        Wire-action name MUST match what every sender uses (the
        client's ``ClientCancellationManager._attempt_with_redirects``
        and the gate's cancellation coordinator both target
        ``"cancel_job"``). The ``@tcp.receive()`` decorator
        registers handlers by ``func.__name__``, so this method
        being named ``cancel_job`` is what makes the wire match
        succeed. A previous incarnation of this method was named
        ``job_cancel`` — every cancel request silently mismatched
        and the server returned no response, hanging the client
        until its per-target timeout.

        Robust cancellation flow:
        1. Verify job exists
        2. Remove ALL pending workflows from dispatch queue
        3. Cancel ALL running workflows on workers
        4. Wait for verification that no workflows are still running
        5. Return detailed per-workflow cancellation results

        Accepts both legacy CancelJob and new JobCancelRequest formats at the
        boundary, but normalizes to AD-20 internally.
        """
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit_for_operation(
                client_id, "cancel"
            )
            if not allowed:
                return RateLimitResponse(
                    operation="cancel",
                    retry_after_seconds=retry_after,
                ).dump()

            parsed = self._parse_cancel_request(data, addr)
            job_id = parsed.job_id
            fence_token = parsed.fence_token
            requester_id = parsed.requester_id
            timestamp = parsed.timestamp
            reason = parsed.reason
            request_callback_addr = parsed.callback_addr
            client_unreachable_addrs = parsed.unreachable_addrs

            # Seed the local ``_job_callbacks`` entry from the
            # request if we don't already have one. Under
            # leader-failover-during-cancel the previous leader's
            # ``_broadcast_job_leadership`` may not have reached us
            # before it died, leaving ``_push_cancellation_complete_to_origin``
            # unable to find a callback and silently no-op-ing.
            # The request-carried callback is the client's own
            # push address so seeding from it is definitively
            # correct — no risk of racing against a stale value.
            if (
                request_callback_addr is not None
                and self._state.get_job_callback(job_id) is None
            ):
                self._state.set_job_callback(
                    job_id, request_callback_addr
                )

            # ``get_job`` keys by token-string; ``job_id`` here is the
            # bare job id. ``get_job`` now accepts both forms (token
            # or bare id) — see ``JobManager.get_job`` — so this
            # lookup succeeds regardless of which form callers pass.
            job = self._job_manager.get_job_by_id(job_id)
            if not job:
                return self._build_cancel_response(
                    job_id, success=False, error="Job not found"
                )

            # Job-leader fencing: only the manager that owns this job
            # may execute cancellation against worker state. A
            # non-leader has neither the dispatch context nor the
            # workflow-cancellation push chain — silently succeeding
            # with cancelled_count=0 (the prior behavior) leaves
            # workers running indefinitely.
            #
            # Takeover authority is Raft, not SWIM. When we are the
            # Raft DC leader (quorum-elected) but not yet the job
            # leader, we are in the post-failover transition window:
            # the prior job leader (typically the old DC leader) has
            # been superseded and job leadership must reconverge to us.
            # We attempt takeover *unconditionally* rather than gating
            # on our SWIM view of the old leader's liveness, because
            # SWIM failure detection lags Raft election by tens of
            # seconds — gating on it is what produced the
            # ``redirect cycles to already-tried target`` hang, where
            # a freshly-elected leader kept bouncing the client back to
            # the dead prior leader while waiting for its own suspicion
            # timers to fire.
            #
            # This is safe: ``_take_over_job_leadership_as_cluster_leader``
            # (a) requires ``is_leader()`` + manager quorum, (b)
            # re-syncs peer state and refuses to steal leadership from a
            # genuinely *newer* leader (one that isn't the ``old_leader``
            # we set out to supersede), and (c) bumps a fence token that
            # is quorum-replicated before it takes effect. A stale prior
            # leader that resurfaces is fenced out. Job leadership is
            # designed to track DC leadership, so reconverging it to the
            # current Raft leader is correct, not a theft.
            if not self._leases.is_job_leader(job_id) and self._is_cluster_leader():
                old_leader_id = self._leases.get_job_leader(job_id)
                taken_over = await self._take_over_job_leadership_as_cluster_leader(
                    job_id,
                    old_leader_id,
                )
                if taken_over:
                    job = self._job_manager.get_job_by_id(job_id)
                else:
                    # Quorum unavailable, or a newer leader beat us to
                    # it. Transient — no ``leader_addr`` so the client
                    # round-robins to a live target rather than chasing
                    # a stale address.
                    return self._build_cancel_response(
                        job_id,
                        success=False,
                        error="Not job leader; job leader transition in progress",
                        leader_addr=None,
                    )

            # Still not the job leader → we are not the DC leader
            # either (a DC leader would have taken over above, or
            # returned the transient response). Redirect to a live
            # leader. ``_resolve_job_cancel_redirect_addr`` filters out
            # both our own SWIM-dead peers AND the addresses the client
            # reported unreachable, so we never hand back an address the
            # client already proved dead.
            if not self._leases.is_job_leader(job_id):
                leader_addr = self._resolve_job_cancel_redirect_addr(
                    job_id, client_unreachable_addrs
                )
                leader_hint = (
                    f"{leader_addr[0]}:{leader_addr[1]}"
                    if leader_addr
                    else "unknown"
                )
                return self._build_cancel_response(
                    job_id,
                    success=False,
                    error=f"Not job leader, retry at leader: {leader_hint}",
                    leader_addr=leader_addr,
                )

            stored_fence = self._leases.get_fence_token(job_id)
            if fence_token > 0 and stored_fence != fence_token:
                error_msg = (
                    f"Fence token mismatch: expected {stored_fence}, got {fence_token}"
                )
                return self._build_cancel_response(
                    job_id, success=False, error=error_msg
                )

            if job.status == JobStatus.CANCELLED.value:
                return self._build_cancel_response(
                    job_id, success=True, already_cancelled=True
                )

            if job.status == JobStatus.COMPLETED.value:
                return self._build_cancel_response(
                    job_id,
                    success=False,
                    already_completed=True,
                    error="Job already completed",
                )

            pending_cancelled = await self.cancel_pending_workflows(
                job_id, timestamp, reason
            )

            # Seed the cancellation-pending tracker BEFORE sending any
            # ``cancel_workflow`` TCP request to workers. Workers reply
            # asynchronously via ``workflow_cancellation_complete``;
            # because worker→manager TCP roundtrips can complete
            # *inside the same event-loop tick* as the manager→worker
            # send, post-send seeding races with the inbound completion.
            # When the race is lost, the completion arrives with an
            # empty pending set, the decrement no-ops, and once
            # seeding finally happens the pending entry never gets
            # cleared — the client times out.
            #
            # Seeding before send guarantees the completion handler
            # sees the pending entry. ``add_cancellation_pending_workflow``
            # is idempotent so re-adding on Raft replay (state_machine.py)
            # remains correct.
            workflows_to_cancel = self.get_running_workflows_to_cancel(
                job, pending_cancelled
            )
            for sub_token_str, _, _ in workflows_to_cancel:
                self._state.add_cancellation_pending_workflow(
                    job_id, sub_token_str
                )

            running_cancelled, workflow_errors = await self.cancel_running_workflows(
                job, pending_cancelled, requester_id, timestamp, reason,
                workflows_to_cancel=workflows_to_cancel,
            )

            strategy = self._state.get_job_timeout_strategy(job_id)
            if strategy:
                await strategy.stop_tracking(job_id, "cancelled")

            job.status = JobStatus.CANCELLED.value
            job.completed_at = self._clock.time()

            if self._get_job_ledger() is not None:
                await self._log_ledger_shortfall(
                    "JobCancellationRequested",
                    job_id,
                    await self._get_job_ledger().request_cancellation(
                        job_id,
                        reason=reason,
                        requestor_id=requester_id,
                        durability=DurabilityLevel.REGIONAL,
                    ),
                )
                # This datacenter has now cancelled what it was running:
                # its pending workflows and the running ones the workers
                # confirmed (AD-38 JobCancellationAcked).
                await self._log_ledger_shortfall(
                    "JobCancellationAcked",
                    job_id,
                    await self._get_job_ledger().acknowledge_cancellation(
                        job_id,
                        datacenter_id=self._node_id.datacenter,
                        workflows_cancelled=len(pending_cancelled) + len(running_cancelled),
                        durability=DurabilityLevel.REGIONAL,
                    ),
                )
                await self._log_ledger_shortfall(
                    "JobCompleted",
                    job_id,
                    await self._get_job_ledger().complete_job(
                        job_id,
                        final_status=JobStatus.CANCELLED.value,
                        total_completed=job.workflows_completed,
                        total_failed=job.workflows_failed,
                        duration_ms=int(job.elapsed_seconds() * 1000),
                        durability=DurabilityLevel.REGIONAL,
                    ),
                )
                await self._discard_persisted_submission(job_id)

            await self._state.increment_state_version()

            # Phase F3: emit FAILED outcomes for any still-in-flight
            # workflows whose AD-26 extension history hasn't been
            # closed by a WorkflowFinalResult yet. Cancellation is
            # treated as failure for the H8 Bayesian tuner — the
            # extension(s) didn't get the workflow to completion.
            self._emit_outcomes_for_terminal_job(
                job_id, ExtensionOutcomeKind.FAILED
            )

            total_cancelled = len(pending_cancelled) + len(running_cancelled)
            total_errors = len(workflow_errors)
            overall_success = total_errors == 0

            error_str = None
            if workflow_errors:
                error_details = [
                    f"{workflow_id[:8]}...: {err}"
                    for workflow_id, err in workflow_errors.items()
                ]
                error_str = (
                    f"{total_errors} workflow(s) failed: {'; '.join(error_details)}"
                )

            # Empty-view case: the local ``get_running_workflows_to_cancel``
            # found nothing — typical right after a leader-failover
            # takeover whose peer/worker state-sync hadn't yet
            # repopulated ``job.workflows`` with a cancellable
            # status. The prior shortcut fired
            # ``_push_cancellation_complete_to_origin`` speculatively
            # so the client unblocked, but it didn't actually stop
            # any workflow that might still be running on workers
            # the leader hadn't fully seen. That violates the
            # cancel contract: a successful ``cancel_job`` must
            # mean every in-flight workflow for the job is being
            # cancelled, not just that the client got a success
            # response.
            #
            # Correct fix: broadcast ``CancelJobWorkflowsRequest`` to
            # every worker we know about (including ones added by
            # ``_apply_peer_worker_snapshots`` from peer state sync).
            # Each worker iterates its ``_active_workflows`` for any
            # workflow whose ``progress.job_id`` matches, cancels it
            # via the same ``_cancel_workflow`` path the
            # ``cancel_workflow`` RPC uses, and returns the set of
            # sub-workflow token strings it actually cancelled. We
            # seed the pending tracker from those responses so the
            # canonical
            # ``workflow_cancellation_complete`` → zero-pending →
            # ``_push_cancellation_complete_to_origin`` chain
            # drives the client-facing notification just like the
            # non-empty path.
            #
            # The leader-only gate above already ensures we don't
            # broadcast from a non-leader manager; the takeover
            # path produces ``is_job_leader=True`` before reaching
            # this branch.
            if not workflows_to_cancel and not pending_cancelled:
                broadcast_cancelled, broadcast_errors = (
                    await self._broadcast_cancel_job_workflows_to_workers(
                        job_id=job_id,
                        requester_id=requester_id,
                        timestamp=timestamp,
                        reason=reason,
                    )
                )
                if broadcast_cancelled:
                    # Workers reported real workflows cancelled.
                    # Their ``workflow_cancellation_complete`` pushes
                    # will arrive shortly and decrement the
                    # pending tracker we just seeded — the canonical
                    # push fires when the tracker reaches zero.
                    total_cancelled += len(broadcast_cancelled)
                else:
                    # Nothing was actually running on workers
                    # either. The job is correctly marked CANCELLED
                    # locally and there's no in-flight work to
                    # wait for — fire the completion push directly
                    # so the client's ``await_job_cancellation``
                    # doesn't block on an event that no
                    # zero-pending handler will ever set.
                    self._task_runner.run(
                        self._push_cancellation_complete_to_origin,
                        job_id,
                        overall_success and not broadcast_errors,
                        list(workflow_errors.values()) + broadcast_errors,
                    )
                if broadcast_errors:
                    workflow_errors.update(
                        {
                            f"broadcast_error_{idx}": err
                            for idx, err in enumerate(broadcast_errors)
                        }
                    )
                    total_errors = len(workflow_errors)
                    overall_success = total_errors == 0
                    if workflow_errors and not error_str:
                        error_str = (
                            f"{total_errors} workflow(s) failed: "
                            + "; ".join(
                                f"{wf_id[:8]}...: {err}"
                                for wf_id, err in workflow_errors.items()
                            )
                        )

            return self._build_cancel_response(
                job_id,
                success=overall_success,
                cancelled_count=total_cancelled,
                error=error_str,
            )

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Job cancel error: {error}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id.short,
                )
            )
            return JobCancelResponse(
                job_id="",
                success=False,
                error=str(error),
            ).dump()

    async def _finalize_workflow_cancellation(
        self,
        job_id: str,
        workflow_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """Decrement the pending-cancellation tracker and, if this
        was the last outstanding workflow, fire the origin push.

        Extracted from the inline body of
        ``workflow_cancellation_complete`` so both the async push
        handler AND the synchronous ``already_completed`` detection
        in ``cancel_running_workflow_on_worker`` reach the same
        completion path. The two callers converge on identical
        state after the last workflow terminates — either via the
        worker's async push (canonical cancel-mid-flight) or via
        the direct response's ``already_completed`` field
        (race-close-out when the workflow finished before the
        cancel request landed).

        Idempotent: workflows not in the pending set are a no-op,
        so double-invocation across the race (worker replies
        already_completed AND races to send the async push before
        the tracker drains) is safe.
        """
        pending = self._state.get_cancellation_pending_workflows(job_id)
        if workflow_id not in pending:
            return

        self._state.remove_cancellation_pending_workflow(
            job_id, workflow_id
        )

        # Aggregate any errors reported by the worker's push. When
        # the caller synthesizes the decrement for the
        # already-completed race, ``errors`` is empty — nothing
        # went wrong; the workflow simply finished on its own.
        if not success and errors:
            for error in errors:
                self._state.add_cancellation_error(
                    job_id, f"Workflow {workflow_id[:8]}...: {error}"
                )

        remaining_pending = (
            self._state.get_cancellation_pending_workflows(job_id)
        )
        if remaining_pending:
            return

        # All workflows have reported — fire the completion event
        # and push to origin.
        event = self._state.get_cancellation_completion_event(job_id)
        if event:
            event.set()

        aggregated_errors = self._state.get_cancellation_errors(job_id)
        aggregated_success = len(aggregated_errors) == 0

        self._task_runner.run(
            self._push_cancellation_complete_to_origin,
            job_id,
            aggregated_success,
            aggregated_errors,
        )

        self._state.clear_cancellation_pending_workflows(job_id)
        self._state.clear_cancellation_completion_events(job_id)
        self._state.clear_cancellation_initiated_at(job_id)

    async def handle_workflow_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle workflow cancellation completion push from worker (AD-20).

        Workers push this notification after successfully (or unsuccessfully)
        cancelling a workflow. The manager:
        1. Tracks completion of all workflows in a job cancellation
        2. Aggregates any errors from failed cancellations
        3. When all workflows report, fires the completion event
        4. Pushes aggregated result to origin gate/client
        """
        try:
            completion = WorkflowCancellationComplete.load(data)
            job_id = completion.job_id
            workflow_id = completion.workflow_id

            await self._logger.log(
                ServerInfo(
                    message=f"Received workflow cancellation complete for {workflow_id[:8]}... "
                    f"(job {job_id[:8]}..., success={completion.success})",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id.short,
                )
            )

            # Forward to the actual job leader if we're not it. Workers
            # push ``workflow_cancellation_complete`` to whichever address
            # they have cached at ``get_workflow_job_leader(workflow_id)``;
            # under leader failover that cache can still point at the
            # dead old leader, and the worker-side fallback chain then
            # iterates ``_healthy_manager_ids`` and lands at whichever
            # surviving manager it picks first. The cancellation-pending
            # tracker that drives the completion push lives ONLY on the
            # current job leader (it's seeded inline by the takeover-side
            # ``cancel_job`` right before ``cancel_running_workflows``
            # sends ``cancel_workflow`` to the worker). On a non-leader
            # peer, ``get_cancellation_pending_workflows`` returns an
            # empty set, the ``workflow_id in pending`` check below is
            # False, the decrement no-ops, and the leader's tracker
            # never reaches zero — the client times out on
            # ``await_job_cancellation`` because the
            # ``job_cancellation_complete`` push that only fires from
            # the zero-pending branch is never scheduled. That's the
            # exact failure shape ``test_cancel_during_leader_failover``
            # surfaces about one run in three. Forwarding here closes
            # the gap regardless of which manager the worker happens
            # to land on.
            if not self._leases.is_job_leader(job_id):
                self_addr = (self._node_host, self._node_port)
                # Compose the forward-target list in priority order:
                # 1. The cached ``job_leader_addr`` for this job, when
                #    SWIM still believes it's live. This wins when
                #    ``_broadcast_job_leadership`` already replicated
                #    the takeover to us — typical case after a clean
                #    election.
                # 2. The current DC leader's address, when election
                #    state has converged but the job-leader broadcast
                #    hadn't reached us before the worker push arrived.
                #    After takeover the DC leader and the job leader
                #    are the same manager; falling back here closes
                #    the broadcast-dropped-at-this-peer window.
                # 3. Every other active manager peer, blind-fanned-out
                #    until one returns success. The worker only retries
                #    a handful of fallback addresses (default
                #    ``_healthy_manager_ids`` walk), and any one of
                #    those landing on the leader is enough for the
                #    pending tracker to decrement; serializing through
                #    us guarantees at least one attempt regardless of
                #    which peer the worker happened to pick.
                forward_targets: list[tuple[str, int]] = []
                cached_leader_addr = self._leases.get_job_leader_addr(job_id)
                if (
                    cached_leader_addr is not None
                    and tuple(cached_leader_addr) != self_addr
                    and self._manager_tcp_addr_is_live(tuple(cached_leader_addr))
                ):
                    forward_targets.append(tuple(cached_leader_addr))

                dc_leader_addr = self._resolve_dc_leader_addr()
                if (
                    dc_leader_addr is not None
                    and tuple(dc_leader_addr) != self_addr
                    and tuple(dc_leader_addr) not in forward_targets
                    and self._manager_tcp_addr_is_live(tuple(dc_leader_addr))
                ):
                    forward_targets.append(tuple(dc_leader_addr))

                for peer_addr in list(
                    self._state.get_active_manager_peers()
                ):
                    peer_tuple = tuple(peer_addr)
                    if peer_tuple == self_addr or peer_tuple in forward_targets:
                        continue
                    if not self._manager_tcp_addr_is_live(peer_tuple):
                        continue
                    forward_targets.append(peer_tuple)

                for target_addr in forward_targets:
                    try:
                        response, _clock = await self._send_tcp(
                            target_addr,
                            "workflow_cancellation_complete",
                            data,
                            timeout=self._config.tcp_timeout_standard_seconds,
                        )
                        if isinstance(response, bytes) and response not in (
                            b"",
                            b"ERROR",
                        ):
                            return response
                    except Exception as forward_error:
                        await self._logger.log(
                            ServerWarning(
                                message=(
                                    "Failed to forward "
                                    "workflow_cancellation_complete for "
                                    f"{workflow_id[:8]}... (job {job_id[:8]}...) "
                                    f"to {target_addr}: {forward_error}; "
                                    "trying next forward target."
                                ),
                                node_host=self._node_host,
                                node_port=self._node_port,
                                node_id=self._node_id.short,
                            )
                        )

            # Track this workflow as complete. The finalize pushes the
            # job's cancellation completion to its origin exactly once,
            # when the last pending workflow drains; the coordinator's
            # handle_workflow_cancelled, also called here, found the set
            # already drained and notified the client a second time.
            await self._finalize_workflow_cancellation(
                job_id=job_id,
                workflow_id=workflow_id,
                success=completion.success,
                errors=list(completion.errors or []),
            )

            return b"OK"

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Cancellation complete error: {error}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id.short,
                )
            )
            return b"ERROR"

    async def handle_workflow_cancellation_query(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow cancellation query from worker."""
        try:
            query = WorkflowCancellationQuery.load(data)

            job = self._job_manager.get_job(query.job_id)
            if not job:
                return WorkflowCancellationResponse(
                    job_id=query.job_id,
                    workflow_id=query.workflow_id,
                    workflow_name="",
                    status="UNKNOWN",
                    error="Job not found",
                ).dump()

            # Check job-level cancellation
            if job.status == JobStatus.CANCELLED.value:
                return WorkflowCancellationResponse(
                    job_id=query.job_id,
                    workflow_id=query.workflow_id,
                    workflow_name="",
                    status="CANCELLED",
                ).dump()

            # Check specific workflow status
            for sub_wf in job.sub_workflows.values():
                if str(sub_wf.token) == query.workflow_id:
                    workflow_name = ""
                    status = WorkflowStatus.RUNNING.value
                    if sub_wf.progress is not None:
                        workflow_name = sub_wf.progress.workflow_name
                        status = sub_wf.progress.status
                    return WorkflowCancellationResponse(
                        job_id=query.job_id,
                        workflow_id=query.workflow_id,
                        workflow_name=workflow_name,
                        status=status,
                    ).dump()

            return WorkflowCancellationResponse(
                job_id=query.job_id,
                workflow_id=query.workflow_id,
                workflow_name="",
                status="UNKNOWN",
                error="Workflow not found",
            ).dump()

        except Exception as error:
            return WorkflowCancellationResponse(
                job_id="unknown",
                workflow_id="unknown",
                workflow_name="",
                status="ERROR",
                error=str(error),
            ).dump()

    async def handle_cancel_single_workflow(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle single workflow cancellation request."""
        try:
            request = SingleWorkflowCancelRequest.load(data)

            # Rate limit check
            client_id = f"{addr[0]}:{addr[1]}"
            rate_limit_result = await self._rate_limiter.check_rate_limit(
                client_id, "cancel_workflow"
            )
            if not rate_limit_result.allowed:
                return RateLimitResponse(
                    operation="cancel_workflow",
                    retry_after_seconds=rate_limit_result.retry_after_seconds,
                ).dump()

            # Check if already cancelled
            existing = self._state.get_cancelled_workflow(request.workflow_id)
            if existing:
                return SingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=WorkflowCancellationStatus.ALREADY_CANCELLED.value,
                    cancelled_dependents=existing.dependents,
                    datacenter=self._node_id.datacenter,
                ).dump()

            job = self._job_manager.get_job(request.job_id)
            if not job:
                return SingleWorkflowCancelResponse(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    request_id=request.request_id,
                    status=WorkflowCancellationStatus.NOT_FOUND.value,
                    errors=["Job not found"],
                    datacenter=self._node_id.datacenter,
                ).dump()

            # Add to cancelled workflows
            self._state.set_cancelled_workflow(
                request.workflow_id,
                CancelledWorkflowInfo(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    cancelled_at=self._clock.monotonic(),
                    request_id=request.request_id,
                    dependents=[],
                ),
            )

            return SingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=WorkflowCancellationStatus.CANCELLED.value,
                datacenter=self._node_id.datacenter,
            ).dump()

        except Exception as error:
            return SingleWorkflowCancelResponse(
                job_id="unknown",
                workflow_id="unknown",
                request_id="unknown",
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=[str(error)],
                datacenter=self._node_id.datacenter,
            ).dump()

    async def handle_workflow_cancellation_peer_notification(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow cancellation peer notification."""
        try:
            notification = WorkflowCancellationPeerNotification.load(data)

            # Add all cancelled workflows to our bucket
            for wf_id in notification.cancelled_workflows:
                if not self._state.has_cancelled_workflow(wf_id):
                    self._state.set_cancelled_workflow(
                        wf_id,
                        CancelledWorkflowInfo(
                            job_id=notification.job_id,
                            workflow_id=wf_id,
                            cancelled_at=notification.timestamp or self._clock.monotonic(),
                            request_id=notification.request_id,
                            dependents=[],
                        ),
                    )

            return b"OK"

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Workflow cancellation peer notification error: {error}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id.short,
                )
            )
            return b"ERROR"
