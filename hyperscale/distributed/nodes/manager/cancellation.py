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
from itertools import filterfalse
from types import MappingProxyType
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
    WorkflowCancellationQuery,
    WorkflowCancellationResponse,
    WorkflowCancellationStatus,
    TrackingToken,
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    SubWorkflowInfo,
    WorkflowInfo,
    WorkflowStatus,
    WorkerRegistration,
)
from hyperscale.distributed.workflow import WorkflowLifecycleStateMachine, WorkflowState
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerInfo, ServerWarning

from .models import ParsedCancelRequest

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env
    from hyperscale.distributed.jobs import JobManager, WorkflowDispatcher
    from hyperscale.distributed.ledger.job_ledger import JobLedger
    from hyperscale.distributed.nodes.manager.models.manager_config import ManagerConfig
    from hyperscale.distributed.nodes.manager.leases import ManagerLeaseCoordinator
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.reliability.rate_limiting import ServerRateLimiter
    from hyperscale.distributed.runtime import Clock
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger

# A requested workflow's lifecycle state that settles a single-workflow cancel
# without cancelling anything (AD-54); a state absent here is past running.
# PENDING, DISPATCHED and RUNNING map to None: those are cancelled.
SETTLED_CANCELLATION_STATUS_BY_STATE = MappingProxyType(
    {
        WorkflowState.CANCELLED: WorkflowCancellationStatus.ALREADY_CANCELLED,
        WorkflowState.CANCELLING: WorkflowCancellationStatus.CANCELLING,
        WorkflowState.PENDING: None,
        WorkflowState.DISPATCHED: None,
        WorkflowState.RUNNING: None,
    }
)


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
        complete_job_if_done: Callable[[str], Awaitable[None]],
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
        self._complete_job_if_done = complete_job_if_done

    def _build_cancel_response(
        self,
        job_id: str,
        success: bool,
        error: str | None = None,
        cancelled_count: int = 0,
        already_cancelled: bool = False,
        already_completed: bool = False,
        leader_addr: tuple[str, int] | None = None,
        job_not_found: bool = False,
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
            job_not_found=job_not_found,
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
        job_leader_addr = self._redirectable_job_leader_addr(job_id, client_unreachable_addrs)
        if job_leader_addr is not None:
            return job_leader_addr

        return self._redirectable_dc_leader_addr(client_unreachable_addrs)

    def _is_cancel_redirect_target(
        self,
        addr: tuple[str, int],
        client_unreachable_addrs: frozenset[tuple[str, int]],
    ) -> bool:
        """True when ``addr`` is not this manager, not reported unreachable by
        the client, and live in our SWIM view (AD-20 redirect target)."""
        return (
            addr != (self._node_host, self._node_port)
            and addr not in client_unreachable_addrs
            and self._manager_tcp_addr_is_live(addr)
        )

    def _redirectable_job_leader_addr(
        self,
        job_id: str,
        client_unreachable_addrs: frozenset[tuple[str, int]],
    ) -> tuple[str, int] | None:
        """The job's cached leader address, when it is a live redirect target."""
        job_leader_addr = self._leases.get_job_leader_addr(job_id)
        if job_leader_addr is None:
            return None

        job_leader_addr = tuple(job_leader_addr)
        if self._is_cancel_redirect_target(job_leader_addr, client_unreachable_addrs):
            return job_leader_addr

        return None

    def _redirectable_dc_leader_addr(
        self,
        client_unreachable_addrs: frozenset[tuple[str, int]],
    ) -> tuple[str, int] | None:
        """The DC leader's address, when it is a live redirect target."""
        dc_leader_addr = self._resolve_dc_leader_addr()
        if dc_leader_addr is not None and self._is_cancel_redirect_target(
            tuple(dc_leader_addr), client_unreachable_addrs
        ):
            return dc_leader_addr

        return None

    async def _push_cancellation_complete_to_origin(
        self,
        job_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """Push cancellation complete notification to origin gate/client."""
        callback_addr = self._cancellation_callback_addr(job_id)
        if not callback_addr:
            return

        try:
            await self._send_cancellation_complete(callback_addr, job_id, success, errors)
        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=f"Failed to push cancellation complete to {callback_addr}: {error}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id.short,
                )
            )

    def _cancellation_callback_addr(self, job_id: str) -> tuple[str, int] | None:
        """The job's origin push address: its gate callback, else its client's."""
        return self._state.get_job_callback(job_id) or self._state.get_client_callback(job_id)

    async def _send_cancellation_complete(
        self,
        callback_addr: tuple[str, int],
        job_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """Send ``JobCancellationComplete`` to the origin, raising a transport error."""
        notification = JobCancellationComplete(
            job_id=job_id,
            success=success,
            errors=errors,
        )
        reply = await self._send_to_client(
            callback_addr,
            "job_cancellation_complete",
            notification.dump(),
        )
        # send_tcp returns transport errors rather than raising.
        if isinstance(reply, Exception):
            raise reply

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
                callback_addr=self._request_callback_addr(cancel_request),
                unreachable_addrs=self._request_unreachable_addrs(cancel_request),
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

    def _request_callback_addr(self, cancel_request: JobCancelRequest) -> tuple[str, int] | None:
        """The client's push address the cancel request carries, as a tuple."""
        return (
            tuple(cancel_request.callback_addr)
            if cancel_request.callback_addr is not None
            else None
        )

    def _request_unreachable_addrs(self, cancel_request: JobCancelRequest) -> frozenset[tuple[str, int]]:
        """The manager addresses the client already proved unreachable."""
        return frozenset(
            tuple(unreachable)
            for unreachable in (cancel_request.unreachable_addrs or [])
        )

    async def cancel_pending_workflows(self, job_id: str) -> list[str]:
        """Drop the job's workflows from the dispatch queue: nothing of it
        is dispatched from now on. Returns the removed workflow ids."""
        if not self._get_workflow_dispatcher():
            return []

        return await self._get_workflow_dispatcher().cancel_pending_workflows(job_id)

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

            return await self._apply_worker_cancel_response(job_id, workflow_id, timestamp, reason, response)

        except Exception as send_error:
            return False, f"Failed to send cancellation to worker: {send_error}"

    async def _apply_worker_cancel_response(
        self,
        job_id: str,
        workflow_id: str,
        timestamp: float,
        reason: str,
        response: bytes,
    ) -> tuple[bool, str | None]:
        """Decode the worker's ``WorkflowCancelResponse`` into (success, error_msg) (AD-20)."""
        try:
            workflow_response = WorkflowCancelResponse.load(response)
            if workflow_response.success:
                return await self._record_worker_cancel_success(job_id, workflow_id, timestamp, reason)

            return self._worker_cancel_failure(workflow_response)

        except Exception as parse_error:
            return False, f"Failed to parse worker response: {parse_error}"

    async def _record_worker_cancel_success(
        self,
        job_id: str,
        workflow_id: str,
        timestamp: float,
        reason: str,
    ) -> tuple[bool, str | None]:
        """Record the worker-confirmed cancel and finalize its pending entry (AD-20)."""
        self._state.set_cancelled_workflow(
            job_id,
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
        await self.finalize_workflow_cancellation(
            job_id=job_id,
            workflow_id=workflow_id,
            success=True,
            errors=[],
        )
        return True, None

    def _worker_cancel_failure(
        self,
        workflow_response: WorkflowCancelResponse,
    ) -> tuple[bool, str | None]:
        """The (False, error_msg) result of a worker-refused cancellation."""
        error_msg = (
            workflow_response.error or "Worker reported cancellation failure"
        )
        return False, error_msg

    def get_running_workflows_to_cancel(
        self,
        job: JobInfo,
        in_flight_workflow_ids: list[str],
    ) -> list[tuple[str, str, tuple[str, int]]]:
        """Get list of (sub_workflow_token, worker_id, worker_addr) to stop:
        the subs still running the workflows ``in_flight_workflow_ids``."""
        workflows_to_cancel: list[tuple[str, str, tuple[str, int]]] = []

        # A workflow is "in-flight on a worker" — and therefore needs
        # a worker-side cancel push — once it is DISPATCHED or RUNNING
        # (the callers pass those). DISPATCHED means the manager has
        # dispatched the workflow to a worker; the worker has the
        # sub-workflow token bound to it but may not have reported
        # progress back yet (that report can race against the
        # worker-side RUNNING state the client observes). If those were
        # excluded, a cancel that arrives in that window finds zero
        # workflows to cancel, sends no ``cancel_workflow`` to the
        # worker, never seeds the cancellation-pending tracker, and the
        # client times out waiting for the ``job_cancellation_complete``
        # push that only fires when pending hits zero — even though the
        # worker is actively running the workflow. A superseded sub (its
        # worker lost) or one that already reported its result runs
        # nothing to stop.
        for workflow_info in job.workflows.values():
            if self._workflow_is_in_flight(workflow_info, in_flight_workflow_ids):
                workflows_to_cancel.extend(self._running_subs_to_cancel(job, workflow_info))

        return workflows_to_cancel

    def _workflow_is_in_flight(
        self,
        workflow_info: WorkflowInfo,
        in_flight_workflow_ids: list[str],
    ) -> bool:
        """True when the workflow is one of the DISPATCHED/RUNNING ones to stop."""
        return (workflow_info.token.workflow_id or "") in in_flight_workflow_ids

    def _running_subs_to_cancel(
        self,
        job: JobInfo,
        workflow_info: WorkflowInfo,
    ) -> list[tuple[str, str, tuple[str, int]]]:
        """(sub_workflow_token, worker_id, worker_addr) of each of the workflow's
        subs still running on a known worker."""
        running_subs: list[tuple[str, str, tuple[str, int]]] = []
        for sub_workflow_token in workflow_info.sub_workflow_tokens:
            cancel_entry = self._running_sub_cancel_entry(job.sub_workflows.get(sub_workflow_token))
            if cancel_entry is not None:
                running_subs.append(cancel_entry)

        return running_subs

    def _running_sub_cancel_entry(
        self,
        sub_workflow: SubWorkflowInfo | None,
    ) -> tuple[str, str, tuple[str, int]] | None:
        """The cancel entry of a sub still running on a known worker, else None."""
        if not self._sub_workflow_is_running(sub_workflow):
            return None

        worker = self._state.get_worker(sub_workflow.token.worker_id)
        if not worker:
            return None

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
        return (
            str(sub_workflow.token),
            sub_workflow.token.worker_id,
            worker_addr,
        )

    def _sub_workflow_is_running(self, sub_workflow: SubWorkflowInfo | None) -> bool:
        """True for a sub bound to a worker that is neither superseded (its
        worker lost) nor finished (its result reported)."""
        if not (sub_workflow and sub_workflow.token.worker_id):
            return False

        return self._sub_workflow_is_unfinished(sub_workflow)

    def _sub_workflow_is_unfinished(self, sub_workflow: SubWorkflowInfo) -> bool:
        """True when the sub was not superseded and reported no result yet."""
        return not sub_workflow.superseded and sub_workflow.result is None

    async def cancel_running_workflows(
        self,
        job: JobInfo,
        requester_id: str,
        timestamp: float,
        reason: str,
        workflows_to_cancel: list[tuple[str, str, tuple[str, int]]],
    ) -> tuple[list[str], dict[str, str]]:
        """Cancel the sub-workflows ``workflows_to_cancel`` on their workers.
        Returns (cancelled_list, errors_dict).

        The caller computes the list (and seeds the pending tracker from
        it) before this sends anything: that closes the race where worker
        completions arrive before the manager's pending tracker is seeded.
        """
        running_cancelled: list[str] = []
        workflow_errors: dict[str, str] = {}

        for workflow_id, worker_id, worker_addr in workflows_to_cancel:
            success, error_msg = await self.cancel_running_workflow_on_worker(
                job.job_id,
                workflow_id,
                worker_addr,
                requester_id,
                timestamp,
                reason,
            )

            self._record_running_cancel_result(
                running_cancelled, workflow_errors, workflow_id, success, error_msg
            )

        return running_cancelled, workflow_errors

    def _record_running_cancel_result(
        self,
        running_cancelled: list[str],
        workflow_errors: dict[str, str],
        workflow_id: str,
        success: bool,
        error_msg: str | None,
    ) -> None:
        """File one worker cancel result as cancelled or as an error."""
        if success:
            running_cancelled.append(workflow_id)
        elif error_msg:
            workflow_errors[workflow_id] = error_msg

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

        results = await asyncio.gather(
            *(
                self._dispatch_cancel_job_workflows_to_worker(
                    job_id, requester_id, timestamp, reason, worker_id, worker
                )
                for worker_id, worker in worker_entries
            ),
            return_exceptions=False,
        )

        cancelled_ids, errors = self._merge_worker_broadcast_results(results)
        await self._seed_and_finalize_broadcast_cancellations(job_id, cancelled_ids)

        return cancelled_ids, errors

    async def _dispatch_cancel_job_workflows_to_worker(
        self,
        job_id: str,
        requester_id: str,
        timestamp: float,
        reason: str,
        worker_id: str,
        worker: WorkerRegistration,
    ) -> tuple[list[str], list[str]]:
        """Send one worker ``CancelJobWorkflowsRequest``; return the sub-workflow
        ids it cancelled and its errors (AD-20 broadcast)."""
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

        return self._cancel_job_workflows_result(worker_id, response)

    def _cancel_job_workflows_result(
        self,
        worker_id: str,
        response: bytes | Exception | None,
    ) -> tuple[list[str], list[str]]:
        """A worker's ``cancel_job_workflows`` reply as (cancelled ids, errors)."""
        if not isinstance(response, bytes) or not response:
            return [], [
                f"worker {worker_id[:8]}...: no response from "
                "cancel_job_workflows"
            ]

        return self._decode_cancel_job_workflows_response(worker_id, response)

    def _decode_cancel_job_workflows_response(
        self,
        worker_id: str,
        response: bytes,
    ) -> tuple[list[str], list[str]]:
        """Decode a ``CancelJobWorkflowsResponse``, reporting a decode failure as an error."""
        try:
            decoded = CancelJobWorkflowsResponse.load(response)
        except Exception as decode_error:
            return [], [
                f"worker {worker_id[:8]}...: decode response: "
                f"{decode_error}"
            ]

        return list(decoded.cancelled_workflow_ids), list(decoded.errors)

    def _merge_worker_broadcast_results(
        self,
        results: list[tuple[list[str], list[str]]],
    ) -> tuple[list[str], list[str]]:
        """Concatenate every worker's (cancelled ids, errors) in worker order."""
        cancelled_ids: list[str] = []
        errors: list[str] = []
        for per_worker_ids, per_worker_errors in results:
            cancelled_ids.extend(per_worker_ids)
            errors.extend(per_worker_errors)

        return cancelled_ids, errors

    async def _seed_and_finalize_broadcast_cancellations(
        self,
        job_id: str,
        cancelled_ids: list[str],
    ) -> None:
        """Seed the pending tracker with the broadcast's cancelled ids, then
        finalize each from the workers' synchronous confirmation (AD-20)."""
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
            await self.finalize_workflow_cancellation(
                job_id=job_id,
                workflow_id=workflow_id,
                success=True,
                errors=[],
            )

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
            return await self._cancel_job(addr, data)

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

    async def _cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
    ) -> bytes:
        """Rate-limit, parse and fence a cancel_job request, then cancel the
        job this manager leads (AD-20)."""
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
        self._seed_job_callback_from_request(parsed)

        job = await self._resolve_cancellable_job(parsed)
        if isinstance(job, bytes):
            return job

        return await self._cancel_led_job(job, parsed)

    def _seed_job_callback_from_request(self, parsed: ParsedCancelRequest) -> None:
        """Seed the job's origin callback from the request when none is known (AD-20)."""
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
            parsed.callback_addr is not None
            and self._state.get_job_callback(parsed.job_id) is None
        ):
            self._state.set_job_callback(
                parsed.job_id, parsed.callback_addr
            )

    async def _resolve_cancellable_job(
        self,
        parsed: ParsedCancelRequest,
    ) -> "JobInfo | bytes | None":
        """The job this manager leads and may cancel, or the rejection response
        (not found, leadership transition, redirect, fence, terminal) (AD-20)."""
        job_id = parsed.job_id
        # ``get_job`` keys by token-string; ``job_id`` here is the
        # bare job id. ``get_job`` now accepts both forms (token
        # or bare id) — see ``JobManager.get_job`` — so this
        # lookup succeeds regardless of which form callers pass.
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return self._build_cancel_response(
                job_id, success=False, error="Job not found", job_not_found=True
            )

        job = await self._take_over_job_for_cancel_if_cluster_leader(job_id, job)
        if isinstance(job, bytes):
            return job

        return self._cancel_rejection_or_job(parsed, job)

    async def _take_over_job_for_cancel_if_cluster_leader(
        self,
        job_id: str,
        job: JobInfo,
    ) -> "JobInfo | bytes | None":
        """Take job leadership when this manager is the Raft DC leader but not
        yet the job leader; the job (re-read after takeover) or the transient
        rejection (AD-20)."""
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
        if self._leases.is_job_leader(job_id) or not self._is_cluster_leader():
            return job

        return await self._take_over_job_for_cancel(job_id)

    async def _take_over_job_for_cancel(self, job_id: str) -> "JobInfo | bytes | None":
        """Run the cluster-leader takeover; the re-read job, or the transient
        no-``leader_addr`` rejection when it did not commit (AD-20)."""
        old_leader_id = self._leases.get_job_leader(job_id)
        taken_over = await self._take_over_job_leadership_as_cluster_leader(
            job_id,
            old_leader_id,
        )
        if taken_over:
            return self._job_manager.get_job_by_id(job_id)

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

    def _cancel_rejection_or_job(
        self,
        parsed: ParsedCancelRequest,
        job: "JobInfo | None",
    ) -> "JobInfo | bytes | None":
        """The non-leader redirect or the precondition rejection, else the job."""
        redirect = self._redirect_non_leader_cancel(parsed.job_id, parsed.unreachable_addrs)
        if redirect is not None:
            return redirect

        return self._cancel_precondition_rejection_or_job(parsed, job)

    def _redirect_non_leader_cancel(
        self,
        job_id: str,
        client_unreachable_addrs: frozenset[tuple[str, int]],
    ) -> bytes | None:
        """The redirect response when this manager still is not the job leader."""
        # Still not the job leader → we are not the DC leader
        # either (a DC leader would have taken over above, or
        # returned the transient response). Redirect to a live
        # leader. ``_resolve_job_cancel_redirect_addr`` filters out
        # both our own SWIM-dead peers AND the addresses the client
        # reported unreachable, so we never hand back an address the
        # client already proved dead.
        if self._leases.is_job_leader(job_id):
            return None

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

    def _cancel_precondition_rejection_or_job(
        self,
        parsed: ParsedCancelRequest,
        job: "JobInfo | None",
    ) -> "JobInfo | bytes | None":
        """The fence-token mismatch rejection, else the job's terminal-status
        rejection, else the job."""
        job_id = parsed.job_id
        fence_token = parsed.fence_token
        stored_fence = self._leases.get_fence_token(job_id)
        if fence_token > 0 and stored_fence != fence_token:
            error_msg = (
                f"Fence token mismatch: expected {stored_fence}, got {fence_token}"
            )
            return self._build_cancel_response(
                job_id, success=False, error=error_msg
            )

        return self._terminal_job_cancel_rejection_or_job(job_id, job)

    def _terminal_job_cancel_rejection_or_job(
        self,
        job_id: str,
        job: "JobInfo | None",
    ) -> "JobInfo | bytes | None":
        """The already-cancelled / already-completed response, else the job."""
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

        return job

    async def _cancel_led_job(
        self,
        job: JobInfo,
        parsed: ParsedCancelRequest,
    ) -> bytes:
        """Cancel every unfinished workflow of a job this manager leads, record
        it in the AD-38 ledger, and build the AD-20 response."""
        job_id = parsed.job_id
        requester_id = parsed.requester_id
        timestamp = parsed.timestamp
        reason = parsed.reason

        # The job is cancelled from here on, before any of its
        # workflows is: a workflow cancelled within a cancelled job is
        # not one the job failed to complete.
        job.status = JobStatus.CANCELLED.value
        job.completed_at = self._clock.time()

        # Every unfinished workflow: a PENDING one is cancelled now; a
        # DISPATCHED or RUNNING one turns CANCELLING until its workers
        # stop it. Then nothing of the job is dispatched again.
        pending_cancelled, cancelling_workflow_ids = (
            await self._job_manager.cancel_workflows(job_id, None, reason)
        )
        await self.cancel_pending_workflows(job_id)

        workflows_to_cancel = self._seed_cancellation_pending_workflows(
            job_id, job, cancelling_workflow_ids
        )

        # A cancelling workflow with no sub left running is cancelled now.
        await self._finish_workflows_without_running_subs(
            job_id, cancelling_workflow_ids, workflows_to_cancel
        )

        running_cancelled, workflow_errors = await self.cancel_running_workflows(
            job,
            requester_id,
            timestamp,
            reason,
            workflows_to_cancel,
        )

        await self._stop_job_timeout_tracking(job_id)

        await self._record_job_cancellation_in_ledger(
            job,
            parsed,
            len(pending_cancelled) + len(cancelling_workflow_ids),
        )

        await self._state.increment_state_version()

        # Phase F3: emit FAILED outcomes for any still-in-flight
        # workflows whose AD-26 extension history hasn't been
        # closed by a WorkflowFinalResult yet. Cancellation is
        # treated as failure for the H8 Bayesian tuner — the
        # extension(s) didn't get the workflow to completion.
        self._emit_outcomes_for_terminal_job(
            job_id, ExtensionOutcomeKind.FAILED
        )

        total_cancelled = len(pending_cancelled) + len(cancelling_workflow_ids)
        total_errors = len(workflow_errors)
        overall_success = total_errors == 0

        error_str = self._format_workflow_errors(workflow_errors)

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
            total_cancelled, overall_success, error_str = await self._cancel_through_worker_broadcast(
                parsed,
                workflow_errors,
                total_cancelled,
                overall_success,
                error_str,
            )

        return self._build_cancel_response(
            job_id,
            success=overall_success,
            cancelled_count=total_cancelled,
            error=error_str,
        )

    def _seed_cancellation_pending_workflows(
        self,
        job_id: str,
        job: JobInfo,
        cancelling_workflow_ids: list[str],
    ) -> list[tuple[str, str, tuple[str, int]]]:
        """Seed the cancellation-pending tracker with each running sub before
        any worker is told to stop it; return those subs (AD-20)."""
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
            job, cancelling_workflow_ids
        )
        for sub_token_str, _, _ in workflows_to_cancel:
            self._state.add_cancellation_pending_workflow(
                job_id, sub_token_str
            )

        return workflows_to_cancel

    async def _finish_workflows_without_running_subs(
        self,
        job_id: str,
        cancelling_workflow_ids: list[str],
        workflows_to_cancel: list[tuple[str, str, tuple[str, int]]],
    ) -> None:
        """Cancel (CANCELLING -> CANCELLED) each cancelling workflow no sub
        of which is still running."""
        workflows_with_running_subs = {
            TrackingToken.parse(sub_token_str).workflow_id
            for sub_token_str, _, _ in workflows_to_cancel
        }
        for workflow_id in filterfalse(workflows_with_running_subs.__contains__, cancelling_workflow_ids):
            await self._job_manager.finish_workflow_cancellation(job_id, workflow_id)

    async def _stop_job_timeout_tracking(self, job_id: str) -> None:
        """Stop the job's timeout strategy tracking it, if it has one."""
        strategy = self._state.get_job_timeout_strategy(job_id)
        if strategy:
            await strategy.stop_tracking(job_id, "cancelled")

    async def _record_job_cancellation_in_ledger(
        self,
        job: JobInfo,
        parsed: ParsedCancelRequest,
        workflows_cancelled: int,
    ) -> None:
        """Record the cancellation's request, acknowledgement and completion in
        the AD-38 job ledger, then discard the persisted submission."""
        if self._get_job_ledger() is None:
            return

        job_id = parsed.job_id
        await self._log_ledger_shortfall(
            "JobCancellationRequested",
            job_id,
            await self._get_job_ledger().request_cancellation(
                job_id,
                reason=parsed.reason,
                requestor_id=parsed.requester_id,
                durability=DurabilityLevel.REGIONAL,
            ),
        )
        # This datacenter has now cancelled what it was running:
        # its pending workflows, and the dispatched and running ones
        # its workers are stopping (AD-38 JobCancellationAcked) --
        # each workflow once.
        await self._log_ledger_shortfall(
            "JobCancellationAcked",
            job_id,
            await self._get_job_ledger().acknowledge_cancellation(
                job_id,
                datacenter_id=self._node_id.datacenter,
                workflows_cancelled=workflows_cancelled,
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

    def _format_workflow_errors(self, workflow_errors: dict[str, str]) -> str | None:
        """The AD-20 response's error summary of per-workflow errors, or None."""
        if not workflow_errors:
            return None

        error_details = [
            f"{workflow_id[:8]}...: {err}"
            for workflow_id, err in workflow_errors.items()
        ]
        return f"{len(workflow_errors)} workflow(s) failed: {'; '.join(error_details)}"

    async def _cancel_through_worker_broadcast(
        self,
        parsed: ParsedCancelRequest,
        workflow_errors: dict[str, str],
        total_cancelled: int,
        overall_success: bool,
        error_str: str | None,
    ) -> tuple[int, bool, str | None]:
        """Broadcast the cancel to every worker when the local view found nothing
        to stop; the updated (total_cancelled, overall_success, error_str) (AD-20)."""
        job_id = parsed.job_id
        broadcast_cancelled, broadcast_errors = (
            await self._broadcast_cancel_job_workflows_to_workers(
                job_id=job_id,
                requester_id=parsed.requester_id,
                timestamp=parsed.timestamp,
                reason=parsed.reason,
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
            self._push_cancellation_complete_after_empty_broadcast(
                job_id, overall_success, workflow_errors, broadcast_errors
            )
        if broadcast_errors:
            overall_success, error_str = self._merge_broadcast_errors(
                workflow_errors, broadcast_errors, error_str
            )

        return total_cancelled, overall_success, error_str

    def _push_cancellation_complete_after_empty_broadcast(
        self,
        job_id: str,
        overall_success: bool,
        workflow_errors: dict[str, str],
        broadcast_errors: list[str],
    ) -> None:
        """Push the job's cancellation completion to its origin now: no worker
        runs anything of it, so no zero-pending drain will (AD-20)."""
        self._task_runner.run(
            self._push_cancellation_complete_to_origin,
            job_id,
            overall_success and not broadcast_errors,
            list(workflow_errors.values()) + broadcast_errors,
        )

    def _merge_broadcast_errors(
        self,
        workflow_errors: dict[str, str],
        broadcast_errors: list[str],
        error_str: str | None,
    ) -> tuple[bool, str | None]:
        """Fold the broadcast's errors into ``workflow_errors``; the updated
        (overall_success, error_str)."""
        workflow_errors.update(
            {
                f"broadcast_error_{idx}": err
                for idx, err in enumerate(broadcast_errors)
            }
        )
        overall_success = len(workflow_errors) == 0
        if not error_str:
            error_str = self._format_workflow_errors(workflow_errors)

        return overall_success, error_str

    async def finalize_workflow_cancellation(
        self,
        job_id: str,
        workflow_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """Decrement the pending-cancellation tracker and, if this
        was the last outstanding workflow, fire the origin push. The
        tracker holds sub-workflow tokens: a workflow whose last tracked
        sub drains is cancelled (CANCELLING -> CANCELLED).

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
        self._record_cancellation_errors(job_id, workflow_id, success, errors)

        remaining_pending = (
            self._state.get_cancellation_pending_workflows(job_id)
        )

        # The workflow's last running sub stopped: it is cancelled.
        await self._finish_parent_workflow_if_drained(job_id, workflow_id, remaining_pending)

        if remaining_pending:
            return

        # All workflows have reported — push to origin.
        aggregated_errors = self._state.get_cancellation_errors(job_id)
        aggregated_success = len(aggregated_errors) == 0

        self._task_runner.run(
            self._push_cancellation_complete_to_origin,
            job_id,
            aggregated_success,
            aggregated_errors,
        )

        self._state.clear_cancellation_pending_workflows(job_id)

    def _record_cancellation_errors(
        self,
        job_id: str,
        workflow_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """Add a failed cancellation's worker-reported errors to the job's
        cancellation errors (AD-20 aggregation)."""
        if success:
            return

        for error in errors:
            self._state.add_cancellation_error(
                job_id, f"Workflow {workflow_id[:8]}...: {error}"
            )

    async def _finish_parent_workflow_if_drained(
        self,
        job_id: str,
        workflow_id: str,
        remaining_pending: set[str],
    ) -> None:
        """Cancel the sub's parent workflow once none of its subs is pending."""
        parent_workflow_id = TrackingToken.parse(workflow_id).workflow_id
        if not parent_workflow_id or self._has_pending_sub_of(parent_workflow_id, remaining_pending):
            return

        await self._finish_drained_parent_workflow(job_id, parent_workflow_id)

    def _has_pending_sub_of(
        self,
        parent_workflow_id: str,
        remaining_pending: set[str],
    ) -> bool:
        """True when a pending sub-workflow token belongs to the parent workflow."""
        return any(
            TrackingToken.parse(pending_sub_workflow).workflow_id == parent_workflow_id
            for pending_sub_workflow in remaining_pending
        )

    async def _finish_drained_parent_workflow(
        self,
        job_id: str,
        parent_workflow_id: str,
    ) -> None:
        """Cancel the drained parent workflow; complete the job if that was its last."""
        if await self._job_manager.finish_workflow_cancellation(job_id, parent_workflow_id):
            await self._complete_job_if_done(job_id)

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
            return await self._receive_workflow_cancellation_complete(data)

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

    async def _receive_workflow_cancellation_complete(self, data: bytes) -> bytes:
        """Forward a worker's cancellation completion to the job leader, or
        finalize it here when this manager leads the job (AD-20)."""
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
        if not self._leases.is_job_leader(job_id) and (
            forwarded_response := await self._forward_cancellation_complete_to_leader(
                job_id, workflow_id, data
            )
        ) is not None:
            return forwarded_response

        # Track this workflow as complete. The finalize pushes the
        # job's cancellation completion to its origin exactly once,
        # when the last pending workflow drains; the coordinator's
        # handle_workflow_cancelled, also called here, found the set
        # already drained and notified the client a second time.
        await self._finalize_reported_cancellation(completion)

        return b"OK"

    async def _finalize_reported_cancellation(
        self,
        completion: WorkflowCancellationComplete,
    ) -> None:
        """Finalize the pending-cancellation entry a worker's push reports."""
        await self.finalize_workflow_cancellation(
            job_id=completion.job_id,
            workflow_id=completion.workflow_id,
            success=completion.success,
            errors=list(completion.errors or []),
        )

    async def _forward_cancellation_complete_to_leader(
        self,
        job_id: str,
        workflow_id: str,
        data: bytes,
    ) -> bytes | None:
        """Forward the completion to each candidate leader in priority order;
        the first accepted response, or None when none accepted it (AD-20)."""
        forward_targets = self._cancellation_complete_forward_targets(job_id)
        for target_addr in forward_targets:
            response = await self._try_forward_cancellation_complete(
                target_addr, job_id, workflow_id, data
            )
            if response is not None:
                return response

        return None

    def _cancellation_complete_forward_targets(self, job_id: str) -> list[tuple[str, int]]:
        """The live, non-self managers to forward a completion to, in priority order."""
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
        self._append_leader_forward_target(forward_targets, cached_leader_addr, self_addr)

        dc_leader_addr = self._resolve_dc_leader_addr()
        self._append_leader_forward_target(forward_targets, dc_leader_addr, self_addr)

        for peer_addr in sorted(
            self._state.get_active_manager_peers()
        ):
            if self._is_forward_target(tuple(peer_addr), self_addr, forward_targets):
                forward_targets.append(tuple(peer_addr))

        return forward_targets

    def _append_leader_forward_target(
        self,
        forward_targets: list[tuple[str, int]],
        leader_addr: tuple[str, int] | None,
        self_addr: tuple[str, int],
    ) -> None:
        """Append a known leader address when it is a live, new forward target."""
        if leader_addr is None:
            return

        if self._is_forward_target(tuple(leader_addr), self_addr, forward_targets):
            forward_targets.append(tuple(leader_addr))

    def _is_forward_target(
        self,
        candidate_addr: tuple[str, int],
        self_addr: tuple[str, int],
        forward_targets: list[tuple[str, int]],
    ) -> bool:
        """True for a manager other than this one, not yet listed, and live in SWIM."""
        return (
            candidate_addr != self_addr
            and candidate_addr not in forward_targets
            and self._manager_tcp_addr_is_live(candidate_addr)
        )

    async def _try_forward_cancellation_complete(
        self,
        target_addr: tuple[str, int],
        job_id: str,
        workflow_id: str,
        data: bytes,
    ) -> bytes | None:
        """Forward the completion to one target; its accepted response, or None
        (a failure is logged and the next target tried)."""
        try:
            response, _clock = await self._send_tcp(
                target_addr,
                "workflow_cancellation_complete",
                data,
                timeout=self._config.tcp_timeout_standard_seconds,
            )
            return self._accepted_forward_response(response)
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

        return None

    def _accepted_forward_response(self, response: bytes | Exception | None) -> bytes | None:
        """The forward's response when the target accepted it; raises a transport error."""
        # send_tcp returns transport errors rather than raising.
        if isinstance(response, Exception):
            raise response

        return self._non_error_response(response)

    def _non_error_response(self, response: bytes | None) -> bytes | None:
        """The response unless it is empty, ``ERROR`` or not bytes."""
        return (
            response
            if isinstance(response, bytes) and response not in (b"", b"ERROR")
            else None
        )

    async def handle_workflow_cancellation_query(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow cancellation query from worker."""
        try:
            return self._answer_workflow_cancellation_query(data)

        except Exception as error:
            return WorkflowCancellationResponse(
                job_id="unknown",
                workflow_id="unknown",
                workflow_name="",
                status="ERROR",
                error=str(error),
            ).dump()

    def _answer_workflow_cancellation_query(self, data: bytes) -> bytes:
        """The job- or workflow-level cancellation status a worker asked about."""
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
        return self._sub_workflow_cancellation_status(query, job)

    def _sub_workflow_cancellation_status(
        self,
        query: WorkflowCancellationQuery,
        job: JobInfo,
    ) -> bytes:
        """The queried sub-workflow's status, or UNKNOWN when the job has no such sub."""
        for sub_wf in job.sub_workflows.values():
            if str(sub_wf.token) == query.workflow_id:
                return self._sub_workflow_status_response(query, sub_wf)

        return WorkflowCancellationResponse(
            job_id=query.job_id,
            workflow_id=query.workflow_id,
            workflow_name="",
            status="UNKNOWN",
            error="Workflow not found",
        ).dump()

    def _sub_workflow_status_response(
        self,
        query: WorkflowCancellationQuery,
        sub_wf: SubWorkflowInfo,
    ) -> bytes:
        """The sub's last reported name and status (RUNNING before any progress)."""
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

    async def handle_cancel_single_workflow(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Cancel one workflow of a job -- and, when asked, every workflow that
        waits on it (Section 6, AD-54).

        Only the job's leader cancels; another manager forwards the request
        to it. A PENDING workflow is cancelled at once. A DISPATCHED or
        RUNNING one turns CANCELLING, its workers are told to stop it, and
        it is CANCELLED once they all confirmed (or, failing that, when
        their cancelled results arrive). Its dependents can never run: they
        are cancelled with it -- or, when the request keeps them, fail, as
        any workflow whose dependency did not complete does. The job goes
        on; what was cancelled counts as not completed.
        """
        try:
            return await self._cancel_single_workflow(addr, data)

        except Exception as error:
            return SingleWorkflowCancelResponse(
                job_id="unknown",
                workflow_id="unknown",
                request_id="unknown",
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=[str(error)],
                datacenter=self._node_id.datacenter,
            ).dump()

    async def _cancel_single_workflow(
        self,
        addr: tuple[str, int],
        data: bytes,
    ) -> bytes:
        """Rate-limit the single-workflow cancel and resolve its job (AD-54)."""
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

        job = self._job_manager.get_job_by_id(request.job_id)
        if not job:
            return SingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=["Job not found"],
                datacenter=self._node_id.datacenter,
            ).dump()

        return await self._cancel_single_workflow_as_leader(request, data, job)

    async def _cancel_single_workflow_as_leader(
        self,
        request: SingleWorkflowCancelRequest,
        data: bytes,
        job: JobInfo,
    ) -> bytes:
        """Forward to the job leader, settle an already-terminal workflow, or
        cancel it here as the job's leader (AD-54)."""
        if not self._leases.is_job_leader(request.job_id):
            return await self._forward_single_workflow_cancel(request, data)

        lifecycle = self._job_manager.workflow_lifecycle
        requested_state = lifecycle.get_state(request.job_id, request.workflow_id)
        if (settled_response := self._settled_workflow_cancel_response(request, requested_state)) is not None:
            return settled_response

        return await self._cancel_unsettled_workflow(request, job, lifecycle)

    async def _forward_single_workflow_cancel(
        self,
        request: SingleWorkflowCancelRequest,
        data: bytes,
    ) -> bytes:
        """Forward the request to the job's live leader, or answer NOT_FOUND."""
        leader_addr = self._leases.get_job_leader_addr(request.job_id)
        if leader_addr is None or not self._manager_tcp_addr_is_live(tuple(leader_addr)):
            return SingleWorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                request_id=request.request_id,
                status=WorkflowCancellationStatus.NOT_FOUND.value,
                errors=["Not the job's leader, and its leader is unknown or unreachable"],
                datacenter=self._node_id.datacenter,
            ).dump()

        return await self._send_single_workflow_cancel_to_leader(tuple(leader_addr), data)

    async def _send_single_workflow_cancel_to_leader(
        self,
        leader_addr: tuple[str, int],
        data: bytes,
    ) -> bytes:
        """Send the request to the job leader; its response, raising a transport error."""
        forwarded, _clock = await self._send_tcp(
            leader_addr,
            "receive_cancel_single_workflow",
            data,
            timeout=self._config.tcp_timeout_standard_seconds,
        )
        # send_tcp returns transport errors rather than raising.
        if isinstance(forwarded, Exception):
            raise forwarded
        return forwarded

    def _settled_workflow_cancel_response(
        self,
        request: SingleWorkflowCancelRequest,
        requested_state: WorkflowState | None,
    ) -> bytes | None:
        """The response for a workflow absent, cancelled, cancelling or past
        running; None when it is still to be cancelled."""
        cancellation_status = self._settled_cancellation_status(requested_state)
        if cancellation_status is None:
            return None

        return SingleWorkflowCancelResponse(
            job_id=request.job_id,
            workflow_id=request.workflow_id,
            request_id=request.request_id,
            status=cancellation_status.value,
            errors=["Workflow not found"] if requested_state is None else [],
            datacenter=self._node_id.datacenter,
        ).dump()

    def _settled_cancellation_status(
        self,
        requested_state: WorkflowState | None,
    ) -> WorkflowCancellationStatus | None:
        """NOT_FOUND for an unknown workflow, else its settled status by state."""
        if requested_state is None:
            return WorkflowCancellationStatus.NOT_FOUND

        return SETTLED_CANCELLATION_STATUS_BY_STATE.get(
            requested_state, WorkflowCancellationStatus.ALREADY_COMPLETED
        )

    async def _cancel_unsettled_workflow(
        self,
        request: SingleWorkflowCancelRequest,
        job: JobInfo,
        lifecycle: WorkflowLifecycleStateMachine,
    ) -> bytes:
        """Cancel the workflow (and its dependents when asked), stop its running
        subs, and report the outcome (AD-54)."""
        # Every workflow waiting on it, directly or transitively.
        dependent_workflow_ids = self._transitive_dependent_workflow_ids(
            self._dependents_by_dependency(job), request.workflow_id
        )

        reason = f"cancelled by request {request.request_id} from {request.requester_id}"
        cancelled_workflow_ids, cancelling_workflow_ids = await self._job_manager.cancel_workflows(
            request.job_id,
            {request.workflow_id, *dependent_workflow_ids}
            if request.cancel_dependents
            else {request.workflow_id},
            reason,
        )
        workflow_dispatcher = self._get_workflow_dispatcher()
        await self._remove_from_dispatch_queue(
            workflow_dispatcher, request.job_id, [*cancelled_workflow_ids, *cancelling_workflow_ids]
        )
        await self._fail_kept_dependents(request, workflow_dispatcher, dependent_workflow_ids)

        workflows_to_cancel = self.get_running_workflows_to_cancel(job, cancelling_workflow_ids)
        running_cancelled, workflow_errors = await self.cancel_running_workflows(
            job,
            request.requester_id,
            request.timestamp,
            reason,
            workflows_to_cancel,
        )
        # A workflow whose workers all confirmed stopping it (or with no
        # sub left running) is cancelled now; one with an unconfirmed
        # sub stays CANCELLING until its cancelled result arrives.
        await self._finish_confirmed_workflow_cancellations(
            request.job_id, cancelling_workflow_ids, workflows_to_cancel, running_cancelled
        )
        await self._complete_job_if_done(request.job_id)

        cancellation_status = self._single_workflow_cancel_outcome(
            request, lifecycle, cancelled_workflow_ids
        )
        return self._single_workflow_cancel_response(
            request,
            cancellation_status,
            cancelled_workflow_ids,
            cancelling_workflow_ids,
            workflow_errors,
        )

    def _dependents_by_dependency(self, job: JobInfo) -> dict[str, list[str]]:
        """Each workflow id mapped to the workflows that depend on it directly."""
        dependents_by_dependency: dict[str, list[str]] = {}
        for workflow_info in job.workflows.values():
            for dependency_workflow_id in workflow_info.dependency_workflow_ids:
                dependents_by_dependency.setdefault(dependency_workflow_id, []).append(
                    self._workflow_id_of(workflow_info)
                )
        return dependents_by_dependency

    def _workflow_id_of(self, workflow_info: WorkflowInfo) -> str:
        """The workflow's id, or "" when its token carries none."""
        return workflow_info.token.workflow_id or ""

    def _transitive_dependent_workflow_ids(
        self,
        dependents_by_dependency: dict[str, list[str]],
        workflow_id: str,
    ) -> set[str]:
        """Every workflow depending on ``workflow_id``, directly or transitively
        (iterative walk; no recursion)."""
        dependent_workflow_ids: set[str] = set()
        unvisited_workflow_ids = [workflow_id]
        while unvisited_workflow_ids:
            for dependent_workflow_id in filterfalse(
                dependent_workflow_ids.__contains__,
                dependents_by_dependency.get(unvisited_workflow_ids.pop(), ()),
            ):
                dependent_workflow_ids.add(dependent_workflow_id)
                unvisited_workflow_ids.append(dependent_workflow_id)
        return dependent_workflow_ids

    async def _remove_from_dispatch_queue(
        self,
        workflow_dispatcher: "WorkflowDispatcher | None",
        job_id: str,
        workflow_ids: list[str],
    ) -> None:
        """Drop the workflows from the dispatch queue when a dispatcher exists."""
        if workflow_dispatcher is not None:
            await workflow_dispatcher.remove_pending_workflows(job_id, workflow_ids)

    async def _fail_kept_dependents(
        self,
        request: SingleWorkflowCancelRequest,
        workflow_dispatcher: "WorkflowDispatcher | None",
        dependent_workflow_ids: set[str],
    ) -> None:
        """Fail the dependents a request keeps: their dependency never completes."""
        if request.cancel_dependents or not dependent_workflow_ids:
            return

        failed_dependent_ids = await self._job_manager.fail_workflow_dependents(
            request.job_id,
            request.workflow_id,
            f"dependency {request.workflow_id} was cancelled — dependent workflow can never dispatch",
        )
        await self._remove_from_dispatch_queue(workflow_dispatcher, request.job_id, failed_dependent_ids)

    async def _finish_confirmed_workflow_cancellations(
        self,
        job_id: str,
        cancelling_workflow_ids: list[str],
        workflows_to_cancel: list[tuple[str, str, tuple[str, int]]],
        running_cancelled: list[str],
    ) -> None:
        """Cancel each cancelling workflow whose running subs all confirmed stopping."""
        for workflow_id in cancelling_workflow_ids:
            if self._all_running_subs_confirmed(workflow_id, workflows_to_cancel, running_cancelled):
                await self._job_manager.finish_workflow_cancellation(job_id, workflow_id)

    def _all_running_subs_confirmed(
        self,
        workflow_id: str,
        workflows_to_cancel: list[tuple[str, str, tuple[str, int]]],
        running_cancelled: list[str],
    ) -> bool:
        """True when every running sub of the workflow confirmed it stopped."""
        return all(
            sub_token_str in running_cancelled
            for sub_token_str, _, _ in workflows_to_cancel
            if TrackingToken.parse(sub_token_str).workflow_id == workflow_id
        )

    def _single_workflow_cancel_outcome(
        self,
        request: SingleWorkflowCancelRequest,
        lifecycle: WorkflowLifecycleStateMachine,
        cancelled_workflow_ids: list[str],
    ) -> WorkflowCancellationStatus:
        """PENDING_CANCELLED, CANCELLED or CANCELLING: where the requested workflow ended."""
        if request.workflow_id in cancelled_workflow_ids:
            return WorkflowCancellationStatus.PENDING_CANCELLED
        if lifecycle.get_state(request.job_id, request.workflow_id) == WorkflowState.CANCELLED:
            return WorkflowCancellationStatus.CANCELLED
        return WorkflowCancellationStatus.CANCELLING

    def _single_workflow_cancel_response(
        self,
        request: SingleWorkflowCancelRequest,
        cancellation_status: WorkflowCancellationStatus,
        cancelled_workflow_ids: list[str],
        cancelling_workflow_ids: list[str],
        workflow_errors: dict[str, str],
    ) -> bytes:
        """The single-workflow cancel response, listing the dependents cancelled with it."""
        return SingleWorkflowCancelResponse(
            job_id=request.job_id,
            workflow_id=request.workflow_id,
            request_id=request.request_id,
            status=cancellation_status.value,
            cancelled_dependents=[
                workflow_id
                for workflow_id in (*cancelled_workflow_ids, *cancelling_workflow_ids)
                if workflow_id != request.workflow_id
            ],
            errors=list(workflow_errors.values()),
            datacenter=self._node_id.datacenter,
        ).dump()

    async def stop_dispatched_plans(
        self,
        job_id: str,
        plans: list[tuple[str, str]],
    ) -> None:
        """Stop sub-workflows a worker took after their workflow began
        cancelling: the cancellation crossed their dispatch, so it found
        nothing to stop on that worker. Each ``(sub_workflow_token,
        worker_id)`` gets the cancel the cancellation would have sent."""
        for sub_workflow_token, worker_id in plans:
            await self._stop_dispatched_plan(job_id, sub_workflow_token, worker_id)

    async def _stop_dispatched_plan(
        self,
        job_id: str,
        sub_workflow_token: str,
        worker_id: str,
    ) -> None:
        """Send one crossed-dispatch sub its cancel; log when the worker refuses."""
        if (worker := self._state.get_worker(worker_id)) is None:
            return
        stopped, error = await self.cancel_running_workflow_on_worker(
            job_id,
            sub_workflow_token,
            (worker.node.host, worker.node.port),
            self._node_id.full,
            self._clock.time(),
            "its workflow was cancelled while it was being dispatched",
        )
        if not stopped:
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Could not stop sub-workflow {sub_workflow_token[:8]}... dispatched "
                        f"into its workflow's cancellation on worker {worker_id[:8]}...: {error}"
                    ),
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id.short,
                )
            )
