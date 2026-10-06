"""
Gate job dispatch coordination module.

Dispatches an admitted job to its datacenters' managers (submission
admission is GateJobHandler's).
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.models import (
    GlobalJobStatus,
    JobSubmission,
    JobAck,
    JobStatus,
    JobStatusPush,
    restricted_loads,
)
from hyperscale.distributed.capacity import (
    DatacenterCapacity,
    DatacenterCapacityAggregator,
    SpilloverEvaluator,
)
from hyperscale.distributed.nodes.gate.datacenter_manager_selector import (
    DatacenterManagerSelector,
)
from hyperscale.distributed.nodes.gate.models import TransientDispatchError
from hyperscale.distributed.protocol.transient_errors import (
    is_transient_rejection,
)
from hyperscale.distributed.reliability import (
    RetryExecutor,
    RetryConfig,
    JitterStrategy,
)
from hyperscale.logging.hyperscale_logging_models import (
    ObservedLatencyRecorded,
    ServerWarning,
    ServerInfo,
    ServerError,
)

from hyperscale.distributed.runtime import Clock



if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.distributed.jobs.gates import GateJobManager, GateJobTimeoutTracker
    from hyperscale.distributed.routing import ObservedLatencyTracker
    from hyperscale.distributed.health import CircuitBreakerManager
    from hyperscale.distributed.swim.core import ErrorStats
    from hyperscale.logging import Logger
    from hyperscale.distributed.taskex import TaskRunner


class GateDispatchCoordinator:
    """
    Coordinates job dispatch to datacenter managers.

    Responsibilities:
    - Handle job submissions from clients
    - Select target datacenters
    - Dispatch jobs to managers
    - Track job state
    """

    def __init__(
        self,
        state: "GateRuntimeState",
        logger: "Logger",
        task_runner: "TaskRunner",
        job_manager: "GateJobManager",
        job_timeout_tracker: "GateJobTimeoutTracker",
        circuit_breaker_manager: "CircuitBreakerManager",
        datacenter_managers: dict[str, list[tuple[str, int]]],
        quorum_circuit: "ErrorStats",
        select_datacenters: Callable[..., Awaitable[tuple[list[str], list[str], str]]],
        broadcast_leadership: Callable[
            [str, int, tuple[str, int] | None], Awaitable[None]
        ],
        send_tcp: Callable,
        increment_version: Callable,
        confirm_manager_for_dc: Callable,
        suspect_manager_for_dc: Callable,
        record_forward_throughput_event: Callable,
        record_forward_attempt_event: Callable,
        get_node_host: Callable[[], str],
        get_node_port: Callable[[], int],
        get_node_id_short: Callable[[], str],
        manager_dispatch_timeout_seconds: float,
        client_push_timeout_seconds: float,
        clock: Clock,
        capacity_aggregator: DatacenterCapacityAggregator | None = None,
        spillover_evaluator: SpilloverEvaluator | None = None,
        record_dispatch_failure: Callable[[str, str], None] | None = None,
        persist_accepted_job: Callable[[JobSubmission, list[str], int], Awaitable[None]] | None = None,
        *,
        observed_latency_tracker: "ObservedLatencyTracker",
        estimate_datacenter_latencies_ms: Callable[[], dict[str, float]],
        manager_selector: DatacenterManagerSelector,
        finalize_failed_job: Callable[[str, tuple[str, ...], str], Awaitable[None]],
        on_job_dispatched: Callable[[JobSubmission, list[str]], Awaitable[None]],
        datacenter_leader_failover_seconds: float,
        leader_heartbeat_interval_seconds: float,
        record_fallback_used: Callable[[str, str], None],
    ) -> None:
        # Told which datacenters actually accepted a job (AD-44 best-effort
        # tracking starts from exactly those).
        self._clock: Clock = clock
        self._on_job_dispatched = on_job_dispatched
        # Every terminal transition goes through the gate's terminal hook:
        # the AD-38 terminal record (these two FAILED paths never wrote
        # one) and the terminal status replicated to peer gates.
        self._finalize_failed_job = finalize_failed_job
        self._manager_dispatch_timeout_seconds: float = (
            manager_dispatch_timeout_seconds
        )
        # A datacenter answering "retry" is replacing its leader: its
        # dispatch keeps retrying for as long as that can take, at the pace
        # its managers learn a new leader -- one leader heartbeat (full
        # jitter): waiting longer only delays the job past the election,
        # asking sooner mostly re-asks a question whose answer has not
        # changed.
        self._dispatch_retry_config = RetryConfig(
            max_attempts=None,
            base_delay=leader_heartbeat_interval_seconds,
            max_delay=leader_heartbeat_interval_seconds,
            jitter=JitterStrategy.FULL,
            # Transient JobAck rejections (mid-election, warmup, load
            # shedding) must retry alongside the transport errors the
            # default whitelist covers.
            retryable_exceptions=(
                ConnectionError,
                TimeoutError,
                OSError,
                TransientDispatchError,
            ),
        )
        self._datacenter_leader_failover_seconds = datacenter_leader_failover_seconds
        # AD-36: counts each fallback a dispatch lands on, from -> to.
        self._record_fallback_used = record_fallback_used
        # AD-28: orders a datacenter's managers for dispatch (known leader
        # first, then rendezvous + EWMA) and learns from dispatch outcomes.
        self._manager_selector: DatacenterManagerSelector = manager_selector
        self._state: "GateRuntimeState" = state
        self._logger: "Logger" = logger
        self._task_runner: "TaskRunner" = task_runner
        self._job_manager: "GateJobManager" = job_manager
        self._job_timeout_tracker: "GateJobTimeoutTracker" = job_timeout_tracker
        # Phase 8 gate durable tier: awaited with (submission,
        # successful_dcs, fence_token) at the acceptance point so a
        # restarted gate recovers its accepted jobs. None = volatile
        # gate (the pre-Phase-8 behavior).
        self._persist_accepted_job: Callable[[JobSubmission, list[str], int], Awaitable[None]] | None = (
            persist_accepted_job
        )
        self._circuit_breaker_manager: "CircuitBreakerManager" = circuit_breaker_manager
        self._datacenter_managers: dict[str, list[tuple[str, int]]] = (
            datacenter_managers
        )
        self._quorum_circuit: "ErrorStats" = quorum_circuit
        self._select_datacenters: Callable[..., Awaitable[tuple[list[str], list[str], str]]] = select_datacenters
        self._broadcast_leadership: Callable[
            [str, int, tuple[str, int] | None], Awaitable[None]
        ] = broadcast_leadership
        self._send_tcp: Callable = send_tcp
        self._client_push_timeout_seconds: float = client_push_timeout_seconds
        self._increment_version: Callable = increment_version
        self._confirm_manager_for_dc: Callable = confirm_manager_for_dc
        self._suspect_manager_for_dc: Callable = suspect_manager_for_dc
        self._record_forward_throughput_event: Callable = (
            record_forward_throughput_event
        )
        self._record_forward_attempt_event: Callable = record_forward_attempt_event
        self._get_node_host: Callable[[], str] = get_node_host
        self._get_node_port: Callable[[], int] = get_node_port
        self._get_node_id_short: Callable[[], str] = get_node_id_short
        self._capacity_aggregator: DatacenterCapacityAggregator | None = (
            capacity_aggregator
        )
        self._spillover_evaluator: SpilloverEvaluator | None = spillover_evaluator
        # AD-45: learns each datacenter's time to accept a dispatched job.
        self._observed_latency_tracker: "ObservedLatencyTracker" = observed_latency_tracker
        # Every known datacenter's latency estimate, as routing ranks
        # them: spillover weighs the same numbers.
        self._estimate_datacenter_latencies_ms = estimate_datacenter_latencies_ms
        self._record_dispatch_failure: Callable[[str, str], None] | None = (
            record_dispatch_failure
        )

    async def _push_job_status_to_client(
        self,
        job_id: str,
        status: str,
        message: str,
        *,
        is_final: bool = False,
    ) -> None:
        """Push a gate-owned job status update to the client callback."""
        callback = self._job_manager.get_callback(job_id)
        if callback is None:
            return

        elapsed_seconds, total_completed, total_failed, overall_rate = self._job_progress_snapshot(job_id)

        push = JobStatusPush(
            job_id=job_id,
            status=status,
            message=message,
            total_completed=total_completed,
            total_failed=total_failed,
            overall_rate=overall_rate,
            elapsed_seconds=elapsed_seconds,
            is_final=is_final,
            fence_token=self._job_manager.get_fence_token(job_id),
            callback_addr=callback,
        )
        payload = push.dump()
        sequence = await self._state.record_client_update(
            job_id,
            "job_status_push",
            payload,
            self._clock.monotonic(),
        )

        await self._deliver_status_push(job_id, status, callback, payload, sequence)

    def _job_progress_snapshot(self, job_id: str) -> tuple[float, int, int, float]:
        """The job's elapsed seconds, completions, failures and rate; zeros
        for a job not held here."""
        job = self._job_manager.get_job(job_id)
        if job is None:
            return 0.0, 0, 0, 0.0
        return self._job_elapsed_seconds(job), job.total_completed, job.total_failed, job.overall_rate

    def _job_elapsed_seconds(self, job: GlobalJobStatus) -> float:
        """Seconds since the job started, or 0.0 when it has no start."""
        return max(0.0, self._clock.monotonic() - job.timestamp) if job.timestamp > 0 else 0.0

    async def _deliver_status_push(
        self,
        job_id: str,
        status: str,
        callback: tuple[str, int],
        payload: bytes,
        sequence: int,
    ) -> None:
        """Send the push and record how far the client received updates; a
        failed push is logged."""
        try:
            response, _ = await self._send_tcp(
                callback,
                "job_status_push",
                payload,
                timeout=self._client_push_timeout_seconds,
            )
            self._raise_if_push_failed(response)
            await self._state.set_client_update_position(job_id, callback, sequence)
        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Failed to push gate status {status} for "
                        f"{job_id[:8]}... to client {callback}: {error}"
                    ),
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                )
            )

    @staticmethod
    def _raise_if_push_failed(response: bytes | Exception | None) -> None:
        """Raise the transport error, or a rejection the client answered."""
        if isinstance(response, Exception):
            raise response
        if response not in (b"ok", None):
            raise RuntimeError(f"status push rejected: {response!r}")

    async def dispatch_job(
        self,
        submission: JobSubmission,
        target_dcs: list[str],
    ) -> None:
        """
        Dispatch job to all target datacenters with fallback support.

        Sets origin_gate_addr so managers send results directly to this gate.
        Handles health-based routing: UNHEALTHY -> fail, DEGRADED/BUSY -> warn, HEALTHY -> proceed.
        """
        job = self._job_manager.get_job(submission.job_id)
        if not job:
            return

        submission.origin_gate_addr = (self._get_node_host(), self._get_node_port())
        job.status = JobStatus.DISPATCHING.value
        self._job_manager.set_job(submission.job_id, job)
        self._increment_version()
        await self._push_job_status_to_client(
            submission.job_id,
            JobStatus.DISPATCHING.value,
            "Job dispatching",
        )

        primary_dcs, fallback_dcs, worst_health = await self._select_datacenters(
            len(target_dcs),
            self._datacenter_preference(target_dcs),
            job_id=submission.job_id,
        )

        if await self._hold_or_fail_unplaceable_dispatch(submission, job, target_dcs, worst_health):
            return

        await self._dispatch_to_selected_datacenters(
            submission, job, primary_dcs, fallback_dcs, worst_health
        )

    @staticmethod
    def _datacenter_preference(target_dcs: list[str]) -> list[str] | None:
        """The datacenters to select among, or None for any."""
        return target_dcs if target_dcs else None

    async def _hold_or_fail_unplaceable_dispatch(
        self,
        submission: JobSubmission,
        job: GlobalJobStatus,
        target_dcs: list[str],
        worst_health: str,
    ) -> bool:
        """Hold the job PENDING while its datacenters initialize, or fail it
        when every one is unhealthy; True when it did either."""
        if worst_health == "initializing":
            await self._hold_dispatch_while_initializing(submission, job)
            return True

        if worst_health == "unhealthy":
            await self._fail_dispatch_to_unhealthy(submission, job, target_dcs)
            return True

        return False

    async def _hold_dispatch_while_initializing(self, submission: JobSubmission, job: GlobalJobStatus) -> None:
        """Return the job to PENDING while its datacenters initialize."""
        job.status = JobStatus.PENDING.value
        self._job_manager.set_job(submission.job_id, job)
        await self._logger.log(
            ServerWarning(
                message=f"Job {submission.job_id}: DCs became initializing after acceptance - waiting",
                node_host=self._get_node_host(),
                node_port=self._get_node_port(),
                node_id=self._get_node_id_short(),
            ),
        )
        self._increment_version()
        await self._push_job_status_to_client(
            submission.job_id,
            JobStatus.PENDING.value,
            "Datacenters initializing",
        )

    async def _fail_dispatch_to_unhealthy(
        self,
        submission: JobSubmission,
        job: GlobalJobStatus,
        target_dcs: list[str],
    ) -> None:
        """Fail a job whose every target datacenter is unhealthy."""
        job.status = JobStatus.FAILED.value
        job.failed_datacenters = len(target_dcs)
        self._job_manager.set_job(submission.job_id, job)
        self._quorum_circuit.record_error()
        await self._finalize_failed_job(
            submission.job_id,
            tuple(sorted(target_dcs)),
            "every target datacenter is unhealthy",
        )

        if self._record_dispatch_failure:
            for datacenter_id in target_dcs:
                self._record_dispatch_failure(submission.job_id, datacenter_id)

        await self._logger.log(
            ServerError(
                message=f"Job {submission.job_id}: All datacenters are UNHEALTHY - job failed",
                node_host=self._get_node_host(),
                node_port=self._get_node_port(),
                node_id=self._get_node_id_short(),
            ),
        )
        self._increment_version()
        await self._push_job_status_to_client(
            submission.job_id,
            JobStatus.FAILED.value,
            "All datacenters are unhealthy",
            is_final=True,
        )

    async def _dispatch_to_selected_datacenters(
        self,
        submission: JobSubmission,
        job: GlobalJobStatus,
        primary_dcs: list[str],
        fallback_dcs: list[str],
        worst_health: str,
    ) -> None:
        """Dispatch to the selected datacenters, then record and push
        whether the job started."""
        await self._log_degraded_routing(submission.job_id, worst_health, primary_dcs)

        # The datacenters the job runs in are the ones it is sent to: each
        # result slot starts with a primary and moves -- before the job is
        # sent on -- to any datacenter that takes the primary's place.
        self._job_manager.set_target_dcs(submission.job_id, set(primary_dcs))
        successful_dcs, failed_dcs = await self._dispatch_job_with_fallback(
            submission,
            primary_dcs,
            fallback_dcs,
        )

        if not successful_dcs:
            await self._record_dispatch_failed(submission, job, failed_dcs)
        else:
            await self._record_dispatch_started(submission, job, successful_dcs, failed_dcs)

        self._increment_version()
        await self._push_dispatch_outcome(submission.job_id, successful_dcs)

    async def _log_degraded_routing(self, job_id: str, worst_health: str, primary_dcs: list[str]) -> None:
        """Note a job routed to DEGRADED or BUSY datacenters."""
        if worst_health == "degraded":
            await self._logger.log(
                ServerWarning(
                    message=f"Job {job_id}: No HEALTHY or BUSY DCs available, routing to DEGRADED: {primary_dcs}",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                ),
            )
        elif worst_health == "busy":
            await self._logger.log(
                ServerInfo(
                    message=f"Job {job_id}: No HEALTHY DCs available, routing to BUSY: {primary_dcs}",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                ),
            )

    async def _record_dispatch_failed(
        self,
        submission: JobSubmission,
        job: GlobalJobStatus,
        failed_dcs: list[str],
    ) -> None:
        """Mark a job no datacenter took FAILED (not finalized)."""
        self._quorum_circuit.record_error()
        job.status = JobStatus.FAILED.value
        job.failed_datacenters = len(failed_dcs)
        self._job_manager.set_job(submission.job_id, job)
        # Not finalized: a dispatch that exhausted its retries on
        # timeouts may still have reached a manager, which runs the
        # job and supersedes this status. Only a certain failure
        # (nothing was ever sent) takes the terminal path.
        await self._logger.log(
            ServerError(
                message=f"Job {submission.job_id}: Failed to dispatch to any datacenter",
                node_host=self._get_node_host(),
                node_port=self._get_node_port(),
                node_id=self._get_node_id_short(),
            ),
        )

    async def _record_dispatch_started(
        self,
        submission: JobSubmission,
        job: GlobalJobStatus,
        successful_dcs: list[str],
        failed_dcs: list[str],
    ) -> None:
        """Mark a job a datacenter took RUNNING: track its timeout (AD-34)
        and record its durable acceptance."""
        self._quorum_circuit.record_success()
        job.status = JobStatus.RUNNING.value
        job.completed_datacenters = 0
        job.failed_datacenters = len(failed_dcs)
        self._job_manager.set_job(submission.job_id, job)

        if failed_dcs:
            await self._logger.log(
                ServerInfo(
                    message=f"Job {submission.job_id}: Dispatched to {len(successful_dcs)} DCs, {len(failed_dcs)} failed",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                ),
            )

        await self._job_timeout_tracker.start_tracking_job(
            job_id=submission.job_id,
            timeout_seconds=submission.timeout_seconds,
            target_dcs=successful_dcs,
        )
        await self._on_job_dispatched(submission, successful_dcs)

        if self._persist_accepted_job is not None:
            # Durable acceptance: recorded only for jobs that
            # actually reached a datacenter (the honest acceptance
            # instant — a fully failed dispatch is already terminal
            # above and needs no recovery). The fence token comes
            # from the job manager: this method receives only
            # (submission, target_dcs).
            await self._persist_accepted_job(
                submission,
                successful_dcs,
                self._job_manager.get_fence_token(submission.job_id),
            )

    async def _push_dispatch_outcome(self, job_id: str, successful_dcs: list[str]) -> None:
        """Tell the client the job started, or (finally) that no datacenter
        took it."""
        if successful_dcs:
            await self._push_job_status_to_client(
                job_id,
                JobStatus.RUNNING.value,
                "Job started",
            )
        else:
            await self._push_job_status_to_client(
                job_id,
                JobStatus.FAILED.value,
                "Failed to dispatch to any datacenter",
                is_final=True,
            )

    async def _evaluate_spillover(
        self,
        job_id: str,
        primary_dc: str,
        fallback_dcs: list[str],
        job_cores_required: int,
    ) -> str | None:
        """
        Evaluate if job should spillover to a fallback DC based on capacity.

        Uses SpilloverEvaluator (AD-43) to check if a fallback DC would provide
        better wait times than the primary DC.

        Args:
            job_id: Job identifier for logging
            primary_dc: Primary datacenter ID
            fallback_dcs: List of fallback datacenter IDs
            job_cores_required: Number of cores required for the job

        Returns:
            Spillover datacenter ID if spillover recommended, None otherwise
        """
        if not self._may_spill_over(fallback_dcs):
            return None

        primary_capacity = self._capacity_aggregator.get_capacity(primary_dc)
        if primary_capacity.can_serve_immediately(job_cores_required):
            return None

        return await self._spillover_datacenter(
            job_id, primary_dc, fallback_dcs, job_cores_required, primary_capacity
        )

    def _may_spill_over(self, fallback_dcs: list[str]) -> bool:
        """Whether spillover is configured and there is a fallback to take."""
        return (
            self._spillover_evaluator is not None
            and self._capacity_aggregator is not None
            and bool(fallback_dcs)
        )

    async def _spillover_datacenter(
        self,
        job_id: str,
        primary_dc: str,
        fallback_dcs: list[str],
        job_cores_required: int,
        primary_capacity: DatacenterCapacity,
    ) -> str | None:
        """The fallback the AD-43 evaluator spills the job over to, if any."""
        latencies_ms = self._estimate_datacenter_latencies_ms()
        decision = self._spillover_evaluator.evaluate(
            job_cores_required=job_cores_required,
            primary_capacity=primary_capacity,
            fallback_capacities=self._fallback_capacities(fallback_dcs, latencies_ms),
            primary_rtt_ms=latencies_ms[primary_dc],
        )

        if decision.should_spillover and decision.spillover_dc:
            await self._logger.log(
                ServerInfo(
                    message=f"Job {job_id}: Spillover from {primary_dc} to {decision.spillover_dc} "
                    f"(primary_wait={decision.primary_wait_seconds:.1f}s, "
                    f"spillover_wait={decision.spillover_wait_seconds:.1f}s, "
                    f"reason={decision.reason})",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                ),
            )
            return decision.spillover_dc

        return None

    def _fallback_capacities(
        self,
        fallback_dcs: list[str],
        latencies_ms: dict[str, float],
    ) -> list[tuple[DatacenterCapacity, float]]:
        """Each fallback's capacity with its estimated latency."""
        return [
            (
                self._capacity_aggregator.get_capacity(fallback_dc),
                latencies_ms[fallback_dc],
            )
            for fallback_dc in fallback_dcs
        ]

    async def _dispatch_job_with_fallback(
        self,
        submission: JobSubmission,
        primary_dcs: list[str],
        fallback_dcs: list[str],
    ) -> tuple[list[str], list[str]]:
        """Dispatch to primary DCs with automatic fallback on failure."""
        successful: list[str] = []
        failed: list[str] = []
        fallback_queue = list(fallback_dcs)
        job_id = submission.job_id

        job_cores = self._first_workflows_cores(submission)

        for datacenter in primary_dcs:
            await self._dispatch_to_primary(
                job_id, datacenter, submission, job_cores, fallback_queue, successful, failed
            )

        return (successful, failed)

    def _first_workflows_cores(self, submission: JobSubmission) -> int:
        """The cores the job's first workflows would use."""
        # The cores the job's first workflows -- those depending on none --
        # would use, as a manager's dispatcher allocates them: one per VU
        # (the workflow's own, else the job's), at least one each. A job
        # submission names no core requirement; reading one that is not
        # there left every job asking for one core.
        return sum(
            self._workflow_cores(workflow, submission.vus)
            for _workflow_id, dependencies, workflow in restricted_loads(submission.workflows)
            if not dependencies
        )

    @staticmethod
    def _workflow_cores(workflow: Workflow, submission_vus: int) -> int:
        """One core per VU -- the workflow's own, else the job's -- at least one."""
        return max(1, workflow.vus if workflow.vus and workflow.vus > 0 else submission_vus)

    async def _dispatch_to_primary(
        self,
        job_id: str,
        datacenter: str,
        submission: JobSubmission,
        job_cores: int,
        fallback_queue: list[str],
        successful: list[str],
        failed: list[str],
    ) -> None:
        """Dispatch to a primary datacenter, or the fallback it spills over
        to (AD-43); a failed dispatch falls back to the next fallback."""
        target_dc = await self._spill_over_or_keep(job_id, datacenter, job_cores, fallback_queue)

        success, _, accepting_manager = await self._try_dispatch_to_dc(
            job_id, target_dc, submission
        )

        if success:
            successful.append(target_dc)
            self._record_dc_manager_for_job(job_id, target_dc, accepting_manager)
            return

        await self._fall_back_from(job_id, target_dc, submission, fallback_queue, successful, failed)

    async def _spill_over_or_keep(
        self,
        job_id: str,
        datacenter: str,
        job_cores: int,
        fallback_queue: list[str],
    ) -> str:
        """The datacenter to dispatch to in a primary's place: the fallback
        it spills over to (AD-43), else the primary."""
        spillover_dc = await self._evaluate_spillover(
            job_id=job_id,
            primary_dc=datacenter,
            fallback_dcs=fallback_queue,
            job_cores_required=job_cores,
        )

        target_dc = spillover_dc if spillover_dc else datacenter
        self._claim_spillover_datacenter(job_id, datacenter, spillover_dc, fallback_queue)
        return target_dc

    def _claim_spillover_datacenter(
        self,
        job_id: str,
        datacenter: str,
        spillover_dc: str | None,
        fallback_queue: list[str],
    ) -> None:
        """Take a queued spillover datacenter off the fallback queue, moving
        the primary's result slot to it."""
        if spillover_dc and spillover_dc in fallback_queue:
            fallback_queue.remove(spillover_dc)
            self._job_manager.move_target_dc(job_id, datacenter, spillover_dc)

    async def _fall_back_from(
        self,
        job_id: str,
        target_dc: str,
        submission: JobSubmission,
        fallback_queue: list[str],
        successful: list[str],
        failed: list[str],
    ) -> None:
        """Release a datacenter that did not take the job and dispatch to
        the next fallback; the datacenter fails when none takes it."""
        self._release_failed_datacenter(job_id, target_dc)

        fallback_dc, fallback_manager = await self._try_fallback_dispatch(
            job_id, target_dc, submission, fallback_queue
        )

        if fallback_dc:
            successful.append(fallback_dc)
            self._record_dc_manager_for_job(job_id, fallback_dc, fallback_manager)
        else:
            failed.append(target_dc)

    async def dispatch_to_datacenter(
        self,
        job_id: str,
        datacenter: str,
        submission: JobSubmission,
    ) -> bool:
        """Dispatch a job's submission to one datacenter -- a share of a
        job already placed, re-run where a lost datacenter's went (AD-36)
        -- recording the manager that took it. False when no manager of
        the datacenter took it within the dispatch retry budget."""
        success, _, accepting_manager = await self._try_dispatch_to_dc(
            job_id, datacenter, submission
        )
        if success:
            self._record_dc_manager_for_job(job_id, datacenter, accepting_manager)
        elif self._record_dispatch_failure:
            self._record_dispatch_failure(job_id, datacenter)
        return success

    async def _try_dispatch_to_dc(
        self,
        job_id: str,
        datacenter: str,
        submission: JobSubmission,
    ) -> tuple[bool, str | None, tuple[str, int] | None]:
        """Try to dispatch job to a single datacenter, iterating through managers."""
        managers = self._manager_selector.ordered_managers(
            datacenter,
            job_id,
            self._datacenter_managers.get(datacenter, []),
        )

        datacenter_dispatch_started = self._clock.monotonic()
        # One retry budget for the datacenter, not one per manager.
        retry_deadline_at = datacenter_dispatch_started + self._datacenter_leader_failover_seconds
        self._record_forward_attempt_event()
        for manager_addr in managers:
            dispatch_started = self._clock.monotonic()
            accepting_manager, error = await self._try_dispatch_to_manager(
                datacenter, manager_addr, submission, retry_deadline_at
            )
            if accepting_manager is not None:
                accepted_at = self._clock.monotonic()
                # Time to an accepted dispatch (transient retries included):
                # the responsiveness the gate actually gets from the manager
                # that took it.
                self._manager_selector.record_success(
                    datacenter,
                    accepting_manager,
                    (accepted_at - dispatch_started) * 1000.0,
                )
                # AD-45: the datacenter's time to start the job -- network,
                # leader availability, admission, durable acceptance and
                # first placement. A job's run time is set by its
                # workflows, not by the datacenter, so it is not sampled.
                latency_ms = (accepted_at - datacenter_dispatch_started) * 1000.0
                observed_latency_ms, sample_count = await self._observed_latency_tracker.record_job_latency(
                    datacenter,
                    latency_ms,
                )
                await self._logger.log(
                    ObservedLatencyRecorded(
                        message=(
                            f"{datacenter} accepted job {job_id} in {latency_ms:.1f}ms: observed "
                            f"latency {observed_latency_ms:.1f}ms over {sample_count} samples"
                        ),
                        datacenter_id=datacenter,
                        latency_ms=latency_ms,
                        observed_latency_ms=observed_latency_ms,
                        sample_count=sample_count,
                    )
                )
                self._task_runner.run(
                    self._confirm_manager_for_dc, datacenter, accepting_manager
                )
                self._record_forward_throughput_event()
                return (True, None, accepting_manager)
            else:
                self._manager_selector.record_failure(datacenter, manager_addr)
                self._task_runner.run(
                    self._suspect_manager_for_dc, datacenter, manager_addr
                )

        return (False, f"All managers in {datacenter} failed to accept job", None)

    async def _try_fallback_dispatch(
        self,
        job_id: str,
        failed_dc: str,
        submission: JobSubmission,
        fallback_queue: list[str],
    ) -> tuple[str | None, tuple[str, int] | None]:
        """Try fallback DCs when primary fails. The failed datacenter's
        result slot moves to each fallback before the job is sent there,
        and is dropped when none takes the job."""
        slot_holder = failed_dc
        while fallback_queue:
            fallback_dc = fallback_queue.pop(0)
            self._job_manager.move_target_dc(job_id, slot_holder, fallback_dc)
            slot_holder = fallback_dc
            success, _, accepting_manager = await self._try_dispatch_to_dc(
                job_id, fallback_dc, submission
            )
            if success:
                self._record_fallback_used(failed_dc, fallback_dc)
                await self._logger.log(
                    ServerInfo(
                        message=f"Job {job_id}: Fallback from {failed_dc} to {fallback_dc}",
                        node_host=self._get_node_host(),
                        node_port=self._get_node_port(),
                        node_id=self._get_node_id_short(),
                    ),
                )
                return (fallback_dc, accepting_manager)

            self._release_failed_datacenter(job_id, fallback_dc)

        self._job_manager.discard_target_dc(job_id, slot_holder)
        return (None, None)

    def _release_failed_datacenter(self, job_id: str, datacenter: str) -> None:
        """Record a dispatch no manager of the datacenter took, and release
        it: a dispatch that ran out of retries may have reached a manager
        that runs the job anyway, and is told to stop."""
        if self._record_dispatch_failure:
            self._record_dispatch_failure(job_id, datacenter)
        self._job_manager.release_datacenter(job_id, datacenter)

    async def _try_dispatch_to_manager(
        self,
        datacenter: str,
        manager_addr: tuple[str, int],
        submission: JobSubmission,
        retry_deadline_at: float,
    ) -> tuple[tuple[str, int] | None, str | None]:
        """Dispatch a job to a datacenter starting at one of its managers,
        retrying transient answers until ``retry_deadline_at``, behind each
        manager's circuit breaker. Returns the manager that accepted it --
        the leader a follower redirected to, when one did -- or the error.

        A transient rejection ("Not DC leader", "no quorum", "not accepting
        jobs") means the datacenter is replacing its leader or warming up;
        it resolves within the datacenter's leader failover, so retrying
        stops there and no sooner. A follower that names a leader this gate
        knows is taken at its word at once: retrying the follower would
        only hear the same redirect until the budget ran out.
        """
        if await self._circuit_breaker_manager.is_circuit_open(manager_addr):
            return (None, "Circuit breaker is OPEN")

        return await self._dispatch_through_manager(
            datacenter, manager_addr, submission, retry_deadline_at
        )

    async def _dispatch_through_manager(
        self,
        datacenter: str,
        manager_addr: tuple[str, int],
        submission: JobSubmission,
        retry_deadline_at: float,
    ) -> tuple[tuple[str, int] | None, str | None]:
        """Dispatch through a manager whose circuit is closed, following
        redirects to the leader and retrying transient answers; a failure
        counts against the circuit of the manager last sent to."""
        known_managers = self._datacenter_managers.get(datacenter, [])
        target = manager_addr
        circuit = await self._circuit_breaker_manager.get_circuit(target)

        async def dispatch_operation() -> tuple[tuple[str, int] | None, str | None]:
            nonlocal target, circuit
            # Redirects are followed within an attempt until one points
            # back; the next attempt starts over, as leadership may have
            # moved since.
            redirected_from: set[tuple[str, int]] = set()
            while True:
                ack = await self._send_submission(target, submission)
                # Follow a redirect to a leader this gate knows of.
                if (leader := await self._redirect_leader(ack, target, known_managers, redirected_from)) is not None:
                    # An answer proves the follower reachable.
                    circuit.record_success()
                    redirected_from.add(target)
                    target = leader
                    circuit = await self._circuit_breaker_manager.get_circuit(target)
                    continue
                return self._dispatch_outcome(ack, target, circuit)

        try:
            return await RetryExecutor(self._dispatch_retry_config, clock=self._clock).execute(
                dispatch_operation,
                operation_name=f"dispatch_to_manager_{manager_addr}",
                deadline_at=retry_deadline_at,
            )
        except TransientDispatchError as exception:
            # The datacenter kept answering "retry" for its whole failover:
            # it is reachable, so this is not a circuit failure.
            return (None, str(exception))
        except Exception as exception:
            circuit.record_failure()
            return (None, str(exception))

    async def _send_submission(self, target: tuple[str, int], submission: JobSubmission) -> JobAck:
        """Send the job to one manager; its ack, or ConnectionError when it
        gave no valid answer."""
        # ``_send_tcp`` returns ``(response_bytes | None, clock_time)``.
        response, _clock = await self._send_tcp(
            target,
            "job_submission",
            submission.dump(),
            timeout=self._manager_dispatch_timeout_seconds,
        )
        if not isinstance(response, bytes):
            raise ConnectionError(f"No valid response from manager {target}")
        return JobAck.load(response)

    async def _redirect_leader(
        self,
        ack: JobAck,
        target: tuple[str, int],
        known_managers: list[tuple[str, int]],
        redirected_from: set[tuple[str, int]],
    ) -> tuple[str, int] | None:
        """The leader a refusing manager names, when this gate knows it, has
        not been redirected from it this attempt, and its circuit is closed."""
        if not self._names_leader(ack):
            return None
        leader = (ack.leader_addr[0], ack.leader_addr[1])
        return leader if await self._may_follow_redirect(leader, target, known_managers, redirected_from) else None

    @staticmethod
    def _names_leader(ack: JobAck) -> bool:
        """Whether a refusing ack names a leader."""
        return not ack.accepted and ack.leader_addr is not None

    async def _may_follow_redirect(
        self,
        leader: tuple[str, int],
        target: tuple[str, int],
        known_managers: list[tuple[str, int]],
        redirected_from: set[tuple[str, int]],
    ) -> bool:
        """Whether a named leader is a new, known manager with its circuit
        closed."""
        return self._is_new_known_leader(
            leader, target, known_managers, redirected_from
        ) and not await self._circuit_breaker_manager.is_circuit_open(leader)

    @staticmethod
    def _is_new_known_leader(
        leader: tuple[str, int],
        target: tuple[str, int],
        known_managers: list[tuple[str, int]],
        redirected_from: set[tuple[str, int]],
    ) -> bool:
        """Whether the leader is another manager this gate knows, not one
        this attempt was redirected from."""
        return leader != target and leader in known_managers and leader not in redirected_from

    def _dispatch_outcome(
        self,
        ack: JobAck,
        target: tuple[str, int],
        circuit: "ErrorStats",
    ) -> tuple[tuple[str, int] | None, str | None]:
        """The manager that accepted the job, or the error it refused with."""
        accepted, error = self._process_dispatch_ack(ack, target, circuit)
        return (target if accepted else None, error)

    def _process_dispatch_ack(
        self,
        ack: JobAck,
        manager_addr: tuple[str, int],
        circuit: "ErrorStats",
    ) -> tuple[bool, str | None]:
        """Process dispatch acknowledgment from manager.

        Rejections in the shared transient vocabulary (mid-election
        "Not DC leader", warmup "not accepting jobs", load shedding,
        ...) RAISE so the surrounding ``RetryExecutor`` re-attempts
        with backoff — the same classification the client submitter
        applies to these acks. Returning them as terminal (the old
        behavior) failed the whole job on the first rejection even
        though the condition resolves in seconds; with a single
        datacenter there is no fallback to hide that.

        Any answer proves the manager reachable, so no rejection is a
        circuit failure: counting them opened the breaker on a manager
        that was merely electing, and every job dispatched to it next --
        a pinned one has nowhere else to go -- failed without a send.
        """
        if ack.accepted:
            circuit.record_success()
            return (True, None)

        if is_transient_rejection(ack.error):
            raise TransientDispatchError(ack.error)

        return (False, ack.error)

    def _record_dc_manager_for_job(
        self,
        job_id: str,
        datacenter: str,
        manager_addr: tuple[str, int] | None,
    ) -> None:
        """Record the accepting manager as job leader for a DC."""
        if manager_addr:
            self._state.set_job_dc_manager(job_id, datacenter, manager_addr)


__all__ = ["GateDispatchCoordinator"]
