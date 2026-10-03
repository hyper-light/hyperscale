"""
Gate job dispatch coordination module.

Dispatches an admitted job to its datacenters' managers (submission
admission is GateJobHandler's).
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    JobSubmission,
    JobAck,
    JobStatus,
    JobStatusPush,
)
from hyperscale.distributed.capacity import (
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
    ServerWarning,
    ServerInfo,
    ServerError,
)

from hyperscale.distributed.runtime import Clock



if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.distributed.jobs.gates import GateJobManager, GateJobTimeoutTracker
    from hyperscale.distributed.routing import (
        DispatchTimeTracker,
        ObservedLatencyTracker,
    )
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
        dispatch_time_tracker: "DispatchTimeTracker",
        circuit_breaker_manager: "CircuitBreakerManager",
        datacenter_managers: dict[str, list[tuple[str, int]]],
        quorum_circuit: "ErrorStats",
        select_datacenters: Callable,
        broadcast_leadership: Callable[
            [str, int, tuple[str, int] | None], Awaitable[None]
        ],
        send_tcp: Callable,
        increment_version: Callable,
        confirm_manager_for_dc: Callable,
        suspect_manager_for_dc: Callable,
        record_forward_throughput_event: Callable,
        get_node_host: Callable[[], str],
        get_node_port: Callable[[], int],
        get_node_id_short: Callable[[], str],
        manager_dispatch_timeout_seconds: float,
        client_push_timeout_seconds: float,
        clock: Clock,
        capacity_aggregator: DatacenterCapacityAggregator | None = None,
        spillover_evaluator: SpilloverEvaluator | None = None,
        observed_latency_tracker: "ObservedLatencyTracker | None" = None,
        record_dispatch_failure: Callable[[str, str], None] | None = None,
        persist_accepted_job=None,
        *,
        manager_selector: DatacenterManagerSelector,
        finalize_failed_job: Callable[[str, tuple[str, ...], str], Awaitable[None]],
        on_job_dispatched: Callable[[JobSubmission, list[str]], Awaitable[None]],
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
        self._persist_accepted_job = persist_accepted_job
        self._dispatch_time_tracker: "DispatchTimeTracker" = dispatch_time_tracker
        self._circuit_breaker_manager: "CircuitBreakerManager" = circuit_breaker_manager
        self._datacenter_managers: dict[str, list[tuple[str, int]]] = (
            datacenter_managers
        )
        self._quorum_circuit: "ErrorStats" = quorum_circuit
        self._select_datacenters: Callable = select_datacenters
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
        self._get_node_host: Callable[[], str] = get_node_host
        self._get_node_port: Callable[[], int] = get_node_port
        self._get_node_id_short: Callable[[], str] = get_node_id_short
        self._capacity_aggregator: DatacenterCapacityAggregator | None = (
            capacity_aggregator
        )
        self._spillover_evaluator: SpilloverEvaluator | None = spillover_evaluator
        self._observed_latency_tracker: "ObservedLatencyTracker | None" = (
            observed_latency_tracker
        )
        self._record_dispatch_failure: Callable[[str, str], None] | None = (
            record_dispatch_failure
        )

    def _get_observed_rtt_ms(
        self,
        datacenter_id: str,
        default_rtt_ms: float,
        min_confidence: float = 0.3,
    ) -> float:
        if self._observed_latency_tracker is None:
            return default_rtt_ms

        observed_ms, confidence = self._observed_latency_tracker.get_observed_latency(
            datacenter_id
        )
        if confidence < min_confidence or observed_ms <= 0.0:
            return default_rtt_ms

        return observed_ms

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

        job = self._job_manager.get_job(job_id)
        elapsed_seconds = 0.0
        total_completed = 0
        total_failed = 0
        overall_rate = 0.0
        if job is not None:
            if job.timestamp > 0:
                elapsed_seconds = max(0.0, self._clock.monotonic() - job.timestamp)
            total_completed = job.total_completed
            total_failed = job.total_failed
            overall_rate = job.overall_rate

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

        try:
            response, _ = await self._send_tcp(
                callback,
                "job_status_push",
                payload,
                timeout=self._client_push_timeout_seconds,
            )
            if isinstance(response, Exception):
                raise response
            if response not in (b"ok", None):
                raise RuntimeError(f"status push rejected: {response!r}")
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
        for datacenter_id in target_dcs:
            await self._dispatch_time_tracker.record_dispatch(
                submission.job_id, datacenter_id
            )

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

        primary_dcs, fallback_dcs, worst_health = self._select_datacenters(
            len(target_dcs),
            target_dcs if target_dcs else None,
            job_id=submission.job_id,
        )

        if worst_health == "initializing":
            job.status = JobStatus.PENDING.value
            self._job_manager.set_job(submission.job_id, job)
            self._task_runner.run(
                self._logger.log,
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
            return

        if worst_health == "unhealthy":
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

            self._task_runner.run(
                self._logger.log,
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
            return

        if worst_health == "degraded":
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Job {submission.job_id}: No HEALTHY or BUSY DCs available, routing to DEGRADED: {primary_dcs}",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                ),
            )
        elif worst_health == "busy":
            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Job {submission.job_id}: No HEALTHY DCs available, routing to BUSY: {primary_dcs}",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                ),
            )

        successful_dcs, failed_dcs = await self._dispatch_job_with_fallback(
            submission,
            primary_dcs,
            fallback_dcs,
        )

        if not successful_dcs:
            self._quorum_circuit.record_error()
            job.status = JobStatus.FAILED.value
            job.failed_datacenters = len(failed_dcs)
            self._job_manager.set_job(submission.job_id, job)
            # Not finalized: a dispatch that exhausted its retries on
            # timeouts may still have reached a manager, which runs the
            # job and supersedes this status. Only a certain failure
            # (nothing was ever sent) takes the terminal path.
            self._task_runner.run(
                self._logger.log,
                ServerError(
                    message=f"Job {submission.job_id}: Failed to dispatch to any datacenter",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                ),
            )
        else:
            self._quorum_circuit.record_success()
            job.status = JobStatus.RUNNING.value
            job.completed_datacenters = 0
            job.failed_datacenters = len(failed_dcs)
            self._job_manager.set_job(submission.job_id, job)

            if failed_dcs:
                self._task_runner.run(
                    self._logger.log,
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

        self._increment_version()
        if successful_dcs:
            await self._push_job_status_to_client(
                submission.job_id,
                JobStatus.RUNNING.value,
                "Job started",
            )
        else:
            await self._push_job_status_to_client(
                submission.job_id,
                JobStatus.FAILED.value,
                "Failed to dispatch to any datacenter",
                is_final=True,
            )

    def _evaluate_spillover(
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
        if self._spillover_evaluator is None or self._capacity_aggregator is None:
            return None

        if not fallback_dcs:
            return None

        primary_capacity = self._capacity_aggregator.get_capacity(primary_dc)
        if primary_capacity.can_serve_immediately(job_cores_required):
            return None

        fallback_capacities: list[tuple] = []
        for fallback_dc in fallback_dcs:
            fallback_capacity = self._capacity_aggregator.get_capacity(fallback_dc)
            rtt_ms = self._get_observed_rtt_ms(fallback_dc, default_rtt_ms=50.0)
            fallback_capacities.append((fallback_capacity, rtt_ms))

        primary_rtt_ms = self._get_observed_rtt_ms(primary_dc, default_rtt_ms=10.0)
        decision = self._spillover_evaluator.evaluate(
            job_cores_required=job_cores_required,
            primary_capacity=primary_capacity,
            fallback_capacities=fallback_capacities,
            primary_rtt_ms=primary_rtt_ms,
        )

        if decision.should_spillover and decision.spillover_dc:
            self._task_runner.run(
                self._logger.log,
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

        job_cores = getattr(submission, "cores_required", 1)

        for datacenter in primary_dcs:
            spillover_dc = self._evaluate_spillover(
                job_id=job_id,
                primary_dc=datacenter,
                fallback_dcs=fallback_queue,
                job_cores_required=job_cores,
            )

            target_dc = spillover_dc if spillover_dc else datacenter
            if spillover_dc and spillover_dc in fallback_queue:
                fallback_queue.remove(spillover_dc)

            success, _, accepting_manager = await self._try_dispatch_to_dc(
                job_id, target_dc, submission
            )

            if success:
                successful.append(target_dc)
                self._record_dc_manager_for_job(job_id, target_dc, accepting_manager)
                continue

            if self._record_dispatch_failure:
                self._record_dispatch_failure(job_id, target_dc)

            fallback_dc, fallback_manager = await self._try_fallback_dispatch(
                job_id, target_dc, submission, fallback_queue
            )

            if fallback_dc:
                successful.append(fallback_dc)
                self._record_dc_manager_for_job(job_id, fallback_dc, fallback_manager)
            else:
                failed.append(target_dc)

        return (successful, failed)

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

        for manager_addr in managers:
            dispatch_started = self._clock.monotonic()
            success, error = await self._try_dispatch_to_manager(
                manager_addr, submission
            )
            if success:
                # Time to an accepted dispatch (transient retries included):
                # the responsiveness the gate actually gets from this manager.
                self._manager_selector.record_success(
                    datacenter,
                    manager_addr,
                    (self._clock.monotonic() - dispatch_started) * 1000.0,
                )
                self._task_runner.run(
                    self._confirm_manager_for_dc, datacenter, manager_addr
                )
                self._record_forward_throughput_event()
                return (True, None, manager_addr)
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
        """Try fallback DCs when primary fails."""
        while fallback_queue:
            fallback_dc = fallback_queue.pop(0)
            success, _, accepting_manager = await self._try_dispatch_to_dc(
                job_id, fallback_dc, submission
            )
            if success:
                self._task_runner.run(
                    self._logger.log,
                    ServerInfo(
                        message=f"Job {job_id}: Fallback from {failed_dc} to {fallback_dc}",
                        node_host=self._get_node_host(),
                        node_port=self._get_node_port(),
                        node_id=self._get_node_id_short(),
                    ),
                )
                return (fallback_dc, accepting_manager)

            if self._record_dispatch_failure:
                self._record_dispatch_failure(job_id, fallback_dc)

        return (None, None)

    async def _try_dispatch_to_manager(
        self,
        manager_addr: tuple[str, int],
        submission: JobSubmission,
        max_retries: int = 9,
        base_delay: float = 1.0,
    ) -> tuple[bool, str | None]:
        """Try to dispatch job to a single manager with retries and circuit breaker.

        The retry budget must span a full datacenter leader election
        (pre-vote ~2s + 5-7s jittered timeout, so ~9-10s worst case):
        a gate front-running a warming or mid-failover manager gets
        transient rejections ("Not DC leader", "no quorum", "not
        accepting jobs") that resolve within that window — with the
        old 3-attempt/~1s budget the gate gave up while the election
        it was waiting on was still running, terminally failing the
        job. Ten attempts at base 1.0s (full jitter, 5s cap) spans the
        window with margin while staying bounded.
        """
        if await self._circuit_breaker_manager.is_circuit_open(manager_addr):
            return (False, "Circuit breaker is OPEN")

        circuit = await self._circuit_breaker_manager.get_circuit(manager_addr)
        retry_config = RetryConfig(
            max_attempts=max_retries + 1,
            base_delay=base_delay,
            max_delay=5.0,
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
        executor = RetryExecutor(retry_config)

        async def dispatch_operation() -> tuple[bool, str | None]:
            # ``_send_tcp`` returns ``(response_bytes | None, clock_time)`` —
            # unpack the tuple rather than testing isinstance(response, bytes)
            # against the raw tuple, which would always fail and force the
            # retry path to ``raise ConnectionError("No valid response from
            # manager")`` even when the manager handler responded correctly.
            response, _clock = await self._send_tcp(
                manager_addr,
                "job_submission",
                submission.dump(),
                timeout=self._manager_dispatch_timeout_seconds,
            )

            if isinstance(response, bytes):
                ack = JobAck.load(response)
                return self._process_dispatch_ack(ack, manager_addr, circuit)

            raise ConnectionError("No valid response from manager")

        try:
            result = await executor.execute(
                dispatch_operation,
                operation_name=f"dispatch_to_manager_{manager_addr}",
            )
            return result
        except TransientDispatchError as exception:
            # The manager kept answering "retry" for the whole budget: it
            # is reachable, so this is not a circuit failure.
            return (False, str(exception))
        except Exception as exception:
            circuit.record_failure()
            return (False, str(exception))

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
            if job_id not in self._state._job_dc_managers:
                self._state._job_dc_managers[job_id] = {}
            self._state._job_dc_managers[job_id][datacenter] = manager_addr


__all__ = ["GateDispatchCoordinator"]
