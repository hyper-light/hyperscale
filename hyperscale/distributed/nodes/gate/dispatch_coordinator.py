"""
Gate job dispatch coordination module.

Coordinates job submission and dispatch to datacenter managers.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import cloudpickle

from hyperscale.distributed.leases import JobLeaseManager
from hyperscale.distributed.models import (
    JobSubmission,
    JobAck,
    JobStatus,
    GlobalJobStatus,
)
from hyperscale.distributed.capacity import (
    DatacenterCapacityAggregator,
    SpilloverEvaluator,
)
from hyperscale.distributed.protocol.version import (
    ProtocolVersion,
    CURRENT_PROTOCOL_VERSION,
    get_features_for_version,
)
from hyperscale.distributed.swim.core import CircuitState
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

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.distributed.jobs.gates import GateJobManager, GateJobTimeoutTracker
    from hyperscale.distributed.routing import GateJobRouter, DispatchTimeTracker
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
        job_lease_manager: JobLeaseManager,
        datacenter_managers: dict[str, list[tuple[str, int]]],
        check_rate_limit: Callable,
        should_shed_request: Callable,
        has_quorum_available: Callable,
        quorum_size: Callable,
        quorum_circuit: "ErrorStats",
        select_datacenters: Callable,
        assume_leadership: Callable,
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
        capacity_aggregator: DatacenterCapacityAggregator | None = None,
        spillover_evaluator: SpilloverEvaluator | None = None,
    ) -> None:
        self._state: "GateRuntimeState" = state
        self._logger: "Logger" = logger
        self._task_runner: "TaskRunner" = task_runner
        self._job_manager: "GateJobManager" = job_manager
        self._job_router: "GateJobRouter | None" = job_router
        self._job_timeout_tracker: "GateJobTimeoutTracker" = job_timeout_tracker
        self._dispatch_time_tracker: "DispatchTimeTracker" = dispatch_time_tracker
        self._circuit_breaker_manager: "CircuitBreakerManager" = circuit_breaker_manager
        self._job_lease_manager: JobLeaseManager = job_lease_manager
        self._datacenter_managers: dict[str, list[tuple[str, int]]] = (
            datacenter_managers
        )
        self._check_rate_limit: Callable = check_rate_limit
        self._should_shed_request: Callable = should_shed_request
        self._has_quorum_available: Callable = has_quorum_available
        self._quorum_size: Callable = quorum_size
        self._quorum_circuit: "ErrorStats" = quorum_circuit
        self._select_datacenters: Callable = select_datacenters
        self._assume_leadership: Callable = assume_leadership
        self._broadcast_leadership: Callable[
            [str, int, tuple[str, int] | None], Awaitable[None]
        ] = broadcast_leadership
        self._send_tcp: Callable = send_tcp
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

    def _is_terminal_status(self, status: str) -> bool:
        return status in (
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        )

    def _pop_lease_renewal_token(self, job_id: str) -> str | None:
        return self._state._job_lease_renewal_tokens.pop(job_id, None)

    async def _cancel_lease_renewal(self, job_id: str) -> None:
        token = self._pop_lease_renewal_token(job_id)
        if not token:
            return
        try:
            await self._task_runner.cancel(token)
        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=f"Failed to cancel lease renewal for job {job_id}: {error}",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                )
            )

    async def _release_job_lease(
        self,
        job_id: str,
        cancel_renewal: bool = True,
    ) -> None:
        if cancel_renewal:
            await self._cancel_lease_renewal(job_id)
        else:
            self._pop_lease_renewal_token(job_id)
        await self._job_lease_manager.release(job_id)

    async def _renew_job_lease(self, job_id: str, lease_duration: float) -> None:
        renewal_interval = max(1.0, lease_duration * 0.5)

        try:
            while True:
                await asyncio.sleep(renewal_interval)
                job = self._job_manager.get_job(job_id)
                if job is None or self._is_terminal_status(job.status):
                    await self._release_job_lease(job_id, cancel_renewal=False)
                    return

                lease_renewed = await self._job_lease_manager.renew(
                    job_id, lease_duration
                )
                if not lease_renewed:
                    await self._logger.log(
                        ServerError(
                            message=f"Failed to renew lease for job {job_id}: lease lost",
                            node_host=self._get_node_host(),
                            node_port=self._get_node_port(),
                            node_id=self._get_node_id_short(),
                        )
                    )
                    await self._release_job_lease(job_id, cancel_renewal=False)
                    return
        except asyncio.CancelledError:
            self._pop_lease_renewal_token(job_id)
            return

    async def _check_rate_and_load(
        self,
        client_id: str,
        job_id: str,
    ) -> JobAck | None:
        """Check rate limit and load shedding. Returns rejection JobAck if rejected."""
        allowed, retry_after = await self._check_rate_limit(client_id, "job_submit")
        if not allowed:
            return JobAck(
                job_id=job_id,
                accepted=False,
                error=f"Rate limited, retry after {retry_after}s",
            )

        if self._should_shed_request("JobSubmission"):
            return JobAck(
                job_id=job_id,
                accepted=False,
                error="System under load, please retry later",
            )
        return None

    def _check_protocol_version(
        self,
        submission: JobSubmission,
    ) -> tuple[JobAck | None, str]:
        """Check protocol compatibility. Returns (rejection_ack, negotiated_caps)."""
        client_version = ProtocolVersion(
            major=getattr(submission, "protocol_version_major", 1),
            minor=getattr(submission, "protocol_version_minor", 0),
        )

        if client_version.major != CURRENT_PROTOCOL_VERSION.major:
            return (
                JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Incompatible protocol version: {client_version}",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ),
                "",
            )

        client_caps = getattr(submission, "capabilities", "")
        client_features = set(client_caps.split(",")) if client_caps else set()
        our_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
        negotiated = ",".join(sorted(client_features & our_features))
        return (None, negotiated)

    def _check_circuit_and_quorum(self, job_id: str) -> JobAck | None:
        """Check circuit breaker and quorum. Returns rejection JobAck if unavailable."""
        if self._quorum_circuit.circuit_state == CircuitState.OPEN:
            retry_after = self._quorum_circuit.half_open_after
            return JobAck(
                job_id=job_id,
                accepted=False,
                error=f"Circuit open, retry after {retry_after}s",
            )

        if self._state.get_active_peer_count() > 0 and not self._has_quorum_available():
            return JobAck(job_id=job_id, accepted=False, error="Quorum unavailable")
        return None

    def _setup_job_tracking(
        self,
        submission: JobSubmission,
        primary_dcs: list[str],
        fence_token: int,
    ) -> None:
        """Initialize job tracking state for a new submission."""
        job = GlobalJobStatus(
            job_id=submission.job_id,
            status=JobStatus.SUBMITTED.value,
            datacenters=[],
            timestamp=time.monotonic(),
            fence_token=fence_token,
        )
        self._job_manager.set_job(submission.job_id, job)
        self._job_manager.set_target_dcs(submission.job_id, set(primary_dcs))
        self._job_manager.set_fence_token(submission.job_id, fence_token)

        try:
            workflows = cloudpickle.loads(submission.workflows)
            self._state._job_workflow_ids[submission.job_id] = {
                wf_id for wf_id, _, _ in workflows
            }
        except Exception as workflow_parse_error:
            self._state._job_workflow_ids[submission.job_id] = set()
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Failed to parse workflows for job {submission.job_id}: {workflow_parse_error}",
                    node_host="",
                    node_port=0,
                    node_id="",
                ),
            )

        if submission.callback_addr:
            self._job_manager.set_callback(submission.job_id, submission.callback_addr)
            self._state._progress_callbacks[submission.job_id] = (
                submission.callback_addr
            )

        if submission.reporting_configs:
            self._state._job_submissions[submission.job_id] = submission

    async def submit_job(
        self,
        addr: tuple[str, int],
        submission: JobSubmission,
    ) -> JobAck:
        """
        Process job submission from client.

        Args:
            addr: Client address
            submission: Job submission message

        Returns:
            JobAck with acceptance status
        """
        client_id = f"{addr[0]}:{addr[1]}"
        negotiated_caps = ""
        lease_acquired = False
        lease_duration = 0.0
        fence_token = 0

        try:
            # Validate rate limit and load (AD-22, AD-24)
            if rejection := await self._check_rate_and_load(
                client_id, submission.job_id
            ):
                return rejection

            # Validate protocol version (AD-25)
            rejection, negotiated_caps = self._check_protocol_version(submission)
            if rejection:
                return rejection

            lease_result = await self._job_lease_manager.acquire(submission.job_id)
            if not lease_result.success:
                current_owner = lease_result.current_owner or "unknown"
                error_message = (
                    f"Job lease held by {current_owner} "
                    f"(expires in {lease_result.expires_in:.1f}s)"
                )
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=error_message,
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                    capabilities=negotiated_caps,
                )

            lease = lease_result.lease
            if lease is None:
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error="Lease acquisition did not return a lease",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                    capabilities=negotiated_caps,
                )

            lease_acquired = True
            lease_duration = lease.lease_duration
            fence_token = lease.fence_token

            # Check circuit breaker and quorum
            if rejection := self._check_circuit_and_quorum(submission.job_id):
                await self._release_job_lease(submission.job_id)
                return rejection

            # Select datacenters (AD-36)
            primary_dcs, _, worst_health = self._select_datacenters(
                submission.datacenter_count,
                submission.datacenters if submission.datacenters else None,
                job_id=submission.job_id,
            )

            if worst_health == "initializing":
                await self._release_job_lease(submission.job_id)
                return JobAck(
                    job_id=submission.job_id, accepted=False, error="initializing"
                )
            if not primary_dcs:
                await self._release_job_lease(submission.job_id)
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error="No available datacenters",
                )

            # Setup job tracking
            self._setup_job_tracking(submission, primary_dcs, fence_token)

            # Assume and broadcast leadership
            self._assume_leadership(
                submission.job_id,
                len(primary_dcs),
                initial_token=fence_token,
            )
            await self._broadcast_leadership(
                submission.job_id,
                len(primary_dcs),
                submission.callback_addr,
            )
            self._quorum_circuit.record_success()

            # Dispatch in background
            self._task_runner.run(self.dispatch_job, submission, primary_dcs)

            if submission.job_id not in self._state._job_lease_renewal_tokens:
                run = self._task_runner.run(
                    self._renew_job_lease,
                    submission.job_id,
                    lease_duration,
                    alias=f"job-lease-renewal-{submission.job_id}",
                )
                if run:
                    self._state._job_lease_renewal_tokens[submission.job_id] = run.token

            return JobAck(
                job_id=submission.job_id,
                accepted=True,
                queued_position=self._job_manager.job_count(),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps,
            )
        except Exception as error:
            if lease_acquired:
                await self._release_job_lease(submission.job_id)
            await self._logger.log(
                ServerError(
                    message=f"Job submission error: {error}",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                )
            )
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=str(error),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps,
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
            self._dispatch_time_tracker.record_dispatch(
                submission.job_id, datacenter_id
            )

        job = self._job_manager.get_job(submission.job_id)
        if not job:
            return

        submission.origin_gate_addr = (self._get_node_host(), self._get_node_port())
        job.status = JobStatus.DISPATCHING.value
        self._job_manager.set_job(submission.job_id, job)
        self._increment_version()

        primary_dcs, fallback_dcs, worst_health = self._select_datacenters(
            len(target_dcs),
            target_dcs if target_dcs else None,
            job_id=submission.job_id,
        )

        if worst_health == "initializing":
            job.status = JobStatus.PENDING.value
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Job {submission.job_id}: DCs became initializing after acceptance - waiting",
                    node_host=self._get_node_host(),
                    node_port=self._get_node_port(),
                    node_id=self._get_node_id_short(),
                ),
            )
            return

        if worst_health == "unhealthy":
            job.status = JobStatus.FAILED.value
            job.failed_datacenters = len(target_dcs)
            self._quorum_circuit.record_error()
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

        self._increment_version()

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
            rtt_ms = 50.0
            fallback_capacities.append((fallback_capacity, rtt_ms))

        decision = self._spillover_evaluator.evaluate(
            job_cores_required=job_cores_required,
            primary_capacity=primary_capacity,
            fallback_capacities=fallback_capacities,
            primary_rtt_ms=10.0,
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

            fallback_dc, fallback_manager = await self._try_fallback_dispatch(
                job_id, target_dc, submission, fallback_queue
            )

            if fallback_dc:
                successful.append(fallback_dc)
                self._record_dc_manager_for_job(job_id, fallback_dc, fallback_manager)
            else:
                failed.append(datacenter)

        return (successful, failed)

    async def _try_dispatch_to_dc(
        self,
        job_id: str,
        datacenter: str,
        submission: JobSubmission,
    ) -> tuple[bool, str | None, tuple[str, int] | None]:
        """Try to dispatch job to a single datacenter, iterating through managers."""
        managers = self._datacenter_managers.get(datacenter, [])

        for manager_addr in managers:
            success, error = await self._try_dispatch_to_manager(
                manager_addr, submission
            )
            if success:
                self._task_runner.run(
                    self._confirm_manager_for_dc, datacenter, manager_addr
                )
                self._record_forward_throughput_event()
                return (True, None, manager_addr)
            else:
                self._task_runner.run(
                    self._suspect_manager_for_dc, datacenter, manager_addr
                )

        if self._job_router:
            self._job_router.record_dispatch_failure(job_id, datacenter)
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
        return (None, None)

    async def _try_dispatch_to_manager(
        self,
        manager_addr: tuple[str, int],
        submission: JobSubmission,
        max_retries: int = 2,
        base_delay: float = 0.3,
    ) -> tuple[bool, str | None]:
        """Try to dispatch job to a single manager with retries and circuit breaker."""
        if self._circuit_breaker_manager.is_open(manager_addr):
            return (False, "Circuit breaker is OPEN")

        circuit = self._circuit_breaker_manager.get_or_create(manager_addr)
        retry_config = RetryConfig(
            max_attempts=max_retries + 1,
            base_delay=base_delay,
            max_delay=5.0,
            jitter=JitterStrategy.FULL,
        )
        executor = RetryExecutor(retry_config)

        async def dispatch_operation() -> tuple[bool, str | None]:
            response = await self._send_tcp(
                manager_addr,
                "job_submission",
                submission.dump(),
                timeout=5.0,
            )

            if isinstance(response, bytes):
                ack = JobAck.load(response)
                return self._process_dispatch_ack(ack, manager_addr, circuit)

            raise ConnectionError("No valid response from manager")

        try:
            return await executor.execute(
                dispatch_operation,
                operation_name=f"dispatch_to_manager_{manager_addr}",
            )
        except Exception as exception:
            circuit.record_failure()
            return (False, str(exception))

    def _process_dispatch_ack(
        self,
        ack: JobAck,
        manager_addr: tuple[str, int],
        circuit: "ErrorStats",
    ) -> tuple[bool, str | None]:
        """Process dispatch acknowledgment from manager."""
        if ack.accepted:
            circuit.record_success()
            return (True, None)

        circuit.record_failure()
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
