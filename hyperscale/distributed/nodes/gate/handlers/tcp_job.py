"""
TCP handlers for job submission and status operations.

Handles client-facing job operations:
- Job submission from clients
- Job status queries
- Job progress updates from managers
"""

import asyncio
import cloudpickle
import time
from typing import TYPE_CHECKING, Awaitable, Callable

from hyperscale.distributed.models import (
    GlobalJobStatus,
    JobAck,
    JobProgress,
    JobProgressAck,
    JobStatus,
    JobSubmission,
)
from hyperscale.distributed.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    ProtocolVersion,
    get_features_for_version,
)
from hyperscale.distributed.models import RateLimitResponse
from hyperscale.distributed.swim.core.error_handler import CircuitState
from hyperscale.distributed.swim.core.errors import (
    QuorumCircuitOpenError,
    QuorumError,
    QuorumUnavailableError,
)
from hyperscale.distributed.idempotency import (
    GateIdempotencyCache,
    IdempotencyKey,
    IdempotencyStatus,
)
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerError,
    ServerInfo,
)

from ..state import GateRuntimeState

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId, ErrorStats
    from hyperscale.distributed.jobs.gates import GateJobManager
    from hyperscale.distributed.jobs import JobLeadershipTracker
    from hyperscale.distributed.reliability import LoadShedder
    from hyperscale.distributed.routing import GateJobRouter
    from hyperscale.distributed.models import GateInfo
    from hyperscale.distributed.taskex import TaskRunner


class GateJobHandler:
    """
    Handles job submission and status operations.

    Provides TCP handler methods for client-facing job operations.
    """

    def __init__(
        self,
        state: GateRuntimeState,
        logger: Logger,
        task_runner: "TaskRunner",
        job_manager: "GateJobManager",
        job_router: "GateJobRouter",
        job_leadership_tracker: "JobLeadershipTracker",
        quorum_circuit: "ErrorStats",
        load_shedder: "LoadShedder",
        job_lease_manager: object,
        idempotency_cache: GateIdempotencyCache[bytes] | None,
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        is_leader: Callable[[], bool],
        check_rate_limit: Callable[[str, str], tuple[bool, float]],
        should_shed_request: Callable[[str], bool],
        has_quorum_available: Callable[[], bool],
        quorum_size: Callable[[], int],
        select_datacenters_with_fallback: Callable,
        get_healthy_gates: Callable[[], list["GateInfo"]],
        broadcast_job_leadership: Callable[
            [str, int, tuple[str, int] | None], Awaitable[None]
        ],
        dispatch_job_to_datacenters: Callable,
        forward_job_progress_to_peers: Callable,
        record_request_latency: Callable[[float], None],
        record_dc_job_stats: Callable,
        handle_update_by_tier: Callable,
    ) -> None:
        """
        Initialize the job handler.

        Args:
            state: Runtime state container
            logger: Async logger instance
            task_runner: Background task executor
            job_manager: Job management service
            job_router: Job routing service
            job_leadership_tracker: Per-job leadership tracker
            quorum_circuit: Quorum operation circuit breaker
            load_shedder: Load shedding manager
            job_lease_manager: Job lease manager
            idempotency_cache: Idempotency cache for duplicate detection
            get_node_id: Callback to get this gate's node ID
            get_host: Callback to get this gate's host
            get_tcp_port: Callback to get this gate's TCP port
            is_leader: Callback to check if this gate is SWIM cluster leader
            check_rate_limit: Callback to check rate limit for operation
            should_shed_request: Callback to check if request should be shed
            has_quorum_available: Callback to check quorum availability
            quorum_size: Callback to get quorum size
            select_datacenters_with_fallback: Callback for DC selection
            get_healthy_gates: Callback to get healthy gate list
            broadcast_job_leadership: Callback to broadcast leadership
            dispatch_job_to_datacenters: Callback to dispatch job
            forward_job_progress_to_peers: Callback to forward progress
            record_request_latency: Callback to record latency
            record_dc_job_stats: Callback to record DC stats
            handle_update_by_tier: Callback for tiered update handling
        """
        self._state: GateRuntimeState = state
        self._logger: Logger = logger
        self._task_runner: "TaskRunner" = task_runner
        self._job_manager: "GateJobManager" = job_manager
        self._job_router: "GateJobRouter" = job_router
        self._job_leadership_tracker: "JobLeadershipTracker" = job_leadership_tracker
        self._quorum_circuit: "ErrorStats" = quorum_circuit
        self._load_shedder: "LoadShedder" = load_shedder
        self._job_lease_manager: object = job_lease_manager
        self._idempotency_cache: GateIdempotencyCache[bytes] | None = idempotency_cache
        self._get_node_id: Callable[[], "NodeId"] = get_node_id
        self._get_host: Callable[[], str] = get_host
        self._get_tcp_port: Callable[[], int] = get_tcp_port
        self._is_leader: Callable[[], bool] = is_leader
        self._check_rate_limit: Callable[[str, str], tuple[bool, float]] = (
            check_rate_limit
        )
        self._should_shed_request: Callable[[str], bool] = should_shed_request
        self._has_quorum_available: Callable[[], bool] = has_quorum_available
        self._quorum_size: Callable[[], int] = quorum_size
        self._select_datacenters_with_fallback: Callable = (
            select_datacenters_with_fallback
        )
        self._get_healthy_gates: Callable[[], list["GateInfo"]] = get_healthy_gates
        self._broadcast_job_leadership: Callable[
            [str, int, tuple[str, int] | None], Awaitable[None]
        ] = broadcast_job_leadership
        self._dispatch_job_to_datacenters: Callable = dispatch_job_to_datacenters
        self._forward_job_progress_to_peers: Callable = forward_job_progress_to_peers
        self._record_request_latency: Callable[[float], None] = record_request_latency
        self._record_dc_job_stats: Callable = record_dc_job_stats
        self._handle_update_by_tier: Callable = handle_update_by_tier

    def _is_terminal_status(self, status: str) -> bool:
        return status in (
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
        )

    async def _release_job_lease(self, job_id: str) -> None:
        await self._job_lease_manager.release(job_id)

    async def _renew_job_lease(self, job_id: str, lease_duration: float) -> None:
        renewal_interval = max(1.0, lease_duration * 0.5)

        while True:
            await asyncio.sleep(renewal_interval)
            job = self._job_manager.get_job(job_id)
            if job is None or self._is_terminal_status(job.status):
                await self._release_job_lease(job_id)
                return

            lease_renewed = await self._job_lease_manager.renew(job_id, lease_duration)
            if not lease_renewed:
                await self._logger.log(
                    ServerError(
                        message=f"Failed to renew lease for job {job_id}: lease lost",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    )
                )
                return

    async def handle_submission(
        self,
        addr: tuple[str, int],
        data: bytes,
        active_gate_peer_count: int,
    ) -> bytes:
        """
        Handle job submission from client.

        Any gate can accept a job and become its leader. Per-job leadership
        is independent of SWIM cluster leadership.

        Args:
            addr: Client address
            data: Serialized JobSubmission
            active_gate_peer_count: Number of active gate peers

        Returns:
            Serialized JobAck response
        """
        submission: JobSubmission | None = None
        idempotency_key: IdempotencyKey | None = None
        lease_acquired = False
        lease_duration: float | None = None

        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit(client_id, "job_submit")
            if not allowed:
                return RateLimitResponse(
                    operation="job_submit",
                    retry_after_seconds=retry_after,
                ).dump()

            if self._should_shed_request("JobSubmission"):
                overload_state = self._load_shedder.get_current_state()
                return JobAck(
                    job_id="",
                    accepted=False,
                    error=f"System under load ({overload_state.value}), please retry later",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            submission = JobSubmission.load(data)

            client_version = ProtocolVersion(
                major=getattr(submission, "protocol_version_major", 1),
                minor=getattr(submission, "protocol_version_minor", 0),
            )

            if client_version.major != CURRENT_PROTOCOL_VERSION.major:
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Incompatible protocol version: {client_version} (requires major version {CURRENT_PROTOCOL_VERSION.major})",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            client_caps_str = getattr(submission, "capabilities", "")
            client_features = (
                set(client_caps_str.split(",")) if client_caps_str else set()
            )
            our_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
            negotiated_features = client_features & our_features
            negotiated_caps_str = ",".join(sorted(negotiated_features))

            if submission.idempotency_key and self._idempotency_cache is not None:
                idempotency_key = IdempotencyKey.parse(submission.idempotency_key)
                found, entry = await self._idempotency_cache.check_or_insert(
                    idempotency_key,
                    submission.job_id,
                    self._get_node_id().full,
                )
                if found and entry is not None:
                    if entry.status in (
                        IdempotencyStatus.COMMITTED,
                        IdempotencyStatus.REJECTED,
                    ):
                        if entry.result is not None:
                            return entry.result
                        return JobAck(
                            job_id=submission.job_id,
                            accepted=entry.status == IdempotencyStatus.COMMITTED,
                            error="Duplicate request"
                            if entry.status == IdempotencyStatus.REJECTED
                            else None,
                        ).dump()

            lease_result = await self._job_lease_manager.acquire(submission.job_id)
            if not lease_result.success:
                error_message = (
                    f"Job lease held by {lease_result.current_owner} "
                    f"(expires in {lease_result.expires_in:.1f}s)"
                )
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=error_message,
                ).dump()

            lease_acquired = True
            lease_duration = (
                lease_result.lease.lease_duration
                if lease_result.lease is not None
                else None
            )

            if self._quorum_circuit.circuit_state == CircuitState.OPEN:
                await self._release_job_lease(submission.job_id)
                retry_after = self._quorum_circuit.half_open_after
                raise QuorumCircuitOpenError(
                    recent_failures=self._quorum_circuit.error_count,
                    window_seconds=self._quorum_circuit.window_seconds,
                    retry_after_seconds=retry_after,
                )

            if active_gate_peer_count > 0 and not self._has_quorum_available():
                await self._release_job_lease(submission.job_id)
                active_gates = active_gate_peer_count + 1
                raise QuorumUnavailableError(
                    active_managers=active_gates,
                    required_quorum=self._quorum_size(),
                )

            primary_dcs, fallback_dcs, worst_health = (
                self._select_datacenters_with_fallback(
                    submission.datacenter_count,
                    submission.datacenters if submission.datacenters else None,
                    job_id=submission.job_id,
                )
            )

            if worst_health == "initializing":
                await self._release_job_lease(submission.job_id)
                self._task_runner.run(
                    self._logger.log,
                    ServerInfo(
                        message=f"Job {submission.job_id}: Datacenters still initializing - client should retry",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    ),
                )
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error="initializing",
                ).dump()

            target_dcs = primary_dcs

            if not target_dcs:
                await self._release_job_lease(submission.job_id)
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error="No available datacenters - all unhealthy",
                ).dump()

            job = GlobalJobStatus(
                job_id=submission.job_id,
                status=JobStatus.SUBMITTED.value,
                datacenters=[],
                timestamp=time.monotonic(),
            )
            self._job_manager.set_job(submission.job_id, job)
            self._job_manager.set_target_dcs(submission.job_id, set(target_dcs))
            if lease_result.lease is not None:
                self._job_manager.set_fence_token(
                    submission.job_id,
                    lease_result.lease.fence_token,
                )

            try:
                workflows: list[tuple[str, list[str], object]] = cloudpickle.loads(
                    submission.workflows
                )
                workflow_ids = {wf_id for wf_id, _, _ in workflows}
                self._state._job_workflow_ids[submission.job_id] = workflow_ids
            except Exception as workflow_parse_error:
                self._state._job_workflow_ids[submission.job_id] = set()
                self._task_runner.run(
                    self._logger.log,
                    ServerError(
                        message=f"Failed to parse workflows for job {submission.job_id}: {workflow_parse_error}",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    ),
                )

            if submission.callback_addr:
                self._job_manager.set_callback(
                    submission.job_id, submission.callback_addr
                )
                self._state._progress_callbacks[submission.job_id] = (
                    submission.callback_addr
                )

            if submission.reporting_configs:
                self._state._job_submissions[submission.job_id] = submission

            self._job_leadership_tracker.assume_leadership(
                job_id=submission.job_id,
                metadata=len(target_dcs),
                initial_token=lease_result.lease.fence_token,
            )

            await self._state.increment_state_version()

            await self._broadcast_job_leadership(
                submission.job_id,
                len(target_dcs),
                submission.callback_addr,
            )

            self._quorum_circuit.record_success()

            ack_response = JobAck(
                job_id=submission.job_id,
                accepted=True,
                queued_position=self._job_manager.job_count(),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()

            # Commit idempotency BEFORE dispatch to prevent duplicate jobs
            # if a retry arrives while dispatch is queued
            if idempotency_key is not None and self._idempotency_cache is not None:
                await self._idempotency_cache.commit(idempotency_key, ack_response)

            self._task_runner.run(
                self._dispatch_job_to_datacenters, submission, target_dcs
            )

            if lease_duration is None:
                lease_duration = lease_result.lease.lease_duration

            self._task_runner.run(
                self._renew_job_lease,
                submission.job_id,
                lease_duration,
                alias=f"job-lease-renewal-{submission.job_id}",
            )

            return ack_response

        except QuorumCircuitOpenError as error:
            if lease_acquired and submission is not None:
                await self._release_job_lease(submission.job_id)
            job_id = submission.job_id if submission is not None else "unknown"
            error_ack = JobAck(
                job_id=job_id,
                accepted=False,
                error=str(error),
            ).dump()
            if idempotency_key is not None and self._idempotency_cache is not None:
                await self._idempotency_cache.reject(idempotency_key, error_ack)
            return error_ack
        except QuorumError as error:
            if lease_acquired and submission is not None:
                await self._release_job_lease(submission.job_id)
            self._quorum_circuit.record_error()
            job_id = submission.job_id if submission is not None else "unknown"
            error_ack = JobAck(
                job_id=job_id,
                accepted=False,
                error=str(error),
            ).dump()
            if idempotency_key is not None and self._idempotency_cache is not None:
                await self._idempotency_cache.reject(idempotency_key, error_ack)
            return error_ack
        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Job submission error: {error}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )
            job_id = submission.job_id if submission is not None else "unknown"
            error_ack = JobAck(
                job_id=job_id,
                accepted=False,
                error=str(error),
            ).dump()
            if idempotency_key is not None and self._idempotency_cache is not None:
                await self._idempotency_cache.reject(idempotency_key, error_ack)
            return error_ack

    async def handle_status_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        gather_job_status: Callable[[str], Awaitable[GlobalJobStatus]],
    ) -> bytes:
        """
        Handle job status request from client.

        Args:
            addr: Client address
            data: Job ID as bytes
            gather_job_status: Callback to gather job status

        Returns:
            Serialized GlobalJobStatus or empty bytes
        """
        start_time = time.monotonic()
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit(client_id, "job_status")
            if not allowed:
                return RateLimitResponse(
                    operation="job_status",
                    retry_after_seconds=retry_after,
                ).dump()

            if self._should_shed_request("JobStatusRequest"):
                return b""

            job_id = data.decode()
            status = await gather_job_status(job_id)
            return status.dump()

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Job status request error: {error}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )
            return b""
        finally:
            latency_ms = (time.monotonic() - start_time) * 1000
            self._record_request_latency(latency_ms)

    async def handle_progress(
        self,
        addr: tuple[str, int],
        data: bytes,
    ) -> bytes:
        """
        Handle job progress update from manager.

        Uses tiered update strategy (AD-15):
        - Tier 1 (Immediate): Critical state changes -> push immediately
        - Tier 2 (Periodic): Regular progress -> batched

        Args:
            addr: Manager address
            data: Serialized JobProgress

        Returns:
            Serialized JobProgressAck
        """
        start_time = time.monotonic()
        try:
            if self._load_shedder.should_shed_handler("receive_job_progress"):
                return JobProgressAck(
                    gate_id=self._get_node_id().full,
                    is_leader=self._is_leader(),
                    healthy_gates=self._get_healthy_gates(),
                ).dump()

            progress = JobProgress.load(data)

            if not self._job_manager.has_job(progress.job_id):
                forwarded = await self._forward_job_progress_to_peers(progress)
                if forwarded:
                    return JobProgressAck(
                        gate_id=self._get_node_id().full,
                        is_leader=self._is_leader(),
                        healthy_gates=self._get_healthy_gates(),
                    ).dump()

            current_fence = self._job_manager.get_fence_token(progress.job_id)
            if progress.fence_token < current_fence:
                self._task_runner.run(
                    self._logger.log,
                    ServerDebug(
                        message=f"Rejecting stale job progress for {progress.job_id}: "
                        f"fence_token {progress.fence_token} < {current_fence}",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    ),
                )
                return JobProgressAck(
                    gate_id=self._get_node_id().full,
                    is_leader=self._is_leader(),
                    healthy_gates=self._get_healthy_gates(),
                ).dump()

            if progress.fence_token > current_fence:
                self._job_manager.set_fence_token(progress.job_id, progress.fence_token)

            job = self._job_manager.get_job(progress.job_id)
            if job:
                if job.status in (
                    JobStatus.COMPLETED.value,
                    JobStatus.FAILED.value,
                    JobStatus.CANCELLED.value,
                ):
                    return JobProgressAck(
                        gate_id=self._get_node_id().full,
                        is_leader=self._is_leader(),
                        healthy_gates=self._get_healthy_gates(),
                    ).dump()

                old_status = job.status

                for idx, dc_prog in enumerate(job.datacenters):
                    if dc_prog.datacenter == progress.datacenter:
                        job.datacenters[idx] = progress
                        break
                else:
                    job.datacenters.append(progress)

                job.total_completed = sum(p.total_completed for p in job.datacenters)
                job.total_failed = sum(p.total_failed for p in job.datacenters)
                job.overall_rate = sum(p.overall_rate for p in job.datacenters)
                job.timestamp = time.monotonic()

                await self._record_dc_job_stats(
                    job_id=progress.job_id,
                    datacenter_id=progress.datacenter,
                    completed=progress.total_completed,
                    failed=progress.total_failed,
                    rate=progress.overall_rate,
                    status=progress.status,
                )

                completed_dcs = sum(
                    1
                    for p in job.datacenters
                    if p.status in (JobStatus.COMPLETED.value, JobStatus.FAILED.value)
                )
                if completed_dcs == len(job.datacenters):
                    failed_dcs = sum(
                        1 for p in job.datacenters if p.status == JobStatus.FAILED.value
                    )
                    job.status = (
                        JobStatus.FAILED.value
                        if failed_dcs > 0
                        else JobStatus.COMPLETED.value
                    )
                    job.completed_datacenters = len(job.datacenters) - failed_dcs
                    job.failed_datacenters = failed_dcs

                self._handle_update_by_tier(
                    progress.job_id,
                    old_status,
                    job.status,
                    data,
                )

                await self._state.increment_state_version()

            return JobProgressAck(
                gate_id=self._get_node_id().full,
                is_leader=self._is_leader(),
                healthy_gates=self._get_healthy_gates(),
            ).dump()

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Job progress error: {error}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )
            return b"error"
        finally:
            latency_ms = (time.monotonic() - start_time) * 1000
            self._record_request_latency(latency_ms)
