"""
TCP handlers for job submission and status operations.

Handles client-facing job operations:
- Job submission from clients
- Job status queries
- Job progress updates from managers
"""

import asyncio
import operator
from operator import attrgetter
from typing import TYPE_CHECKING, Awaitable, Callable

from hyperscale.core.graph.workflow import Workflow

from hyperscale.distributed.models import JobStatusQuery
from hyperscale.distributed.models import (
    GateJobLeaderTransfer,
    GateJobReplica,
    GlobalJobStatus,
    JobAck,
    JobLeaderGateTransfer,
    JobLeaderGateTransferAck,
    JobProgress,
    JobProgressAck,
    JobStatus,
    JobSubmission,
    restricted_loads,
)
from hyperscale.distributed.jobs.workflow_dependencies import (
    resolve_job_deadline_seconds,
    validate_workflow_dependencies,
)
from hyperscale.distributed.leases import JobLeaseManager
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
    IdempotencyEntry,
    IdempotencyKey,
    IdempotencyStatus,
)
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerError,
    ServerInfo,
    ServerWarning,
)

from hyperscale.distributed.nodes.gate.state import GateRuntimeState

from hyperscale.distributed.runtime import Clock



if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId, ErrorStats
    from hyperscale.distributed.jobs.gates import GateJobManager
    from hyperscale.distributed.jobs import JobLeadershipTracker
    from hyperscale.distributed.reliability import LoadShedder

    from hyperscale.distributed.models import GateInfo
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.distributed.nodes.gate.replication_coordinator import (
        GateJobReplicationCoordinator,
    )


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
        job_leadership_tracker: "JobLeadershipTracker",
        quorum_circuit: "ErrorStats",
        load_shedder: "LoadShedder",
        job_lease_manager: JobLeaseManager,
        send_tcp: Callable,
        idempotency_cache: GateIdempotencyCache[bytes] | None,
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        is_leader: Callable[[], bool],
        check_rate_limit: Callable[[str, str], tuple[bool, float]],
        should_shed_request: Callable[[str], bool],
        has_quorum_available: Callable[[], bool],
        quorum_size: Callable[[], int],
        select_datacenters_with_fallback: Callable[..., Awaitable[tuple[list[str], list[str], str]]],
        get_healthy_gates: Callable[[], list["GateInfo"]],
        broadcast_job_leadership: Callable[
            [str, int, tuple[str, int] | None], Awaitable[None]
        ],
        dispatch_job_to_datacenters: Callable,
        forward_job_progress_to_peers: Callable,
        record_request_latency: Callable[[float], None],
        record_dc_job_stats: Callable,
        handle_update_by_tier: Callable,
        client_push_timeout_seconds: float,
        clock: Clock,
        replication_coordinator: "GateJobReplicationCoordinator | None" = None,
        get_active_peer_addrs: Callable[[], list[tuple[str, int]]] | None = None,
        *,
        default_timeout_multiplier: float,
        current_raft_members: Callable[[], frozenset[str]],
        cluster_formed: Callable[[], bool],
        cluster_read_only: Callable[[], bool],
        cluster_formation_retry_after_seconds: Callable[[], float],
        overload_retry_after_seconds: float,
        replication_retry_after_seconds: float,
    ) -> None:
        """
        Initialize the job handler.

        Args:
            state: Runtime state container
            logger: Async logger instance
            task_runner: Background task executor
            job_manager: Job management service
            job_leadership_tracker: Per-job leadership tracker
            quorum_circuit: Quorum operation circuit breaker
            load_shedder: Load shedding manager
            job_lease_manager: Job lease manager
            send_tcp: Callback to send TCP messages
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
            default_timeout_multiplier: A workflow's deadline per unit of
                its duration, without a timeout of its own (the budget of
                a job submitted without one)
            current_raft_members: The gate cluster's live members, this
                gate among them: an accepted job's Raft group voters
            cluster_formed: Whether the gate cluster's membership group has
                formed -- a job's Raft group has no voters before it
            cluster_read_only: Whether an operator put the gate cluster in
                read-only mode (AD-52 section 13)
            cluster_formation_retry_after_seconds: When a submission refused
                for an unformed gate cluster may retry: the gate's next
                formation round, the soonest the cluster can form
            overload_retry_after_seconds: When a submission shed for load
                may retry: the gate's overload sampling interval, the
                soonest its verdict can change
            replication_retry_after_seconds: When a submission refused for
                want of a replication quorum may retry: one peer replication
                round's budget, the soonest a retry is not the same round
        """
        self._clock: Clock = clock
        self._default_timeout_multiplier: float = default_timeout_multiplier
        self._current_raft_members = current_raft_members
        self._cluster_formed = cluster_formed
        self._cluster_read_only = cluster_read_only
        self._cluster_formation_retry_after_seconds = cluster_formation_retry_after_seconds
        self._overload_retry_after_seconds = overload_retry_after_seconds
        self._replication_retry_after_seconds = replication_retry_after_seconds
        self._state: GateRuntimeState = state
        self._logger: Logger = logger
        self._task_runner: "TaskRunner" = task_runner
        self._job_manager: "GateJobManager" = job_manager
        self._job_leadership_tracker: "JobLeadershipTracker" = job_leadership_tracker
        self._quorum_circuit: "ErrorStats" = quorum_circuit
        self._load_shedder: "LoadShedder" = load_shedder
        self._job_lease_manager: JobLeaseManager = job_lease_manager
        self._send_tcp: Callable = send_tcp
        self._client_push_timeout_seconds: float = client_push_timeout_seconds
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
        self._select_datacenters_with_fallback: Callable[..., Awaitable[tuple[list[str], list[str], str]]] = (
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
        self._replication_coordinator = replication_coordinator
        self._get_active_peer_addrs = get_active_peer_addrs

    def _is_terminal_status(self, status: str) -> bool:
        return status in (
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        )

    def _calculate_progress_percentage(
        self,
        job: GlobalJobStatus,
        target_dc_count: int,
    ) -> float:
        """
        Calculate job progress percentage based on datacenter completion.

        Calculation strategy:
        - Each target DC contributes equally to progress (100% / target_dc_count)
        - Terminal DCs (completed/failed/cancelled/timeout) contribute 100%
        - Running DCs contribute based on (completed + failed) / max if we had prior data
        - If no data, running DCs contribute 0%

        Returns:
            Progress percentage between 0.0 and 100.0
        """
        if target_dc_count == 0:
            return 0.0

        if self._is_terminal_status(job.status):
            return 100.0

        return self._accumulate_datacenter_progress(job, 100.0 / target_dc_count)

    def _accumulate_datacenter_progress(self, job: GlobalJobStatus, dc_weight: float) -> float:
        """Add up each datacenter's weighted progress, clamped to 0..100."""
        total_progress = 0.0
        for dc_progress in job.datacenters:
            total_progress += self._datacenter_progress_weight(dc_progress, dc_weight)

        return min(100.0, max(0.0, total_progress))

    def _datacenter_progress_weight(self, dc_progress: JobProgress, dc_weight: float) -> float:
        """A datacenter's share of job progress: all of its weight once
        terminal, half once it reported any work done, else none."""
        if self._is_terminal_status(dc_progress.status):
            return dc_weight

        total_done = dc_progress.total_completed + dc_progress.total_failed
        return dc_weight * 0.5 if total_done > 0 else 0.0

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
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
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
        # Renewed while half the lease is left. The one-second floor this
        # had let a lease under two seconds run thinner between renewals,
        # and one under a second lapse -- lost while its job still ran.
        renewal_interval = lease_duration * 0.5

        try:
            keep_renewing = True
            while keep_renewing:
                keep_renewing = await self._renew_job_lease_round(
                    job_id, lease_duration, renewal_interval
                )
        except asyncio.CancelledError:
            self._pop_lease_renewal_token(job_id)
            return

    async def _renew_job_lease_round(
        self,
        job_id: str,
        lease_duration: float,
        renewal_interval: float,
    ) -> bool:
        """Sleep one renewal interval, then renew the job's lease; False (the
        lease released) once the job is gone or terminal, or the lease lost."""
        await self._clock.sleep(renewal_interval)
        job = self._job_manager.get_job(job_id)
        if self._is_job_gone_or_terminal(job):
            await self._release_job_lease(job_id, cancel_renewal=False)
            return False

        lease_renewed = await self._job_lease_manager.renew(
            job_id, lease_duration
        )
        if not lease_renewed:
            await self._logger.log(
                ServerError(
                    message=f"Failed to renew lease for job {job_id}: lease lost",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )
            await self._release_job_lease(job_id, cancel_renewal=False)
            return False

        return True

    def _is_job_gone_or_terminal(self, job: GlobalJobStatus | None) -> bool:
        """Whether this gate no longer tracks the job, or it is terminal."""
        return job is None or self._is_terminal_status(job.status)

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
        # Until the submission loads, a failure holds no lease and no
        # idempotency entry, and is answered for an unknown job.
        try:
            if (refusal := await self._screen_submission_request(addr)) is not None:
                return refusal

            submission = JobSubmission.load(data)

        except Exception as error:
            return await self._submission_error_ack(error, "unknown", [])

        return await self._admit_submission(submission, active_gate_peer_count)

    async def _screen_submission_request(self, addr: tuple[str, int]) -> bytes | None:
        """Refuse a submission the client's rate limit or the gate's load
        shedding turns away; None when it may proceed."""
        client_id = f"{addr[0]}:{addr[1]}"
        allowed, retry_after = await self._check_rate_limit(client_id, "job_submit")
        if not allowed:
            return RateLimitResponse(
                operation="job_submit",
                retry_after_seconds=retry_after,
            ).dump()

        if self._should_shed_request("job_submission"):
            overload_state = self._load_shedder.get_current_state()
            return JobAck(
                job_id="",
                accepted=False,
                error=f"System under load ({overload_state.value}), please retry later",
                retry_after_seconds=self._overload_retry_after_seconds,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()

        return None

    async def _admit_submission(
        self,
        submission: JobSubmission,
        active_gate_peer_count: int,
    ) -> bytes:
        """Admit a loaded submission, answering any failure with a refusal
        and releasing what the attempt still holds."""
        # The idempotency key this request claimed, which it commits on
        # acceptance.
        claimed_idempotency_keys: list[IdempotencyKey] = []
        # The PENDING idempotency entry this request inserted, until the
        # request commits it: every other exit releases it (finally), so
        # a transient refusal is neither replayed to the client's retries
        # nor left pending for them to wait on.
        owned_idempotency_keys: list[IdempotencyKey] = []
        # The job whose lease this request acquired, released when it fails.
        held_lease_job_ids: list[str] = []

        try:
            return await self._admit_loaded_submission(
                submission,
                active_gate_peer_count,
                claimed_idempotency_keys,
                owned_idempotency_keys,
                held_lease_job_ids,
            )

        except Exception as error:
            return await self._submission_error_ack(error, submission.job_id, held_lease_job_ids)
        finally:
            await self._release_owned_idempotency_keys(owned_idempotency_keys)

    async def _release_owned_idempotency_keys(self, owned_idempotency_keys: list[IdempotencyKey]) -> None:
        """Release the PENDING idempotency entry a request still owns."""
        for owned_idempotency_key in owned_idempotency_keys:
            if self._idempotency_cache is not None:
                await self._idempotency_cache.release(owned_idempotency_key)

    async def _submission_error_ack(
        self,
        error: Exception,
        job_id: str,
        held_lease_job_ids: list[str],
    ) -> bytes:
        """Refuse a submission that failed: release the lease it acquired,
        account for the error by kind, and answer with it."""
        for lease_job_id in held_lease_job_ids:
            await self._release_job_lease(lease_job_id)
        await self._account_submission_error(error)
        error_ack = JobAck(
            job_id=job_id,
            accepted=False,
            error=str(error),
        ).dump()
        return error_ack

    async def _account_submission_error(self, error: Exception) -> None:
        """An open quorum circuit is already accounted for; any other quorum
        failure counts against the circuit; anything else is logged."""
        if isinstance(error, QuorumCircuitOpenError):
            return

        if isinstance(error, QuorumError):
            self._quorum_circuit.record_error()
            return

        await self._logger.log(
            ServerError(
                message=f"Job submission error: {error}",
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            )
        )

    async def _admit_loaded_submission(
        self,
        submission: JobSubmission,
        active_gate_peer_count: int,
        claimed_idempotency_keys: list[IdempotencyKey],
        owned_idempotency_keys: list[IdempotencyKey],
        held_lease_job_ids: list[str],
    ) -> bytes:
        """Refuse a submission from an incompatible client or one that cannot
        be placed as asked; admit the rest."""
        if (refusal := self._protocol_version_refusal(submission)) is not None:
            return refusal

        negotiated_caps_str = self._negotiated_capabilities_for(submission)

        if (refusal := self._placement_refusal(submission, negotiated_caps_str)) is not None:
            return refusal

        return await self._admit_placeable_submission(
            submission,
            active_gate_peer_count,
            negotiated_caps_str,
            claimed_idempotency_keys,
            owned_idempotency_keys,
            held_lease_job_ids,
        )

    def _protocol_version_refusal(self, submission: JobSubmission) -> bytes | None:
        """Refuse a client whose protocol major version differs from ours."""
        client_version = ProtocolVersion(
            major=submission.protocol_version_major,
            minor=submission.protocol_version_minor,
        )

        if client_version.major != CURRENT_PROTOCOL_VERSION.major:
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=f"Incompatible protocol version: {client_version} (requires major version {CURRENT_PROTOCOL_VERSION.major})",
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()

        return None

    def _negotiated_capabilities_for(self, submission: JobSubmission) -> str:
        """The capabilities both the client and this gate's protocol
        version support, comma-joined in sorted order."""
        client_caps_str = submission.capabilities
        client_features = (
            set(client_caps_str.split(",")) if client_caps_str else set()
        )
        our_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
        negotiated_features = client_features & our_features
        return ",".join(sorted(negotiated_features))

    def _placement_refusal(self, submission: JobSubmission, negotiated_caps_str: str) -> bytes | None:
        """Refuse a job that cannot be placed as asked."""
        # A job runs in at least one datacenter, and a job that lists
        # its datacenters runs in no more than it lists: anything else
        # cannot be placed as asked (a non-positive count also sliced
        # the routing order from its end).
        listed_datacenters = set(submission.datacenters)
        if self._is_unplaceable(submission, listed_datacenters):
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=(
                    f"Unplaceable job: datacenter_count={submission.datacenter_count} "
                    f"with datacenters={sorted(listed_datacenters)} -- a job runs in "
                    "at least one datacenter and in no more than it lists"
                ),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()

        return None

    @staticmethod
    def _is_unplaceable(submission: JobSubmission, listed_datacenters: set[str]) -> bool:
        """Whether the job asks for no datacenter, or for more than it lists."""
        return submission.datacenter_count < 1 or (
            bool(listed_datacenters)
            and submission.datacenter_count > len(listed_datacenters)
        )

    async def _admit_placeable_submission(
        self,
        submission: JobSubmission,
        active_gate_peer_count: int,
        negotiated_caps_str: str,
        claimed_idempotency_keys: list[IdempotencyKey],
        owned_idempotency_keys: list[IdempotencyKey],
        held_lease_job_ids: list[str],
    ) -> bytes:
        """Refuse a submission whose workflows no manager could run; fill in
        the deadline of one submitted without, and admit it."""
        # The workflows, read as a manager reads them -- through the
        # restricted unpickler, not one that runs whatever a payload
        # names -- and refused here, with the reason, when no manager
        # could run them: in dependency order or at all. A job's
        # workflow ids are how this gate tracks its results.
        try:
            workflows = restricted_loads(submission.workflows)
            validate_workflow_dependencies(workflows)
        except Exception as workflow_error:
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=f"Invalid workflows: {type(workflow_error).__name__}: {workflow_error}",
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()
        workflow_ids = {workflow_id for workflow_id, _, _ in workflows}
        self._fill_job_deadline(submission, workflows)

        return await self._admit_valid_submission(
            submission,
            active_gate_peer_count,
            negotiated_caps_str,
            workflow_ids,
            claimed_idempotency_keys,
            owned_idempotency_keys,
            held_lease_job_ids,
        )

    def _fill_job_deadline(
        self,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
    ) -> None:
        """Give a job submitted without a timeout its dependency-chain budget."""
        # A job submitted without a timeout of its own has as long as
        # its longest chain of dependent workflows may take: the
        # budget every gate times it by (it travels in the replica),
        # and the one its managers are given.
        if submission.timeout_seconds <= 0.0:
            submission.timeout_seconds = resolve_job_deadline_seconds(
                workflows,
                self._default_timeout_multiplier,
            )

    async def _admit_valid_submission(
        self,
        submission: JobSubmission,
        active_gate_peer_count: int,
        negotiated_caps_str: str,
        workflow_ids: set[str],
        claimed_idempotency_keys: list[IdempotencyKey],
        owned_idempotency_keys: list[IdempotencyKey],
        held_lease_job_ids: list[str],
    ) -> bytes:
        """Answer a duplicate from its idempotency entry, refuse while the gate
        cluster cannot take jobs, and admit the rest."""
        if (
            refusal := await self._claim_idempotency_key(
                submission, negotiated_caps_str, claimed_idempotency_keys, owned_idempotency_keys
            )
        ) is not None:
            return refusal

        if (refusal := self._cluster_refusal(submission, negotiated_caps_str)) is not None:
            return refusal

        return await self._admit_under_lease(
            submission,
            active_gate_peer_count,
            negotiated_caps_str,
            workflow_ids,
            claimed_idempotency_keys,
            owned_idempotency_keys,
            held_lease_job_ids,
        )

    async def _claim_idempotency_key(
        self,
        submission: JobSubmission,
        negotiated_caps_str: str,
        claimed_idempotency_keys: list[IdempotencyKey],
        owned_idempotency_keys: list[IdempotencyKey],
    ) -> bytes | None:
        """Check the submission's idempotency key, inserting a PENDING entry
        this request then owns; the answer for a key already decided or
        being decided, else None."""
        if not (submission.idempotency_key and self._idempotency_cache is not None):
            return None

        idempotency_key = IdempotencyKey.parse(submission.idempotency_key)
        claimed_idempotency_keys.append(idempotency_key)
        found, entry = await self._idempotency_cache.check_or_insert(
            idempotency_key,
            submission.job_id,
            self._get_node_id().full,
        )
        return self._idempotency_answer(
            submission, negotiated_caps_str, idempotency_key, found, entry, owned_idempotency_keys
        )

    def _idempotency_answer(
        self,
        submission: JobSubmission,
        negotiated_caps_str: str,
        idempotency_key: IdempotencyKey,
        found: bool,
        entry: IdempotencyEntry[bytes] | None,
        owned_idempotency_keys: list[IdempotencyKey],
    ) -> bytes | None:
        """None when this request inserted the key's PENDING entry, which it
        then owns; else the answer for the earlier attempt's entry."""
        if not found:
            owned_idempotency_keys.append(idempotency_key)
            return None

        return self._existing_idempotency_answer(submission, negotiated_caps_str, entry)

    def _existing_idempotency_answer(
        self,
        submission: JobSubmission,
        negotiated_caps_str: str,
        entry: IdempotencyEntry[bytes] | None,
    ) -> bytes | None:
        """The answer for a key some earlier attempt inserted (AD-40)."""
        if entry is None or entry.status == IdempotencyStatus.PENDING:
            # An earlier attempt with this key is still being
            # decided (or was just released). This request must
            # not decide it too: a waiter that went on to process
            # it admitted the job after the client had given up.
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="submission in progress, retry",
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()

        return self._duplicate_answer(submission, entry)

    def _duplicate_answer(
        self,
        submission: JobSubmission,
        entry: IdempotencyEntry[bytes],
    ) -> bytes | None:
        """A duplicate's answer for a key already decided; None for an entry
        in no decided state."""
        if entry.status not in (
            IdempotencyStatus.COMMITTED,
            IdempotencyStatus.REJECTED,
        ):
            return None

        # AD-40: the original decision, for the original
        # job, marked as a duplicate's answer.
        if entry.result is not None:
            return self._replayed_duplicate_ack(entry.result)

        return self._duplicate_decision_ack(submission, entry)

    @staticmethod
    def _replayed_duplicate_ack(original_result: bytes) -> bytes:
        """The original ack, replayed as a duplicate's answer (AD-40)."""
        original_ack = JobAck.load(original_result)
        original_ack.was_duplicate = True
        original_ack.original_job_id = original_ack.job_id
        return original_ack.dump()

    @staticmethod
    def _duplicate_decision_ack(submission: JobSubmission, entry: IdempotencyEntry[bytes]) -> bytes:
        """A duplicate's answer rebuilt from a decided entry without a stored
        result (AD-40)."""
        original_job_id = entry.job_id or submission.job_id
        return JobAck(
            job_id=original_job_id,
            accepted=entry.status == IdempotencyStatus.COMMITTED,
            error="Duplicate request"
            if entry.status == IdempotencyStatus.REJECTED
            else None,
            was_duplicate=True,
            original_job_id=original_job_id,
        ).dump()

    def _cluster_refusal(self, submission: JobSubmission, negotiated_caps_str: str) -> bytes | None:
        """Refuse while the gate cluster has not formed or is read-only."""
        # AD-52: a job's Raft group is founded with the gate cluster's
        # committed members -- there are none until the cluster forms.
        if not self._cluster_formed():
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="Gate cluster membership not formed yet; retry",
                retry_after_seconds=self._cluster_formation_retry_after_seconds(),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()

        if self._cluster_read_only():
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="Gate cluster is read-only: job submissions are refused",
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()

        return None

    async def _admit_under_lease(
        self,
        submission: JobSubmission,
        active_gate_peer_count: int,
        negotiated_caps_str: str,
        workflow_ids: set[str],
        claimed_idempotency_keys: list[IdempotencyKey],
        owned_idempotency_keys: list[IdempotencyKey],
        held_lease_job_ids: list[str],
    ) -> bytes:
        """Acquire the job's lease, refuse while the gate quorum is unhealthy,
        and place the job."""
        lease = await self._job_lease_manager.acquire(submission.job_id)
        held_lease_job_ids.append(submission.job_id)

        await self._raise_if_quorum_circuit_open(submission)
        await self._raise_if_quorum_unavailable(submission, active_gate_peer_count)

        return await self._place_submission(
            submission,
            negotiated_caps_str,
            workflow_ids,
            lease.lease_duration,
            lease.fence_token,
            claimed_idempotency_keys,
            owned_idempotency_keys,
        )

    async def _raise_if_quorum_circuit_open(self, submission: JobSubmission) -> None:
        """Release the lease and raise while the quorum circuit is open."""
        if self._quorum_circuit.circuit_state == CircuitState.OPEN:
            await self._release_job_lease(submission.job_id)
            retry_after = self._quorum_circuit.half_open_after
            raise QuorumCircuitOpenError(
                recent_failures=self._quorum_circuit.error_count,
                window_seconds=self._quorum_circuit.window_seconds,
                retry_after_seconds=retry_after,
            )

    async def _raise_if_quorum_unavailable(
        self,
        submission: JobSubmission,
        active_gate_peer_count: int,
    ) -> None:
        """Release the lease and raise when gate peers exist but no quorum
        of them is available."""
        if active_gate_peer_count > 0 and not self._has_quorum_available():
            await self._release_job_lease(submission.job_id)
            active_gates = active_gate_peer_count + 1
            raise QuorumUnavailableError(
                active_managers=active_gates,
                required_quorum=self._quorum_size(),
            )

    async def _place_submission(
        self,
        submission: JobSubmission,
        negotiated_caps_str: str,
        workflow_ids: set[str],
        lease_duration: float,
        fence_token: int,
        claimed_idempotency_keys: list[IdempotencyKey],
        owned_idempotency_keys: list[IdempotencyKey],
    ) -> bytes:
        """Select the job's datacenters, refusing when none can take it, and
        accept it there."""
        primary_dcs, fallback_dcs, worst_health = (
            await self._select_datacenters_with_fallback(
                submission.datacenter_count,
                submission.datacenters if submission.datacenters else None,
                job_id=submission.job_id,
                dispatch_latency_budget_ms=submission.dispatch_latency_budget_ms,
            )
        )

        if (refusal := await self._datacenter_selection_refusal(submission, worst_health, primary_dcs)) is not None:
            return refusal

        target_dcs = primary_dcs

        return await self._replicate_and_accept(
            submission,
            negotiated_caps_str,
            workflow_ids,
            target_dcs,
            lease_duration,
            fence_token,
            claimed_idempotency_keys,
            owned_idempotency_keys,
        )

    async def _datacenter_selection_refusal(
        self,
        submission: JobSubmission,
        worst_health: str,
        target_dcs: list[str],
    ) -> bytes | None:
        """Release the lease and refuse while the datacenters initialize, or
        when none is healthy."""
        if worst_health == "initializing":
            await self._release_job_lease(submission.job_id)
            await self._logger.log(
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

        if not target_dcs:
            await self._release_job_lease(submission.job_id)
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="No available datacenters - all unhealthy",
            ).dump()

        return None

    async def _replicate_and_accept(
        self,
        submission: JobSubmission,
        negotiated_caps_str: str,
        workflow_ids: set[str],
        target_dcs: list[str],
        lease_duration: float,
        fence_token: int,
        claimed_idempotency_keys: list[IdempotencyKey],
        owned_idempotency_keys: list[IdempotencyKey],
    ) -> bytes:
        """Replicate the job's takeover capsule to a quorum (AD-31), then
        accept it: announce leadership, commit idempotency, dispatch, and
        keep its lease renewed."""
        replica = self._build_job_replica(submission, workflow_ids, target_dcs, fence_token)

        quorum_committed = await self._replicate_job_replica(replica)

        if not quorum_committed:
            await self._release_job_lease(submission.job_id)
            error_ack = JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="gate_replication_quorum_unavailable",
                retry_after_seconds=self._replication_retry_after_seconds,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()
            return error_ack

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
        await self._commit_idempotency_keys(claimed_idempotency_keys, owned_idempotency_keys, ack_response)

        self._task_runner.run(
            self._dispatch_job_to_datacenters, submission, target_dcs
        )

        self._start_lease_renewal(submission.job_id, lease_duration)

        return ack_response

    def _build_job_replica(
        self,
        submission: JobSubmission,
        workflow_ids: set[str],
        target_dcs: list[str],
        fence_token: int,
    ) -> GateJobReplica:
        """Build the takeover capsule a peer gate dispatches the job from."""
        # Build the takeover capsule. ``submission_payload`` carries
        # the serialized submission so peer gates that eventually
        # take over leadership can dispatch the job without
        # re-fetching from the client.
        origin_addr = (self._get_host(), self._get_tcp_port())
        replica_callback = (
            tuple(submission.callback_addr)
            if submission.callback_addr
            else None
        )
        return GateJobReplica(
            job_id=submission.job_id,
            sequence=fence_token,
            fence_token=fence_token,
            leader_id=self._get_node_id().full,
            leader_addr=origin_addr,
            origin_gate_addr=origin_addr,
            callback_addr=replica_callback,
            target_dcs=list(target_dcs),
            target_dc_count=len(target_dcs),
            status_seed=JobStatus.SUBMITTED.value,
            submitted_wall_time=self._clock.time(),
            # The job's Raft group voters: the gates live now, the same
            # on every gate that joins the group (AD-52).
            raft_voters=sorted(self._current_raft_members()),
            workflow_ids=list(workflow_ids),
            # The submission as admitted -- its budget filled in.
            submission_payload=submission.dump(),
            idempotency_key=submission.idempotency_key or "",
        )

    async def _replicate_job_replica(self, replica: GateJobReplica) -> bool:
        """Replicate the capsule to a quorum of live gates (AD-31); False
        when no quorum committed it."""
        # AD-31 takeover invariant: JobAck(accepted=True) must not
        # return until the replica is durably replicated to a
        # quorum of live gates. Single-gate clusters pass quorum
        # with self alone (no peer round-trip); multi-gate
        # clusters require ⌊N/2⌋ peer prepare-acks. On quorum
        # failure the coordinator has already aborted any
        # prepared peers; we release the lease, reject
        # idempotency, and return a retry hint.
        quorum_committed = False
        if self._replication_coordinator is not None:
            peer_addrs: list[tuple[str, int]] = (
                self._get_active_peer_addrs()
                if self._get_active_peer_addrs is not None
                else []
            )
            quorum_committed = (
                await self._replication_coordinator.replicate_with_quorum(
                    replica=replica,
                    peer_addrs=peer_addrs,
                    quorum_size=self._quorum_size(),
                )
            )
        return quorum_committed

    async def _commit_idempotency_keys(
        self,
        claimed_idempotency_keys: list[IdempotencyKey],
        owned_idempotency_keys: list[IdempotencyKey],
        ack_response: bytes,
    ) -> None:
        """Commit the claimed idempotency key with the accepting ack; the
        request no longer owns a PENDING entry to release."""
        for idempotency_key in claimed_idempotency_keys:
            await self._idempotency_cache.commit(idempotency_key, ack_response)
            owned_idempotency_keys.clear()

    def _start_lease_renewal(self, job_id: str, lease_duration: float) -> None:
        """Keep the job's lease renewed, unless a renewal already runs."""
        if job_id not in self._state._job_lease_renewal_tokens:
            run = self._task_runner.run(
                self._renew_job_lease,
                job_id,
                lease_duration,
                alias=f"job-lease-renewal-{job_id}",
            )
            if run:
                self._state._job_lease_renewal_tokens[job_id] = run.token

    async def handle_status_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        answer_status_query: Callable[[JobStatusQuery], Awaitable[bytes]],
    ) -> bytes:
        """
        Handle job status request from client.

        Args:
            addr: Client address
            data: A ``JobStatusQuery``, or a bare job id (an EVENTUAL read,
                as older clients send)
            answer_status_query: Answers a query at its consistency level

        Returns:
            Serialized GlobalJobStatus, or empty bytes when this gate has no
            answer at the level asked (the client asks elsewhere)
        """
        start_time = self._clock.monotonic()
        try:
            return await self._answer_status_request(addr, data, answer_status_query)

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
            latency_ms = (self._clock.monotonic() - start_time) * 1000
            self._record_request_latency(latency_ms)

    async def _answer_status_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        answer_status_query: Callable[[JobStatusQuery], Awaitable[bytes]],
    ) -> bytes:
        """Answer a status request the client's rate limit and the gate's
        load shedding admit."""
        client_id = f"{addr[0]}:{addr[1]}"
        allowed, retry_after = await self._check_rate_limit(client_id, "job_status")
        if not allowed:
            return RateLimitResponse(
                operation="job_status",
                retry_after_seconds=retry_after,
            ).dump()

        if self._should_shed_request("job_status"):
            return b""

        return await answer_status_query(self._parse_status_query(data))

    @staticmethod
    def _parse_status_query(data: bytes) -> JobStatusQuery:
        """Read a status query, or a bare job id as an EVENTUAL query."""
        # A pickled query begins with the pickle protocol marker; a bare
        # job id is text.
        return JobStatusQuery.load(data) if data[:1] == b"\x80" else JobStatusQuery(job_id=data.decode())

    def _progress_ack(self) -> bytes:
        """The ack a manager's progress report gets, whatever became of it."""
        return JobProgressAck(
            gate_id=self._get_node_id().full,
            is_leader=self._is_leader(),
            healthy_gates=self._get_healthy_gates(),
        ).dump()

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
        start_time = self._clock.monotonic()
        try:
            return await self._process_progress(data)

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
            latency_ms = (self._clock.monotonic() - start_time) * 1000
            self._record_request_latency(latency_ms)

    async def _process_progress(self, data: bytes) -> bytes:
        """Fold a manager's progress report into its job, unless the report
        is shed, stale, misdirected or for a datacenter the job left."""
        if self._load_shedder.should_shed_handler("receive_job_progress"):
            return self._progress_ack()

        progress = JobProgress.load(data)

        job = self._job_manager.get_job(progress.job_id)
        if (answer := await self._screen_progress(progress, job)) is not None:
            return answer

        return await self._apply_progress(progress, data)

    async def _screen_progress(
        self,
        progress: JobProgress,
        job: GlobalJobStatus | None,
    ) -> bytes | None:
        """The ack for a report this gate does not fold in -- its job is
        terminal, or another gate leads it; None to fold it in."""
        if job and self._is_terminal_status(job.status):
            await self._logger.log(
                ServerInfo(
                    message=(
                        "Discarding progress update for terminal job "
                        f"{progress.job_id} (status={job.status})"
                    ),
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )
            await self._release_job_lease(progress.job_id)
            return self._progress_ack()

        return await self._screen_untracked_job_progress(progress, job)

    async def _screen_untracked_job_progress(
        self,
        progress: JobProgress,
        job: GlobalJobStatus | None,
    ) -> bytes | None:
        """The ack for a report of a job this gate does not track that a peer
        gate took; None to screen it further."""
        if job is None and await self._forward_job_progress_to_peers(progress):
            return self._progress_ack()

        return await self._screen_progress_source(progress, job)

    async def _screen_progress_source(
        self,
        progress: JobProgress,
        job: GlobalJobStatus | None,
    ) -> bytes | None:
        """The ack for a report from a datacenter the job left, or one out of
        sequence; None to fold it in."""
        target_dcs = self._job_manager.get_target_dcs(progress.job_id)
        if self._is_from_released_datacenter(progress, job, target_dcs):
            # A datacenter the job moved off -- lost and replaced, or
            # released at dispatch: its work counts as it stood when it
            # was lost (AD-36), and it was told to stop.
            await self._logger.log(
                ServerDebug(
                    message=(
                        f"Dropped progress of job {progress.job_id} from DC "
                        f"{progress.datacenter}: the job runs in {sorted(target_dcs)}"
                    ),
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                ),
            )
            return self._progress_ack()

        accepted, reason = await self._state.check_and_record_progress(
            job_id=progress.job_id,
            datacenter_id=progress.datacenter,
            progress_sequence=progress.progress_sequence,
            timestamp=progress.timestamp,
        )
        if not accepted:
            await self._logger.log(
                ServerDebug(
                    message=f"Rejecting job progress for {progress.job_id} from {progress.datacenter}: "
                    f"reason={reason}, progress_sequence={progress.progress_sequence}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                ),
            )
            return self._progress_ack()

        return None

    @staticmethod
    def _is_from_released_datacenter(
        progress: JobProgress,
        job: GlobalJobStatus | None,
        target_dcs: set[str],
    ) -> bool:
        """Whether a tracked job's report comes from outside its target DCs."""
        return job is not None and bool(target_dcs) and progress.datacenter not in target_dcs

    async def _apply_progress(self, progress: JobProgress, data: bytes) -> bytes:
        """Advance the job's fence token and fold the report into the job."""
        current_fence = self._advance_fence_token(progress)

        job = self._job_manager.get_job(progress.job_id)
        if job:
            await self._merge_progress_into_job(job, progress, current_fence, data)

        return self._progress_ack()

    def _advance_fence_token(self, progress: JobProgress) -> int:
        """Raise the job's fence token to the report's when the report's is
        higher; returns the job's fence token."""
        current_fence = self._job_manager.get_fence_token(progress.job_id)
        if progress.fence_token > current_fence:
            current_fence = progress.fence_token
            self._job_manager.set_fence_token(progress.job_id, progress.fence_token)
        return current_fence

    async def _merge_progress_into_job(
        self,
        job: GlobalJobStatus,
        progress: JobProgress,
        current_fence: int,
        data: bytes,
    ) -> None:
        """Fold a datacenter's report into the job's totals and progress,
        settle the job once every datacenter is done, and push the update
        by tier (AD-15)."""
        job.fence_token = current_fence
        old_status = job.status

        self._replace_datacenter_progress(job, progress)

        job.total_completed = sum(map(attrgetter("total_completed"), job.datacenters))
        job.total_failed = sum(map(attrgetter("total_failed"), job.datacenters))
        job.overall_rate = sum(map(attrgetter("overall_rate"), job.datacenters))

        target_dcs = self._job_manager.get_target_dcs(progress.job_id)
        target_dc_count = (
            len(target_dcs) if target_dcs else len(job.datacenters)
        )
        job.progress_percentage = self._calculate_progress_percentage(
            job, target_dc_count
        )

        await self._record_dc_job_stats(
            job_id=progress.job_id,
            datacenter_id=progress.datacenter,
            completed=progress.total_completed,
            failed=progress.total_failed,
            rate=progress.overall_rate,
            status=progress.status,
        )

        await self._settle_job_completion(job, progress, target_dcs, target_dc_count)

        if self._is_terminal_status(job.status):
            await self._release_job_lease(progress.job_id)
            self._state.cleanup_job_progress_tracking(progress.job_id)

        self._handle_update_by_tier(
            progress.job_id,
            old_status,
            job.status,
            data,
        )

        await self._state.increment_state_version()

    @staticmethod
    def _replace_datacenter_progress(job: GlobalJobStatus, progress: JobProgress) -> None:
        """Replace the datacenter's last report with this one, or add it."""
        for idx, dc_prog in enumerate(job.datacenters):
            if dc_prog.datacenter == progress.datacenter:
                job.datacenters[idx] = progress
                break
        else:
            job.datacenters.append(progress)

    async def _settle_job_completion(
        self,
        job: GlobalJobStatus,
        progress: JobProgress,
        target_dcs: set[str],
        target_dc_count: int,
    ) -> None:
        """Settle the job's status once every target datacenter reported a
        terminal state; warn of target datacenters that never reported."""
        reported_dc_ids = {p.datacenter for p in job.datacenters}
        terminal_dcs = self._count_terminal_datacenters(job)

        all_target_dcs_reported = self._all_target_dcs_reported(target_dcs, reported_dc_ids)
        all_reported_dcs_terminal = terminal_dcs == len(job.datacenters)

        await self._warn_of_missing_target_dcs(
            progress, target_dcs, reported_dc_ids, all_target_dcs_reported, all_reported_dcs_terminal
        )

        if self._job_can_complete(target_dcs, all_target_dcs_reported, all_reported_dcs_terminal):
            self._complete_job(job, target_dc_count)

    def _count_terminal_datacenters(self, job: GlobalJobStatus) -> int:
        """How many of the job's datacenters reported a terminal status."""
        return sum(1 for p in job.datacenters if self._is_terminal_status(p.status))

    @staticmethod
    def _all_target_dcs_reported(target_dcs: set[str], reported_dc_ids: set[str]) -> bool:
        """Whether the job has target datacenters and each one reported."""
        return bool(target_dcs) and target_dcs <= reported_dc_ids

    @staticmethod
    def _job_can_complete(
        target_dcs: set[str],
        all_target_dcs_reported: bool,
        all_reported_dcs_terminal: bool,
    ) -> bool:
        """A job with targets completes once each reported and all reports
        are terminal; one without, once every report is terminal."""
        return (
            (all_target_dcs_reported and all_reported_dcs_terminal)
            if target_dcs
            else all_reported_dcs_terminal
        )

    async def _warn_of_missing_target_dcs(
        self,
        progress: JobProgress,
        target_dcs: set[str],
        reported_dc_ids: set[str],
        all_target_dcs_reported: bool,
        all_reported_dcs_terminal: bool,
    ) -> None:
        """Warn when every reporting datacenter is terminal but some target
        datacenter never reported: the job waits for its timeout."""
        if self._has_missing_target_dcs(target_dcs, all_target_dcs_reported, all_reported_dcs_terminal):
            missing_dcs = target_dcs - reported_dc_ids
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Job {progress.job_id[:8]}... has {len(missing_dcs)} "
                        f"missing target DCs: {missing_dcs}. Waiting for timeout."
                    ),
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                ),
            )

    @staticmethod
    def _has_missing_target_dcs(
        target_dcs: set[str],
        all_target_dcs_reported: bool,
        all_reported_dcs_terminal: bool,
    ) -> bool:
        """Whether every report is terminal yet some target never reported."""
        return (
            not all_target_dcs_reported
            and all_reported_dcs_terminal
            and bool(target_dcs)
        )

    def _complete_job(self, job: GlobalJobStatus, target_dc_count: int) -> None:
        """Set a completed job's status from its datacenters' outcomes."""
        datacenter_statuses = [p.status for p in job.datacenters]
        completed_count = operator.countOf(datacenter_statuses, JobStatus.COMPLETED.value)
        failed_count = operator.countOf(datacenter_statuses, JobStatus.FAILED.value)
        cancelled_count = operator.countOf(datacenter_statuses, JobStatus.CANCELLED.value)
        timeout_count = operator.countOf(datacenter_statuses, JobStatus.TIMEOUT.value)

        job.status = self._completed_job_status(
            failed_count, cancelled_count, timeout_count, completed_count, target_dc_count
        )

        job.completed_datacenters = completed_count
        job.failed_datacenters = target_dc_count - completed_count

    @staticmethod
    def _completed_job_status(
        failed_count: int,
        cancelled_count: int,
        timeout_count: int,
        completed_count: int,
        target_dc_count: int,
    ) -> str:
        """FAILED, CANCELLED or TIMEOUT when any datacenter ended so (in that
        precedence); COMPLETED when every target completed; else FAILED."""
        unsuccessful_status = GateJobHandler._first_unsuccessful_status(
            failed_count, cancelled_count, timeout_count
        )
        if unsuccessful_status is not None:
            return unsuccessful_status.value
        return JobStatus.COMPLETED.value if completed_count == target_dc_count else JobStatus.FAILED.value

    @staticmethod
    def _first_unsuccessful_status(
        failed_count: int,
        cancelled_count: int,
        timeout_count: int,
    ) -> JobStatus | None:
        """FAILED, CANCELLED or TIMEOUT -- the first, in that precedence, any
        datacenter ended in; None when none did."""
        return next(
            (
                status
                for count, status in (
                    (failed_count, JobStatus.FAILED),
                    (cancelled_count, JobStatus.CANCELLED),
                    (timeout_count, JobStatus.TIMEOUT),
                )
                if count > 0
            ),
            None,
        )

    def _transfer_ack(self, job_id: str, manager_id: str, accepted: bool) -> bytes:
        """The ack for a job leader gate transfer."""
        return JobLeaderGateTransferAck(
            job_id=job_id,
            manager_id=manager_id,
            accepted=accepted,
        ).dump()

    async def handle_job_leader_gate_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
    ) -> bytes:
        try:
            return await self._accept_job_leader_gate_transfer(JobLeaderGateTransfer.load(data))

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Job leader gate transfer error: {error}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )
            return self._transfer_ack("unknown", self._get_node_id().full, False)

    async def _accept_job_leader_gate_transfer(self, transfer: JobLeaderGateTransfer) -> bytes:
        """Take over a job's leadership transferred to this gate, then tell
        its client which gate leads it now."""
        node_id = self._get_node_id()

        if not await self._claim_transferred_job(transfer, node_id):
            return self._transfer_ack(transfer.job_id, node_id.full, False)

        await self._state.increment_state_version()

        await self._logger.log(
            ServerInfo(
                message=(
                    f"Job {transfer.job_id[:8]}... leader gate transferred: "
                    f"{transfer.old_gate_id} -> {transfer.new_gate_id}"
                ),
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=node_id.short,
            ),
        )

        await self._notify_client_of_gate_transfer(transfer, node_id)

        return self._transfer_ack(transfer.job_id, node_id.full, True)

    async def _claim_transferred_job(self, transfer: JobLeaderGateTransfer, node_id: "NodeId") -> bool:
        """Claim a job transferred to this gate under a fence token newer
        than any seen; False when the transfer names another gate or is stale."""
        if transfer.new_gate_id != node_id.full:
            return False

        if not await self._is_transfer_fence_current(transfer, node_id):
            return False

        target_dc_count = len(self._job_manager.get_target_dcs(transfer.job_id))
        return self._job_leadership_tracker.process_leadership_claim(
            job_id=transfer.job_id,
            claimer_id=node_id.full,
            claimer_addr=(self._get_host(), self._get_tcp_port()),
            fencing_token=transfer.fence_token,
            metadata=target_dc_count,
        )

    async def _is_transfer_fence_current(self, transfer: JobLeaderGateTransfer, node_id: "NodeId") -> bool:
        """Whether the transfer's fence token is newer than both the
        leadership tracker's and the job's, raising the job's to it."""
        current_fence = self._job_leadership_tracker.get_fencing_token(
            transfer.job_id
        )
        if transfer.fence_token <= current_fence:
            await self._logger.log(
                ServerDebug(
                    message=(
                        f"Rejecting stale gate transfer for job {transfer.job_id[:8]}... "
                        f"(fence {transfer.fence_token} <= {current_fence})"
                    ),
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=node_id.short,
                ),
            )
            return False

        fence_updated = await self._job_manager.update_fence_token_if_higher(
            transfer.job_id,
            transfer.fence_token,
        )
        if not fence_updated:
            job_fence = self._job_manager.get_fence_token(transfer.job_id)
            await self._logger.log(
                ServerDebug(
                    message=(
                        f"Rejecting gate transfer for job {transfer.job_id[:8]}... "
                        f"(fence {transfer.fence_token} <= {job_fence})"
                    ),
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=node_id.short,
                ),
            )
            return False

        return True

    async def _notify_client_of_gate_transfer(self, transfer: JobLeaderGateTransfer, node_id: "NodeId") -> None:
        """Tell the job's client, if it has a callback, which gate leads it."""
        callback_addr = self._state._progress_callbacks.get(transfer.job_id)
        if callback_addr is None:
            callback_addr = self._job_manager.get_callback(transfer.job_id)

        if callback_addr:
            await self._send_gate_transfer_notification(transfer, node_id, callback_addr)

    async def _send_gate_transfer_notification(
        self,
        transfer: JobLeaderGateTransfer,
        node_id: "NodeId",
        callback_addr: tuple[str, int],
    ) -> None:
        """Push the leader transfer to the client; a failed push is logged."""
        notification = GateJobLeaderTransfer(
            job_id=transfer.job_id,
            new_gate_id=node_id.full,
            new_gate_addr=(self._get_host(), self._get_tcp_port()),
            fence_token=transfer.fence_token,
            old_gate_id=transfer.old_gate_id,
            old_gate_addr=transfer.old_gate_addr,
        )
        try:
            response, _ = await self._send_tcp(
                callback_addr,
                "receive_gate_job_leader_transfer",
                notification.dump(),
                timeout=self._client_push_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response, Exception):
                raise response
        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=(
                        "Failed to notify client about gate leader transfer for job "
                        f"{transfer.job_id[:8]}...: {error}"
                    ),
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=node_id.short,
                )
            )
