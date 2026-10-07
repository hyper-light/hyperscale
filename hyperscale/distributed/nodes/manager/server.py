"""
Manager server composition root.

Thin orchestration layer that wires all manager modules together.
All business logic is delegated to specialized coordinators.
"""

import asyncio
import hashlib
from itertools import chain
import traceback
import cloudpickle
from pathlib import Path
from typing import TYPE_CHECKING, Awaitable, Callable, Iterable, NoReturn

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.cluster import ClusterJoinError, decode_join_message
from hyperscale.distributed.cluster.cluster_membership import ClusterMembership
from hyperscale.distributed.cluster.models import ClusterLeaveReply, ClusterMetricsReply
from hyperscale.distributed.cluster.telemetry_sections import latency_section, slo_section
from hyperscale.distributed.jobs.logical_id_generator import LogicalIdGenerator
from hyperscale.distributed.cluster.joined_peer import JoinedPeer
from hyperscale.distributed.cluster.joined_peer_store import JoinedPeerStore
from hyperscale.distributed.swim import HealthAwareServer, ManagerStateEmbedder
from hyperscale.distributed.swim.core import CircuitState
from hyperscale.distributed.swim.detection import HierarchicalConfig
from hyperscale.distributed.swim.health import CrossClusterAck
from hyperscale.distributed.env import Env
from hyperscale.distributed.server import tcp
from hyperscale.distributed.idempotency import (
    IdempotencyKey,
    IdempotencyLedgerEntry,
    IdempotencyStatus,
    ManagerIdempotencyLedger,
    create_idempotency_config_from_env,
)

from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.reporting.results import Results
from hyperscale.distributed.models import (
    JobStatusQuery,
    ReadConsistency,
    GlobalJobStatus,
    ManagerRegistrationResponse,
    NodeInfo,
    NodeRole,
    ManagerInfo,
    ManagerState as ManagerStateEnum,
    ManagerHeartbeat,
    GateInfo,
    GateHeartbeat,
    GateRegistrationRequest,
    GateRegistrationResponse,
    WorkerRegistration,
    WorkerHeartbeat,
    WorkerState,
    WorkerStateSnapshot,
    RegistrationResponse,
    ManagerPeerRegistration,
    ManagerPeerRegistrationResponse,
    JobSubmission,
    JobAck,
    JobStatus,
    JobFinalResult,
    JobStatusPush,
    WorkflowProgress,
    WorkflowProgressAck,
    WorkflowFinalResult,
    WorkflowFinalResultAck,
    WorkflowResult,
    WorkflowResultPush,
    WorkflowStatus,
    HealthcheckExtensionRequest,
    HealthcheckExtensionResponse,
    WorkerEvictionNotice,
    WorkerEvictionNoticeAck,
    WorkerDiscoveryBroadcast,
    JobLeadershipAnnouncement,
    JobLeadershipAck,
    JobStateSyncMessage,
    JobStateSyncAck,
    JobLeaderGateTransfer,
    JobLeaderGateTransferAck,
    JobLeaderManagerTransfer,
    JobLeaderManagerTransferAck,
    JobLeaderWorkerTransfer,
    JobLeaderWorkerTransferAck,
    JobGlobalTimeout,
    PingRequest,
    ManagerPingResponse,
    WorkerStatus,
    WorkflowQueryRequest,
    WorkflowStatusInfo,
    WorkflowQueryResponse,
    RegisterCallback,
    RegisterCallbackResponse,
    RateLimitResponse,
    TrackingToken,
    DatacenterHealth,
    restricted_loads,
    JobInfo,
    WorkflowInfo,
    SubWorkflowInfo,
    CancelledWorkflowInfo,
)
from hyperscale.distributed.models.sub_workflow_state_snapshot import SubWorkflowStateSnapshot
from hyperscale.distributed.models.workflow_state_snapshot import WorkflowStateSnapshot
from hyperscale.distributed.models.worker_state import (
    WorkerStateUpdate,
    WorkerListResponse,
    WorkflowReassignmentBatch,
)
from hyperscale.distributed.reliability import (
    HybridOverloadDetector,
    OverloadConfig,
    RetryBudgetManager,
    AdaptiveRateLimitConfig,
    ServerRateLimiter,
    StatsBuffer,
    StatsBufferConfig,
    classify_handler_to_priority,
    create_reliability_config_from_env,
)
from hyperscale.distributed.resources import ProcessResourceMonitor, ResourceMetrics
from hyperscale.distributed.health import WorkerHealthManager, WorkerHealthManagerConfig
from hyperscale.distributed.health.progress_witness import ThroughputWitness
from hyperscale.distributed.health.progress_witness.witness_feed_derivation import throughput_witness_config
from hyperscale.distributed.taskex.util.time_parser import TimeParser
from hyperscale.distributed.health.workflow_progress_snapshot import (
    WorkflowProgressSnapshot,
)
from hyperscale.distributed.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    NodeCapabilities,
    ProtocolVersion,
)
from hyperscale.distributed.discovery.security.role_validator import (
    CertificateParseError,
    RoleValidator,
)
from hyperscale.distributed.discovery.security.certificate_claims import CertificateClaims
from hyperscale.distributed.server.protocol.utils import get_peer_certificate_der
from hyperscale.distributed.swim.detection.hierarchical_failure_detector import (
    HierarchicalFailureDetector,
    NodeStatus,
)
from hyperscale.distributed.jobs import (
    JobManager,
    WorkerPool,
    WorkflowDispatcher,
    WindowedStatsCollector,
    WindowedStatsPush,
)
from hyperscale.distributed.jobs.workflow_dependencies import (
    resolve_job_deadline_seconds,
    select_rerun_workflows,
    validate_workflow_dependencies,
)
from hyperscale.distributed.jobs.completion_notice_obligation import (
    CompletionNoticeObligation,
)
from hyperscale.distributed.jobs.job_status_order import JobStatusOrder
from hyperscale.distributed.jobs.job_admission_control import JobAdmissionControl
from hyperscale.distributed.jobs.job_admission_refused_error import JobAdmissionRefusedError
from hyperscale.distributed.ledger.wal import NodeWAL
from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.ledger.job_event_applier import JOB_RELINQUISHED_STATUS
from hyperscale.distributed.ledger.job_state import JobState
from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.ledger.pipeline.commit_pipeline import CommitResult
from hyperscale.distributed.raft import LedgerReplicator
from hyperscale.distributed.slo import SLOConfig
from hyperscale.distributed.resources.resource_budget import ResourceBudget
from hyperscale.distributed.resources.resource_enforcer import ResourceEnforcer
from hyperscale.distributed.resources.resource_violation_type import ResourceViolationType
from hyperscale.distributed.resources.led_workflow_resources import LedWorkflowResources
from hyperscale.distributed.resources.manager_resource_report import ManagerResourceReport
from hyperscale.distributed.resources.datacenter_resource_aggregator import (
    DatacenterResourceAggregator,
)
from hyperscale.distributed.resources.manager_resource_gossip import ManagerResourceGossip
from hyperscale.distributed.resources.manager_resource_gossip_message import (
    ManagerResourceGossipMessage,
)
from hyperscale.distributed.raft.models import LedgerProposal
from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.runtime import Filesystem, RealFilesystem
from hyperscale.distributed.raft.store.raft_storage import RaftStorage
from hyperscale.distributed.raft.store.raft_store import RaftStore
from hyperscale.distributed.swim.core.node_id import NodeId
from hyperscale.distributed.swim.core.node_state import NodeState
from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage

# Module-level storage seam (Phase 7): borrowed, never shut down
# here; swap_defaults rebinds it under SIM.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()
from hyperscale.distributed.health.systemic_failure import is_systemic_failure
from hyperscale.distributed.reliability.load_shedding import LoadShedder
from hyperscale.distributed.resources.workflow_throttle_request import WorkflowThrottleRequest
from hyperscale.distributed.resources.workflow_throttle_response import WorkflowThrottleResponse
from hyperscale.distributed.hlc import (
    ClockFenceVerdict,
    ClockOffsetMonitor,
    ClockOffsetProber,
    HybridLogicalClock,
    hlc_node_id,
    tcp_probe_exchange,
)
from hyperscale.distributed.hlc.models import ClockOffsetProbeReply
from hyperscale.distributed.jobs.timeout_strategy import (
    TimeoutStrategy,
    LocalAuthorityTimeout,
    GateCoordinatedTimeout,
)
from hyperscale.distributed.workflow import (
    WORKFLOW_STATUS_BY_WORKFLOW_STATE,
    StateTransition,
    WorkflowLifecycleRecord,
    WorkflowLifecycleStateMachine,
    WorkflowState,
)
from hyperscale.logging.hyperscale_logging_models import (
    SystemicEvictionHeld,
    SystemicEvictionReleased,
    ServerInfo,
    ServerWarning,
    ServerError,
    ServerDebug,
)

from .config import create_manager_config_from_env
from .state import ManagerState
from .models import WorkerEvictionNoticeState
from .registry import ManagerRegistry
from .dispatch import ManagerDispatchCoordinator
from .cancellation import ManagerCancellationCoordinator
from .leases import ManagerLeaseCoordinator
from .health import ManagerHealthMonitor
from .sync import ManagerStateSync
from .leadership import ManagerLeadershipCoordinator
from .version_skew import ManagerVersionSkewHandler
from .capacity_reporter import ManagerCapacityReporter
from .raft_integration import ManagerRaftIntegration
from .stats import ManagerStatsCoordinator

from .worker_dissemination import WorkerDisseminator
from hyperscale.distributed.swim.gossip.worker_state_gossip_buffer import (
    WorkerStateGossipBuffer,
)
from hyperscale.distributed.swim.gossip.extension_decision_gossip_buffer import (
    ExtensionDecisionGossipBuffer,
)
from hyperscale.distributed.swim.gossip.extension_outcome_gossip_buffer import (
    ExtensionOutcomeGossipBuffer,
)
from hyperscale.distributed.health.extension_ledger import (
    ExtensionDecisionEvent,
    ExtensionLedger,
)
from hyperscale.distributed.health.extension_outcome import (
    ExtensionOutcomeEvent,
    ExtensionOutcomeKind,
)
from .models.manager_config import ManagerConfig


if TYPE_CHECKING:
    from hyperscale.distributed.runtime import Clock, Random, TransportFactory

_TERMINAL_WORKFLOW_STATUS_VALUES = frozenset(
    status.value
    for status in (
        WorkflowStatus.COMPLETED,
        WorkflowStatus.FAILED,
        WorkflowStatus.CANCELLED,
        WorkflowStatus.AGGREGATED,
        WorkflowStatus.AGGREGATION_FAILED,
    )
)
_BYTES_PER_MEGABYTE = 1024 * 1024


# Ledger-internal job statuses in the client's vocabulary.
_LEDGER_STATUS_VOCABULARY: dict[str, str] = {
    "pending": JobStatus.SUBMITTED.value,
    "cancelling": JobStatus.RUNNING.value,
}


class ManagerServer(HealthAwareServer):
    """
    Manager node composition root.

    Orchestrates workflow execution within a datacenter by:
    - Receiving jobs from gates (or directly from clients)
    - Dispatching workflows to workers
    - Aggregating status updates from workers
    - Reporting to gates (if present)
    - Participating in leader election among managers
    """

    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "default",
        gate_addrs: list[tuple[str, int]] | None = None,
        gate_udp_addrs: list[tuple[str, int]] | None = None,
        seed_managers: list[tuple[str, int]] | None = None,
        manager_peers: list[tuple[str, int]] | None = None,
        manager_udp_peers: list[tuple[str, int]] | None = None,
        quorum_timeout: float = 5.0,
        workflow_timeout: float = 300.0,
        wal_data_dir: Path | None = None,
        incarnation_storage_dir: str | None = None,
        *,
        clock: "Clock | None" = None,
        random_source: "Random | None" = None,
        transport_factory: "TransportFactory | None" = None,
        raft_store: RaftStore | None = None,
    ) -> None:
        """
        Initialize manager server.

        Args:
            host: Host address to bind
            tcp_port: TCP port for data operations
            udp_port: UDP port for SWIM healthchecks
            env: Environment configuration
            dc_id: Datacenter identifier
            gate_addrs: Optional gate TCP addresses for upstream communication
            gate_udp_addrs: Optional gate UDP addresses for SWIM
            seed_managers: Initial manager TCP addresses for peer discovery
            manager_peers: Deprecated alias for seed_managers
            manager_udp_peers: Manager UDP addresses for SWIM cluster
            quorum_timeout: Timeout for quorum operations
            workflow_timeout: Workflow execution timeout in seconds
        """

        self._config: ManagerConfig = create_manager_config_from_env(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            datacenter_id=dc_id,
            seed_gates=gate_addrs,
            gate_udp_addrs=gate_udp_addrs,
            seed_managers=seed_managers or manager_peers,
            manager_udp_peers=manager_udp_peers,
            quorum_timeout=quorum_timeout,
            workflow_timeout=workflow_timeout,
            wal_data_dir=wal_data_dir,
        )

        self._node_wal: NodeWAL | None = None
        self._job_ledger: JobLedger | None = None
        # One storage-health tracker for every durable store this manager
        # keeps (job ledger WAL + checkpoints, idempotency ledger,
        # persisted submissions): they share a device.
        self._storage_health = StorageHealth()
        # Storage seam for submission-payload persistence (borrowed,
        # never shut down here; swap_defaults rebinds it under SIM).
        self._storage_filesystem: Filesystem = _DEFAULT_FILESYSTEM
        # Gates this manager was joined to at runtime, kept in its data
        # directory (created at start, once the logger exists).
        self._joined_peer_store: JoinedPeerStore | None = None
        # worker_id -> next monotonic instant an unknown-worker
        # re-register nudge may be sent (rate limit).
        self._unknown_worker_nudges: dict[str, float] = {}
        # job_id -> owed completion notice (self-contained serialized
        # payload; resent on the reap-loop cadence until the origin
        # gate acks — see CompletionNoticeObligation).
        self._completion_notice_obligations: dict[
            str, CompletionNoticeObligation
        ] = {}
        # Job ids whose submission a request is deciding right now: a job
        # id's submission is decided by one request at a time.
        self._job_submissions_in_progress: set[str] = set()

        self._env: Env = env
        self._seed_gates: list[tuple[str, int]] = self._addresses_or_empty(gate_addrs)
        self._gate_udp_addrs: list[tuple[str, int]] = self._addresses_or_empty(gate_udp_addrs)
        self._seed_managers: list[tuple[str, int]] = self._addresses_or_empty(
            seed_managers or manager_peers
        )
        self._manager_udp_peers: list[tuple[str, int]] = self._addresses_or_empty(manager_udp_peers)
        self._workflow_timeout: float = workflow_timeout

        self._manager_state: ManagerState = ManagerState(
            slo_config=SLOConfig.from_env(env),
        )
        self._idempotency_config = create_idempotency_config_from_env(env)
        self._idempotency_ledger: ManagerIdempotencyLedger[bytes] | None = None

        # Initialize parent HealthAwareServer. The Phase 5/6 DI seams
        # (``clock`` / ``random_source`` / ``transport_factory``) are
        # forwarded through so SIM mode can inject a ``VirtualClock`` /
        # ``SeededRandom`` / ``SimTransportFactory``; all default to
        # ``None`` (REAL) and are keyword-only.
        # Incarnation persistence rides the node's durable directory
        # unless given its own: a restarted manager must rejoin ABOVE
        # its previous incarnation or peers zombie-reject it.
        incarnation_storage_dir = self._incarnation_storage_dir_for(
            incarnation_storage_dir, wal_data_dir
        )

        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
            node_role="manager",
            clock=clock,
            random_source=random_source,
            transport_factory=transport_factory,
            incarnation_storage_dir=incarnation_storage_dir,
            # D1: an identity the Raft store resumed keeps its start time,
            # so this node is the member its groups knew.
            node_created_ms=self._resumed_node_created_ms(raft_store),
        )
        # Where every Raft group of this node keeps its persistent state
        # (D1): the opened store the node was given, or none -- each start
        # then a new member of every group. Borrowed: its opener closes it.
        self._raft_storage: RaftStorage = self._raft_storage_for(raft_store)

        # Wire logger to modules
        self._init_modules()

        # Initialize address mappings for SWIM callbacks
        self._init_address_mappings()

        # Register callbacks
        self._register_callbacks()

    def _addresses_or_empty(
        self,
        addresses: list[tuple[str, int]] | None,
    ) -> list[tuple[str, int]]:
        """The configured addresses, or a fresh empty list when none were given."""
        return addresses or []

    def _incarnation_storage_dir_for(
        self,
        incarnation_storage_dir: str | None,
        wal_data_dir: Path | None,
    ) -> str | None:
        """The incarnation directory: the one given, else the WAL directory's
        ``incarnation`` (a restarted manager rejoins above its incarnation)."""
        if incarnation_storage_dir is None and wal_data_dir is not None:
            incarnation_storage_dir = str(wal_data_dir / "incarnation")
        return incarnation_storage_dir

    def _resumed_node_created_ms(self, raft_store: RaftStore | None) -> int | None:
        """D1: the start time of the identity the Raft store resumed, if any."""
        return None if raft_store is None else NodeId.created_ms_of(raft_store.identity.node_id_full)

    def _raft_storage_for(self, raft_store: RaftStore | None) -> RaftStorage:
        """D1: the opened Raft store the node was given, else volatile storage."""
        return raft_store if raft_store is not None else VolatileRaftStorage()

    def _leader_lease_drift_bound(self) -> float | None:
        """The Raft clock drift bound when leader leases are enabled, else None."""
        return self._env.RAFT_CLOCK_DRIFT_BOUND if self._env.RAFT_LEADER_LEASES_ENABLED else None

    def _seed_manager_clock_probe_peers(self) -> dict[str, tuple[str, int]]:
        """AD-39's probe peers: each configured seed manager, keyed "host:port"."""
        return {
            f"{peer_host}:{peer_port}": (peer_host, peer_port)
            for peer_host, peer_port in self._seed_managers
        }

    def _init_modules(self) -> None:
        """Initialize all modular coordinators."""
        # Registry for workers, gates, peers
        self._registry = ManagerRegistry(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            # AD-26 extension tracking (trackers, ledger, throughput
            # streams) leaves with the worker.
            on_worker_unregistered=lambda worker_id: self._worker_health_manager.on_worker_removed(worker_id),
        )

        # Lease coordinator for fencing tokens and job leadership
        # NOTE: passes ``self._node_id.full`` because the leader's
        # claim path stores this string in
        # ``_manager_state._job_leaders``; the leadership-announcement
        # path on the receiving manager stores
        # ``announcement.leader_id`` which is also the full id (set
        # via ``leader_id=self._node_id.full`` at the announcement
        # construction site). Using ``.short`` here would cause the
        # leader's self-stored value to disagree with what every
        # follower stores, tripping the AtMostOneJobLeaderPerJob
        # safety invariant on every job submit.
        self._leases = ManagerLeaseCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.full,
            task_runner=self._task_runner,
        )

        # Health monitor for worker health tracking
        self._worker_health_monitor = ManagerHealthMonitor(
            state=self._manager_state,
            config=self._config,
            registry=self._registry,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Dispatch coordinator for workflow dispatch
        # JobManager must exist before any coordinator that takes it as a
        # dependency (e.g. cancellation below). Constructed here so the rest
        # of the init sequence can reference it.
        # JobManager and WorkflowDispatcher must share the same manager_id
        # because TrackingToken keys built on one side ("manager creates the
        # workflow token, dispatches to worker, worker echoes it back in
        # WorkflowFinalResult") have to match what the other side indexed.
        # Use the full NodeId everywhere — it's the globally-unique form
        # that's safe to embed in network messages and stable across the
        # node's lifetime. ``self._node_id.short`` is for human-readable
        # logging, not for keys.
        self._job_manager = JobManager(
            datacenter=self._node_id.datacenter,
            manager_id=self._node_id.full,
            clock=self._clock,
            max_budgeted_retries=self.env.RETRY_BUDGET_PER_WORKFLOW_MAX,
        )
        # Every job's AD-44 retry budget: a failed dispatch and a lost
        # worker charge the same per-workflow budget.
        self._retry_budget_manager = RetryBudgetManager(
            config=create_reliability_config_from_env(self.env),
            logger=self._udp_logger,
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
        )
        # A workflow moving along its lifecycle is its job's progress (AD-34).
        self._job_manager.workflow_lifecycle.register_observer(
            self._report_lifecycle_progress
        )

        # Worker and peer-manager state sync (state_sync_request delegates)
        self._state_sync = ManagerStateSync(
            state=self._manager_state,
            config=self._config,
            registry=self._registry,
            leases=self._leases,
            job_manager=self._job_manager,
            logger=self._udp_logger,
            node_id=self._node_id,
            node_host=self._host,
            node_port=self._tcp_port,
            task_runner=self._task_runner,
            send_tcp=self.send_tcp,
            is_cluster_leader=self.is_leader,
            get_current_term=lambda: self._leader_election.state.current_term,
            build_job_state_sync_message=self._build_job_state_sync_message,
            apply_job_state_sync_message=self._apply_job_state_sync_message,
            get_job_callback_addr=self._get_job_callback_addr,
            validate_mtls_claims=self._validate_mtls_claims,
        )

        # Cancellation coordinator for AD-20
        # State sync coordinator
        # AD-25 protocol version negotiation
        self._version_skew = ManagerVersionSkewHandler(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Leadership coordinator
        self._leadership = ManagerLeadershipCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            is_leader_fn=self.is_leader,
            step_down_fn=self._step_down_from_cluster_leadership,
            cohort_size_fn=lambda: len(self._cluster_membership.cohort),
        )

        # Load shedding (AD-22), at this node's OVERLOAD_* settings (the
        # AD-24 rate-limit window is derived from them)
        self._overload_detector = HybridOverloadDetector(OverloadConfig.from_env(self._env))
        self._resource_monitor = ProcessResourceMonitor()
        self._last_resource_metrics: "ResourceMetrics | None" = None
        self._manager_health_state: str = "healthy"
        self._manager_health_state_snapshot: str = "healthy"
        self._previous_manager_health_state: str = "healthy"
        self._manager_health_state_lock: asyncio.Lock = asyncio.Lock()
        self._workflow_reassignment_lock: asyncio.Lock = asyncio.Lock()
        # AD-19: true while deadline evictions are held as systemic.
        self._systemic_eviction_hold = False
        # AD-22 load shedding over the overload detector the resource
        # sampler feeds -- the same shedder the gate runs.
        self._load_shedder = LoadShedder(self._overload_detector, detector_sampled_externally=True)

        # Shared hybrid logical clock (AD-38/AD-39). Used by Raft so that
        # RaftLogEntry.timestamp is a replicated, wall-clock-derived value -- this
        # is what state-machine apply handlers read for any time-related state
        # so all followers converge byte-equal. The same clock instance is
        # reused for the WAL on start() so HLC ordering is coherent across
        # subsystems. Its node id derives from the restart-stable node id.
        self._hlc = HybridLogicalClock(
            node_id=hlc_node_id(self._node_id.full),
            clock=self._clock,
            max_offset_ms=self._env.HLC_MAX_CLOCK_OFFSET_MS,
        )
        # AD-39: fenced while this manager's clock disagrees with a quorum
        # of the cluster (or its HLC ran past its own clock). A fenced
        # manager neither leads (Raft or SWIM) nor accepts jobs.
        self._clock_offset_monitor = ClockOffsetMonitor(
            hlc=self._hlc,
            cluster_size=lambda: len(self._cluster_membership.cohort),
            sample_ttl_seconds=self._env.HLC_OFFSET_SAMPLE_TTL_SECONDS,
            clock=self._clock,
        )
        self._leadership_refusals.append(self._is_clock_fenced)

        # AD-38 REGIONAL: every member's copy of the ledger events its
        # job groups commit; a takeover adopts a job's history from here.
        # D-67: every terminal this member's job groups commit feeds the
        # noisy-job breaker, so a follower that becomes the datacenter's
        # leader holds the quarantines its predecessor opened.
        self._ledger_replica = JobLedgerReplica(on_terminal_event=self._on_replicated_job_terminal)
        # AD-52 slice C: the datacenter's managers keep their membership in
        # one Raft group -- the configured cohort founds it, and every job
        # group takes its members from it.
        self._cluster_membership = ClusterMembership(
            self._node_id.full,
            (self._host, self._tcp_port),
            frozenset(self._seed_managers),
            send_request=self._send_to_peer,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            hlc=self._hlc,
            clock=self._clock,
            may_lead=self._may_lead,
            cluster_uuids=LogicalIdGenerator(scope=self._node_id.full, clock=self._clock),
            formation_interval_seconds=self._env.CLUSTER_FORMATION_INTERVAL_SECONDS,
            tombstone_retention_seconds=self._env.CLUSTER_TOMBSTONE_RETENTION_SECONDS,
            request_timeout_seconds=self._config.tcp_timeout_standard_seconds,
            watch_wait_ceiling_seconds=self._env.CLUSTER_WATCH_WAIT_SECONDS,
            on_cohort_change=self._on_cohort_change,
            snapshot_entries=self._env.CLUSTER_SNAPSHOT_ENTRIES,
            snapshot_catch_up_entries=self._env.CLUSTER_SNAPSHOT_CATCH_UP_ENTRIES,
            leader_lease_drift_bound=self._leader_lease_drift_bound(),
            storage=self._raft_storage,
        )
        self._raft = ManagerRaftIntegration(
            node_id=self._node_id.full,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            send_tcp=self._send_to_peer,
            configured_cluster_size=len(self._cluster_membership.cohort),
            proposal_timeout_seconds=self._config.quorum_timeout_seconds,
            on_job_raft_leader=self._on_job_raft_leader,
            on_job_raft_lose_leader=self._on_job_raft_lose_leader,
            clock=self._hlc,
            may_lead=self._may_lead,
            ledger_replica=self._ledger_replica,
            request_timeout_seconds=self._config.tcp_timeout_standard_seconds,
            cluster_members=self._cluster_membership.node_addresses,
            storage=self._raft_storage,
        )
        # AD-39 measures the configured cohort, one entry per address, from
        # the start: before the cluster's membership forms, and never
        # counting one address twice under two processes' ids.
        self._clock_probe_peers = self._seed_manager_clock_probe_peers()
        self._clock_offset_prober = ClockOffsetProber(
            node_id=self._node_id.full,
            hlc=self._hlc,
            monitor=self._clock_offset_monitor,
            peers=lambda: self._clock_probe_peers,
            exchange=tcp_probe_exchange(self.send_tcp),
            clock=self._clock,
            probe_interval_seconds=self._env.HLC_OFFSET_PROBE_INTERVAL_SECONDS,
            logger=self._udp_logger,
            on_fence_change=self._on_clock_fence_change,
        )
        # A forwarded proposal waits out the group leader's proposal
        # timeout, plus one short transit for the request and its reply.
        self._ledger_replicator = LedgerReplicator(
            consensus=self._raft.consensus,
            node_id=self._node_id.full,
            send_tcp=self._send_to_peer,
            forward_method="raft_ledger_proposal",
            forward_timeout_seconds=(
                self._config.quorum_timeout_seconds
                + self._config.tcp_timeout_short_seconds
            ),
            clock=self._clock,
            logger=self._udp_logger,
        )
        # AD-41: per-workflow resource budgets, judged on each progress
        # report the job leader receives (None when guards are disabled).
        self._resource_enforcer: ResourceEnforcer | None = (
            ResourceEnforcer(
                clock=self._clock,
                default_budget=ResourceBudget.from_env(self._env),
                on_warn=self._warn_resource_violation,
                on_throttle_workflow=self._throttle_workflow_for_resources,
                on_release_throttle=self._release_workflow_throttle,
                on_kill_workflow=self._kill_workflow_for_resources,
                on_evict_worker=self._evict_worker_for_resources,
            )
            if self._env.RESOURCE_GUARD_ENABLED
            else None
        )
        # AD-41: resource use of the workflows this manager leads, reported
        # to gates for datacenter pressure.
        self._led_workflow_resources = LedWorkflowResources(
            clock=self._clock,
            staleness_seconds=self._env.RESOURCE_VIEW_STALENESS_SECONDS,
            leads_job=self._is_job_leader,
        )
        # AD-41 Part 4: the datacenter's resource view on the manager tier,
        # by gossip among its managers -- what a gate sees, for clients that
        # run jobs on this datacenter without one.
        self._resource_gossip = ManagerResourceGossip(
            datacenter=self._node_id.datacenter,
            own_address=(self._host, self._tcp_port),
            aggregator=DatacenterResourceAggregator(
                self._clock, self._env.RESOURCE_VIEW_STALENESS_SECONDS
            ),
        )

        self._worker_pool = WorkerPool(
            health_grace_period=30.0,
            get_swim_status=self._get_swim_status_for_worker,
            manager_id=self._node_id.short,
            datacenter=self._node_id.datacenter,
            dispatch_failure_base_cooldown_seconds=(
                self._config.dispatch_routing_failure_base_cooldown_seconds
            ),
            dispatch_failure_max_cooldown_seconds=(
                self._config.dispatch_routing_failure_max_cooldown_seconds
            ),
            dispatch_readiness_cooldown_seconds=(
                self._config.dispatch_routing_readiness_cooldown_seconds
            ),
        )

        self._registry.set_worker_pool(self._worker_pool)

        # Rate limiting (AD-24)
        # Health-gated (AD-24): limits tighten with the overload state the
        # node's resource sampler settles on.
        self._rate_limiter = ServerRateLimiter(
            adaptive_config=AdaptiveRateLimitConfig.from_env(self._env, self._tcp_server_state.max_connections),
            overload_detector=self._overload_detector,
            detector_sampled_externally=True,
        )

        # AD-20 cancellation protocol (the server's cancel handlers delegate)
        self._cancellation = ManagerCancellationCoordinator(
            state=self._manager_state,
            config=self._config,
            env=self._env,
            logger=self._udp_logger,
            node_id=self._node_id,
            node_host=self._host,
            node_port=self._tcp_port,
            task_runner=self._task_runner,
            clock=self._clock,
            job_manager=self._job_manager,
            leases=self._leases,
            rate_limiter=self._rate_limiter,
            get_job_ledger=lambda: self._job_ledger,
            get_workflow_dispatcher=lambda: self._workflow_dispatcher,
            is_cluster_leader=self.is_leader,
            send_tcp=self.send_tcp,
            send_to_worker=self._send_to_worker,
            send_to_client=self._send_to_client,
            check_rate_limit_for_operation=self._check_rate_limit_for_operation,
            take_over_job_leadership_as_cluster_leader=self._take_over_job_leadership_as_cluster_leader,
            resolve_dc_leader_addr=self._resolve_dc_leader_addr,
            manager_tcp_addr_is_live=self._manager_tcp_addr_is_live,
            emit_outcomes_for_terminal_job=self._emit_outcomes_for_terminal_job,
            discard_persisted_submission=self._discard_persisted_submission,
            log_ledger_shortfall=self._log_ledger_shortfall,
            complete_job_if_done=self._complete_job_if_done,
        )

        # Stats buffer (AD-23)
        self._stats_buffer = StatsBuffer(
            StatsBufferConfig(
                hot_max_entries=self._config.stats_hot_max_entries,
                throttle_threshold=self._config.stats_throttle_threshold,
                batch_threshold=self._config.stats_batch_threshold,
                reject_threshold=self._config.stats_reject_threshold,
            )
        )

        # Windowed stats collector
        self._windowed_stats = WindowedStatsCollector(
            window_size_ms=self._config.stats_window_size_ms,
            drift_tolerance_ms=self._config.stats_drift_tolerance_ms,
            max_window_age_ms=self._config.stats_max_window_age_ms,
        )

        # Stats coordinator
        self._stats = ManagerStatsCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            stats_buffer=self._stats_buffer,
            windowed_stats=self._windowed_stats,
            clock=self._clock,
            get_healthy_worker_count=lambda: len(self._registry.get_healthy_worker_ids()),
            send_to_callback=self._send_to_client,
        )
        # Sends each workflow dispatch the WorkflowDispatcher decides on
        self._dispatch = ManagerDispatchCoordinator(
            registry=self._registry,
            worker_pool=self._worker_pool,
            stats=self._stats,
            send_tcp=self.send_tcp,
            logger=self._udp_logger,
            node_host=self._host,
            node_port=self._tcp_port,
            node_id=self._node_id.short,
            dispatch_timeout_seconds=self._config.tcp_timeout_standard_seconds,
            clock=self._clock,
            record_dispatch_latency=self._manager_state.record_dispatch_latency,
        )

        # Worker health manager (AD-26), its extension policy from Env.
        # The H6 throughput witness makes the H5 multi-witness decision
        # live, its false-deny rate held to the configured budget.
        # The witness samples each in-flight workflow's progress rate at
        # the interval its K-S confirmation needs at the α floor (8
        # samples per 5 s trigger poll at floor 1e-5: 0.625 s) and models
        # one base extension deadline as its fresh-run window (30 s /
        # 0.625 s / 0.25 = 192 run lengths). Measured cost ~208 us per
        # update -> ~0.33 ms per in-flight workflow per second; taking all
        # 20 progress reports/s at the old 1000 cap cost 18.5 ms. The
        # arithmetic is in progress_witness/witness_feed_derivation.py.
        self._worker_health_manager = WorkerHealthManager(
            WorkerHealthManagerConfig.from_env(self.env),
            throughput_witness=ThroughputWitness(
                throughput_witness_config(
                    self.env.HYPERSCALE_EXTENSION_FPR_BUDGET,
                    TimeParser(self.env.HYPERSCALE_EXTENSION_TRIGGER_INTERVAL).time,
                    self.env.EXTENSION_BASE_DEADLINE,
                ),
                clock=self._clock,
            ),
            clock=self._clock,
        )

        # WorkflowDispatcher (initialized in start())
        self._workflow_dispatcher: WorkflowDispatcher | None = None

        # AD-43 heartbeat capacity inputs (pending backlog + active
        # remaining work); the dispatcher is read through a getter because
        # it is created in start().
        self._capacity_reporter = ManagerCapacityReporter(
            job_manager=self._job_manager,
            get_workflow_dispatcher=lambda: self._workflow_dispatcher,
            get_total_cores=self._get_total_cores,
            node_id=self._node_id.full,
            clock=self._clock,
        )

        # D-65 concurrency caps and the D-67 noisy-job breaker, applied by
        # this manager to each job it admits as the datacenter's leader --
        # the one admission point gate-routed and gateless jobs share.
        self._job_admission_control = JobAdmissionControl(
            env=self.env,
            job_manager=self._job_manager,
            is_submission_in_progress=self._job_submissions_in_progress.__contains__,
            get_registered_cores=self._get_total_cores,
            clock=self._clock,
            logger=self._udp_logger,
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
        )

        # WorkerDisseminator (AD-48, initialized in start())
        self._worker_disseminator: "WorkerDisseminator | None" = None

        # AD-26 H7b: extension decision dissemination. Always
        # constructed (not deferred to start()) so any decision the
        # local ``WorkerHealthManager`` produces during early
        # registration is captured and disseminated as soon as the
        # SWIM piggyback channels open.
        self._extension_decision_buffer: ExtensionDecisionGossipBuffer = (
            ExtensionDecisionGossipBuffer()
        )

        # AD-26 H8b: extension outcome dissemination. Symmetric to
        # the H7b decision buffer, ferrying workflow-termination
        # outcomes (the H8 Bayesian-tuner training signal) across
        # the cluster.
        self._extension_outcome_buffer: ExtensionOutcomeGossipBuffer = (
            ExtensionOutcomeGossipBuffer()
        )

        # Recovery semaphore
        self._recovery_semaphore = asyncio.Semaphore(
            self._config.recovery_max_concurrent
        )

        # Role validator for mTLS
        self._role_validator = RoleValidator(
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
            strict_mode=self._config.mtls_strict_mode,
        )

        # Background tasks
        self._dead_node_reap_task: asyncio.Task | None = None
        self._orphan_scan_task: asyncio.Task | None = None
        self._job_responsiveness_task: asyncio.Task | None = None
        self._stats_push_task: asyncio.Task | None = None
        self._windowed_stats_flush_task: asyncio.Task | None = None
        self._gate_heartbeat_task: asyncio.Task | None = None
        self._rate_limit_cleanup_task: asyncio.Task | None = None
        self._job_cleanup_task: asyncio.Task | None = None
        self._unified_timeout_task: asyncio.Task | None = None
        self._deadline_enforcement_task: asyncio.Task | None = None
        self._manager_peer_registration_sync_task: asyncio.Task | None = None
        self._peer_job_state_sync_task: asyncio.Task | None = None
        self._resource_sample_task: asyncio.Task | None = None
        self._resource_gossip_task: asyncio.Task | None = None

    def _init_address_mappings(self) -> None:
        """Initialize UDP to TCP address mappings."""
        # Gate UDP to TCP mapping
        for tcp_addr, udp_addr in zip(self._seed_gates, self._gate_udp_addrs):
            self._manager_state.set_gate_udp_to_tcp_mapping(udp_addr, tcp_addr)

        # Manager UDP to TCP mapping
        for tcp_addr, udp_addr in zip(self._seed_managers, self._manager_udp_peers):
            self._manager_state.set_manager_udp_to_tcp_mapping(udp_addr, tcp_addr)

    def _register_callbacks(self) -> None:
        """Register SWIM and leadership callbacks."""
        self.register_on_become_leader(self._on_manager_become_leader)
        self.register_on_lose_leadership(self._on_manager_lose_leadership)
        self.register_on_node_dead(self._on_node_dead)
        self.register_on_node_join(self._on_node_join)
        self.register_on_peer_confirmed(self._on_peer_confirmed)

        # Initialize hierarchical failure detector (AD-30). Keep the
        # global-layer Lifeguard bracket on its tuned defaults
        # (5 s / 30 s); the previously hard-coded 10 s / 60 s manager
        # override was a magic number that — once the bounded prob-OR
        # LHM extension kicks in — pushed the worst-case suspicion
        # bracket past 45 s (the operator-budgeted detection envelope
        # for worker liveness). The job-layer override stays: jobs
        # need tighter detection than full SWIM probes because their
        # liveness check is workflow-local. ``on_global_death`` is no
        # longer overridable (enforced by ``init_hierarchical_detector``);
        # post-DEAD work runs through ``register_on_node_dead`` above.
        self.init_hierarchical_detector(
            config=HierarchicalConfig(
                global_min_timeout=float(self._env.SWIM_SUSPICION_MIN_TIMEOUT),
                global_max_timeout=float(self._env.SWIM_SUSPICION_MAX_TIMEOUT),
                global_no_witness_timeout=float(
                    self._env.SWIM_NO_WITNESS_SUSPICION_TIMEOUT
                ),
                job_min_timeout=float(self._env.MANAGER_SWIM_JOB_MIN_TIMEOUT),
                job_max_timeout=float(self._env.MANAGER_SWIM_JOB_MAX_TIMEOUT),
            ),
            on_job_death=self._on_worker_dead_for_job,
            get_job_n_members=self._get_job_worker_count,
        )

        # Set state embedder
        self.set_state_embedder(self._create_state_embedder())

    def _create_state_embedder(self) -> ManagerStateEmbedder:
        """Create state embedder for SWIM heartbeat embedding."""
        return ManagerStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_datacenter=lambda: self._node_id.datacenter,
            is_leader=self.is_leader,
            get_term=lambda: self._leader_election.state.current_term,
            get_state_version=lambda: self._manager_state.state_version,
            get_active_jobs=lambda: self._job_manager.job_count,
            get_active_workflows=self._get_active_workflow_count,
            get_worker_count=self._manager_state.get_worker_count,
            get_healthy_worker_count=lambda: len(
                self._registry.get_healthy_worker_ids()
            ),
            get_available_cores=self._get_available_cores_for_healthy_workers,
            get_total_cores=self._get_total_cores,
            on_worker_heartbeat=self._handle_embedded_worker_heartbeat,
            on_manager_heartbeat=self._handle_manager_peer_heartbeat,
            on_gate_heartbeat=self._handle_gate_heartbeat,
            get_manager_state=lambda: self._manager_state.manager_state_enum.value,
            get_tcp_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_udp_host=lambda: self._host,
            get_udp_port=lambda: self._udp_port,
            get_health_accepting_jobs=self._is_accepting_jobs,
            get_health_has_quorum=self._leadership.has_quorum,
            get_health_throughput=self._stats.get_dispatch_throughput,
            get_health_expected_throughput=self._stats.get_expected_throughput,
            get_health_overload_state=self._get_manager_health_state_snapshot,
            get_current_gate_leader_id=lambda: self._manager_state.current_gate_leader_id,
            get_current_gate_leader_host=lambda: (
                self._manager_state.current_gate_leader_addr[0]
                if self._manager_state.current_gate_leader_addr
                else None
            ),
            get_current_gate_leader_port=lambda: (
                self._manager_state.current_gate_leader_addr[1]
                if self._manager_state.current_gate_leader_addr
                else None
            ),
            get_known_gates=self._get_known_gates_for_heartbeat,
            get_job_leaderships=self._get_job_leaderships_for_heartbeat,
            get_storage_writable=self._is_storage_writable,
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def node_info(self) -> NodeInfo:
        """Get this manager's node info."""
        return NodeInfo(
            node_id=self._node_id.full,
            role=NodeRole.MANAGER.value,
            host=self._host,
            port=self._tcp_port,
            datacenter=self._node_id.datacenter,
            version=self._manager_state.state_version,
            udp_port=self._udp_port,
        )

    def _get_election_member_count(self) -> int:
        """Configured manager cluster size for SWIM-tier leader election.

        Overrides ``HealthAwareServer._get_election_member_count``,
        which counts the dynamically-discovered ``_peer_roles`` (or
        falls back to the incarnation tracker). Both of those are
        runtime views and shrink to the local node alone immediately
        after a restart, before peer registrations have re-converged.
        Using them feeds a quorum of ``1`` into ``_run_election`` /
        ``_run_pre_vote`` (``(1 // 2) + 1 == 1``); a freshly-restarted
        manager whose peers haven't yet registered with it will see
        itself as the entire cluster and grant itself leadership —
        precisely the split-brain scenario AD-3 forbids.

        For the manager tier that is the cohort: the managers this one was
        configured with plus itself, until a committed resize changes it
        (AD-52) -- never the managers seen so far, which a restart resets,
        whatever peer registrations have landed.
        """
        return len(self._cluster_membership.cohort)

    def _is_election_cohort_voter(self, voter_udp_address: tuple[str, int]) -> bool:
        """Only the cohort the majority is counted over votes (Raft section
        5.2): a manager outside it -- removed, or never in it -- cannot
        carry a minority of the cohort to a majority."""
        return self._manager_state.get_manager_tcp_from_udp(voter_udp_address) in self._cluster_membership.cohort

    def _on_cohort_change(self, cohort: frozenset[tuple[str, int]]) -> None:
        """A committed resize changed the datacenter's manager cohort (AD-52
        ``ResizeCluster``): job groups count its majority, and clock offsets
        are measured to its members. The leadership quorum, election size
        and clock fence read the cohort as they count."""
        self._raft.set_cohort_size(len(cohort))
        self._clock_probe_peers = {
            f"{peer_host}:{peer_port}": (peer_host, peer_port)
            for peer_host, peer_port in cohort
            if (peer_host, peer_port) != (self._host, self._tcp_port)
        }
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager cohort resized to {sorted(cohort)}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _is_clock_fenced(self) -> bool:
        return self._clock_offset_monitor.is_fenced

    def _may_lead(self) -> bool:
        return not self._clock_offset_monitor.is_fenced

    def _on_replicated_job_terminal(self, event_type: JobEventType, payload: bytes, job_state: JobState) -> None:
        """Mirror a committed job terminal into the D-67 noisy-job breaker."""
        self._job_admission_control.record_replicated_job_outcome(event_type, payload, job_state.created_hlc)

    def _is_accepting_jobs(self) -> bool:
        return (
            self._manager_state.manager_state_enum == ManagerStateEnum.ACTIVE
            and not self._clock_offset_monitor.is_fenced
        )

    async def _on_clock_fence_change(self, verdict: ClockFenceVerdict) -> None:
        """A fenced manager gives up datacenter leadership (re-election is
        refused while fenced); Raft groups relinquish on their next tick."""
        if verdict.fenced and self.is_leader():
            self._task_runner.run(self._leader_election._step_down)

    def _get_manager_health_state_snapshot(self) -> str:
        return self._manager_health_state_snapshot

    async def _get_manager_health_state(self) -> str:
        async with self._manager_health_state_lock:
            return self._manager_health_state

    async def _set_manager_health_state(self, new_state: str) -> tuple[str, str, bool]:
        async with self._manager_health_state_lock:
            if new_state == self._manager_health_state:
                return self._manager_health_state, new_state, False

            previous_state = self._manager_health_state
            self._previous_manager_health_state = previous_state
            self._manager_health_state = new_state
            self._manager_health_state_snapshot = new_state

        return previous_state, new_state, True

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    async def start(self, timeout: float | None = None) -> None:
        """Start the manager server."""
        # Initialize locks (requires async context)
        self._manager_state.initialize_locks()

        # Start the underlying server
        await self.start_server(init_context=self._env.get_swim_init_context())

        # Restore (or create) this node's persisted incarnation so a
        # restarted manager rejoins above its pre-restart value.
        await self.initialize_incarnation_store()

        # Gates this manager was joined to in earlier runs: reported to
        # and SWIM-joined exactly like configured ones.
        await self._restore_joined_gates()

        if self._config.wal_data_dir is not None:
            # The full event-sourced job ledger (WAL + checkpoints +
            # archive + recovery), sharing the HLC created in __init__
            # so Raft proposals and job events are causally ordered
            # against the same logical clock. Recovery replays the WAL
            # on open: jobs this manager accepted survive a restart.
            self._job_ledger = await JobLedger.open(
                wal_path=self._config.wal_data_dir / "wal",
                checkpoint_dir=self._config.wal_data_dir / "checkpoints",
                archive_dir=self._config.wal_data_dir / "archive",
                region_code=self._node_id.datacenter,
                gate_id=self._node_id.short,
                regional_replicator=self._ledger_replicator.replicate,
                logger=self._udp_logger,
                clock=self._hlc,
                storage_health=self._storage_health,
            )
            self._node_wal = self._job_ledger._wal

        ledger_base_dir = self._idempotency_ledger_base_dir()
        ledger_path = ledger_base_dir / f"manager-idempotency-{self._node_id.short}.wal"
        self._idempotency_ledger = ManagerIdempotencyLedger(
            config=self._idempotency_config,
            wal_path=ledger_path,
            task_runner=self._task_runner,
            logger=self._udp_logger,
            storage_health=self._storage_health,
        )
        await self._idempotency_ledger.start()

        self._workflow_dispatcher = WorkflowDispatcher(
            job_manager=self._job_manager,
            worker_pool=self._worker_pool,
            manager_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            send_dispatch=self._dispatch.send_workflow_dispatch,
            task_runner=self._task_runner,
            on_dispatch_exhausted=self._fail_workflow_for_good,
            stop_dispatched_plans=self._cancellation.stop_dispatched_plans,
            on_dispatch_state_registered=self._replicate_job_state_for_dispatch,
            retry_budget_manager=self._retry_budget_manager,
            env=self.env,
            max_concurrent_dispatches=self._config.dispatch_max_concurrent_workers,
        )
        # THE dependency-completion edge. Without it, dependent
        # workflows never dispatch: the dispatcher holds them pending on
        # ``dependencies <= completed_dependencies`` and
        # ``WorkflowDispatcher.mark_workflow_completed`` — the sole
        # writer of ``completed_dependencies`` — is only reachable
        # through this callback (measured: A executed, B stranded, the
        # job died as an AD-34 timeout on every seed).
        self._job_manager.set_on_workflow_completed(
            self._handle_workflow_terminal_for_dispatch
        )

        self._worker_disseminator = WorkerDisseminator(
            state=self._manager_state,
            config=self._config,
            worker_pool=self._worker_pool,
            logger=self._udp_logger,
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            task_runner=self._task_runner,
            send_tcp=self._send_to_peer,
            gossip_buffer=WorkerStateGossipBuffer(),
        )

        # Mark as started
        self._started = True
        self._manager_state.set_manager_state_enum(ManagerStateEnum.ACTIVE)

        # Register with seed managers
        await self._register_with_peer_managers()

        # Join SWIM clusters
        await self._join_swim_clusters()

        # Request worker lists from peer managers (AD-48). Then push
        # ``ManagerToWorkerRegistration`` down to every learned worker
        # so they update their TCP-level ``_known_managers`` registry
        # to include this manager — the second half of AD-48's
        # bidirectional-registration spec that the original landing
        # never wired. Without this push the cluster looks healthy
        # at the manager tier (every manager sees every worker via
        # state-sync) but workers continue to dispatch only to the
        # managers they originally registered with, leaving a
        # returning manager invisible to the very workers it was
        # told about.
        if self._worker_disseminator:
            await self._worker_disseminator.request_worker_list_from_peers()
            await self._worker_disseminator.push_registration_to_remote_workers(
                is_leader=self.is_leader(),
                term=self._leader_election.state.current_term,
            )

        # Start SWIM probe cycle
        self._task_runner.run(self.start_probe_cycle)

        # Start the SWIM-tier DC-leader election. ``is_leader()`` reads the
        # state this loop populates and the job submission path rejects
        # work until ``is_leader()`` returns True for some manager in the
        # DC. Without this, every submit returns "Not DC leader, retry at
        # leader: unknown" because the election task is never created. The
        # gate server starts its election the same way (gate/server.py).
        await self.start_leader_election()

        # Start background tasks
        self._start_background_tasks()

        # Start Raft consensus tick loop and seed membership from known peers.
        # Re-bind the live task runner: _raft was constructed in __init__ when
        # the parent's TaskRunner was still None; start_server has populated it
        # by now.
        self._raft._consensus._task_runner = self._task_runner
        await self._raft.start()
        await self._cluster_membership.start()
        self._task_runner.run(self._clock_offset_prober.run, alias="clock_offset_prober")

        manager_count = self._manager_state.get_known_manager_peer_count() + 1
        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager started, {manager_count} managers in cluster",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Recovery LAST: resuming a recovered job re-runs the submit
        # tail (dispatcher registration, Raft job group, leadership
        # broadcast, dispatch) — machinery that only exists once the
        # dispatcher is constructed, the Raft tick loop is rebound, and
        # leader election has started. Running this in the ledger block
        # (where recovery originally lived) silently no-opped dispatch
        # (`_dispatch_job_workflows` guards on a dispatcher that was
        # still None) and wedged start() via a pre-tick Raft job group.
        await self._fail_recovered_active_jobs()

    def _idempotency_ledger_base_dir(self) -> Path:
        """The idempotency ledger's directory: the WAL data directory, else the logs directory."""
        return (
            self._config.wal_data_dir
            if self._config.wal_data_dir is not None
            else Path(self._env.MERCURY_SYNC_LOGS_DIRECTORY)
        )

    async def leave_cluster(self) -> None:
        """Drain this node's cluster membership (AD-52 section 13): the
        group releases its address now rather than after the tombstone
        retention, so the cluster's quorum stops counting a node that is
        going away. The outcome is logged; a refusal leaves the release to
        the leader's silence detection."""
        if not self._cluster_membership.formed:
            return
        reply = await self._cluster_membership.leave()
        await self._udp_logger.log(self._cluster_leave_log_entry(reply))

    def _cluster_leave_log_entry(self, reply: ClusterLeaveReply) -> ServerInfo | ServerWarning:
        """The log entry for the drain's outcome: released, or refused (AD-52)."""
        return (ServerInfo if reply.released else ServerWarning)(
            message=(
                f"Left cluster membership as {reply.released_member_id}"
                if reply.released
                else f"Cluster membership drain refused: {reply.refusal}"
            ),
            node_host=self._host,
            node_port=self._tcp_port,
            node_id=self._node_id.short,
        )

    async def stop(
        self,
        drain_timeout: float = 5,
        broadcast_leave: bool = True,
    ) -> None:
        """Stop the manager server."""
        if not self._running and not hasattr(self, "_started"):
            return
        await self._stop_started_manager(drain_timeout, broadcast_leave)

    async def _stop_started_manager(self, drain_timeout: float, broadcast_leave: bool) -> None:
        """Drain membership, stop background work and stores, then the server."""
        # Drain membership first, while its loops and this transport run.
        if broadcast_leave and self._running:
            await self.leave_cluster()

        self._running = False
        self._manager_state.set_manager_state_enum(ManagerStateEnum.DRAINING)

        # Cancel background tasks
        await self._cancel_background_tasks()
        await self._shut_down_dispatch_and_stores()

        # Stop the membership group, Raft consensus and clock offset probing
        await self._cluster_membership.stop()
        self._clock_offset_prober.stop()
        await self._raft.stop()

        # Graceful shutdown
        await super().stop(
            drain_timeout=drain_timeout,
            broadcast_leave=broadcast_leave,
        )

    async def _shut_down_dispatch_and_stores(self) -> None:
        """Shut the dispatcher down and close the idempotency ledger and job ledger/WAL."""
        # Each job's dispatch loop is a task of the dispatcher's: never
        # stopped here, every one outlived the manager.
        if self._workflow_dispatcher is not None:
            await self._workflow_dispatcher.shutdown()

        if self._idempotency_ledger is not None:
            await self._idempotency_ledger.close()

        await self._close_job_ledger_or_wal()

    async def _close_job_ledger_or_wal(self) -> None:
        """Close the job ledger (which owns the WAL), else the bare WAL."""
        if self._job_ledger is not None:
            await self._job_ledger.close()
            self._node_wal = None
        elif self._node_wal is not None:
            await self._node_wal.close()

    def abort(self) -> None:
        """Abort the manager server immediately."""
        self._running = False
        self._manager_state.set_manager_state_enum(ManagerStateEnum.OFFLINE)

        # Cancel all background tasks synchronously
        for task in filter(self._background_task_is_pending, self._get_background_tasks()):
            task.cancel()
        if self._workflow_dispatcher is not None:
            self._workflow_dispatcher.abort()

        super().abort()

    def _get_background_tasks(self) -> list[asyncio.Task | None]:
        """Get list of background tasks."""
        return [
            self._dead_node_reap_task,
            self._orphan_scan_task,
            self._job_responsiveness_task,
            self._stats_push_task,
            self._windowed_stats_flush_task,
            self._gate_heartbeat_task,
            self._rate_limit_cleanup_task,
            self._job_cleanup_task,
            self._unified_timeout_task,
            self._deadline_enforcement_task,
            self._manager_peer_registration_sync_task,
            self._peer_job_state_sync_task,
            self._resource_sample_task,
            self._resource_gossip_task,
        ]

    def _start_background_tasks(self) -> None:
        self._dead_node_reap_task = self._create_background_task(
            self._dead_node_reap_loop(), "dead_node_reap"
        )
        self._orphan_scan_task = self._create_background_task(
            self._orphan_scan_loop(), "orphan_scan"
        )
        self._job_responsiveness_task = self._create_background_task(
            self._job_responsiveness_loop(), "job_responsiveness"
        )
        self._stats_push_task = self._create_background_task(
            self._stats_push_loop(), "stats_push"
        )
        self._windowed_stats_flush_task = self._create_background_task(
            self._windowed_stats_flush_loop(), "windowed_stats_flush"
        )
        self._gate_heartbeat_task = self._create_background_task(
            self._gate_heartbeat_loop(), "gate_heartbeat"
        )
        self._rate_limit_cleanup_task = self._create_background_task(
            self._rate_limit_cleanup_loop(), "rate_limit_cleanup"
        )
        self._job_cleanup_task = self._create_background_task(
            self._job_cleanup_loop(), "job_cleanup"
        )
        self._unified_timeout_task = self._create_background_task(
            self._unified_timeout_loop(), "unified_timeout"
        )
        self._deadline_enforcement_task = self._create_background_task(
            self._deadline_enforcement_loop(), "deadline_enforcement"
        )
        self._manager_peer_registration_sync_task = self._create_background_task(
            self._manager_peer_registration_sync_loop(),
            "manager_peer_registration_sync",
        )
        self._peer_job_state_sync_task = self._create_background_task(
            self._peer_job_state_sync_loop(), "peer_job_state_sync"
        )
        self._resource_sample_task = self._create_background_task(
            self._resource_sample_loop(), "resource_sample"
        )
        self._resource_gossip_task = self._create_background_task(
            self._resource_gossip_loop(), "resource_gossip"
        )

    async def _cancel_background_tasks(self) -> None:
        """Cancel all background tasks."""
        for task in filter(self._background_task_is_pending, self._get_background_tasks()):
            await self._cancel_and_await_background_task(task)

    def _background_task_is_pending(self, task: asyncio.Task | None) -> bool:
        """True for a background task that exists and has not finished."""
        return task and not task.done()

    async def _cancel_and_await_background_task(self, task: asyncio.Task) -> None:
        """Cancel the task and wait it out, re-raising a cancel aimed at the caller."""
        task.cancel()
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        try:
            await task
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise

    # =========================================================================
    # Registration
    # =========================================================================

    def _build_manager_info(self) -> ManagerInfo:
        """Build this manager's peer-registration identity."""
        return ManagerInfo(
            node_id=self._node_id.full,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            udp_host=self._host,
            udp_port=self._udp_port,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
        )

    def _manager_udp_addr_for_tcp(
        self,
        tcp_addr: tuple[str, int],
    ) -> tuple[str, int] | None:
        """Resolve a configured or already-known manager UDP address from TCP."""
        known_udp_addr = self._known_manager_udp_addr_for_tcp(tcp_addr)
        if known_udp_addr is not None:
            return known_udp_addr

        return self._seed_manager_udp_addr_for_tcp(tcp_addr)

    def _known_manager_udp_addr_for_tcp(self, tcp_addr: tuple[str, int]) -> tuple[str, int] | None:
        """The UDP address of the first known manager peer at ``tcp_addr``."""
        for peer_info in self._manager_state.get_known_manager_peer_values():
            if (peer_info.tcp_host, peer_info.tcp_port) == tcp_addr:
                return (peer_info.udp_host, peer_info.udp_port)
        return None

    def _seed_manager_udp_addr_for_tcp(self, tcp_addr: tuple[str, int]) -> tuple[str, int] | None:
        """The configured UDP address paired with the seed manager at ``tcp_addr``."""
        for seed_tcp_addr, seed_udp_addr in zip(
            self._seed_managers,
            self._manager_udp_peers,
            strict=False,
        ):
            if seed_tcp_addr == tcp_addr:
                return seed_udp_addr

        return None

    def _build_manager_info_from_registration_response(
        self,
        manager_addr: tuple[str, int],
        response: ManagerPeerRegistrationResponse,
    ) -> ManagerInfo | None:
        """Build responder manager info from a registration response."""
        if response.manager_info is not None:
            return response.manager_info

        udp_addr = self._manager_udp_addr_for_tcp(manager_addr)
        if udp_addr is None:
            return None

        return ManagerInfo(
            node_id=response.manager_id,
            tcp_host=manager_addr[0],
            tcp_port=manager_addr[1],
            udp_host=udp_addr[0],
            udp_port=udp_addr[1],
            datacenter=self._node_id.datacenter,
            is_leader=response.is_leader,
        )

    def _find_manager_peer_address_collisions(
        self,
        peer_info: ManagerInfo,
    ) -> list[str]:
        """Return registered manager peer IDs already bound to this peer's address."""
        tcp_addr = (peer_info.tcp_host, peer_info.tcp_port)
        udp_addr = (peer_info.udp_host, peer_info.udp_port)
        return [
            peer_id
            for peer_id, known_peer in self._manager_state.iter_known_manager_peers()
            if self._is_other_node_at_address(peer_id, known_peer, peer_info.node_id, tcp_addr, udp_addr)
        ]

    def _manager_peer_registration_requires_rejoin_reset(
        self,
        peer_info: ManagerInfo,
        address_collision_peer_ids: list[str],
    ) -> bool:
        """Return True when manager TCP registration proves a stale SWIM peer rejoined."""
        tcp_addr = (peer_info.tcp_host, peer_info.tcp_port)
        udp_addr = (peer_info.udp_host, peer_info.udp_port)
        node_state = self._incarnation_tracker.get_node_state(udp_addr)
        has_dead_or_suspect_state = self._node_state_suspect_or_dead(node_state)
        has_death_record = (
            self._incarnation_tracker.get_required_rejoin_incarnation(udp_addr) > 0
        )
        is_unhealthy = (
            self._manager_state.get_manager_peer_unhealthy_since(peer_info.node_id)
            is not None
        )
        return self._has_swim_rejoin_evidence(
            address_collision_peer_ids, has_dead_or_suspect_state, has_death_record
        ) or self._has_manager_death_evidence(tcp_addr, is_unhealthy)

    def _has_swim_rejoin_evidence(
        self,
        address_collision_peer_ids: list[str],
        has_dead_or_suspect_state: bool,
        has_death_record: bool,
    ) -> bool:
        """True for an address collision, a SUSPECT/DEAD SWIM state or a death record."""
        return (
            bool(address_collision_peer_ids)
            or has_dead_or_suspect_state
            or has_death_record
        )

    def _has_manager_death_evidence(self, tcp_addr: tuple[str, int], is_unhealthy: bool) -> bool:
        """True when the peer is recorded dead or tracked unhealthy."""
        return tcp_addr in self._manager_state.get_dead_managers() or is_unhealthy

    async def _ingest_manager_peer_info(
        self,
        peer_info: ManagerInfo,
        *,
        authoritative_registration: bool,
    ) -> bool:
        """Apply manager peer identity to registry, SWIM, and recovery state."""
        if peer_info.node_id == self._node_id.full:
            return False

        return await self._ingest_other_manager_peer_info(peer_info, authoritative_registration)

    async def _ingest_other_manager_peer_info(
        self,
        peer_info: ManagerInfo,
        authoritative_registration: bool,
    ) -> bool:
        """Ingest another manager's identity; False when it collides on an
        address and the registration is not authoritative."""
        address_collision_peer_ids = self._find_manager_peer_address_collisions(
            peer_info
        )
        if address_collision_peer_ids and not authoritative_registration:
            return False

        await self._apply_manager_peer_info(
            peer_info, address_collision_peer_ids, authoritative_registration
        )
        return True

    async def _apply_manager_peer_info(
        self,
        peer_info: ManagerInfo,
        address_collision_peer_ids: list[str],
        authoritative_registration: bool,
    ) -> None:
        """Register the peer, map its addresses into SWIM, and admit it when
        the registration is authoritative."""
        peer_tcp_addr = (peer_info.tcp_host, peer_info.tcp_port)
        peer_udp_addr = (peer_info.udp_host, peer_info.udp_port)
        requires_rejoin_reset = (
            authoritative_registration
            and self._manager_peer_registration_requires_rejoin_reset(
                peer_info,
                address_collision_peer_ids,
            )
        )
        existing_peer_info = self._manager_state.get_known_manager_peer(
            peer_info.node_id
        )

        if self._manager_peer_registry_is_stale(
            existing_peer_info, address_collision_peer_ids, peer_info
        ):
            await self._registry.register_manager_peer(peer_info)
        self._manager_state.set_manager_udp_to_tcp_mapping(peer_udp_addr, peer_tcp_addr)
        self._probe_scheduler.add_member(peer_udp_addr)
        self.record_peer_role(peer_udp_addr, NodeRole.MANAGER.value)

        await self._admit_authoritatively_registered_manager_peer(
            peer_info, peer_udp_addr, authoritative_registration, requires_rejoin_reset
        )

    def _manager_peer_registry_is_stale(
        self,
        existing_peer_info: ManagerInfo | None,
        address_collision_peer_ids: list[str],
        peer_info: ManagerInfo,
    ) -> bool:
        """True when the registry lacks the peer, it collides, or its info changed."""
        return (
            existing_peer_info is None
            or bool(address_collision_peer_ids)
            or existing_peer_info != peer_info
        )

    async def _admit_authoritatively_registered_manager_peer(
        self,
        peer_info: ManagerInfo,
        peer_udp_addr: tuple[str, int],
        authoritative_registration: bool,
        requires_rejoin_reset: bool,
    ) -> None:
        """Register an authoritatively registered peer with SWIM, reset it for a
        rejoin when required, and activate it (a rejoin reset implies authority)."""
        if not authoritative_registration:
            return

        self.register_peer(peer_udp_addr)

        if requires_rejoin_reset:
            await self.reset_peer_for_rejoin(peer_udp_addr)

        await self._activate_registered_manager_peer(
            peer_info,
            bump_epoch=requires_rejoin_reset,
        )

    async def _activate_registered_manager_peer(
        self,
        peer_info: ManagerInfo,
        *,
        bump_epoch: bool,
    ) -> None:
        """Mark a TCP-registered manager peer active across membership stores."""
        tcp_addr = (peer_info.tcp_host, peer_info.tcp_port)
        peer_lock = await self._manager_state.get_peer_state_lock(tcp_addr)

        async with peer_lock:
            if bump_epoch:
                self._manager_state.increment_peer_state_epoch(tcp_addr)

            await self._manager_state.add_active_peer(tcp_addr, peer_info.node_id)
            self._manager_state.clear_manager_peer_unhealthy_since(peer_info.node_id)
            self._manager_state.remove_dead_manager(tcp_addr)

    async def _register_with_peer_managers(self) -> None:
        """Register with seed peer managers."""
        for seed_addr in self._seed_managers:
            try:
                await self._register_with_manager(seed_addr)
            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to register with peer manager {seed_addr}: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    def _manager_peer_registration_targets(self) -> list[tuple[str, int]]:
        """Return deduplicated manager TCP addrs for registration reconciliation."""
        local_addr = (self._host, self._tcp_port)
        target_addrs: list[tuple[str, int]] = [
            *self._non_local_seed_managers(local_addr),
            *self._non_local_known_manager_tcp_addrs(local_addr),
        ]

        return list(dict.fromkeys(target_addrs))

    def _non_local_seed_managers(self, local_addr: tuple[str, int]) -> list[tuple[str, int]]:
        """The seed manager addresses other than this manager's."""
        return [
            manager_addr for manager_addr in self._seed_managers if manager_addr != local_addr
        ]

    def _non_local_known_manager_tcp_addrs(self, local_addr: tuple[str, int]) -> list[tuple[str, int]]:
        """The known manager peers' TCP addresses other than this manager's."""
        return [
            manager_addr
            for manager_addr in map(
                self._manager_info_tcp_addr,
                self._manager_state.get_known_manager_peer_values(),
            )
            if manager_addr != local_addr
        ]

    def _manager_info_tcp_addr(self, peer_info: ManagerInfo) -> tuple[str, int]:
        """The manager's TCP address."""
        return (peer_info.tcp_host, peer_info.tcp_port)

    async def _sync_manager_peer_registrations(self) -> None:
        """Reconcile manager peer registration against every known peer addr."""
        manager_addrs = self._manager_peer_registration_targets()
        if not manager_addrs:
            return

        await asyncio.gather(
            *(
                self._register_with_manager_under_recovery_limit(
                    manager_addr,
                    log_transient_errors=False,
                )
                for manager_addr in manager_addrs
            )
        )

    async def _register_with_manager_under_recovery_limit(
        self,
        manager_addr: tuple[str, int],
        *,
        log_transient_errors: bool,
    ) -> bool:
        """Register with a manager peer under the recovery concurrency cap."""
        async with self._recovery_semaphore:
            return await self._register_with_manager(
                manager_addr,
                log_transient_errors=log_transient_errors,
            )

    async def _manager_peer_registration_sync_loop(self) -> None:
        """Periodically re-run manager peer registration for missed rejoins."""
        sync_interval = self._config.peer_sync_interval_seconds
        if sync_interval <= 0:
            return

        await self._run_manager_peer_registration_sync(sync_interval)

    async def _run_manager_peer_registration_sync(self, sync_interval: float) -> None:
        """Re-run peer registration every ``sync_interval`` while the manager runs."""
        while self._running:
            if not await self._manager_peer_registration_sync_iteration(sync_interval):
                break

    async def _manager_peer_registration_sync_iteration(self, sync_interval: float) -> bool:
        """One registration sync round (backing off a full interval after an
        error); False when cancelled."""
        try:
            await self._sync_manager_peer_registrations()
            await self._clock.sleep(sync_interval)
        except asyncio.CancelledError:
            return False
        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Manager peer registration sync error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            await self._clock.sleep(sync_interval)
        return True

    async def _restore_joined_gates(self) -> None:
        """Add the gates saved by earlier runs' joins to this manager's gate
        addresses (TCP and UDP, paired by position)."""
        if self._config.wal_data_dir is None:
            return

        self._joined_peer_store = JoinedPeerStore(
            self._config.wal_data_dir,
            self._storage_filesystem,
            self._udp_logger,
            self._host,
            self._tcp_port,
        )
        for joined_gate in await self._joined_peer_store.load():
            self._restore_joined_gate(joined_gate)

    def _restore_joined_gate(self, joined_gate: JoinedPeer) -> None:
        """Add one saved gate's addresses and mapping, unless already a seed gate."""
        if joined_gate.tcp_address in self._seed_gates:
            return

        self._seed_gates.append(joined_gate.tcp_address)
        self._gate_udp_addrs.append(joined_gate.udp_address)
        self._manager_state.set_gate_udp_to_tcp_mapping(
            joined_gate.udp_address, joined_gate.tcp_address
        )

    async def _join_node(self, target_addr: tuple[str, int]) -> None:
        """Operator join: register this manager with the gate at ``target_addr``.

        Uses the gate's existing ``manager_register`` endpoint, which
        validates cluster/environment isolation, mTLS role and protocol
        version, records this datacenter, and answers with its healthy
        gate tier (itself first). Each returned gate is tracked exactly as
        a gate registering with us would be. Manager peers are not a
        valid target: that endpoint exists only on gates (and workers),
        so a peer join is refused by the target.
        """
        response, _ = await self.send_tcp(
            target_addr,
            "manager_register",
            self._build_manager_heartbeat().dump(),
            timeout=self._config.tcp_timeout_standard_seconds,
        )
        self._raise_if_join_unanswered(target_addr, response)

        registration = decode_join_message(
            response,
            ManagerRegistrationResponse,
            f"manager registration reply from {target_addr[0]}:{target_addr[1]} "
            "(managers can only join gates)",
        )

        if not registration.accepted:
            raise ClusterJoinError(
                f"gate {target_addr[0]}:{target_addr[1]} refused registration: "
                f"{registration.error}"
            )

        for gate_info in registration.healthy_gates:
            await self._track_registered_gate(gate_info)

        self._record_joined_seed_gate(target_addr)

        await self._persist_joined_gates(registration.healthy_gates)

    def _raise_if_join_unanswered(
        self,
        target_addr: tuple[str, int],
        response: bytes | Exception | None,
    ) -> None:
        """Raise ``ClusterJoinError`` when the registration send failed."""
        if isinstance(response, Exception):
            raise ClusterJoinError(
                f"{target_addr[0]}:{target_addr[1]} did not answer manager "
                f"registration: {type(response).__name__}: {response}"
            )

    def _record_joined_seed_gate(self, target_addr: tuple[str, int]) -> None:
        """Add the joined gate to the seed gates once."""
        if target_addr not in self._seed_gates:
            self._seed_gates.append(target_addr)

    async def _persist_joined_gates(self, healthy_gates: list[GateInfo]) -> None:
        """Keep the joined gate tier in the joined-peer store, when there is one."""
        if self._joined_peer_store is not None:
            await self._joined_peer_store.add(
                [
                    JoinedPeer(
                        datacenter=gate_info.datacenter,
                        tcp_address=(gate_info.tcp_host, gate_info.tcp_port),
                        udp_address=(gate_info.udp_host, gate_info.udp_port),
                    )
                    for gate_info in healthy_gates
                ]
            )

    async def _register_with_manager(
        self,
        manager_addr: tuple[str, int],
        *,
        log_transient_errors: bool = True,
    ) -> bool:
        """Register with a single peer manager."""
        registration = ManagerPeerRegistration(
            node=self._build_manager_info(),
            term=self._leader_election.state.current_term,
            is_leader=self.is_leader(),
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
        )

        try:
            response, _clock = await self.send_tcp(
                manager_addr,
                "manager_peer_register",
                registration.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
            if await self._manager_registration_accepted(manager_addr, response):
                return True

        except Exception as error:
            await self._log_manager_registration_error(error, log_transient_errors)

        return False

    async def _manager_registration_accepted(
        self,
        manager_addr: tuple[str, int],
        response: bytes | Exception | None,
    ) -> bool:
        """Ingest an accepting peer's registration response; raises a transport error."""
        # send_tcp returns transport errors rather than raising.
        if isinstance(response, Exception):
            raise response

        if not response:
            return False

        return await self._ingest_manager_registration_response(manager_addr, response)

    async def _ingest_manager_registration_response(
        self,
        manager_addr: tuple[str, int],
        response: bytes,
    ) -> bool:
        """Ingest the responder and its known peers when it accepted; True if it did."""
        parsed = ManagerPeerRegistrationResponse.load(response)
        if not parsed.accepted:
            return False

        await self._ingest_accepted_manager_registration(manager_addr, parsed)
        return True

    async def _ingest_accepted_manager_registration(
        self,
        manager_addr: tuple[str, int],
        parsed: ManagerPeerRegistrationResponse,
    ) -> None:
        """Ingest the responder authoritatively and each of its known peers."""
        responder_info = self._build_manager_info_from_registration_response(
            manager_addr,
            parsed,
        )
        if responder_info is not None:
            await self._ingest_manager_peer_info(
                responder_info,
                authoritative_registration=True,
            )
        for peer_info in parsed.known_peers:
            await self._ingest_manager_peer_info(
                peer_info,
                authoritative_registration=False,
            )

    async def _log_manager_registration_error(
        self,
        error: Exception,
        log_transient_errors: bool,
    ) -> None:
        """Log a registration error -- at debug level when transient errors are quiet."""
        log_entry = ServerError if log_transient_errors else ServerDebug

        await self._udp_logger.log(
            log_entry(
                message=f"Manager registration error: {error}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _join_swim_clusters(self) -> None:
        """Join SWIM clusters for managers, gates, and workers.

        We know the role of each static seed by configuration (manager
        peers come from ``_manager_udp_peers``, gate seeds from
        ``_gate_udp_addrs``); pass it through ``join_cluster`` so the
        seed's entry in ``_peer_roles`` is authoritative immediately —
        leader-election cohort filtering does not need to wait for
        gossip to propagate role info.
        """
        # Join manager SWIM cluster
        for udp_addr in self._manager_udp_peers:
            await self.join_cluster(udp_addr, seed_role="manager")

        # Join gate SWIM cluster if gates configured
        for udp_addr in self._gate_udp_addrs:
            await self.join_cluster(udp_addr, seed_role="gate")

    # =========================================================================
    # SWIM Callbacks
    # =========================================================================

    def _on_peer_confirmed(self, peer: tuple[str, int]) -> None:
        """Handle peer confirmation via SWIM (AD-29)."""
        # Check if manager peer
        tcp_addr = self._manager_state.get_manager_tcp_from_udp(peer)
        if tcp_addr:
            self._activate_confirmed_manager_peer(peer, tcp_addr)

    def _activate_confirmed_manager_peer(self, peer: tuple[str, int], tcp_addr: tuple[str, int]) -> None:
        """Mark the SWIM-confirmed known manager peer active (AD-29)."""
        peer_id = self._node_id_with_udp_addr(self._manager_state.iter_known_manager_peers(), peer)
        if peer_id is not None:
            self._task_runner.run(
                self._manager_state.add_active_peer, tcp_addr, peer_id
            )

    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """Handle node death detected by SWIM."""
        worker_id = self._manager_state.get_worker_id_from_addr(node_addr)
        if worker_id:
            self._worker_pool.mark_worker_draining_immediate(
                worker_id,
                "swim_node_dead",
            )
            self._detach_worker_membership(worker_id)
            # An unexplained death: charged to the workflows it ran (AD-44).
            self._task_runner.run(self._handle_worker_failure, worker_id, True)
            return

        self._on_manager_or_gate_dead(node_addr)

    def _on_manager_or_gate_dead(self, node_addr: tuple[str, int]) -> None:
        """Run the failure handling of the dead manager peer or gate at ``node_addr``."""
        manager_tcp_addr = self._manager_state.get_manager_tcp_from_udp(node_addr)
        if manager_tcp_addr:
            self._task_runner.run(
                self._handle_manager_peer_failure, node_addr, manager_tcp_addr
            )
            return

        # Check if gate
        gate_tcp_addr = self._manager_state.get_gate_tcp_from_udp(node_addr)
        if gate_tcp_addr:
            self._task_runner.run(
                self._handle_gate_peer_failure, node_addr, gate_tcp_addr
            )

    def _on_node_join(self, node_addr: tuple[str, int]) -> None:
        """Handle node join detected by SWIM."""
        # Check if worker
        worker_id = self._manager_state.get_worker_id_from_addr(node_addr)
        if worker_id:
            self._manager_state.clear_worker_unhealthy_since(worker_id)
            return

        self._on_manager_or_gate_join(node_addr)

    def _on_manager_or_gate_join(self, node_addr: tuple[str, int]) -> None:
        """Run the recovery of the rejoining manager peer or gate at ``node_addr``."""
        # Check if manager peer
        manager_tcp_addr = self._manager_state.get_manager_tcp_from_udp(node_addr)
        if manager_tcp_addr:
            dead_managers = self._manager_state.get_dead_managers()
            dead_managers.discard(manager_tcp_addr)
            self._task_runner.run(
                self._register_with_manager,
                manager_tcp_addr,
                log_transient_errors=False,
            )
            self._task_runner.run(
                self._handle_manager_peer_recovery, node_addr, manager_tcp_addr
            )
            return

        # Check if gate
        gate_tcp_addr = self._manager_state.get_gate_tcp_from_udp(node_addr)
        if gate_tcp_addr:
            self._task_runner.run(
                self._handle_gate_peer_recovery, node_addr, gate_tcp_addr
            )

    def _on_manager_become_leader(self) -> None:
        """Handle becoming SWIM cluster leader."""
        self._task_runner.run(self._state_sync.sync_state_from_workers)
        self._task_runner.run(self._state_sync.sync_full_state_from_manager_peers)
        self._task_runner.run(self._scan_for_orphaned_jobs)
        self._task_runner.run(self._resume_timeout_tracking_for_all_jobs)

    def _on_manager_lose_leadership(self) -> None:
        self._task_runner.run(self._handle_leadership_loss)

    async def _handle_leadership_loss(self) -> None:
        await self._udp_logger.log(
            ServerInfo(
                message="Lost SWIM cluster leadership - pausing leader-only tasks",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        for job_id in self._leases.get_led_job_ids():
            await self._stop_timeout_tracking_on_leadership_loss(job_id)

    async def _stop_timeout_tracking_on_leadership_loss(self, job_id: str) -> None:
        """Stop the led job's timeout tracking, logging a failure to stop it."""
        strategy = self._manager_state.get_job_timeout_strategy(job_id)
        if strategy:
            try:
                await strategy.stop_tracking(job_id, "leadership_lost")
            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to stop timeout tracking for job {job_id[:8]}...: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    # =========================================================================
    # Per-Job Raft Leader Callbacks
    # =========================================================================

    def _on_job_raft_leader(self, job_id: str) -> None:
        """Called when this node becomes the per-job Raft leader."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerDebug(
                message=(
                    f"Ignoring per-job Raft leadership for {job_id[:8]}... "
                    "as a failover authority; SWIM leadership coordinates takeover"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _on_job_raft_lose_leader(self, job_id: str) -> None:
        """Called when this node loses per-job Raft leadership."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Lost Raft leadership for job {job_id[:8]}...",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    async def _check_raft_leader_takeover(self, job_id: str) -> None:
        """Deprecated per-job Raft takeover hook.

        Job leadership is independent of SWIM cluster leadership during steady
        state, but failover is coordinated only by the SWIM cluster leader.
        """
        return

    def _on_worker_dead_for_job(self, job_id: str, worker_id: str) -> None:
        if not self._workflow_dispatcher or not self._job_manager:
            return

        self._task_runner.run(
            self._handle_worker_dead_for_job_reassignment,
            job_id,
            worker_id,
        )

    async def _handle_worker_dead_for_job_reassignment(
        self,
        job_id: str,
        worker_id: str,
    ) -> None:
        # The job's leader decides its workflows' retries.
        if not self._may_reassign_job_workflows(job_id):
            return

        # The same filter as a worker's global death: a superseded sub, or
        # one whose parent already finished (a multi-core workflow completed
        # on its surviving sub), is not reassigned -- requeueing it
        # dispatched a finished workflow again, running its load twice.
        # The worker stalled on this job (AD-30): charged to its workflows.
        for _, workflow_id, sub_token in self._job_manager.get_reassignable_sub_workflows_on_worker(
            worker_id,
            job_id=job_id,
        ):
            await self._apply_workflow_reassignment_state(
                job_id=job_id,
                workflow_id=workflow_id,
                sub_workflow_token=sub_token,
                failed_worker_id=worker_id,
                reason="job_unresponsive",
                loss_is_charged=not self._systemic_eviction_hold,
            )

    def _may_reassign_job_workflows(self, job_id: str) -> bool:
        """True when dispatch runs here and this manager leads the job."""
        return bool(
            self._workflow_dispatcher
            and self._job_manager
            and self._leases.is_job_leader(job_id)
        )

    async def _apply_workflow_reassignment_state(
        self,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        failed_worker_id: str,
        reason: str,
        loss_is_charged: bool,
    ) -> tuple[bool, bool]:
        """
        A worker the workflow ran on is lost; this manager leads its job
        and decides what that means (a follower only mirrors the superseded
        sub):

        * a sub still runs elsewhere -> the workflow finishes with reduced
          parallelism, or completes now if the survivors already reported;
        * every worker it ran on is lost -> it runs again, unless the loss
          is charged to it (AD-44: an unexplained death, an AD-41
          over-budget eviction, an AD-30 stall, an orphan -- never our own
          eviction or a systemic loss) and its retry budget is spent, or
          this manager holds no dispatch entry to run it with (its job was
          taken over without the workflow): then it fails for good, loudly,
          with the cause.

        Returns ``(applied, requeued)``.
        """
        if not self._workflow_dispatcher or not self._job_manager:
            return False, False

        async with self._workflow_reassignment_lock:
            applied, lost_every_sub = await self._job_manager.apply_workflow_reassignment(
                job_id=job_id,
                workflow_id=workflow_id,
                sub_workflow_token=sub_workflow_token,
                failed_worker_id=failed_worker_id,
            )
            requeued, failure_reason, ready_result = await self._settle_reassigned_workflow(
                applied,
                lost_every_sub,
                job_id,
                workflow_id,
                sub_workflow_token,
                failed_worker_id,
                reason,
                loss_is_charged,
            )

        await self._finish_workflow_reassignment(
            job_id,
            workflow_id,
            failed_worker_id,
            reason,
            applied,
            failure_reason,
            ready_result,
        )
        return applied, requeued

    async def _settle_reassigned_workflow(
        self,
        applied: bool,
        lost_every_sub: bool,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        failed_worker_id: str,
        reason: str,
        loss_is_charged: bool,
    ) -> tuple[bool, str | None, WorkflowFinalResult | None]:
        """
        Decide, under the reassignment lock, what an applied worker loss means for its workflow.

        Returns ``(requeued, failure_reason, ready_result)``: whether the
        workflow was requeued, why it fails for good (None when it does not),
        and the parent's result when the surviving subs already completed it.
        A workflow being cancelled lost a worker: nothing runs it there any
        more, so its tracked sub drains instead.
        """
        if not applied:
            return False, None, None
        if self._job_manager.workflow_lifecycle.get_state(job_id, workflow_id) == WorkflowState.CANCELLING:
            await self._drain_cancelling_workflow_after_loss(
                job_id, workflow_id, sub_workflow_token, lost_every_sub
            )
            return False, None, None
        return await self._settle_running_reassigned_workflow(
            lost_every_sub,
            job_id,
            workflow_id,
            sub_workflow_token,
            failed_worker_id,
            reason,
            loss_is_charged,
        )

    async def _drain_cancelling_workflow_after_loss(
        self,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        lost_every_sub: bool,
    ) -> None:
        """
        Drain a cancelling workflow's sub on a lost worker, cancelling the workflow once no sub is left.

        Its tracked sub drains, survivors or not -- the job's cancellation
        completes only once every sub did; with no sub left anywhere the
        workflow is cancelled and the job completes if it is done.
        """
        await self._cancellation.finalize_workflow_cancellation(
            job_id=job_id,
            workflow_id=sub_workflow_token,
            success=True,
            errors=[],
        )
        if lost_every_sub and await self._job_manager.finish_workflow_cancellation(
            job_id, workflow_id
        ):
            await self._complete_job_if_done(job_id)

    async def _settle_running_reassigned_workflow(
        self,
        lost_every_sub: bool,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        failed_worker_id: str,
        reason: str,
        loss_is_charged: bool,
    ) -> tuple[bool, str | None, WorkflowFinalResult | None]:
        """
        Settle a running workflow that lost a worker: finish on its survivors, or run it again.

        Returns ``(requeued, failure_reason, ready_result)``. A workflow with
        a sub still running elsewhere yields its ready result, if any; one
        that lost every sub while dispatched or running is requeued or fails
        for good; any other workflow needs nothing.
        """
        if not lost_every_sub:
            return False, None, await self._ready_result_after_superseding(
                job_id, workflow_id, failed_worker_id
            )
        if self._job_manager.workflow_lifecycle.get_state(
            job_id, workflow_id
        ) not in (WorkflowState.DISPATCHED, WorkflowState.RUNNING):
            return False, None, None
        requeued, failure_reason = await self._rerun_lost_workflow(
            job_id,
            workflow_id,
            sub_workflow_token,
            failed_worker_id,
            reason,
            loss_is_charged,
        )
        return requeued, failure_reason, None

    async def _ready_result_after_superseding(
        self,
        job_id: str,
        workflow_id: str,
        failed_worker_id: str,
    ) -> WorkflowFinalResult | None:
        """Fetch the parent's ready result once a failed worker's sub is superseded, logging which way it went."""
        ready_result = await self._job_manager.get_parent_ready_result(
            job_id,
            workflow_id,
        )
        await self._udp_logger.log(
            ServerInfo(
                message=(
                    f"Workflow {workflow_id[:8]}... is ready after "
                    f"superseding failed worker {failed_worker_id[:8]}..."
                    if ready_result is not None
                    else f"Workflow {workflow_id[:8]}... still has active "
                    f"sub-workflows after worker {failed_worker_id[:8]}... failed"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        return ready_result

    async def _rerun_lost_workflow(
        self,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        failed_worker_id: str,
        reason: str,
        loss_is_charged: bool,
    ) -> tuple[bool, str | None]:
        """Requeue a workflow that lost every worker it ran on and log the outcome; returns (requeued, failure_reason)."""
        requeued, failure_reason = await self._requeue_lost_workflow(
            job_id,
            workflow_id,
            sub_workflow_token,
            failed_worker_id,
            reason,
            loss_is_charged,
        )
        await self._log_lost_workflow_requeue(job_id, workflow_id, failed_worker_id, reason, requeued)
        return requeued, failure_reason

    async def _requeue_lost_workflow(
        self,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        failed_worker_id: str,
        reason: str,
        loss_is_charged: bool,
    ) -> tuple[bool, str | None]:
        """
        Return a lost workflow to pending and requeue it, or name why it fails for good.

        It fails for good when this manager holds no dispatch entry to run it
        again (its job was taken over without the workflow), or when the loss
        is charged to it and its retry budget is spent.
        """
        if f"{job_id}:{workflow_id}" not in self._workflow_dispatcher.get_pending_workflows():
            return False, (
                f"lost with worker {failed_worker_id} ({reason}), and this manager "
                "holds no dispatch entry to run it again (its job was taken over "
                "without the workflow)"
            )
        retry_allowed, budget_state = await self._charge_lost_workflow_retry(
            job_id, workflow_id, loss_is_charged
        )
        if not retry_allowed:
            return False, (
                f"lost with worker {failed_worker_id} ({reason}), and its "
                f"retry budget is spent ({budget_state})"
            )
        return await self._return_lost_workflow_to_pending(
            job_id, workflow_id, sub_workflow_token, failed_worker_id, reason
        ), None

    async def _charge_lost_workflow_retry(
        self,
        job_id: str,
        workflow_id: str,
        loss_is_charged: bool,
    ) -> tuple[bool, str]:
        """Consume a retry from the workflow's budget when the loss is charged to it; returns (allowed, budget state)."""
        if not loss_is_charged:
            return True, "the loss is not charged to the workflow"
        return await self._retry_budget_manager.check_and_consume(job_id, workflow_id)

    async def _return_lost_workflow_to_pending(
        self,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
        failed_worker_id: str,
        reason: str,
    ) -> bool:
        """Move a lost workflow back to pending and requeue it away from the failed worker; returns whether requeued."""
        if await self._job_manager.return_workflow_to_pending(
            job_id,
            workflow_id,
            f"worker {failed_worker_id} lost ({reason})",
        ):
            return await self._workflow_dispatcher.requeue_workflow(
                sub_workflow_token,
                excluded_worker_id=failed_worker_id,
            )
        return False

    async def _log_lost_workflow_requeue(
        self,
        job_id: str,
        workflow_id: str,
        failed_worker_id: str,
        reason: str,
        requeued: bool,
    ) -> None:
        """Log a lost workflow's requeue, and warn when no healthy worker is left to run it."""
        if requeued:
            await self._udp_logger.log(
                ServerInfo(
                    message=(
                        f"Requeued workflow {workflow_id[:8]}... from "
                        f"failed worker {failed_worker_id[:8]}... ({reason})"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        if not self._worker_pool.get_healthy_worker_ids():
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"No healthy workers available to reassign workflow "
                        f"{workflow_id[:8]}... for job {job_id[:8]}..."
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _finish_workflow_reassignment(
        self,
        job_id: str,
        workflow_id: str,
        failed_worker_id: str,
        reason: str,
        applied: bool,
        failure_reason: str | None,
        ready_result: WorkflowFinalResult | None,
    ) -> None:
        """
        Act on a settled reassignment outside its lock.

        Tells the gate about an applied reassignment, fails a workflow that
        cannot run again for good, and completes a parent workflow its
        surviving subs made ready.
        """
        if applied:
            await self._notify_gate_of_workflow_reassignment(
                job_id=job_id,
                workflow_id=workflow_id,
                failed_worker_id=failed_worker_id,
                reason=reason,
                new_worker_id=None,
            )

        if failure_reason is not None:
            await self._fail_workflow_for_good(job_id, workflow_id, failure_reason)

        await self._complete_ready_parent_workflow(job_id, ready_result)

    async def _complete_ready_parent_workflow(
        self,
        job_id: str,
        ready_result: WorkflowFinalResult | None,
    ) -> None:
        """Complete a parent workflow from its ready result, then the job if that finished it; None does nothing."""
        if ready_result is None:
            return
        await self._handle_parent_workflow_completion(ready_result, True, True)
        if self._is_job_complete(job_id):
            await self._handle_job_completion(job_id)

    def _aggregate_job_progress(
        self,
        job: JobInfo,
    ) -> tuple[int, int, float]:
        return self._sub_workflow_progress_totals(self._present_job_sub_workflows(job))

    async def _notify_gate_of_workflow_reassignment(
        self,
        job_id: str,
        workflow_id: str,
        failed_worker_id: str,
        reason: str,
        new_worker_id: str | None,
    ) -> None:
        if not self._is_job_leader(job_id):
            return

        origin_gate_addr = self._manager_state.get_job_origin_gate(job_id)
        if not origin_gate_addr:
            return

        await self._send_reassignment_update_to_gate(
            job_id, origin_gate_addr, workflow_id, failed_worker_id, reason, new_worker_id
        )

    async def _send_reassignment_update_to_gate(
        self,
        job_id: str,
        origin_gate_addr: tuple[str, int],
        workflow_id: str,
        failed_worker_id: str,
        reason: str,
        new_worker_id: str | None,
    ) -> None:
        """Push the job's status with the reassignment message to its origin gate."""
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return

        total_completed, total_failed, overall_rate = self._aggregate_job_progress(job)
        elapsed_seconds = job.elapsed_seconds()

        push = JobStatusPush(
            job_id=job_id,
            status=job.status,
            message=self._workflow_reassignment_message(
                workflow_id, failed_worker_id, reason, new_worker_id
            ),
            total_completed=total_completed,
            total_failed=total_failed,
            overall_rate=overall_rate,
            elapsed_seconds=elapsed_seconds,
            is_final=False,
            fence_token=self._leases.get_fence_token(job_id),
            callback_addr=self._get_job_callback_addr(job_id),
        )

        try:
            response = await self._send_to_peer(
                origin_gate_addr,
                "job_status_push_forward",
                push.dump(),
                timeout=self._config.tcp_timeout_short_seconds,
            )
            self._raise_unless_status_push_forwarded(response)
        except Exception as error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to send reassignment update to gate: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _workflow_reassignment_message(
        self,
        workflow_id: str,
        failed_worker_id: str,
        reason: str,
        new_worker_id: str | None,
    ) -> str:
        """The status message naming the reassigned workflow and its workers."""
        message = (
            f"Workflow {workflow_id[:8]}... reassigned from worker "
            f"{failed_worker_id[:8]}... ({reason})"
        )
        if new_worker_id:
            message = f"{message} -> {new_worker_id[:8]}..."
        return message

    def _raise_unless_status_push_forwarded(self, response: bytes | Exception | None) -> None:
        """Raise the transport error or the gate's rejection of a status push."""
        if isinstance(response, Exception):
            raise response
        if response not in (b"ok", b"stored", None):
            raise RuntimeError(
                f"job_status_push_forward rejected with {response!r}"
            )

    # =========================================================================
    # Failure/Recovery Handlers
    # =========================================================================

    async def _handle_worker_failure(self, worker_id: str, loss_is_charged: bool) -> None:
        """A worker is gone. ``loss_is_charged`` says whether its loss is
        charged to the retry budgets of the workflows it ran (AD-44): an
        unexplained death or an AD-41 over-budget eviction is, our own
        deadline eviction is not -- and nothing is during a systemic hold.
        Only a job's leader decides its workflows' retries; a follower
        mirrors the superseded subs."""
        await self._udp_logger.log(
            ServerError(
                message=(
                    f"[WORKER-FAIL] worker_id={worker_id} "
                    f"worker_count_before={self._manager_state.get_worker_count()} "
                    f"has_worker={self._manager_state.has_worker(worker_id)} "
                    f"all_worker_ids={list(self._manager_state._workers.keys())}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        await self._release_failed_worker(worker_id)

        if self._workflow_dispatcher and self._job_manager:
            await self._reassign_failed_worker_workflows(worker_id, loss_is_charged)

    async def _release_failed_worker(self, worker_id: str) -> None:
        """Fail the worker's health tracking, detach it, and free its cores."""
        if self._manager_state.has_worker(worker_id):
            await self._worker_health_monitor.handle_worker_failure(worker_id)

        self._detach_worker_membership(worker_id)
        if await self._worker_pool.deregister_worker(worker_id):
            await self._worker_pool.notify_cores_available()

    async def _reassign_failed_worker_workflows(self, worker_id: str, loss_is_charged: bool) -> None:
        """Reassign every sub-workflow the failed worker ran (AD-44), then
        broadcast the reassignments."""
        reassignable_sub_workflows = (
            self._job_manager.get_reassignable_sub_workflows_on_worker(worker_id)
        )

        loss_reason = "worker_dead" if loss_is_charged else "worker_evicted"
        for job_id, workflow_id, sub_token in reassignable_sub_workflows:
            await self._reassign_failed_worker_sub_workflow(
                worker_id, loss_reason, loss_is_charged, job_id, workflow_id, sub_token
            )

        await self._broadcast_failed_worker_reassignments(
            worker_id, loss_reason, reassignable_sub_workflows
        )

    async def _reassign_failed_worker_sub_workflow(
        self,
        worker_id: str,
        loss_reason: str,
        loss_is_charged: bool,
        job_id: str,
        workflow_id: str,
        sub_token: str,
    ) -> None:
        """A job's leader decides the sub's retry; a follower mirrors the superseded sub."""
        if not self._leases.is_job_leader(job_id):
            await self._job_manager.apply_workflow_reassignment(
                job_id=job_id,
                workflow_id=workflow_id,
                sub_workflow_token=sub_token,
                failed_worker_id=worker_id,
            )
            return
        await self._apply_workflow_reassignment_state(
            job_id=job_id,
            workflow_id=workflow_id,
            sub_workflow_token=sub_token,
            failed_worker_id=worker_id,
            reason=loss_reason,
            loss_is_charged=loss_is_charged and not self._systemic_eviction_hold,
        )

    async def _broadcast_failed_worker_reassignments(
        self,
        worker_id: str,
        loss_reason: str,
        reassignable_sub_workflows: list[tuple[str, str, str]],
    ) -> None:
        """Broadcast the failed worker's reassignments when there are any."""
        if reassignable_sub_workflows and self._worker_disseminator:
            await self._worker_disseminator.broadcast_workflow_reassignments(
                failed_worker_id=worker_id,
                reason=loss_reason,
                reassignments=reassignable_sub_workflows,
            )

        # Losing the last worker fails nothing more: the workflows it held
        # went back to PENDING above (or failed for good when their retry
        # budget was spent), and a datacenter without workers is a capacity
        # wait like any other -- a worker joining dispatches them, and the
        # job's deadline (AD-34) fails them loudly if none does.

    def _detach_worker_membership(
        self, worker_id: str, reason: str = "worker_failure"
    ) -> None:
        """Remove a dead worker from membership indexes immediately."""
        self._deregister_worker_with_notice(worker_id, reason)
        self._manager_state._worker_lhm_scores.pop(worker_id, None)

    def _deregister_worker_with_notice(self, worker_id: str, reason: str) -> None:
        """Deregister a worker AND record the obligation to tell it so.

        Deregistration was one-sided: the manager forgot the worker but
        kept acking its SWIM probes, so a live (e.g. wedged-then-
        recovered) worker never learned it was dropped and never
        re-registered — silent permanent divergence. Every failure/reap
        deregistration now records a notice obligation and pushes a
        ``WorkerEvictionNotice`` immediately; ``_dead_node_reap_loop``
        re-sends it with capped backoff until the worker acks or
        re-registers (both discharge it). Plain re-registration
        overwrites and sync mirrors deliberately do NOT come through
        here — the worker is present in the first case, and the
        deciding manager owns the obligation in the second.
        """
        registration = self._manager_state.get_worker(worker_id)
        self._registry.unregister_worker(worker_id)
        if registration is None:
            return

        now = self._clock.monotonic()
        self._manager_state.record_eviction_notice(
            WorkerEvictionNoticeState(
                worker_id=worker_id,
                worker_tcp_addr=(
                    registration.node.host,
                    registration.node.port,
                ),
                reason=reason,
                evicted_at=now,
                last_notice_at=now,
            )
        )
        self._task_runner.run(self._send_eviction_notice, worker_id)

    async def _send_eviction_notice(self, worker_id: str) -> None:
        """Send one ``WorkerEvictionNotice`` for an outstanding obligation.

        An ack discharges the obligation (the worker now KNOWS); a
        failed or unanswered send leaves it outstanding for the
        re-send pass in ``_dead_node_reap_loop``.
        """
        notice_state = self._manager_state.get_eviction_notice(worker_id)
        if notice_state is None:
            return

        notice_state.last_notice_at = self._clock.monotonic()
        notice_state.notice_count += 1

        notice = WorkerEvictionNotice(
            manager_id=self._node_id.full,
            manager_tcp_host=self._host,
            manager_tcp_port=self._tcp_port,
            worker_id=worker_id,
            reason=notice_state.reason,
        )
        try:
            response, _clock = await self.send_tcp(
                notice_state.worker_tcp_addr,
                "eviction_notice",
                notice.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
            self._apply_eviction_notice_reply(worker_id, response)
        except Exception as send_error:
            await self._udp_logger.log(
                ServerDebug(
                    message=(
                        f"Eviction notice to worker {worker_id[:8]}... at "
                        f"{notice_state.worker_tcp_addr} not delivered "
                        f"(attempt {notice_state.notice_count}): {send_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _apply_eviction_notice_reply(
        self,
        worker_id: str,
        response: bytes | Exception | None,
    ) -> None:
        """Discharge the notice on the worker's ack; raises a transport error."""
        # send_tcp returns transport errors rather than raising.
        if isinstance(response, Exception):
            raise response
        if not response:
            return
        self._clear_acknowledged_eviction_notice(worker_id, response)

    def _clear_acknowledged_eviction_notice(self, worker_id: str, response: bytes) -> None:
        """Clear the obligation when the ack names this worker."""
        ack = WorkerEvictionNoticeAck.load(response)
        if ack.worker_id == worker_id:
            self._manager_state.clear_eviction_notice(worker_id)

    def _resend_eviction_notices(self, now: float) -> None:
        """Re-send outstanding eviction notices on capped exponential
        backoff — the state-based backstop for a worker that was wedged
        through the initial push (the usual reason it was evicted) and
        recovered later."""
        base = self._config.eviction_notice_base_interval_seconds
        cap = self._config.eviction_notice_max_interval_seconds
        for notice_state in self._manager_state.iter_eviction_notices():
            backoff = min(
                base * (2 ** max(0, notice_state.notice_count - 1)),
                cap,
            )
            if now - notice_state.last_notice_at >= backoff:
                self._task_runner.run(
                    self._send_eviction_notice, notice_state.worker_id
                )

    def _get_registered_node_id_for_addr(self, addr: tuple[str, int]) -> str | None:
        """Return the registered node identity currently bound to ``addr``."""
        if worker_id := self._manager_state.get_worker_id_from_addr(addr):
            return worker_id

        if (peer_id := self._registered_manager_peer_id_for_udp_addr(addr)) is not None:
            return peer_id

        return self._registered_gate_id_for_udp_addr(addr)

    def _registered_manager_peer_id_for_udp_addr(self, addr: tuple[str, int]) -> str | None:
        """The known manager peer bound to the UDP address, when it is mapped."""
        if not self._manager_state.get_manager_tcp_from_udp(addr):
            return None
        return self._node_id_with_udp_addr(self._manager_state.iter_known_manager_peers(), addr)

    def _registered_gate_id_for_udp_addr(self, addr: tuple[str, int]) -> str | None:
        """The known gate bound to the UDP address, when it is mapped."""
        if not self._manager_state.get_gate_tcp_from_udp(addr):
            return None
        return self._node_id_with_udp_addr(self._manager_state.iter_known_gates(), addr)

    def _node_id_with_udp_addr(
        self,
        known_nodes: Iterable[tuple[str, ManagerInfo | GateInfo]],
        addr: tuple[str, int],
    ) -> str | None:
        """The id of the first known node whose UDP address is ``addr``."""
        for node_id, node_info in known_nodes:
            if (node_info.udp_host, node_info.udp_port) == addr:
                return node_id
        return None

    async def _handle_manager_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        node_state = self._incarnation_tracker.get_node_state(udp_addr)
        if node_state is None or node_state.status != b"DEAD":
            return

        # Resolve the peer's node_id from the address so we can update
        # both the address-keyed and id-keyed indices atomically. Without
        # the id, ``_active_manager_peer_ids`` keeps the dead peer until
        # the 120 s reap window elapses — too coarse for any liveness
        # check that needs sub-minute reaction.
        peer_id_for_addr = self._known_manager_peer_id_for_tcp_addr(tcp_addr)

        await self._mark_manager_peer_dead(tcp_addr, peer_id_for_addr)

        # Start the unhealthy-since clock so the reap path can later
        # fully unregister the peer (releasing peer-locks, latency
        # samples, etc.) once the reap interval elapses.
        self._start_manager_peer_unhealthy_clock(peer_id_for_addr)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager peer {tcp_addr} marked DEAD",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        await self._handle_job_leader_failure(tcp_addr)
        await self._leadership.check_quorum_status()

    def _known_manager_peer_id_for_tcp_addr(self, tcp_addr: tuple[str, int]) -> str | None:
        """The id of the first known manager peer at ``tcp_addr``, if any."""
        return next(
            (
                peer_id
                for peer_id, info in self._manager_state.iter_known_manager_peers()
                if (info.tcp_host, info.tcp_port) == tcp_addr
            ),
            None,
        )

    async def _mark_manager_peer_dead(
        self,
        tcp_addr: tuple[str, int],
        peer_id_for_addr: str | None,
    ) -> None:
        """Under the peer's state lock: bump its epoch, drop it from the active
        indices and record it dead."""
        peer_lock = await self._manager_state.get_peer_state_lock(tcp_addr)
        async with peer_lock:
            self._manager_state.increment_peer_state_epoch(tcp_addr)
            # ``remove_active_peer`` updates both the address-keyed and
            # id-keyed indices atomically under the state's counter
            # lock, keeping ``_active_manager_peers`` and
            # ``_active_manager_peer_ids`` from drifting out of sync.
            # When the peer_id can't be resolved (peer was never
            # registered), fall back to address-only removal — the
            # reaper still cleans up the id set on its next pass.
            if peer_id_for_addr is not None:
                await self._manager_state.remove_active_peer(
                    tcp_addr, peer_id_for_addr
                )
            else:
                self._manager_state.remove_active_manager_peer(tcp_addr)
            self._manager_state.add_dead_manager(tcp_addr, self._clock.monotonic())

    def _start_manager_peer_unhealthy_clock(self, peer_id_for_addr: str | None) -> None:
        """Start the resolved peer's unhealthy-since clock for the reap path."""
        if peer_id_for_addr is not None:
            self._manager_state.set_manager_peer_unhealthy_since(
                peer_id_for_addr, self._clock.monotonic()
            )

    async def _handle_manager_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        peer_lock = await self._manager_state.get_peer_state_lock(tcp_addr)

        async with peer_lock:
            initial_epoch = self._manager_state.get_peer_state_epoch(tcp_addr)

        async with self._recovery_semaphore:
            readmitted = await self._readmit_recovered_manager_peer(
                tcp_addr, peer_lock, initial_epoch
            )

        if not readmitted:
            return

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager peer {tcp_addr} REJOINED (verified)",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _readmit_recovered_manager_peer(
        self,
        tcp_addr: tuple[str, int],
        peer_lock: asyncio.Lock,
        initial_epoch: int,
    ) -> bool:
        """After a jittered pause, verify the recovered peer and re-add it unless
        its state epoch moved (caller holds the recovery semaphore)."""
        jitter = self._random.uniform(
            self._config.recovery_jitter_min_seconds,
            self._config.recovery_jitter_max_seconds,
        )
        await self._clock.sleep(jitter)

        async with peer_lock:
            current_epoch = self._manager_state.get_peer_state_epoch(tcp_addr)
            if current_epoch != initial_epoch:
                return False

        verification_success = await self._verify_peer_recovery(tcp_addr)
        if not verification_success:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Manager peer {tcp_addr} recovery verification failed, not re-adding",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

        # Resolve the peer's id once outside the lock so the
        # mirrored-set updates inside the critical section are
        # straight-line.
        peer_id_for_addr = self._known_manager_peer_id_for_tcp_addr(tcp_addr)

        return await self._readmit_verified_manager_peer(
            tcp_addr, peer_lock, initial_epoch, peer_id_for_addr
        )

    async def _readmit_verified_manager_peer(
        self,
        tcp_addr: tuple[str, int],
        peer_lock: asyncio.Lock,
        initial_epoch: int,
        peer_id_for_addr: str | None,
    ) -> bool:
        """Under the peer's state lock, re-add it to the active indices and clear
        its dead mark; False when its epoch moved meanwhile."""
        async with peer_lock:
            current_epoch = self._manager_state.get_peer_state_epoch(tcp_addr)
            if current_epoch != initial_epoch:
                return False

            # ``add_active_peer`` updates both the address-keyed
            # and id-keyed indices atomically; the failure path
            # cleared the id set, and using the same paired API
            # here keeps the two indices consistent. The
            # unhealthy-since clock cleanup happens outside the
            # counter lock since it's tracked in a different
            # dictionary.
            if peer_id_for_addr is not None:
                await self._manager_state.add_active_peer(
                    tcp_addr, peer_id_for_addr
                )
                self._manager_state.clear_manager_peer_unhealthy_since(
                    peer_id_for_addr
                )
            else:
                self._manager_state.add_active_manager_peer(tcp_addr)
            self._manager_state.remove_dead_manager(tcp_addr)
        return True

    async def _verify_peer_recovery(self, tcp_addr: tuple[str, int]) -> bool:
        try:
            # ``PingRequest`` is keyed by ``request_id`` — the prior
            # ``requester_id`` argument raised ``TypeError`` before the
            # network call ever happened, and the broad except below
            # silently swallowed the failure. Result: ``verify_peer_recovery``
            # always returned False, the manager peer-recovery handler
            # always early-returned, and rejoining peers were never re-
            # added to ``_active_manager_peer_ids`` even after SWIM
            # confirmed their liveness.
            ping_request = PingRequest(request_id=self._node_id.full)
            response = await self._clock.wait_for(
                self._send_to_peer(
                    tcp_addr,
                    "ping",
                    ping_request.dump(),
                    self._config.tcp_timeout_short_seconds,
                ),
                timeout=self._config.tcp_timeout_short_seconds + 1.0,
            )
            return self._peer_recovery_ping_answered(response)
        except asyncio.TimeoutError:
            return False
        except Exception as verify_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Manager peer {tcp_addr} ping verification raised "
                        f"{type(verify_error).__name__}: {verify_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

    def _peer_recovery_ping_answered(self, response: bytes | Exception | None) -> bool:
        """True for a non-error ping reply; raises a transport error."""
        # send_tcp returns transport errors rather than raising.
        if isinstance(response, Exception):
            raise response
        return response is not None and response != b"error"

    async def _handle_gate_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer failure."""
        # Find gate by address
        gate_node_id = self._known_gate_id_at_tcp_addr(tcp_addr)

        if gate_node_id:
            self._mark_failed_gate_unhealthy(gate_node_id)

    def _known_gate_id_at_tcp_addr(self, tcp_addr: tuple[str, int]) -> str | None:
        """The id of the first known gate at ``tcp_addr``, if any."""
        for gate_id, gate_info in self._manager_state.iter_known_gates():
            if (gate_info.tcp_host, gate_info.tcp_port) == tcp_addr:
                return gate_id
        return None

    def _mark_failed_gate_unhealthy(self, gate_node_id: str) -> None:
        """Mark the gate unhealthy and move the primary off it when it was primary."""
        self._registry.mark_gate_unhealthy(gate_node_id)

        if self._manager_state.primary_gate_id == gate_node_id:
            self._manager_state.set_primary_gate_id(
                self._manager_state.get_first_healthy_gate_id()
            )

    async def _handle_gate_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer recovery."""
        for gate_id, gate_info in self._manager_state.iter_known_gates():
            if self._node_info_shares_address(gate_info, tcp_addr, udp_addr):
                self._registry.mark_gate_healthy(gate_id)
                break

    async def _handle_job_leader_failure(self, failed_addr: tuple[str, int]) -> None:
        """Handle job leader manager failure.

        Job leadership is per-job during normal operation. When a job leader
        dies, the SWIM cluster leader is the only node allowed to advance the
        fenced leadership epoch and publish the takeover.
        """
        if not self.is_leader():
            return

        jobs_to_takeover = self._jobs_led_from(failed_addr)

        for job_id in jobs_to_takeover:
            await self._take_over_job_of_failed_leader(job_id)

    def _jobs_led_from(self, leader_addr_to_match: tuple[str, int]) -> list[str]:
        """The jobs whose recorded leader address is ``leader_addr_to_match``."""
        return [
            job_id
            for job_id, leader_addr in self._manager_state.iter_job_leader_addrs()
            if leader_addr == leader_addr_to_match
        ]

    async def _take_over_job_of_failed_leader(self, job_id: str) -> None:
        """Take the job's leadership over as cluster leader; log a takeover."""
        old_leader_id = self._leases.get_job_leader(job_id)
        taken_over = await self._take_over_job_leadership_as_cluster_leader(
            job_id,
            old_leader_id,
        )
        if not taken_over:
            return

        await self._udp_logger.log(
            ServerInfo(
                message=f"Took over leadership for job {job_id[:8]}... as SWIM cluster leader",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _take_over_job_leadership_as_cluster_leader(
        self,
        job_id: str,
        old_leader_id: str | None,
    ) -> bool:
        """Advance a job's fenced leadership epoch as the SWIM cluster leader."""
        if not await self._may_take_over_job_leadership(job_id):
            return False

        peers_answered = await self._state_sync.sync_state_from_manager_peers(
            force_full=True
        )
        verdict, job = await self._job_takeover_verdict(job_id, old_leader_id, peers_answered)
        if verdict is not None:
            return verdict
        return await self._claim_job_leadership_takeover(job_id, job, old_leader_id)

    async def _may_take_over_job_leadership(self, job_id: str) -> bool:
        """Whether this manager is the cluster leader with manager quorum; a missing quorum is logged."""
        if not self.is_leader():
            return False
        if not self._leadership.has_quorum():
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Cannot take over job {job_id[:8]}... without manager quorum"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False
        return True

    async def _job_takeover_verdict(
        self,
        job_id: str,
        old_leader_id: str | None,
        peers_answered: int,
    ) -> tuple[bool | None, JobInfo | None]:
        """
        Decide whether a job's takeover is settled before any claim is made.

        Returns (verdict, None) when the takeover is already decided -- True
        when this manager leads the job, False when it may not or need not
        take it over -- and (None, job) when the job is to be claimed. A job
        that ended or was never admitted is settled here rather than claimed.
        """
        if (verdict := self._job_takeover_claim_verdict(job_id, old_leader_id)) is not None:
            return verdict, None
        job = self._job_manager.get_job_by_id(job_id)
        if not await self._job_still_needs_takeover(job_id, job, peers_answered):
            return False, None
        return None, job

    def _job_takeover_claim_verdict(self, job_id: str, old_leader_id: str | None) -> bool | None:
        """
        Return True when this manager already leads the job, False when the claim is blocked, else None.

        The claim is blocked while another manager than the dead one leads
        the job, or while the job's consensus group has not settled.
        """
        current_leader_id = self._leases.get_job_leader(job_id)
        if self._already_leads_job(current_leader_id, job_id):
            return True
        if self._job_takeover_blocked(job_id, current_leader_id, old_leader_id):
            return False
        return None

    def _already_leads_job(self, current_leader_id: str | None, job_id: str) -> bool:
        """Whether the job's recorded leader is this manager and this manager holds the job's leadership."""
        return current_leader_id == self._node_id.full and self._leases.is_job_leader(job_id)

    def _job_takeover_blocked(
        self,
        job_id: str,
        current_leader_id: str | None,
        old_leader_id: str | None,
    ) -> bool:
        """Whether another live manager leads the job, or the job's consensus group is not yet settled."""
        return self._job_led_by_another_manager(
            current_leader_id, old_leader_id
        ) or self._job_group_unsettled(job_id, old_leader_id)

    def _job_led_by_another_manager(
        self,
        current_leader_id: str | None,
        old_leader_id: str | None,
    ) -> bool:
        """Whether the job's recorded leader is neither this manager nor the leader being taken over from."""
        return (
            current_leader_id is not None
            and current_leader_id != self._node_id.full
            and self._differs_from_old_leader(current_leader_id, old_leader_id)
        )

    @staticmethod
    def _differs_from_old_leader(current_leader_id: str, old_leader_id: str | None) -> bool:
        """Whether a job's recorded leader is not the old leader -- always so when no old leader is named."""
        return old_leader_id is None or current_leader_id != old_leader_id

    def _job_group_unsettled(self, job_id: str, old_leader_id: str | None) -> bool:
        """
        Whether the job's consensus group has yet to settle on what its dead leader last recorded.

        The group settles once it has a new group leader and every entry held
        here is applied: an end the leader committed sits unapplied here
        until then, and the job would read as running. The orphan scan
        retries.
        """
        job_group = self._raft.consensus.get_node(job_id)
        return job_group is not None and (
            job_group.current_leader in (None, old_leader_id)
            or job_group.last_applied_index < job_group.last_log_index
        )

    async def _job_still_needs_takeover(
        self,
        job_id: str,
        job: JobInfo | None,
        peers_answered: int,
    ) -> bool:
        """
        Settle a job that ended or was never admitted, and report whether it still needs a takeover.

        An ended job's leader died after finishing it, before every member
        heard: taken over, it re-ran what its copy still showed unfinished.
        A never-admitted job's leader announced it and died before
        replicating it to a quorum, so its submitter was never told it was
        accepted: taken over, it had nothing to run and "completed".
        """
        status_order = JobStatusOrder()
        replicated_state = self._ledger_replica.job_state(job_id)
        if self._job_ended(replicated_state, job, status_order):
            await self._settle_ended_job_copy(job_id, job, replicated_state, status_order)
            return False
        if self._is_unadmitted_job_copy(job):
            await self._settle_unadmitted_job_copy(job_id, peers_answered)
            return False
        return True

    @classmethod
    def _job_ended(
        cls,
        replicated_state: JobState | None,
        job: JobInfo | None,
        status_order: JobStatusOrder,
    ):
        """Whether the replicated ledger or the job held here shows the job terminal."""
        return cls._replicated_state_is_terminal(replicated_state) or cls._job_is_terminal(
            job, status_order
        )

    @staticmethod
    def _replicated_state_is_terminal(replicated_state: JobState | None):
        """Whether the replicated ledger holds the job in a terminal state."""
        return replicated_state is not None and replicated_state.is_terminal

    @staticmethod
    def _job_is_terminal(job: JobInfo | None, status_order: JobStatusOrder):
        """Whether the job is held here with a terminal status."""
        return job is not None and status_order.is_terminal(job.status)

    @staticmethod
    def _is_unadmitted_job_copy(job: JobInfo | None) -> bool:
        """Whether the job is unknown here or holds no workflows -- it was never admitted."""
        return job is None or not job.workflows

    async def _settle_ended_job_copy(
        self,
        job_id: str,
        job: JobInfo | None,
        replicated_state: JobState | None,
        status_order: JobStatusOrder,
    ) -> None:
        """End a live copy of a job the replicated ledger shows terminal, and destroy the job's group."""
        if job is None or status_order.is_terminal(job.status):
            return
        await self._end_job_copy_at_clock_time(job, replicated_state.status)
        await self._raft.consensus.destroy_job_raft(job_id)

    async def _end_job_copy_at_clock_time(self, job: JobInfo, status: str) -> None:
        """Under the job's lock, set its status and stamp its completion with the clock's time when it has none."""
        async with job.lock:
            job.status = status
            if job.completed_at <= 0:
                job.completed_at = self._clock.time()

    async def _settle_unadmitted_job_copy(self, job_id: str, peers_answered: int) -> None:
        """
        Clean up a never-admitted job's state when a quorum answered the state sync.

        Settled on a quorum's word only -- an unheard member may hold the
        replicated job.
        """
        if peers_answered + 1 >= self._leadership.get_quorum_size():
            await self._cleanup_job_state(job_id)

    async def _claim_job_leadership_takeover(
        self,
        job_id: str,
        job: JobInfo | None,
        old_leader_id: str | None,
    ) -> bool:
        """
        Claim a job's leadership at the next fencing token through a quorum, then assume it.

        Returns False when the claim did not reach a quorum or the lease
        coordinator refused it, and True once this manager leads the job.
        """
        next_fencing_token = max(2, self._leases.get_fence_token(job_id) + 1)
        takeover_claim = self._build_job_state_sync_message(
            job_id,
            job,
            leader_id=self._node_id.full,
            leader_addr=(self._host, self._tcp_port),
            fencing_token=next_fencing_token,
            replace_existing=job is not None,
        )
        replicated = await self._sync_job_state_message_to_peers(
            takeover_claim,
            require_quorum=True,
        )
        if not replicated:
            return False

        accepted = self._leases.apply_job_leadership(
            job_id=job_id,
            leader_id=self._node_id.full,
            leader_addr=(self._host, self._tcp_port),
            fencing_token=next_fencing_token,
        )
        if not accepted:
            return False

        await self._assume_taken_over_job_leadership(job_id, old_leader_id, next_fencing_token)
        return True

    async def _assume_taken_over_job_leadership(
        self,
        job_id: str,
        old_leader_id: str | None,
        next_fencing_token: int,
    ) -> None:
        """
        Act as a taken-over job's leader: hydrate and adopt its state, announce it, and tell its gate and workers.

        Phase F4: the job inherits AD-26 H7/H8 state from the previous
        leader's persisted TimeoutTrackingState.
        """
        await self._hydrate_job_state_for_takeover(job_id)
        await self._adopt_replicated_ledger_history(job_id, old_leader_id, next_fencing_token)

        workflow_names = self._stamp_taken_over_job_leadership(job_id, next_fencing_token)

        await self._manager_state.increment_state_version()
        await self._broadcast_job_leadership(
            job_id,
            len(workflow_names),
            workflow_names,
            callback_addr=self._manager_state.get_job_callback(job_id),
            origin_gate_addr=self._manager_state.get_job_origin_gate(job_id),
        )
        await self._notify_origin_gate_job_leader_transfer(
            job_id,
            old_leader_id,
            next_fencing_token,
        )
        await self._notify_workers_job_leader_transfer(job_id, old_leader_id)
        self._replay_extension_state_for_job(job_id)

    def _stamp_taken_over_job_leadership(self, job_id: str, next_fencing_token: int) -> list[str]:
        """Record this manager as the job's leader at the new fencing token; returns the job's workflow names."""
        job = self._job_manager.get_job_by_id(job_id)
        if job is None:
            return []
        job.leader_node_id = self._node_id.full
        job.leader_addr = (self._host, self._tcp_port)
        job.fencing_token = next_fencing_token
        return [
            workflow.name for workflow in job.workflows.values()
        ]

    async def _adopt_replicated_ledger_history(
        self, job_id: str, previous_leader_id: str | None, lease_fence_token: int
    ) -> None:
        """AD-38: take over the job's ledger record along with the job, and
        record the takeover in it (``JobLeadershipAcquired``).

        The previous leader's ledger held the job; this member mirrored
        every REGIONAL entry of it through the job's Raft group. Without
        adopting that history here, this ledger does not know the job and
        every later event -- the terminal included -- appends nothing, so
        the job's outcome is never durably recorded anywhere.
        """
        if self._job_ledger is None:
            return

        if self._ledger_replica.job_state(job_id) is None:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Taking over job {job_id[:8]}... with no replicated "
                        "JobCreated; its ledger record cannot be adopted"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return

        adopted = await self._job_ledger.adopt_replicated_history(
            job_id, self._ledger_replica.history(job_id)
        )
        await self._job_ledger.record_leadership_acquired(
            job_id, self._node_id.full, previous_leader_id, lease_fence_token
        )
        await self._udp_logger.log(
            ServerInfo(
                message=(
                    f"Adopted {adopted} replicated ledger events for taken-over "
                    f"job {job_id[:8]}..."
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _hydrate_job_state_for_takeover(self, job_id: str) -> None:
        """Hydrate executable job state before serving a taken-over job."""
        await self._state_sync.sync_state_from_manager_peers(force_full=True)
        await self._state_sync.sync_state_from_workers()

        job = self._job_manager.get_job_by_id(job_id)
        if job is None:
            return

        await self._sync_job_state_to_peers(
            job_id,
            job,
            require_quorum=False,
        )

    async def _notify_origin_gate_job_leader_transfer(
        self,
        job_id: str,
        old_leader_id: str | None,
        fencing_token: int,
    ) -> None:
        """Notify the origin gate that this manager now leads the job."""
        origin_gate_addr = self._manager_state.get_job_origin_gate(job_id)
        if origin_gate_addr is None:
            return

        transfer = JobLeaderManagerTransfer(
            job_id=job_id,
            datacenter_id=self._node_id.datacenter,
            new_manager_id=self._node_id.full,
            new_manager_addr=(self._host, self._tcp_port),
            fence_token=fencing_token,
            old_manager_id=old_leader_id,
        )
        try:
            response, _clock_time = await self.send_tcp(
                origin_gate_addr,
                "job_leader_manager_transfer",
                transfer.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
            await self._check_origin_gate_transfer_reply(job_id, origin_gate_addr, response)
        except Exception as transfer_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Failed to notify origin gate {origin_gate_addr} about manager "
                        f"leader transfer for job {job_id[:8]}...: {transfer_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _check_origin_gate_transfer_reply(
        self,
        job_id: str,
        origin_gate_addr: tuple[str, int],
        response: bytes | Exception | None,
    ) -> None:
        """Check the origin gate's transfer ack; raises a transport error."""
        # send_tcp returns transport errors rather than raising.
        if isinstance(response, Exception):
            raise response
        if not response:
            return
        await self._log_origin_gate_transfer_rejection(job_id, origin_gate_addr, response)

    async def _log_origin_gate_transfer_rejection(
        self,
        job_id: str,
        origin_gate_addr: tuple[str, int],
        response: bytes,
    ) -> None:
        """Log the origin gate's rejection of the transfer, if it rejected it."""
        ack = JobLeaderManagerTransferAck.load(response)
        if ack.accepted:
            return

        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"Origin gate {origin_gate_addr} rejected manager leader "
                    f"transfer for job {job_id[:8]}..."
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    def _step_down_from_cluster_leadership(self) -> None:
        self._task_runner.run(self._leader_election._step_down)

    def _should_backup_orphan_scan(self) -> bool:
        if self.is_leader():
            return False

        leader_addr = self._leader_election.state.current_leader
        if leader_addr is None:
            return True

        leader_last_seen = self._leader_election.state.last_heartbeat_time
        leader_timeout = self._config.orphan_scan_interval_seconds * 3
        return (self._clock.monotonic() - leader_last_seen) > leader_timeout

    # =========================================================================
    # Heartbeat Handlers
    # =========================================================================

    async def _handle_embedded_worker_heartbeat(
        self,
        heartbeat: WorkerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        await self._worker_health_monitor.handle_worker_heartbeat(heartbeat, source_addr)

        worker_id = heartbeat.node_id
        await self._route_embedded_worker_heartbeat(worker_id, heartbeat)

        # SWIM-confirm the worker so failure detection actually engages.
        # Without this, ``can_suspect_node`` (AD-29) blocks all attempts to
        # SUSPECT the worker because the manager never marked it as a
        # confirmed peer, and detection falls back to the coarse
        # deadline-enforcement loop. Manager-peer and gate heartbeat
        # handlers already do this; the worker handler must match.
        await self.confirm_peer(source_addr)

        # AD-26 heartbeat-piggyback extension request. The worker
        # carries ``extension_requested`` plus the supporting metric
        # fields on every heartbeat once its ExtensionTrigger has
        # marked an extension as pending; without this branch the
        # piggyback was load-bearing on paper only — the manager
        # never processed it, so AD-26's whole point (workers self-
        # reporting they need bracket room) failed silently and the
        # SWIM-vs-workload contention window produced false-positive
        # deaths on live workers. The same shared processor as the
        # TCP ``extension_request`` endpoint runs here, so the H5
        # multi-witness route, deadline write, SWIM-bracket extension
        # and timeout-strategy notification all fire end-to-end.
        if getattr(heartbeat, "extension_requested", False):
            await self._process_piggybacked_extension_request(worker_id, heartbeat)

    async def _route_embedded_worker_heartbeat(
        self,
        worker_id: str,
        heartbeat: WorkerHeartbeat,
    ) -> None:
        """Feed a known worker's heartbeat to the pool; nudge an unknown one."""
        if self._manager_state.has_worker(worker_id):
            await self._worker_pool.process_heartbeat(worker_id, heartbeat)
            return

        await self._nudge_addressable_unknown_worker(worker_id, heartbeat)

    async def _nudge_addressable_unknown_worker(
        self,
        worker_id: str,
        heartbeat: WorkerHeartbeat,
    ) -> None:
        """Nudge an unknown heartbeating worker to re-register when it gave a TCP address."""
        if heartbeat.tcp_host and heartbeat.tcp_port:
            # A worker heartbeating a manager that does not know it is
            # the RESTART signature from the manager's side: the worker
            # registered with a previous generation, the down window
            # was shorter than its death bound, so it never re-
            # registered — and an idle worker has no failure path to
            # trigger one. Nudge it through the existing eviction-
            # notice channel (its handler does a targeted
            # re-registration).
            await self._nudge_unknown_heartbeating_worker(
                worker_id, (heartbeat.tcp_host, heartbeat.tcp_port)
            )

    async def _process_piggybacked_extension_request(
        self,
        worker_id: str,
        heartbeat: WorkerHeartbeat,
    ) -> None:
        """Process the heartbeat's AD-26 extension request and push the decision back."""
        worker = self._manager_state.get_worker(worker_id)
        if worker is None:
            return

        request = HealthcheckExtensionRequest(
            worker_id=worker_id,
            reason=heartbeat.extension_reason or "heartbeat-piggyback",
            current_progress=heartbeat.extension_current_progress,
            estimated_completion=heartbeat.extension_estimated_completion,
            active_workflow_count=heartbeat.extension_active_workflow_count,
            completed_items=heartbeat.extension_completed_items,
            total_items=heartbeat.extension_total_items,
            workflow_id=heartbeat.extension_workflow_id,
            step_transitions=heartbeat.extension_step_transitions,
            actions_completed=heartbeat.extension_actions_completed,
            snapshot_time=heartbeat.extension_snapshot_time,
        )
        response = await self._process_extension_request_core(
            request, worker_id, worker
        )
        await self._push_extension_decision_to_worker(worker_id, worker, response)

    async def _push_extension_decision_to_worker(
        self,
        worker_id: str,
        worker: WorkerRegistration,
        response: HealthcheckExtensionResponse,
    ) -> None:
        """Push the extension decision so the worker clears its request latch (AD-26)."""
        # Close the piggyback loop: push the decision back so
        # the worker CLEARS its request latch (the TCP endpoint
        # returns the response inline; the heartbeat path has
        # no reply channel, and discarding the response left
        # the latch frozen forever — the perpetual denial
        # stream + dead autonomous trigger). Best-effort: a
        # lost push self-heals because the still-latched worker
        # re-carries the request on its next heartbeat and this
        # branch re-responds.
        try:
            push_reply, _ = await self.send_tcp(
                (worker.node.host, worker.node.port),
                "extension_response",
                response.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(push_reply, Exception):
                raise push_reply
        except Exception as send_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Extension decision push to worker "
                        f"{worker_id[:8]}... failed "
                        f"({type(send_error).__name__}); the "
                        "worker re-carries the request next "
                        "heartbeat"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _nudge_unknown_heartbeating_worker(
        self,
        worker_id: str,
        worker_tcp_addr: tuple[str, int],
    ) -> None:
        """Ask an unknown-but-heartbeating worker to re-register.

        Rate-limited per worker (one nudge per window) — heartbeats
        arrive every probe round and re-registration takes a few
        seconds. Best-effort: a failed send just waits for the next
        heartbeat. The map is pruned on success and bounded by the
        number of distinct heartbeating workers.
        """
        now = self._clock.monotonic()
        next_allowed = self._unknown_worker_nudges.get(worker_id, 0.0)
        if now < next_allowed:
            return
        self._unknown_worker_nudges[worker_id] = now + 10.0

        notice = WorkerEvictionNotice(
            manager_id=self._node_id.full,
            manager_tcp_host=self._host,
            manager_tcp_port=self._tcp_port,
            worker_id=worker_id,
            reason="unknown_worker_heartbeat (manager restarted?)",
        )
        await self._send_reregister_nudge(worker_id, worker_tcp_addr, notice)

    async def _send_reregister_nudge(
        self,
        worker_id: str,
        worker_tcp_addr: tuple[str, int],
        notice: WorkerEvictionNotice,
    ) -> None:
        """Send the re-register nudge; log the outcome."""
        try:
            response, _ = await self.send_tcp(
                worker_tcp_addr,
                "eviction_notice",
                notice.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response, Exception):
                raise response
            await self._udp_logger.log(
                ServerInfo(
                    message=(
                        f"Nudged unknown heartbeating worker "
                        f"{worker_id[:8]}... at {worker_tcp_addr} to "
                        "re-register"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        except Exception as nudge_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Re-register nudge to {worker_id[:8]}... at "
                        f"{worker_tcp_addr} failed: {nudge_error} (next "
                        "heartbeat retries)"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _handle_manager_peer_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        peer_id = heartbeat.node_id
        peer_info = self._manager_info_from_heartbeat(heartbeat, source_addr)
        ingested = await self._ingest_manager_peer_info(
            peer_info,
            authoritative_registration=False,
        )
        if not ingested and not self._manager_state.has_known_manager_peer(peer_id):
            return

        await self._apply_known_manager_peer_heartbeat(peer_id, heartbeat, source_addr)

    def _manager_info_from_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> ManagerInfo:
        """The peer's ManagerInfo from its heartbeat, its TCP address defaulted
        from the UDP source (port - 1)."""
        return ManagerInfo(
            node_id=heartbeat.node_id,
            tcp_host=heartbeat.tcp_host or source_addr[0],
            tcp_port=heartbeat.tcp_port or source_addr[1] - 1,
            udp_host=source_addr[0],
            udp_port=source_addr[1],
            datacenter=heartbeat.datacenter,
            is_leader=heartbeat.is_leader,
        )

    async def _apply_known_manager_peer_heartbeat(
        self,
        peer_id: str,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """Record the known peer's leadership and health, then SWIM-confirm it."""
        if heartbeat.is_leader:
            self._manager_state.set_dc_leader_manager_id(peer_id)

        peer_health_state = getattr(heartbeat, "health_overload_state", "healthy")
        previous_peer_state = (
            await self._manager_state.update_peer_manager_health_state(
                peer_id,
                peer_health_state,
            )
        )

        await self._note_peer_manager_health_change(peer_id, previous_peer_state, peer_health_state)

        await self.confirm_peer(source_addr)

    async def _note_peer_manager_health_change(
        self,
        peer_id: str,
        previous_peer_state: str | None,
        peer_health_state: str,
    ) -> None:
        """Log a peer manager's health transition and re-check the peer alerts."""
        if previous_peer_state and previous_peer_state != peer_health_state:
            await self._log_peer_manager_health_transition(
                peer_id, previous_peer_state, peer_health_state
            )
            await self._worker_health_monitor.check_peer_manager_health_alerts()

    async def _handle_gate_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """Handle embedded gate heartbeat from SWIM."""
        gate_id = heartbeat.node_id

        # Register gate if not known
        if not self._manager_state.get_known_gate(gate_id):
            await self._registry.register_gate(
                self._gate_info_from_heartbeat(heartbeat, source_addr)
            )

        # Update gate leader tracking
        if heartbeat.is_leader:
            self._track_current_gate_leader(gate_id)

        # Confirm peer
        await self.confirm_peer(source_addr)

    def _gate_info_from_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        source_addr: tuple[str, int],
    ) -> GateInfo:
        """The gate's GateInfo from its heartbeat, its TCP address defaulted
        from the UDP source (port - 1)."""
        return GateInfo(
            node_id=heartbeat.node_id,
            tcp_host=heartbeat.tcp_host or source_addr[0],
            tcp_port=heartbeat.tcp_port or source_addr[1] - 1,
            udp_host=source_addr[0],
            udp_port=source_addr[1],
            datacenter=heartbeat.datacenter,
            is_leader=heartbeat.is_leader,
        )

    def _track_current_gate_leader(self, gate_id: str) -> None:
        """Record the leading gate with its TCP address (None when it is unknown)."""
        gate_info = self._manager_state.get_known_gate(gate_id)
        if gate_info:
            self._manager_state.set_current_gate_leader(
                gate_id, (gate_info.tcp_host, gate_info.tcp_port)
            )
        else:
            self._manager_state.set_current_gate_leader(gate_id, None)

    # =========================================================================
    # Background Loops
    # =========================================================================

    def _reap_dead_workers(self, now: float) -> None:
        worker_reap_threshold = now - self._config.dead_worker_reap_interval_seconds
        workers_to_reap = self._reapable_workers(worker_reap_threshold)

        for worker_id in workers_to_reap:
            self._deregister_worker_with_notice(worker_id, "unhealthy_reaped")

    def _reapable_workers(self, worker_reap_threshold: float) -> list[str]:
        """The workers unhealthy since before the threshold whose circuit is not HALF_OPEN."""
        return [
            worker_id
            for worker_id, unhealthy_since in self._manager_state.iter_worker_unhealthy_since()
            if self._worker_is_reapable(worker_id, unhealthy_since, worker_reap_threshold)
        ]

    def _worker_is_reapable(
        self,
        worker_id: str,
        unhealthy_since: float,
        worker_reap_threshold: float,
    ) -> bool:
        """True when the worker has been unhealthy past the threshold and its
        circuit is not probing it (HALF_OPEN)."""
        if unhealthy_since >= worker_reap_threshold:
            return False

        circuit = self._manager_state._worker_circuits.get(worker_id)
        return not (circuit and circuit.circuit_state == CircuitState.HALF_OPEN)

    def _reap_dead_peers(self, now: float) -> None:
        peer_reap_threshold = now - self._config.dead_peer_reap_interval_seconds
        peers_to_reap = self._unhealthy_since_before(
            self._manager_state.iter_manager_peer_unhealthy_since(), peer_reap_threshold
        )
        for peer_id in peers_to_reap:
            self._registry.unregister_manager_peer(peer_id)

    def _unhealthy_since_before(
        self,
        unhealthy_since_entries: Iterable[tuple[str, float]],
        reap_threshold: float,
    ) -> list[str]:
        """The node ids unhealthy since before the reap threshold."""
        return [
            node_id
            for node_id, unhealthy_since in unhealthy_since_entries
            if unhealthy_since < reap_threshold
        ]

    def _reap_dead_gates(self, now: float) -> None:
        gate_reap_threshold = now - self._config.dead_gate_reap_interval_seconds
        gates_to_reap = self._unhealthy_since_before(
            self._manager_state.iter_gate_unhealthy_since(), gate_reap_threshold
        )
        for gate_id in gates_to_reap:
            self._registry.unregister_gate(gate_id)

    def _cleanup_stale_dead_manager_tracking(self, now: float) -> None:
        dead_manager_cleanup_threshold = now - (
            self._config.dead_peer_reap_interval_seconds * 2
        )
        # A dead manager still leading a job held here stays tracked: the
        # orphan scan finds a dead leader's jobs by it, and untracked, they
        # were never taken over.
        leaders_of_held_jobs = {
            leader_addr for _, leader_addr in self._manager_state.iter_job_leader_addrs()
        }
        dead_managers_to_cleanup = self._stale_dead_managers(
            dead_manager_cleanup_threshold, leaders_of_held_jobs
        )
        for tcp_addr in dead_managers_to_cleanup:
            self._manager_state.remove_dead_manager(tcp_addr)
            self._manager_state.clear_dead_manager_timestamp(tcp_addr)
            self._manager_state.remove_peer_lock(tcp_addr)

    def _stale_dead_managers(
        self,
        dead_manager_cleanup_threshold: float,
        leaders_of_held_jobs: set[tuple[str, int]],
    ) -> list[tuple[str, int]]:
        """Dead managers past the cleanup threshold that lead no job held here."""
        return [
            tcp_addr
            for tcp_addr, dead_since in self._manager_state.iter_dead_manager_timestamps()
            if self._dead_manager_is_stale(
                tcp_addr, dead_since, dead_manager_cleanup_threshold, leaders_of_held_jobs
            )
        ]

    def _dead_manager_is_stale(
        self,
        tcp_addr: tuple[str, int],
        dead_since: float,
        dead_manager_cleanup_threshold: float,
        leaders_of_held_jobs: set[tuple[str, int]],
    ) -> bool:
        """True when the manager died before the threshold and leads no held job."""
        return dead_since < dead_manager_cleanup_threshold and tcp_addr not in leaders_of_held_jobs

    async def _dead_node_reap_loop(self) -> None:
        await self._run_background_loop(
            lambda: self._dead_node_reap_iteration(),
            "Dead node reap error",
        )

    async def _dead_node_reap_iteration(self) -> bool:
        """One dead-node reap round; True keeps the loop running."""
        await self._clock.sleep(self._config.dead_node_check_interval_seconds)

        now = self._clock.monotonic()
        self._reap_dead_workers(now)
        self._reap_dead_peers(now)
        self._reap_dead_gates(now)
        self._cleanup_stale_dead_manager_tracking(now)
        self._resend_eviction_notices(now)
        await self._resend_completion_notices(now)
        await self._checkpoint_ledger_if_due()
        return True

    async def _run_background_loop(
        self,
        run_iteration: Callable[[], Awaitable[bool]],
        error_label: str,
        error_model: type[ServerError] | type[ServerWarning] = ServerError,
    ) -> None:
        """Run ``run_iteration`` while the manager runs, until it returns False
        or the loop is cancelled; an iteration's error is logged, not fatal."""
        while self._running:
            if not await self._run_background_iteration(run_iteration, error_label, error_model):
                break

    async def _run_background_iteration(
        self,
        run_iteration: Callable[[], Awaitable[bool]],
        error_label: str,
        error_model: type[ServerError] | type[ServerWarning],
    ) -> bool:
        """One guarded iteration: False on cancellation (the loop stops), the
        iteration's own verdict otherwise; an error is logged as ``error_label``."""
        try:
            return await run_iteration()
        except asyncio.CancelledError:
            return False
        except Exception as error:
            await self._udp_logger.log(
                error_model(
                    message=f"{error_label}: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        return True

    async def _checkpoint_ledger_if_due(self) -> None:
        """AD-38's compaction cadence, driven by the reap loop's tick.

        The ledger owns the policy and contains its own disk failures,
        so this is a no-op when the durable tier is unconfigured and
        two counter reads when a checkpoint is not yet due. Without a
        caller the WAL never compacts: pending entries accumulate for
        the process lifetime and recovery replays from LSN 0.
        """
        if self._job_ledger is None:
            return
        # While storage is unwritable gates route jobs away, so nothing
        # writes on its own: probe it here, on the same cadence, so the
        # recovery is noticed and placement returns.
        if not self._storage_health.writable:
            await self._probe_storage()
            return
        await self._job_ledger.maybe_checkpoint()

    def _is_storage_writable(self) -> bool:
        """What this manager's heartbeats report: whether its durable
        storage can take its writes. Only a manager with a durable tier
        is storage-gated."""
        if self._job_ledger is None:
            return True
        return self._storage_health.writable

    async def _probe_storage(self) -> None:
        """Prove the data directory can again take the largest write it
        refused: write a file of exactly that size, durably, then remove
        it. A smaller write could fit where the refused one cannot."""
        unproven_bytes = self._storage_health.unproven_bytes
        if unproven_bytes is None or self._config.wal_data_dir is None:
            return
        await self._write_storage_probe(unproven_bytes)

    async def _write_storage_probe(self, unproven_bytes: int) -> None:
        """Durably write, then remove, a probe file of the refused write's size."""
        probe_path = self._config.wal_data_dir / ".storage-probe"
        try:
            await self._storage_filesystem.atomic_write(
                probe_path, bytes(unproven_bytes)
            )
        except OSError as probe_error:
            self._storage_health.record_failure(probe_error, unproven_bytes)
            return
        self._storage_health.record_success(unproven_bytes)
        await self._storage_filesystem.remove(probe_path)

    def _get_manager_tracked_workflow_ids_for_worker(self, worker_id: str) -> set[str]:
        """Get workflow tokens that the manager thinks are running on a specific worker."""
        tracked_ids: set[str] = set()

        for job in self._job_manager.iter_jobs():
            self._add_running_sub_workflows_on_worker(tracked_ids, job, worker_id)

        return tracked_ids

    def _add_running_sub_workflows_on_worker(
        self,
        tracked_ids: set[str],
        job: JobInfo,
        worker_id: str,
    ) -> None:
        """Add the job's sub-workflow tokens on the worker whose parent is RUNNING."""
        for sub_workflow_token, sub_workflow in job.sub_workflows.items():
            if self._is_running_sub_workflow_on_worker(job, sub_workflow, worker_id):
                tracked_ids.add(sub_workflow_token)

    def _is_running_sub_workflow_on_worker(
        self,
        job: JobInfo,
        sub_workflow: SubWorkflowInfo,
        worker_id: str,
    ) -> bool:
        """True for a sub on the worker whose parent workflow is RUNNING."""
        if sub_workflow.worker_id != worker_id:
            return False

        parent_workflow = self._parent_workflow_by_token(job, sub_workflow)
        return bool(parent_workflow and parent_workflow.status == WorkflowStatus.RUNNING)

    def _parent_workflow_by_token(
        self,
        job: JobInfo,
        sub_workflow: SubWorkflowInfo,
    ) -> WorkflowInfo | None:
        """The sub's parent WorkflowInfo, keyed by its parent's workflow token."""
        return job.workflows.get(
            sub_workflow.parent_token.workflow_token or ""
        )

    async def _query_worker_active_workflows(
        self,
        worker_addr: tuple[str, int],
    ) -> set[str] | None:
        """Query a worker for its active workflow IDs. Returns None on failure."""
        request = WorkflowQueryRequest(
            requester_id=self._node_id.full,
            query_type="active",
        )

        response = await self._send_to_worker(
            worker_addr,
            "workflow_query",
            request.dump(),
            timeout=self._config.orphan_scan_worker_timeout_seconds,
        )

        if not response or isinstance(response, Exception):
            return None

        return self._queried_active_workflow_ids(response)

    def _queried_active_workflow_ids(self, response: bytes) -> set[str]:
        """The workflow ids of the worker's ``WorkflowQueryResponse``."""
        query_response = WorkflowQueryResponse.load(response)
        return {workflow.workflow_id for workflow in query_response.workflows}

    async def _handle_orphaned_workflows(
        self,
        orphaned_tokens: set[str],
        worker_id: str,
    ) -> None:
        """Sub-workflows this manager believes run on ``worker_id``, which
        no longer has them, are lost there: they go through reassignment --
        superseded, and the workflow retried (charged: an unexplained loss)
        or failed for good -- decided by the job's leader. Orphans of jobs
        another manager leads are broadcast to the peers for their leader."""
        orphans_led_elsewhere: list[tuple[str, str, str]] = []
        for orphaned_token in orphaned_tokens:
            await self._handle_orphaned_workflow(orphaned_token, worker_id, orphans_led_elsewhere)

        await self._broadcast_orphans_led_elsewhere(worker_id, orphans_led_elsewhere)

    async def _handle_orphaned_workflow(
        self,
        orphaned_token: str,
        worker_id: str,
        orphans_led_elsewhere: list[tuple[str, str, str]],
    ) -> None:
        """Reassign one orphan of a job led here; collect one led elsewhere."""
        await self._udp_logger.log(
            ServerWarning(
                message=f"Orphaned sub-workflow {orphaned_token[:8]}... detected on worker {worker_id[:8]}..., scheduling retry",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        orphan = TrackingToken.parse(orphaned_token)
        if not orphan.workflow_id:
            return
        if not self._leases.is_job_leader(orphan.job_id):
            orphans_led_elsewhere.append((orphan.job_id, orphan.workflow_id, orphaned_token))
            return
        await self._apply_workflow_reassignment_state(
            job_id=orphan.job_id,
            workflow_id=orphan.workflow_id,
            sub_workflow_token=orphaned_token,
            failed_worker_id=worker_id,
            reason="orphaned",
            loss_is_charged=not self._systemic_eviction_hold,
        )

    async def _broadcast_orphans_led_elsewhere(
        self,
        worker_id: str,
        orphans_led_elsewhere: list[tuple[str, str, str]],
    ) -> None:
        """Broadcast the orphans other managers lead to the peers, for their leaders."""
        if orphans_led_elsewhere and self._worker_disseminator:
            await self._worker_disseminator.broadcast_workflow_reassignments(
                failed_worker_id=worker_id,
                reason="orphaned",
                reassignments=orphans_led_elsewhere,
            )

    async def _scan_worker_for_orphans(
        self, worker_id: str, worker_addr: tuple[str, int]
    ) -> None:
        worker_workflow_ids = await self._query_worker_active_workflows(worker_addr)
        if worker_workflow_ids is None:
            return

        manager_tracked_ids = self._get_manager_tracked_workflow_ids_for_worker(
            worker_id
        )
        orphaned_sub_workflows = manager_tracked_ids - worker_workflow_ids
        await self._handle_orphaned_workflows(orphaned_sub_workflows, worker_id)

    async def _orphan_scan_loop(self) -> None:
        """
        Periodically scan for orphaned workflows.

        An orphaned workflow is one that:
        1. The manager thinks is running on a worker, but
        2. The worker no longer has it (worker restarted, crashed, etc.)

        This reconciliation ensures no workflows are "lost" due to state
        inconsistencies between manager and workers.
        """
        await self._run_background_loop(
            lambda: self._orphan_scan_iteration(),
            "Orphan scan error",
        )

    async def _orphan_scan_iteration(self) -> bool:
        """One orphan scan round, run by the leader or a backup scanner."""
        await self._clock.sleep(self._config.orphan_scan_interval_seconds)

        if not (self.is_leader() or self._should_backup_orphan_scan()):
            return True

        await self._run_orphan_scan()
        return True

    async def _run_orphan_scan(self) -> None:
        """Scan jobs for orphans (leader only), then every worker."""
        if self.is_leader():
            await self._scan_for_orphaned_jobs()

        for worker_id, worker in self._manager_state.iter_workers():
            await self._scan_one_worker_for_orphans(worker_id, worker)

    async def _scan_one_worker_for_orphans(self, worker_id: str, worker: WorkerRegistration) -> None:
        """Scan one worker for orphans; its failure is logged, not fatal to the round."""
        try:
            worker_addr = (worker.node.host, worker.node.port)
            await self._scan_worker_for_orphans(worker_id, worker_addr)

        except Exception as worker_error:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Orphan scan for worker {worker_id[:8]}... failed: {worker_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _job_responsiveness_loop(self) -> None:
        """Check job responsiveness (AD-30 + cross-layer escalation).

        AD-30 designed two independent failure-detection layers (global SWIM
        probing and per-job responsiveness) explicitly so that *either*
        layer can drive death detection. The job-layer wiring was incomplete:
        :meth:`ManagerHealthMonitor.suspect_job` had no caller in the
        manager, so the per-job timestamps maintained by
        :meth:`record_job_progress` never escalated into anything.

        This loop now closes the wiring in two steps:

        1. Promote silent ``(job_id, worker_id)`` pairs into a
           :class:`JobSuspicion` with a tight ``job_min_timeout`` budget
           (separate from the long stale-progress threshold, which only
           gates the *start* of suspicion). Stale-progress means "we already
           waited a full responsiveness threshold for any signal"; further
           silence past the tight suspicion timer is strong evidence.

        2. When a job-suspicion expires, run the existing per-job
           reassignment path AND escalate the worker to global DEAD via
           :meth:`_escalate_job_death_to_global` — a second-layer signal
           that bypasses the serial SWIM probe walk when the cluster is
           in a burst-failure regime. The escalation is gated so it never
           contradicts a recent successful SWIM probe.
        """
        await self._run_background_loop(
            lambda: self._job_responsiveness_iteration(),
            "Job responsiveness check error",
        )

    async def _job_responsiveness_iteration(self) -> bool:
        """One AD-30 job-responsiveness round: suspect silent pairs, act on expiries."""
        await self._clock.sleep(
            self._config.job_responsiveness_check_interval_seconds
        )

        silent_pairs = self._worker_health_monitor.find_silent_worker_jobs(
            self._config.job_responsiveness_threshold_seconds,
        )
        for job_id, worker_id in silent_pairs:
            await self._worker_health_monitor.suspect_job(
                job_id,
                worker_id,
                timeout_seconds=self._hierarchical_detector.config.job_min_timeout,
            )

        expired = await self._worker_health_monitor.check_job_suspicion_expiry()

        for job_id, worker_id in expired:
            self._on_worker_dead_for_job(job_id, worker_id)
            await self._escalate_job_death_to_global(worker_id)
        return True

    async def _escalate_job_death_to_global(self, worker_id: str) -> None:
        """Promote a job-layer-confirmed death into a global DEAD declaration.

        AD-30 cross-layer escalation. The job layer has *independently*
        observed the worker miss progress past both the stale-threshold and
        the tight job-suspicion timer. Without this escalation the worker
        sits as ``ALIVE`` in the global incarnation tracker until the SWIM
        probe walk reaches it — at large ``N`` that walk is the limiting
        factor on dead-detection latency. With this escalation the global
        layer treats the job-layer expiry as a peer-equivalent confirmation
        and runs the standard confirmed-DEAD commit chain so registry cleanup,
        reassignment, HFD global-death state, and SWIM ``dead`` gossip fire
        identically to a probe-driven death.

        Two gates protect against false positives:

        * **Recent probe-success**: if the per-peer probe reliability tracks
          a healthy success rate, the worker is still SWIM-responsive and
          the job silence is more likely workflow-internal (e.g. busy CPU)
          than node death. Defer to the SWIM layer in that case.

        * **Already-terminal**: if the incarnation tracker already records
          DEAD (or has no record at all), no action is needed.
        """
        worker = self._registry.get_worker(worker_id)
        udp_addr = self._escalatable_worker_udp_addr(worker)
        if udp_addr is None:
            return

        # Defer to the SWIM layer when it has independently confirmed
        # the worker as alive within the past two probe cycles. Sized
        # against the configured protocol period so the gate scales
        # with operator tuning.
        recent_success_window = 2.0 * float(
            self.env.SWIM_UDP_POLL_INTERVAL
        )
        if self._peer_probe_reliability.had_recent_success(
            udp_addr, within_seconds=recent_success_window
        ):
            return

        await self._escalate_unprobed_worker_death(worker_id, udp_addr)

    def _escalatable_worker_udp_addr(
        self,
        worker: WorkerRegistration | None,
    ) -> tuple[str, int] | None:
        """The worker's UDP address, or None when it is unknown or has no UDP port."""
        if worker is None or worker.node is None:
            return None

        return self._bound_node_udp_addr(worker.node)

    def _bound_node_udp_addr(self, node: NodeInfo) -> tuple[str, int] | None:
        """The node's UDP address when it has a UDP port."""
        if not node.udp_port:
            return None

        return (node.host, node.udp_port)

    async def _escalate_unprobed_worker_death(
        self,
        worker_id: str,
        udp_addr: tuple[str, int],
    ) -> None:
        """Commit the global death unless the tracker has no record or holds it DEAD (AD-30)."""
        node_state = self._incarnation_tracker.get_node_state(udp_addr)
        if node_state is None or node_state.status == b"DEAD":
            return

        await self._commit_job_layer_escalated_death(worker_id, udp_addr, node_state)

    async def _commit_job_layer_escalated_death(
        self,
        worker_id: str,
        udp_addr: tuple[str, int],
        node_state: NodeState,
    ) -> None:
        """Run the confirmed-DEAD commit chain and log the escalation (AD-30)."""
        incarnation = node_state.incarnation
        updated = await self._commit_confirmed_global_death(
            udp_addr,
            incarnation,
            "ad30_job_layer_escalation",
        )
        if not updated:
            return

        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"AD-30 job-layer escalation: worker {worker_id[:8]}... "
                    f"declared globally DEAD (no recent successful probe; "
                    f"job-suspicion expired)"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _stats_push_loop(self) -> None:
        """Periodically push stats to gates/clients."""
        await self._run_background_loop(
            lambda: self._stats_push_iteration(),
            "Stats push error",
        )

    async def _stats_push_iteration(self) -> bool:
        """One stats push round."""
        await self._clock.sleep(self._config.batch_push_interval_seconds)

        await self._stats.refresh_dispatch_throughput()

        # Push aggregated stats
        await self._stats.push_batch_stats()
        return True

    async def _windowed_stats_flush_loop(self) -> None:
        flush_interval = self._config.stats_push_interval_ms / 1000.0

        await self._run_background_loop(
            lambda: self._windowed_stats_flush_iteration(flush_interval),
            "Windowed stats flush error",
        )

    async def _windowed_stats_flush_iteration(self, flush_interval: float) -> bool:
        """One windowed-stats flush; False once the manager stopped during the sleep."""
        await self._clock.sleep(flush_interval)
        if not self._running:
            return False
        await self._flush_windowed_stats()
        return True

    async def _flush_windowed_stats(self) -> None:
        """Forward the closed windows of gate-routed jobs to their origin
        gate, per worker (the gate aggregates across datacenters). A job a
        client submitted directly keeps its windows for the client stats
        push, which aggregates them: each job has one consumer."""
        for job_id in self._windowed_stats.get_jobs_with_pending_stats():
            await self._forward_closed_job_windows(job_id)

    async def _forward_closed_job_windows(self, job_id: str) -> None:
        """Flush a gate-routed job's closed windows to its origin gate."""
        origin_gate_addr = self._manager_state.get_job_origin_gate(job_id)
        if origin_gate_addr is None:
            return

        for stats_push in await self._windowed_stats.flush_closed_job_windows(
            job_id,
            aggregate=False,
        ):
            await self._push_windowed_stats_to_gate(stats_push, origin_gate_addr)

    async def _push_windowed_stats_to_gate(
        self,
        stats_push: WindowedStatsPush,
        origin_gate_addr: tuple[str, int],
    ) -> None:
        stats_push.datacenter = self._node_id.datacenter

        try:
            response = await self._send_to_peer(
                origin_gate_addr,
                "windowed_stats_push",
                stats_push.dump(),
                timeout=self._config.tcp_timeout_short_seconds,
            )
            self._raise_unless_windowed_stats_taken(response)
        except Exception as error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to send windowed stats to gate: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _raise_unless_windowed_stats_taken(self, response: bytes | Exception | None) -> None:
        """Raise the transport error or the gate's rejection of the stats push."""
        if isinstance(response, Exception):
            raise response
        if response not in (b"ok", b"discarded", None):
            raise RuntimeError(f"windowed_stats_push rejected with {response!r}")

    async def _send_gate_heartbeat(
        self,
        gate_addr: tuple[str, int],
        heartbeat_payload: bytes,
    ) -> bool:
        """Send one heartbeat to one gate; whether the gate took it. A
        failure -- raised or returned -- is logged."""
        try:
            response, _clock = await self.send_tcp(
                gate_addr,
                "manager_status_update",
                heartbeat_payload,
                timeout=self._config.tcp_timeout_short_seconds,
            )

        except Exception as heartbeat_error:
            response = heartbeat_error

        if isinstance(response, Exception):
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to send heartbeat to gate {gate_addr}: {response}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

        return True

    async def _gate_heartbeat_loop(self) -> None:
        """
        Periodically send ManagerHeartbeat to gates via TCP.

        This supplements the Serf-style SWIM embedding for reliability.
        Gates use this for datacenter health classification.
        """
        heartbeat_interval = self._config.heartbeat_interval_seconds

        await self._udp_logger.log(
            ServerInfo(
                message="Gate heartbeat loop started",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        await self._run_background_loop(
            lambda: self._gate_heartbeat_iteration(heartbeat_interval),
            "Gate heartbeat error",
        )

    async def _gate_heartbeat_iteration(self, heartbeat_interval: float) -> bool:
        """One heartbeat round to every healthy (else seed) gate."""
        await self._clock.sleep(heartbeat_interval)

        heartbeat = self._build_manager_heartbeat()

        # Send to all healthy gates (use known gates if available, else seed gates)
        gate_addrs = self._get_healthy_gate_tcp_addrs() or self._seed_gates

        # Concurrently: an unreachable gate costs the round one send
        # timeout, not one per gate after it.
        heartbeat_payload = heartbeat.dump()
        deliveries = await asyncio.gather(
            *(
                self._send_gate_heartbeat(gate_addr, heartbeat_payload)
                for gate_addr in gate_addrs
            )
        )
        sent_count = sum(deliveries)

        await self._log_gate_heartbeat_round(sent_count, len(gate_addrs))
        return True

    async def _log_gate_heartbeat_round(self, sent_count: int, gate_count: int) -> None:
        """Debug-log a heartbeat round that reached at least one gate."""
        if sent_count > 0:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Sent heartbeat to {sent_count}/{gate_count} gates",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _rate_limit_cleanup_loop(self) -> None:
        """
        Periodically clean up inactive clients from the rate limiter.

        Removes token buckets for clients that haven't made requests
        within the inactive_cleanup_seconds window to prevent memory leaks.
        """
        cleanup_interval = self._config.rate_limit_cleanup_interval_seconds

        await self._run_background_loop(
            lambda: self._rate_limit_cleanup_iteration(cleanup_interval),
            "Rate limit cleanup error",
        )

    async def _rate_limit_cleanup_iteration(self, cleanup_interval: float) -> bool:
        """One rate-limiter cleanup round."""
        await self._clock.sleep(cleanup_interval)

        cleaned = await self._cleanup_inactive_rate_limit_clients()

        if cleaned > 0:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Rate limiter: cleaned up {cleaned} inactive clients",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        return True

    async def _job_cleanup_loop(self) -> None:
        """
        Periodically clean up completed/failed jobs and their associated state.

        Runs at JOB_CLEANUP_INTERVAL (default 60s).
        Jobs are eligible for cleanup when:
        - Status is terminal (completed, failed, cancelled or timed out)
        - More than their status's max age has elapsed since completion:
          COMPLETED_JOB_MAX_AGE for a completed job, FAILED_JOB_MAX_AGE
          for a failed, cancelled or timed-out one
        """
        cleanup_interval = self._config.job_cleanup_interval_seconds
        completed_job_max_age = self._config.completed_job_max_age_seconds
        unsuccessful_job_max_age = self._config.failed_job_max_age_seconds
        # TIMEOUT used to be missing from a hand-written terminal set, so
        # every timed-out job (completed_at stamped, status TIMEOUT) was
        # retained forever.
        status_order = JobStatusOrder()

        await self._run_background_loop(
            lambda: self._job_cleanup_iteration(
                cleanup_interval,
                status_order,
                completed_job_max_age,
                unsuccessful_job_max_age,
            ),
            "Job cleanup error",
        )

    async def _job_cleanup_iteration(
        self,
        cleanup_interval: float,
        status_order: JobStatusOrder,
        completed_job_max_age: float,
        unsuccessful_job_max_age: float,
    ) -> bool:
        """One job cleanup round."""
        await self._clock.sleep(cleanup_interval)
        await self._run_job_cleanup_pass(
            cleanup_interval,
            status_order,
            completed_job_max_age,
            unsuccessful_job_max_age,
        )
        return True

    async def _run_job_cleanup_pass(
        self,
        cleanup_interval: float,
        status_order: JobStatusOrder,
        completed_job_max_age: float,
        unsuccessful_job_max_age: float,
    ) -> None:
        """
        Run one job cleanup sweep: drop terminal jobs past their retention, then reconcile silent copies.

        Wall-clock seconds: the sweep's time matches the semantic of
        job.completed_at, which is set from Raft entry.timestamp (HLC) or the
        local clock. The sweep's counts are logged once both steps are done.
        """
        current_time = self._clock.time()
        jobs_cleaned = await self._sweep_expired_terminal_jobs(
            current_time,
            status_order,
            completed_job_max_age,
            unsuccessful_job_max_age,
        )
        copies_reconciled = await self._reconcile_silent_job_copies(
            self._silent_job_copies(current_time, cleanup_interval, status_order),
            current_time,
            status_order,
        )
        await self._log_job_cleanup_pass(copies_reconciled, jobs_cleaned)

    async def _sweep_expired_terminal_jobs(
        self,
        current_time: float,
        status_order: JobStatusOrder,
        completed_job_max_age: float,
        unsuccessful_job_max_age: float,
    ) -> int:
        """Clean up every terminal job held longer than its status's retention age; returns how many were cleaned."""
        jobs_cleaned = 0
        for job in list(self._job_manager.iter_jobs()):
            if self._terminal_job_retention_expired(
                job,
                current_time,
                status_order,
                completed_job_max_age,
                unsuccessful_job_max_age,
            ):
                await self._cleanup_job_state(job.job_id)
                jobs_cleaned += 1
        return jobs_cleaned

    @classmethod
    def _terminal_job_retention_expired(
        cls,
        job: JobInfo,
        current_time: float,
        status_order: JobStatusOrder,
        completed_job_max_age: float,
        unsuccessful_job_max_age: float,
    ) -> bool:
        """Whether a job is terminal, stamped complete, and held longer than its status's retention age."""
        if not status_order.is_terminal(job.status) or job.completed_at <= 0:
            return False
        max_age = cls._retention_age_for_status(
            job.status, completed_job_max_age, unsuccessful_job_max_age
        )
        return current_time - job.completed_at > max_age

    @staticmethod
    def _retention_age_for_status(
        status: str,
        completed_job_max_age: float,
        unsuccessful_job_max_age: float,
    ) -> float:
        """The retention age of a terminal job: the completed age for a completed job, else the unsuccessful one."""
        return (
            completed_job_max_age
            if status == JobStatus.COMPLETED.value
            else unsuccessful_job_max_age
        )

    def _silent_job_copies(
        self,
        current_time: float,
        cleanup_interval: float,
        status_order: JobStatusOrder,
    ) -> list[tuple[JobInfo, float, tuple[str, int] | None]]:
        """
        List the copies of jobs led elsewhere that heard nothing for a whole sweep, with whom to ask about each.

        Each entry is the job, when it was last heard from, and the address
        to ask: its leader while that leader is an active peer, else the
        datacenter leader (None when this manager is it). A job's leader
        re-syncs every job it leads each interval, so the silence means the
        leader dropped the job -- it ended, or its submission was refused --
        and the one terminal sync saying so was lost; or the leader died, and
        the datacenter leader settles its jobs. Kept, the copy was never
        swept, held its consensus group, and stood as a takeover candidate
        for a job already over.
        """
        active_peers = self._manager_state.get_active_manager_peers()
        datacenter_leader_addr = self._datacenter_leader_addr_to_ask()
        return [
            (
                job,
                job.timestamp,
                self._job_copy_contact(leader_addr, active_peers, datacenter_leader_addr),
            )
            for job in self._job_manager.iter_jobs()
            if (
                leader_addr := self._silent_job_copy_leader(
                    job, current_time, cleanup_interval, status_order
                )
            )
            is not None
        ]

    def _datacenter_leader_addr_to_ask(self) -> tuple[str, int] | None:
        """The datacenter leader's address to ask about a job copy, or None when this manager is the leader."""
        return None if self.is_leader() else self._resolve_dc_leader_addr()

    @staticmethod
    def _job_copy_contact(
        leader_addr: tuple[str, int],
        active_peers,
        datacenter_leader_addr: tuple[str, int] | None,
    ) -> tuple[str, int] | None:
        """The job's leader when it is an active peer, else the datacenter leader's address."""
        return leader_addr if leader_addr in active_peers else datacenter_leader_addr

    def _silent_job_copy_leader(
        self,
        job: JobInfo,
        current_time: float,
        cleanup_interval: float,
        status_order: JobStatusOrder,
    ) -> tuple[str, int] | None:
        """The leader address of a silent, unled copy of a live job, or None when the job is not one or has none."""
        if not self._is_silent_unled_job_copy(job, current_time, cleanup_interval, status_order):
            return None
        return self._manager_state.get_job_leader_addr(job.job_id)

    def _is_silent_unled_job_copy(
        self,
        job: JobInfo,
        current_time: float,
        cleanup_interval: float,
        status_order: JobStatusOrder,
    ) -> bool:
        """Whether a job is live, unheard from for longer than a sweep interval, and not led by this manager."""
        return (
            not status_order.is_terminal(job.status)
            and current_time - job.timestamp > cleanup_interval
            and not self._leases.is_job_leader(job.job_id)
        )

    async def _reconcile_silent_job_copies(
        self,
        silent_copies: list[tuple[JobInfo, float, tuple[str, int] | None]],
        current_time: float,
        status_order: JobStatusOrder,
    ) -> int:
        """Reconcile each silent job copy with its settled status; returns how many copies were reconciled."""
        copies_reconciled = 0
        for job, last_heard_at, asked_addr in silent_copies:
            if await self._reconcile_silent_job_copy(
                job, last_heard_at, asked_addr, current_time, status_order
            ):
                copies_reconciled += 1
        return copies_reconciled

    async def _reconcile_silent_job_copy(
        self,
        job: JobInfo,
        last_heard_at: float,
        asked_addr: tuple[str, int] | None,
        current_time: float,
        status_order: JobStatusOrder,
    ) -> bool:
        """
        Retire one silent job copy once its status is settled; returns whether it was reconciled.

        A copy whose status is still unsettled is asked about again next
        sweep. A sync that landed meanwhile speaks for the copy itself, so a
        copy heard from since the sweep began is left alone.
        """
        known_status, settled = await self._settled_status_of_silent_copy(
            job, asked_addr, status_order
        )
        if not settled:
            return False
        if self._job_copy_heard_from_meanwhile(job, last_heard_at):
            return False
        await self._retire_silent_job_copy(job, known_status, current_time)
        return True

    async def _settled_status_of_silent_copy(
        self,
        job: JobInfo,
        asked_addr: tuple[str, int] | None,
        status_order: JobStatusOrder,
    ) -> tuple[str | None, bool]:
        """
        Settle a silent copy's status from the replicated ledger, else by asking its leader.

        Returns (status, True) when settled -- the status None when the job
        is unknown where it is led -- and (None, False) when it stays
        unsettled: there is no one to ask, the ask went unanswered, or the
        job is live where it is led.
        """
        if (known_status := self._replicated_terminal_status(job.job_id)) is not None:
            return known_status, True
        if asked_addr is None:
            return None, False
        return await self._ask_leader_for_settled_status(job, asked_addr, status_order)

    def _replicated_terminal_status(self, job_id: str) -> str | None:
        """The job's status in the replicated ledger when that status is terminal, else None."""
        replicated_state = self._ledger_replica.job_state(job_id)
        return (
            replicated_state.status
            if replicated_state is not None and replicated_state.is_terminal
            else None
        )

    async def _ask_leader_for_settled_status(
        self,
        job: JobInfo,
        asked_addr: tuple[str, int],
        status_order: JobStatusOrder,
    ) -> tuple[str | None, bool]:
        """Ask a silent copy's leader for the job's status; an unanswered ask leaves it unsettled until next sweep."""
        response = await self._send_to_peer(
            asked_addr,
            "job_status",
            job.job_id.encode(),
            timeout=self._config.tcp_timeout_short_seconds,
        )
        if isinstance(response, Exception) or response is None:
            return None, False
        return self._settled_status_from_reply(response, status_order)

    @staticmethod
    def _settled_status_from_reply(
        response: bytes,
        status_order: JobStatusOrder,
    ) -> tuple[str | None, bool]:
        """
        Read a leader's job-status reply into (status, settled).

        An empty reply means no such job where it is led: settled with no
        status. A live status means the job is held live where it is led --
        the syncs are what fail -- so it stays unsettled.
        """
        if not response:
            return None, True
        known_status = GlobalJobStatus.load(response).status
        return known_status, status_order.is_terminal(known_status)

    def _job_copy_heard_from_meanwhile(self, job: JobInfo, last_heard_at: float) -> bool:
        """Whether a job copy was replaced, synced since the sweep began, or came to be led here."""
        return (
            self._job_manager.get_job_by_id(job.job_id) is not job
            or job.timestamp != last_heard_at
            or self._leases.is_job_leader(job.job_id)
        )

    async def _retire_silent_job_copy(
        self,
        job: JobInfo,
        known_status: str | None,
        current_time: float,
    ) -> None:
        """
        Retire a reconciled job copy: drop it, or end it with its settled status and destroy its group.

        No such job where it is led, or one never admitted (announced, then
        refused): there is no job to keep, so its state is cleaned up.
        """
        if known_status is None or not job.workflows:
            await self._cleanup_job_state(job.job_id)
            return
        await self._apply_settled_job_copy_status(job, known_status, current_time)
        await self._raft.consensus.destroy_job_raft(job.job_id)

    @staticmethod
    async def _apply_settled_job_copy_status(
        job: JobInfo,
        known_status: str,
        current_time: float,
    ) -> None:
        """Under the job's lock, set its settled status and stamp its completion time when it has none."""
        async with job.lock:
            job.status = known_status
            if job.completed_at <= 0:
                job.completed_at = current_time

    async def _log_job_cleanup_pass(self, copies_reconciled: int, jobs_cleaned: int) -> None:
        """Log how many silent job copies a cleanup sweep reconciled and how many jobs it cleaned, when any."""
        if copies_reconciled > 0:
            await self._udp_logger.log(
                ServerInfo(
                    message=(
                        f"Reconciled {copies_reconciled} job copies their "
                        "leaders no longer hold"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        if jobs_cleaned > 0:
            await self._udp_logger.log(
                ServerInfo(
                    message=f"Cleaned up {jobs_cleaned} completed jobs",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _unified_timeout_loop(self) -> None:
        """
        Background task that checks for job timeouts (AD-34 Part 10.4.3).

        Runs at JOB_TIMEOUT_CHECK_INTERVAL (default 30s). Only leader checks timeouts.
        Delegates to strategy.check_timeout() which handles both:
        - Extension-aware timeout (base_timeout + extensions)
        - Stuck detection (no progress for 2+ minutes)
        """
        check_interval = self._config.job_timeout_check_interval_seconds

        await self._run_background_loop(
            lambda: self._unified_timeout_iteration(check_interval),
            "Unified timeout loop error",
        )

    async def _unified_timeout_iteration(self, check_interval: float) -> bool:
        """One AD-34 timeout round: check every job's strategy, then expire
        cancellations their workers never confirmed (AD-54)."""
        await self._clock.sleep(check_interval)

        # Only leader checks timeouts
        if not self.is_leader():
            return True

        for job_id, strategy in list(
            self._manager_state.iter_job_timeout_strategies()
        ):
            await self._check_job_timeout_strategy(job_id, strategy)

        # A cancellation its workers never confirmed within the
        # window they have to confirm one is over anyway (AD-54): the
        # workers are gone, or stop it when they learn its job's
        # fate -- the job's cancellation must not wait forever.
        await self._expire_unconfirmed_cancellations()
        return True

    async def _check_job_timeout_strategy(self, job_id: str, strategy: TimeoutStrategy) -> None:
        """Run one job's timeout check (AD-34), logging a timeout or a check error."""
        try:
            timed_out, reason = await strategy.check_timeout(job_id)
            if timed_out:
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            f"Job {job_id[:8]}... timeout strategy "
                            f"reported: {reason}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
        except Exception as check_error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Timeout check error for job {job_id[:8]}...: {check_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _expire_unconfirmed_cancellations(self) -> None:
        """Close each led workflow stuck CANCELLING past the confirm window (AD-54)."""
        for job_id, workflow_id, _state, _seconds_cancelling in (
            self._job_manager.workflow_lifecycle.get_stuck_workflows(
                frozenset({WorkflowState.CANCELLING}),
                self.env.CANCELLED_WORKFLOW_TIMEOUT,
            )
        ):
            if not self._leases.is_job_leader(job_id):
                continue
            await self._expire_unconfirmed_cancellation(job_id, workflow_id)

    async def _expire_unconfirmed_cancellation(self, job_id: str, workflow_id: str) -> None:
        """Fail the workflow's unconfirmed sub cancellations, then cancel it (AD-54)."""
        await self._fail_unconfirmed_sub_cancellations(job_id, workflow_id)
        if await self._job_manager.finish_workflow_cancellation(job_id, workflow_id):
            await self._complete_job_if_done(job_id)

    async def _fail_unconfirmed_sub_cancellations(self, job_id: str, workflow_id: str) -> None:
        """Finalize, as failed, each pending sub cancellation of the workflow."""
        for pending_sub_workflow in list(
            self._manager_state.get_cancellation_pending_workflows(job_id)
        ):
            if TrackingToken.parse(pending_sub_workflow).workflow_id == workflow_id:
                await self._cancellation.finalize_workflow_cancellation(
                    job_id=job_id,
                    workflow_id=pending_sub_workflow,
                    success=False,
                    errors=[
                        "its worker never confirmed stopping it within "
                        f"{self.env.CANCELLED_WORKFLOW_TIMEOUT:.0f}s"
                    ],
                )

    async def _deadline_enforcement_loop(self) -> None:
        """
        Background loop for worker deadline enforcement (AD-26 Issue 2).

        Checks worker deadlines every 5 seconds and takes action:
        - If deadline expired but within grace period: mark worker as SUSPECTED
        - If deadline expired beyond grace period: evict worker
        """
        check_interval = 5.0

        await self._run_background_loop(
            lambda: self._deadline_enforcement_iteration(check_interval),
            "Deadline enforcement error",
        )

    async def _deadline_enforcement_iteration(self, check_interval: float) -> bool:
        """One AD-26 deadline enforcement round."""
        await self._clock.sleep(check_interval)

        current_time = self._clock.monotonic()
        grace_period = self._worker_health_manager.base_deadline

        await self._enforce_worker_deadlines(current_time, grace_period)
        return True

    async def _enforce_worker_deadlines(self, current_time: float, grace_period: float) -> None:
        """One enforcement pass: suspect workers within the grace period,
        evict those beyond it -- unless that many evictions at once look
        systemic (AD-19), in which case every eviction is held."""
        lateness = self._expired_worker_deadlines(current_time)
        await self._suspect_workers_within_grace(lateness, grace_period)

        eviction_candidates = self._workers_beyond_grace(lateness, grace_period)
        population = self._manager_state.get_worker_count()
        if is_systemic_failure(len(eviction_candidates), population):
            await self._hold_systemic_evictions(eviction_candidates, population)
            return

        await self._release_systemic_eviction_hold(population)
        for worker_id in eviction_candidates:
            await self._evict_worker_deadline_expired(worker_id)

    async def _suspect_workers_within_grace(
        self,
        lateness: dict[str, float],
        grace_period: float,
    ) -> None:
        """Suspect each worker past its deadline but within the grace period (AD-26)."""
        for worker_id, seconds_late in lateness.items():
            if seconds_late <= grace_period:
                await self._suspect_worker_deadline_expired(worker_id)

    def _workers_beyond_grace(self, lateness: dict[str, float], grace_period: float) -> list[str]:
        """The workers past their deadline beyond the grace period: eviction candidates."""
        return [
            worker_id for worker_id, seconds_late in lateness.items() if seconds_late > grace_period
        ]

    async def _hold_systemic_evictions(self, held_worker_ids: list[str], population: int) -> None:
        """Keep the would-be evictions suspected instead: more than half the
        workers missing their deadlines together points at this manager's
        own view (its network, its loop), and evicting them would strand
        their work for nothing. Jobs stay bounded by their timeouts."""
        for worker_id in held_worker_ids:
            await self._suspect_worker_deadline_expired(worker_id)
        if self._systemic_eviction_hold:
            return
        self._systemic_eviction_hold = True
        await self._udp_logger.log(
            SystemicEvictionHeld(
                message=(
                    f"Holding eviction of {len(held_worker_ids)}/{population} workers past their "
                    "deadlines: a failure that wide looks systemic"
                ),
                node_id=self._node_id.short,
                held_count=len(held_worker_ids),
                population=population,
            )
        )

    async def _release_systemic_eviction_hold(self, population: int) -> None:
        if not self._systemic_eviction_hold:
            return
        self._systemic_eviction_hold = False
        await self._udp_logger.log(
            SystemicEvictionReleased(
                message=f"Systemic eviction hold released ({population} workers)",
                node_id=self._node_id.short,
                population=population,
            )
        )

    def _expired_worker_deadlines(self, current_time: float) -> dict[str, float]:
        """Seconds past its deadline, per worker whose deadline expired with
        work still on it; expired deadlines with no work left are cleared."""
        lateness: dict[str, float] = {}
        for worker_id, deadline in self._manager_state.iter_worker_deadlines():
            self._record_worker_lateness(lateness, worker_id, deadline, current_time)
        return lateness

    def _record_worker_lateness(
        self,
        lateness: dict[str, float],
        worker_id: str,
        deadline: float,
        current_time: float,
    ) -> None:
        """Record an expired deadline of a worker with work; clear one without (AD-26)."""
        if current_time <= deadline:
            return

        # A worker deadline is a property of ACTIVE work —
        # AD-26 extensions are granted against dispatched
        # workflows, and nothing else refreshes the stored
        # value once that work drains. If the worker has no
        # unfinished sub-workflows, the expired deadline is
        # vestigial: enforcing it suspected and then evicted
        # a healthy, SWIM-OK, idle worker ~30s+ after every
        # job completed (and the dead-node reaper then
        # deregistered it, flipping the DC to "busy" on an
        # idle cluster). Clear it and move on — the next
        # dispatch/extension writes a fresh deadline. Uses
        # the same unfinished-work query the eviction path
        # itself uses for reassignment, so "nothing left to
        # protect" and "nothing to reassign" stay one
        # definition.
        if not self._worker_has_reassignable_work(worker_id):
            self._manager_state.clear_worker_deadline(worker_id)
            return

        lateness[worker_id] = current_time - deadline

    def _worker_has_reassignable_work(self, worker_id: str) -> bool:
        """True when unfinished sub-workflows remain on the worker."""
        job_manager = self._job_manager
        return bool(
            job_manager
            and job_manager.get_reassignable_sub_workflows_on_worker(
                worker_id
            )
        )

    def _build_job_state_sync_message(
        self,
        job_id: str,
        job: JobInfo | None,
        *,
        leader_id: str | None = None,
        leader_addr: tuple[str, int] | None = None,
        fencing_token: int | None = None,
        replace_existing: bool = True,
    ) -> JobStateSyncMessage:
        elapsed_seconds = self._job_elapsed_seconds(job)
        origin_gate_addr = self._job_origin_gate_addr(job_id, job)
        callback_addr = self._get_job_callback_addr(job_id)
        effective_leader_id = self._effective_job_leader_id(job_id, job, leader_id)
        effective_leader_addr = self._effective_job_leader_addr(job_id, job, leader_addr)
        effective_fencing_token = self._effective_job_fencing_token(job_id, fencing_token)
        (
            job_status,
            workflows_total,
            workflows_completed,
            workflows_failed,
            workflow_statuses,
            workflow_snapshots,
            sub_workflow_snapshots,
            context_snapshot,
            layer_version,
        ) = self._job_sync_progress(job)

        return JobStateSyncMessage(
            leader_id=effective_leader_id,
            job_id=job_id,
            status=job_status,
            fencing_token=effective_fencing_token,
            workflows_total=workflows_total,
            workflows_completed=workflows_completed,
            workflows_failed=workflows_failed,
            workflow_statuses=workflow_statuses,
            elapsed_seconds=elapsed_seconds,
            timestamp=self._clock.monotonic(),
            origin_gate_addr=origin_gate_addr,
            callback_addr=callback_addr,
            leader_addr=effective_leader_addr,
            workflow_snapshots=workflow_snapshots,
            sub_workflow_snapshots=sub_workflow_snapshots,
            replace_existing=replace_existing,
            context_snapshot=context_snapshot,
            layer_version=layer_version,
            raft_voters=self._job_raft_voters(job_id),
        )

    def _job_elapsed_seconds(self, job: JobInfo | None) -> float:
        """Seconds since the job started on the monotonic clock, or 0.0 for no job or one not yet started."""
        return (
            self._clock.monotonic() - job.started_at
            if job is not None and job.started_at
            else 0.0
        )

    def _job_origin_gate_addr(self, job_id: str, job: JobInfo | None) -> tuple[str, int] | None:
        """The origin gate the job's submission names, else the one the manager state records for the job."""
        return self._submitted_origin_gate_addr(job) or self._manager_state.get_job_origin_gate(job_id)

    def _effective_job_leader_id(
        self,
        job_id: str,
        job: JobInfo | None,
        leader_id: str | None,
    ) -> str:
        """The leader a job's state sync names: the one given, else the one recorded, else this manager."""
        return leader_id or self._recorded_job_leader_id(job_id, job) or self._node_id.full

    def _effective_job_leader_addr(
        self,
        job_id: str,
        job: JobInfo | None,
        leader_addr: tuple[str, int] | None,
    ) -> tuple[str, int]:
        """The leader address a job's state sync names: the one given, else the one recorded, else this manager's."""
        return leader_addr or self._recorded_job_leader_addr(job_id, job) or (self._host, self._tcp_port)

    def _effective_job_fencing_token(self, job_id: str, fencing_token: int | None) -> int:
        """The fencing token a job's state sync carries: the one given, else the job's current fence token."""
        return (
            fencing_token
            if fencing_token is not None
            else self._leases.get_fence_token(job_id)
        )

    @staticmethod
    def _submitted_origin_gate_addr(job: JobInfo | None):
        """The origin gate named by the job's held submission, or a falsy value when there is none."""
        return job is not None and job.submission and job.submission.origin_gate_addr

    def _recorded_job_leader_id(self, job_id: str, job: JobInfo | None) -> str | None:
        """The job's leader as the lease coordinator records it, else as the job records it."""
        return self._leases.get_job_leader(job_id) or self._job_record_leader_id(job)

    @staticmethod
    def _job_record_leader_id(job: JobInfo | None) -> str | None:
        """The leader node id the job records, or None for no job."""
        return job.leader_node_id if job is not None else None

    def _recorded_job_leader_addr(self, job_id: str, job: JobInfo | None) -> tuple[str, int] | None:
        """The job's leader address as the manager state records it, else as the job records it."""
        return self._manager_state.get_job_leader_addr(job_id) or self._job_record_leader_addr(job)

    @staticmethod
    def _job_record_leader_addr(job: JobInfo | None) -> tuple[str, int] | None:
        """The leader address the job records, or None for no job."""
        return job.leader_addr if job is not None else None

    def _job_sync_progress(
        self,
        job: JobInfo | None,
    ) -> tuple[
        str,
        int,
        int,
        int,
        dict[str, str],
        dict[str, WorkflowStateSnapshot],
        dict[str, SubWorkflowStateSnapshot],
        dict[str, dict[str, object]],
        int,
    ]:
        """
        The job's progress fields for a state sync, or a running job's empty progress when there is no job.

        Returns, in order: the job's status, its total, completed and failed
        workflow counts, its workflow statuses, workflow snapshots and
        sub-workflow snapshots, its context snapshot, and its layer version.
        """
        if job is None:
            return JobStatus.RUNNING.value, 0, 0, 0, {}, {}, {}, {}, 0
        workflow_statuses = {
            wf_id: wf.status.value for wf_id, wf in job.workflows.items()
        }
        workflow_snapshots = self._build_workflow_state_snapshots(job)
        sub_workflow_snapshots = self._build_sub_workflow_state_snapshots(job)
        context_snapshot = job.context.dict()
        return (
            job.status,
            job.workflows_total,
            job.workflows_completed,
            job.workflows_failed,
            workflow_statuses,
            workflow_snapshots,
            sub_workflow_snapshots,
            context_snapshot,
            job.layer_version,
        )

    def _job_raft_voters(self, job_id: str) -> list[str]:
        """The sorted initial voters of the job's consensus group, or an empty list when this manager holds none."""
        return (
            sorted(job_group.initial_voters)
            if (job_group := self._raft.consensus.get_node(job_id)) is not None
            else []
        )

    def _build_workflow_state_snapshots(
        self,
        job: JobInfo,
    ) -> dict[str, WorkflowStateSnapshot]:
        snapshots: dict[str, WorkflowStateSnapshot] = {}
        lifecycle = self._job_manager.workflow_lifecycle
        for workflow_token, workflow in job.workflows.items():
            snapshots[workflow_token] = self._workflow_state_snapshot(job, workflow, lifecycle)

        return snapshots

    def _workflow_state_snapshot(
        self,
        job: JobInfo,
        workflow: WorkflowInfo,
        lifecycle: WorkflowLifecycleStateMachine,
    ) -> WorkflowStateSnapshot:
        """One workflow's snapshot: its status with its AD-54 lifecycle."""
        status = self._workflow_status_text(workflow)

        # The lifecycle travels with the status (AD-54): a manager that
        # takes the job over knows where each workflow is, how many times
        # it was retried, and what it waits on.
        lifecycle_record = lifecycle.get_record(
            job.job_id, self._workflow_token_id(workflow)
        )
        lifecycle_state, retry_generation = self._lifecycle_snapshot_fields(lifecycle_record)
        return {
            "token": str(workflow.token),
            "name": workflow.name,
            "status": status,
            "lifecycle_state": lifecycle_state,
            "retry_generation": retry_generation,
            "dependency_workflow_ids": sorted(workflow.dependency_workflow_ids),
            "is_test": workflow.is_test,
            "sub_workflow_tokens": list(workflow.sub_workflow_tokens),
            "error": workflow.error,
            "aggregation_error": workflow.aggregation_error,
            "terminal_pushed": workflow.terminal_pushed,
            "terminal_status": workflow.terminal_status,
        }

    def _workflow_status_text(self, workflow: WorkflowInfo) -> str:
        """The workflow's status as its string value."""
        if isinstance(workflow.status, WorkflowStatus):
            return workflow.status.value
        return str(workflow.status)

    def _lifecycle_snapshot_fields(
        self,
        lifecycle_record: WorkflowLifecycleRecord | None,
    ) -> tuple[str | None, int]:
        """The record's lifecycle state value and retry generation (None, 0 without one)."""
        return (
            None if lifecycle_record is None else lifecycle_record.state.value,
            0 if lifecycle_record is None else lifecycle_record.retry_generation,
        )

    def _build_sub_workflow_state_snapshots(
        self,
        job: JobInfo,
    ) -> dict[str, SubWorkflowStateSnapshot]:
        snapshots: dict[str, SubWorkflowStateSnapshot] = {}
        for sub_workflow_token, sub_workflow in job.sub_workflows.items():
            snapshots[sub_workflow_token] = {
                "token": str(sub_workflow.token),
                "parent_token": str(sub_workflow.parent_token),
                "cores_allocated": sub_workflow.cores_allocated,
                "fence_token": sub_workflow.fence_token,
                "progress": sub_workflow.progress,
                "result": sub_workflow.result,
                "dispatched_context": sub_workflow.dispatched_context,
                "dispatched_version": sub_workflow.dispatched_version,
                "superseded": sub_workflow.superseded,
            }

        return snapshots

    async def _sync_job_state_to_peers(
        self,
        job_id: str,
        job: JobInfo | None,
        *,
        require_quorum: bool = False,
    ) -> bool:
        sync_msg = self._build_job_state_sync_message(job_id, job)
        return await self._sync_job_state_message_to_peers(
            sync_msg,
            require_quorum=require_quorum,
        )

    async def _sync_job_state_message_to_peers(
        self,
        sync_msg: JobStateSyncMessage,
        *,
        require_quorum: bool = False,
    ) -> bool:
        peer_addrs = sorted(self._manager_state.get_active_manager_peers())
        if not peer_addrs:
            return self._job_state_sync_satisfied_without_peers(require_quorum)

        return await self._sync_job_state_to_peer_addrs(sync_msg, peer_addrs, require_quorum)

    def _job_state_sync_satisfied_without_peers(self, require_quorum: bool) -> bool:
        """With no active peer, a sync is satisfied unless it needs a quorum of more than one."""
        return not require_quorum or self._leadership.get_quorum_size() <= 1

    async def _sync_job_state_to_peer_addrs(
        self,
        sync_msg: JobStateSyncMessage,
        peer_addrs: list[tuple[str, int]],
        require_quorum: bool,
    ) -> bool:
        """Send the sync to every peer concurrently; True unless a required quorum missed."""
        results = await asyncio.gather(
            *(self._send_job_state_sync(sync_msg, peer_addr) for peer_addr in peer_addrs)
        )
        replicated_count = 1 + self._count_accepted_syncs(results)
        if require_quorum:
            return replicated_count >= self._leadership.get_quorum_size()

        return True

    def _count_accepted_syncs(self, results: list[bool]) -> int:
        """How many peers accepted the sync."""
        return sum(1 for accepted in results if accepted)

    async def _send_job_state_sync(
        self,
        sync_msg: JobStateSyncMessage,
        peer_addr: tuple[str, int],
    ) -> bool:
        """Send one peer the job state sync; True when it accepted (failures logged)."""
        try:
            response = await self._send_to_peer(
                peer_addr,
                "job_state_sync",
                sync_msg.dump(),
                timeout=self._config.tcp_timeout_short_seconds,
            )
            return self._job_state_sync_accepted(response)
        except Exception as sync_error:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Peer job state sync to {peer_addr} failed: {sync_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

    def _job_state_sync_accepted(self, response: bytes | Exception | None) -> bool:
        """Whether the peer's ``JobStateSyncAck`` accepted; raises a transport error."""
        # send_tcp returns transport errors rather than raising.
        if isinstance(response, Exception):
            raise response
        if not response:
            return False

        ack = JobStateSyncAck.load(response)
        return ack.accepted

    async def _replicate_job_state_for_dispatch(self, job_id: str) -> bool:
        """Replicate executable job state before dispatch leaves this manager."""
        job = self._job_manager.get_job_by_id(job_id)
        if job is None:
            return False

        return await self._sync_job_state_to_peers(
            job_id,
            job,
            require_quorum=True,
        )

    async def _peer_job_state_sync_loop(self) -> None:
        """
        Background loop for periodic job state sync to peer managers.

        Syncs job state (leadership, fencing tokens, context versions)
        to ensure consistency across manager cluster.
        """
        sync_interval = self._config.peer_job_sync_interval_seconds

        await self._run_background_loop(
            lambda: self._peer_job_state_sync_iteration(sync_interval),
            "Peer job state sync error",
        )

    async def _peer_job_state_sync_iteration(self, sync_interval: float) -> bool:
        """One round syncing every job this manager leads to its peers."""
        await self._clock.sleep(sync_interval)

        # Every manager syncs the jobs it leads: job leadership
        # (AD-31) is not datacenter leadership, and peers accept a
        # sync from the job's leader whoever leads the datacenter.
        led_jobs = self._leases.get_led_job_ids()
        if not led_jobs:
            return True

        await self._sync_led_jobs_to_peers(led_jobs)
        return True

    async def _sync_led_jobs_to_peers(self, led_jobs: list[str]) -> None:
        """Sync each led job that still exists to the peer managers (AD-31)."""
        for job_id in led_jobs:
            if (job := self._job_manager.get_job_by_id(job_id)) is None:
                continue
            await self._sync_job_state_to_peers(job_id, job)

    async def _resource_gossip_loop(self) -> None:
        """AD-41 Part 4: send every peer manager this datacenter's fresh
        resource reports once per heartbeat interval -- the cadence this
        manager's report reaches its gates at, so a gateless client's view is
        as fresh as a gate's."""
        await self._run_background_loop(
            lambda: self._resource_gossip_iteration(),
            "Resource gossip error",
        )

    async def _resource_gossip_iteration(self) -> bool:
        """One AD-41 gossip round; False once the manager stopped during the sleep."""
        await self._clock.sleep(self._config.heartbeat_interval_seconds)
        if not self._running:
            return False

        await self._gossip_resources_to_peers()
        return True

    async def _gossip_resources_to_peers(self) -> None:
        """Record this manager's report and send the gossip to every active peer (AD-41)."""
        self._resource_gossip.record_own_report(self._build_resource_report())
        payload = self._resource_gossip.gossip_message().dump()
        peer_addresses = sorted(self._manager_state.get_active_manager_peers())
        responses = await asyncio.gather(
            *(
                self._send_to_peer(
                    peer_address,
                    "manager_resource_gossip",
                    payload,
                    timeout=self._config.tcp_timeout_short_seconds,
                )
                for peer_address in peer_addresses
            ),
            return_exceptions=True,
        )
        for peer_address, response in zip(peer_addresses, responses):
            await self._log_untaken_resource_gossip(peer_address, response)

    async def _log_untaken_resource_gossip(
        self,
        peer_address: tuple[str, int],
        response: bytes | BaseException,
    ) -> None:
        """Debug-log a peer that did not take this round's gossip."""
        if isinstance(response, Exception) or response != b"ok":
            # A peer that missed a round hears the next; a dead
            # one leaves the active peers.
            await self._udp_logger.log(
                ServerDebug(
                    message=(
                        f"Resource gossip to peer manager {peer_address} "
                        f"not taken: {response!r}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _resource_sample_loop(self) -> None:
        """
        Background loop for periodic CPU/memory sampling.

        Samples manager's own resource usage and feeds to HybridOverloadDetector
        for overload state classification, every OVERLOAD_SAMPLE_INTERVAL_SECONDS.
        """
        sample_interval = self.env.OVERLOAD_SAMPLE_INTERVAL_SECONDS

        await self._run_background_loop(
            lambda: self._resource_sample_iteration(sample_interval),
            "Resource sampling error",
            ServerWarning,
        )

    async def _resource_sample_iteration(self, sample_interval: float) -> bool:
        """One CPU/memory sample fed to the overload detector."""
        await self._clock.sleep(sample_interval)

        metrics = await self._resource_monitor.sample()
        self._last_resource_metrics = metrics

        new_state = self._overload_detector.get_state(
            metrics.cpu_percent,
            metrics.memory_percent,
        )
        new_state_str = new_state.value

        (
            previous_state,
            current_state,
            changed,
        ) = await self._set_manager_health_state(new_state_str)
        if changed:
            await self._log_manager_health_transition(previous_state, current_state)
        return True

    async def _log_manager_health_transition(
        self,
        previous_state: str,
        new_state: str,
    ) -> None:
        """Log manager health state transitions."""
        state_severity = {"healthy": 0, "busy": 1, "stressed": 2, "overloaded": 3}
        previous_severity = state_severity.get(previous_state, 0)
        new_severity = state_severity.get(new_state, 0)
        is_degradation = new_severity > previous_severity

        if is_degradation:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Manager health degraded: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
        else:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Manager health improved: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    async def _log_peer_manager_health_transition(
        self,
        peer_id: str,
        previous_state: str,
        new_state: str,
    ) -> None:
        state_severity = {"healthy": 0, "busy": 1, "stressed": 2, "overloaded": 3}
        previous_severity = state_severity.get(previous_state, 0)
        new_severity = state_severity.get(new_state, 0)
        is_degradation = new_severity > previous_severity

        if is_degradation:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Peer manager {peer_id[:8]}... health degraded: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
        else:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Peer manager {peer_id[:8]}... health improved: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    # =========================================================================
    # State Sync
    # =========================================================================


    async def _scan_for_orphaned_jobs(self) -> None:
        """Take over the unfinished jobs of dead managers.

        Run every orphan scan while this node is the SWIM cluster leader:
        the takeover attempted when a job's leader died happens once, on
        whichever node led the cluster then, and one that could not finish
        -- no quorum then, leadership changing under it -- left the job
        with no leader for good. A job whose copy here ended is settled
        and not taken over again.
        """
        dead_managers = set(self._manager_state.get_dead_managers())
        status_order = JobStatusOrder()
        for job_id, leader_addr in self._manager_state.iter_job_leader_addrs():
            if self._job_needs_orphan_takeover(job_id, leader_addr, dead_managers, status_order):
                await self._take_over_job_leadership_as_cluster_leader(
                    job_id,
                    self._leases.get_job_leader(job_id),
                )

    def _job_needs_orphan_takeover(
        self,
        job_id: str,
        leader_addr: tuple[str, int],
        dead_managers: set[tuple[str, int]],
        status_order: JobStatusOrder,
    ) -> bool:
        """True for a job led by a dead manager whose copy here has not ended."""
        if leader_addr not in dead_managers:
            return False
        return not self._held_job_has_ended(job_id, status_order)

    def _held_job_has_ended(self, job_id: str, status_order: JobStatusOrder) -> bool:
        """True when the job's copy here is in a terminal status."""
        return (job := self._job_manager.get_job_by_id(job_id)) is not None and (
            status_order.is_terminal(job.status)
        )

    async def _resume_timeout_tracking_for_all_jobs(self) -> None:
        """Resume timeout tracking for all jobs as new leader."""
        for job_id in self._leases.get_led_job_ids():
            strategy = self._manager_state.get_job_timeout_strategy(job_id)
            if strategy:
                await strategy.resume_tracking(job_id)

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _get_swim_status_for_worker(
        self, worker_udp_addr: tuple[str, int]
    ) -> str | None:
        """SWIM membership status for a worker, by UDP address.

        The ``WorkerPool`` health check consults this with the worker's
        UDP address and expects the SWIM tracker vocabulary
        ("OK"/"SUSPECT"/"DEAD") — the previous implementation took a
        worker_id string and returned "healthy"/"unhealthy", so the
        pool's SWIM branch compared a tuple-keyed lookup's fallback
        against words that could never match: dead code, and the pool
        fell through to heartbeat staleness alone. Returns None for
        nodes SWIM has no state for (never probed/confirmed), letting
        the pool fall through to its explicit-health and grace-period
        checks exactly as before.
        """
        node_state = self._incarnation_tracker.get_node_state(worker_udp_addr)
        if node_state is None:
            return None
        status = node_state.status
        return status.decode() if isinstance(status, bytes) else str(status)

    def _get_active_workflow_count(self) -> int:
        """Get count of active workflows."""
        return sum(
            self._running_workflow_count(job)
            for job in self._job_manager.iter_jobs()
        )

    def _running_workflow_count(self, job: JobInfo) -> int:
        """How many of the job's workflows are RUNNING."""
        return len(
            [
                w
                for w in job.workflows.values()
                if w.status == WorkflowStatus.RUNNING
            ]
        )

    def _get_available_cores_for_healthy_workers(self) -> int:
        """Get total available cores across healthy workers.

        Uses WorkerPool which tracks real-time worker capacity from heartbeats,
        rather than stale WorkerRegistration data from initial registration.
        """
        return self._worker_pool.get_total_available_cores()

    def _get_total_cores(self) -> int:
        """Get total cores across all workers."""
        return sum(
            w.total_cores for w in self._manager_state.get_all_workers().values()
        )

    def _get_job_worker_count(self, job_id: str) -> int:
        """Get number of unique workers assigned to a job's sub-workflows."""
        job = self._job_manager.get_job(job_id)
        if not job:
            return 0
        return len(self._job_sub_workflow_token_worker_ids(job))

    def _job_sub_workflow_token_worker_ids(self, job: JobInfo) -> set[str]:
        """The distinct workers the job's sub-workflow tokens are bound to."""
        return {
            sub_wf.token.worker_id
            for sub_wf in job.sub_workflows.values()
            if sub_wf.token.worker_id
        }

    def _get_active_job_workflows_by_worker(self, job: JobInfo) -> dict[str, list[str]]:
        workflow_ids_by_worker: dict[str, set[str]] = {}
        for sub_workflow in job.sub_workflows.values():
            if self._is_active_sub_workflow_of_running_parent(job, sub_workflow):
                workflow_ids_by_worker.setdefault(sub_workflow.worker_id, set()).add(
                    str(sub_workflow.token)
                )

        return self._listed_workflow_ids_by_worker(workflow_ids_by_worker)

    def _is_active_sub_workflow_of_running_parent(
        self,
        job: JobInfo,
        sub_workflow: SubWorkflowInfo,
    ) -> bool:
        """True for an unfinished sub bound to a worker whose parent is RUNNING
        (or no longer known)."""
        if sub_workflow.result is not None or not sub_workflow.worker_id:
            return False

        return self._parent_workflow_running_or_unknown(job, sub_workflow)

    def _parent_workflow_running_or_unknown(
        self,
        job: JobInfo,
        sub_workflow: SubWorkflowInfo,
    ) -> bool:
        """False only when the sub's parent workflow is known and not RUNNING."""
        workflow_info = job.workflows.get(str(sub_workflow.parent_token))
        return not (workflow_info and workflow_info.status != WorkflowStatus.RUNNING)

    def _listed_workflow_ids_by_worker(
        self,
        workflow_ids_by_worker: dict[str, set[str]],
    ) -> dict[str, list[str]]:
        """Each worker's workflow ids as a list."""
        return {
            worker_id: list(workflow_ids)
            for worker_id, workflow_ids in workflow_ids_by_worker.items()
        }

    def _get_worker_registration_for_transfer(
        self, worker_id: str
    ) -> WorkerRegistration | None:
        if (registration := self._manager_state.get_worker(worker_id)) is not None:
            return registration

        return self._worker_pool_registration(worker_id)

    def _worker_pool_registration(self, worker_id: str) -> WorkerRegistration | None:
        """The registration the worker pool holds for the worker, if any."""
        worker_status = self._worker_pool.get_worker(worker_id)
        if worker_status and worker_status.registration:
            return worker_status.registration

        return None

    async def _notify_workers_job_leader_transfer(
        self,
        job_id: str,
        old_leader_id: str | None,
    ) -> None:
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Skipped worker leader transfer; job not found: "
                        f"{job_id[:8]}..."
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return

        await self._notify_job_workers_of_leader_transfer(job_id, job, old_leader_id)

    async def _notify_job_workers_of_leader_transfer(
        self,
        job_id: str,
        job: JobInfo,
        old_leader_id: str | None,
    ) -> None:
        """Send each worker running the job's workflows the leader transfer."""
        async with job.lock:
            workflows_by_worker = self._get_active_job_workflows_by_worker(job)

        if not workflows_by_worker:
            return

        fence_token = self._leases.get_fence_token(job_id)

        for worker_id, workflow_ids in workflows_by_worker.items():
            await self._notify_worker_job_leader_transfer(
                job_id, old_leader_id, fence_token, worker_id, workflow_ids
            )

    async def _notify_worker_job_leader_transfer(
        self,
        job_id: str,
        old_leader_id: str | None,
        fence_token: int,
        worker_id: str,
        workflow_ids: list[str],
    ) -> None:
        """Send one registered worker the leader transfer; log an unregistered one."""
        worker_registration = self._get_worker_registration_for_transfer(worker_id)
        if worker_registration is None:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Cannot notify worker of leader transfer; "
                        f"worker {worker_id[:8]}... not registered "
                        f"for job {job_id[:8]}..."
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return

        await self._send_job_leader_transfer(
            job_id, old_leader_id, fence_token, worker_id, workflow_ids, worker_registration
        )

    async def _send_job_leader_transfer(
        self,
        job_id: str,
        old_leader_id: str | None,
        fence_token: int,
        worker_id: str,
        workflow_ids: list[str],
        worker_registration: WorkerRegistration,
    ) -> None:
        """Send the ``JobLeaderWorkerTransfer`` and check the worker's ack."""
        worker_addr = (
            worker_registration.node.host,
            worker_registration.node.port,
        )
        transfer = JobLeaderWorkerTransfer(
            job_id=job_id,
            workflow_ids=workflow_ids,
            new_manager_id=self._node_id.full,
            new_manager_addr=(self._host, self._tcp_port),
            fence_token=fence_token,
            old_manager_id=old_leader_id,
        )

        try:
            response = await self._send_to_worker(
                worker_addr,
                "job_leader_worker_transfer",
                transfer.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
        except Exception as error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Leader transfer notification failed for job "
                        f"{job_id[:8]}... to worker {worker_id[:8]}...: {error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return

        await self._check_job_leader_transfer_response(job_id, worker_id, response)

    async def _check_job_leader_transfer_response(
        self,
        job_id: str,
        worker_id: str,
        response: bytes | Exception | None,
    ) -> None:
        """Log a missing response, else decode and check the worker's ack."""
        if self._is_missing_transfer_response(response):
            await self._log_missing_leader_transfer_response(job_id, worker_id, response)
            return

        await self._check_job_leader_transfer_ack(job_id, worker_id, response)

    def _is_missing_transfer_response(self, response: bytes | Exception | None) -> bool:
        """True for a transport error, no response, or an empty one."""
        return isinstance(response, Exception) or response is None or not response

    async def _log_missing_leader_transfer_response(
        self,
        job_id: str,
        worker_id: str,
        response: bytes | Exception | None,
    ) -> None:
        """Log the worker's missing leader-transfer response."""
        error_message = (
            str(response) if isinstance(response, Exception) else "no response"
        )
        await self._udp_logger.log(
            ServerWarning(
                message=(
                    "Leader transfer notification missing response for job "
                    f"{job_id[:8]}... worker {worker_id[:8]}...: {error_message}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _check_job_leader_transfer_ack(
        self,
        job_id: str,
        worker_id: str,
        response: bytes,
    ) -> None:
        """Decode the worker's ack; log a decode failure or a rejection."""
        try:
            ack = JobLeaderWorkerTransferAck.load(response)
        except Exception as decode_error:
            # ``response`` passed the bytes / Exception / empty
            # checks above but the payload still failed to
            # deserialize — most commonly because the worker's
            # TCP handler returned a framing error indicator
            # rather than a valid ack. Treat as a transfer
            # rejection that the worker will need to resolve on
            # its next ``worker_register`` cycle. Letting the
            # exception bubble would surface to the cancel
            # handler's outer try/except as
            # ``Job cancellation failed: unpickling stack
            # underflow`` and abort an otherwise-recoverable
            # cancellation.
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Failed to decode leader transfer ack from worker "
                        f"{worker_id[:8]}... for job {job_id[:8]}...: "
                        f"{decode_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return
        if not ack.accepted:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Worker rejected leader transfer for job "
                        f"{job_id[:8]}... worker {worker_id[:8]}...: "
                        f"{ack.rejection_reason}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _get_known_gates_for_heartbeat(self) -> list[GateInfo]:
        """Get known gates for heartbeat embedding."""
        return self._manager_state.get_known_gate_values()

    def _get_job_leaderships_for_heartbeat(self) -> list[str]:
        """Get job leaderships for heartbeat embedding."""
        return self._leases.get_led_job_ids()

    async def _check_rate_limit_for_operation(
        self,
        client_id: str,
        operation: str,
        handler_name: str,
    ) -> tuple[bool, float]:
        """
        Check if a client request is within rate limits for a specific operation.

        The check runs at the priority ``handler_name``'s AD-37 message class
        assigns: cancellation and extensions are CONTROL (CRITICAL), so an
        overloaded manager never refuses them (AD-20/AD-37).

        Args:
            client_id: Identifier for the client (typically addr as string)
            operation: AD-24 operation whose budget the request draws from
            handler_name: The TCP handler serving the request

        Returns:
            Tuple of (allowed, retry_after_seconds). If not allowed,
            retry_after_seconds indicates when client can retry.
        """
        result = await self._rate_limiter.check_rate_limit_with_priority(
            client_id, operation, classify_handler_to_priority(handler_name)
        )
        return result.allowed, result.retry_after_seconds

    async def _cleanup_inactive_rate_limit_clients(self) -> int:
        """
        Clean up inactive clients from rate limiter.

        Returns:
            Number of clients cleaned up
        """
        return await self._rate_limiter.cleanup_inactive_clients()


    def _max_worker_lhm_score(self) -> int:
        """The highest registered worker LHM score; 0 with none reporting (AD-19)."""
        worker_lhm_scores = self._manager_state._worker_lhm_scores
        return max(worker_lhm_scores.values()) if worker_lhm_scores else 0

    def _build_manager_heartbeat(self) -> ManagerHeartbeat:
        health_state_counts = self._worker_health_monitor.get_worker_health_state_counts()
        # AD-19 addendum (Phase D): aggregate worker-tier LHM as the
        # max across registered workers. Empty dict -> 0 (no workers
        # reporting yet, or all evicted).
        worker_max_lhm = self._max_worker_lhm_score()
        # AD-42 Phase E2: per-DC SLO summary built from the manager's
        # T-Digest. Embedded in the heartbeat so gates see fresh
        # latency percentiles every probe interval without an extra
        # RPC. Returns SLOSummary.empty() (neutral baseline) when no
        # workflow latencies have been recorded yet.
        slo_summary = self._manager_state.get_slo_summary(self._clock.monotonic())
        (
            pending_workflow_count,
            pending_duration_seconds,
            active_remaining_seconds,
            cores_freeing_schedule,
        ) = self._capacity_reporter.snapshot()
        return ManagerHeartbeat(
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            # AD-28: real isolation ids, not the model's "hyperscale"/
            # "default" placeholders -- a gate validating isolation on
            # this heartbeat must see the configured values.
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            version=self._manager_state.state_version,
            active_jobs=self._job_manager.job_count,
            active_workflows=sum(
                len(getattr(job, "workflows", {}) or {})
                for job in self._job_manager.iter_jobs()
            ),
            state=self._manager_state.manager_state_enum.value,
            worker_count=self._manager_state.get_worker_count(),
            healthy_worker_count=len(self._registry.get_healthy_worker_ids()),
            available_cores=self._get_available_cores_for_healthy_workers(),
            total_cores=self._get_total_cores(),
            # AD-43: the gate's wait-estimation inputs (were never set, so
            # every heartbeat shipped zeros and spillover saw no backlog).
            pending_workflow_count=pending_workflow_count,
            pending_duration_seconds=pending_duration_seconds,
            active_remaining_seconds=active_remaining_seconds,
            cores_freeing_schedule=cores_freeing_schedule,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            udp_host=self._host,
            udp_port=self._udp_port,
            overloaded_worker_count=health_state_counts.get("overloaded", 0),
            stressed_worker_count=health_state_counts.get("stressed", 0),
            busy_worker_count=health_state_counts.get("busy", 0),
            # AD-19/AD-43 capacity piggyback, from the same sources the
            # SWIM-embedded heartbeat already reports. The model's
            # defaults (accepting=True, has_quorum=True, throughput=0)
            # made every manager read "ready" at the gate regardless of
            # its actual state: the gate's health coordinator feeds
            # these two booleans straight into ManagerHealthState
            # readiness, so a draining or quorum-less manager kept
            # receiving jobs.
            health_accepting_jobs=self._is_accepting_jobs(),
            health_has_quorum=self._leadership.has_quorum(),
            health_throughput=self._stats.get_dispatch_throughput(),
            health_expected_throughput=self._stats.get_expected_throughput(),
            health_overload_state=self._manager_health_state_snapshot,
            # AD-19 addendum (Phase D): manager's own LHM + max worker LHM
            lhm_score=self._local_health.score,
            worker_max_lhm_score=worker_max_lhm,
            # AD-42 Phase E2 SLO piggyback
            slo_p50_ms=slo_summary.p50_ms,
            slo_p95_ms=slo_summary.p95_ms,
            slo_p99_ms=slo_summary.p99_ms,
            slo_sample_count=slo_summary.sample_count,
            slo_compliance_score=slo_summary.compliance_score,
            slo_routing_factor=slo_summary.routing_factor,
            slo_updated_at=slo_summary.updated_at,
            resource_report=self._build_resource_report(),
            storage_writable=self._is_storage_writable(),
        )

    async def _build_xprobe_response(
        self,
        source_addr: tuple[str, int] | bytes,
        probe_data: bytes,
    ) -> bytes | None:
        """Build a federated health acknowledgment for gate xprobes."""
        if not self.is_leader():
            return None

        heartbeat = self._build_manager_heartbeat()
        healthy_managers = self._manager_state.get_active_peer_count()
        cluster_size = max(
            healthy_managers,
            self._manager_state.get_known_manager_peer_count() + 1,
            len(self._cluster_membership.cohort),
        )
        incarnation = await self._manager_state.increment_external_incarnation()
        datacenter_health = self._classify_xprobe_datacenter_health(
            heartbeat,
            healthy_managers,
            cluster_size,
        )

        return CrossClusterAck(
            datacenter=heartbeat.datacenter,
            node_id=heartbeat.node_id,
            incarnation=incarnation,
            is_leader=heartbeat.is_leader,
            leader_term=heartbeat.term,
            cluster_size=cluster_size,
            healthy_managers=healthy_managers,
            worker_count=heartbeat.worker_count,
            healthy_workers=heartbeat.healthy_worker_count,
            total_cores=heartbeat.total_cores,
            available_cores=heartbeat.available_cores,
            active_jobs=heartbeat.active_jobs,
            active_workflows=heartbeat.active_workflows,
            dc_health=datacenter_health.value.upper(),
            health_reason=self._get_xprobe_health_reason(
                heartbeat,
                healthy_managers,
                cluster_size,
                datacenter_health,
            ),
        ).dump()

    def _classify_xprobe_datacenter_health(
        self,
        heartbeat: ManagerHeartbeat,
        healthy_managers: int,
        cluster_size: int,
    ) -> DatacenterHealth:
        """Classify this DC for a federated xprobe ack."""
        if heartbeat.worker_count == 0 or healthy_managers == 0:
            return DatacenterHealth.UNHEALTHY

        return self._classify_reachable_xprobe_datacenter_health(
            heartbeat, healthy_managers, cluster_size
        )

    def _classify_reachable_xprobe_datacenter_health(
        self,
        heartbeat: ManagerHeartbeat,
        healthy_managers: int,
        cluster_size: int,
    ) -> DatacenterHealth:
        """DEGRADED without quorum, else classify by load."""
        if self._xprobe_quorum_unavailable(heartbeat, healthy_managers, cluster_size):
            return DatacenterHealth.DEGRADED

        return self._classify_xprobe_datacenter_load(heartbeat)

    def _xprobe_quorum_unavailable(
        self,
        heartbeat: ManagerHeartbeat,
        healthy_managers: int,
        cluster_size: int,
    ) -> bool:
        """True without a manager, worker or leadership quorum."""
        quorum_size = cluster_size // 2 + 1
        worker_quorum = heartbeat.worker_count // 2 + 1
        return (
            healthy_managers < quorum_size
            or heartbeat.healthy_worker_count < worker_quorum
            or not self._leadership.has_quorum()
        )

    def _classify_xprobe_datacenter_load(self, heartbeat: ManagerHeartbeat) -> DatacenterHealth:
        """DEGRADED when overloaded, BUSY with no free core, else HEALTHY."""
        if self._manager_health_state_snapshot == "overloaded":
            return DatacenterHealth.DEGRADED

        if heartbeat.available_cores <= 0:
            return DatacenterHealth.BUSY

        return DatacenterHealth.HEALTHY

    def _get_xprobe_health_reason(
        self,
        heartbeat: ManagerHeartbeat,
        healthy_managers: int,
        cluster_size: int,
        datacenter_health: DatacenterHealth,
    ) -> str:
        """Return a compact reason string for non-healthy xprobe acks."""
        if datacenter_health == DatacenterHealth.HEALTHY:
            return ""
        return self._xprobe_reachability_reason(
            heartbeat, healthy_managers, cluster_size, datacenter_health
        )

    def _xprobe_reachability_reason(
        self,
        heartbeat: ManagerHeartbeat,
        healthy_managers: int,
        cluster_size: int,
        datacenter_health: DatacenterHealth,
    ) -> str:
        """The reason when no worker or manager is reachable, else the quorum reasons."""
        if heartbeat.worker_count == 0:
            return "no workers registered"
        if healthy_managers == 0:
            return "no managers reachable"
        return self._xprobe_quorum_reason(
            heartbeat, healthy_managers, cluster_size, datacenter_health
        )

    def _xprobe_quorum_reason(
        self,
        heartbeat: ManagerHeartbeat,
        healthy_managers: int,
        cluster_size: int,
        datacenter_health: DatacenterHealth,
    ) -> str:
        """The reason when the manager or worker quorum is short, else the load reasons."""
        if healthy_managers < cluster_size // 2 + 1:
            return "manager quorum unavailable"
        if heartbeat.healthy_worker_count < heartbeat.worker_count // 2 + 1:
            return "worker quorum unavailable"
        return self._xprobe_leadership_reason(heartbeat, datacenter_health)

    def _xprobe_leadership_reason(
        self,
        heartbeat: ManagerHeartbeat,
        datacenter_health: DatacenterHealth,
    ) -> str:
        """The reason when leadership lacks quorum or the manager is overloaded."""
        if not self._leadership.has_quorum():
            return "manager leadership quorum unavailable"
        if self._manager_health_state_snapshot == "overloaded":
            return "manager overloaded"
        return self._xprobe_capacity_reason(heartbeat, datacenter_health)

    def _xprobe_capacity_reason(
        self,
        heartbeat: ManagerHeartbeat,
        datacenter_health: DatacenterHealth,
    ) -> str:
        """The "all cores busy" reason without a free core, else the health value."""
        if heartbeat.available_cores <= 0:
            return "all cores busy"
        return datacenter_health.value

    def _get_healthy_gate_tcp_addrs(self) -> list[tuple[str, int]]:
        """Get TCP addresses of healthy gates."""
        healthy_gate_ids = self._manager_state.get_healthy_gate_ids()
        return [
            (gate.tcp_host, gate.tcp_port)
            for gate_id, gate in self._manager_state.iter_known_gates()
            if gate_id in healthy_gate_ids
        ]

    def _get_worker_state_piggyback(self, max_size: int) -> bytes:
        if self._worker_disseminator is None:
            return b""
        return self._worker_disseminator.get_gossip_buffer().encode_piggyback(
            max_count=5,
            max_size=max_size,
        )

    async def _process_worker_state_piggyback(
        self,
        piggyback_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        if self._worker_disseminator is None:
            return

        updates = WorkerStateGossipBuffer.decode_piggyback(piggyback_data)
        for update in updates:
            await self._worker_disseminator.handle_worker_state_update(
                update, source_addr
            )

    def _get_extension_decision_piggyback(self, max_size: int) -> bytes:
        """AD-26 H7b: encode pending extension decisions into a
        ``#|x``-prefixed piggyback frame bounded by ``max_size``.
        """
        return self._extension_decision_buffer.encode_piggyback(
            max_count=5,
            max_size=max_size,
        )

    async def _process_extension_decision_piggyback(
        self,
        piggyback_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """AD-26 H7b: decode an inbound ``#|x`` frame and ingest each
        event into the local ``WorkerHealthManager`` ledger.

        ``ExtensionLedger.record`` is idempotent on
        (workflow_id, fence_token, timestamp) and rejects stale-term
        events, so re-disseminated events from multiple peers
        collapse to one ledger entry. We also re-add accepted events
        to the local buffer so this manager continues their
        dissemination — that's how AD-48's
        ``broadcast_multiplier × log(n+1)`` cluster fan-out is
        achieved.
        """
        events = ExtensionDecisionGossipBuffer.decode_piggyback(piggyback_data)
        if not events:
            return
        number_of_managers = len(self._manager_state._active_manager_peers) + 1
        for event in events:
            self._worker_health_manager.ingest_remote_decision_event(event)
            self._extension_decision_buffer.add_event(
                event, number_of_managers=number_of_managers
            )

    def disseminate_extension_decision(
        self, event: ExtensionDecisionEvent
    ) -> None:
        """Queue a locally-produced extension decision for AD-48
        dissemination. Called by the manager request handler right
        after ``WorkerHealthManager.handle_extension_request_with_witnesses``
        commits the decision into the ledger.
        """
        number_of_managers = len(self._manager_state._active_manager_peers) + 1
        self._extension_decision_buffer.add_event(
            event, number_of_managers=number_of_managers
        )

    def _get_extension_outcome_piggyback(self, max_size: int) -> bytes:
        """AD-26 H8b: encode pending outcome events into a
        ``#|o``-prefixed piggyback frame bounded by ``max_size``.
        """
        return self._extension_outcome_buffer.encode_piggyback(
            max_count=5,
            max_size=max_size,
        )

    async def _process_extension_outcome_piggyback(
        self,
        piggyback_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """AD-26 H8b: decode an inbound ``#|o`` frame and ingest each
        outcome event into the local ``WorkerHealthManager``.

        The first copy of each workflow's outcome is applied and
        re-added to the local buffer so this manager continues its
        dissemination — same fan-out discipline as the decision
        channel. Repeat copies are dropped: re-counting them would
        skew the H8 posterior, and re-arming them would restart the
        epidemic once a peer's buffer had let the event go.
        """
        events = ExtensionOutcomeGossipBuffer.decode_piggyback(piggyback_data)
        number_of_managers = len(self._manager_state._active_manager_peers) + 1
        for event in events:
            if not self._worker_health_manager.ingest_remote_outcome_event(event):
                continue
            self._extension_outcome_buffer.add_event(
                event, number_of_managers=number_of_managers
            )

    def disseminate_extension_outcome(
        self, event: ExtensionOutcomeEvent
    ) -> None:
        """Queue a locally-produced outcome event for AD-48
        dissemination. Called by the workflow-termination path
        after ``WorkerHealthManager.record_workflow_outcome``
        emits the event.
        """
        number_of_managers = len(self._manager_state._active_manager_peers) + 1
        self._extension_outcome_buffer.add_event(
            event, number_of_managers=number_of_managers
        )

    def _route_extension_through_witnesses(
        self,
        *,
        request: HealthcheckExtensionRequest,
        current_deadline: float,
    ) -> "HealthcheckExtensionResponse | None":
        """Phase F1: route an extension request through the H5 multi-
        witness decision path when the request carries an H3
        ``workflow_id``.

        Returns the response on success, or ``None`` to fall back
        to the legacy path (e.g. the workflow can't be looked up
        in the local ``JobManager`` — typical when the workflow
        belongs to a job led by a different manager and our state
        is stale). The caller passes the result of this method
        through to its existing post-processing (deadline update,
        SWIM detector, dissemination, persistence).

        The H5 inputs that don't ride on the wire (throughput,
        overload_state, fence_token, leader_term, workflow_class)
        are reconstructed locally from:

        - ``WorkerPool.get_worker`` for the AD-19 heartbeat fields
        - ``JobManager`` for job/workflow lookup
        - ``LeadershipCoordinator`` for the cluster-wide leader term
        """
        snapshot = self._build_progress_snapshot_from_request(request)

        last_snapshot = (
            self._worker_health_manager.ledger.latest_progress_snapshot(
                request.workflow_id
            )
        )

        throughput, overload_state = self._worker_heartbeat_health(request.worker_id)

        job_id, workflow_class, fence_token = (
            self._lookup_workflow_context(request.workflow_id)
        )
        if job_id == "":
            # No local job context — fall back to legacy worker-
            # level path. The H5 path requires job/workflow context
            # to record a meaningful event.
            return None

        leader_term = self._leader_election.state.current_term

        active_on_worker = max(1, request.active_workflow_count)
        active_on_manager = max(active_on_worker, self._count_active_workflows())
        active_in_dc = active_on_manager
        active_in_cluster = active_on_manager

        response, decision, event = (
            self._worker_health_manager.handle_extension_request_with_witnesses(
                request=request,
                current_deadline=current_deadline,
                snapshot=snapshot,
                last_snapshot=last_snapshot,
                throughput=throughput,
                overload_state=overload_state,
                active_in_cluster=active_in_cluster,
                active_in_dc=active_in_dc,
                active_on_manager=active_on_manager,
                active_on_worker=active_on_worker,
                workflow_class=workflow_class,
                job_id=job_id,
                fence_token=fence_token,
                leader_term=leader_term,
            )
        )
        del decision  # Available for future hooks (alerting, etc.)
        # Disseminate via AD-48 #|x and persist into the job's
        # TimeoutTrackingState so leader transfer survives.
        self.disseminate_extension_decision(event)
        self._persist_decision_for_job(job_id, event)
        return response

    def _worker_heartbeat_health(self, worker_id: str) -> tuple[float, str]:
        """The worker's last heartbeat throughput and overload state (AD-19);
        (0.0, "healthy") without one."""
        throughput = 0.0
        overload_state = "healthy"
        worker_status = self._worker_pool.get_worker(worker_id)
        if worker_status is not None and worker_status.heartbeat is not None:
            throughput = worker_status.heartbeat.health_throughput
            overload_state = worker_status.heartbeat.health_overload_state
        return throughput, overload_state

    def _build_progress_snapshot_from_request(
        self, request: HealthcheckExtensionRequest
    ) -> WorkflowProgressSnapshot:
        """Materialize the wire-side H3 fields into a snapshot."""
        cores_completed = (
            request.completed_items if request.completed_items is not None else 0
        )
        cores_total = (
            request.total_items if request.total_items is not None else 0
        )
        return WorkflowProgressSnapshot(
            workflow_id=request.workflow_id,
            cores_completed=cores_completed,
            cores_total=cores_total,
            step_transitions=request.step_transitions,
            actions_completed=request.actions_completed,
            snapshot_time=request.snapshot_time,
        )

    def _lookup_workflow_context(
        self, workflow_id: str
    ) -> tuple[str, str, int]:
        """Resolve (job_id, workflow_class, fence_token) for a
        workflow_id. Returns ("", "", 0) when no matching job is
        tracked locally.

        Resolves BOTH id shapes on the wire: the bare parent workflow
        id ("wf-0001"), and the full SUB-workflow tracking-token string
        — ``WorkflowDispatch.workflow_id`` is ``str(sub_token)``, and
        the worker echoes exactly that id in its AD-26 extension
        requests. The sub-token shape used to miss here, which silently
        disabled the whole H5 witness route for every dispatch-time
        extension request (the legacy fallback records nothing in the
        H7 ledger — measured: ExtensionLedger empty across entire
        runs)."""
        for job in self._job_manager.iter_jobs():
            workflow_context = self._workflow_context_in_job(job, workflow_id)
            if workflow_context is not None:
                return workflow_context
        return "", "", 0

    def _workflow_context_in_job(
        self,
        job: JobInfo,
        workflow_id: str,
    ) -> tuple[str, str, int] | None:
        """The context of the job's parent workflow with this id, else of its sub."""
        for wf_info in job.workflows.values():
            if wf_info.token.workflow_id == workflow_id:
                return job.job_id, wf_info.name, job.fencing_token

        return self._sub_workflow_context_in_job(job, workflow_id)

    def _sub_workflow_context_in_job(
        self,
        job: JobInfo,
        workflow_id: str,
    ) -> tuple[str, str, int] | None:
        """The context of the job's sub-workflow keyed by this token string."""
        sub_workflow_info = job.sub_workflows.get(workflow_id)
        if sub_workflow_info is None:
            return None
        parent_info = job.workflows.get(
            str(sub_workflow_info.parent_token)
        )
        if parent_info is None:
            return None
        return job.job_id, parent_info.name, job.fencing_token

    def _count_active_workflows(self) -> int:
        """Count workflows actively running across all jobs on this
        manager. Used as the H6 ``active_on_manager`` input for
        hierarchical α-budget allocation."""
        return sum(
            self._running_workflow_count(job)
            for job in self._job_manager.iter_jobs()
        )

    def _persist_decision_for_job(
        self, job_id: str, event: ExtensionDecisionEvent
    ) -> None:
        """Mirror the just-recorded decision into the job's
        ``TimeoutTrackingState`` so leader takeover (AD-34) inherits
        the H7 ledger view. No-op if the job has no timeout
        tracking state (e.g. queue-only jobs)."""
        job_token = self._job_manager.create_job_token(job_id)
        job = self._job_manager.get_job(job_token)
        if job is None or job.timeout_tracking is None:
            return
        self._worker_health_manager.persist_decision_to_tracking(
            job.timeout_tracking, event
        )

    def _persist_outcome_for_job(
        self, job_id: str, event: ExtensionOutcomeEvent
    ) -> None:
        """Mirror the just-recorded outcome into the job's
        ``TimeoutTrackingState``. No-op if the job has no timeout
        tracking state."""
        job_token = self._job_manager.create_job_token(job_id)
        job = self._job_manager.get_job(job_token)
        if job is None or job.timeout_tracking is None:
            return
        self._worker_health_manager.persist_outcome_to_tracking(
            job.timeout_tracking, event
        )

    def _replay_extension_state_for_job(self, job_id: str) -> int:
        """Phase F4: replay persisted AD-26 H7/H8 state into the
        local ``WorkerHealthManager`` on leader takeover.

        Called after SWIM-leader takeover so the new leader immediately sees
        the previous leader's:

        - Most-recent decision per workflow (H7 ledger entries)
        - In-flight outcome events not yet disseminated (H8)
        - Frozen Beta posteriors per workflow class (H8 tuner)

        Returns the number of events replayed for observability.
        """
        job_token = self._job_manager.create_job_token(job_id)
        job = self._job_manager.get_job(job_token)
        if job is None or job.timeout_tracking is None:
            return 0
        return self._worker_health_manager.replay_persisted_state(
            job.timeout_tracking
        )

    def _emit_outcomes_for_terminal_job(
        self,
        job_id: str,
        outcome_kind: ExtensionOutcomeKind,
    ) -> None:
        """Phase F3: emit H8 outcome events for every still-in-flight
        workflow on a job that's hit a terminal state without going
        through the per-workflow ``WorkflowFinalResult`` path
        (timeout, cancellation, eviction).

        Iterates the job's sub-workflows: for each one without a
        result yet (i.e. truly in-flight), emits one outcome event
        keyed on the sub-workflow's ``workflow_id``. The Bayesian
        tuner sees one Bernoulli trial per dropped workflow,
        weighted by the most-recent progress snapshot the H7
        ledger has for that workflow.

        Idempotent: ``ExtensionLedger.record_outcome`` rejects
        re-records with equal-or-lower leader_term, and
        ``forget_workflow`` makes subsequent calls safe.
        """
        job_token = self._job_manager.create_job_token(job_id)
        job = self._job_manager.get_job(job_token)
        if job is None:
            return

        leader_term = self._leader_election.state.current_term
        completed_at = self._clock.monotonic()
        ledger = self._worker_health_manager.ledger

        for sub_info in list(job.sub_workflows.values()):
            self._emit_terminal_outcome_for_sub_workflow(
                job_id, job, sub_info, outcome_kind, leader_term, completed_at, ledger
            )

    def _emit_terminal_outcome_for_sub_workflow(
        self,
        job_id: str,
        job: JobInfo,
        sub_info: SubWorkflowInfo,
        outcome_kind: ExtensionOutcomeKind,
        leader_term: int,
        completed_at: float,
        ledger: ExtensionLedger,
    ) -> None:
        """Emit, disseminate and settle one in-flight sub-workflow's H8 outcome (Phase F3)."""
        workflow_id = self._in_flight_sub_workflow_id(sub_info)
        if not workflow_id:
            return

        progress_fraction = self._latest_progress_fraction(ledger, workflow_id)
        workflow_class = self._sub_workflow_class(job, sub_info)

        event = self._worker_health_manager.record_workflow_outcome(
            job_id=job_id,
            workflow_id=workflow_id,
            workflow_class=workflow_class,
            worker_id=sub_info.worker_id or "",
            outcome_kind=outcome_kind,
            final_progress_fraction=progress_fraction,
            completed_at=completed_at,
            fence_token=job.fencing_token,
            leader_term=leader_term,
        )
        self._settle_terminal_outcome(job, event, workflow_id)

    def _in_flight_sub_workflow_id(self, sub_info: SubWorkflowInfo) -> str:
        """The sub's workflow id while it has no result yet, else "".

        The id is the full sub-workflow token string: the id the
        workflow was dispatched under (``WorkflowDispatch.workflow_id``),
        so the one its extension requests, H7 ledger entry, H6 streams
        and ``WorkflowFinalResult`` carry. The token's bare
        ``workflow_id`` names the parent workflow shared by every sub:
        it found no ledger snapshot (every in-flight outcome read as
        zero progress, the heaviest failure weight) and forgot nothing.
        """
        if sub_info.result is not None:
            return ""
        return str(sub_info.token)

    def _latest_progress_fraction(self, ledger: ExtensionLedger, workflow_id: str) -> float:
        """The workflow's progress fraction from its most-recent H7 snapshot, capped at 1."""
        # Pull progress fraction from the most-recent H7
        # snapshot. cores_completed / cores_total = the AD-26
        # primary progress dimension.
        progress_fraction = 0.0
        snapshot = ledger.latest_progress_snapshot(workflow_id)
        if snapshot is not None and snapshot.cores_total > 0:
            progress_fraction = min(
                snapshot.cores_completed / snapshot.cores_total,
                1.0,
            )
        return progress_fraction

    def _sub_workflow_class(self, job: JobInfo, sub_info: SubWorkflowInfo) -> str:
        """The sub's workflow class: its parent WorkflowInfo's name, else ""."""
        # Look up workflow_class from the parent WorkflowInfo —
        # sub-workflows share their parent's name.
        workflow_class = ""
        parent_token = sub_info.parent_token
        if parent_token is not None:
            parent = job.workflows.get(str(parent_token))
            if parent is not None:
                workflow_class = parent.name
        return workflow_class

    def _settle_terminal_outcome(
        self,
        job: JobInfo,
        event: ExtensionOutcomeEvent,
        workflow_id: str,
    ) -> None:
        """Disseminate the outcome, persist it to the job's tracking, forget the workflow."""
        self.disseminate_extension_outcome(event)
        if job.timeout_tracking is not None:
            self._worker_health_manager.persist_outcome_to_tracking(
                job.timeout_tracking, event
            )
        self._worker_health_manager.forget_workflow(workflow_id)

    def _emit_workflow_outcome_event(
        self, result: "WorkflowFinalResult"
    ) -> None:
        """Phase F2: emit an H8 ``ExtensionOutcomeEvent`` when a
        workflow terminates.

        This closes the AD-26 outcome feedback loop: every
        terminating workflow contributes one Bernoulli observation
        to the per-workflow-class Beta posterior, which the H5
        evaluator reads through ``HierarchicalAlphaTuner.alpha_budget``
        to set the significance level of the H6 throughput witness's
        next test on a workflow of that class.

        Outcome classification:

        * ``status == "COMPLETED"`` — ``ExtensionOutcomeKind.COMPLETED``,
          progress fraction 1.0.
        * Otherwise — ``ExtensionOutcomeKind.FAILED``, progress
          fraction 0.0. (TIMED_OUT and EVICTED are emitted at their
          own dedicated call sites, not here.)

        Idempotent: the ledger / dissemination layer dedupes on
        workflow_id, so duplicate result deliveries do not
        double-count the Bernoulli observation.
        """
        if not result.workflow_id:
            return

        outcome_kind, progress_fraction = self._workflow_outcome_classification(result)

        fence_token = self._result_job_fence_token(result)

        leader_term = self._leader_election.state.current_term

        event = self._worker_health_manager.record_workflow_outcome(
            job_id=result.job_id,
            workflow_id=result.workflow_id,
            workflow_class=result.workflow_name,
            worker_id=result.worker_id,
            outcome_kind=outcome_kind,
            final_progress_fraction=progress_fraction,
            completed_at=self._clock.monotonic(),
            fence_token=fence_token,
            leader_term=leader_term,
        )
        self.disseminate_extension_outcome(event)
        self._persist_outcome_for_job(result.job_id, event)
        # Release the H7 ledger entry — the workflow is terminal,
        # no further decisions matter.
        self._worker_health_manager.forget_workflow(result.workflow_id)

    def _workflow_outcome_classification(
        self,
        result: "WorkflowFinalResult",
    ) -> tuple[ExtensionOutcomeKind, float]:
        """COMPLETED with full progress, else FAILED with none (Phase F2)."""
        if result.status == WorkflowStatus.COMPLETED.value:
            return ExtensionOutcomeKind.COMPLETED, 1.0
        return ExtensionOutcomeKind.FAILED, 0.0

    def _result_job_fence_token(self, result: "WorkflowFinalResult") -> int:
        """The result's job's fencing token; 0 without local job context."""
        # Look up fence_token from the parent job for AD-10/AD-34
        # leader-aware idempotency. Without job context (cross-
        # manager workflows we're tracking at low fidelity), fall
        # back to 0 — the outcome dissemination still teaches the
        # tuner; only the per-decision dedup is weakened.
        fence_token = 0
        job_token = self._job_manager.create_job_token(result.job_id)
        job = self._job_manager.get_job(job_token)
        if job is not None:
            fence_token = job.fencing_token
        return fence_token

    def _resolve_manager_tcp_from_udp(
        self,
        udp_addr: tuple[str, int],
    ) -> tuple[str, int] | None:
        """Translate a manager UDP/SWIM address to its TCP address.

        The leader-election and SWIM layers identify peers by their UDP
        address; every TCP-facing consumer (redirect targets, cancel /
        submission forwarding) needs the TCP address instead. Resolve
        via the known-manager-peers index, with a self short-circuit for
        the common case where this manager is itself the leader.

        Returns ``None`` when the UDP address matches no known peer —
        the caller then falls back to a TCP-addressed source rather than
        forwarding an address it can't otherwise justify.
        """
        if udp_addr == (self._host, self._udp_port):
            return (self._host, self._tcp_port)
        return self._known_manager_tcp_addr_for_udp(udp_addr)

    def _known_manager_tcp_addr_for_udp(self, udp_addr: tuple[str, int]) -> tuple[str, int] | None:
        """The TCP address of the first known manager peer at ``udp_addr``."""
        for _peer_id, info in self._manager_state.iter_known_manager_peers():
            if (info.udp_host, info.udp_port) == udp_addr:
                return (info.tcp_host, info.tcp_port)
        return None

    def _resolve_dc_leader_addr(self) -> tuple[str, int] | None:
        """Best-effort lookup of the DC leader's TCP address.

        Falls through three layers in priority order so a manager
        that hasn't yet won an election locally still returns a
        usable redirect target whenever *any* peer information is
        available:

        1. ``self._leader_election.state.current_leader`` — the
           authoritative source once the local leader-election state
           machine has converged. Returns the leader's TCP address.
        2. ``self._manager_state.dc_leader_manager_id`` — set when
           any peer's ``ManagerHeartbeat.is_leader`` was True. The
           leader's TCP address is then resolved through the known-
           manager-peers index.
        3. Linear scan of every known manager peer's last-known
           heartbeat for ``is_leader=True``. Backstop for any path
           that updates heartbeat state without flipping
           ``dc_leader_manager_id``.

        Returns ``None`` only when every layer is empty — a genuine
        "cluster has not converged on a leader" state. Clients
        receiving such a response treat the error as transient
        (per ``TRANSIENT_ERRORS`` matching) and round-robin retry.

        This is the source-side fix for the otherwise-symptomatic
        ``"Not DC leader, retry at leader: unknown"`` ack: instead
        of letting the client guess across N managers, every
        manager that has *any* leader information forwards it.
        """
        translated = self._election_leader_tcp_addr()
        if translated is not None:
            return translated
        # Untranslatable (peer not yet in the index) — fall through
        # to the heartbeat-derived layers, which are TCP-addressed.

        return self._heartbeat_derived_dc_leader_addr()

    def _election_leader_tcp_addr(self) -> tuple[str, int] | None:
        """Layer 1: the converged election leader, translated to its TCP address."""
        # Layer 1: locally-converged election state. ``current_leader``
        # is a **UDP/SWIM-namespace** address — the leader election runs
        # over UDP (``self_addr=self._get_self_udp_addr()`` at
        # construction), so ``current_leader`` is set from UDP peer
        # addresses. Redirect targets must be **TCP** addresses (the
        # gate and client forward cancels/submissions over TCP), so
        # translate through the peer index before returning. Returning
        # the raw UDP address — the prior behavior — handed clients and
        # gates an address one port off the real TCP endpoint (e.g. a
        # manager listening TCP on 20006 was advertised as its UDP
        # 20007), which the peer then failed to connect to. In an L2
        # deployment the client's round-robin over its full manager
        # list masked this; a gate forwarding by redirect has no such
        # fallback and the cancel/submit dead-ended.
        election_leader = self._leader_election.state.current_leader
        if not election_leader:
            return None
        return self._resolve_manager_tcp_from_udp(
            tuple(election_leader)
        )

    def _heartbeat_derived_dc_leader_addr(self) -> tuple[str, int] | None:
        """Layers 2 and 3: the leader peer heartbeats named, else any peer flagged leader."""
        leader_addr = self._dc_leader_manager_id_addr()
        if leader_addr is not None:
            return leader_addr

        return self._scanned_leader_peer_addr()

    def _dc_leader_manager_id_addr(self) -> tuple[str, int] | None:
        """Layer 2: the TCP address of the peer-heartbeat-derived dc_leader_manager_id."""
        # Layer 2: peer-heartbeat-derived dc_leader_manager_id.
        leader_id = self._manager_state.dc_leader_manager_id
        if not leader_id:
            return None
        info = self._manager_state.get_known_manager_peer(leader_id)
        if info is None:
            return None
        return (info.tcp_host, info.tcp_port)

    def _scanned_leader_peer_addr(self) -> tuple[str, int] | None:
        """Layer 3: the first other known peer whose last heartbeat claimed leadership."""
        # Layer 3: linear scan as a last resort. Cheap (peer count
        # is bounded by cluster size) and only runs in the
        # already-degenerate case where layers 1 and 2 are empty.
        for peer_id, info in self._manager_state.iter_known_manager_peers():
            if self._is_other_leader_peer(peer_id, info):
                return (info.tcp_host, info.tcp_port)

        return None

    def _is_other_leader_peer(self, peer_id: str, info: ManagerInfo) -> bool:
        """True for another manager whose last-known heartbeat claimed leadership."""
        return peer_id != self._node_id.full and getattr(info, "is_leader", False)

    def _manager_tcp_addr_is_live(
        self,
        tcp_addr: tuple[str, int],
    ) -> bool:
        """Return False when a manager TCP address is believed not-live.

        Liveness composes three signals so any one channel catching up
        ahead of the others is sufficient:

        1. ``_dead_managers`` — populated by SWIM peer-death handling.
           This is the fastest local view once the death callback fires.
        2. Per-peer ``unhealthy_since`` — set the moment a peer flips to
           SUSPECT or DEAD; persists across reap windows.
        3. ``IncarnationTracker`` state for the peer's UDP address —
           ``SUSPECT`` or ``DEAD`` short-circuits well before the manager
           death handler commits ``_dead_managers``.

        A peer the resolver can't index in ``_known_manager_peers``
        (never registered locally) is treated as live: the resolver
        cannot disprove liveness without registry data, and the higher
        layers (TCP send, client retry) will fail it over.
        """
        if tcp_addr in self._manager_state.get_dead_managers():
            return False

        known_peer = self._known_manager_peer_at_tcp_addr(tcp_addr)
        if known_peer is None:
            return True

        return self._known_manager_peer_is_live(*known_peer)

    def _known_manager_peer_at_tcp_addr(
        self,
        tcp_addr: tuple[str, int],
    ) -> tuple[str, ManagerInfo] | None:
        """The first known manager peer (id, info) at ``tcp_addr``, if any."""
        for peer_id, info in self._manager_state.iter_known_manager_peers():
            if (info.tcp_host, info.tcp_port) == tcp_addr:
                return peer_id, info
        return None

    def _known_manager_peer_is_live(self, peer_id: str, info: ManagerInfo) -> bool:
        """False when the peer is tracked unhealthy or SWIM holds it SUSPECT/DEAD."""
        if (
            self._manager_state.get_manager_peer_unhealthy_since(peer_id)
            is not None
        ):
            return False
        udp_addr = (info.udp_host, info.udp_port)
        node_state = self._incarnation_tracker.get_node_state(udp_addr)
        return not self._node_state_suspect_or_dead(node_state)


    async def _send_job_update_to_origin(
        self,
        job_id: str,
        callback_addr: tuple[str, int] | None,
        gate_method: str,
        client_method: str,
        payload: bytes,
        timeout: float = 5.0,
    ) -> tuple[str, tuple[str, int]] | None:
        """Send a job update through the origin gate when one owns the job.

        Gate-tier failover: when ``origin_gate_addr`` is set but the send
        fails (gate killed, partitioned, dead transport), iterate every
        healthy peer gate in deterministic order and try each. Push
        payloads carry the client callback and target-DC metadata, so a
        surviving gate can deliver client-ready updates or durably accept
        raw workflow results for aggregation without relying on callback
        state from the original accepting gate. On peer success the origin
        is updated to the gate that accepted, so subsequent pushes route
        directly to the new owner instead of paying the dead-origin
        timeout per send. The ``JobLeadershipAnnouncement`` handler will
        independently rewrite the origin when an orphan-coordinator
        takeover lands; failover here is the in-flight bridge between the
        origin dying and the announcement arriving.

        Falls through to ``callback_addr`` (direct-to-client) only when
        no origin gate was ever recorded — preserves L1/L2 semantics.
        """
        origin_gate_addr = self._manager_state.get_job_origin_gate(job_id)
        if origin_gate_addr is not None:
            return await self._send_job_update_through_gates(
                job_id, tuple(origin_gate_addr), gate_method, payload, timeout
            )

        if callback_addr is None:
            return None
        return await self._send_job_update_to_client(
            tuple(callback_addr), client_method, payload, timeout
        )

    async def _send_job_update_through_gates(
        self,
        job_id: str,
        origin_tuple: tuple[str, int],
        gate_method: str,
        payload: bytes,
        timeout: float,
    ) -> tuple[str, tuple[str, int]]:
        """Send through the origin gate, failing over to peer gates; re-home the
        job's origin to the gate that accepted."""
        tried: list[tuple[str, int]] = []

        destination, response, last_error = await self._send_to_gate_with_failover(
            job_id=job_id,
            primary_addr=origin_tuple,
            gate_method=gate_method,
            payload=payload,
            timeout=timeout,
            tried=tried,
        )

        if destination is None:
            self._raise_gate_update_failure(gate_method, tried, last_error)

        method = gate_method
        if destination != origin_tuple:
            self._manager_state.set_job_origin_gate(job_id, destination)
        return method, destination

    def _raise_gate_update_failure(
        self,
        gate_method: str,
        tried: list[tuple[str, int]],
        last_error: Exception | None,
    ) -> NoReturn:
        """Raise the most recent gate error, else a failure naming every gate tried."""
        # Every gate attempt failed; surface the most recent
        # error so the caller's exception handler can log
        # something actionable rather than a silent drop.
        if last_error is not None:
            raise last_error
        raise RuntimeError(
            f"{gate_method} failed against every healthy gate "
            f"(tried={tried})"
        )

    async def _send_job_update_to_client(
        self,
        destination: tuple[str, int],
        client_method: str,
        payload: bytes,
        timeout: float,
    ) -> tuple[str, tuple[str, int]]:
        """Send the update straight to the client's callback; raise when it is not taken."""
        response = await self._send_to_client(
            destination,
            client_method,
            payload,
            timeout=timeout,
        )
        method = client_method

        if isinstance(response, Exception):
            raise response
        if response not in (b"ok", b"forwarded", None):
            raise RuntimeError(f"{method} rejected update with {response!r}")

        return method, destination

    async def _send_to_gate_with_failover(
        self,
        job_id: str,
        primary_addr: tuple[str, int],
        gate_method: str,
        payload: bytes,
        timeout: float,
        tried: list[tuple[str, int]],
    ) -> tuple[tuple[str, int] | None, bytes | None, Exception | None]:
        """Try ``primary_addr`` then healthy peer gates in deterministic order.

        Returns ``(destination_that_succeeded, response_bytes, last_error)``.
        ``destination`` is ``None`` when every attempt failed. Healthy peer
        gates are drawn from the heartbeat-freshness-driven healthy set;
        gates whose heartbeats have aged past the registry's reap
        threshold are already excluded by ``get_healthy_gate_ids``. The
        deterministic sort by ``(host, port)`` keeps failover ordering
        stable across managers so the same surviving gate accumulates
        pushes for a given job rather than scattering.
        """
        # Try the recorded origin first — happy-path is unchanged.
        destination, response, last_error = await self._attempt_send_to_gate(
            primary_addr, gate_method, payload, timeout, tried,
        )
        if destination is not None:
            return destination, response, None

        return await self._fail_over_to_peer_gates(
            primary_addr, gate_method, payload, timeout, tried, last_error
        )

    async def _fail_over_to_peer_gates(
        self,
        primary_addr: tuple[str, int],
        gate_method: str,
        payload: bytes,
        timeout: float,
        tried: list[tuple[str, int]],
        last_error: Exception | None,
    ) -> tuple[tuple[str, int] | None, bytes | None, Exception | None]:
        """Walk the healthy peer gates not yet tried, in (host, port) order."""
        # Failover: rank peers deterministically and walk them.
        healthy_addrs = sorted(self._get_healthy_gate_tcp_addrs())
        for peer_addr in filter(
            lambda candidate_addr: self._is_untried_failover_gate(candidate_addr, primary_addr, tried),
            healthy_addrs,
        ):
            destination, response, last_error = await self._attempt_failover_gate(
                peer_addr, gate_method, payload, timeout, tried, last_error
            )
            if destination is not None:
                return destination, response, last_error

        return None, None, last_error

    def _is_untried_failover_gate(
        self,
        candidate_addr: tuple[str, int],
        primary_addr: tuple[str, int],
        tried: list[tuple[str, int]],
    ) -> bool:
        """True for a gate other than the primary that no attempt has tried yet."""
        return candidate_addr != primary_addr and candidate_addr not in tried

    async def _attempt_failover_gate(
        self,
        peer_addr: tuple[str, int],
        gate_method: str,
        payload: bytes,
        timeout: float,
        tried: list[tuple[str, int]],
        last_error: Exception | None,
    ) -> tuple[tuple[str, int] | None, bytes | None, Exception | None]:
        """One failover attempt: (destination, response, None) on success, else
        (None, None, the newest error)."""
        destination, response, send_error = await self._attempt_send_to_gate(
            peer_addr, gate_method, payload, timeout, tried,
        )
        if destination is not None:
            return destination, response, None
        return None, None, send_error if send_error is not None else last_error

    async def _attempt_send_to_gate(
        self,
        addr: tuple[str, int],
        gate_method: str,
        payload: bytes,
        timeout: float,
        tried: list[tuple[str, int]],
    ) -> tuple[tuple[str, int] | None, bytes | None, Exception | None]:
        """Single send attempt; classify outcome without raising.

        Returns ``(addr, response, None)`` on accept,
        ``(None, None, exception_or_runtime_error)`` on any failure.
        Appends ``addr`` to ``tried`` so the outer loop skips it.
        """
        tried.append(addr)
        try:
            response = await self._send_to_peer(
                addr,
                gate_method,
                payload,
                timeout=timeout,
            )
        except Exception as send_error:
            return None, None, send_error

        return self._classify_gate_reply(addr, gate_method, response)

    def _classify_gate_reply(
        self,
        addr: tuple[str, int],
        gate_method: str,
        response: bytes | Exception | None,
    ) -> tuple[tuple[str, int] | None, bytes | None, Exception | None]:
        """``(addr, response, None)`` for an accepting reply, else ``(None, None, error)``."""
        if isinstance(response, Exception):
            return None, None, response
        if response not in (b"ok", b"stored", None):
            return None, None, RuntimeError(
                f"{gate_method} rejected by {addr} with {response!r}"
            )
        return addr, response, None

    def _get_job_callback_addr(self, job_id: str) -> tuple[str, int] | None:
        """Resolve the client callback address for a job update."""
        callback_addr = self._manager_state.get_job_callback(job_id)
        if not callback_addr:
            callback_addr = self._manager_state.get_client_callback(job_id)
        if isinstance(callback_addr, list):
            return tuple(callback_addr)
        return callback_addr

    def _get_job_target_dcs_for_push(self, job_id: str) -> list[str]:
        """Return the target datacenters known to this manager for push routing."""
        submission = self._manager_state.get_job_submission(job_id)
        if submission is None:
            return []
        return list(submission.datacenters)

    def _get_job_target_dc_count_for_push(
        self,
        job_id: str,
        target_dcs: list[str],
    ) -> int:
        """Return expected DC count for a workflow-result push."""
        submission = self._manager_state.get_job_submission(job_id)
        if submission is None:
            return len(target_dcs)
        return max(submission.datacenter_count, len(target_dcs))

    def _manager_leadership_fence(self) -> int:
        """Return the manager's current per-DC leadership generation.

        Split-fence rule (see ``state.py`` and
        ``WorkflowResultPush.manager_fence_token``): stamped onto
        every data-plane result push so gate receivers can validate
        the *producing manager* against per-DC manager leadership,
        independently of any gate-tier leadership fence.

        Backed by ``LocalLeaderElection.state.current_term``: that
        value advances on each DC manager election, which is the
        correct epoch for "the manager that produced this push was
        leader at term T".
        """
        if self._leader_election is None:
            return 0
        return int(self._leader_election.state.current_term or 0)

    def _data_plane_provenance(
        self, job_id: str
    ) -> dict[str, str | int | tuple[str, int]]:
        """Producer identity + both fence-domain stamps for a job.

        Shared between ``WorkflowResultPush`` and ``JobFinalResult``
        builders — these fields are identical for both message types
        because the split-fence semantics apply to every
        manager-originated data-plane result. The per-workflow
        result-sequence is intentionally NOT here; it belongs only to
        ``WorkflowResultPush`` and is allocated by
        ``_data_plane_push_fields``.
        """
        return {
            "producer_id": self._node_id.full,
            "producer_addr": (self._host, self._tcp_port),
            "producer_role": "manager",
            "manager_fence_token": self._manager_leadership_fence(),
            "gate_fence_token": (
                self._manager_state.get_job_gate_routing_fence(job_id)
            ),
        }

    def _data_plane_push_fields(
        self,
        job_id: str,
        workflow_id: str,
        datacenter: str,
    ) -> dict[str, str | int | tuple[str, int]]:
        """Provenance + per-``(job, workflow, dc)`` result-sequence.

        Use for ``WorkflowResultPush`` only. The sequence allocation
        is monotonic per-triple and powers the gate's late-arrival
        dedup independently of either fence domain.
        """
        fields = self._data_plane_provenance(job_id)
        fields["result_sequence"] = (
            self._manager_state.next_workflow_result_sequence(
                job_id, workflow_id, datacenter
            )
        )
        return fields

    async def _push_job_status_to_client(
        self,
        job_id: str,
        status: str,
        message: str,
        is_final: bool = False,
    ) -> None:
        """Tier-1 status push to the client/gate that registered a
        callback for this job (AD-26 healthcheck-extension surfaces
        and AD-32 client visibility both depend on this).

        Resolves the destination via the same job-callback /
        client-callback fallback as
        ``_push_cancellation_complete_to_origin``. Silently no-ops
        when no callback is registered (e.g. fire-and-forget
        submissions).
        """
        callback_addr = self._get_job_callback_addr(job_id)
        if not callback_addr and not self._manager_state.get_job_origin_gate(job_id):
            return

        push = self._job_status_push(job_id, status, message, is_final, callback_addr)
        await self._send_job_status_push(job_id, status, callback_addr, push)

    def _job_status_push(
        self,
        job_id: str,
        status: str,
        message: str,
        is_final: bool,
        callback_addr: tuple[str, int] | None,
    ) -> JobStatusPush:
        """The job's status push with its progress totals (zero when the job is gone)."""
        job = self._job_manager.get_job_by_id(job_id)
        elapsed = job.elapsed_seconds() if job else 0.0
        total_completed, total_failed, overall_rate = (
            self._aggregate_job_progress(job) if job else (0, 0, 0.0)
        )

        return JobStatusPush(
            job_id=job_id,
            status=status,
            message=message,
            total_completed=total_completed,
            total_failed=total_failed,
            overall_rate=overall_rate,
            elapsed_seconds=elapsed,
            is_final=is_final,
            fence_token=self._leases.get_fence_token(job_id),
            callback_addr=callback_addr,
        )

    async def _send_job_status_push(
        self,
        job_id: str,
        status: str,
        callback_addr: tuple[str, int] | None,
        push: JobStatusPush,
    ) -> None:
        """Send the status push toward the job's origin; log a failure."""
        try:
            await self._send_job_update_to_origin(
                job_id,
                callback_addr,
                "job_status_push_forward",
                "job_status_push",
                push.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
        except Exception as send_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Failed to push job status {status} for "
                        f"{job_id} to {callback_addr}: {send_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )


    async def _notify_timeout_strategies_of_extension(
        self,
        worker_id: str,
        extension_seconds: float,
        worker_progress: float,
    ) -> None:
        """Notify timeout strategies of worker extension (AD-34 Part 10.4.7)."""
        # Find jobs with workflows on this worker
        for job in self._job_manager.iter_jobs():
            if self._job_runs_on_worker(job, worker_id):
                await self._record_job_worker_extension(
                    job, worker_id, extension_seconds, worker_progress
                )

    def _job_runs_on_worker(self, job: JobInfo, worker_id: str) -> bool:
        """True when one of the job's sub-workflows is assigned to the worker."""
        job_worker_ids = {
            sub_wf.worker_id
            for sub_wf in job.sub_workflows.values()
            if sub_wf.worker_id
        }
        return worker_id in job_worker_ids

    async def _record_job_worker_extension(
        self,
        job: JobInfo,
        worker_id: str,
        extension_seconds: float,
        worker_progress: float,
    ) -> None:
        """Stretch the job's AD-34 effective timeout by the worker's granted extension."""
        # ``record_worker_extension`` is on the TimeoutStrategy
        # ABC itself — no capability check. The old
        # ``hasattr(strategy, "record_extension")`` guard named
        # a method NO strategy defines, so this call was
        # unreachable and granted extensions never stretched
        # the job's AD-34 effective timeout (measured: the hard
        # timeout fired on the base budget with a 30s grant on
        # the books).
        strategy = self._manager_state.get_job_timeout_strategy(job.job_id)
        if strategy:
            await strategy.record_worker_extension(
                job_id=job.job_id,
                worker_id=worker_id,
                extension_seconds=extension_seconds,
                worker_progress=worker_progress,
            )

    def _select_timeout_strategy(self, submission: JobSubmission) -> TimeoutStrategy:
        """
        Auto-detect timeout strategy based on deployment type (AD-34 Part 10.4.2).

        Single-DC (no gate): LocalAuthorityTimeout - manager has full authority
        Multi-DC (with gate): GateCoordinatedTimeout - gate coordinates globally

        Args:
            submission: Job submission with optional gate_addr

        Returns:
            Appropriate TimeoutStrategy instance
        """
        if submission.origin_gate_addr:
            return GateCoordinatedTimeout(self)
        else:
            return LocalAuthorityTimeout(self)

    async def _timeout_job(self, job_id: str, reason: str) -> bool:
        """Mark ``job_id`` timed out and publish terminal notifications."""
        job = self._job_manager.get_job_by_id(job_id)
        if job is None:
            return await self._ignore_timeout_for_unknown_job(job_id)

        return await self._time_out_known_job(job_id, job, reason or "Job timed out")

    async def _ignore_timeout_for_unknown_job(self, job_id: str) -> bool:
        """Drop the unknown job's timeout strategy and log the ignored timeout."""
        self._manager_state.remove_job_timeout_strategy(job_id)
        await self._udp_logger.log(
            ServerWarning(
                message=f"Timeout ignored for unknown job {job_id[:8]}...",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        return False

    async def _time_out_known_job(self, job_id: str, job: JobInfo, timeout_reason: str) -> bool:
        """Time the job out; False when it already was terminal."""
        timestamp = self._clock.monotonic()
        await self._cancellation.cancel_pending_workflows(job_id)
        # What is in flight on workers, before the timeout fails it.
        workflows_to_cancel = self._cancellation.get_running_workflows_to_cancel(
            job,
            self._dispatched_or_running_workflow_ids(job_id, job),
        )
        timeout_summary = await self._mark_job_timed_out(job, timeout_reason)
        if timeout_summary is None:
            self._manager_state.remove_job_timeout_strategy(job_id)
            return False

        await self._publish_job_timeout(
            job_id, job, timeout_reason, timestamp, workflows_to_cancel, timeout_summary
        )
        return True

    def _dispatched_or_running_workflow_ids(self, job_id: str, job: JobInfo) -> list[str]:
        """The ids of the job's workflows DISPATCHED or RUNNING on workers."""
        return [
            workflow_id
            for workflow_id in map(self._workflow_token_id, job.workflows.values())
            if self._job_manager.workflow_lifecycle.get_state(job_id, workflow_id)
            in (WorkflowState.DISPATCHED, WorkflowState.RUNNING)
        ]

    async def _publish_job_timeout(
        self,
        job_id: str,
        job: JobInfo,
        timeout_reason: str,
        timestamp: float,
        workflows_to_cancel: list[tuple[str, str, tuple[str, int]]],
        timeout_summary: tuple[
            list[WorkflowResultPush],
            list[WorkflowResult],
            list[str],
            int,
            int,
            float,
        ],
    ) -> None:
        """Publish a timed-out job: outcomes, workflow results, client status,
        worker cancellations, and the gate's completion (AD-34)."""
        (
            workflow_pushes,
            workflow_results,
            errors,
            total_completed,
            total_failed,
            elapsed_seconds,
        ) = timeout_summary

        await self._manager_state.increment_state_version()
        self._emit_outcomes_for_terminal_job(job_id, ExtensionOutcomeKind.TIMED_OUT)
        await self._push_timeout_workflow_results(workflow_pushes)
        await self._push_job_status_to_client(
            job_id,
            JobStatus.TIMEOUT.value,
            timeout_reason,
            is_final=True,
        )

        _running_cancelled, workflow_errors = await self._cancellation.cancel_running_workflows(
            job,
            self._node_id.full,
            timestamp,
            timeout_reason,
            workflows_to_cancel,
        )
        if workflow_errors:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Timeout cancellation issues for job {job_id[:8]}...: "
                        f"{workflow_errors}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        await self._send_job_completion_to_gate(
            job_id,
            JobStatus.TIMEOUT.value,
            workflow_results,
            errors,
            total_completed,
            total_failed,
            elapsed_seconds,
        )

    async def _mark_job_timed_out(
        self,
        job: JobInfo,
        reason: str,
    ) -> tuple[
        list[WorkflowResultPush],
        list[WorkflowResult],
        list[str],
        int,
        int,
        float,
    ] | None:
        """Transition a non-terminal job to TIMEOUT under its job lock."""
        terminal_job_statuses = {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }
        terminal_workflow_statuses = {
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.AGGREGATED,
            WorkflowStatus.AGGREGATION_FAILED,
            WorkflowStatus.CANCELLED,
        }
        workflow_pushes: list[WorkflowResultPush] = []
        timeout_transitions: list[StateTransition] = []

        async with job.lock:
            if job.status in terminal_job_statuses:
                return None

            elapsed_seconds = await self._record_job_timed_out(job, reason)
            self._fail_unfinished_workflows_for_timeout(
                job, reason, terminal_workflow_statuses, workflow_pushes, timeout_transitions
            )
            workflow_results, errors, total_completed, total_failed = (
                self._summarize_timed_out_job(job, reason)
            )

        await self._job_manager.workflow_lifecycle.publish_transitions(timeout_transitions)
        return (
            workflow_pushes,
            workflow_results,
            errors,
            total_completed,
            total_failed,
            elapsed_seconds,
        )

    async def _record_job_timed_out(self, job: JobInfo, reason: str) -> float:
        """Stamp the job TIMEOUT and record it in the AD-38 ledger (caller
        holds the job lock); the job's elapsed seconds."""
        job.status = JobStatus.TIMEOUT.value
        job.completed_at = self._clock.time()
        job.timestamp = job.completed_at
        elapsed_seconds = job.elapsed_seconds()

        if self._job_ledger is not None:
            await self._log_ledger_shortfall(
                "JobTimedOut",
                job.job_id,
                await self._job_ledger.time_out_job(
                    job.job_id,
                    timeout_type=reason,
                    total_completed=job.workflows_completed,
                    total_failed=job.workflows_failed,
                    duration_ms=int(elapsed_seconds * 1000),
                    durability=DurabilityLevel.REGIONAL,
                ),
            )
            await self._discard_persisted_submission(job.job_id)
        return elapsed_seconds

    def _fail_unfinished_workflows_for_timeout(
        self,
        job: JobInfo,
        reason: str,
        terminal_workflow_statuses: set[WorkflowStatus],
        workflow_pushes: list[WorkflowResultPush],
        timeout_transitions: list[StateTransition],
    ) -> None:
        """Fail every non-terminal workflow of the timed-out job (caller holds the job lock)."""
        for workflow in job.workflows.values():
            if workflow.status in terminal_workflow_statuses:
                continue
            self._fail_workflow_for_timeout(job, workflow, reason, workflow_pushes, timeout_transitions)

    def _fail_workflow_for_timeout(
        self,
        job: JobInfo,
        workflow: WorkflowInfo,
        reason: str,
        workflow_pushes: list[WorkflowResultPush],
        timeout_transitions: list[StateTransition],
    ) -> None:
        """Transition one workflow FAILED for the timeout and queue its result push."""
        timeout_transitions.append(
            timeout_transition := self._job_manager.workflow_lifecycle.apply_transition(
                job.job_id,
                workflow.token.workflow_id or "",
                WorkflowState.FAILED,
                reason,
            )
        )
        if not timeout_transition.accepted:
            return
        workflow.status = WORKFLOW_STATUS_BY_WORKFLOW_STATE[timeout_transition.to_state]
        workflow.error = reason
        workflow.terminal_pushed = True
        workflow.terminal_status = WorkflowStatus.FAILED.value
        workflow.completion_event.set()
        workflow_pushes.append(
            self._build_timeout_workflow_push(job, workflow, reason)
        )

    def _summarize_timed_out_job(
        self,
        job: JobInfo,
        reason: str,
    ) -> tuple[list[WorkflowResult], list[str], int, int]:
        """Count the timed-out job's workflows and aggregate its results, with
        the timeout reason among its errors (caller holds the job lock)."""
        completed_count = self._count_workflows_with_status(
            job,
            frozenset({WorkflowStatus.COMPLETED, WorkflowStatus.AGGREGATED}),
        )
        failed_count = self._count_workflows_with_status(
            job,
            frozenset(
                {
                    WorkflowStatus.FAILED,
                    WorkflowStatus.AGGREGATION_FAILED,
                    WorkflowStatus.CANCELLED,
                }
            ),
        )
        job.workflows_completed = completed_count
        job.workflows_failed = failed_count
        workflow_results, errors, total_completed, total_failed = (
            self._aggregate_workflow_results(job)
        )
        total_completed = max(total_completed, completed_count)
        total_failed = max(total_failed, failed_count)
        if reason and reason not in errors:
            errors.append(reason)
        return workflow_results, errors, total_completed, total_failed

    def _count_workflows_with_status(
        self,
        job: JobInfo,
        statuses: frozenset[WorkflowStatus],
    ) -> int:
        """How many of the job's workflows are in one of ``statuses``."""
        return sum(
            1
            for workflow in job.workflows.values()
            if workflow.status in statuses
        )

    def _build_timeout_workflow_push(
        self,
        job: JobInfo,
        workflow: WorkflowInfo,
        reason: str,
    ) -> WorkflowResultPush:
        """Build a terminal workflow push for a job timeout."""
        callback_addr = self._get_job_callback_addr(job.job_id)
        target_dcs = self._get_job_target_dcs_for_push(job.job_id)
        workflow_id = workflow.token.workflow_id or workflow.token_str
        return WorkflowResultPush(
            job_id=job.job_id,
            workflow_id=workflow_id,
            workflow_name=workflow.name,
            datacenter=self._node_id.datacenter,
            status=WorkflowStatus.FAILED.value,
            fence_token=self._leases.get_fence_token(job.job_id),
            results=[],
            error=reason,
            elapsed_seconds=job.elapsed_seconds(),
            completed_at=self._clock.time(),
            callback_addr=callback_addr,
            target_dcs=target_dcs,
            target_dc_count=self._get_job_target_dc_count_for_push(
                job.job_id,
                target_dcs,
            ),
            **self._data_plane_push_fields(
                job.job_id, workflow_id, self._node_id.datacenter
            ),
        )

    async def _push_timeout_workflow_results(
        self,
        workflow_pushes: list[WorkflowResultPush],
    ) -> None:
        """Push workflow timeout results to the registered callback."""
        for push in workflow_pushes:
            await self._push_timeout_workflow_result(push)

    async def _push_timeout_workflow_result(self, push: WorkflowResultPush) -> None:
        """Push one timed-out workflow's result, when the job has a callback or origin gate."""
        callback_addr = self._get_job_callback_addr(push.job_id)
        if not callback_addr and not self._manager_state.get_job_origin_gate(push.job_id):
            return
        push.callback_addr = callback_addr
        self._complete_push_targets(push)
        await self._send_timeout_workflow_push(push, callback_addr)

    async def _send_timeout_workflow_push(
        self,
        push: WorkflowResultPush,
        callback_addr: tuple[str, int] | None,
    ) -> None:
        """Send the timed-out workflow's result toward its origin; log a failure."""
        try:
            await self._send_job_update_to_origin(
                push.job_id,
                callback_addr,
                "workflow_result_push",
                "workflow_result_push",
                push.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
        except Exception as send_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Failed to push timeout workflow result for "
                        f"{push.job_id}/{push.workflow_name}: {send_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _complete_push_targets(self, push: WorkflowResultPush) -> None:
        """Fill the push's target datacenters and their count when unset."""
        if not push.target_dcs:
            push.target_dcs = self._get_job_target_dcs_for_push(push.job_id)
        if push.target_dc_count <= 0:
            push.target_dc_count = self._get_job_target_dc_count_for_push(
                push.job_id,
                push.target_dcs,
            )

    async def _suspect_worker_deadline_expired(self, worker_id: str) -> None:
        """
        Mark a worker as suspected when its deadline expires (AD-26 Issue 2).

        Called when a worker's deadline has expired but is still within
        the grace period.

        Args:
            worker_id: The worker node ID that missed its deadline
        """
        worker = self._manager_state.get_worker(worker_id)
        if worker is None:
            self._manager_state.clear_worker_deadline(worker_id)
            return

        hierarchical_detector = self.get_hierarchical_detector()
        if hierarchical_detector is None:
            return

        await self._suspect_late_worker_globally(worker_id, worker, hierarchical_detector)

    async def _suspect_late_worker_globally(
        self,
        worker_id: str,
        worker: WorkerRegistration,
        hierarchical_detector: HierarchicalFailureDetector,
    ) -> None:
        """Suspect the late worker globally unless already SUSPECTED or DEAD (AD-26)."""
        worker_addr = (worker.node.host, worker.node.udp_port)
        current_status = await hierarchical_detector.get_node_status(worker_addr)

        if current_status in (NodeStatus.SUSPECTED_GLOBAL, NodeStatus.DEAD_GLOBAL):
            return

        await self.suspect_node_global(
            node=worker_addr,
            incarnation=0,
            from_node=(self._host, self._udp_port),
        )

        await self._udp_logger.log(
            ServerWarning(
                message=f"Worker {worker_id[:8]}... deadline expired, marked as SUSPECTED (within grace period)",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _evict_worker_deadline_expired(self, worker_id: str) -> None:
        """
        Evict a worker when its deadline expires beyond the grace period (AD-26 Issue 2).

        Args:
            worker_id: The worker node ID to evict
        """
        await self._udp_logger.log(
            ServerError(
                message=f"Worker {worker_id[:8]}... deadline expired beyond grace period, evicting",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Our own eviction: not charged to the workflows it ran (AD-44).
        await self._handle_worker_failure(worker_id, False)
        self._manager_state.clear_worker_deadline(worker_id)

        if self._worker_disseminator:
            await self._worker_disseminator.broadcast_worker_dead(worker_id, "evicted")

    # =========================================================================
    # TCP Send Helpers
    # =========================================================================

    async def _send_to_worker(
        self,
        addr: tuple[str, int],
        method: str,
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | Exception | None:
        """Send TCP message to worker."""
        response, _clock = await self.send_tcp(
            addr,
            method,
            data,
            timeout=timeout or self._config.tcp_timeout_standard_seconds,
        )
        return response

    async def _send_to_peer(
        self,
        addr: tuple[str, int],
        method: str,
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | Exception | None:
        """Send TCP message to peer manager."""
        response, _clock = await self.send_tcp(
            addr,
            method,
            data,
            timeout=timeout or self._config.tcp_timeout_standard_seconds,
        )
        return response

    async def _send_to_client(
        self,
        addr: tuple[str, int],
        method: str,
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | Exception | None:
        """Send TCP message to client."""
        response, _clock = await self.send_tcp(
            addr,
            method,
            data,
            timeout=timeout or self._config.tcp_timeout_standard_seconds,
        )
        return response

    async def prepare_workers_for_drain(
        self,
        worker_ids: set[str],
        reason: str = "planned_drain",
    ) -> set[str]:
        """Mark workers as draining before voluntary leave begins."""
        marked_worker_ids = await self._worker_pool.mark_workers_draining(
            worker_ids,
            reason,
        )
        if not marked_worker_ids:
            return set()

        await self._announce_draining_workers(marked_worker_ids, reason)

        return marked_worker_ids

    async def _announce_draining_workers(self, marked_worker_ids: set[str], reason: str) -> None:
        """Broadcast the draining workers and wake dispatch to re-plan around them."""
        if self._worker_disseminator:
            await self._worker_disseminator.broadcast_workers_draining(
                marked_worker_ids,
                reason,
            )

        if self._workflow_dispatcher:
            self._workflow_dispatcher.signal_cores_available()

    async def _validate_mtls_claims(
        self,
        addr: tuple[str, int],
        peer_label: str,
        peer_id: str,
    ) -> str | None:
        cert_der = self._peer_certificate_der(addr)
        if cert_der is not None:
            return await self._validate_peer_certificate_claims(cert_der, peer_label, peer_id)

        if self._config.mtls_strict_mode:
            await self._log_peer_rejection(peer_label, peer_id, "no certificate in strict mode")
            return "mTLS strict mode requires valid certificate"

        return None

    def _peer_certificate_der(self, addr: tuple[str, int]) -> bytes | None:
        """The DER certificate of the peer's TLS transport, if it presented one."""
        transport = self._tcp_server_request_transports.get(addr)
        return get_peer_certificate_der(transport) if transport else None

    async def _log_peer_rejection(self, peer_label: str, peer_id: str, reason: str) -> None:
        """Log a peer rejected by mTLS validation."""
        await self._udp_logger.log(
            ServerWarning(
                message=f"{peer_label} {peer_id} rejected: {reason}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _validate_peer_certificate_claims(
        self,
        cert_der: bytes,
        peer_label: str,
        peer_id: str,
    ) -> str | None:
        """Validate the certificate's cluster, environment and role claims; the
        rejection error, or None when they pass (AD-28)."""
        try:
            claims = self._role_validator.extract_peer_claims(cert_der)
        except CertificateParseError as parse_error:
            # Strict mode treats an unparseable certificate as a
            # validation failure, not a fallback to defaults -- the
            # defaults are this node's own cluster/environment, so
            # defaulted claims would pass every check below.
            reason = f"unparseable certificate in strict mode: {parse_error}"
            await self._log_peer_rejection(peer_label, peer_id, reason)
            return f"Certificate validation failed: {reason}"

        reason = self._certificate_isolation_mismatch(claims)
        if reason is not None:
            await self._log_peer_rejection(peer_label, peer_id, reason)
            return f"Certificate validation failed: {reason}"

        return await self._validate_certificate_role_claims(claims, peer_label, peer_id)

    def _certificate_isolation_mismatch(self, claims: CertificateClaims) -> str | None:
        """The cluster or environment mismatch of the claims, else None."""
        if claims.cluster_id != self._config.cluster_id:
            return f"Cluster mismatch: {claims.cluster_id} != {self._config.cluster_id}"

        if claims.environment_id != self._config.environment_id:
            return f"Environment mismatch: {claims.environment_id} != {self._config.environment_id}"

        return None

    async def _validate_certificate_role_claims(
        self,
        claims: CertificateClaims,
        peer_label: str,
        peer_id: str,
    ) -> str | None:
        """The role validator's rejection error for the claims, or None."""
        validation_result = self._role_validator.validate_claims(claims)
        if not validation_result.allowed:
            await self._log_peer_rejection(peer_label, peer_id, validation_result.reason)
            return f"Certificate validation failed: {validation_result.reason}"
        return None

    # =========================================================================
    # TCP Handlers
    # =========================================================================

    def _build_worker_registration_response(
        self,
        *,
        accepted: bool,
        error: str | None = None,
    ) -> RegistrationResponse:
        """Build a worker registration response with the current manager view."""
        healthy_managers = self._manager_state.get_active_known_manager_peers()
        healthy_managers.append(
            ManagerInfo(
                node_id=self._node_id.full,
                tcp_host=self._host,
                tcp_port=self._tcp_port,
                udp_host=self._host,
                udp_port=self._udp_port,
                datacenter=self._node_id.datacenter,
                is_leader=self.is_leader(),
            )
        )

        return RegistrationResponse(
            accepted=accepted,
            manager_id=self._node_id.full,
            healthy_managers=healthy_managers,
            error=error,
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
        )

    @tcp.receive()
    async def worker_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle worker registration."""
        try:
            registration = WorkerRegistration.load(data)

            (
                worker_udp_addr,
                is_same_worker_registration,
                needs_fresh_liveness,
                refusal,
            ) = await self._screen_worker_registration(addr, registration)
            if refusal is not None:
                return refusal

            await self._admit_registered_worker(
                registration,
                worker_udp_addr,
                is_same_worker_registration,
                needs_fresh_liveness,
            )

            response = self._build_worker_registration_response(accepted=True)

            return response.dump()

        except Exception as error:
            # Log the failure with traceback so we can diagnose what's
            # actually breaking. Silently returning an error response
            # makes registration failures invisible — the worker just
            # sees `accepted=False` with an opaque `error` string.
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"worker_register failed: "
                        f"{type(error).__name__}: {error}\n"
                        + "".join(traceback.format_exception(error))
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return self._build_worker_registration_response(
                accepted=False,
                error=str(error),
            ).dump()

    async def _screen_worker_registration(
        self,
        addr: tuple[str, int],
        registration: WorkerRegistration,
    ) -> tuple[tuple[str, int] | None, bool, bool, bytes | None]:
        """
        Decide whether a worker's registration may be admitted.

        Returns the worker's UDP address, whether it re-registers the worker
        already known at that address, whether that re-registration needed
        fresh SWIM liveness, and the refusal to send back -- None when the
        registration is admitted. A refused registration's other fields are
        not used.
        """
        if (refusal := await self._worker_registration_admission_refusal(addr, registration)) is not None:
            return None, False, False, refusal
        worker_udp_addr, is_new_worker, is_same_worker_registration = self._worker_registration_identity(
            registration
        )
        needs_fresh_liveness, refusal = await self._worker_reregistration_refusal(
            worker_udp_addr,
            is_same_worker_registration,
            is_new_worker,
        )
        return worker_udp_addr, is_same_worker_registration, needs_fresh_liveness, refusal

    async def _worker_registration_admission_refusal(
        self,
        addr: tuple[str, int],
        registration: WorkerRegistration,
    ) -> bytes | None:
        """Refuse a worker from another cluster or environment, or one whose mTLS claims do not hold; else None."""
        if (refusal := await self._worker_isolation_refusal(registration)) is not None:
            return refusal

        mtls_error = await self._validate_mtls_claims(
            addr,
            "Worker",
            registration.node.node_id,
        )
        if mtls_error:
            return self._worker_registration_refusal_response(mtls_error)
        return None

    async def _worker_isolation_refusal(self, registration: WorkerRegistration) -> bytes | None:
        """Refuse, and log, a worker whose cluster id or environment id differs from this manager's; else None."""
        if registration.cluster_id != self._config.cluster_id:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Worker {registration.node.node_id} rejected: cluster_id mismatch",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return self._worker_registration_refusal_response(
                "Cluster isolation violation: cluster_id mismatch"
            )

        if registration.environment_id != self._config.environment_id:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Worker {registration.node.node_id} rejected: environment_id mismatch",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return self._worker_registration_refusal_response(
                "Environment isolation violation: environment_id mismatch"
            )
        return None

    def _worker_registration_refusal_response(self, error: str) -> bytes:
        """A refused worker registration's response, naming this manager and carrying the reason."""
        return RegistrationResponse(
            accepted=False,
            manager_id=self._node_id.full,
            healthy_managers=[],
            error=error,
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
        ).dump()

    def _worker_registration_identity(
        self,
        registration: WorkerRegistration,
    ) -> tuple[tuple[str, int], bool, bool]:
        """
        Place a registering worker against the registry.

        Returns the worker's UDP address, whether its node id is new to the
        registry, and whether the registry's entry for that node id sits at
        the same UDP address -- a re-registration of the same worker.
        """
        existing_worker = self._registry.get_worker(registration.node.node_id)
        worker_udp_addr = (registration.node.host, registration.node.udp_port)
        existing_worker_udp_addr = self._registered_worker_udp_addr(existing_worker)
        return (
            worker_udp_addr,
            existing_worker is None,
            existing_worker_udp_addr == worker_udp_addr,
        )

    @staticmethod
    def _registered_worker_udp_addr(
        existing_worker: WorkerRegistration | None,
    ) -> tuple[str, int] | None:
        """The UDP address a registered worker was registered at, or None when there is no such worker or node."""
        if existing_worker is not None and existing_worker.node is not None:
            return (
                existing_worker.node.host,
                existing_worker.node.udp_port,
            )
        return None

    async def _worker_reregistration_refusal(
        self,
        worker_udp_addr: tuple[str, int],
        is_same_worker_registration: bool,
        is_new_worker: bool,
    ) -> tuple[bool, bytes | None]:
        """
        Refuse a stale duplicate registration or a new worker past this manager's capacity.

        Returns whether the registration needed fresh SWIM liveness, and the
        refusal -- None when the worker may register.
        """
        needs_fresh_liveness, refusal = await self._confirm_reregistering_worker_liveness(
            worker_udp_addr,
            is_same_worker_registration,
        )
        return needs_fresh_liveness, (
            refusal if refusal is not None else self._worker_capacity_refusal(is_new_worker)
        )

    async def _confirm_reregistering_worker_liveness(
        self,
        worker_udp_addr: tuple[str, int],
        is_same_worker_registration: bool,
    ) -> tuple[bool, bytes | None]:
        """
        Confirm over SWIM that a worker re-registering while suspected or dead is alive.

        Returns whether fresh liveness was needed, and the refusal for a
        stale duplicate registration SWIM could not confirm -- None otherwise.
        """
        node_state = self._incarnation_tracker.get_node_state(worker_udp_addr)
        needs_fresh_liveness = self._needs_fresh_liveness(is_same_worker_registration, node_state)
        if not needs_fresh_liveness:
            return needs_fresh_liveness, None
        confirmed_alive, _witness_consulted = (
            await self._confirm_peer_reachable_by_swim(
                worker_udp_addr,
                node_state.incarnation,
            )
        )
        if not confirmed_alive:
            return needs_fresh_liveness, self._build_worker_registration_response(
                accepted=False,
                error=(
                    "Worker registration rejected: "
                    "stale duplicate registration could not be "
                    "confirmed over SWIM"
                ),
            ).dump()
        return needs_fresh_liveness, None

    @staticmethod
    def _needs_fresh_liveness(is_same_worker_registration: bool, node_state: NodeState | None) -> bool:
        """Whether a same-address re-registration finds the worker SUSPECT or DEAD in SWIM."""
        return (
            is_same_worker_registration
            and node_state is not None
            and node_state.status in (b"SUSPECT", b"DEAD")
        )

    def _worker_capacity_refusal(self, is_new_worker: bool) -> bytes | None:
        """Refuse a new worker once this manager holds MAX_WORKERS_PER_MANAGER workers; else None."""
        max_workers = self._config.max_workers_per_manager
        if self._worker_capacity_reached(max_workers, is_new_worker):
            return self._worker_registration_refusal_response(
                "Worker registration rejected: "
                f"MAX_WORKERS_PER_MANAGER={max_workers} reached"
            )
        return None

    def _worker_capacity_reached(self, max_workers: int | None, is_new_worker: bool) -> bool:
        """Whether an enforced worker limit is reached for a new worker."""
        return (
            self._is_enforced_worker_limit(max_workers)
            and is_new_worker
            and self._manager_state.get_worker_count() >= max_workers
        )

    @staticmethod
    def _is_enforced_worker_limit(max_workers: int | None) -> bool:
        """Whether a worker limit is configured and non-negative."""
        return max_workers is not None and max_workers >= 0

    async def _admit_registered_worker(
        self,
        registration: WorkerRegistration,
        worker_udp_addr: tuple[str, int],
        is_same_worker_registration: bool,
        needs_fresh_liveness: bool,
    ) -> None:
        """
        Register an admitted worker everywhere it is tracked, recovering any incarnation it supersedes.

        The worker joins the registry and the worker pool, any previous
        incarnation at its address is recovered as a dead worker, it joins
        SWIM, its address is mapped and probed, and its registration is
        disseminated.
        """
        superseded_worker_ids = self._superseded_worker_ids(registration, worker_udp_addr)

        # Register worker
        await self._registry.register_worker(registration)

        # Add to worker pool
        await self._worker_pool.register_worker(registration)

        await self._recover_superseded_workers(registration, superseded_worker_ids)

        await self._admit_worker_to_swim(
            worker_udp_addr,
            is_same_worker_registration,
            needs_fresh_liveness,
        )

        self._manager_state.set_worker_addr_mapping(
            worker_udp_addr, registration.node.node_id
        )
        self._probe_scheduler.add_member(worker_udp_addr)

        if self._worker_disseminator:
            await self._worker_disseminator.broadcast_worker_registered(
                registration
            )

    def _superseded_worker_ids(
        self,
        registration: WorkerRegistration,
        worker_udp_addr: tuple[str, int],
    ) -> set[str]:
        """
        The node ids of previous incarnations registered at the registering worker's TCP or UDP address.

        A different node id at this worker's address is a previous
        incarnation of the process now registering (a node id embeds its
        process start time, so a restart always registers anew). SWIM never
        declares it dead -- the new process answers its probes -- so the work
        it held must be recovered here.
        """
        return {
            worker_id
            for address in (
                (registration.node.host, registration.node.port),
                worker_udp_addr,
            )
            if (
                worker_id := self._superseded_worker_id_at(
                    address, registration.node.node_id
                )
            )
            is not None
        }

    def _superseded_worker_id_at(
        self,
        address: tuple[str, int],
        registering_node_id: str,
    ) -> str | None:
        """The node id mapped to an address when it is not the registering worker's own, else None."""
        worker_id = self._manager_state.get_worker_id_from_addr(address)
        return worker_id if worker_id != registering_node_id else None

    async def _recover_superseded_workers(
        self,
        registration: WorkerRegistration,
        superseded_worker_ids: set[str],
    ) -> None:
        """
        Recover each superseded incarnation as a dead worker.

        Its pool entry goes and its unfinished workflows are reassigned. The
        registry already dropped it, so no eviction notice is owed to the
        address the new process now holds, and the new worker is counted, so
        this never fails the cluster's work as workerless. The cached
        transport to the worker's address belongs to the dead incarnation;
        the reassigned work must not wait out a send timeout on it. The
        incarnation died unexplained, so it is charged (AD-44).
        """
        if superseded_worker_ids:
            self._invalidate_tcp_client_transport(
                (registration.node.host, registration.node.port)
            )
        for superseded_worker_id in sorted(superseded_worker_ids):
            await self._handle_worker_failure(superseded_worker_id, True)

    async def _admit_worker_to_swim(
        self,
        worker_udp_addr: tuple[str, int],
        is_same_worker_registration: bool,
        needs_fresh_liveness: bool,
    ) -> None:
        """
        Add a registered worker to SWIM, re-seating it at a fresh incarnation unless it is a live duplicate.

        TCP registration is the authoritative "fresh start" signal for this
        address: it tells the manager that the worker process at
        ``worker_udp_addr`` is brand-new (a different ``node_id`` from any
        predecessor that may have died there). ``reset_peer_for_rejoin``
        wipes leftover SWIM state and re-seats the tracker entry at a
        *bumped* incarnation so stale DEAD gossip about the predecessor
        (still in flight at this point) is rejected by the freshness check
        rather than regressing the new instance back to DEAD. Duplicate
        registration from the same worker is intentionally idempotent: it
        refreshes registry/pool metadata but does not manufacture a new
        incarnation or disseminate ALIVE gossip.
        """
        if is_same_worker_registration and not needs_fresh_liveness:
            self.register_peer(worker_udp_addr)
        else:
            await self.reset_peer_for_rejoin(worker_udp_addr)

    @tcp.receive()
    async def manager_resource_gossip(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """AD-41 Part 4: a peer manager's resource reports for this
        datacenter's view."""
        try:
            message = ManagerResourceGossipMessage.load(data)
        except Exception as error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Unreadable resource gossip from {addr}: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

        if not self._resource_gossip.receive(message):
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Resource gossip from {addr} is datacenter "
                        f"{message.datacenter}'s, not {self._node_id.datacenter}'s"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"wrong_datacenter"

        return b"ok"

    @tcp.receive()
    async def manager_peer_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle manager peer registration."""
        try:
            return await self._register_manager_peer(addr, data)

        except Exception as error:
            return self._manager_peer_registration_refusal(str(error))

    def _manager_peer_registration_refusal(self, error: str) -> bytes:
        """A refused ``ManagerPeerRegistrationResponse`` carrying ``error``."""
        return ManagerPeerRegistrationResponse(
            accepted=False,
            manager_id=self._node_id.full,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            known_peers=self._manager_state.get_known_manager_peer_values(),
            manager_info=self._build_manager_info(),
            error=error,
        ).dump()

    async def _register_manager_peer(self, addr: tuple[str, int], data: bytes) -> bytes:
        """Validate the peer's isolation and certificate, ingest it, and accept it."""
        registration = ManagerPeerRegistration.load(data)

        refusal = await self._manager_peer_registration_isolation_refusal(addr, registration)
        if refusal is not None:
            return refusal

        await self._ingest_manager_peer_info(
            registration.node,
            authoritative_registration=True,
        )

        response = ManagerPeerRegistrationResponse(
            accepted=True,
            manager_id=self._node_id.full,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            known_peers=self._manager_state.get_known_manager_peer_values(),
            manager_info=self._build_manager_info(),
        )

        return response.dump()

    async def _manager_peer_registration_isolation_refusal(
        self,
        addr: tuple[str, int],
        registration: ManagerPeerRegistration,
    ) -> bytes | None:
        """The refusal for a cluster, environment or mTLS mismatch, else None (AD-28)."""
        if registration.cluster_id != self._config.cluster_id:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Manager {registration.node.node_id} rejected: cluster_id mismatch"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return self._manager_peer_registration_refusal(
                "Cluster isolation violation: manager cluster_id mismatch"
            )

        if registration.environment_id != self._config.environment_id:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Manager {registration.node.node_id} rejected: environment_id mismatch"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return self._manager_peer_registration_refusal(
                "Environment isolation violation: manager environment_id mismatch"
            )

        return await self._manager_peer_registration_mtls_refusal(addr, registration)

    async def _manager_peer_registration_mtls_refusal(
        self,
        addr: tuple[str, int],
        registration: ManagerPeerRegistration,
    ) -> bytes | None:
        """The refusal for a peer failing mTLS claim validation, else None."""
        mtls_error = await self._validate_mtls_claims(
            addr,
            "Manager",
            registration.node.node_id,
        )
        if mtls_error:
            return self._manager_peer_registration_refusal(mtls_error)
        return None


    def _record_held_job_progress(self, job_id: str, worker_id: str | None) -> None:
        """Record a worker's progress on a job for AD-30 responsiveness
        tracking -- for a job this manager holds. A report arriving after
        its job ended here (a worker's last buffered progress) re-created
        the pair's entry after the job's teardown had removed it; nothing
        removed it again, and its silence was taken for the worker failing a
        finished job -- suspected, and escalated toward global death."""
        if worker_id and self._job_manager.get_job_by_id(job_id) is not None:
            self._worker_health_monitor.record_job_progress(job_id, worker_id)

    @tcp.receive()
    async def workflow_progress(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow progress update from worker."""
        try:
            progress = WorkflowProgress.load(data)

            worker_id = self._manager_state.get_worker_id_from_addr(addr)
            self._record_held_job_progress(progress.job_id, worker_id)

            await self._report_workflow_progress_to_timeout(progress)
            self._feed_throughput_witness(worker_id, progress)
            await self._update_worker_cores_from_workflow_progress(worker_id, progress)
            await self._record_workflow_progress_stats(addr, worker_id, progress)

            # AD-41: resources are the job leader's to account and judge;
            # a non-leader that got this progress points the worker at
            # the leader in its ack.
            if self._is_job_leader(progress.job_id):
                self._track_led_workflow_resources(progress)
                await self._enforce_workflow_resources(progress, worker_id)

            return self._workflow_progress_ack(progress.job_id)

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow progress error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            return WorkflowProgressAck(
                manager_id=self._node_id.full,
                is_leader=self.is_leader(),
                healthy_managers=self._get_healthy_managers(),
                job_leader_addr=None,
                backpressure_level=0,
                backpressure_delay_ms=0,
                backpressure_batch_only=False,
            ).dump()

    def _feed_throughput_witness(self, worker_id: str | None, progress: WorkflowProgress) -> None:
        """AD-26 H6: feed the workflow's progress counters to the throughput
        witness, which samples the workflow's own rate between reports.

        Only while the sub-workflow is in flight on a live job: a report
        arriving after its result, or after its job ended, would reopen a
        stream the workflow's end already forgot. The check and the
        ingest run with no await between them, so no result can land in
        the gap."""
        if worker_id and self._job_manager.sub_workflow_in_flight(progress.workflow_id):
            self._worker_health_manager.ingest_workflow_progress(
                worker_id, progress.workflow_id, progress.completed_count, progress.elapsed_seconds
            )

    async def _report_workflow_progress_to_timeout(self, progress: WorkflowProgress) -> None:
        """
        Record a workflow's progress in the job manager and report advancing work to the job's timeout.

        Work that advanced is the job's progress (AD-34): a workflow
        completing actions is not stuck, however long it runs.
        """
        if await self._job_manager.update_workflow_progress(
            sub_workflow_token=progress.workflow_id,
            progress=progress,
        ) and (
            timeout_strategy := self._manager_state.get_job_timeout_strategy(
                progress.job_id
            )
        ) is not None:
            await timeout_strategy.report_progress(progress.job_id, "workflow_progress")

    async def _update_worker_cores_from_workflow_progress(
        self,
        worker_id: str | None,
        progress: WorkflowProgress,
    ) -> None:
        """
        Update a known worker's free cores from its progress report, signalling the dispatcher when they changed.

        The worker's free cores as of this report are fresher than its last
        heartbeat (cores a workflow finished with are freed as it reports),
        and proof its dispatch is counted in them.
        """
        if await self._worker_cores_changed_by_progress(worker_id, progress) and self._workflow_dispatcher:
            self._workflow_dispatcher.signal_cores_available()

    async def _worker_cores_changed_by_progress(self, worker_id: str | None, progress: WorkflowProgress) -> bool:
        """Whether a known worker's progress report updated its free cores
        (an unknown worker's report updates nothing)."""
        return bool(worker_id) and await self._worker_pool.update_worker_cores_from_progress(
            worker_id,
            progress.worker_available_cores,
            progress.workflow_id,
            progress.worker_cores_version,
        )

    async def _record_workflow_progress_stats(
        self,
        addr: tuple[str, int],
        worker_id: str | None,
        progress: WorkflowProgress,
    ) -> None:
        """Record a progress update in the stats, under the worker's id or, when unknown, its address."""
        stats_worker_id = worker_id or f"{addr[0]}:{addr[1]}"
        await self._stats.record_progress_update(stats_worker_id, progress)

    def _workflow_progress_ack(self, job_id: str) -> bytes:
        """Ack a progress update with this manager's view, the job's leader, and the current backpressure."""
        backpressure = self._stats.get_backpressure_signal()
        job_leader_addr = self._manager_state.get_job_leader_addr(job_id)
        if isinstance(job_leader_addr, list):
            job_leader_addr = tuple(job_leader_addr)

        ack = WorkflowProgressAck(
            manager_id=self._node_id.full,
            is_leader=self.is_leader(),
            healthy_managers=self._get_healthy_managers(),
            job_leader_addr=job_leader_addr,
            backpressure_level=backpressure.level.value,
            backpressure_delay_ms=backpressure.delay_ms,
            backpressure_batch_only=backpressure.batch_only,
        )

        return ack.dump()

    def _resource_budget_rejection(self, submission: JobSubmission) -> str | None:
        """Why this manager cannot enforce the job's own AD-41 budget, if
        it cannot. A job that asked for limits is never run without them."""
        if submission.resource_budget is None:
            return None
        return self._requested_resource_budget_rejection(submission.resource_budget)

    def _requested_resource_budget_rejection(self, resource_budget: ResourceBudget) -> str | None:
        """Why a requested AD-41 budget cannot be enforced here: guards off, or invalid."""
        if self._resource_enforcer is None:
            return "resource budget requested, but resource guards are disabled on this manager"
        if errors := resource_budget.validation_errors():
            return f"invalid resource budget: {'; '.join(errors)}"
        return None

    def _assign_resource_budget(self, submission: JobSubmission) -> None:
        """Enforce the job's workflows against its own budget, when it set one."""
        if self._resource_enforcer is not None and submission.resource_budget is not None:
            self._resource_enforcer.assign_budget(submission.job_id, submission.resource_budget)

    def _track_led_workflow_resources(self, progress: WorkflowProgress) -> None:
        """AD-41: keep a led workflow's latest resource estimate until it ends."""
        if progress.status in _TERMINAL_WORKFLOW_STATUS_VALUES:
            self._led_workflow_resources.release_workflow(progress.workflow_id)
            return
        self._led_workflow_resources.record(
            workflow_id=progress.workflow_id,
            job_id=progress.job_id,
            cpu_percent=progress.total_cpu_percent,
            cpu_uncertainty=progress.total_cpu_uncertainty,
            memory_bytes=progress.total_memory_mb * _BYTES_PER_MEGABYTE,
            memory_uncertainty=progress.total_memory_uncertainty_mb * _BYTES_PER_MEGABYTE,
        )

    def _build_resource_report(self) -> ManagerResourceReport:
        """AD-41: this manager's led workload and the datacenter's worker
        capacity: 100 per allotted core of every worker whose cores are the
        datacenter's to use, busy or idle; each worker host's memory once,
        however many workers share it. Both come from what each worker
        registered: the pool's live core count is derived from free cores
        and reads zero for a fully busy worker."""
        registrations = self._capacity_worker_registrations()
        host_memory_megabytes = self._host_memory_megabytes(registrations)
        return ManagerResourceReport(
            manager_metrics=self._last_resource_metrics,
            workload=self._led_workflow_resources.totals(),
            cpu_capacity_percent=100.0 * sum(registration.total_cores for registration in registrations),
            memory_capacity_bytes=sum(host_memory_megabytes.values()) * _BYTES_PER_MEGABYTE,
        )

    def _capacity_worker_registrations(self) -> list[WorkerRegistration]:
        """The registrations of the workers whose cores count toward capacity (AD-41)."""
        return [
            worker.registration
            for worker in self._worker_pool.iter_workers()
            if self._worker_counts_toward_capacity(worker)
        ]

    def _worker_counts_toward_capacity(self, worker: WorkerStatus) -> bool:
        """True for a registered worker whose cores are the datacenter's to use."""
        return worker.registration is not None and self._worker_pool.counts_toward_capacity(
            worker.worker_id
        )

    def _host_memory_megabytes(self, registrations: list[WorkerRegistration]) -> dict[str, int]:
        """Each worker host's registered memory, counted once per host."""
        return {
            registration.node.host: registration.memory_mb for registration in registrations
        }

    async def _enforce_workflow_resources(
        self,
        progress: WorkflowProgress,
        worker_id: str | None,
    ) -> None:
        """AD-41: judge a running workflow's resource estimates; forget it
        once it reaches a terminal status."""
        if self._resource_enforcer is None:
            return
        if worker_id is None:
            await self._udp_logger.log(
                ServerDebug(
                    message=(
                        f"Resource check skipped for workflow {progress.workflow_id[:8]}...: "
                        "progress came from an address with no registered worker"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return
        await self._judge_workflow_resources(progress, worker_id)

    async def _judge_workflow_resources(self, progress: WorkflowProgress, worker_id: str) -> None:
        """Release a terminal workflow's AD-41 tracking, else check its estimates."""
        if progress.status in _TERMINAL_WORKFLOW_STATUS_VALUES:
            self._resource_enforcer.release_workflow(progress.workflow_id)
            return
        await self._resource_enforcer.check_workflow(
            workflow_id=progress.workflow_id,
            worker_id=worker_id,
            job_id=progress.job_id,
            cpu_percent=progress.total_cpu_percent,
            cpu_uncertainty=progress.total_cpu_uncertainty,
            memory_bytes=progress.total_memory_mb * _BYTES_PER_MEGABYTE,
            memory_uncertainty=progress.total_memory_uncertainty_mb * _BYTES_PER_MEGABYTE,
        )

    async def _warn_resource_violation(
        self,
        workflow_id: str,
        worker_id: str,
        violation_type: ResourceViolationType,
        value: float,
        limit: float,
    ) -> None:
        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"Workflow {workflow_id[:8]}... on worker {worker_id[:8]}... "
                    f"{violation_type.value}: {value:.1f} against a limit of {limit:.1f}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _throttle_workflow_for_resources(
        self,
        workflow_id: str,
        worker_id: str,
        job_id: str,
        scale: float,
    ) -> bool:
        """Cut an over-budget workflow's concurrency to ``scale`` of its
        operating point; True when its worker applied it."""
        response = await self._send_workflow_throttle(workflow_id, worker_id, job_id, scale)
        applied = response is not None and response.applied
        outcome = self._throttle_outcome_text(response, applied)
        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"Resource throttle of workflow {workflow_id[:8]}... on worker "
                    f"{worker_id[:8]}... to {scale:.3f} of its concurrency: {outcome}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        return applied

    def _throttle_outcome_text(
        self,
        response: WorkflowThrottleResponse | None,
        applied: bool,
    ) -> str:
        """The applied concurrency cap, or why the throttle was not applied."""
        return (
            f"concurrency cap {response.concurrency_cap}"
            if applied
            else f"not applied ({response.error if response is not None else 'no response'})"
        )

    async def _release_workflow_throttle(self, workflow_id: str, worker_id: str, job_id: str) -> bool:
        """Restore a throttled workflow's concurrency. Done once the worker
        answered at all -- a workflow no longer running there has nothing
        left to release -- so only a lost exchange is retried."""
        response = await self._send_workflow_throttle(workflow_id, worker_id, job_id, None)
        if response is None:
            return False
        await self._udp_logger.log(
            ServerInfo(
                message=f"Resource throttle of workflow {workflow_id[:8]}... released",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        return True

    async def _send_workflow_throttle(
        self,
        workflow_id: str,
        worker_id: str,
        job_id: str,
        scale: float | None,
    ) -> WorkflowThrottleResponse | None:
        """One throttle exchange with the workflow's worker; None when it
        did not complete."""
        if (worker := self._manager_state.get_worker(worker_id)) is None:
            return None
        response = await self._send_to_worker(
            (worker.node.host, worker.node.port),
            "throttle_workflow",
            WorkflowThrottleRequest(job_id=job_id, workflow_id=workflow_id, scale=scale).dump(),
        )
        return self._decode_workflow_throttle_response(response)

    def _decode_workflow_throttle_response(
        self,
        response: bytes | Exception | None,
    ) -> WorkflowThrottleResponse | None:
        """The worker's throttle response, or None when it sent none."""
        if not isinstance(response, bytes) or not response:
            return None
        return WorkflowThrottleResponse.load(response)

    async def _kill_workflow_for_resources(
        self,
        workflow_id: str,
        worker_id: str,
        job_id: str,
        violation_type: ResourceViolationType,
    ) -> bool:
        """Kill an over-budget workflow through the normal cancel path."""
        if (worker := self._manager_state.get_worker(worker_id)) is None:
            return False

        killed, error = await self._cancellation.cancel_running_workflow_on_worker(
            job_id=job_id,
            workflow_id=workflow_id,
            worker_addr=(worker.node.host, worker.node.port),
            requester_id=self._node_id.full,
            timestamp=self._clock.time(),
            reason=f"resource budget exceeded: {violation_type.value}",
        )
        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"Resource kill of workflow {workflow_id[:8]}... on worker "
                    f"{worker_id[:8]}... ({violation_type.value}): "
                    f"{'requested' if killed else f'failed ({error})'}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        return killed

    async def _evict_worker_for_resources(
        self,
        worker_id: str,
        violation_type: ResourceViolationType,
    ) -> bool:
        """Evict a worker that keeps an over-budget workflow running after
        repeated kill requests (AD-41 failure modes)."""
        if self._manager_state.get_worker(worker_id) is None:
            return False
        await self._udp_logger.log(
            ServerError(
                message=(
                    f"Worker {worker_id[:8]}... ignored resource kill requests "
                    f"({violation_type.value}); evicting"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        # AD-41: it ignored resource kills -- over-budget evidence, charged.
        await self._handle_worker_failure(worker_id, True)
        if self._worker_disseminator:
            await self._worker_disseminator.broadcast_worker_dead(worker_id, "evicted")
        return True

    async def _handle_parent_workflow_completion(
        self,
        result: WorkflowFinalResult,
        result_recorded: bool,
        parent_complete: bool,
    ) -> None:
        if not (result_recorded and parent_complete):
            return
        await self._complete_parent_workflow(result)

    async def _complete_parent_workflow(self, result: WorkflowFinalResult) -> None:
        """
        Close a parent workflow whose every sub-workflow reported, and push its result.

        Aggregates across *all* sub-workflows for the parent rather than
        forwarding whichever result happened to be the last to land. A
        worker that died mid-execution races the manager's reassignment
        path: its CANCELLED final-result can arrive after the surviving
        workers reported COMPLETED, and using ``result.status`` directly
        would push that misleading CANCELLED terminal state to the client.
        The aggregator returns the same payload shape the legacy path
        produced when only one sub-workflow existed.

        The aggregated WorkflowResultPush goes to the client when no gate is
        involved. The model docstring spells out the contract: "Sent from
        Manager to Client (aggregated) or Manager to Gate (raw)". Without
        this push, L1/L2 jobs (no gate) silently complete on the manager and
        the client's on_workflow_result callback never fires. Gates handle
        the cross-DC aggregation case via their own workflow_result_push
        handler.
        """
        sub_token = TrackingToken.parse(result.workflow_id)
        parent_workflow_token = sub_token.workflow_token
        if not parent_workflow_token:
            return

        aggregate = await self._job_manager.aggregate_parent_workflow_outcome(
            result.workflow_id
        )
        if aggregate is None:
            return
        aggregate_status, aggregate_error, aggregated_results = aggregate

        await self._mark_parent_workflow_outcome(
            result, sub_token, parent_workflow_token, aggregate_status, aggregate_error
        )

        await self._push_workflow_result_to_client(
            result,
            sub_token,
            aggregate_status=aggregate_status,
            aggregate_error=aggregate_error,
            aggregated_results=aggregated_results,
        )

    async def _mark_parent_workflow_outcome(
        self,
        result: WorkflowFinalResult,
        sub_token: TrackingToken,
        parent_workflow_token: str,
        aggregate_status: str,
        aggregate_error: str | None,
    ) -> None:
        """Mark a parent workflow completed when its aggregate completed, else close it as unsuccessful."""
        if aggregate_status == WorkflowStatus.COMPLETED.value:
            await self._job_manager.mark_workflow_completed(parent_workflow_token)
            return
        await self._mark_unsuccessful_parent_workflow(
            result, sub_token, parent_workflow_token, aggregate_status, aggregate_error
        )

    async def _mark_unsuccessful_parent_workflow(
        self,
        result: WorkflowFinalResult,
        sub_token: TrackingToken,
        parent_workflow_token: str,
        aggregate_status: str,
        aggregate_error: str | None,
    ) -> None:
        """Mark a parent workflow whose aggregate failed as failed, else close it as cancelled."""
        if aggregate_status == WorkflowStatus.FAILED.value:
            await self._job_manager.mark_workflow_failed(
                parent_workflow_token, aggregate_error or "workflow failed"
            )
            return
        await self._close_cancelled_parent_workflow(
            result, sub_token, parent_workflow_token, aggregate_error
        )

    async def _close_cancelled_parent_workflow(
        self,
        result: WorkflowFinalResult,
        sub_token: TrackingToken,
        parent_workflow_token: str,
        aggregate_error: str | None,
    ) -> None:
        """
        Close a parent workflow every sub of which came back cancelled.

        A workflow that was cancelling finishes its cancellation: every sub
        came back cancelled. One nothing was cancelling (a cancellation turns
        it CANCELLING before it sends a single cancel) was stopped by this
        manager -- an AD-41 resource kill of a sub -- or the worker, so it
        failed. The job's arithmetic must close on it: unmarked, the job
        stood until AD-34 declared a misleading timeout.
        """
        workflow_id = sub_token.workflow_id or ""
        if self._job_manager.workflow_lifecycle.get_state(
            result.job_id, workflow_id
        ) == WorkflowState.CANCELLING:
            await self._job_manager.finish_workflow_cancellation(
                result.job_id, workflow_id
            )
            return
        cancellation = self._manager_state.get_cancelled_workflow(
            result.job_id, result.workflow_id
        )
        await self._job_manager.mark_workflow_failed(
            parent_workflow_token,
            self._uncancelled_workflow_failure_reason(cancellation, aggregate_error),
        )

    @classmethod
    def _uncancelled_workflow_failure_reason(
        cls,
        cancellation: CancelledWorkflowInfo | None,
        aggregate_error: str | None,
    ) -> str:
        """The failure reason of a workflow stopped before completing: its cancellation's reason, else the error."""
        if cancellation_reason := cls._cancellation_reason(cancellation):
            return f"cancelled before completing: {cancellation_reason}"
        return aggregate_error or "cancelled before completing"

    @staticmethod
    def _cancellation_reason(cancellation: CancelledWorkflowInfo | None):
        """A recorded cancellation's reason, or a falsy value when there is no cancellation or no reason."""
        return cancellation is not None and cancellation.reason

    async def _push_workflow_result_to_client(
        self,
        result: WorkflowFinalResult,
        sub_token: TrackingToken,
        aggregate_status: str | None = None,
        aggregate_error: str | None = None,
        aggregated_results: list[dict] | None = None,
    ) -> None:
        callback_addr = self._get_job_callback_addr(result.job_id)
        if self._has_no_workflow_result_destination(result.job_id, callback_addr):
            return
        push = self._build_workflow_result_push(
            result,
            sub_token,
            callback_addr,
            aggregate_status,
            aggregate_error,
            aggregated_results,
        )
        await self._send_workflow_result_push(result, callback_addr, push)

    def _has_no_workflow_result_destination(
        self,
        job_id: str,
        callback_addr: tuple[str, int] | None,
    ) -> bool:
        """Whether a job has neither a client callback nor an origin gate to push results to."""
        return not callback_addr and not self._manager_state.get_job_origin_gate(job_id)

    def _build_workflow_result_push(
        self,
        result: WorkflowFinalResult,
        sub_token: TrackingToken,
        callback_addr: tuple[str, int] | None,
        aggregate_status: str | None,
        aggregate_error: str | None,
        aggregated_results: list[dict] | None,
    ) -> WorkflowResultPush:
        """
        Build the workflow result push for a parent workflow's terminal outcome.

        The aggregate computed across every sub-workflow is the
        authoritative terminal state for the parent. Falling back to
        ``result.*`` keeps callers without an aggregate (legacy paths)
        working unchanged, but the standard push goes through the
        aggregated values so a CANCELLED sub-workflow from a dying worker
        cannot override a sibling's COMPLETED result. Whether the workflow
        drives load decides how its results combine, here and at the gate: a
        test workflow's per-core results merge into one set of load-test
        stats, any other workflow's travel as they are. The push left
        ``is_test`` at its default (True), so every workflow's results were
        merged as load-test stats. The workflow ran as long as its longest
        run on any worker.
        """
        push_status, push_error = self._workflow_result_push_outcome(
            result, aggregate_status, aggregate_error
        )
        push_results = self._workflow_result_push_results(result, aggregated_results)
        is_test = self._is_test_workflow(self._parent_workflow_info(result, sub_token))
        is_client_ready = not bool(self._manager_state.get_job_origin_gate(result.job_id))
        push_results = self._client_bound_push_results(push_results, is_client_ready, is_test)
        target_dcs = self._get_job_target_dcs_for_push(result.job_id)
        workflow_id = sub_token.workflow_id or result.workflow_id
        return WorkflowResultPush(
            job_id=result.job_id,
            workflow_id=workflow_id,
            workflow_name=result.workflow_name,
            datacenter=self._node_id.datacenter,
            status=push_status,
            fence_token=self._leases.get_fence_token(result.job_id),
            results=push_results,
            error=push_error,
            elapsed_seconds=self._longest_result_elapsed_seconds(push_results),
            completed_at=self._clock.time(),
            callback_addr=callback_addr,
            is_test=is_test,
            target_dcs=target_dcs,
            target_dc_count=self._get_job_target_dc_count_for_push(
                result.job_id,
                target_dcs,
            ),
            is_client_ready=is_client_ready,
            **self._data_plane_push_fields(
                result.job_id, workflow_id, self._node_id.datacenter
            ),
        )

    @staticmethod
    def _workflow_result_push_outcome(
        result: WorkflowFinalResult,
        aggregate_status: str | None,
        aggregate_error: str | None,
    ) -> tuple[str, str | None]:
        """The pushed status and error: the aggregate's when there is one, else the result's own."""
        if aggregate_status is not None:
            return aggregate_status, aggregate_error
        return result.status, result.error

    @staticmethod
    def _workflow_result_push_results(
        result: WorkflowFinalResult,
        aggregated_results: list[dict] | None,
    ) -> list[dict]:
        """The pushed results: the aggregated ones when given, else a copy of the result's own (or none)."""
        if aggregated_results is not None:
            return aggregated_results
        return list(result.results) if result.results else []

    def _parent_workflow_info(
        self,
        result: WorkflowFinalResult,
        sub_token: TrackingToken,
    ) -> WorkflowInfo | None:
        """The job's record of a sub-workflow token's parent workflow, or None when either is unknown."""
        job = self._job_manager.get_job_by_id(result.job_id)
        return (
            job.workflows.get(str(sub_token.to_parent_workflow_token()))
            if job is not None and sub_token.is_sub_workflow_token
            else None
        )

    @staticmethod
    def _is_test_workflow(workflow_info: WorkflowInfo | None) -> bool:
        """Whether a known workflow drives load (a test workflow)."""
        return workflow_info is not None and workflow_info.is_test

    @classmethod
    def _client_bound_push_results(
        cls,
        push_results: list[dict],
        is_client_ready: bool,
        is_test: bool,
    ) -> list[dict]:
        """
        Merge a client-bound test workflow's per-core results into one.

        A client-bound push carries one merged result, as a gate's does: the
        per-core results are merged here, not left for the client (which
        reads the first) to drop.
        """
        if cls._merges_client_push_results(push_results, is_client_ready, is_test):
            return [Results().merge_results(push_results)]
        return push_results

    @staticmethod
    def _merges_client_push_results(
        push_results: list[dict],
        is_client_ready: bool,
        is_test: bool,
    ) -> bool:
        """Whether a client-bound test workflow's push carries more than one result to merge."""
        return is_client_ready and is_test and len(push_results) > 1

    @staticmethod
    def _longest_result_elapsed_seconds(push_results: list[dict]) -> float:
        """The longest elapsed time among the pushed results, or 0.0 when there are none."""
        return max(
            (float(stats.get("elapsed", 0.0)) for stats in push_results),
            default=0.0,
        )

    async def _send_workflow_result_push(
        self,
        result: WorkflowFinalResult,
        callback_addr: tuple[str, int] | None,
        push: WorkflowResultPush,
    ) -> None:
        """Send a workflow result push to the job's origin, logging a failed send."""
        try:
            await self._send_job_update_to_origin(
                result.job_id,
                callback_addr,
                "workflow_result_push",
                "workflow_result_push",
                push.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
        except Exception as send_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Failed to push workflow_result for "
                        f"{result.job_id}/{result.workflow_name} to client "
                        f"{callback_addr}: {send_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _report_lifecycle_progress(self, transition: StateTransition) -> None:
        """A workflow taking a lifecycle transition is its job's progress
        (AD-34): the job's timeout strategy hears of it."""
        if transition.accepted and (
            timeout_strategy := self._manager_state.get_job_timeout_strategy(
                transition.job_id
            )
        ) is not None:
            await timeout_strategy.report_progress(
                transition.job_id, f"workflow_{transition.to_state.value}"
            )

    async def _complete_job_if_done(self, job_id: str) -> None:
        """Complete the job once every workflow of it finished."""
        if self._is_job_complete(job_id):
            await self._handle_job_completion(job_id)

    def _is_job_complete(self, job_id: str) -> bool:
        job = self._job_manager.get_job(job_id)
        if not job:
            return False
        return job.workflows_completed + job.workflows_failed >= job.workflows_total

    async def _fail_workflow_for_good(
        self,
        job_id: str,
        workflow_id: str,
        reason: str,
    ) -> None:
        """Fail for good a workflow that cannot run again -- its dispatch
        failed until its retry budget was spent (AD-44), or it was lost with
        its workers and cannot be retried: its dependents cascade, its
        failure goes to the job's origin with the cause, and the job
        completes if that was its last open workflow -- promptly, never left
        for the AD-34 timeout. The dispatcher runs this outside the dispatch
        pass that decided it: completing the job stops that pass's dispatch
        loop."""
        if (job := self._job_manager.get_job_by_id(job_id)) is None:
            return
        if (workflow := await self._mark_job_workflow_failed(job, workflow_id, reason)) is None:
            return

        await self._push_timeout_workflow_results(
            [self._build_timeout_workflow_push(job, workflow, reason)]
        )
        # Marking it failed ran its cascade, which completes the job itself
        # when its dependents closed it; a completed job is already gone.
        await self._complete_job_if_done(job_id)

    async def _mark_job_workflow_failed(
        self,
        job: JobInfo,
        workflow_id: str,
        reason: str,
    ) -> WorkflowInfo | None:
        """Mark the job's workflow with this id failed; returns it, or None when it is unknown or was not marked."""
        workflow = self._job_workflow_by_id(job, workflow_id)
        if workflow is None or not await self._job_manager.mark_workflow_failed(
            workflow.token, reason
        ):
            return None
        return workflow

    @staticmethod
    def _job_workflow_by_id(job: JobInfo, workflow_id: str) -> WorkflowInfo | None:
        """The job's first workflow whose token names this workflow id, or None."""
        return next(
            (
                workflow_info
                for workflow_info in job.workflows.values()
                if workflow_info.token.workflow_id == workflow_id
            ),
            None,
        )

    async def _handle_workflow_terminal_for_dispatch(
        self, job_id: str, workflow_id: str
    ) -> None:
        """Route a workflow's terminal transition into the dispatcher's
        dependency machinery (the ``JobManager.on_workflow_completed``
        callback — fired for BOTH polarities, outside the job lock).

        Success unblocks dependents (``mark_workflow_completed`` adds to
        their ``completed_dependencies`` and signals ready). Failure
        cascade-fails every transitive dependent at the JOB level, in one
        sweep along the graph the ``JobManager`` stored at registration:
        a dependent that can never dispatch must count toward
        ``workflows_failed`` so the job reaches its terminal promptly and
        truthfully — otherwise it strands until AD-34 declares a
        misleading ``timeout`` for work that deterministically failed.
        The sweep announces nothing (no re-entry into this callback), and
        the failed workflow and every dependent it failed leave the
        dispatch queue together; the job's tallies are then reported and
        its completion checked once.
        """
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return

        terminal_status, workflows_completed, workflows_failed = (
            await self._workflow_terminal_status_and_tallies(job, workflow_id)
        )
        if terminal_status is None:
            return

        # AD-38 JobProgressReported at LOCAL durability: tallies only
        # change on a workflow terminal, so this bounds WAL growth by
        # workflow count; unchanged tallies append nothing.
        await self._report_job_tallies_to_ledger(job_id, workflows_completed, workflows_failed)

        await self._route_terminal_workflow_to_dispatch(job, job_id, workflow_id, terminal_status)

    @classmethod
    async def _workflow_terminal_status_and_tallies(
        cls,
        job: JobInfo,
        workflow_id: str,
    ) -> tuple[WorkflowStatus | None, int, int]:
        """Under the job's lock, read the workflow's status and the job's completed and failed tallies."""
        async with job.lock:
            terminal_status = cls._job_workflow_status(job, workflow_id)
            workflows_completed = job.workflows_completed
            workflows_failed = job.workflows_failed
        return terminal_status, workflows_completed, workflows_failed

    @classmethod
    def _job_workflow_status(cls, job: JobInfo, workflow_id: str) -> WorkflowStatus | None:
        """The status of the job's first workflow whose token names this workflow id, or None."""
        return next(
            (
                workflow_info.status
                for workflow_info in job.workflows.values()
                if cls._workflow_token_id(workflow_info) == workflow_id
            ),
            None,
        )

    @staticmethod
    def _workflow_token_id(workflow_info: WorkflowInfo) -> str:
        """The workflow id a workflow's token names, or an empty string when it names none."""
        return workflow_info.token.workflow_id or ""

    async def _report_job_tallies_to_ledger(
        self,
        job_id: str,
        workflows_completed: int,
        workflows_failed: int,
    ) -> None:
        """Report the job's workflow tallies to the job ledger at LOCAL durability, when this manager keeps one."""
        if self._job_ledger is not None:
            await self._job_ledger.report_progress(
                job_id,
                datacenter_id=self._node_id.datacenter,
                completed_count=workflows_completed,
                failed_count=workflows_failed,
                durability=DurabilityLevel.LOCAL,
            )

    async def _route_terminal_workflow_to_dispatch(
        self,
        job: JobInfo,
        job_id: str,
        workflow_id: str,
        terminal_status: WorkflowStatus,
    ) -> None:
        """Unblock a succeeded workflow's dependents, or cascade a failed one's failure through them."""
        if terminal_status in (
            WorkflowStatus.COMPLETED,
            WorkflowStatus.AGGREGATED,
        ):
            await self._workflow_dispatcher.mark_workflow_completed(
                job_id, workflow_id
            )
            return
        await self._cascade_failed_workflow_dependents(job, job_id, workflow_id)

    async def _cascade_failed_workflow_dependents(
        self,
        job: JobInfo,
        job_id: str,
        workflow_id: str,
    ) -> None:
        """
        Fail every dependent of a failed workflow and drop them all from the dispatch queue.

        When dependents were failed, the job's new tallies are reported and
        its completion checked.
        """
        cascade_failed_workflow_ids = await self._job_manager.fail_workflow_dependents(
            job_id,
            workflow_id,
            f"dependency {workflow_id} failed — dependent workflow can never dispatch",
        )
        await self._workflow_dispatcher.remove_pending_workflows(
            job_id, [workflow_id, *cascade_failed_workflow_ids]
        )
        if not cascade_failed_workflow_ids:
            return

        if self._job_ledger is not None:
            await self._report_locked_job_tallies(job, job_id)

        await self._complete_job_if_done(job_id)

    async def _report_locked_job_tallies(self, job: JobInfo, job_id: str) -> None:
        """Read the job's tallies under its lock and report them to the job ledger."""
        async with job.lock:
            workflows_completed = job.workflows_completed
            workflows_failed = job.workflows_failed
        await self._report_job_tallies_to_ledger(job_id, workflows_completed, workflows_failed)

    def _is_known_terminal_job(self, job_id: str) -> bool:
        """Whether this manager knows ``job_id`` ended: its tracked job is
        terminal, or (after the tracked job's retention cleanup) the
        ledger records it terminal."""
        if (job := self._job_manager.get_job(job_id)) is not None:
            return JobStatusOrder().is_terminal(job.status)
        return self._ledger_records_job_terminal(job_id)

    def _ledger_records_job_terminal(self, job_id: str) -> bool:
        """Whether this manager keeps a job ledger and it records the job as terminal."""
        if self._job_ledger is None:
            return False
        ledger_job = self._job_ledger.get_job(job_id)
        return ledger_job is not None and ledger_job.is_terminal

    def _workflow_final_result_ack(
        self,
        *,
        accepted: bool,
        forwarded: bool = False,
        duplicate: bool = False,
        stale: bool = False,
        leader_addr: tuple[str, int] | None = None,
        error: str | None = None,
        reason: str = "",
    ) -> bytes:
        return WorkflowFinalResultAck(
            accepted=accepted,
            manager_id=self._node_id.full,
            is_leader=self.is_leader(),
            forwarded=forwarded,
            duplicate=duplicate,
            stale=stale,
            leader_addr=leader_addr,
            error=error,
            reason=reason,
        ).dump()

    def _resolve_job_leader_addr_for_result(
        self,
        job_id: str,
    ) -> tuple[str, int] | None:
        leader_addr = self._leases.get_job_leader_addr(job_id)
        if leader_addr is None:
            leader_addr = self._resolve_dc_leader_addr()
        if isinstance(leader_addr, list):
            leader_addr = tuple(leader_addr)
        return leader_addr

    async def _forward_workflow_final_result_to_leader(
        self,
        result: WorkflowFinalResult,
        data: bytes,
    ) -> bytes:
        leader_addr = self._resolve_job_leader_addr_for_result(result.job_id)
        if (refusal := self._unforwardable_workflow_final_result_ack(leader_addr)) is not None:
            return refusal

        response, _clock = await self.send_tcp(
            leader_addr,
            "workflow_final_result",
            data,
            timeout=self._config.tcp_timeout_standard_seconds,
        )
        return self._leader_workflow_final_result_answer(response, leader_addr)

    def _unforwardable_workflow_final_result_ack(
        self,
        leader_addr: tuple[str, int] | None,
    ) -> bytes | None:
        """Refuse a final result when no job leader is known or the known leader is this manager; else None."""
        if leader_addr is None:
            return self._workflow_final_result_ack(
                accepted=False,
                error="not job leader and no leader is known",
                reason="unknown_job_leader",
            )
        if leader_addr == (self._host, self._tcp_port):
            return self._workflow_final_result_ack(
                accepted=False,
                leader_addr=leader_addr,
                error="local manager does not own job lease",
                reason="local_lease_not_held",
            )
        return None

    def _leader_workflow_final_result_answer(
        self,
        response: bytes | Exception | None,
        leader_addr: tuple[str, int],
    ) -> bytes:
        """Relay the job leader's answer to a forwarded final result, or refuse when the forward failed or was rejected."""
        if isinstance(response, Exception):
            return self._workflow_final_result_ack(
                accepted=False,
                leader_addr=leader_addr,
                error=str(response),
                reason="forward_failed",
            )
        if self._is_leader_workflow_final_result_reply(response):
            return response
        return self._workflow_final_result_ack(
            accepted=False,
            leader_addr=leader_addr,
            error="leader rejected workflow final result",
            reason="leader_rejected",
        )

    @staticmethod
    def _is_leader_workflow_final_result_reply(response: bytes | None):
        """Whether a forwarded final result's response is a non-empty answer other than the error marker."""
        return response and isinstance(response, bytes) and response != b"error"

    @tcp.receive()
    async def workflow_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        try:
            result = WorkflowFinalResult.load(data)
            answer, result_recorded, parent_complete = await self._record_workflow_final_result(
                result, data
            )
            if answer is not None:
                return answer

            await self._apply_recorded_workflow_final_result(
                result, result_recorded, parent_complete
            )

            return self._workflow_final_result_ack(
                accepted=True,
                leader_addr=(self._host, self._tcp_port),
            )

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow result error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return self._workflow_final_result_ack(
                accepted=False,
                error=str(error),
                reason="exception",
            )

    async def _record_workflow_final_result(
        self,
        result: WorkflowFinalResult,
        data: bytes,
    ) -> tuple[bytes | None, bool, bool]:
        """
        Route a workflow final result to its job's leader, or record it here as that leader.

        Returns the answer to send back when the result is not newly
        recorded here -- for an ended job, a job led elsewhere, a duplicate
        or stale result, or one the job manager did not record -- else None,
        with whether the result was recorded and completed its parent.
        """
        if (answer := await self._route_workflow_final_result(result, data)) is not None:
            return answer, False, False
        (
            result_recorded,
            parent_complete,
            duplicate_result,
            stale_result,
            record_reason,
        ) = await self._job_manager.record_sub_workflow_result_checked(
            sub_workflow_token=result.workflow_id,
            result=result,
        )
        return (
            self._unrecorded_workflow_final_result_ack(
                result_recorded, duplicate_result, stale_result, record_reason
            ),
            result_recorded,
            parent_complete,
        )

    async def _route_workflow_final_result(
        self,
        result: WorkflowFinalResult,
        data: bytes,
    ) -> bytes | None:
        """
        Answer a final result this manager does not record as the job's leader, or return None.

        A result for a job that already ended cannot change it -- whichever
        manager leads it, and after its lease is gone. It is acked stale so
        the worker drops it instead of retrying it against a job nobody
        leads any more. A result for a job led elsewhere is forwarded to its
        leader.
        """
        if self._is_known_terminal_job(result.job_id):
            return self._workflow_final_result_ack(
                accepted=True,
                stale=True,
                leader_addr=(self._host, self._tcp_port),
                reason="job_terminal",
            )
        if not self._leases.is_job_leader(result.job_id):
            return await self._forward_workflow_final_result_to_leader(
                result,
                data,
            )
        return None

    def _unrecorded_workflow_final_result_ack(
        self,
        result_recorded: bool,
        duplicate_result: bool,
        stale_result: bool,
        record_reason: str | None,
    ) -> bytes | None:
        """Ack a duplicate or stale result as accepted, refuse an unrecorded one, and return None for a new one."""
        if self._is_repeated_workflow_final_result(duplicate_result, stale_result):
            return self._repeated_workflow_final_result_ack(
                duplicate_result, stale_result, record_reason
            )
        if not result_recorded:
            return self._not_recorded_workflow_final_result_ack(record_reason)
        return None

    def _repeated_workflow_final_result_ack(
        self,
        duplicate_result: bool,
        stale_result: bool,
        record_reason: str | None,
    ) -> bytes:
        """Ack a duplicate or stale final result as accepted, so the worker stops resending it."""
        return self._workflow_final_result_ack(
            accepted=True,
            duplicate=duplicate_result,
            stale=stale_result,
            leader_addr=(self._host, self._tcp_port),
            reason=record_reason or "",
        )

    @staticmethod
    def _is_repeated_workflow_final_result(duplicate_result: bool, stale_result: bool) -> bool:
        """Whether a final result was already recorded or is stale."""
        return duplicate_result or stale_result

    def _not_recorded_workflow_final_result_ack(self, record_reason: str | None) -> bytes:
        """Refuse a final result the job manager did not record, with its reason."""
        return self._workflow_final_result_ack(
            accepted=False,
            leader_addr=(self._host, self._tcp_port),
            error=record_reason or "workflow final result was not recorded",
            reason=record_reason or "not_recorded",
        )

    async def _apply_recorded_workflow_final_result(
        self,
        result: WorkflowFinalResult,
        result_recorded: bool,
        parent_complete: bool,
    ) -> None:
        """
        Act on a newly recorded final result: its counts, context and cores, its parent, and its job.

        Phase F2: closes the AD-26 outcome feedback loop. Emits an H8
        ExtensionOutcomeEvent, disseminates via #|o, and mirrors into
        TimeoutTrackingState. Idempotent on workflow_id -- duplicate result
        deliveries are safe. The job completes when this was its last open
        workflow.
        """
        await self._record_final_result_progress_and_context(result)

        if result.worker_id:
            await self._update_worker_cores_from_final_result(result)

        await self._handle_parent_workflow_completion(
            result, result_recorded, parent_complete
        )

        self._emit_workflow_outcome_event(result)

        await self._complete_job_if_done(result.job_id)

    async def _record_final_result_progress_and_context(self, result: WorkflowFinalResult) -> None:
        """
        Store a final result's run counts and apply its context updates, when it carries them.

        The run's final counts: the job's totals are built from them.
        """
        if result.final_progress is not None:
            await self._job_manager.set_final_workflow_progress(
                result.workflow_id, result.final_progress
            )

        if result.context_updates:
            await self._job_manager.apply_workflow_context(
                job_id=result.job_id,
                context_updates_bytes=result.context_updates,
            )

    async def _update_worker_cores_from_final_result(self, result: WorkflowFinalResult) -> None:
        """Update the reporting worker's available cores, signalling the dispatcher when they changed."""
        cores_updated = (
            await self._worker_pool.update_worker_cores_from_progress(
                result.worker_id,
                result.worker_available_cores,
                result.workflow_id,
                result.worker_cores_version,
            )
        )
        if cores_updated and self._workflow_dispatcher:
            self._workflow_dispatcher.signal_cores_available()


    @tcp.receive()
    async def cancel_job(self, addr: tuple[str, int], data: bytes, clock_time: int) -> bytes:
        return await self._cancellation.handle_cancel_job(addr, data, clock_time)


    @tcp.receive()
    async def workflow_cancellation_complete(self, addr: tuple[str, int], data: bytes, clock_time: int) -> bytes:
        return await self._cancellation.handle_workflow_cancellation_complete(addr, data, clock_time)

    @tcp.receive()
    async def state_sync_request(self, addr: tuple[str, int], data: bytes, clock_time: int) -> bytes:
        return await self._state_sync.handle_state_sync_request(addr, data, clock_time)

    @tcp.receive()
    async def extension_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle deadline extension request from worker (AD-26).

        Workers can request deadline extensions when:
        - Executing long-running workflows
        - System is under heavy load but making progress
        - Approaching timeout but not stuck

        Extensions use logarithmic decay and require progress to be granted.
        """
        try:
            return await self._handle_extension_request(addr, data)

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Extension request error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return self._extension_denial(str(error))

    def _extension_denial(self, denial_reason: str) -> bytes:
        """A denied ``HealthcheckExtensionResponse`` carrying ``denial_reason``."""
        return HealthcheckExtensionResponse(
            granted=False,
            extension_seconds=0.0,
            new_deadline=0.0,
            remaining_extensions=0,
            denial_reason=denial_reason,
        ).dump()

    async def _handle_extension_request(self, addr: tuple[str, int], data: bytes) -> bytes:
        """Rate-limit the request (AD-24), resolve its worker, and decide it."""
        request = HealthcheckExtensionRequest.load(data)

        # Rate limit check (AD-24)
        client_id = f"{addr[0]}:{addr[1]}"
        allowed, retry_after = await self._check_rate_limit_for_operation(
            client_id, "extension", "extension_request"
        )
        if not allowed:
            return self._extension_denial(f"Rate limited, retry after {retry_after:.1f}s")

        # Check if worker is registered
        worker_id = request.worker_id or self._manager_state.get_worker_id_from_addr(addr)

        return await self._decide_registered_worker_extension(request, worker_id)

    async def _decide_registered_worker_extension(
        self,
        request: HealthcheckExtensionRequest,
        worker_id: str | None,
    ) -> bytes:
        """Deny an unregistered or unknown worker; decide a known one's request."""
        if not worker_id:
            return self._extension_denial("Worker not registered")

        worker = self._manager_state.get_worker(worker_id)
        if not worker:
            return self._extension_denial("Worker not found")

        response = await self._process_extension_request_core(
            request, worker_id, worker
        )
        return response.dump()

    async def _process_extension_request_core(
        self,
        request: HealthcheckExtensionRequest,
        worker_id: str,
        worker,
    ) -> HealthcheckExtensionResponse:
        """Apply an AD-26 extension request once it's been validated.

        Shared by the TCP ``extension_request`` endpoint and the
        heartbeat-piggyback path on ``_handle_embedded_worker_heartbeat``.
        The caller is responsible for parsing the request and looking
        up ``worker_id`` / ``worker`` — this helper handles the route
        through H5 witnesses (or the legacy worker-level path), the
        worker-deadline write, the SWIM-bracket extension via the
        hierarchical failure detector, and the AD-34 Part 10.4.7
        timeout-strategy notification.

        Returning the response object (rather than a serialized blob)
        lets the heartbeat path inspect ``granted`` without needing
        to re-parse it.
        """
        # Get current deadline (or set default)
        current_deadline = self._current_worker_deadline(worker_id)

        response = self._resolve_extension_response(request, current_deadline)

        if response.granted:
            await self._apply_granted_extension(request, worker_id, worker, response)
        else:
            await self._handle_denied_extension(worker_id, response)

        return response

    def _current_worker_deadline(self, worker_id: str) -> float:
        """The worker's stored deadline, or 30s from now when it has none."""
        current_deadline = self._manager_state.get_worker_deadline(worker_id)
        if current_deadline is None:
            current_deadline = self._clock.monotonic() + 30.0
        return current_deadline

    def _resolve_extension_response(
        self,
        request: HealthcheckExtensionRequest,
        current_deadline: float,
    ) -> HealthcheckExtensionResponse:
        """Decide the extension through the H5 witnesses, else the worker-level path."""
        # Phase F1: route through the H5 multi-witness path when
        # the request carries an H3 ``workflow_id`` (workers using
        # the H4 autonomous trigger always set this). Falls back
        # to the legacy worker-level path when the workflow can't
        # be located on this manager (cross-manager workflows or
        # pre-H4 callers).
        response: HealthcheckExtensionResponse | None = None
        if request.workflow_id:
            response = self._route_extension_through_witnesses(
                request=request,
                current_deadline=current_deadline,
            )
        if response is None:
            response = self._worker_health_manager.handle_extension_request(
                request=request,
                current_deadline=current_deadline,
            )
        return response

    async def _apply_granted_extension(
        self,
        request: HealthcheckExtensionRequest,
        worker_id: str,
        worker: WorkerRegistration,
        response: HealthcheckExtensionResponse,
    ) -> None:
        """Write the granted deadline, extend the SWIM bracket, and stretch the
        job timeouts when the worker showed progress (AD-26, AD-34)."""
        self._manager_state.set_worker_deadline(
            worker_id, response.new_deadline
        )

        # AD-26 Issue 3: Integrate with SWIM timing wheels (SWIM as authority)
        await self._extend_swim_bracket_for_extension(request, worker_id, worker)

        # Notify timeout strategies of extension (AD-34 Part 10.4.7) --
        # only a grant the worker earned with progress: AD-34 stretches
        # a job's timeout for legitimately long work. A request showing
        # none (the dispatch-time one, protecting the worker's liveness
        # through workflow startup) extends the worker's deadline and
        # SWIM bracket above, never the job's -- or every dispatch
        # would add a full grant to the job's explicit timeout.
        if self._extension_request_shows_progress(request):
            await self._notify_timeout_strategies_of_extension(
                worker_id=worker_id,
                extension_seconds=response.extension_seconds,
                worker_progress=request.current_progress,
            )

        await self._udp_logger.log(
            ServerInfo(
                message=f"Granted {response.extension_seconds:.1f}s extension to worker {worker_id[:8]}... (reason: {request.reason})",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _extend_swim_bracket_for_extension(
        self,
        request: HealthcheckExtensionRequest,
        worker_id: str,
        worker: WorkerRegistration,
    ) -> None:
        """Ask the hierarchical detector to extend the worker's SWIM bracket (AD-26 Issue 3)."""
        hierarchical_detector = self.get_hierarchical_detector()
        if not hierarchical_detector:
            return

        worker_addr = (worker.node.host, worker.node.udp_port)
        (
            swim_granted,
            swim_extension,
            swim_denial,
            is_warning,
        ) = await hierarchical_detector.request_extension(
            node=worker_addr,
            reason=request.reason,
            current_progress=request.current_progress,
        )
        if not swim_granted:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"SWIM denied extension for {worker_id[:8]}... despite WorkerHealthManager grant: {swim_denial}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _extension_request_shows_progress(self, request: HealthcheckExtensionRequest) -> bool:
        """True when the request reports any progress dimension above zero (AD-34)."""
        return self._extension_request_shows_item_progress(
            request
        ) or self._extension_request_shows_step_progress(request)

    def _extension_request_shows_item_progress(self, request: HealthcheckExtensionRequest) -> bool:
        """True when the request reports progress or completed items."""
        return request.current_progress > 0.0 or (request.completed_items or 0) > 0

    def _extension_request_shows_step_progress(self, request: HealthcheckExtensionRequest) -> bool:
        """True when the request reports step transitions or completed actions."""
        return request.step_transitions > 0 or request.actions_completed > 0

    async def _handle_denied_extension(
        self,
        worker_id: str,
        response: HealthcheckExtensionResponse,
    ) -> None:
        """Log the denial and whether the worker now should be evicted (AD-26)."""
        await self._udp_logger.log(
            ServerWarning(
                message=f"Denied extension to worker {worker_id[:8]}...: {response.denial_reason}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Check if worker should be evicted
        should_evict, eviction_reason = (
            self._worker_health_manager.should_evict_worker(worker_id)
        )
        if should_evict:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Worker {worker_id[:8]}... should be evicted: {eviction_reason}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    @tcp.receive()
    async def ping(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle ping request: this manager's status, its workers' health
        and its active jobs. A request that cannot be answered gets the
        server's error reply, and the failure is logged there."""
        request = PingRequest.load(data)

        worker_statuses = self._ping_worker_statuses()
        active_jobs = self._non_terminal_jobs()

        return ManagerPingResponse(
            request_id=request.request_id,
            manager_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            host=self._host,
            port=self._tcp_port,
            is_leader=self.is_leader(),
            state=self._manager_state.manager_state_enum.value,
            term=self._leader_election.state.current_term,
            total_cores=sum(worker_status.total_cores for worker_status in worker_statuses),
            available_cores=self._healthy_available_cores(worker_statuses),
            worker_count=self._manager_state.get_worker_count(),
            healthy_worker_count=self._worker_health_monitor.get_healthy_worker_count(),
            workers=worker_statuses,
            active_job_ids=[job.job_id for job in active_jobs],
            active_job_count=len(active_jobs),
            active_workflow_count=self._unfinished_workflow_count(active_jobs),
            peer_managers=sorted(self._manager_state.get_active_manager_peers()),
            resources=self._resource_gossip.view(),
        ).dump()

    def _ping_worker_statuses(self) -> list[WorkerStatus]:
        """Each registered worker's health and cores, for a ping response."""
        return [
            WorkerStatus(
                worker_id=worker_id,
                state=self._worker_health_monitor.get_worker_health_status(worker_id),
                available_cores=worker.available_cores,
                total_cores=worker.total_cores,
            )
            for worker_id, worker in self._manager_state.iter_workers()
        ]

    def _non_terminal_jobs(self) -> list[JobInfo]:
        """The jobs not yet in a terminal status."""
        status_order = JobStatusOrder()
        return [
            job for job in self._job_manager.iter_jobs() if not status_order.is_terminal(job.status)
        ]

    def _healthy_available_cores(self, worker_statuses: list[WorkerStatus]) -> int:
        """The available cores of the healthy workers."""
        return sum(
            worker_status.available_cores
            for worker_status in worker_statuses
            if worker_status.state == "healthy"
        )

    def _unfinished_workflow_count(self, active_jobs: list[JobInfo]) -> int:
        """How many workflows of the active jobs are not completed."""
        return sum(
            job.workflows_total - job.workflows_completed for job in active_jobs
        )

    async def _track_registered_gate(self, gate_info: GateInfo) -> None:
        """Record a gate learned through a registration handshake.

        Shared by both registration directions — a gate registering with
        us (``gate_register``) and us registering with a gate (operator
        join via the gate's ``manager_register``) — so stale-identity
        eviction, rejoin reset and SWIM probing behave identically.
        """
        # Track gate addresses
        gate_tcp_addr = (gate_info.tcp_host, gate_info.tcp_port)
        gate_udp_addr = (gate_info.udp_host, gate_info.udp_port)
        stale_gate_ids = self._stale_gate_ids_sharing_address(gate_info, gate_tcp_addr, gate_udp_addr)
        requires_rejoin_reset = self._gate_registration_requires_rejoin_reset(
            gate_info, gate_udp_addr, stale_gate_ids
        )

        await self._registry.register_gate(gate_info)
        self._manager_state.set_gate_udp_to_tcp_mapping(
            gate_udp_addr, gate_tcp_addr
        )

        # Add to SWIM probing
        if requires_rejoin_reset:
            await self.reset_peer_for_rejoin(gate_udp_addr)
            self._task_runner.run(
                self._handle_gate_peer_recovery,
                gate_udp_addr,
                gate_tcp_addr,
            )
        else:
            await self.add_unconfirmed_peer(gate_udp_addr)
        self._probe_scheduler.add_member(gate_udp_addr)
        # Explicit registration handshake — see ``manager_peer_register``.
        self.register_peer(gate_udp_addr)

    def _stale_gate_ids_sharing_address(
        self,
        gate_info: GateInfo,
        gate_tcp_addr: tuple[str, int],
        gate_udp_addr: tuple[str, int],
    ) -> list[str]:
        """Known gates under another id at the registering gate's TCP or UDP address."""
        return [
            gate_id
            for gate_id, known_gate in self._manager_state.iter_known_gates()
            if self._is_other_node_at_address(
                gate_id, known_gate, gate_info.node_id, gate_tcp_addr, gate_udp_addr
            )
        ]

    def _is_other_node_at_address(
        self,
        known_node_id: str,
        known_node: GateInfo | ManagerInfo,
        registering_node_id: str,
        tcp_addr: tuple[str, int],
        udp_addr: tuple[str, int],
    ) -> bool:
        """True for another node id known at one of the registering node's addresses."""
        return known_node_id != registering_node_id and self._node_info_shares_address(
            known_node, tcp_addr, udp_addr
        )

    def _node_info_shares_address(
        self,
        known_node: GateInfo | ManagerInfo,
        tcp_addr: tuple[str, int],
        udp_addr: tuple[str, int],
    ) -> bool:
        """True when the known node has the TCP or the UDP address."""
        return (
            (known_node.tcp_host, known_node.tcp_port) == tcp_addr
            or (known_node.udp_host, known_node.udp_port) == udp_addr
        )

    def _gate_registration_requires_rejoin_reset(
        self,
        gate_info: GateInfo,
        gate_udp_addr: tuple[str, int],
        stale_gate_ids: list[str],
    ) -> bool:
        """True when the gate replaces a stale identity, is SUSPECT/DEAD in
        SWIM, owes a rejoin incarnation, or is tracked unhealthy."""
        gate_node_state = self._incarnation_tracker.get_node_state(gate_udp_addr)
        return (
            bool(stale_gate_ids)
            or self._node_state_suspect_or_dead(gate_node_state)
            or self._gate_rejoin_pending(gate_info, gate_udp_addr)
        )

    def _node_state_suspect_or_dead(self, node_state: NodeState | None) -> bool:
        """True when SWIM holds the node SUSPECT or DEAD."""
        return (
            node_state is not None
            and node_state.status in (b"SUSPECT", b"DEAD")
        )

    def _gate_rejoin_pending(self, gate_info: GateInfo, gate_udp_addr: tuple[str, int]) -> bool:
        """True when the gate owes a rejoin incarnation or is tracked unhealthy."""
        return (
            self._incarnation_tracker.get_required_rejoin_incarnation(
                gate_udp_addr
            )
            > 0
            or self._manager_state.get_gate_unhealthy_since(gate_info.node_id)
            is not None
        )

    @tcp.receive()
    async def gate_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle gate registration via TCP."""
        try:
            return await self._register_gate(addr, data)

        except Exception as error:
            return self._gate_registration_rejection(str(error))

    def _gate_registration_rejection(self, error: str) -> bytes:
        """A refused ``GateRegistrationResponse`` carrying ``error``."""
        return GateRegistrationResponse(
            accepted=False,
            manager_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            healthy_managers=[],
            error=error,
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
        ).dump()

    async def _register_gate(self, addr: tuple[str, int], data: bytes) -> bytes:
        """Validate the gate's isolation and certificate, then accept it."""
        registration = GateRegistrationRequest.load(data)

        rejection = await self._gate_registration_isolation_rejection(addr, registration)
        if rejection is not None:
            return rejection

        return await self._accept_gate_registration(registration)

    async def _gate_registration_isolation_rejection(
        self,
        addr: tuple[str, int],
        registration: GateRegistrationRequest,
    ) -> bytes | None:
        """The rejection for a cluster, environment or mTLS mismatch, else None (AD-28)."""
        # Cluster isolation validation (AD-28)
        if registration.cluster_id != self._env.CLUSTER_ID:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Gate {registration.node_id} rejected: cluster_id mismatch"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return self._gate_registration_rejection(
                f"Cluster isolation violation: gate cluster_id '{registration.cluster_id}' does not match"
            )

        if registration.environment_id != self._env.ENVIRONMENT_ID:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Gate {registration.node_id} rejected: environment_id mismatch"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return self._gate_registration_rejection(
                "Environment isolation violation: gate environment_id mismatch"
            )

        return await self._gate_registration_mtls_rejection(addr, registration)

    async def _gate_registration_mtls_rejection(
        self,
        addr: tuple[str, int],
        registration: GateRegistrationRequest,
    ) -> bytes | None:
        """The rejection for a gate failing mTLS claim validation, else None."""
        mtls_error = await self._validate_mtls_claims(
            addr,
            "Gate",
            registration.node_id,
        )
        if mtls_error:
            return self._gate_registration_rejection(mtls_error)
        return None

    async def _accept_gate_registration(self, registration: GateRegistrationRequest) -> bytes:
        """Negotiate the protocol version (AD-25), track the gate, and accept it."""
        # Protocol version validation (AD-25)
        gate_version = ProtocolVersion(
            registration.protocol_version_major,
            registration.protocol_version_minor,
        )
        gate_caps = NodeCapabilities(
            protocol_version=gate_version,
            capabilities=self._gate_capability_set(registration),
        )
        try:
            negotiated = await self._version_skew.negotiate_with_gate(registration.node_id, gate_caps)
        except ValueError:
            return self._gate_registration_rejection(
                f"Incompatible protocol version: {gate_version}"
            )

        # Store gate info
        gate_info = GateInfo(
            node_id=registration.node_id,
            tcp_host=registration.tcp_host,
            tcp_port=registration.tcp_port,
            udp_host=registration.udp_host,
            udp_port=registration.udp_port,
            datacenter=(
                registration.datacenter if registration.datacenter else "global"
            ),
            is_leader=registration.is_leader,
        )

        await self._track_registered_gate(gate_info)

        negotiated_caps_str = ",".join(sorted(negotiated.common_features))
        return GateRegistrationResponse(
            accepted=True,
            manager_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            healthy_managers=self._get_healthy_managers(),
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            capabilities=negotiated_caps_str,
        ).dump()

    def _gate_capability_set(self, registration: GateRegistrationRequest) -> set[str]:
        """The gate's advertised capabilities as a set."""
        return (
            set(registration.capabilities.split(","))
            if registration.capabilities
            else set()
        )

    @tcp.receive()
    async def worker_discovery(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle worker discovery broadcast from peer manager."""
        try:
            broadcast = WorkerDiscoveryBroadcast.load(data)

            worker_id = broadcast.worker_id

            # Skip if already registered
            if self._manager_state.has_worker(worker_id):
                return b"ok"

            # Schedule direct registration with the worker
            worker_tcp_addr = tuple(broadcast.worker_tcp_addr)
            worker_udp_addr = tuple(broadcast.worker_udp_addr)

            worker_snapshot = WorkerStateSnapshot(
                node_id=worker_id,
                host=worker_tcp_addr[0],
                tcp_port=worker_tcp_addr[1],
                udp_port=worker_udp_addr[1],
                state=WorkerState.HEALTHY.value,
                total_cores=broadcast.available_cores,
                available_cores=broadcast.available_cores,
                version=0,
            )

            self._task_runner.run(
                self._register_with_discovered_worker,
                worker_snapshot,
            )

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Worker discovery error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def worker_heartbeat(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle worker heartbeat via TCP."""
        try:
            heartbeat = WorkerHeartbeat.load(data)

            await self._worker_health_monitor.handle_worker_heartbeat(heartbeat, addr)

            worker_id = heartbeat.node_id
            if self._manager_state.has_worker(worker_id):
                # Cores the worker reports freed wake every job's dispatch
                # loop through the pool's capacity generation. No dispatch
                # runs inside this reply: a pass per job here held the
                # worker's notification open through every dispatch it sent.
                await self._worker_pool.process_heartbeat(worker_id, heartbeat)

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Worker heartbeat error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def worker_state_update(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        try:
            update = WorkerStateUpdate.from_bytes(data)
            if update is None:
                return b"invalid"

            return await self._disseminate_worker_state_update(update, addr)

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Worker state update error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    async def _disseminate_worker_state_update(
        self,
        update: WorkerStateUpdate,
        addr: tuple[str, int],
    ) -> bytes:
        """Hand the update to the worker disseminator; its verdict as the reply."""
        if self._worker_disseminator is None:
            return b"not_ready"

        accepted = await self._worker_disseminator.handle_worker_state_update(
            update, addr
        )

        return b"accepted" if accepted else b"rejected"

    @tcp.receive()
    async def list_workers(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        try:
            if self._worker_disseminator is None:
                return WorkerListResponse(
                    manager_id=self._node_id.full, workers=[]
                ).dump()

            response = self._worker_disseminator.build_worker_list_response()
            return response.dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"List workers error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def workflow_reassignment(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        try:
            batch = WorkflowReassignmentBatch.from_bytes(data)
            if (refusal := await self._workflow_reassignment_batch_refusal(batch)) is not None:
                return refusal

            applied_reassignments, requeued_workflows = await self._apply_workflow_reassignment_batch(batch)
            await self._log_applied_workflow_reassignments(applied_reassignments, requeued_workflows)

            return b"accepted"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow reassignment error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    async def _workflow_reassignment_batch_refusal(
        self,
        batch: WorkflowReassignmentBatch | None,
    ) -> bytes | None:
        """
        Answer a reassignment batch this manager does not apply, or return None to apply it.

        An unreadable batch is invalid and this manager's own batch is
        ignored; any other is logged, then refused while this manager is not
        ready to apply it.
        """
        if (answer := self._unusable_workflow_reassignment_batch_answer(batch)) is not None:
            return answer

        await self._udp_logger.log(
            ServerDebug(
                message=f"Received {len(batch.reassignments)} workflow reassignments from {batch.originating_manager_id[:8]}... (worker {batch.failed_worker_id[:8]}... {batch.reason})",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        return self._workflow_reassignment_not_ready_answer()

    def _unusable_workflow_reassignment_batch_answer(
        self,
        batch: WorkflowReassignmentBatch | None,
    ) -> bytes | None:
        """Answer an unreadable batch as invalid and this manager's own batch as self; else None."""
        if batch is None:
            return b"invalid"

        if batch.originating_manager_id == self._node_id.full:
            return b"self"
        return None

    def _workflow_reassignment_not_ready_answer(self) -> bytes | None:
        """Answer not ready while this manager has no job manager or workflow dispatcher; else None."""
        if not self._job_manager or not self._workflow_dispatcher:
            return b"not_ready"
        return None

    async def _apply_workflow_reassignment_batch(
        self,
        batch: WorkflowReassignmentBatch,
    ) -> tuple[int, int]:
        """Apply each reassignment in a batch; returns how many were applied and how many requeued."""
        applied_reassignments = 0
        requeued_workflows = 0

        for job_id, workflow_id, sub_workflow_token in batch.reassignments:
            applied_count, requeued_count = self._workflow_reassignment_tallies(
                *await self._apply_one_workflow_reassignment(
                    batch, job_id, workflow_id, sub_workflow_token
                )
            )
            applied_reassignments += applied_count
            requeued_workflows += requeued_count

        return applied_reassignments, requeued_workflows

    @staticmethod
    def _workflow_reassignment_tallies(applied: bool, requeued: bool) -> tuple[int, int]:
        """Count one reassignment's outcome: one applied when applied, one requeued when requeued."""
        return (1 if applied else 0), (1 if requeued else 0)

    async def _apply_one_workflow_reassignment(
        self,
        batch: WorkflowReassignmentBatch,
        job_id: str,
        workflow_id: str,
        sub_workflow_token: str,
    ) -> tuple[bool, bool]:
        """
        Apply one reassignment from a batch; returns (applied, requeued).

        Only the job's leader decides its workflows' retries; a follower
        mirrors the superseded sub and requeues nothing.
        """
        if not self._leases.is_job_leader(job_id):
            applied, _lost_every_sub = await self._job_manager.apply_workflow_reassignment(
                job_id=job_id,
                workflow_id=workflow_id,
                sub_workflow_token=sub_workflow_token,
                failed_worker_id=batch.failed_worker_id,
            )
            return applied, False
        return await self._apply_workflow_reassignment_state(
            job_id=job_id,
            workflow_id=workflow_id,
            sub_workflow_token=sub_workflow_token,
            failed_worker_id=batch.failed_worker_id,
            reason=batch.reason,
            loss_is_charged=self._workflow_reassignment_loss_is_charged(batch),
        )

    def _workflow_reassignment_loss_is_charged(self, batch: WorkflowReassignmentBatch) -> bool:
        """Whether a batch's worker loss is charged to its workflows: not our own eviction, nor a systemic hold."""
        return batch.reason != "worker_evicted" and not self._systemic_eviction_hold

    async def _log_applied_workflow_reassignments(
        self,
        applied_reassignments: int,
        requeued_workflows: int,
    ) -> None:
        """Log a batch's applied and requeued counts when either is non-zero."""
        if applied_reassignments or requeued_workflows:
            await self._udp_logger.log(
                ServerDebug(
                    message=(
                        "Applied workflow reassignment updates: "
                        f"applied={applied_reassignments}, requeued={requeued_workflows}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _release_job_submission_claim(self, claimed_job_id: str | None) -> None:
        """Release the job id this request claimed for its decision, if it claimed one."""
        if claimed_job_id is not None:
            self._job_submissions_in_progress.discard(claimed_job_id)

    @tcp.receive()
    async def job_submission(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job submission from gate or client."""
        submission: JobSubmission | None = None
        idempotency_key: IdempotencyKey | None = None
        idempotency_reserved = False
        claimed_job_id: str | None = None
        # The job this request created, until it is handed to dispatch: a
        # failure before then takes it down again, so a refused submission
        # leaves no job behind -- one a retry would be told was accepted.
        unadmitted_job: JobInfo | None = None

        try:
            if (refusal := await self._job_submission_load_refusal(addr)) is not None:
                return refusal

            submission = JobSubmission.load(data)
            negotiated_caps_str, idempotency_key, refusal = self._screen_job_submission(submission)
            if refusal is not None:
                return refusal
            self._job_submissions_in_progress.add(submission.job_id)
            claimed_job_id = submission.job_id

            idempotency_reserved, refusal = await self._admit_and_reserve_job_submission(
                submission, idempotency_key
            )
            if refusal is not None:
                return refusal

            workflows = self._prepare_submission_workflows(submission)
            callback_addr = self._submission_callback_address(submission)
            await self._clear_refused_record_and_admit(submission, workflows)

            job_info = await self._job_manager.create_job(
                submission=submission,
                callback_addr=callback_addr,
            )
            if job_info.submission is not submission:
                # The job arrived while this submission was being decided
                # -- announced by a peer deciding the same job id. It is
                # that peer's to decide: nothing here registers over it,
                # and the retry is answered once it is decided. A key
                # reserved here is released, or it would answer "in
                # progress" until its reservation lapsed.
                idempotency_reserved = await self._release_contested_idempotency_key(
                    idempotency_reserved, idempotency_key
                )
                return self._contested_submission_ack(submission.job_id, job_info.leader_node_id)
            unadmitted_job = job_info

            await self._admit_submitted_job(job_info, submission, workflows, callback_addr, addr)
            # Dispatch can reach workers from here: the job is admitted, and
            # whatever happens next happens to a job that runs.
            unadmitted_job = None
            await self._dispatch_admitted_job(submission)

            return await self._accept_job_submission(
                submission, negotiated_caps_str, idempotency_reserved, idempotency_key
            )

        except Exception as error:
            return await self._answer_failed_job_submission(
                error, submission, unadmitted_job, idempotency_reserved, idempotency_key
            )
        finally:
            self._release_job_submission_claim(claimed_job_id)

    async def _job_submission_load_refusal(self, addr: tuple[str, int]) -> bytes | None:
        """
        Refuse a submission this manager is too loaded to take, before it is parsed.

        Returns the AD-24 rate-limit response when the submitting address is
        over its job-submission rate, the overload refusal when the load
        shedder sheds job submissions, and None when the submission may be
        read.
        """
        client_id = f"{addr[0]}:{addr[1]}"
        rate_limit_result = await self._rate_limiter.check_rate_limit(
            client_id, "job_submit"
        )
        if not rate_limit_result.allowed:
            return RateLimitResponse(
                operation="job_submit",
                retry_after_seconds=rate_limit_result.retry_after_seconds,
            ).dump()

        if self._load_shedder.should_shed_handler("job_submission"):
            overload_state = self._load_shedder.get_current_state()
            return JobAck(
                job_id="",
                accepted=False,
                error=f"System under load ({overload_state.value}), please retry later",
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()
        return None

    def _screen_job_submission(
        self,
        submission: JobSubmission,
    ) -> tuple[str | None, IdempotencyKey | None, bytes | None]:
        """
        Screen a parsed submission before its job id is claimed.

        Negotiates the protocol version (AD-25), checks the submission's own
        contract, parses its idempotency key and looks for an answer already
        decided for that key or job id. Returns the negotiated capabilities,
        the parsed idempotency key, and the refusal or earlier answer to send
        back -- None when the submission goes on to be decided here.
        """
        client_version = ProtocolVersion(
            major=getattr(submission, "protocol_version_major", 1),
            minor=getattr(submission, "protocol_version_minor", 0),
        )
        negotiated_caps_str = self._version_skew.negotiate_with_client(
            client_version, getattr(submission, "capabilities", "")
        )
        if (
            refusal := self._submission_contract_refusal(
                submission, negotiated_caps_str, client_version
            )
        ) is not None:
            return negotiated_caps_str, None, refusal

        idempotency_key, refusal = self._parse_submission_idempotency_key(submission)
        if refusal is not None:
            return negotiated_caps_str, idempotency_key, refusal
        return (
            negotiated_caps_str,
            idempotency_key,
            self._existing_job_submission_answer(submission, negotiated_caps_str),
        )

    def _submission_contract_refusal(
        self,
        submission: JobSubmission,
        negotiated_caps_str: str | None,
        client_version: ProtocolVersion,
    ) -> bytes | None:
        """
        Refuse a submission whose protocol version or resource budget this manager cannot honor.

        Returns the refusal for a client version no capability set was
        negotiated with, then for a job whose own resource budget (AD-41)
        cannot be enforced here, and None when neither applies.
        """
        if negotiated_caps_str is None:
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=f"Incompatible protocol version: {client_version}",
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()

        if (budget_error := self._resource_budget_rejection(submission)) is not None:
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=budget_error,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()
        return None

    def _submission_carries_ledgered_idempotency_key(self, submission: JobSubmission):
        """Whether a submission names an idempotency key and this manager keeps an idempotency ledger."""
        return submission.idempotency_key and self._idempotency_ledger is not None

    def _parse_submission_idempotency_key(
        self,
        submission: JobSubmission,
    ) -> tuple[IdempotencyKey | None, bytes | None]:
        """
        Parse a submission's idempotency key and answer from the ledger when the key was seen before.

        Returns (None, None) when the submission names no key or this manager
        keeps no idempotency ledger, (None, refusal) when the key does not
        parse, and otherwise the parsed key with the answer the ledger already
        holds for it -- None when the key is new.
        """
        if not self._submission_carries_ledgered_idempotency_key(submission):
            return None, None
        try:
            idempotency_key = IdempotencyKey.parse(submission.idempotency_key)
        except ValueError as error:
            return None, JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=str(error),
            ).dump()
        return idempotency_key, self._recorded_idempotency_answer(idempotency_key, submission.job_id)

    def _recorded_idempotency_answer(
        self,
        idempotency_key: IdempotencyKey,
        job_id: str,
    ) -> bytes | None:
        """Return the answer the idempotency ledger holds for a key, or None when the ledger has no entry for it."""
        existing_entry = self._idempotency_ledger.get_by_key(idempotency_key)
        if existing_entry is None:
            return None
        return self._duplicate_idempotency_answer(existing_entry, job_id)

    def _duplicate_idempotency_answer(
        self,
        entry: IdempotencyLedgerEntry,
        job_id: str,
    ) -> bytes:
        """
        Answer a submission whose idempotency key the ledger already holds.

        AD-40: an entry with a recorded answer is replayed -- the original
        decision, for the original job, marked as a duplicate's answer. A
        committed or rejected entry without one is answered from its status.
        A pending entry is transient by the shared vocabulary: the attempt
        holding the key is decided shortly. Worded otherwise, the gate took it
        for a refusal and dispatched the job to another datacenter while this
        one ran it.
        """
        if entry.result_serialized is not None:
            original_ack = JobAck.load(entry.result_serialized)
            original_ack.was_duplicate = True
            original_ack.original_job_id = original_ack.job_id
            return original_ack.dump()
        if entry.status in (
            IdempotencyStatus.COMMITTED,
            IdempotencyStatus.REJECTED,
        ):
            return self._decided_idempotency_answer(entry, job_id)
        return self._submission_in_progress_ack(job_id)

    @staticmethod
    def _decided_idempotency_answer(
        entry: IdempotencyLedgerEntry,
        job_id: str,
    ) -> bytes:
        """Answer a duplicate submission from a committed or rejected idempotency entry that recorded no answer."""
        original_job_id = entry.job_id or job_id
        return JobAck(
            job_id=original_job_id,
            accepted=entry.status == IdempotencyStatus.COMMITTED,
            error="Duplicate request"
            if entry.status == IdempotencyStatus.REJECTED
            else None,
            was_duplicate=True,
            original_job_id=original_job_id,
        ).dump()

    @staticmethod
    def _submission_in_progress_ack(job_id: str) -> bytes:
        """The transient refusal telling a submitter its job id is still being decided, so it retries."""
        return JobAck(
            job_id=job_id,
            accepted=False,
            error="submission in progress, retry",
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
        ).dump()

    def _accepted_submission_ack(self, job_id: str, negotiated_caps_str: str | None) -> bytes:
        """The acceptance answer for a job id, queued behind the jobs this manager holds."""
        return JobAck(
            job_id=job_id,
            accepted=True,
            queued_position=self._job_manager.job_count,
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            capabilities=negotiated_caps_str,
        ).dump()

    def _existing_job_submission_answer(
        self,
        submission: JobSubmission,
        negotiated_caps_str: str | None,
    ) -> bytes | None:
        """
        Answer a submission for a job id this manager already knows, or return None for a new one.

        A job id's submission is decided once, by one request at a time. A
        retry -- its earlier attempt's answer was lost or late -- waits out an
        attempt still being decided, and is answered from the job once there
        is one: re-running the submission reset the job's fence, restarted
        its timeout, re-recorded it in the ledger and registered its
        workflows again over the ones running.
        """
        if submission.job_id in self._job_submissions_in_progress:
            return self._submission_in_progress_ack(submission.job_id)
        existing_job = self._job_manager.get_job_by_id(submission.job_id)
        if self._is_admitted_job(existing_job):
            return self._admitted_job_submission_answer(existing_job, submission, negotiated_caps_str)
        return self._announced_job_submission_answer(existing_job, submission.job_id)

    @staticmethod
    def _is_admitted_job(job: JobInfo | None):
        """Whether a job exists and was admitted -- its submission is held here or its workflows are known."""
        return job is not None and (job.submission is not None or job.workflows)

    def _admitted_job_submission_answer(
        self,
        existing_job: JobInfo,
        submission: JobSubmission,
        negotiated_caps_str: str | None,
    ) -> bytes:
        """
        Answer a submission for a job that was already admitted.

        An admitted job -- submitted here, or replicated here by the leader
        that admitted it -- was accepted, whatever this manager's role now.
        Its spec is compared where this manager has it (a job a peer leads is
        known by id only): a different spec under the same id is refused.
        """
        if (
            existing_job.submission is not None
            and existing_job.submission.workflows != submission.workflows
        ):
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=f"Job id {submission.job_id} is in use by another job",
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()
        return self._accepted_submission_ack(submission.job_id, negotiated_caps_str)

    def _announced_job_submission_answer(
        self,
        existing_job: JobInfo | None,
        job_id: str,
    ) -> bytes | None:
        """
        Answer a submission for a job announced but not admitted, or return None.

        The manager that announced it is still deciding it -- or died deciding
        it, and its takeover settles it -- so the submitter retries. A job
        that ended unadmitted was refused and is decided afresh, as is an
        unknown job id.
        """
        if existing_job is not None and not JobStatusOrder().is_terminal(existing_job.status):
            return self._submission_in_progress_ack(job_id)
        return None

    async def _admit_and_reserve_job_submission(
        self,
        submission: JobSubmission,
        idempotency_key: IdempotencyKey | None,
    ) -> tuple[bool, bytes | None]:
        """
        Refuse a claimed submission this manager may not admit, else reserve its idempotency key.

        Returns (False, refusal) when the manager's role, the cluster or the
        datacenter's capacity refuses the job, and otherwise the outcome of
        reserving the idempotency key: whether it was reserved, and the
        earlier answer to send back when the key was already held.
        """
        if (refusal := self._job_admission_refusal(submission.job_id)) is not None:
            return False, refusal
        return await self._reserve_submission_idempotency_key(idempotency_key, submission.job_id)

    def _job_admission_refusal(self, job_id: str) -> bytes | None:
        """
        Refuse a new job this manager may not admit now, or return None when it may.

        The checks run in order -- the manager's own state, its leadership and
        quorum, the cluster's membership, then the datacenter's worker
        capacity -- and the first refusal is the answer.
        """
        for admission_check in (
            self._manager_state_admission_refusal,
            self._leadership_admission_refusal,
            self._cluster_admission_refusal,
            self._capacity_admission_refusal,
        ):
            if (refusal := admission_check(job_id)) is not None:
                return refusal
        return None

    def _manager_state_admission_refusal(self, job_id: str) -> bytes | None:
        """Refuse a job when this manager is not ACTIVE or its clock is fenced; otherwise return None."""
        # Only active managers accept jobs
        if self._manager_state.manager_state_enum != ManagerStateEnum.ACTIVE:
            return JobAck(
                job_id=job_id,
                accepted=False,
                error=f"Manager is {self._manager_state.manager_state_enum.value}, not accepting jobs",
            ).dump()

        if self._is_clock_fenced():
            # The fence is re-judged only as clock offsets are re-measured
            # (AD-39): one probe interval is the soonest it can lift.
            return JobAck(
                job_id=job_id,
                accepted=False,
                error="Manager clock fenced (offset beyond bound), not accepting jobs",
                retry_after_seconds=self._env.HLC_OFFSET_PROBE_INTERVAL_SECONDS,
            ).dump()
        return None

    def _leadership_admission_refusal(self, job_id: str) -> bytes | None:
        """
        Refuse a job when this manager is not the datacenter leader or lacks quorum; otherwise return None.

        Leader fencing: only the DC leader accepts new jobs, to prevent
        duplicates during multi-gate submit storms (FIX 2.5). AD-3:
        leadership is not enough to accept writes. A node isolated from
        configured quorum may still have a locally valid leader lease for a
        short window, but accepting a new job in that state creates a
        partition-side write that cannot be safely replicated or fenced.
        """
        if not self.is_leader():
            return self._not_leader_submission_ack(job_id)

        if not self._leadership.has_quorum():
            # Quorum returns as SWIM confirms peers alive again, one probe
            # period at a time (AD-29): the soonest it can be regained.
            return JobAck(
                job_id=job_id,
                accepted=False,
                error="No quorum available; rejecting job submission",
                leader_addr=None,
                retry_after_seconds=float(self._env.SWIM_UDP_POLL_INTERVAL),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()
        return None

    def _not_leader_submission_ack(self, job_id: str) -> bytes:
        """
        Refuse a job at a follower, naming the datacenter leader to retry at.

        Multi-source leader resolution -- election state, peer heartbeats and
        a last-known-leader scan -- yields None only when no peer has ever
        reported a leader, in which case the client treats the response as
        transient and comes back when this node's election next decides:
        the refusal carries that wait as its retry hint. A named leader
        needs no hint; the submitter redirects to it at once.
        """
        leader_addr = self._resolve_dc_leader_addr()
        leader_hint = (
            f"{leader_addr[0]}:{leader_addr[1]}" if leader_addr else "unknown"
        )
        return JobAck(
            job_id=job_id,
            accepted=False,
            error=f"Not DC leader, retry at leader: {leader_hint}",
            leader_addr=leader_addr,
            retry_after_seconds=0.0 if leader_addr else self._leader_election.seconds_until_next_decision(),
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
        ).dump()

    def _cluster_admission_refusal(self, job_id: str) -> bytes | None:
        """
        Refuse a job while the cluster's membership is unformed or read-only; otherwise return None.

        AD-52: a job's Raft group is founded with the cluster's committed
        members -- there are none until the cluster forms. AD-52 section 13:
        an operator may put the cluster in read-only mode.
        """
        if not self._cluster_membership.formed:
            return JobAck(
                job_id=job_id,
                accepted=False,
                error="Cluster membership not formed yet; retry",
                leader_addr=None,
                retry_after_seconds=self._cluster_membership.seconds_until_next_formation_round(),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()

        if self._cluster_membership.read_only:
            return JobAck(
                job_id=job_id,
                accepted=False,
                error="Cluster is read-only: job submissions are refused",
                leader_addr=None,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()
        return None

    def _capacity_admission_refusal(self, job_id: str) -> bytes | None:
        """
        Refuse a job when no worker is registered in this datacenter; otherwise return None.

        Capacity fencing: an ACTIVE leader with ZERO registered workers must
        reject, not accept-then-strand. Accepting without capacity guarantees
        the dispatch fails ~5s later (or strands to the AD-34 timeout) -- the
        client's retry loop is BUILT for rejection-until-capacity, so refusing
        here is the honest, retryable signal. Workers registered but busy is
        NOT a rejection: queueing behind busy capacity is legitimate.
        """
        if self._manager_state.get_worker_count() < 1:
            # No retry hint: capacity returns when a worker's registration
            # arrives -- after a booting worker's randomized first-attempt
            # delay, its registration retries, or a rejoin -- at an instant
            # nothing here times. The submitter's growing back-off polls
            # for it (measured: a fixed rejoin-interval hint made boot-time
            # clients wait past registrations that had already landed).
            return JobAck(
                job_id=job_id,
                accepted=False,
                error=(
                    "No workers registered in this datacenter; "
                    "rejecting job submission"
                ),
                leader_addr=None,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()
        return None

    def _idempotency_ledger_tracks(self, idempotency_key: IdempotencyKey | None) -> bool:
        """Whether there is an idempotency key and this manager keeps an idempotency ledger to record it in."""
        return idempotency_key is not None and self._idempotency_ledger is not None

    async def _reserve_submission_idempotency_key(
        self,
        idempotency_key: IdempotencyKey | None,
        job_id: str,
    ) -> tuple[bool, bytes | None]:
        """
        Reserve a submission's idempotency key in the ledger for the job id being decided.

        Returns (False, None) when there is no key or no ledger to reserve it
        in, (False, answer) when another attempt already holds the key, and
        (True, None) once the key is reserved for this attempt.
        """
        if not self._idempotency_ledger_tracks(idempotency_key):
            return False, None
        found, entry = await self._idempotency_ledger.check_or_reserve(
            idempotency_key,
            job_id,
        )
        return self._idempotency_reservation_outcome(found, entry, job_id)

    def _idempotency_reservation_outcome(
        self,
        found: bool,
        entry: IdempotencyLedgerEntry | None,
        job_id: str,
    ) -> tuple[bool, bytes | None]:
        """Turn the ledger's check-or-reserve result into (reserved, earlier answer to send back or None)."""
        if found and entry is not None:
            return False, self._duplicate_idempotency_answer(entry, job_id)
        return True, None

    def _prepare_submission_workflows(
        self,
        submission: JobSubmission,
    ) -> list[tuple[str, list[str], Workflow]]:
        """
        Unpickle and validate a submission's workflows, and settle its timeout.

        Before any job state exists, a job whose workflows cannot all run in
        dependency order is refused with the reason (the validation raises).
        A gate re-running a lost datacenter's unfinished share here (AD-36)
        names it: those workflows and their ancestors run. A job submitted
        without a timeout of its own (a gate fills it in for the jobs it
        dispatches) has as long as its longest chain of dependent workflows
        may take; the submission is updated with it.
        """
        workflows: list[tuple[str, list[str], Workflow]] = restricted_loads(
            submission.workflows
        )
        validate_workflow_dependencies(workflows)
        if submission.rerun_workflow_ids:
            workflows = select_rerun_workflows(
                workflows, submission.rerun_workflow_ids
            )
        if submission.timeout_seconds <= 0.0:
            submission.timeout_seconds = resolve_job_deadline_seconds(
                workflows,
                self.env.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            )
        return workflows

    @staticmethod
    def _submission_callback_address(submission: JobSubmission):
        """The submission's callback address as a tuple (a list off the wire becomes one), or None when it has none."""
        if not submission.callback_addr:
            return None
        return (
            tuple(submission.callback_addr)
            if isinstance(submission.callback_addr, list)
            else submission.callback_addr
        )

    async def _clear_refused_record_and_admit(
        self,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
    ) -> None:
        """
        Clear a refused submission's record for the job id, then admit the job against the caps (D-65, D-67).

        The admission is recorded only after the clear: clearing cleans up
        the job id's state, the admission's record with it.

        Raises:
            JobAdmissionRefusedError: a concurrency cap has no room for the
                job, or its class is quarantined (``_answer_failed_job_submission``
                answers with the refusal it carries).
        """
        await self._clear_refused_job_record(submission.job_id)
        await self._job_admission_control.admit(submission, workflows)

    async def _clear_refused_job_record(self, job_id: str) -> None:
        """
        Remove a refused submission's record so the submission decided now takes its place.

        A refused submission's record -- announced, never admitted, ended --
        holds the job id until the retention sweep.
        """
        if self._is_refused_job_record(self._job_manager.get_job_by_id(job_id)):
            await self._cleanup_job_state(job_id)

    @classmethod
    def _is_refused_job_record(cls, record: JobInfo | None):
        """Whether a job record exists and belongs to a refused submission: unadmitted and terminal."""
        return record is not None and cls._is_unadmitted_terminal_job(record)

    @staticmethod
    def _is_unadmitted_terminal_job(record: JobInfo):
        """Whether a job holds no submission and no workflows and has reached a terminal status."""
        return (
            record.submission is None
            and not record.workflows
            and JobStatusOrder().is_terminal(record.status)
        )

    async def _release_contested_idempotency_key(
        self,
        idempotency_reserved: bool,
        idempotency_key: IdempotencyKey | None,
    ) -> bool:
        """
        Release an idempotency key reserved for a submission whose job a peer is deciding.

        Returns whether the key is still reserved: False once released, or
        the reservation unchanged when there was nothing to release.
        """
        if not (idempotency_reserved and self._idempotency_ledger_tracks(idempotency_key)):
            return idempotency_reserved
        await self._idempotency_ledger.release(idempotency_key)
        return False

    @staticmethod
    def _contested_submission_ack(job_id: str, leader_node_id: str | None) -> bytes:
        """The transient refusal naming the manager deciding the same job id, so the submitter retries."""
        return JobAck(
            job_id=job_id,
            accepted=False,
            error=(
                f"submission in progress at manager "
                f"{leader_node_id}, retry"
            ),
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
        ).dump()

    async def _admit_submitted_job(
        self,
        job_info: JobInfo,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
        callback_addr: tuple[str, int] | None,
        addr: tuple[str, int],
    ) -> None:
        """
        Admit a created job: lead it, record it, announce it, and register its workflows.

        Broadcasts job leadership to peers, including the client callback
        and origin gate addresses, so any peer that later takes over
        leadership can push terminal-state notifications back to the
        originating client. The ledger records the job after that broadcast.
        """
        await self._start_submitted_job_tracking(job_info, submission)
        self._record_submitted_job_contacts(submission)

        await self._manager_state.increment_state_version()

        workflow_names = [wf.name for _, _, wf in workflows]
        await self._broadcast_job_leadership(
            submission.job_id,
            len(workflows),
            workflow_names,
            callback_addr=submission.callback_addr,
            origin_gate_addr=submission.origin_gate_addr,
        )

        if self._job_ledger is not None:
            await self._record_submitted_job_in_ledger(submission, callback_addr, addr)

        await self._register_job_workflows(submission, workflows)

    async def _start_submitted_job_tracking(
        self,
        job_info: JobInfo,
        submission: JobSubmission,
    ) -> None:
        """
        Make this manager the job's leader and start its timeout, lease and consensus group.

        Stores the submission for dispatch, assigns its resource budget,
        starts AD-34 timeout tracking, claims the job's leadership lease and
        creates the job's Raft group with the managers live now -- every peer
        joins it with these voters (AD-52).
        """
        job_info.leader_node_id = self._node_id.full
        job_info.leader_addr = (self._host, self._tcp_port)
        job_info.fencing_token = 1

        self._manager_state.set_job_submission(submission.job_id, submission)
        self._assign_resource_budget(submission)

        timeout_strategy = self._select_timeout_strategy(submission)
        await timeout_strategy.start_tracking(
            job_id=submission.job_id,
            timeout_seconds=submission.timeout_seconds,
            gate_addr=tuple(submission.origin_gate_addr)
            if submission.origin_gate_addr
            else None,
        )
        self._manager_state.set_job_timeout_strategy(
            submission.job_id, timeout_strategy
        )

        await self._leases.claim_job_leadership(
            job_id=submission.job_id,
            tcp_addr=(self._host, self._tcp_port),
        )
        await self._raft.consensus.create_job_raft(
            submission.job_id, self._raft.consensus.current_members()
        )

    def _record_submitted_job_contacts(self, submission: JobSubmission) -> None:
        """Store the submission's client callback (for job and progress pushes) and its origin gate, when given."""
        if submission.callback_addr:
            self._manager_state.set_job_callback(
                submission.job_id, submission.callback_addr
            )
            self._manager_state.set_progress_callback(
                submission.job_id, submission.callback_addr
            )

        if submission.origin_gate_addr:
            self._manager_state.set_job_origin_gate(
                submission.job_id, submission.origin_gate_addr
            )

    async def _record_submitted_job_in_ledger(
        self,
        submission: JobSubmission,
        callback_addr: tuple[str, int] | None,
        addr: tuple[str, int],
    ) -> None:
        """
        Write the job's durable acceptance record and persist its submission payload.

        AD-38 REGIONAL: fsynced here, then committed in the job's Raft group.
        Written after the leadership broadcast: peers create the job's group
        on the announcement, and a REGIONAL commit needs a majority of
        members to hold the group. A restart after this point recovers the
        job instead of forgetting it. The requestor contact is the client's
        callback listener when it registered one (where a restarted manager
        can reach it), else the submitting socket. A shortfall means the
        record is durable here and applied to ledger state, just not
        replicated: a durability warning, not a reason to abort acceptance of
        a job that exists. The payload is what a restarted manager needs to
        resume the job rather than fail it; a crash between the two records
        degrades to the fail-loudly path, never silence.
        """
        requestor_contact = (
            f"{callback_addr[0]}:{callback_addr[1]}"
            if callback_addr
            else f"{addr[0]}:{addr[1]}"
        )
        _ledger_job_id, create_result = (
            await self._job_ledger.create_job(
                spec_hash=hashlib.sha256(
                    submission.workflows
                ).digest(),
                assigned_datacenters=(self._node_id.datacenter,),
                requestor_id=requestor_contact,
                durability=DurabilityLevel.REGIONAL,
                job_id=submission.job_id,
            )
        )
        await self._log_ledger_shortfall(
            "JobCreated", submission.job_id, create_result
        )
        await self._log_ledger_shortfall(
            "JobAccepted",
            submission.job_id,
            await self._job_ledger.accept_job(
                submission.job_id,
                datacenter_id=self._node_id.datacenter,
                worker_count=len(self._worker_pool.iter_workers()),
                durability=DurabilityLevel.REGIONAL,
            ),
        )
        await self._persist_submission_payload(submission)

    async def _dispatch_admitted_job(self, submission: JobSubmission) -> None:
        """
        Start dispatching an admitted job's workflows, logging a failure instead of refusing the job.

        The job is admitted -- its workflows may be on workers -- and its
        timeout (AD-34) bounds it whatever its dispatch did. Refused now, it
        would be submitted elsewhere and run twice.
        """
        try:
            await self._dispatch_job_workflows(submission)
        except Exception as dispatch_error:
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"Starting the dispatch of admitted job "
                        f"{submission.job_id} raised "
                        f"{type(dispatch_error).__name__}: {dispatch_error}\n"
                        + "".join(traceback.format_exception(dispatch_error))
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _accept_job_submission(
        self,
        submission: JobSubmission,
        negotiated_caps_str: str | None,
        idempotency_reserved: bool,
        idempotency_key: IdempotencyKey | None,
    ) -> bytes:
        """Build the acceptance answer for an admitted job and commit it under the idempotency key reserved for it."""
        ack_response = self._accepted_submission_ack(submission.job_id, negotiated_caps_str)
        if idempotency_reserved and self._idempotency_ledger_tracks(idempotency_key):
            await self._commit_idempotency_answer(idempotency_key, ack_response, submission.job_id)
        return ack_response

    async def _commit_idempotency_answer(
        self,
        idempotency_key: IdempotencyKey,
        ack_response: bytes,
        job_id: str,
    ) -> None:
        """
        Record an accepted job's answer under its idempotency key, logging a disk failure.

        The job runs, so the answer is that it was accepted. When the commit
        fails, the key stays reserved: a retry carrying it is told the
        submission is in progress until the reservation lapses, and then
        finds the job.
        """
        try:
            await self._idempotency_ledger.commit(idempotency_key, ack_response)
        except OSError as commit_error:
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"Job {job_id} was accepted, but its "
                        f"idempotency key {idempotency_key} could not be "
                        f"recorded: {commit_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _answer_failed_job_submission(
        self,
        error: Exception,
        submission: JobSubmission | None,
        unadmitted_job: JobInfo | None,
        idempotency_reserved: bool,
        idempotency_key: IdempotencyKey | None,
    ) -> bytes:
        """
        Log a submission that raised, take down the job it created, and refuse it.

        The refusal carries the error, and is recorded as the idempotency
        key's rejection when this submission reserved the key. A refusal by
        admission control (D-65, D-67) is answered as decided instead.
        """
        if isinstance(error, JobAdmissionRefusedError):
            return await self._answer_admission_refusal(error, idempotency_reserved, idempotency_key)
        await self._udp_logger.log(
            ServerError(
                message=(
                    f"Job submission error: {type(error).__name__}: {error}\n"
                    + "".join(traceback.format_exception(error))
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        if unadmitted_job is not None:
            await self._take_down_unadmitted_job(unadmitted_job, error)
        error_ack = self._failed_submission_ack(error, submission)
        await self._record_failed_submission_rejection(idempotency_reserved, idempotency_key, error_ack)
        return error_ack

    async def _answer_admission_refusal(
        self,
        refusal: JobAdmissionRefusedError,
        idempotency_reserved: bool,
        idempotency_key: IdempotencyKey | None,
    ) -> bytes:
        """
        Answer a submission admission control refused (D-65, D-67) with its retry-hinted refusal.

        The refusal is decided before the job is created, so nothing of the
        job exists to take down. It is not the idempotency key's final
        answer -- the submitter is told to come back -- so a key this
        submission reserved is released, not recorded as rejected: the
        retry is decided afresh.
        """
        await self._release_contested_idempotency_key(idempotency_reserved, idempotency_key)
        return refusal.ack

    async def _take_down_unadmitted_job(self, unadmitted_job: JobInfo, error: Exception) -> None:
        """
        Remove a job whose submission failed before dispatch from everywhere the submission put it.

        Nothing of the job reached a worker, so a retry decides the job
        afresh. A payload persisted to resume it goes BEFORE its ledger record
        closes, so a restart in between fails it rather than resuming a job
        its submitter was refused. The job's own state goes LAST, and
        whatever the durable steps did: the record closes through the job's
        consensus group, which the teardown destroys (a ledger write after it
        re-created the group, to leak it), and peers that heard the
        announcement hear it is terminal. A teardown failure is logged: raised
        inside the submission's error handler, it would replace the refusal
        with the transport's raw error bytes.
        """
        unadmitted_job.status = JobStatus.FAILED.value
        try:
            try:
                await self._discard_persisted_submission(unadmitted_job.job_id)
                if self._job_ledger is not None:
                    await self._fail_unadmitted_job_in_ledger(unadmitted_job, error)
            finally:
                await self._cleanup_job_state(unadmitted_job.job_id)
        except Exception as teardown_error:
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"Taking down refused job {unadmitted_job.job_id} "
                        f"failed: {type(teardown_error).__name__}: "
                        f"{teardown_error}\n"
                        + "".join(traceback.format_exception(teardown_error))
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _fail_unadmitted_job_in_ledger(self, unadmitted_job: JobInfo, error: Exception) -> None:
        """Close the ledger record of a job whose submission failed before dispatch as failed, with the reason."""
        await self._log_ledger_shortfall(
            "JobFailed",
            unadmitted_job.job_id,
            await self._job_ledger.fail_job(
                unadmitted_job.job_id,
                error_message=(
                    "its submission failed before dispatch: "
                    f"{type(error).__name__}: {error}"
                ),
                failed_datacenter=self._node_id.datacenter,
                total_completed=0,
                total_failed=0,
                duration_ms=0,
                durability=DurabilityLevel.REGIONAL,
            ),
        )

    @staticmethod
    def _failed_submission_ack(error: Exception, submission: JobSubmission | None) -> bytes:
        """The refusal for a submission that raised, carrying the error; its job id is "unknown" before it parsed."""
        job_id = submission.job_id if submission is not None else "unknown"
        return JobAck(
            job_id=job_id,
            accepted=False,
            error=str(error),
        ).dump()

    async def _record_failed_submission_rejection(
        self,
        idempotency_reserved: bool,
        idempotency_key: IdempotencyKey | None,
        error_ack: bytes,
    ) -> None:
        """Record a failed submission's refusal under the idempotency key it reserved, when it reserved one."""
        if idempotency_reserved and self._idempotency_ledger_tracks(idempotency_key):
            await self._record_idempotency_rejection(idempotency_key, error_ack)

    async def _record_idempotency_rejection(
        self,
        idempotency_key: IdempotencyKey,
        error_ack: bytes,
    ) -> None:
        """
        Record a refusal under its idempotency key, logging a ledger failure instead of raising it.

        The ledger write itself failing (disk full is the canonical case)
        must not raise inside the submission's error handler -- that degrades
        the structured JobAck to the transport's raw error bytes. The client
        still gets the real rejection.
        """
        try:
            await self._idempotency_ledger.reject(
                idempotency_key, error_ack
            )
        except Exception as reject_error:
            await self._udp_logger.log(
                ServerError(
                    message=(
                        "Failed to record idempotency rejection "
                        f"for {idempotency_key}: {reject_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    @tcp.receive()
    async def job_status(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Client status query -- the gateless (L2) poll path -- at the
        consistency the reader asks (AD-38 Part 8; a bare job id is an
        EVENTUAL read, as older clients send).

        Answers from live in-memory state first, then the durable ledger
        (jobs recovered from a restart, terminal/archived jobs). EVENTUAL
        reads, and any read of a terminal status (which never changes),
        are answered from what this manager holds. Otherwise the job's
        leader answers -- for STRONG, once a quorum of its peers accepted
        its state again -- and a follower answers SESSION and
        BOUNDED_STALENESS reads its leader's last sync satisfies, passing
        the rest to the leader. Empty bytes = no answer (unknown job, or
        none at the level asked): the client asks elsewhere.
        """
        try:
            query = self._parse_job_status_query(data)
            consistency = ReadConsistency(query.consistency)
            job_id = query.job_id
            local_status = await self._local_job_status(job_id)
            if (
                answer := await self._answer_job_status_locally(query, consistency, local_status)
            ) is not None:
                return answer
            return await self._forward_job_status_query(query)
        except Exception as query_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"job_status query failed: {query_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b""

    @staticmethod
    def _parse_job_status_query(data: bytes) -> JobStatusQuery:
        """
        Read a job status query off the wire.

        A pickled query begins with the pickle protocol marker; a bare job
        id is text, and reads as an EVENTUAL query for that job.
        """
        return (
            JobStatusQuery.load(data)
            if data[:1] == b"\x80"
            else JobStatusQuery(job_id=data.decode())
        )

    async def _answer_job_status_locally(
        self,
        query: JobStatusQuery,
        consistency: ReadConsistency,
        local_status: GlobalJobStatus | None,
    ) -> bytes | None:
        """
        Answer a status query from what this manager holds, or return None to pass it to the job's leader.

        A held EVENTUAL or terminal status answers as is; the job's leader
        answers at any level; a follower answers from its leader's last
        sync when that view satisfies the read.
        """
        if self._held_status_answers_read(local_status, consistency):
            return local_status.dump()
        if self._leases.is_job_leader(query.job_id):
            return await self._answer_job_status_as_leader(query.job_id, consistency, local_status)
        return self._answer_job_status_from_leader_view(query, consistency, local_status)

    @staticmethod
    def _held_status_answers_read(
        local_status: GlobalJobStatus | None,
        consistency: ReadConsistency,
    ):
        """Whether a held status answers the read as is: an EVENTUAL read, or a terminal status that never changes."""
        return local_status is not None and (
            consistency is ReadConsistency.EVENTUAL
            or JobStatusOrder().is_terminal(local_status.status)
        )

    async def _answer_job_status_as_leader(
        self,
        job_id: str,
        consistency: ReadConsistency,
        local_status: GlobalJobStatus | None,
    ) -> bytes:
        """
        Answer a status query as the job's leader, stamped with its fence token and view time.

        Empty bytes when this manager holds no status, or when a STRONG read
        could not be confirmed by a quorum.
        """
        if local_status is None:
            return b""
        if await self._strong_read_unconfirmed(job_id, consistency):
            return b""
        local_status.fence_token = self._leases.get_fence_token(job_id)
        local_status.view_time = self._clock.monotonic()
        return local_status.dump()

    async def _strong_read_unconfirmed(self, job_id: str, consistency: ReadConsistency) -> bool:
        """
        Whether a STRONG read fails to re-replicate the job's state to a quorum.

        STRONG: this manager is still the job's leader by a quorum's word --
        its peers accept a sync only from the current fenced leader -- and
        the state answered is now held by that quorum. Other levels need no
        confirmation.
        """
        return consistency is ReadConsistency.STRONG and not await self._sync_job_state_to_peers(
            job_id, self._job_manager.get_job_by_id(job_id), require_quorum=True
        )

    def _answer_job_status_from_leader_view(
        self,
        query: JobStatusQuery,
        consistency: ReadConsistency,
        local_status: GlobalJobStatus | None,
    ) -> bytes | None:
        """Answer a follower's status query from its leader's last sync, or return None when it holds neither."""
        if local_status is None:
            return None
        if (leader_view := self._manager_state.get_job_leader_view(query.job_id)) is None:
            return None
        return self._answer_from_leader_view(query, consistency, local_status, leader_view)

    def _answer_from_leader_view(
        self,
        query: JobStatusQuery,
        consistency: ReadConsistency,
        local_status: GlobalJobStatus,
        leader_view: tuple[int, float, float],
    ) -> bytes | None:
        """
        Answer a SESSION or BOUNDED_STALENESS read the leader's last sync satisfies, else return None.

        The view's age is at most the time since it arrived plus the longest
        a sync takes to arrive (its send timeout).
        """
        view_fence_token, view_time, received_at = leader_view
        view_age_bound = (
            self._clock.monotonic() - received_at + self._config.tcp_timeout_short_seconds
        )
        if not self._leader_view_satisfies_read(
            query, consistency, (view_fence_token, view_time), view_age_bound
        ):
            return None
        local_status.fence_token = view_fence_token
        local_status.view_time = view_time
        return local_status.dump()

    @classmethod
    def _leader_view_satisfies_read(
        cls,
        query: JobStatusQuery,
        consistency: ReadConsistency,
        view_position: tuple[int, float],
        view_age_bound: float,
    ) -> bool:
        """Whether a leader's view satisfies a SESSION read's observed position or a BOUNDED_STALENESS read's bound."""
        return cls._session_read_satisfied(
            query, consistency, view_position
        ) or cls._bounded_staleness_read_satisfied(query, consistency, view_age_bound)

    @staticmethod
    def _session_read_satisfied(
        query: JobStatusQuery,
        consistency: ReadConsistency,
        view_position: tuple[int, float],
    ) -> bool:
        """Whether a SESSION read's leader view is at or past the fence token and view time the reader observed."""
        return consistency is ReadConsistency.SESSION and view_position >= (
            query.observed_fence_token,
            query.observed_view_time,
        )

    @staticmethod
    def _bounded_staleness_read_satisfied(
        query: JobStatusQuery,
        consistency: ReadConsistency,
        view_age_bound: float,
    ) -> bool:
        """Whether a BOUNDED_STALENESS read's leader view is no older than the reader allows."""
        return (
            consistency is ReadConsistency.BOUNDED_STALENESS
            and view_age_bound <= query.max_staleness_seconds
        )

    async def _forward_job_status_query(self, query: JobStatusQuery) -> bytes:
        """
        Pass a status query to the job's leader once, returning its answer.

        Empty bytes when the query was already forwarded, the leader is
        unknown or is this manager, or the leader's answer is not bytes.
        """
        leader_addr = self._manager_state.get_job_leader_addr(query.job_id)
        if self._job_status_query_unforwardable(query, leader_addr):
            return b""
        query.forwarded = True
        response, _clock = await self.send_tcp(
            tuple(leader_addr),
            "job_status",
            query.dump(),
            timeout=self._config.tcp_timeout_standard_seconds,
        )
        return response if isinstance(response, bytes) else b""

    def _job_status_query_unforwardable(
        self,
        query: JobStatusQuery,
        leader_addr: tuple[str, int] | None,
    ):
        """Whether a status query was already forwarded, or its job's leader is unknown or is this manager."""
        return query.forwarded or leader_addr is None or tuple(leader_addr) == (self._host, self._tcp_port)

    async def _local_job_status(self, job_id: str) -> GlobalJobStatus | None:
        """``job_id``'s status as this manager holds it: its live state, or
        its durable ledger record (a job recovered from a restart, or
        terminal and archived); None when it holds neither. Ledger-internal
        statuses map onto the client vocabulary here."""
        job = self._job_manager.get_job_by_id(job_id)
        if job is not None:
            return self._live_job_status(job_id, job)
        return await self._ledger_job_status(job_id)

    def _live_job_status(self, job_id: str, job: JobInfo) -> GlobalJobStatus:
        """The live job's status and progress."""
        total_completed, total_failed, overall_rate = self._aggregate_job_progress(job)
        return GlobalJobStatus(
            job_id=job_id,
            status=job.status,
            total_completed=total_completed,
            total_failed=total_failed,
            overall_rate=overall_rate,
            elapsed_seconds=job.elapsed_seconds(),
        )

    async def _ledger_job_status(self, job_id: str) -> GlobalJobStatus | None:
        """The job's status from its live or archived AD-38 ledger record."""
        if self._job_ledger is None:
            return None
        job_state = await self._ledger_job_state(job_id)
        return self._answerable_ledger_job_status(job_id, job_state)

    async def _ledger_job_state(self, job_id: str) -> JobState | None:
        """The job's ledger record, else its archived one."""
        if (job_state := self._job_ledger.get_job(job_id)) is None:
            job_state = await self._job_ledger.get_archived_job(job_id)
        return job_state

    def _answerable_ledger_job_status(
        self,
        job_id: str,
        job_state: JobState | None,
    ) -> GlobalJobStatus | None:
        """The record's status in the client vocabulary, unless it was relinquished."""
        # A record this manager relinquished is not its to answer from: the
        # job's leader is another manager.
        if job_state is None or job_state.status == JOB_RELINQUISHED_STATUS:
            return None
        return GlobalJobStatus(
            job_id=job_id,
            status=_LEDGER_STATUS_VOCABULARY.get(job_state.status, job_state.status),
            total_completed=job_state.completed_count,
            total_failed=job_state.failed_count,
        )

    @tcp.receive()
    async def job_global_timeout(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle global timeout decision from gate (AD-34)."""
        try:
            timeout_msg = JobGlobalTimeout.load(data)

            # b"ok" acknowledges delivery: the decision was processed, even
            # when there is nothing to do (job no longer tracked here) or
            # its fence token lost; b"" means it could not be processed.
            strategy = self._manager_state.get_job_timeout_strategy(timeout_msg.job_id)
            if not strategy:
                return b"ok"

            await self._apply_global_timeout_decision(strategy, timeout_msg)

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job global timeout error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b""

    async def _apply_global_timeout_decision(
        self,
        strategy: TimeoutStrategy,
        timeout_msg: JobGlobalTimeout,
    ) -> None:
        """Hand the gate's timeout to the job's strategy; drop the strategy once it accepts (AD-34)."""
        accepted = await strategy.handle_global_timeout(
            timeout_msg.job_id,
            timeout_msg.reason,
            timeout_msg.fence_token,
        )

        if accepted:
            self._manager_state.remove_job_timeout_strategy(timeout_msg.job_id)
            await self._udp_logger.log(
                ServerInfo(
                    message=f"Job {timeout_msg.job_id} globally timed out: {timeout_msg.reason}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    @tcp.receive()
    async def workflow_cancellation_query(self, addr: tuple[str, int], data: bytes, clock_time: int) -> bytes:
        return await self._cancellation.handle_workflow_cancellation_query(addr, data, clock_time)

    @tcp.receive()
    async def receive_cancel_single_workflow(self, addr: tuple[str, int], data: bytes, clock_time: int) -> bytes:
        return await self._cancellation.handle_cancel_single_workflow(addr, data, clock_time)

    @tcp.receive()
    async def job_leadership_announcement(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job leadership announcement from another manager."""
        try:
            return await self._apply_job_leadership_announcement(data)

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job leadership announcement error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    async def _apply_job_leadership_announcement(self, data: bytes) -> bytes:
        """Accept the announced job leader, replicate its push destinations,
        join the job's Raft group and track the remote job."""
        announcement = JobLeadershipAnnouncement.load(data)
        leader_addr = (announcement.leader_host, announcement.leader_tcp_port)
        fencing_token = announcement.fence_token or 1

        accepted = self._leases.apply_job_leadership(
            job_id=announcement.job_id,
            leader_id=announcement.leader_id,
            leader_addr=leader_addr,
            fencing_token=fencing_token,
        )
        if not accepted:
            return JobLeadershipAck(
                job_id=announcement.job_id,
                accepted=False,
                responder_id=self._node_id.full,
            ).dump()

        self._replicate_announced_push_destinations(announcement)

        await self._join_announced_job_raft(announcement)

        # Track remote job
        await self._job_manager.track_remote_job(
            job_id=announcement.job_id,
            leader_node_id=announcement.leader_id,
            leader_addr=leader_addr,
        )

        return JobLeadershipAck(
            job_id=announcement.job_id,
            accepted=True,
            responder_id=self._node_id.full,
        ).dump()

    def _replicate_announced_push_destinations(self, announcement: JobLeadershipAnnouncement) -> None:
        """Store the announced client callback and origin gate for this job."""
        # Replicate push-notification destinations from the
        # announcement so any subsequent leadership takeover on
        # this node can push terminal-state notifications back
        # to the originating client / origin gate. Coercing the
        # tuple normalises the wire-decoded list-of-pairs back
        # to the (host, port) shape stored in state.
        if announcement.callback_addr is not None:
            self._manager_state.set_job_callback(
                announcement.job_id, tuple(announcement.callback_addr)
            )
            self._manager_state.set_progress_callback(
                announcement.job_id, tuple(announcement.callback_addr)
            )
        if announcement.origin_gate_addr is not None:
            self._manager_state.set_job_origin_gate(
                announcement.job_id, tuple(announcement.origin_gate_addr)
            )

    async def _join_announced_job_raft(self, announcement: JobLeadershipAnnouncement) -> None:
        """Join the job's Raft group with the announced voters, or log their absence."""
        if announcement.raft_voters:
            await self._raft.consensus.create_job_raft(
                announcement.job_id, frozenset(announcement.raft_voters)
            )
        else:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Leadership announcement of job {announcement.job_id} names "
                        "no Raft voters: its group is joined when the job's sync does"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _apply_job_state_sync_message(
        self,
        sync_msg: JobStateSyncMessage,
        source_addr: tuple[str, int],
        *,
        sender_leads_job: bool,
    ) -> JobInfo:
        """Apply peer-replicated executable job state to the local manager.

        ``sender_leads_job``: the message is the job leader's own view (a
        sync or a takeover claim from it), not a peer's copy of the job.
        """
        leader_id, leader_addr, fencing_token = self._job_sync_leadership(sync_msg, source_addr)
        callback_addr = self._job_sync_callback_addr(sync_msg)

        accepted = self._leases.apply_job_leadership(
            job_id=sync_msg.job_id,
            leader_id=leader_id,
            leader_addr=leader_addr,
            fencing_token=fencing_token,
        )
        if not accepted:
            raise RuntimeError(
                f"Rejected conflicting job state sync for {sync_msg.job_id} "
                f"from leader {sync_msg.leader_id} at fence {sync_msg.fencing_token}"
            )

        # This manager's own job only moves forward on a peer's view of
        # it; a follower mirrors its fenced leader (AD-54) -- the leader
        # alone. A peer's copy is evidence of how far the job got, merged
        # forward: mirrored, a peer that missed the leader's last syncs
        # regressed the job here, and a workflow the leader saw finish ran
        # again once this manager took the job over.
        job = await self._job_manager.hydrate_remote_job_state(
            merge_forward_only=self._job_sync_merges_forward_only(sync_msg.job_id, sender_leads_job),
            job_id=sync_msg.job_id,
            leader_node_id=leader_id,
            leader_addr=leader_addr,
            status=sync_msg.status,
            workflows_total=sync_msg.workflows_total,
            workflows_completed=sync_msg.workflows_completed,
            workflows_failed=sync_msg.workflows_failed,
            fencing_token=fencing_token,
            callback_addr=callback_addr,
            workflow_snapshots=sync_msg.workflow_snapshots,
            sub_workflow_snapshots=sync_msg.sub_workflow_snapshots,
            layer_version=sync_msg.layer_version,
            elapsed_seconds=sync_msg.elapsed_seconds,
            timestamp=self._clock.time(),
            replace_existing=sync_msg.replace_existing,
        )

        await self._reconcile_job_raft_group_after_sync(sync_msg, job)
        await self._apply_job_sync_context(sync_msg, job)

        self._leases.update_fence_token_if_higher(
            sync_msg.job_id, fencing_token
        )
        if sender_leads_job:
            self._manager_state.record_job_leader_view(
                sync_msg.job_id, sync_msg.fencing_token, sync_msg.timestamp, self._clock.monotonic()
            )

        self._record_job_sync_contacts(sync_msg, callback_addr)

        return job

    def _job_sync_leadership(
        self,
        sync_msg: JobStateSyncMessage,
        source_addr: tuple[str, int],
    ) -> tuple[str, tuple[str, int], int]:
        """
        The leader id, leader address and fencing token a job state sync is applied under.

        The sync's own leadership (its leader address defaulting to the
        sender's) -- unless this manager already holds a newer fenced
        leadership for the job, which is kept.
        """
        leader_addr = (
            tuple(sync_msg.leader_addr)
            if sync_msg.leader_addr is not None
            else source_addr
        )
        current_fencing_token = self._leases.get_fence_token(sync_msg.job_id)
        current_leader_id = self._leases.get_job_leader(sync_msg.job_id)
        current_leader_addr = self._manager_state.get_job_leader_addr(sync_msg.job_id)
        if self._holds_newer_job_leadership(
            current_leader_id,
            current_leader_addr,
            current_fencing_token,
            sync_msg.fencing_token,
        ):
            return current_leader_id, tuple(current_leader_addr), current_fencing_token
        return sync_msg.leader_id, leader_addr, sync_msg.fencing_token

    @staticmethod
    def _holds_newer_job_leadership(
        current_leader_id: str | None,
        current_leader_addr: tuple[str, int] | None,
        current_fencing_token: int,
        synced_fencing_token: int,
    ) -> bool:
        """Whether a known leader at a known address holds the job at a fence above the synced one."""
        return (
            current_leader_id is not None
            and current_leader_addr is not None
            and current_fencing_token > synced_fencing_token
        )

    @staticmethod
    def _job_sync_callback_addr(sync_msg: JobStateSyncMessage) -> tuple[str, int] | None:
        """The client callback address a job state sync carries, as a tuple, or None."""
        return (
            tuple(sync_msg.callback_addr)
            if sync_msg.callback_addr is not None
            else None
        )

    def _job_sync_merges_forward_only(self, job_id: str, sender_leads_job: bool) -> bool:
        """Whether a sync only moves the job forward: this manager leads the job, or the sender does not."""
        return self._leases.is_job_leader(job_id) or not sender_leads_job

    async def _reconcile_job_raft_group_after_sync(
        self,
        sync_msg: JobStateSyncMessage,
        job: JobInfo,
    ) -> None:
        """
        Destroy a terminal synced job's Raft group, or join a live one's.

        Members that learn a live job by state sync rather than the
        leadership announcement (e.g. after a restart) join its Raft group
        here; RPCs no longer create groups on arrival. A terminal job takes
        no further ledger entries, so its group goes now rather than
        heartbeating until the retention sweep.
        """
        if JobStatusOrder().is_terminal(job.status):
            await self._raft.consensus.destroy_job_raft(sync_msg.job_id)
            return
        await self._join_synced_job_raft_group(sync_msg)

    async def _join_synced_job_raft_group(self, sync_msg: JobStateSyncMessage) -> None:
        """
        Join a live synced job's Raft group with the voters the sync names.

        With the voters every member of the group has (AD-52). A sync naming
        none, for a job whose group this manager does not hold, is logged:
        its sender holds no group for it, so none is joined here.
        """
        if sync_msg.raft_voters:
            await self._raft.consensus.create_job_raft(
                sync_msg.job_id, frozenset(sync_msg.raft_voters)
            )
        elif self._raft.consensus.get_node(sync_msg.job_id) is None:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Sync of live job {sync_msg.job_id} names no Raft voters: "
                        "its sender holds no group for it, so none is joined here"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _apply_job_sync_context(
        self,
        sync_msg: JobStateSyncMessage,
        job: JobInfo,
    ) -> None:
        """Load a sync's context snapshot into the job under its lock when the sync's layer is not older."""
        if not self._sync_carries_current_context(sync_msg, job):
            return
        async with job.lock:
            for workflow_name, values in sync_msg.context_snapshot.items():
                await job.context.from_dict(workflow_name, values)
            job.layer_version = sync_msg.layer_version

    @staticmethod
    def _sync_carries_current_context(sync_msg: JobStateSyncMessage, job: JobInfo):
        """Whether a sync carries a context snapshot at a layer version at or above the job's."""
        return sync_msg.context_snapshot and sync_msg.layer_version >= job.layer_version

    def _record_job_sync_contacts(
        self,
        sync_msg: JobStateSyncMessage,
        callback_addr: tuple[str, int] | None,
    ) -> None:
        """Store the client callback (for job and progress pushes) and the origin gate a sync carries, when given."""
        if callback_addr is not None:
            self._manager_state.set_job_callback(sync_msg.job_id, callback_addr)
            self._manager_state.set_progress_callback(sync_msg.job_id, callback_addr)

        if sync_msg.origin_gate_addr:
            self._manager_state.set_job_origin_gate(
                sync_msg.job_id, tuple(sync_msg.origin_gate_addr)
            )

    @tcp.receive()
    async def job_state_sync(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job state sync from job leader."""
        try:
            sync_msg = JobStateSyncMessage.load(data)

            # Only accept from actual job leader
            current_leader = self._leases.get_job_leader(sync_msg.job_id)
            current_fencing_token = self._leases.get_fence_token(sync_msg.job_id)
            if (
                refusal := await self._job_state_sync_refusal(
                    sync_msg, current_leader, current_fencing_token
                )
            ) is not None:
                return refusal

            await self._apply_job_state_sync_message(sync_msg, addr, sender_leads_job=True)

            return self._job_state_sync_ack(sync_msg.job_id, True)

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job state sync error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    async def _job_state_sync_refusal(
        self,
        sync_msg: JobStateSyncMessage,
        current_leader: str | None,
        current_fencing_token: int,
    ) -> bytes | None:
        """
        Refuse a job state sync from a sender that does not lead the job here; else return None.

        A sync from another leader at a fence no newer than the current one
        is stale. A sync from another leader at a newer fence is a takeover
        claim, refused when the job ended or its consensus group has not
        settled here.
        """
        if self._is_stale_job_state_sync(sync_msg, current_leader, current_fencing_token):
            return self._job_state_sync_ack(sync_msg.job_id, False)
        if self._is_job_takeover_claim(sync_msg, current_leader):
            return await self._job_takeover_claim_refusal(sync_msg, current_leader)
        return None

    @staticmethod
    def _is_job_takeover_claim(sync_msg: JobStateSyncMessage, current_leader: str | None):
        """Whether a sync names a different leader than the one this manager knows for the job."""
        return current_leader and current_leader != sync_msg.leader_id

    @classmethod
    def _is_stale_job_state_sync(
        cls,
        sync_msg: JobStateSyncMessage,
        current_leader: str | None,
        current_fencing_token: int,
    ):
        """Whether a sync names a different leader at a fence no newer than the job's current one."""
        return (
            cls._is_job_takeover_claim(sync_msg, current_leader)
            and sync_msg.fencing_token <= current_fencing_token
        )

    async def _job_takeover_claim_refusal(
        self,
        sync_msg: JobStateSyncMessage,
        current_leader: str,
    ) -> bytes | None:
        """
        Refuse a takeover claim for a job that ended or whose consensus group has not settled; else None.

        A job this member knows ended -- its copy is terminal, or the job's
        replicated ledger records its end -- is not taken over: its leader
        died after finishing it, and the claimant missed the end; a live
        copy here is ended with the replicated status. Nor is one whose
        consensus group has not settled on its dead leader's last entries
        here (no new group leader yet, or an entry held unapplied): an end
        may be among them. A REGIONAL end is on a majority, so the members
        refusing here keep any claimant short of quorum.
        """
        status_order = JobStatusOrder()
        job = self._job_manager.get_job_by_id(sync_msg.job_id)
        replicated_state = self._ledger_replica.job_state(sync_msg.job_id)
        if self._job_group_unsettled(sync_msg.job_id, current_leader):
            return self._job_state_sync_ack(sync_msg.job_id, False)
        if self._job_ended(replicated_state, job, status_order):
            await self._settle_ended_job_copy(sync_msg.job_id, job, replicated_state, status_order)
            return self._job_state_sync_ack(sync_msg.job_id, False)
        return None

    def _job_state_sync_ack(self, job_id: str, accepted: bool) -> bytes:
        """This manager's answer to a job state sync, accepting or refusing it."""
        return JobStateSyncAck(
            job_id=job_id,
            responder_id=self._node_id.full,
            accepted=accepted,
        ).dump()

    @tcp.receive()
    async def job_leader_gate_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job leader gate transfer notification from gate.

        ``transfer.fence_token`` is a *gate-leadership* fence — it
        advances when an orphan-takeover at the gate tier elects a
        new job-leader gate. It must NOT mutate ``_leases``
        (manager-leadership fence). Mixing those two epochs causes
        in-flight manager-originated results to be rejected by the
        gate as "stale" after every takeover; the gate's
        ``workflow_result_push`` handler used to gate on
        ``manager_push.fence < gate_current_fence`` and silently
        dropped legitimate completed work. The two domains are now
        tracked in separate storage and never cross-pollinate.
        """
        try:
            transfer = JobLeaderGateTransfer.load(data)

            current_routing_fence = (
                self._manager_state.get_job_gate_routing_fence(transfer.job_id)
            )
            if transfer.fence_token < current_routing_fence:
                return JobLeaderGateTransferAck(
                    job_id=transfer.job_id,
                    manager_id=self._node_id.full,
                    accepted=False,
                ).dump()

            self._manager_state.set_job_origin_gate(
                transfer.job_id, transfer.new_gate_addr
            )
            self._manager_state.update_job_gate_routing_fence_if_higher(
                transfer.job_id, transfer.fence_token
            )

            await self._udp_logger.log(
                ServerInfo(
                    message=f"Job {transfer.job_id} leader gate transferred: {transfer.old_gate_id} -> {transfer.new_gate_id}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            return JobLeaderGateTransferAck(
                job_id=transfer.job_id,
                manager_id=self._node_id.full,
                accepted=True,
            ).dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job leader gate transfer error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def register_callback(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle client callback registration for job reconnection."""
        try:
            return await self._register_client_callback(addr, data)

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Register callback error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    async def _register_client_callback(self, addr: tuple[str, int], data: bytes) -> bytes:
        """Rate-limit, then register the client's callback for the job."""
        # Rate limit check
        client_id = f"{addr[0]}:{addr[1]}"
        rate_limit_result = await self._rate_limiter.check_rate_limit(
            client_id, "reconnect"
        )
        if not rate_limit_result.allowed:
            return RateLimitResponse(
                operation="reconnect",
                retry_after_seconds=rate_limit_result.retry_after_seconds,
            ).dump()

        request = RegisterCallback.load(data)
        job_id = request.job_id

        job = self._job_manager.get_job(job_id)
        if not job:
            return RegisterCallbackResponse(
                job_id=job_id,
                success=False,
                error="Job not found",
            ).dump()

        # Register callback
        self._manager_state.set_job_callback(job_id, request.callback_addr)
        self._manager_state.set_progress_callback(job_id, request.callback_addr)

        return self._register_callback_response(job_id, job)

    def _register_callback_response(self, job_id: str, job: JobInfo) -> bytes:
        """The job's status, progress totals and elapsed time for a reconnecting client."""
        # Calculate elapsed time (job.timestamp is wall-clock seconds set by Raft apply or local handlers)
        elapsed = self._clock.time() - job.timestamp if job.timestamp > 0 else 0.0

        # Aggregate completed/failed from sub-workflows (WorkflowInfo has no counts;
        # they live on SubWorkflowInfo.progress)
        total_completed, total_failed = self._job_progress_totals(job)

        return RegisterCallbackResponse(
            job_id=job_id,
            success=True,
            status=job.status,
            total_completed=total_completed,
            total_failed=total_failed,
            elapsed_seconds=elapsed,
        ).dump()

    def _job_progress_totals(self, job: JobInfo) -> tuple[int, int]:
        """Completed and failed counts summed over the job's sub-workflow progress."""
        total_completed = 0
        total_failed = 0
        for progress in filter(None, (sub_info.progress for sub_info in self._present_job_sub_workflows(job))):
            total_completed += progress.completed_count
            total_failed += progress.failed_count
        return total_completed, total_failed

    def _present_job_sub_workflows(self, job: JobInfo) -> list[SubWorkflowInfo]:
        """Every workflow's sub-workflows the job still holds, in workflow then token order."""
        return list(
            filter(
                None,
                map(
                    job.sub_workflows.get,
                    chain.from_iterable(
                        workflow_info.sub_workflow_tokens for workflow_info in job.workflows.values()
                    ),
                ),
            )
        )

    @tcp.receive()
    async def workflow_query(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow status query from client."""
        try:
            return await self._answer_workflow_query(addr, data)

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow query error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    async def _answer_workflow_query(self, addr: tuple[str, int], data: bytes) -> bytes:
        """Rate-limit the query and report the status of each named workflow."""
        # Rate limit check
        client_id = f"{addr[0]}:{addr[1]}"
        rate_limit_result = await self._rate_limiter.check_rate_limit(
            client_id, "workflow_query"
        )
        if not rate_limit_result.allowed:
            return RateLimitResponse(
                operation="workflow_query",
                retry_after_seconds=rate_limit_result.retry_after_seconds,
            ).dump()

        request = WorkflowQueryRequest.load(data)
        workflows: list[WorkflowStatusInfo] = []

        job = self._job_manager.get_job(request.job_id)
        if job is not None:
            # Find matching workflows
            workflows = self._matching_workflow_statuses(job, request.workflow_names)

        return WorkflowQueryResponse(
            request_id=request.request_id,
            manager_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            workflows=workflows,
        ).dump()

    def _matching_workflow_statuses(
        self,
        job: JobInfo,
        workflow_names: list[str],
    ) -> list[WorkflowStatusInfo]:
        """The status of each of the job's workflows named in the query."""
        workflows: list[WorkflowStatusInfo] = []
        for wf_info in job.workflows.values():
            if wf_info.name in workflow_names:
                workflows.append(self._workflow_status_info(job, wf_info))
        return workflows

    def _workflow_status_info(self, job: JobInfo, wf_info: WorkflowInfo) -> WorkflowStatusInfo:
        """One workflow's status, aggregated from its sub-workflows."""
        # Aggregate from sub-workflows
        sub_infos = self._present_sub_workflows(job, wf_info)
        completed_count, failed_count, rate_per_second = self._sub_workflow_progress_totals(sub_infos)

        return WorkflowStatusInfo(
            workflow_id=self._workflow_token_id(wf_info),
            workflow_name=wf_info.name,
            status=wf_info.status.value,
            is_enqueued=wf_info.status == WorkflowStatus.PENDING,
            queue_position=0,
            provisioned_cores=sum(sub_info.cores_allocated for sub_info in sub_infos),
            completed_count=completed_count,
            failed_count=failed_count,
            rate_per_second=rate_per_second,
            assigned_workers=self._assigned_sub_workflow_workers(sub_infos),
        )

    def _present_sub_workflows(self, job: JobInfo, wf_info: WorkflowInfo) -> list[SubWorkflowInfo]:
        """The workflow's sub-workflows the job still holds, in token order."""
        return [
            sub_info
            for sub_info in map(job.sub_workflows.get, wf_info.sub_workflow_tokens)
            if sub_info
        ]

    def _assigned_sub_workflow_workers(self, sub_infos: list[SubWorkflowInfo]) -> list[str]:
        """The worker ids the sub-workflows are assigned to."""
        return [sub_info.worker_id for sub_info in sub_infos if sub_info.worker_id]

    def _sub_workflow_progress_totals(
        self,
        sub_infos: list[SubWorkflowInfo],
    ) -> tuple[int, int, float]:
        """Completed, failed and rate totals over the sub-workflows' progress,
        accumulated in sub order."""
        completed_count = 0
        failed_count = 0
        rate_per_second = 0.0
        for progress in filter(None, (sub_info.progress for sub_info in sub_infos)):
            completed_count += progress.completed_count
            failed_count += progress.failed_count
            rate_per_second += progress.rate_per_second
        return completed_count, failed_count, rate_per_second

    # =========================================================================
    # Helper Methods - Job Submission
    # =========================================================================

    async def _broadcast_job_leadership(
        self,
        job_id: str,
        workflow_count: int,
        workflow_names: list[str],
        callback_addr: tuple[str, int] | None = None,
        origin_gate_addr: tuple[str, int] | None = None,
    ) -> None:
        """Broadcast job leadership to peer managers.

        ``callback_addr`` / ``origin_gate_addr`` are replicated so a
        peer that subsequently takes over job leadership after the original
        leader dies has the destinations
        needed to push job-completion / cancellation-completion
        notifications back to the originating client and origin
        gate. Without replication, only the original leader knows
        these addresses and any post-takeover terminal-state push
        silently no-ops in ``_push_cancellation_complete_to_origin``.
        """
        announcement = JobLeadershipAnnouncement(
            job_id=job_id,
            leader_id=self._node_id.full,
            leader_host=self._host,
            leader_tcp_port=self._tcp_port,
            workflow_count=workflow_count,
            workflow_names=workflow_names,
            fence_token=self._leases.get_fence_token(job_id),
            callback_addr=callback_addr,
            origin_gate_addr=origin_gate_addr,
            raft_voters=self._job_raft_initial_voters(job_id),
        )

        # Snapshot before iterating — the loop body awaits send_tcp,
        # so a concurrent peer-death handler removing from the live
        # set would otherwise raise ``Set changed size during
        # iteration`` mid-broadcast.
        for peer_addr in sorted(self._manager_state.get_active_manager_peers()):
            await self._send_leadership_announcement(peer_addr, announcement)

    def _job_raft_initial_voters(self, job_id: str) -> list[str]:
        """The job's Raft group's initial voters, sorted; [] without a group."""
        return (
            sorted(job_group.initial_voters)
            if (job_group := self._raft.consensus.get_node(job_id)) is not None
            else []
        )

    async def _send_leadership_announcement(
        self,
        peer_addr: tuple[str, int],
        announcement: JobLeadershipAnnouncement,
    ) -> None:
        """Send one peer the leadership announcement; log a failed send."""
        try:
            announcement_reply, _ = await self.send_tcp(
                peer_addr,
                "job_leadership_announcement",
                announcement.dump(),
                timeout=self._config.tcp_timeout_short_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(announcement_reply, Exception):
                raise announcement_reply
        except Exception as announcement_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to send leadership announcement to peer {peer_addr}: {announcement_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _register_job_workflows(
        self,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
    ) -> None:
        """Register the job's workflows and replicate the job to a quorum
        of managers: everything its dispatch needs, with nothing sent to a
        worker yet."""
        if self._workflow_dispatcher:
            await self._register_and_replicate_job_workflows(submission, workflows)

    async def _register_and_replicate_job_workflows(
        self,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
    ) -> None:
        """Register the workflows with the dispatcher, then quorum-replicate the job."""
        registered = await self._workflow_dispatcher.register_workflows(
            submission,
            workflows,
        )
        if not registered:
            raise RuntimeError(
                f"Could not register the workflows of job {submission.job_id}"
            )

        job = self._job_manager.get_job_by_id(submission.job_id)
        if job is None:
            raise RuntimeError(
                f"Registered workflows for missing job {submission.job_id}"
            )
        await self._quorum_replicate_job_before_dispatch(submission.job_id, job)

    async def _quorum_replicate_job_before_dispatch(self, job_id: str, job: JobInfo) -> None:
        """Replicate the job to a quorum of managers, raising when it cannot."""
        replicated = await self._sync_job_state_to_peers(
            job_id,
            job,
            require_quorum=True,
        )
        if not replicated:
            raise RuntimeError(
                f"Could not quorum-replicate job {job_id} before dispatch"
            )

    async def _dispatch_job_workflows(self, submission: JobSubmission) -> None:
        """Dispatch the job's registered workflows, respecting dependencies."""
        if self._workflow_dispatcher:
            await self._workflow_dispatcher.start_job_dispatch(
                submission.job_id, submission
            )
            await self._workflow_dispatcher.try_dispatch(
                submission.job_id, submission
            )

        await self._mark_dispatched_job_running(submission)

    async def _mark_dispatched_job_running(self, submission: JobSubmission) -> None:
        """Mark the dispatched job RUNNING and push its start to the client."""
        # NOTE: ``get_job`` expects a token string, not a bare job_id;
        # the previous code passed ``submission.job_id`` and got
        # ``None`` every time, leaving ``job.status`` stuck at QUEUED
        # and silently breaking every "wait for RUNNING" path on the
        # client. ``get_job_by_id`` does the token construction
        # correctly.
        job = self._job_manager.get_job_by_id(submission.job_id)
        if job:
            previous_status = job.status
            job.status = JobStatus.RUNNING.value
            await self._manager_state.increment_state_version()
            # Tier-1 push to the client per the JobStatusPush
            # contract ("Sent from Gate/Manager to Client when
            # significant status changes occur ... Job started ...").
            # Without this, clients have no signal that the job
            # has begun executing — they only see WorkflowResultPush
            # at completion or JobFinalResult at the very end. The
            # gap blocks any "wait until running" pattern (e.g.
            # WorkloadDriver.wait_until_running) on L1/L2 deployments
            # without a gate.
            if previous_status != JobStatus.RUNNING.value:
                self._task_runner.run(
                    self._push_job_status_to_client,
                    submission.job_id,
                    JobStatus.RUNNING.value,
                    "Job started",
                )

    async def _register_with_discovered_worker(
        self,
        worker_snapshot: WorkerStateSnapshot,
    ) -> None:
        """Register a discovered worker from peer manager gossip."""
        worker_id = worker_snapshot.node_id
        if self._manager_state.has_worker(worker_id):
            return

        # Peer managers share this manager's datacenter.
        node_info = NodeInfo(
            node_id=worker_id,
            role=NodeRole.WORKER.value,
            host=worker_snapshot.host,
            port=worker_snapshot.tcp_port,
            datacenter=self._node_id.datacenter,
            udp_port=worker_snapshot.udp_port,
        )

        # A discovery broadcast carries the worker's free cores only: the
        # worker's first heartbeat replaces the total with its own.
        registration = WorkerRegistration(
            node=node_info,
            total_cores=worker_snapshot.total_cores,
            available_cores=worker_snapshot.available_cores,
            memory_mb=0,
        )

        await self._registry.register_worker(registration)
        await self._worker_pool.register_worker(registration)

    def _is_job_leader(self, job_id: str) -> bool:
        """Check if this manager is the leader for a job."""
        leader_id = self._leases.get_job_leader(job_id)
        return leader_id == self._node_id.full

    def _get_healthy_managers(self) -> list[ManagerInfo]:
        """Get list of healthy managers including self."""
        managers = [
            ManagerInfo(
                node_id=self._node_id.full,
                tcp_host=self._host,
                tcp_port=self._tcp_port,
                udp_host=self._host,
                udp_port=self._udp_port,
                datacenter=self._node_id.datacenter,
                is_leader=self.is_leader(),
            )
        ]

        managers.extend(self._manager_state.get_active_known_manager_peers())

        return managers

    # =========================================================================
    # Job Completion
    # =========================================================================

    async def _persist_submission_payload(
        self, submission: JobSubmission
    ) -> None:
        """Durably store the submission payload for restart RESUME."""
        submissions_dir = self._config.wal_data_dir / "submissions"
        await self._storage_filesystem.mkdir(
            submissions_dir, parents=True, exist_ok=True
        )
        payload = submission.dump()
        try:
            await self._storage_filesystem.atomic_write(
                submissions_dir / f"{submission.job_id}.bin",
                payload,
            )
        except OSError as storage_error:
            self._storage_health.record_failure(storage_error, len(payload))
            raise
        self._storage_health.record_success(len(payload))

    async def _discard_persisted_submission(self, job_id: str) -> None:
        """Remove a terminal job's persisted submission payload —
        terminal jobs must not resume on the next restart."""
        if self._config.wal_data_dir is None:
            return
        submission_path = (
            self._config.wal_data_dir / "submissions" / f"{job_id}.bin"
        )
        await self._remove_persisted_submission(job_id, submission_path)

    async def _remove_persisted_submission(self, job_id: str, submission_path: Path) -> None:
        """Remove the payload if present; a failed removal is logged."""
        try:
            if await self._storage_filesystem.exists(submission_path):
                await self._storage_filesystem.remove(submission_path)
        except OSError as remove_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Could not discard persisted submission for "
                        f"{job_id}: {remove_error} (it will fail loudly "
                        "on a future restart resume attempt)"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _resume_recovered_job(
        self, submission: JobSubmission, elapsed_seconds: float
    ) -> None:
        """Re-activate a recovered ACTIVE job from its persisted
        submission — the tail of the submit handler, minus quorum and
        idempotency (both settled when the job was first accepted). It
        has what is left of its budget: ``elapsed_seconds`` of it passed
        since it was first accepted.

        Workflow execution is AT-LEAST-ONCE across a manager restart: a
        worker may still be running the pre-restart dispatch while this
        re-dispatch runs fresh. The client-facing outcome stays
        exactly-once (the ledger refuses a second terminal transition).
        """
        workflows = self._recovered_submission_workflows(submission)
        remaining_timeout_seconds = submission.timeout_seconds - elapsed_seconds
        callback_addr = self._recovered_callback_address(submission)

        job_info = await self._job_manager.create_job(
            submission=submission,
            callback_addr=callback_addr,
        )
        job_info.leader_node_id = self._node_id.full
        job_info.leader_addr = (self._host, self._tcp_port)
        job_info.fencing_token = 1

        self._manager_state.set_job_submission(submission.job_id, submission)
        self._assign_resource_budget(submission)

        await self._start_recovered_job_timeout(submission, remaining_timeout_seconds)

        await self._leases.claim_job_leadership(
            job_id=submission.job_id,
            tcp_addr=(self._host, self._tcp_port),
        )
        # No peer holds the job (its recovery asked them): its group is new,
        # with the managers live now.
        await self._raft.consensus.create_job_raft(
            submission.job_id, self._raft.consensus.current_members()
        )

        self._record_submitted_job_contacts(submission)

        await self._manager_state.increment_state_version()

        workflow_names = [workflow.name for _, _, workflow in workflows]
        await self._broadcast_job_leadership(
            submission.job_id,
            len(workflows),
            workflow_names,
            callback_addr=submission.callback_addr,
            origin_gate_addr=submission.origin_gate_addr,
        )

        # The dispatcher queues when no worker is registered yet (the
        # late-joiner path) and dispatches as workers re-register.
        await self._register_job_workflows(submission, workflows)
        await self._dispatch_job_workflows(submission)

    def _recovered_submission_workflows(
        self,
        submission: JobSubmission,
    ) -> list[tuple[str, list[str], Workflow]]:
        """
        Unpickle a recovered submission's workflows and settle its timeout.

        Only the workflows a re-run names, and their ancestors, run; a
        submission without a timeout of its own gets as long as its longest
        chain of dependent workflows may take.
        """
        workflows: list[tuple[str, list[str], Workflow]] = restricted_loads(
            submission.workflows
        )
        if submission.rerun_workflow_ids:
            workflows = select_rerun_workflows(workflows, submission.rerun_workflow_ids)
        if submission.timeout_seconds <= 0.0:
            submission.timeout_seconds = resolve_job_deadline_seconds(
                workflows,
                self.env.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            )
        return workflows

    @staticmethod
    def _recovered_callback_address(submission: JobSubmission) -> tuple[str, int] | None:
        """A recovered submission's callback address as a tuple, or None when it has none."""
        return (
            tuple(submission.callback_addr)
            if submission.callback_addr
            else None
        )

    async def _start_recovered_job_timeout(
        self,
        submission: JobSubmission,
        remaining_timeout_seconds: float,
    ) -> None:
        """
        Start a resumed job's timeout tracking with what is left of its budget.

        Restarted from zero, a job resumed after each restart ran past its
        budget. One timeout check of grace when the budget ran out
        meanwhile: a completion already on its way wins.
        """
        timeout_strategy = self._select_timeout_strategy(submission)
        await timeout_strategy.start_tracking(
            job_id=submission.job_id,
            timeout_seconds=(
                remaining_timeout_seconds
                if remaining_timeout_seconds > 0.0
                else self._config.job_timeout_check_interval_seconds
            ),
            gate_addr=tuple(submission.origin_gate_addr)
            if submission.origin_gate_addr
            else None,
        )
        self._manager_state.set_job_timeout_strategy(
            submission.job_id, timeout_strategy
        )

    async def _fail_recovered_active_jobs(self) -> None:
        """Restart handling for jobs recovered ACTIVE from the WAL.

        Each job is first asked about across the datacenter. A peer
        holding it -- live, or ended -- has it from a takeover while this
        manager was down: its leader is another manager now, and this
        manager's record of it is relinquished. Resuming it ran it twice
        under two leaders; failing it told its requestor it failed while
        it ran on.

        A job a quorum of the datacenter knows nowhere died with this
        manager. With a persisted submission payload it RESUMES: the job
        re-activates under the same job id and re-dispatches (workflow
        execution is at-least-once across the restart; the client outcome
        stays exactly-once). Without a payload -- or when its resume
        fails -- it transitions to FAILED durably, and the client's
        recorded callback contact gets a best-effort final push; the
        durable record lands FIRST, so a missed notification still leaves
        status queries truthful.

        A job no quorum could be heard on (a partition at boot) is asked
        about again every peer-sync interval until it is settled.
        """
        if self._job_ledger is None:
            return

        undecided_job_ids = await self._settle_recovered_jobs_once()

        if undecided_job_ids:
            self._task_runner.run(
                self._settle_undecided_recovered_jobs,
                undecided_job_ids,
                alias="settle_recovered_jobs",
            )

    async def _settle_recovered_jobs_once(self) -> list[str]:
        """Try to settle every job the ledger recovered ACTIVE; returns the ids of those left undecided."""
        undecided_job_ids: list[str] = []
        for job_id, job_state in dict(self._job_ledger.get_all_jobs()).items():
            if not await self._settle_recovered_job(job_id, job_state):
                undecided_job_ids.append(job_id)
        return undecided_job_ids

    async def _settle_undecided_recovered_jobs(self, job_ids: list[str]) -> None:
        """Ask about the recovered jobs not yet settled until each is
        settled or this manager stops: the moment the cluster's membership
        forms, while it has not; every peer-sync interval while no quorum
        of the datacenter is heard on them."""
        undecided_job_ids = job_ids
        while undecided_job_ids and self._running:
            await self._wait_for_recovered_job_settle_turn()
            undecided_job_ids = await self._settle_still_undecided_jobs(undecided_job_ids)

    async def _wait_for_recovered_job_settle_turn(self) -> None:
        """Wait a peer-sync interval once the cluster's membership formed, else until it forms."""
        if self._cluster_membership.formed:
            await self._clock.sleep(self._config.peer_job_sync_interval_seconds)
        else:
            await self._cluster_membership.wait_formed()

    async def _settle_still_undecided_jobs(self, undecided_job_ids: list[str]) -> list[str]:
        """Try again to settle each undecided recovered job; returns the ids still undecided."""
        recovered_active = self._job_ledger.get_all_jobs()
        still_undecided_job_ids: list[str] = []
        for job_id in undecided_job_ids:
            if await self._recovered_job_still_undecided(job_id, recovered_active):
                still_undecided_job_ids.append(job_id)
        return still_undecided_job_ids

    async def _recovered_job_still_undecided(
        self,
        job_id: str,
        recovered_active: dict[str, JobState],
    ) -> bool:
        """Whether a job the ledger still holds as recovered could not be settled this time."""
        return (
            job_state := recovered_active.get(job_id)
        ) is not None and not await self._settle_recovered_job(job_id, job_state)

    async def _settle_recovered_job(self, job_id: str, job_state: JobState) -> bool:
        """Settle one job this manager's ledger recovered ACTIVE (see
        ``_fail_recovered_active_jobs``); False when no quorum of the
        datacenter could be heard on it, to be asked about again."""
        if self._held_job_submitted_since_restart(job_id):
            # Submitted here since the restart: the record is that job's.
            return True

        peer_addresses, answers = await self._ask_peers_about_recovered_job(job_id)
        if (verdict := await self._recovered_job_peer_verdict(job_id, peer_addresses, answers)) is not None:
            return verdict

        await self._resume_or_fail_recovered_job(job_id, job_state)
        return True

    def _held_job_submitted_since_restart(self, job_id: str) -> bool:
        """Whether this manager holds the job with a submission: it was submitted here since the restart."""
        return (
            held_job := self._job_manager.get_job_by_id(job_id)
        ) is not None and held_job.submission is not None

    async def _ask_peers_about_recovered_job(
        self,
        job_id: str,
    ) -> tuple[list[tuple[str, int]], list[bytes | None]]:
        """
        Ask every active manager peer, concurrently, for a recovered job's status.

        Returns the peers asked, in sorted order, and each one's answer: None
        for a peer whose ask failed, else the bytes it answered (empty when
        it does not hold the job).
        """
        peer_addresses = sorted(self._manager_state.get_active_manager_peers())

        async def ask(peer_address: tuple[str, int]) -> bytes | None:
            answer = await self._send_to_peer(
                peer_address,
                "job_status",
                job_id.encode(),
                timeout=self._config.tcp_timeout_short_seconds,
            )
            return None if isinstance(answer, Exception) else answer

        answers = await asyncio.gather(*(ask(peer_address) for peer_address in peer_addresses))
        return peer_addresses, answers

    async def _recovered_job_peer_verdict(
        self,
        job_id: str,
        peer_addresses: list[tuple[str, int]],
        answers: list[bytes | None],
    ) -> bool | None:
        """
        Settle a recovered job from its peers' answers, or return None when this manager decides it.

        Returns True once a job a peer holds is relinquished, False when the
        job is to be asked about again, and None when this manager is to
        resume or fail it.
        """
        if holders := self._recovered_job_holders(peer_addresses, answers):
            await self._relinquish_recovered_job(job_id, holders[0])
            return True
        if not await self._recovered_job_decidable_here(job_id, answers):
            return False
        return None

    @staticmethod
    def _recovered_job_holders(
        peer_addresses: list[tuple[str, int]],
        answers: list[bytes | None],
    ) -> list[tuple[str, int]]:
        """The peers whose answer shows they hold the recovered job."""
        return [
            peer_address for peer_address, answer in zip(peer_addresses, answers) if answer
        ]

    async def _relinquish_recovered_job(self, job_id: str, holder: tuple[str, int]) -> None:
        """Relinquish a recovered job a peer holds: close its record here, discard its payload, and log it."""
        await self._log_ledger_shortfall(
            "JobRelinquished",
            job_id,
            await self._job_ledger.relinquish_job(
                job_id, held_by=f"{holder[0]}:{holder[1]}"
            ),
        )
        await self._discard_persisted_submission(job_id)
        await self._udp_logger.log(
            ServerInfo(
                message=(
                    f"Recovered job {job_id} is held by manager "
                    f"{holder[0]}:{holder[1]}: relinquished, "
                    "not resumed"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _recovered_job_decidable_here(
        self,
        job_id: str,
        answers: list[bytes | None],
    ) -> bool:
        """
        Whether this manager may decide a recovered job no peer holds now.

        Not while no quorum of the datacenter answered. Nor, for a job that
        can be resumed, while the cluster's membership has not formed: it
        resumes from its persisted submission into a Raft group founded with
        the cluster's committed members, so it is asked about again once the
        cluster has formed. One that cannot be resumed fails durably now --
        that needs no membership.
        """
        if self._recovered_job_lacks_quorum(answers):
            return False
        return not await self._recovered_job_awaits_membership(job_id)

    def _recovered_job_lacks_quorum(self, answers: list[bytes | None]) -> bool:
        """Whether this manager and the peers that answered fall short of the datacenter's quorum."""
        return sum(answer is not None for answer in answers) + 1 < self._leadership.get_quorum_size()

    async def _recovered_job_awaits_membership(self, job_id: str) -> bool:
        """Whether a recovered job has a persisted submission to resume from while the cluster has not formed."""
        return (
            self._config.wal_data_dir is not None
            and not self._cluster_membership.formed
            and await self._storage_filesystem.exists(
                self._config.wal_data_dir / "submissions" / f"{job_id}.bin"
            )
        )

    async def _resume_or_fail_recovered_job(self, job_id: str, job_state: JobState) -> None:
        """Resume a recovered job no peer holds, or fail it durably when it cannot be resumed."""
        resumed, left_behind = await self._try_resume_recovered_job(job_id, job_state)
        if resumed:
            return
        await self._fail_recovered_job(job_id, job_state, left_behind)

    async def _fail_recovered_job(
        self,
        job_id: str,
        job_state: JobState,
        left_behind: JobInfo | None,
    ) -> None:
        """
        Fail a recovered job that could not be resumed, and tell its requestor.

        The durable record closes first, then its payload goes, then what a
        failed resume built, and the requestor gets a best-effort push.
        """
        await self._log_ledger_shortfall(
            "JobFailed",
            job_id,
            await self._job_ledger.fail_job(
                job_id,
                error_message=(
                    "manager restarted and the job could not be resumed "
                    "(no persisted submission, or its resume failed)"
                ),
                failed_datacenter=self._node_id.datacenter,
                total_completed=job_state.completed_count,
                total_failed=job_state.failed_count,
                duration_ms=0,
                durability=DurabilityLevel.REGIONAL,
            ),
        )
        await self._discard_persisted_submission(job_id)
        if left_behind is not None:
            await self._take_down_failed_resume(job_id, left_behind)
        await self._udp_logger.log(
            ServerInfo(
                message=(
                    f"Recovered job {job_id} failed on restart: in-flight "
                    "state was lost with the previous process"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        await self._notify_requestor_of_restart_failure(job_id, job_state.requestor_id)

    async def _take_down_failed_resume(self, job_id: str, left_behind: JobInfo) -> None:
        """
        Remove what a failed resume built, after the job's record closed.

        It goes through the job's consensus group, which this destroys: left
        here, it would run on -- or time out -- under its FAILED record. A
        teardown failure is logged: raised here, it would end recovery for
        every job after this one.
        """
        left_behind.status = JobStatus.FAILED.value
        try:
            await self._cleanup_job_state(job_id)
        except Exception as teardown_error:
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"Taking down failed resume of job {job_id} "
                        f"failed: {type(teardown_error).__name__}: "
                        f"{teardown_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _try_resume_recovered_job(
        self, job_id: str, job_state: JobState
    ) -> tuple[bool, JobInfo | None]:
        """Attempt payload-based resume. Not resumed (payload missing, or
        the resume raised) falls through to the durable-FAIL path, with
        the job a failed resume left behind -- one it created, not one a
        peer's announcement brought -- for that path to take down."""
        if self._config.wal_data_dir is None:
            return False, None
        submission_path = (
            self._config.wal_data_dir / "submissions" / f"{job_id}.bin"
        )
        if not await self._storage_filesystem.exists(submission_path):
            return False, None

        return await self._resume_from_persisted_submission(job_id, job_state, submission_path)

    async def _resume_from_persisted_submission(
        self,
        job_id: str,
        job_state: JobState,
        submission_path: Path,
    ) -> tuple[bool, JobInfo | None]:
        """Resume the recovered job from its persisted submission; on failure,
        the job the failed resume itself created, for the durable-FAIL path."""
        submission: JobSubmission | None = None
        try:
            submission = JobSubmission.load(
                await self._storage_filesystem.read_bytes(submission_path)
            )
            await self._resume_recovered_job(
                submission,
                # Since the job was first accepted: its record's creation.
                max((self._hlc.now().wall_ms - job_state.created_hlc.wall_ms) / 1000.0, 0.0),
            )
        except Exception as resume_error:
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"Resume of recovered job {job_id} failed: "
                        f"{type(resume_error).__name__}: {resume_error} — "
                        "falling back to durable FAILED"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False, self._job_left_by_failed_resume(job_id, submission)

        await self._udp_logger.log(
            ServerInfo(
                message=(
                    f"Recovered job {job_id} RESUMED from its persisted "
                    "submission (workflows re-dispatched)"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        return True, None

    def _job_left_by_failed_resume(
        self,
        job_id: str,
        submission: JobSubmission | None,
    ) -> JobInfo | None:
        """The job the failed resume created from ``submission`` -- not one a
        peer's announcement brought -- else None."""
        resumed_job = self._job_manager.get_job_by_id(job_id)
        return (
            resumed_job
            if submission is not None
            and self._job_is_of_submission(resumed_job, submission)
            else None
        )

    def _job_is_of_submission(self, job: JobInfo | None, submission: JobSubmission) -> bool:
        """True when the job exists and was created from this very submission."""
        return job is not None and job.submission is submission

    async def _notify_requestor_of_restart_failure(
        self,
        job_id: str,
        requestor_id: str,
    ) -> None:
        host, separator, port_text = requestor_id.rpartition(":")
        if not separator or not port_text.isdigit():
            return

        push = JobStatusPush(
            job_id=job_id,
            status=JobStatus.FAILED.value,
            message=(
                "manager restarted; in-flight job state was lost"
            ),
            is_final=True,
        )
        await self._send_restart_failure_push(job_id, requestor_id, host, port_text, push)

    async def _send_restart_failure_push(
        self,
        job_id: str,
        requestor_id: str,
        host: str,
        port_text: str,
        push: JobStatusPush,
    ) -> None:
        """Push the final FAILED status to the requestor; log a failed send."""
        try:
            push_reply, _ = await self.send_tcp(
                (host, int(port_text)),
                "job_status_push",
                push.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(push_reply, Exception):
                raise push_reply
        except Exception as send_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Restart-failure notification for {job_id} to "
                        f"{requestor_id} failed: {send_error} (the "
                        "durable FAILED record is already committed)"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _handle_job_completion(self, job_id: str) -> None:
        """Handle job completion with notification and cleanup -- once.

        Completion triggers can race (two workflows finishing together each
        find the job complete). The first completes it; another that finds
        it already completing, or already gone, has nothing to do: a second
        completion re-sent every result and, once the job was dropped,
        announced a bare COMPLETED -- even for a failed job.
        """
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return

        completion_summary = await self._complete_job_under_lock(job_id, job)
        if completion_summary is None:
            return

        (
            final_status,
            workflow_results,
            errors,
            total_completed,
            total_failed,
            elapsed_seconds,
        ) = completion_summary

        # D-67: before the retry budget goes with the job's cleanup.
        await self._job_admission_control.record_job_outcome(
            job_id,
            final_status,
            self._retry_budget_manager.refused_retries(job_id),
        )

        # A client that submitted directly gets every workflow's results
        # ahead of the terminal status: result pushes still in flight (or
        # lost) cannot leave its view of the job incomplete.
        await self._send_final_result_to_client(
            job_id,
            final_status,
            workflow_results,
            errors,
            total_completed,
            total_failed,
            elapsed_seconds,
        )

        # Tier-1 terminal push to whoever registered the job callback.
        # The gate path below covers L3 deployments; without this push,
        # gateless (L1/L2, client-direct) deployments never deliver the
        # terminal JobStatusPush and ``client.wait_for_job`` hangs on
        # *successful* completion — the timeout path already pushes
        # ``is_final=True`` through this exact channel, and the RUNNING
        # transition was likewise patched for gateless visibility. Must
        # run before ``_send_job_completion_to_gate``, whose
        # ``_cleanup_job_state`` wipes the callback registration.
        await self._push_job_status_to_client(
            job_id,
            final_status,
            "Job completed",
            is_final=True,
        )

        await self._send_job_completion_to_gate(
            job_id,
            final_status,
            workflow_results,
            errors,
            total_completed,
            total_failed,
            elapsed_seconds,
        )

    async def _complete_job_under_lock(
        self,
        job_id: str,
        job: JobInfo,
    ) -> tuple[str, list[WorkflowResult], list[str], int, int, float] | None:
        """Under the job lock, mark the job COMPLETED once and record its
        outcome durably; its final summary, or None when it already was."""
        async with job.lock:
            if job.status == JobStatus.COMPLETED.value:
                return None
            job.status = JobStatus.COMPLETED.value
            job.completed_at = self._clock.time()
            elapsed_seconds = job.elapsed_seconds()
            final_status = self._determine_final_job_status(job)
            workflow_results, errors, total_completed, total_failed = (
                self._aggregate_workflow_results(job)
            )

            if self._job_ledger is not None:
                # Durable terminal record; idempotent (the ledger
                # refuses a second terminal transition). The ledger's
                # own lock nests inside job.lock with no reverse order
                # anywhere, so this cannot deadlock.
                await self._record_job_outcome_durable(
                    job_id,
                    final_status=final_status,
                    errors=errors,
                    total_completed=total_completed,
                    total_failed=total_failed,
                    duration_ms=int(elapsed_seconds * 1000),
                )
                await self._discard_persisted_submission(job_id)

        return (
            final_status,
            workflow_results,
            errors,
            total_completed,
            total_failed,
            elapsed_seconds,
        )

    async def _send_final_result_to_client(
        self,
        job_id: str,
        final_status: str,
        workflow_results: list[WorkflowResult],
        errors: list[str],
        total_completed: int,
        total_failed: int,
        elapsed_seconds: float,
    ) -> None:
        """Send a directly submitted job's final result -- every workflow's
        results -- to its client. A gate-routed job's client gets the
        gate's global result instead."""
        callback_addr = self._get_job_callback_addr(job_id)
        if callback_addr is None or self._manager_state.get_job_origin_gate(job_id):
            return

        final_result = JobFinalResult(
            job_id=job_id,
            datacenter=self._node_id.datacenter,
            status=final_status,
            workflow_results=workflow_results,
            total_completed=total_completed,
            total_failed=total_failed,
            errors=errors,
            elapsed_seconds=elapsed_seconds,
            fence_token=self._leases.get_fence_token(job_id),
            **self._data_plane_provenance(job_id),
        )
        response = await self._send_to_client(
            tuple(callback_addr),
            "receive_job_final_result",
            final_result.dump(),
            timeout=self._config.tcp_timeout_standard_seconds,
        )
        await self._log_untaken_final_result(job_id, callback_addr, response)

    async def _log_untaken_final_result(
        self,
        job_id: str,
        callback_addr: tuple[str, int],
        response: bytes | Exception | None,
    ) -> None:
        """Log a final result the client did not take."""
        if isinstance(response, Exception) or response not in (b"ok", None):
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Final result for job {job_id[:8]}... was not taken by "
                        f"client {callback_addr}: {response!r}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _log_ledger_shortfall(
        self,
        event_name: str,
        job_id: str,
        result: CommitResult | None,
    ) -> None:
        """Log a job-ledger record that fell short of its requested level.

        The record is durable on this node and applied to ledger state
        either way (the ledger's apply contract); a shortfall only means
        it did not reach the requested tier, which an operator must see.
        ``None`` means the ledger appended nothing (unknown or already
        terminal job), which is not a shortfall.
        """
        if result is None or result.success:
            return
        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"Job ledger {event_name} for {job_id[:8]}... is "
                    f"{result.level_achieved.name}-durable only: {result.error}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _record_job_outcome_durable(
        self,
        job_id: str,
        final_status: str,
        errors: list[str],
        total_completed: int,
        total_failed: int,
        duration_ms: int,
    ) -> None:
        """Record a finished job's terminal as AD-38 ``JobFailed`` when every
        workflow failed, ``JobCompleted`` otherwise -- with the job's class
        and refused retries, the D-67 breaker's facts every member of the
        job's group mirrors (``_on_replicated_job_terminal``)."""
        job_class = self._job_admission_control.job_class_of(job_id)
        refused_retries = self._retry_budget_manager.refused_retries(job_id)
        if final_status == JobStatus.FAILED.value:
            await self._log_ledger_shortfall(
                "JobFailed",
                job_id,
                await self._job_ledger.fail_job(
                    job_id,
                    error_message="; ".join(errors) or "every workflow failed",
                    failed_datacenter=self._node_id.datacenter,
                    total_completed=total_completed,
                    total_failed=total_failed,
                    duration_ms=duration_ms,
                    durability=DurabilityLevel.REGIONAL,
                    job_class=job_class,
                    refused_retries=refused_retries,
                ),
            )
            return

        await self._log_ledger_shortfall(
            "JobCompleted",
            job_id,
            await self._job_ledger.complete_job(
                job_id,
                final_status=final_status,
                total_completed=total_completed,
                total_failed=total_failed,
                duration_ms=duration_ms,
                durability=DurabilityLevel.REGIONAL,
                job_class=job_class,
                refused_retries=refused_retries,
            ),
        )

    def _determine_final_job_status(self, job: JobInfo) -> str:
        if job.workflows_failed == 0:
            return JobStatus.COMPLETED.value
        if job.workflows_failed == job.workflows_total:
            return JobStatus.FAILED.value
        return JobStatus.COMPLETED.value

    def _aggregate_workflow_results(
        self, job: JobInfo
    ) -> tuple[list[WorkflowResult], list[str], int, int]:
        workflow_results: list[WorkflowResult] = []
        errors: list[str] = []
        total_completed = 0
        total_failed = 0

        for workflow_token, workflow_info in job.workflows.items():
            stats, completed, failed = self._aggregate_sub_workflow_stats(
                job, workflow_info
            )
            total_completed += completed
            total_failed += failed

            workflow_results.append(
                self._workflow_result_of(workflow_token, workflow_info, stats)
            )
            if workflow_info.error:
                errors.append(f"{workflow_info.name}: {workflow_info.error}")

        return workflow_results, errors, total_completed, total_failed

    def _workflow_result_of(
        self,
        workflow_token: str,
        workflow_info: WorkflowInfo,
        stats: list[WorkflowStats],
    ) -> WorkflowResult:
        """The workflow's result entry, keyed by its id (else its token)."""
        return WorkflowResult(
            workflow_id=workflow_info.token.workflow_id or workflow_token,
            workflow_name=workflow_info.name,
            status=workflow_info.status.value,
            results=stats,
            error=workflow_info.error,
        )

    def _aggregate_sub_workflow_stats(
        self, job: JobInfo, workflow_info: WorkflowInfo
    ) -> tuple[list[WorkflowStats], int, int]:
        sub_workflows = self._present_sub_workflows(job, workflow_info)
        stats = self._sub_workflow_result_stats(sub_workflows)
        completed, failed, _rate_per_second = self._sub_workflow_progress_totals(sub_workflows)

        return stats, completed, failed

    def _sub_workflow_result_stats(self, sub_workflows: list[SubWorkflowInfo]) -> list[WorkflowStats]:
        """Every result stat of the sub-workflows that reported a result, in sub order."""
        stats: list[WorkflowStats] = []
        for sub_wf in sub_workflows:
            if sub_wf.result:
                stats.extend(sub_wf.result.results)
        return stats

    async def _send_job_completion_to_gate(
        self,
        job_id: str,
        final_status: str,
        workflow_results: list[WorkflowResult],
        errors: list[str],
        total_completed: int,
        total_failed: int,
        elapsed_seconds: float,
    ) -> None:
        await self._notify_gate_of_completion(
            job_id,
            final_status,
            workflow_results,
            total_completed,
            total_failed,
            errors,
            elapsed_seconds,
        )
        await self._cleanup_job_state(job_id)
        await self._log_job_completion(
            job_id, final_status, total_completed, total_failed
        )

    async def _notify_gate_of_completion(
        self,
        job_id: str,
        final_status: str,
        workflow_results: list[WorkflowResult],
        total_completed: int,
        total_failed: int,
        errors: list[str],
        elapsed_seconds: float,
    ) -> None:
        origin_gate_addr = self._manager_state.get_job_origin_gate(job_id)
        if not origin_gate_addr:
            return

        final_result = JobFinalResult(
            job_id=job_id,
            datacenter=self._node_id.datacenter,
            status=final_status,
            workflow_results=workflow_results,
            total_completed=total_completed,
            total_failed=total_failed,
            errors=errors,
            elapsed_seconds=elapsed_seconds,
            fence_token=self._leases.get_fence_token(job_id),
            # JobFinalResult is per-``(job, datacenter)`` — the gate's
            # existing ``set_dc_result`` / ``get_all_dc_results``
            # mapping already dedupes by ``(job_id, datacenter)``, so
            # ``result_sequence`` stays at its default 0. Producer +
            # fence stamps come from ``_data_plane_provenance``.
            **self._data_plane_provenance(job_id),
        )

        final_result_payload = final_result.dump()
        delivered = await self._attempt_completion_notice_send(
            job_id, origin_gate_addr, final_result_payload
        )
        if not delivered:
            # The durable terminal OWES the gate this notice: register
            # the obligation with the fully serialized payload (cleanup
            # erases the source state immediately after this method
            # returns) and let the resend loop carry it until the gate
            # acks. One un-retried send here turned completed work into
            # a client-observed timeout whenever a partition covered
            # the completion instant.
            await self._register_completion_notice_obligation(
                job_id, origin_gate_addr, final_result_payload
            )

    async def _attempt_completion_notice_send(
        self,
        job_id: str,
        origin_gate_addr: tuple[str, int],
        final_result_payload: bytes,
    ) -> bool:
        """One completion-notice send attempt. Returns True when the
        gate ACCEPTED the notice.

        Every reply that proves the terminal REACHED a gate discharges
        the obligation: ``ok`` (this gate applied it), ``duplicate`` /
        ``already_completed`` (a gate already holds the terminal — an
        earlier attempt landed, or a peer delivered it), ``forwarded``
        (a gate took ownership of delivery). ``already_completed`` was
        missing from this set, so an obligation whose notice HAD been
        applied kept resending on the backoff ladder until the 1800s
        age ceiling dropped it loudly — measured on the gate durable-
        restart scenario, where the recovered gate answers exactly
        that."""
        try:
            response = await self._send_to_peer(
                origin_gate_addr,
                "job_final_result",
                final_result_payload,
                timeout=self._config.tcp_timeout_standard_seconds,
            )
            self._raise_unless_completion_notice_taken(response)
            return True
        except Exception as send_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Completion notice for {job_id[:8]}... to gate "
                        f"{origin_gate_addr} failed: {send_error} (owed — "
                        "resend loop carries it until acked)"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

    def _raise_unless_completion_notice_taken(self, response: bytes | Exception | None) -> None:
        """Raise the transport error or a reply that does not discharge the notice."""
        if isinstance(response, Exception):
            raise response
        if response not in (
            b"ok",
            b"duplicate",
            b"already_completed",
            b"forwarded",
            None,
        ):
            raise RuntimeError(f"job_final_result rejected with {response!r}")

    async def _register_completion_notice_obligation(
        self,
        job_id: str,
        origin_gate_addr: tuple[str, int],
        final_result_payload: bytes,
    ) -> None:
        now = self._clock.monotonic()
        self._completion_notice_obligations[job_id] = CompletionNoticeObligation(
            job_id=job_id,
            origin_gate_addr=origin_gate_addr,
            payload=final_result_payload,
            created_at=now,
            last_attempt_at=now,
            attempt_count=1,
        )
        if len(self._completion_notice_obligations) > 256:
            evicted_job_id = next(iter(self._completion_notice_obligations))
            del self._completion_notice_obligations[evicted_job_id]
            await self._udp_logger.log(
                ServerError(
                    message=(
                        "Completion-notice obligation map overflow — "
                        f"dropping oldest owed notice for {evicted_job_id[:8]}"
                        "... (its durable terminal remains in the ledger)"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    async def _resend_completion_notices(self, now: float) -> None:
        """Re-send owed completion notices on capped exponential
        backoff (same reap-loop cadence as eviction notices). Age-
        expired obligations are dropped LOUDLY — the gate's own AD-34
        tracker terminal-resolved the job long before the ceiling, so
        a delivery after that is a duplicate, not a correction."""
        base = self._config.completion_notice_base_interval_seconds
        cap = self._config.completion_notice_max_interval_seconds
        max_age = self._config.completion_notice_max_age_seconds
        for job_id, obligation in list(
            self._completion_notice_obligations.items()
        ):
            await self._resend_or_drop_completion_notice(
                job_id, obligation, now, base, cap, max_age
            )

    async def _resend_or_drop_completion_notice(
        self,
        job_id: str,
        obligation: CompletionNoticeObligation,
        now: float,
        base: float,
        cap: float,
        max_age: float,
    ) -> None:
        """Drop an age-expired obligation loudly; resend one whose backoff elapsed."""
        if obligation.expired(now, max_age):
            del self._completion_notice_obligations[job_id]
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"Completion notice for {job_id[:8]}... to gate "
                        f"{obligation.origin_gate_addr} still unacked "
                        f"after {obligation.attempt_count} attempts over "
                        f"{now - obligation.created_at:.0f}s — dropping "
                        "(durable terminal remains in the ledger)"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
            return
        if now >= obligation.next_attempt_due(base, cap):
            obligation.last_attempt_at = now
            obligation.attempt_count += 1
            self._task_runner.run(
                self._resend_one_completion_notice, job_id
            )

    async def _resend_one_completion_notice(self, job_id: str) -> None:
        obligation = self._completion_notice_obligations.get(job_id)
        if obligation is None:
            return
        delivered = await self._attempt_completion_notice_send(
            job_id, obligation.origin_gate_addr, obligation.payload
        )
        if delivered:
            self._completion_notice_obligations.pop(job_id, None)
            await self._udp_logger.log(
                ServerInfo(
                    message=(
                        f"Owed completion notice for {job_id[:8]}... "
                        f"delivered on attempt {obligation.attempt_count}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _cleanup_job_state(self, job_id: str) -> None:
        # The one teardown of a job this manager drops -- at its completion
        # and at the retention sweep alike -- so nothing kept for the job
        # outlives it on either path.
        await self._announce_terminal_job_to_peers(job_id)
        self._leases.clear_job_leases(job_id)
        self._worker_health_monitor.cleanup_job_progress(job_id)
        self._worker_health_monitor.clear_job_suspicions(job_id)
        self._manager_state.clear_job_state(job_id)
        await self._job_manager.remove_job(job_id)
        # The dispatcher's per-job state goes with the job: the retention
        # sweep that also cleans it walks the JobManager's jobs, and this
        # job just left them, so its queue entries, retry budget and
        # dispatch loop were otherwise held for the process's lifetime.
        if self._workflow_dispatcher is not None:
            await self._workflow_dispatcher.cleanup_job(job_id)
        # A dispatch no report ever showed here (its result went to
        # another manager) leaves its reservation with the job.
        await self._worker_pool.release_job_reservations(job_id)
        await self._raft.consensus.destroy_job_raft(job_id)
        if self._resource_enforcer is not None:
            self._resource_enforcer.release_job(job_id)
        self._led_workflow_resources.release_job(job_id)
        self._job_admission_control.release(job_id)

    async def _announce_terminal_job_to_peers(self, job_id: str) -> None:
        """As the job's leader, sync the job to the peers before dropping it."""
        # The job's leader tells peers the job is terminal BEFORE dropping
        # it: the periodic peer sync only covers jobs this manager still
        # holds, so without this followers kept the job non-terminal
        # forever -- never eligible for their retention sweep, its Raft
        # group never destroyed, and a takeover candidate for a finished
        # job. A follower dropping its copy has nothing to announce.
        if (
            self._leases.is_job_leader(job_id)
            and (job := self._job_manager.get_job_by_id(job_id)) is not None
        ):
            await self._sync_job_state_to_peers(job_id, job)

    async def _log_job_completion(
        self, job_id: str, final_status: str, total_completed: int, total_failed: int
    ) -> None:
        await self._udp_logger.log(
            ServerInfo(
                message=f"Job {job_id[:8]}... {final_status.lower()} ({total_completed} completed, {total_failed} failed)",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )


    # =========================================================================
    # Raft TCP Handlers
    # =========================================================================

    @tcp.receive()
    async def raft_request_vote(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle incoming Raft RequestVote RPC from a manager peer."""
        response = await self._raft.handle_request_vote(data)
        return response if response is not None else b""

    @tcp.receive()
    async def raft_request_vote_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle incoming Raft RequestVoteResponse from a manager peer."""
        await self._raft.handle_request_vote_response(data)
        return b""

    @tcp.receive()
    async def raft_append_entries(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle incoming Raft AppendEntries RPC from a manager peer."""
        response = await self._raft.handle_append_entries(data)
        return response if response is not None else b""

    @tcp.receive()
    async def raft_ledger_proposal(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Propose a peer's forwarded AD-38 ledger entry (if Raft leader)."""
        proposal = LedgerProposal.load(data)
        result = await self._ledger_replicator.handle_forwarded(proposal)
        return result.dump()

    @tcp.receive()
    async def clock_offset_probe(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """AD-39: answer a peer's clock offset probe with this node's
        physical time."""
        return ClockOffsetProbeReply(
            responder_id=self._node_id.full,
            responder_physical_ms=self._hlc.physical_ms(),
        ).dump()

    @tcp.receive()
    async def raft_append_entries_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle incoming Raft AppendEntriesResponse from a manager peer."""
        await self._raft.handle_append_entries_response(data)
        return b""

    # =========================================================================
    # Cluster Membership Group TCP Handlers (AD-52 slice C)
    # =========================================================================

    @tcp.receive()
    async def cluster_hello(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Answer a founder's greeting with where this node stands in its
        cluster's formation."""
        return await self._cluster_membership.handle_hello(data)

    @tcp.receive()
    async def found_cluster(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Adopt a proposed founding that names this node, if it holds no
        membership group."""
        return await self._cluster_membership.handle_found(data)

    @tcp.receive()
    async def cluster_join(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Take a node of the cohort into the formed cluster (leader only)."""
        return await self._cluster_membership.handle_join(data)

    @tcp.receive()
    async def cluster_mode(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Set the cluster's mode: open, frozen or read-only (AD-52
        section 13)."""
        return await self._cluster_membership.handle_mode(data)

    @tcp.receive()
    async def cluster_resize(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Add an address to the cluster's cohort, or remove one (AD-52
        ``ResizeCluster``)."""
        return await self._cluster_membership.handle_resize(data)

    @tcp.receive()
    async def cluster_status(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """The cluster's membership as of now -- a linearizable read (AD-52
        section 11)."""
        return await self._cluster_membership.handle_status(data)

    @tcp.receive()
    async def cluster_watch(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """A membership watch's long poll (AD-52 section 9)."""
        return await self._cluster_membership.handle_watch(data)

    @tcp.receive()
    async def cluster_metrics(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """This node's metrics of its cluster's membership (AD-52 section
        18), with its AD-44 retry-budget counters per job and its own
        telemetry in the schema every role shares (D-68)."""
        if not (membership_metrics := await self._cluster_membership.handle_metrics(data)):
            return membership_metrics
        reply = ClusterMetricsReply.load(membership_metrics)
        reply.retry_budget_consumed = self._retry_budget_manager.consumed_by_job()
        reply.retry_budget_exhausted = self._retry_budget_manager.exhausted_by_job()
        self._add_manager_telemetry(reply)
        return reply.dump()

    def _add_manager_telemetry(self, reply: ClusterMetricsReply) -> None:
        """D-68: what this manager's heartbeat tells its gates (state,
        capacity, workload, AD-19 throughput, its datacenter's AD-42 SLO),
        its dispatch sends by outcome, and each worker's dispatch round
        trips (D-5)."""
        heartbeat = self._build_manager_heartbeat()
        reply.role = "manager"
        reply.node_state = heartbeat.state
        reply.capacity = {
            "total_cores": heartbeat.total_cores,
            "available_cores": heartbeat.available_cores,
            "workers": heartbeat.worker_count,
            "healthy_workers": heartbeat.healthy_worker_count,
        }
        reply.workload = {
            "active_jobs": heartbeat.active_jobs,
            "active_workflows": heartbeat.active_workflows,
            "pending_workflows": heartbeat.pending_workflow_count,
        }
        reply.dispatch_throughput = {
            "observed": heartbeat.health_throughput,
            "expected": heartbeat.health_expected_throughput,
        }
        reply.dispatch_outcomes = self._dispatch.dispatch_outcome_counts()
        reply.dispatch_latency = {
            worker_id: latency_section(observation)
            for worker_id, observation in self._manager_state.get_worker_dispatch_latency_observations(
                self._clock.monotonic()
            ).items()
        }
        reply.slo = {heartbeat.datacenter: slo_section(heartbeat)}

    @tcp.receive()
    async def cluster_leave(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Release a member's address: a member draining itself, or an
        operator removing one that is gone (AD-52 section 13)."""
        return await self._cluster_membership.handle_leave(data)

    @tcp.receive()
    async def cluster_raft_request_vote(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle the membership group's RequestVote."""
        response = await self._cluster_membership.handle_request_vote(data)
        return response if response is not None else b""

    @tcp.receive()
    async def cluster_raft_append_entries(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle the membership group's AppendEntries."""
        response = await self._cluster_membership.handle_append_entries(data)
        return response if response is not None else b""

    @tcp.receive()
    async def cluster_raft_install_snapshot(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Install the membership group's snapshot from its leader."""
        response = await self._cluster_membership.handle_install_snapshot(data)
        return response if response is not None else b""


__all__ = ["ManagerServer"]
