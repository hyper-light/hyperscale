"""
Gate Server composition root.

This module provides the GateServer class that inherits directly from
HealthAwareServer and implements all gate functionality through modular
coordinators and handlers.

Gates coordinate job execution across datacenters:
- Accept jobs from clients
- Dispatch jobs to datacenter managers
- Aggregate global job status
- Handle cross-DC retry with leases
- Provide the global job view to clients

Protocols:
- UDP: SWIM healthchecks (inherited from HealthAwareServer)
  - Gates form a gossip cluster with other gates
  - Gates probe managers to detect DC failures
  - Leader election uses SWIM membership info
- TCP: Data operations
  - Job submission from clients
  - Job dispatch to managers
  - Status aggregation from managers
  - Lease coordination between gates

Module Structure:
- Coordinators: Business logic (leadership, dispatch, stats, cancellation, peer, health)
- Handlers: TCP message processing (job, manager, cancellation, state sync, ping)
- State: GateRuntimeState for mutable runtime state
- Configuration: Env, read directly (derived settings in ``config``)
"""

import asyncio
import dataclasses
import hashlib
import statistics
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import cloudpickle

from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKey
from hyperscale.distributed.slo.latency_slo import LatencySLO
from hyperscale.distributed.slo.slo_health_classifier import SLOHealthClassifier
from hyperscale.distributed.server import tcp
from hyperscale.distributed.leases import JobLeaseManager
from hyperscale.distributed.jobs.job_status_order import JobStatusOrder
from hyperscale.distributed.ledger import DatacenterReassignment, JobLedger
from hyperscale.distributed.health.phi_accrual_config import PhiAccrualConfig
from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
from hyperscale.distributed.ledger.pipeline.commit_pipeline import (
    REGIONAL_TIMEOUT_SECONDS,
    CommitResult,
)
from hyperscale.distributed.raft import LedgerReplicator
from hyperscale.distributed.ledger.wal.wal_entry import WALEntry
from hyperscale.distributed.raft.models import LedgerPlacementQuery, LedgerProposal
from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.reporting.results import Results
from hyperscale.reporting.reporter import Reporter
from hyperscale.reporting.common.types import ReporterTypes
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.distributed.server.events import VersionedStateClock
from hyperscale.distributed.cluster import ClusterJoinError, decode_join_message
from hyperscale.distributed.cluster.cluster_membership import ClusterMembership
from hyperscale.distributed.cluster.cluster_view_cache import ClusterViewCache
from hyperscale.distributed.cluster.models import ClusterMetricsReply
from hyperscale.distributed.cluster.cluster_watch_follower import ClusterWatchFollower
from hyperscale.distributed.cluster.models.cluster_view import ClusterView
from hyperscale.distributed.jobs.logical_id_generator import LogicalIdGenerator
from hyperscale.distributed.cluster.joined_peer import JoinedPeer
from hyperscale.distributed.cluster.joined_peer_store import JoinedPeerStore
from hyperscale.distributed.runtime import Filesystem, RealFilesystem
from hyperscale.distributed.raft.store.raft_storage import RaftStorage
from hyperscale.distributed.raft.store.raft_store import RaftStore
from hyperscale.distributed.swim.core.node_id import NodeId
from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.swim import HealthAwareServer, GateStateEmbedder
from hyperscale.distributed.swim.health import (
    FederatedHealthMonitor,
    DCLeaderAnnouncement,
    CrossClusterAck,
)
from hyperscale.distributed.models import (
    JobStatusQuery,
    ReadConsistency,
    GateRegistrationResponse,
    GateInfo,
    GateState,
    NodeRole,
    GateHeartbeat,
    GateRegistrationRequest,
    AggregatedJobStats,
    GlobalJobResult,
    GlobalJobStatus,
    ManagerDiscoveryBroadcast,
    ManagerHeartbeat,
    JobAck,
    JobLeaderGateTransfer,
    JobSubmission,
    JobStatus,
    JobStatusPush,
    JobProgress,
    JobFinalResult,
    CancelJob,
    CancelAck,
    JobCancelResponse,
    GateStateSnapshot,
    DatacenterHealth,
    DatacenterSubstitution,
    DatacenterRegistrationState,
    DatacenterStatus,
    UpdateTier,
    DatacenterInfo,
    DatacenterListRequest,
    DatacenterListResponse,
    WorkflowQueryRequest,
    WorkflowStatusInfo,
    WorkflowQueryResponse,
    DatacenterWorkflowStatus,
    GateWorkflowQueryResponse,
    RegisterCallback,
    RegisterCallbackResponse,
    JobUpdateRecord,
    JobUpdatePollRequest,
    JobUpdatePollResponse,
    RateLimitResponse,
    ReporterResultPush,
    WorkflowResultPush,
    WorkflowDCResult,
    restricted_loads,
    GateJobReplica,
    JobLeadershipAnnouncement,
    JobLeadershipAck,
    JobLeaderGateTransferAck,
    JobLeaderManagerTransfer,
    JobLeaderManagerTransferAck,
    ManagerJobLeaderTransfer,
    GateStateSyncRequest,
    GateStateSyncResponse,
    JobStatsCRDT,
    JobProgressReport,
    JobTimeoutReport,
    JobLeaderTransfer,
    JobFinalStatus,
    WorkflowProgress,
    PingRequest,
    GatePingResponse,
)
from hyperscale.distributed.models.coordinates import NetworkCoordinate
from hyperscale.distributed.swim.core import (
    ErrorStats,
)
from hyperscale.distributed.swim.detection import HierarchicalConfig
from hyperscale.distributed.health import (
    ManagerHealthConfig,
    CircuitBreakerManager,
    GateHealthState,
    LatencyTracker,
)
from hyperscale.distributed.monitoring import ProcessResourceMonitor, ResourceMetrics
from hyperscale.distributed.reliability import (
    HybridOverloadDetector,
    LoadShedder,
    ServerRateLimiter,
    BackpressureSignal,
)
from hyperscale.distributed.jobs.gates import (
    GateJobManager,
    ConsistentHashRing,
    GateJobTimeoutTracker,
)
from hyperscale.distributed.jobs import (
    WindowedStatsCollector,
    WindowedStatsPush,
    JobLeadershipTracker,
)

from hyperscale.distributed.idempotency import (
    GateIdempotencyCache,
    create_idempotency_config_from_env,
)
from hyperscale.distributed.datacenters import (
    DatacenterHealthManager,
    DatacenterOverloadConfig,
    LeaseManager as DatacenterLeaseManager,
    CrossDCCorrelationDetector,
)
from hyperscale.distributed.protocol.version import (
    NodeCapabilities,
    CURRENT_PROTOCOL_VERSION,
)
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.discovery.security.role_validator import (
    RoleValidator,
)
from hyperscale.distributed.routing import (
    BlendedScoringConfig,
    DatacenterCandidate,
    DatacenterLatencyEstimator,
    ObservedLatencyTracker,
    BlendedLatencyScorer,
    GateJobRouter,
    JobDispatchCooldowns,
    RoutingScorer,
    ScoringConfig,
)
from hyperscale.distributed.hlc import (
    ClockFenceVerdict,
    ClockOffsetMonitor,
    ClockOffsetProber,
    HybridLogicalClock,
    hlc_node_id,
    tcp_probe_exchange,
)
from hyperscale.distributed.hlc.models import ClockOffsetProbeReply
from hyperscale.distributed.capacity import (
    DatacenterCapacityAggregator,
    SpilloverEvaluator,
)
from hyperscale.distributed.reliability.best_effort_manager import BestEffortManager
from hyperscale.distributed.reliability.reliability_config import (
    create_reliability_config_from_env,
)
from hyperscale.distributed.resources.datacenter_resource_aggregator import (
    DatacenterResourceAggregator,
)
from hyperscale.distributed.slo.resource_aware_predictor import (
    ResourceAwareSLOPredictor,
)
from hyperscale.logging import LogLevel
from hyperscale.logging.hyperscale_logging_models import (
    ClusterWatchConnectivityChanged,
    DatacenterRegenerated,
    ServerInfo,
    ServerWarning,
    ServerDebug,
    StaleObservationsDecayed,
)

from .stats_coordinator import GateStatsCoordinator
from .dispatch_coordinator import GateDispatchCoordinator
from .job_failover_coordinator import GateJobFailoverCoordinator
from .ledger_region_span import LedgerRegionSpan
from .leadership_coordinator import GateLeadershipCoordinator
from .peer_coordinator import GatePeerCoordinator
from .health_coordinator import GateHealthCoordinator
from .orphan_job_coordinator import GateOrphanJobCoordinator
from .datacenter_manager_selector import DatacenterManagerSelector
from .raft_integration import GateRaftIntegration
from .replication_coordinator import GateJobReplicationCoordinator
from .config import (
    derive_datacenter_leader_failover_seconds,
    derive_gate_orphan_grace_seconds,
)
from .state import GateRuntimeState
from .handlers import (
    GatePingHandler,
    GateJobHandler,
    GateManagerHandler,
    GateCancellationHandler,
    GateStateSyncHandler,
)

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env
    from hyperscale.distributed.runtime import Clock, Random, TransportFactory


# Storage seam (borrowed, never shut down here; swap_defaults rebinds it
# under SIM).
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()


class GateServer(HealthAwareServer):
    """
    Gate node in the distributed Hyperscale system.

    This is the composition root that wires together all gate modules:
    - Configuration (Env)
    - Runtime state (GateRuntimeState)
    - Coordinators (leadership, dispatch, stats, cancellation, peer, health)
    - Handlers (TCP/UDP message handlers)

    Gates:
    - Form a gossip cluster for leader election (UDP SWIM)
    - Accept job submissions from clients (TCP)
    - Dispatch jobs to managers in target datacenters (TCP)
    - Aggregate global job status across DCs (TCP)
    - Manage leases for at-most-once semantics
    """

    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: "Env",
        dc_id: str = "global",
        datacenter_managers: dict[str, list[tuple[str, int]]] | None = None,
        datacenter_manager_udp: dict[str, list[tuple[str, int]]] | None = None,
        gate_peers: list[tuple[str, int]] | None = None,
        gate_udp_peers: list[tuple[str, int]] | None = None,
        lease_timeout: float = 30.0,
        incarnation_storage_dir: str | None = None,
        wal_data_dir: Path | None = None,
        *,
        clock: "Clock | None" = None,
        random_source: "Random | None" = None,
        transport_factory: "TransportFactory | None" = None,
        raft_store: RaftStore | None = None,
    ):
        """
        Initialize the Gate server.

        Args:
            host: Host address to bind
            tcp_port: TCP port for data operations
            udp_port: UDP port for SWIM protocol
            env: Environment configuration
            dc_id: Datacenter identifier (default "global" for gates)
            datacenter_managers: DC -> manager TCP addresses mapping
            datacenter_manager_udp: DC -> manager UDP addresses mapping
            gate_peers: Peer gate TCP addresses
            gate_udp_peers: Peer gate UDP addresses
            lease_timeout: Lease timeout in seconds
        """
        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
            node_role="gate",
            clock=clock,
            random_source=random_source,
            transport_factory=transport_factory,
            incarnation_storage_dir=incarnation_storage_dir,
            # D1: an identity the Raft store resumed keeps its start time,
            # so this node is the member its groups knew.
            node_created_ms=(
                None if raft_store is None else NodeId.created_ms_of(raft_store.identity.node_id_full)
            ),
        )
        # Where every Raft group of this node keeps its persistent state
        # (D1): the opened store the node was given, or none -- each start
        # then a new member of every group. Borrowed: its opener closes it.
        self._raft_storage: RaftStorage = raft_store if raft_store is not None else VolatileRaftStorage()

        # Store reference to env
        self.env = env

        # Phase 8 gate durable tier: when a data dir is given, accepted
        # jobs are persisted to a JobLedger (the same WAL + checkpoint +
        # archive composite the manager runs) and recovered at start —
        # a rebooted gate re-serves status and resumes AD-34 tracking
        # for its own jobs instead of having amnesia. None = volatile
        # gate (the pre-Phase-8 behavior, unchanged).
        self._wal_data_dir = wal_data_dir
        self._job_ledger: JobLedger | None = None
        # Managers this gate was joined to at runtime, kept in its data
        # directory (created at start, once the logger exists).
        self._joined_peer_store: JoinedPeerStore | None = None
        # AD-52 section 10: each datacenter's manager membership as last
        # observed through its watch -- the cache routing's manager lists
        # follow, so a resize of a datacenter's managers reaches this gate.
        self._datacenter_views: dict[str, ClusterViewCache] = {}
        self._datacenter_watches: dict[str, ClusterWatchFollower] = {}
        # The cluster each datacenter's managers last answered as: a
        # different one means the datacenter was founded again.
        self._datacenter_cluster_uuids: dict[str, str] = {}
        # AD-38 replication of the ledger; built with the Raft integration
        # in _init_coordinators (ledger writes only happen after start).
        self._ledger_replicator: LedgerReplicator | None = None
        self._ledger_tier_spans_regions: bool | None = None

        # Create modular runtime state
        self._modular_state = GateRuntimeState(
            forward_throughput_interval_start=self._clock.monotonic(),
        )
        self._modular_state.set_client_update_history_limit(
            env.GATE_CLIENT_UPDATE_HISTORY_LIMIT
        )

        # Datacenter -> manager addresses mapping
        self._datacenter_managers = datacenter_managers or {}
        self._datacenter_manager_udp = datacenter_manager_udp or {}
        # Operator-declared managers -- configured, or joined with
        # `hyperscale join` -- stay expected members of their datacenters;
        # managers learned from heartbeats and gossip are forgotten again
        # when the stale-manager reaper retires them.
        self._declared_datacenter_managers: dict[str, frozenset[tuple[str, int]]] = {
            datacenter_id: frozenset(manager_addrs)
            for datacenter_id, manager_addrs in self._datacenter_managers.items()
        }

        # Per-DC registration state tracking (AD-27)
        self._dc_registration_states: dict[str, DatacenterRegistrationState] = {}
        for datacenter_id, manager_addrs in self._datacenter_managers.items():
            self._dc_registration_states[datacenter_id] = DatacenterRegistrationState(
                dc_id=datacenter_id,
                configured_managers=list(manager_addrs),
            )

        # A manager's circuit also opens while phi accrual on its heartbeats
        # suspects it (AD-52 section 8); the health manager is built below.
        self._circuit_breaker_manager = CircuitBreakerManager(
            env,
            is_peer_suspected=lambda manager_addr: self._dc_health_manager.is_manager_suspected(manager_addr),
        )
        # Peer gates are watched by SWIM, with no phi detector on their
        # edges yet: only request errors open theirs.
        self._peer_gate_circuit_breaker = CircuitBreakerManager(env, is_peer_suspected=lambda _gate_addr: False)

        # Gate peers
        self._gate_peers = gate_peers or []
        self._gate_udp_peers = gate_udp_peers or []

        for idx, tcp_addr in enumerate(self._gate_peers):
            if idx < len(self._gate_udp_peers):
                self._modular_state.set_udp_to_tcp_mapping(
                    self._gate_udp_peers[idx], tcp_addr
                )

        # Health state tracking (AD-19). Manager heartbeats, last-seen
        # times, health and negotiated capabilities live in
        # GateRuntimeState (_modular_state).
        self._manager_health_config = ManagerHealthConfig()

        # Latency tracking
        self._peer_gate_latency_tracker = LatencyTracker(
            sample_max_age=60.0,
            sample_max_count=30,
        )

        # Load shedding (AD-22)
        self._overload_detector = HybridOverloadDetector()
        self._resource_monitor = ProcessResourceMonitor()
        self._last_resource_metrics: ResourceMetrics | None = None
        self._gate_health_state: str = "healthy"
        self._previous_gate_health_state: str = "healthy"
        # The resource sampling loop is the detector's sampler; shedding
        # checks read the state it settled on.
        self._load_shedder = LoadShedder(self._overload_detector, detector_sampled_externally=True)

        # Backpressure tracking (AD-37) - state managed by _modular_state

        self._forward_throughput_interval_seconds: float = (
            env.GATE_THROUGHPUT_INTERVAL_SECONDS
        )

        # Rate limiting (AD-24)
        # Health-gated (AD-24): limits tighten with the overload state the
        # node's resource sampler settles on.
        self._rate_limiter = ServerRateLimiter(
            inactive_cleanup_seconds=env.RATE_LIMIT_CLIENT_IDLE_TIMEOUT,
            overload_detector=self._overload_detector,
            detector_sampled_externally=True,
            overload_retry_after_seconds=env.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
        )

        # Protocol version (AD-25)
        self._node_capabilities = NodeCapabilities.current(node_version=f"gate-{dc_id}")

        # Versioned state clock
        self._versioned_clock = VersionedStateClock()

        # Job management
        self._job_manager = GateJobManager()
        self._job_final_statuses: dict[tuple[str, str], float] = {}
        self._job_global_result_sent: set[str] = set()
        # Jobs whose global result was built (claimed under the job lock):
        # each job is finished exactly once, whichever of a datacenter's
        # final result or the AD-44 deadline gets there first.
        self._job_completion_claimed: set[str] = set()

        # Consistent hash ring
        self._job_hash_ring = ConsistentHashRing(replicas=150)

        self._workflow_dc_results: dict[
            str, dict[str, dict[str, WorkflowResultPush]]
        ] = {}
        self._workflow_dc_results_lock = asyncio.Lock()
        self._workflow_result_timeout_seconds: float = (
            env.GATE_WORKFLOW_RESULT_TIMEOUT_SECONDS
        )
        self._reporter_submission_timeout_seconds: float = (
            env.REPORTER_SUBMISSION_TIMEOUT_SECONDS
        )
        self._allow_partial_workflow_results: bool = (
            env.GATE_ALLOW_PARTIAL_WORKFLOW_RESULTS
        )
        self._workflow_result_timeout_tokens: dict[str, dict[str, str]] = {}
        self._workflow_result_expected_dc_counts: dict[str, dict[str, int]] = {}

        # Data-plane idempotency: the ``(job_id, workflow_id)`` pairs whose
        # aggregate this gate has claimed -- one aggregate per workflow.
        # Marked when the per-datacenter results are taken for
        # aggregation (all reported, the per-workflow timeout, or the job
        # completing without some datacenters); every later push for the
        # pair is acked without re-aggregating: a duplicate, a result a
        # manager rebuilt (under a new sequence) after a failover, or a
        # datacenter's result arriving after the timeout recorded it
        # missing. Cleaned up with the job's other per-job state; bounded
        # by its workflows.
        self._finalized_workflow_results: set[tuple[str, str]] = set()

        # Per-job leadership tracking
        self._job_leadership_tracker: JobLeadershipTracker[int] = JobLeadershipTracker(
            node_id="",
            node_addr=("", 0),
        )

        # Job lease manager
        self._job_lease_manager = JobLeaseManager(
            node_id="",
            default_duration=env.JOB_LEASE_DURATION,
            cleanup_interval=env.JOB_LEASE_CLEANUP_INTERVAL,
            # As long as the gate keeps the job itself.
            released_retention_seconds=env.FAILED_JOB_MAX_AGE,
            logger=self._udp_logger,
        )

        # Windowed stats
        self._windowed_stats = WindowedStatsCollector(
            window_size_ms=env.STATS_WINDOW_SIZE_MS,
            drift_tolerance_ms=env.STATS_DRIFT_TOLERANCE_MS,
            max_window_age_ms=env.STATS_MAX_WINDOW_AGE_MS,
        )
        self._stats_push_interval_ms: float = env.STATS_PUSH_INTERVAL_MS

        # Shared hybrid logical clock (AD-38/AD-39): Raft entry timestamps
        # and the durable ledger's events are ordered on this one clock.
        # Its node id derives from the restart-stable node id.
        self._hlc = HybridLogicalClock(
            node_id=hlc_node_id(self._node_id.full),
            clock=self._clock,
            max_offset_ms=env.HLC_MAX_CLOCK_OFFSET_MS,
        )
        # AD-39: fenced while this gate's clock disagrees with a quorum of
        # the gate cluster (or its HLC ran past its own clock). A fenced
        # gate neither leads (Raft or SWIM) nor accepts jobs.
        self._clock_offset_monitor = ClockOffsetMonitor(
            hlc=self._hlc,
            cluster_size=self._configured_gate_count,
            sample_ttl_seconds=env.HLC_OFFSET_SAMPLE_TTL_SECONDS,
            clock=self._clock,
        )
        self._leadership_refusals.append(self._is_clock_fenced)
        self._leadership_refusals.append(self._refuses_leadership_for_a_ready_peer)

        self._job_aggregated_workflow_stats: dict[
            str, dict[str, list[WorkflowStats]]
        ] = {}

        # CRDT stats (AD-14)
        self._job_stats_crdt: dict[str, JobStatsCRDT] = {}
        self._job_stats_crdt_lock = asyncio.Lock()

        # Datacenter health manager (AD-16).
        #
        # Capacity saturation is NEVER unhealth at the gate: the default
        # overload config mapped capacity utilization >= 0.95 to
        # UNHEALTHY, so a datacenter whose cores were fully busy (a load
        # generator's steady state — probed: any vus-2 workflow on the
        # 2-core worker) fast-rejected ALL new submissions for the whole
        # execution window, and a manager-side capacity misreport
        # (traced: worker-heartbeat starvation zeroing available_cores)
        # blinded submissions PERMANENTLY. Per the AD-16 contract
        # "BUSY != UNHEALTHY" (and the manager's own accept semantics —
        # workers-busy still accepts and queues), a full DC classifies
        # BUSY/DEGRADED: routing deprioritizes it but submissions stay
        # accepted. The busy/degraded capacity bands keep their
        # defaults; only the capacity->UNHEALTHY edge is removed.
        # Zero-worker / no-heartbeat unhealth is untouched (those are
        # structural signals, not capacity).
        self._dc_health_manager = DatacenterHealthManager(
            phi_config=PhiAccrualConfig.for_manager_heartbeats(env),
            get_configured_managers=lambda dc: self._datacenter_managers.get(dc, []),
            overload_config=DatacenterOverloadConfig(
                capacity_utilization_unhealthy_threshold=float("inf"),
            ),
        )
        for datacenter_id in self._datacenter_managers.keys():
            self._dc_health_manager.add_datacenter(datacenter_id)

        self._capacity_aggregator = DatacenterCapacityAggregator(
            clock=self._clock,
            staleness_threshold_seconds=env.CAPACITY_STALENESS_THRESHOLD_SECONDS,
        )
        self._spillover_evaluator = SpilloverEvaluator.from_env(env, self._clock)

        # Route learning (AD-45)
        self._route_learning_config = BlendedScoringConfig.from_env(env)
        self._observed_latency_tracker = ObservedLatencyTracker(
            config=self._route_learning_config,
            clock=self._clock,
        )
        self._blended_scorer = BlendedLatencyScorer(
            self._observed_latency_tracker,
            adaptive_routing_enabled=self._route_learning_config.adaptive_routing_enabled,
        )
        # One latency estimate per datacenter for routing and spillover
        # alike: Vivaldi and observed evidence, a conservative prior where
        # either is missing (AD-36, AD-45).
        self._latency_estimator = DatacenterLatencyEstimator(
            coordinate_tracker=self._coordinate_tracker,
            get_datacenter_coordinate=self._get_datacenter_coordinate,
            get_observed_latency=self._blended_scorer.get_observed_latency,
        )

        # Datacenter lease manager
        self._dc_lease_manager = DatacenterLeaseManager(
            node_id="",
            lease_timeout=lease_timeout,
        )

        # Orphan job tracking
        self._orphan_grace_period: float = derive_gate_orphan_grace_seconds(env)
        self._orphan_check_interval: float = env.GATE_ORPHAN_CHECK_INTERVAL
        # Every background loop's run token: stopping the gate cancels each
        # before the ledger and caches they write to are closed.
        self._background_loop_tokens: list[str] = []

        self._dead_peer_reap_interval: float = env.GATE_DEAD_PEER_REAP_INTERVAL
        self._dead_peer_check_interval: float = env.GATE_DEAD_PEER_CHECK_INTERVAL
        self._quorum_stepdown_consecutive_failures: int = (
            env.GATE_QUORUM_STEPDOWN_CONSECUTIVE_FAILURES
        )
        self._consecutive_quorum_failures: int = 0

        # Job timeout tracker (AD-34)
        self._job_timeout_tracker = GateJobTimeoutTracker(
            gate=self,
            check_interval=env.GATE_TIMEOUT_CHECK_INTERVAL,
            stuck_threshold=env.GATE_ALL_DC_STUCK_THRESHOLD,
        )

        # Idempotency cache (AD-40); its cleanup task starts in start()
        self._idempotency_config = create_idempotency_config_from_env(env)
        self._idempotency_cache: GateIdempotencyCache[bytes] = GateIdempotencyCache(
            config=self._idempotency_config,
            task_runner=self._task_runner,
            logger=self._udp_logger,
        )

        # Dead gate tracking for Raft-based leadership takeover
        self._dead_gate_addrs: set[tuple[str, int]] = set()

        # The gate's lifecycle state and state version live in
        # GateRuntimeState (_modular_state), shared with the handlers.

        # Quorum circuit breaker
        cb_config = env.get_circuit_breaker_config()
        self._quorum_circuit = ErrorStats(
            max_errors=cb_config["max_errors"],
            window_seconds=cb_config["window_seconds"],
            half_open_after=cb_config["half_open_after"],
        )

        # Recovery semaphore
        self._recovery_semaphore = asyncio.Semaphore(env.RECOVERY_MAX_CONCURRENT)

        # Configuration
        self._lease_timeout = lease_timeout
        # Terminal-job retention (was a literal 3600.0, the same value
        # as this setting's default; now configurable like the manager's).
        self._job_max_age: float = env.FAILED_JOB_MAX_AGE
        self._job_cleanup_interval: float = env.GATE_JOB_CLEANUP_INTERVAL
        self._rate_limit_cleanup_interval: float = env.GATE_RATE_LIMIT_CLEANUP_INTERVAL
        self._batch_stats_interval: float = env.GATE_BATCH_STATS_INTERVAL
        self._tcp_timeout_short: float = env.GATE_TCP_TIMEOUT_SHORT
        self._tcp_timeout_standard: float = env.GATE_TCP_TIMEOUT_STANDARD
        self._tcp_timeout_forward: float = env.GATE_TCP_TIMEOUT_FORWARD

        # State embedder for SWIM heartbeats
        self.set_state_embedder(
            GateStateEmbedder(
                get_node_id=lambda: self._node_id.full,
                get_datacenter=lambda: self._node_id.datacenter,
                is_leader=self.is_leader,
                get_term=lambda: self._leader_election.state.current_term,
                get_state_version=self._modular_state.get_state_version,
                get_gate_state=lambda: self._modular_state.get_gate_state().value,
                get_active_jobs=lambda: self._job_manager.job_count(),
                get_active_datacenters=lambda: self._count_active_datacenters(),
                get_manager_count=lambda: sum(
                    len(managers) for managers in self._datacenter_managers.values()
                ),
                get_tcp_host=lambda: self._host,
                get_tcp_port=lambda: self._tcp_port,
                on_manager_heartbeat=self._handle_embedded_manager_heartbeat,
                on_gate_heartbeat=self._handle_gate_peer_heartbeat,
                get_known_gates=self._get_known_gates_for_piggyback,
                get_job_leaderships=self._get_job_leaderships_for_piggyback,
                # Reachable, not merely configured: a datacenter counts
                # while its managers' heartbeats keep it out of UNHEALTHY.
                get_health_has_dc_connectivity=lambda: self._count_active_datacenters() > 0,
                get_health_connected_dc_count=self._count_active_datacenters,
                get_health_throughput=self._get_forward_throughput,
                get_health_expected_throughput=self._get_expected_forward_throughput,
                get_health_overload_state=lambda: self._gate_health_state,
                get_coordinate=lambda: self._coordinate_tracker.get_coordinate(),
                on_peer_coordinate=self._on_peer_coordinate_update,
                # AD-19 addendum (Phase D): uniform LHM gossip
                get_lhm_score=lambda: self._local_health.score,
            )
        )

        # Register callbacks
        self.register_on_node_dead(self._on_node_dead)
        self.register_on_node_join(self._on_node_join)
        self.register_on_become_leader(self._on_gate_become_leader)
        self.register_on_lose_leadership(self._on_gate_lose_leadership)
        self.register_on_peer_confirmed(self._on_peer_confirmed)

        # Initialize hierarchical failure detector (AD-30).
        # Gate bracket is deliberately wider than the manager's
        # ``SWIM_SUSPICION_*`` defaults — see ``GATE_SWIM_*`` rationale
        # in ``env.py``. Global-layer death runs through the canonical
        # ``_on_suspicion_expired`` pipeline (AD-31); per-peer side
        # effects (circuit-breaker cleanup, dead-log emission) live in
        # ``_on_node_dead`` registered via ``register_on_node_dead``.
        self.init_hierarchical_detector(
            config=HierarchicalConfig(
                global_min_timeout=float(env.GATE_SWIM_GLOBAL_MIN_TIMEOUT),
                global_max_timeout=float(env.GATE_SWIM_GLOBAL_MAX_TIMEOUT),
                job_min_timeout=float(env.GATE_SWIM_JOB_MIN_TIMEOUT),
                job_max_timeout=float(env.GATE_SWIM_JOB_MAX_TIMEOUT),
            ),
            on_job_death=self._on_manager_dead_for_dc,
            get_job_n_members=self._get_dc_manager_count,
        )

        # Federated Health Monitor
        fed_config = env.get_federated_health_config()
        self._dc_health_monitor = FederatedHealthMonitor(
            probe_interval=fed_config["probe_interval"],
            probe_timeout=fed_config["probe_timeout"],
            suspicion_timeout=fed_config["suspicion_timeout"],
            max_consecutive_failures=fed_config["max_consecutive_failures"],
            on_probe_error=self._on_federated_probe_error,
        )

        # Cross-DC correlation detector
        self._cross_dc_correlation = CrossDCCorrelationDetector(
            config=env.get_cross_dc_correlation_config(),
            on_callback_error=self._on_cross_dc_callback_error,
        )
        for datacenter_id in self._datacenter_managers.keys():
            self._cross_dc_correlation.add_datacenter(datacenter_id)

        # Discovery services (AD-28): one per datacenter, owned by the
        # selector that orders dispatch candidates (known leader first,
        # then rendezvous + EWMA). Created on demand so datacenters that
        # join at runtime are covered; peers keyed by "host:port".
        self._manager_selector = DatacenterManagerSelector(
            create_discovery=lambda: DiscoveryService(
                env.get_discovery_config(
                    node_role="gate",
                    static_seeds=[],
                    allow_dynamic_registration=True,
                )
            ),
            get_manager_heartbeats=self._modular_state.get_datacenter_manager_statuses,
        )
        self._dc_manager_discovery: dict[str, DiscoveryService] = (
            self._manager_selector.discovery_by_datacenter
        )
        self._discovery_failure_decay_interval: float = (
            env.DISCOVERY_FAILURE_DECAY_INTERVAL
        )

        for datacenter_id, manager_addrs in self._datacenter_managers.items():
            for manager_addr in manager_addrs:
                self._manager_selector.track_manager(datacenter_id, manager_addr)

        # Peer discovery. A solo gate (no peers) is a valid topology —
        # single-gate L3 deployments and the SIM scenarios — but
        # DiscoveryConfig refuses an empty seed list unless dynamic
        # registration is allowed, so fall back to it in that case
        # (the same solo-node pattern ManagerDiscovery uses).
        peer_static_seeds = [f"{host}:{port}" for host, port in self._gate_peers]
        peer_discovery_config = env.get_discovery_config(
            node_role="gate",
            static_seeds=peer_static_seeds,
            allow_dynamic_registration=not peer_static_seeds,
        )
        self._peer_discovery = DiscoveryService(peer_discovery_config)
        for host, port in self._gate_peers:
            self._peer_discovery.add_peer(
                peer_id=f"{host}:{port}",
                host=host,
                port=port,
                role="gate",
            )

        # Role validator (AD-28)
        self._role_validator = RoleValidator(
            cluster_id=env.CLUSTER_ID,
            environment_id=env.ENVIRONMENT_ID,
            strict_mode=env.MTLS_STRICT_MODE.lower() == "true",
        )

        # Coordinators and handlers exist from construction: SWIM callbacks,
        # peer failure handling and leader election reach them while
        # start() is still running, so they are never absent. The TCP
        # endpoints answer "not ready" until start() opens them
        # (_accepting_requests) -- the gate's warm-up contract.
        self._accepting_requests: bool = False
        self._init_coordinators()
        self._init_handlers()

    # =========================================================================
    # Coordinator and Handler Initialization
    # =========================================================================

    def _init_coordinators(self) -> None:
        """Initialize coordinator instances with dependencies."""
        self._best_effort_manager = BestEffortManager(
            task_runner=self._task_runner,
            config=create_reliability_config_from_env(self.env),
            clock=self._clock,
            completion_handler=self._complete_best_effort_job,
        )
        self._stats_coordinator = GateStatsCoordinator(
            clock=self._clock,
            client_push_timeout_seconds=self._tcp_timeout_short,
            state=self._modular_state,
            logger=self._udp_logger,
            node_host=self._host,
            node_port=self._tcp_port,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            windowed_stats=self._windowed_stats,
            get_job_callback=self._job_manager.get_callback,
            get_job_status=self._job_manager.get_job,
            get_all_running_jobs=self._job_manager.get_running_jobs,
            has_job=self._job_manager.has_job,
            send_tcp=self._send_tcp,
            forward_status_push_to_peers=self._forward_job_status_push_to_peers,
        )


        self._leadership_coordinator = GateLeadershipCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            leadership_tracker=self._job_leadership_tracker,
            get_node_id=lambda: self._node_id,
            get_node_addr=lambda: (self._host, self._tcp_port),
            send_tcp=self._send_tcp,
            get_active_peers=lambda: self._modular_state.get_active_peers_list(),
            get_cluster_size=self._configured_gate_count,
            peer_rpc_timeout_seconds=float(self.env.GATE_TCP_TIMEOUT_STANDARD),
        )

        self._dispatch_coordinator = GateDispatchCoordinator(
            clock=self._clock,
            client_push_timeout_seconds=self._tcp_timeout_standard,
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_timeout_tracker=self._job_timeout_tracker,
            persist_accepted_job=self._persist_accepted_job_durable,
            circuit_breaker_manager=self._circuit_breaker_manager,
            datacenter_managers=self._datacenter_managers,
            quorum_circuit=self._quorum_circuit,
            select_datacenters=self._select_datacenters_with_fallback,
            broadcast_leadership=self._broadcast_job_leadership,
            send_tcp=self._send_tcp,
            increment_version=self._increment_version,
            confirm_manager_for_dc=self._confirm_manager_for_dc,
            suspect_manager_for_dc=self._suspect_manager_for_dc,
            record_forward_throughput_event=self._record_forward_throughput_event,
            record_forward_attempt_event=self._record_forward_attempt_event,
            get_node_host=lambda: self._host,
            get_node_port=lambda: self._tcp_port,
            get_node_id_short=lambda: self._node_id.short,
            capacity_aggregator=self._capacity_aggregator,
            spillover_evaluator=self._spillover_evaluator,
            observed_latency_tracker=self._observed_latency_tracker,
            estimate_datacenter_latencies_ms=lambda: self._latency_estimator.estimate(
                self._datacenter_managers.keys()
            ),
            manager_selector=self._manager_selector,
            finalize_failed_job=self._finalize_failed_job,
            on_job_dispatched=self._on_job_dispatched,
            record_dispatch_failure=lambda job_id,
            datacenter_id: self._job_router.record_dispatch_failure(
                job_id,
                datacenter_id,
            ),
            manager_dispatch_timeout_seconds=self.env.GATE_TCP_TIMEOUT_STANDARD,
            datacenter_leader_failover_seconds=derive_datacenter_leader_failover_seconds(self.env),
            leader_heartbeat_interval_seconds=self.env.LEADER_HEARTBEAT_INTERVAL,
            record_fallback_used=lambda from_datacenter, to_datacenter: self._job_router.record_fallback_used(
                from_datacenter, to_datacenter
            ),
        )

        # AD-36 Part 13: a job's unfinished share moves off a datacenter it
        # lost mid-run. Checked every manager heartbeat interval, the
        # fastest a datacenter's health classification changes.
        self._job_failover_coordinator = GateJobFailoverCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_leadership_tracker=self._job_leadership_tracker,
            job_timeout_tracker=self._job_timeout_tracker,
            dispatch_coordinator=self._dispatch_coordinator,
            datacenter_managers=self._datacenter_managers,
            clock=self._clock,
            send_tcp=self._send_tcp,
            get_node_addr=lambda: (self._host, self._tcp_port),
            get_node_id_short=lambda: self._node_id.short,
            is_running=lambda: self._running,
            # As routing sees it: an UNHEALTHY verdict held at DEGRADED
            # during a correlated failure or a partition is no loss
            # (AD-36 Part 13, AD-33 Part 6).
            classify_datacenter_health=lambda datacenter: (
                self._health_coordinator.build_datacenter_candidates([datacenter])[0]
                .health_bucket.lower()
            ),
            route_replacement=lambda job_id, placement_constraint, occupied_datacenters: next(
                iter(
                    self._job_router.route_job(
                        job_id,
                        1,
                        placement_constraint,
                        occupied_datacenters=occupied_datacenters,
                    ).primary_datacenters
                ),
                None,
            ),
            delivered_workflow_ids=self._delivered_workflow_ids,
            release_workflow_timeouts=self._release_workflow_result_timeouts,
            replicate_placement=self._replicate_job_placement,
            record_reassignment=self._record_datacenter_reassignment_durable,
            check_interval_seconds=self.env.MANAGER_HEARTBEAT_INTERVAL,
            cancel_timeout_seconds=self._tcp_timeout_standard,
        )

        self._peer_coordinator = GatePeerCoordinator(
            clock=self._clock,
            random=self._random,
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            peer_discovery=self._peer_discovery,
            job_hash_ring=self._job_hash_ring,
            job_leadership_tracker=self._job_leadership_tracker,
            versioned_clock=self._versioned_clock,
            recovery_semaphore=self._recovery_semaphore,
            recovery_jitter_min=self.env.RECOVERY_JITTER_MIN,
            recovery_jitter_max=self.env.RECOVERY_JITTER_MAX,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_udp_port=lambda: self._udp_port,
            confirm_peer=self._confirm_peer,
            handle_job_leader_failure=self._handle_job_leader_failure,
            remove_peer_circuit=self._peer_gate_circuit_breaker.remove_circuit,
            is_leader=self.is_leader,
        )

        self._slo_health_classifier = SLOHealthClassifier.from_env(self.env)
        self._health_coordinator = GateHealthCoordinator(
            clock=self._clock,
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            dc_health_manager=self._dc_health_manager,
            dc_health_monitor=self._dc_health_monitor,
            cross_dc_correlation=self._cross_dc_correlation,
            track_manager=self._manager_selector.track_manager,
            versioned_clock=self._versioned_clock,
            manager_health_config=self._manager_health_config,
            datacenter_managers=self._datacenter_managers,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            confirm_manager_for_dc=self._confirm_manager_for_dc,
            record_manager_heartbeat=self._record_manager_heartbeat,
            on_partition_healed=self._on_partition_healed,
            on_partition_detected=self._on_partition_detected,
            capacity_aggregator=self._capacity_aggregator,
            circuit_breaker_manager=self._circuit_breaker_manager,
            resource_aggregator=DatacenterResourceAggregator(
                clock=self._clock,
                staleness_seconds=self.env.RESOURCE_VIEW_STALENESS_SECONDS,
            ),
            resource_predictor=ResourceAwareSLOPredictor.from_env(self.env),
            latency_slo=LatencySLO.from_env(self.env),
            slo_health_classifier=self._slo_health_classifier,
        )

        # A datacenter that failed a job's dispatch is tried last for that
        # job for as long as the gate holds off a failed manager -- its
        # circuit breaker's open-to-half-open interval.
        self._job_router = GateJobRouter(
            get_datacenter_candidates=self._get_datacenter_candidates_for_router,
            latency_estimator=self._latency_estimator,
            scorer=RoutingScorer(ScoringConfig.from_env(self.env)),
            dispatch_cooldowns=JobDispatchCooldowns(
                clock=self._clock,
                cooldown_seconds=self.env.CIRCUIT_BREAKER_HALF_OPEN_AFTER,
            ),
        )

        self._replication_coordinator = GateJobReplicationCoordinator(
            clock=self._clock,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            get_node_id=lambda: self._node_id,
            get_node_addr=lambda: (self._host, self._tcp_port),
            send_tcp=self._send_tcp,
            apply_committed=self._apply_committed_replica,
            drop_committed=self._drop_committed_replica,
            prepared_ttl_seconds=float(self.env.GATE_SWIM_GLOBAL_MAX_TIMEOUT) * 2.0,
            quorum_timeout_seconds=float(self.env.GATE_TCP_TIMEOUT_STANDARD),
            peer_rpc_timeout_seconds=float(self.env.GATE_TCP_TIMEOUT_STANDARD),
        )

        self._orphan_job_coordinator = GateOrphanJobCoordinator(
            clock=self._clock,
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_hash_ring=self._job_hash_ring,
            job_leadership_tracker=self._job_leadership_tracker,
            job_manager=self._job_manager,
            get_node_id=lambda: self._node_id,
            get_node_addr=lambda: (self._host, self._tcp_port),
            send_tcp=self._send_tcp,
            get_active_peers=lambda: self._modular_state.get_active_peers(),
            forward_status_push_to_peers=self._forward_job_status_push_to_peers,
            state_repair_callback=self._repair_orphan_job_state,
            commit_takeover_callback=self._commit_gate_job_leadership_takeover,
            finalize_failed_job=self._finalize_failed_job,
            is_cluster_leader=self.is_leader,
            orphan_check_interval_seconds=self._orphan_check_interval,
            orphan_grace_period_seconds=self._orphan_grace_period,
            orphan_extension_min_grant_seconds=self.env.EXTENSION_MIN_GRANT,
            orphan_extension_max_extensions=self.env.EXTENSION_MAX_EXTENSIONS,
        )

        # Raft consensus integration. Members are keyed by full gate node
        # id (iter_known_gates); the self id must use the same form, or a
        # follower knows its leader by an id it cannot resolve.
        self._ledger_replica = JobLedgerReplica()
        # AD-52 slice C: the gate tier keeps its membership in one Raft
        # group -- the configured gates found it, and every job group takes
        # its members from it.
        self._cluster_membership = ClusterMembership(
            self._node_id.full,
            (self._host, self._tcp_port),
            frozenset(self._gate_peers),
            send_request=lambda address, action, payload: self._send_to_gate_peer(
                address, action, payload, float(self.env.GATE_TCP_TIMEOUT_STANDARD)
            ),
            logger=self._udp_logger,
            task_runner=self._task_runner,
            hlc=self._hlc,
            clock=self._clock,
            may_lead=self._may_lead,
            cluster_uuids=LogicalIdGenerator(scope=self._node_id.full, clock=self._clock),
            formation_interval_seconds=self.env.CLUSTER_FORMATION_INTERVAL_SECONDS,
            tombstone_retention_seconds=self.env.CLUSTER_TOMBSTONE_RETENTION_SECONDS,
            request_timeout_seconds=float(self.env.GATE_TCP_TIMEOUT_STANDARD),
            watch_wait_ceiling_seconds=self.env.CLUSTER_WATCH_WAIT_SECONDS,
            on_cohort_change=self._on_cohort_change,
            snapshot_entries=self.env.CLUSTER_SNAPSHOT_ENTRIES,
            snapshot_catch_up_entries=self.env.CLUSTER_SNAPSHOT_CATCH_UP_ENTRIES,
            leader_lease_drift_bound=(
                self.env.RAFT_CLOCK_DRIFT_BOUND if self.env.RAFT_LEADER_LEASES_ENABLED else None
            ),
            storage=self._raft_storage,
        )
        self._raft = GateRaftIntegration(
            ledger_replica=self._ledger_replica,
            cluster_size=self._configured_gate_count,
            # The gate's quorum timeout -- the bound its replica 2PC uses.
            proposal_timeout_seconds=float(self.env.GATE_TCP_TIMEOUT_STANDARD),
            request_timeout_seconds=float(self.env.GATE_TCP_TIMEOUT_STANDARD),
            cluster_members=self._cluster_membership.node_addresses,
            storage=self._raft_storage,
            node_id=self._node_id.full,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            send_tcp=self._send_tcp,
            on_job_raft_leader=self._on_job_raft_leader,
            on_job_raft_lose_leader=self._on_job_raft_lose_leader,
            clock=self._hlc,
            may_lead=self._may_lead,
        )
        # AD-39 measures the configured gates, one entry per address, from
        # the start: before the cluster's membership forms, and never
        # counting one address twice under two processes' ids.
        self._clock_probe_peers = {
            f"{peer_host}:{peer_port}": (peer_host, peer_port)
            for peer_host, peer_port in self._gate_peers
        }
        self._clock_offset_prober = ClockOffsetProber(
            node_id=self._node_id.full,
            hlc=self._hlc,
            monitor=self._clock_offset_monitor,
            peers=lambda: self._clock_probe_peers,
            exchange=tcp_probe_exchange(self.send_tcp),
            clock=self._clock,
            probe_interval_seconds=self.env.HLC_OFFSET_PROBE_INTERVAL_SECONDS,
            logger=self._udp_logger,
            on_fence_change=self._on_clock_fence_change,
        )
        # A forwarded proposal or placement query waits out the group
        # leader's proposal timeout plus one short transit.
        ledger_rpc_timeout = (
            float(self.env.GATE_TCP_TIMEOUT_STANDARD) + self._tcp_timeout_short
        )
        self._ledger_replicator = LedgerReplicator(
            consensus=self._raft.consensus,
            node_id=self._node_id.full,
            send_tcp=self._send_to_gate_peer,
            forward_method="gate_raft_ledger_proposal",
            forward_timeout_seconds=ledger_rpc_timeout,
            clock=self._clock,
            logger=self._udp_logger,
        )
        self._ledger_region_span = LedgerRegionSpan(
            consensus=self._raft.consensus,
            node_id=self._node_id.full,
            region_of=self._gate_region_of,
            tier_regions=self._gate_tier_regions,
            send_tcp=self._send_to_gate_peer,
            query_method="gate_raft_ledger_placement",
            query_timeout_seconds=ledger_rpc_timeout,
            clock=self._clock,
            logger=self._udp_logger,
        )

    def _init_handlers(self) -> None:
        """Initialize handler instances with dependencies."""
        self._ping_handler = GatePingHandler(
            state=self._modular_state,
            logger=self._udp_logger,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            is_leader=self.is_leader,
            get_current_term=lambda: self._leader_election.state.current_term,
            classify_dc_health=self._classify_datacenter_health,
            count_active_dcs=self._count_active_datacenters,
            get_all_job_ids=self._job_manager.get_all_job_ids,
            get_datacenter_managers=lambda: self._datacenter_managers,
        )

        self._job_handler = GateJobHandler(
            clock=self._clock,
            client_push_timeout_seconds=self._tcp_timeout_standard,
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_leadership_tracker=self._job_leadership_tracker,
            quorum_circuit=self._quorum_circuit,
            load_shedder=self._load_shedder,
            job_lease_manager=self._job_lease_manager,
            send_tcp=self._send_tcp,
            idempotency_cache=self._idempotency_cache,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            is_leader=self.is_leader,
            check_rate_limit=self._check_rate_limit_for_operation,
            should_shed_request=self._should_shed_request,
            has_quorum_available=self._has_quorum_available,
            quorum_size=self._quorum_size,
            select_datacenters_with_fallback=self._select_datacenters_with_fallback,
            get_healthy_gates=self._get_healthy_gates,
            broadcast_job_leadership=self._broadcast_job_leadership,
            dispatch_job_to_datacenters=self._dispatch_job_to_datacenters,
            forward_job_progress_to_peers=self._forward_job_progress_to_peers,
            record_request_latency=self._record_request_latency,
            record_dc_job_stats=self._record_dc_job_stats,
            handle_update_by_tier=self._handle_update_by_tier,
            replication_coordinator=self._replication_coordinator,
            get_active_peer_addrs=lambda: list(
                self._modular_state.get_active_peers_list()
            ),
            default_timeout_multiplier=self.env.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            current_raft_members=lambda: self._raft.consensus.current_members(),
            cluster_formed=lambda: self._cluster_membership.formed,
            cluster_read_only=lambda: self._cluster_membership.read_only,
            overload_retry_after_seconds=self.env.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
            replication_retry_after_seconds=float(self.env.GATE_TCP_TIMEOUT_STANDARD),
        )

        self._manager_handler = GateManagerHandler(
            clock=self._clock,
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            env=self.env,
            datacenter_managers=self._datacenter_managers,
            role_validator=self._role_validator,
            node_capabilities=self._node_capabilities,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_healthy_gates=self._get_healthy_gates,
            record_manager_heartbeat=self._record_manager_heartbeat,
            handle_manager_backpressure_signal=self._handle_manager_backpressure_signal,
            update_dc_backpressure=self._update_dc_backpressure,
            set_manager_backpressure_none=self._set_manager_backpressure_none,
            broadcast_manager_discovery=self._broadcast_manager_discovery,
            send_tcp=self._send_tcp,
            get_progress_callback=self._get_progress_callback_for_job,
            ingest_manager_heartbeat=self._ingest_manager_heartbeat,
        )

        self._cancellation_handler = GateCancellationHandler(
            client_push_timeout_seconds=self._tcp_timeout_short,
            manager_request_timeout_seconds=self._tcp_timeout_standard,
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            datacenter_managers=self._datacenter_managers,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            check_rate_limit=self._check_rate_limit_for_operation,
            send_tcp=self._send_tcp,
            record_cancellation=self._record_cancellation_durable,
        )

        self._state_sync_handler = GateStateSyncHandler(
            peer_forward_timeout_seconds=self._tcp_timeout_forward,
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_leadership_tracker=self._job_leadership_tracker,
            versioned_clock=self._versioned_clock,
            peer_circuit_breaker=self._peer_gate_circuit_breaker,
            send_tcp=self._send_tcp,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            is_leader=self.is_leader,
            get_term=lambda: self._leader_election.state.current_term,
            get_state_snapshot=self._get_state_snapshot,
            apply_state_snapshot=self._apply_gate_state_snapshot,
            get_known_leader_manager_term=self._get_known_leader_manager_term_for_dc,
        )

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    async def start(self) -> None:
        """
        Start the gate server.

        Initializes coordinators, wires handlers, and starts background tasks.
        """
        self._modular_state.initialize_locks()
        await self.start_server(init_context=self.env.get_swim_init_context())

        # Restore (or create) this node's persisted incarnation so a
        # restarted gate rejoins above its pre-restart value.
        await self.initialize_incarnation_store()

        # Managers this gate was joined to in earlier runs: registered
        # with at boot exactly like configured ones.
        await self._restore_joined_managers()

        if self._wal_data_dir is not None:
            # Phase 8 gate durable tier; ledger events share the gate's
            # clock.
            self._job_ledger = await JobLedger.open(
                wal_path=self._wal_data_dir / "wal",
                checkpoint_dir=self._wal_data_dir / "checkpoints",
                archive_dir=self._wal_data_dir / "archive",
                region_code=self._node_id.datacenter,
                gate_id=self._node_id.short,
                regional_replicator=self._replicate_ledger_regional,
                global_replicator=self._replicate_ledger_global,
                # GLOBAL gets REGIONAL's budget: a commit turn waits on it,
                # and a job's later ledger writes queue behind that turn.
                global_timeout_seconds=REGIONAL_TIMEOUT_SECONDS,
                logger=self._udp_logger,
                clock=self._hlc,
            )
            await self._recover_durable_jobs()

        # Set node_id on trackers
        self._job_leadership_tracker.node_id = self._node_id.full
        self._job_leadership_tracker.node_addr = (self._host, self._tcp_port)
        self._job_lease_manager.node_id = self._node_id.full
        self._dc_lease_manager.set_node_id(self._node_id.full)

        await self._job_hash_ring.add_node(
            node_id=self._node_id.full,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
        )

        await self._udp_logger.log(
            ServerInfo(
                message="Gate starting in SYNCING state",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Join SWIM cluster. Gate-tier peers come from configuration so
        # we know their role authoritatively — pre-populate their entry
        # in ``_peer_roles`` so leader-election cohort filtering and
        # role-aware scheduling work without waiting for gossip.
        for peer_udp in self._gate_udp_peers:
            await self.join_cluster(peer_udp, seed_role="gate")

        # Start SWIM probe cycle
        self._task_runner.run(self.start_probe_cycle)

        # Wait for cluster stabilization
        await self._wait_for_cluster_stabilization()

        # Leader election jitter
        jitter_max = self.env.LEADER_ELECTION_JITTER_MAX
        if jitter_max > 0 and len(self._gate_udp_peers) > 0:
            jitter = self._random.uniform(0, jitter_max)
            await self._clock.sleep(jitter)

        # Start leader election
        await self.start_leader_election()

        # Wait for election to stabilize
        await self._clock.sleep(self.env.MANAGER_STARTUP_SYNC_DELAY)

        # Complete startup sync
        await self._complete_startup_sync()

        # Initialize health monitor
        self._dc_health_monitor.set_callbacks(
            send_udp=self._send_xprobe,
            cluster_id=f"gate-{self._node_id.datacenter}",
            node_id=self._node_id.full,
            on_dc_health_change=self._on_dc_health_change,
            on_dc_latency=self._on_dc_latency,
            on_dc_leader_change=self._on_dc_leader_change,
        )

        for datacenter_id, manager_udp_addrs in list(
            self._datacenter_manager_udp.items()
        ):
            if manager_udp_addrs:
                self._dc_health_monitor.add_datacenter(
                    datacenter_id, manager_udp_addrs[0]
                )

        await self._dc_health_monitor.start()

        # Start job lease manager cleanup

        # Start background tasks
        # An expired lease is an orphan candidate from the cleanup's first
        # pass on (the cleanup starts with the background loops).
        self._job_lease_manager.set_on_lease_expired(self._orphan_job_coordinator.on_lease_expired)
        self._start_background_loops()

        # Start timeout tracker (AD-34)
        await self._job_timeout_tracker.start()

        await self._idempotency_cache.start()
        self._best_effort_manager.start_deadline_loop()
        self._accepting_requests = True

        # Start Raft consensus, the gate tier's membership group (whose
        # handlers answer now that requests are accepted) and clock offset
        # probing.
        await self._raft.start()
        await self._cluster_membership.start()
        self._task_runner.run(self._clock_offset_prober.run, alias="clock_offset_prober")

        await self._orphan_job_coordinator.start()

        if self._datacenter_managers:
            await self._register_with_managers()
        for datacenter_id in sorted(self._datacenter_managers):
            self._watch_datacenter_membership(datacenter_id)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Gate started with {len(self._datacenter_managers)} DCs, "
                f"state={self._modular_state.get_gate_state().value}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
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
        await self._udp_logger.log(
            (ServerInfo if reply.released else ServerWarning)(
                message=(
                    f"Left cluster membership as {reply.released_member_id}"
                    if reply.released
                    else f"Cluster membership drain refused: {reply.refusal}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def stop(
        self,
        drain_timeout: float = 5,
        broadcast_leave: bool = True,
    ) -> None:
        """Stop the gate server."""
        # Drain membership first, while its loops and this transport run.
        if broadcast_leave and self._running:
            await self.leave_cluster()
        self._running = False
        await self._stop_background_loops()

        # The datacenter membership watches end with the gate; their
        # tasks end with the task runner's shutdown below.
        for follower in self._datacenter_watches.values():
            follower.stop()
        self._datacenter_watches.clear()
        self._datacenter_views.clear()
        self._datacenter_cluster_uuids.clear()

        await self._dc_health_monitor.stop()
        await self._job_timeout_tracker.stop()
        await self._best_effort_manager.shutdown()

        if self._job_ledger is not None:
            await self._job_ledger.close()

        await self._orphan_job_coordinator.stop()
        await self._idempotency_cache.close()

        # Stop the membership group, Raft consensus and clock offset probing
        await self._cluster_membership.stop()
        self._clock_offset_prober.stop()
        await self._raft.stop()

        await super().stop(
            drain_timeout=drain_timeout,
            broadcast_leave=broadcast_leave,
        )

    def _start_background_loops(self) -> None:
        loops = [
            self._lease_cleanup_loop,
            self._job_cleanup_loop,
            self._rate_limit_cleanup_loop,
            self._batch_stats_loop,
            self._windowed_stats_push_loop,
            self._dead_peer_reap_loop,
            self._datacenter_correlation_loop,
            self._job_failover_coordinator.run,
            self._resource_sampling_loop,
            # AD-28 discovery maintenance.
            self._discovery_maintenance_loop,
            # Job lease expiry (AD-31 orphan detection).
            self._job_lease_manager.run_cleanup,
        ]
        if self._gate_udp_peers:
            loops.append(self._gate_peer_readmission_loop)
        self._background_loop_tokens = [
            f"{run.task_name}:{run.run_id}" for loop in loops if (run := self._task_runner.run(loop))
        ]

    async def _stop_background_loops(self) -> None:
        """Cancel every background loop -- each one, even when another's
        cancel fails -- then raise the first failure."""
        cleanup_error: Exception | None = None
        background_loop_tokens = self._background_loop_tokens
        self._background_loop_tokens = []

        for token in background_loop_tokens:
            try:
                await self._task_runner.cancel(token)
            except Exception as error:
                cleanup_error = cleanup_error or error
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to cancel background loop {token}: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

        if cleanup_error:
            raise cleanup_error

    # =========================================================================
    # UDP Cross-Cluster Overrides
    # =========================================================================

    async def _handle_xack_response(
        self,
        source_addr: tuple[str, int] | bytes,
        ack_data: bytes,
    ) -> None:
        """
        Handle a cross-cluster health acknowledgment (xack) from a DC leader.

        Passes the ack to the FederatedHealthMonitor for processing,
        which updates DC health state and invokes latency callbacks.

        Args:
            source_addr: The source UDP address of the ack (DC leader)
            ack_data: The serialized CrossClusterAck message
        """
        try:
            ack = CrossClusterAck.load(ack_data)

            if ack.is_leader and isinstance(source_addr, tuple):
                self._dc_health_monitor.update_leader(
                    datacenter=ack.datacenter,
                    leader_udp_addr=source_addr,
                    leader_node_id=ack.node_id,
                    leader_term=ack.leader_term,
                )

            self._dc_health_monitor.handle_ack(ack)

        except Exception as error:
            await self.handle_exception(error, "_handle_xack_response")

    # =========================================================================
    # TCP Handlers - Delegating to Handler Classes
    # =========================================================================

    @tcp.receive()
    async def manager_status_update(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle manager status update via TCP."""
        if self._accepting_requests:
            return await self._manager_handler.handle_status_update(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def manager_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle manager registration."""
        if self._accepting_requests and (
            transport := self._tcp_server_request_transports.get(addr)
        ):
            return await self._manager_handler.handle_register(
                addr, data, transport, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def manager_discovery(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle manager discovery broadcast from peer gate."""
        if self._accepting_requests:
            return await self._manager_handler.handle_discovery(
                addr, data, self._datacenter_manager_udp, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def reporter_result_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle reporter result push from manager."""
        if self._accepting_requests:
            return await self._manager_handler.handle_reporter_result_push(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def job_submission(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job submission from client."""
        if self._is_clock_fenced():
            return JobAck(
                job_id=JobSubmission.load(data).job_id,
                accepted=False,
                error="Gate clock fenced (offset beyond bound), not accepting jobs",
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()
        if self._accepting_requests:
            return await self._job_handler.handle_submission(
                addr, data, self._modular_state.get_active_peer_count()
            )
        # The listener is up before start() builds the handlers. Answer
        # with a transient rejection the client retries (protocol
        # transient vocabulary: "not ready") -- a bare b"error" is not a
        # JobAck, so clients failed to decode it (UnpicklingError) and
        # could not tell "starting" from "broken".
        return JobAck(
            job_id=JobSubmission.load(data).job_id,
            accepted=False,
            error="Gate is not ready: still starting, not accepting jobs",
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
        ).dump()

    @tcp.receive()
    async def job_status(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """A client's status query -- the same action a manager answers, so
        a client asks either tier alike (this one was named
        ``receive_job_status_request``, which no client sent: every status
        poll to a gate failed for want of a handler)."""
        if self._accepting_requests:
            return await self._job_handler.handle_status_request(
                addr, data, self._answer_job_status_query
            )
        return b""

    @tcp.receive()
    async def receive_job_progress(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job progress update from manager."""
        if self._accepting_requests:
            return await self._job_handler.handle_progress(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def ping(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """A ping, from a client or a peer gate proving it is alive -- the
        action a manager answers too, so a client pings either tier alike.
        (Named ``receive_gate_ping``, which only peer gates sent, every
        client's gate ping failed for want of a handler.)"""
        if self._accepting_requests:
            return await self._ping_handler.handle_ping(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job cancellation request from client (AD-20).

        Wire-action name must match what the client sends — the
        ``@tcp.receive()`` decorator registers handlers by
        ``func.__name__``, and ``ClientCancellationManager``
        targets the action ``"cancel_job"``. A prior incarnation
        named this ``receive_cancel_job`` which silently mismatched
        every inbound cancel request.
        """
        if self._accepting_requests:
            return await self._cancellation_handler.handle_cancel_job(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def job_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job cancellation complete notification (AD-20).

        Wire-action name aligned with what the manager pushes —
        the ``@tcp.receive()`` decorator registers by
        ``func.__name__`` and the manager's push uses action
        ``"job_cancellation_complete"``.
        """
        if self._accepting_requests:
            return await self._cancellation_handler.handle_cancellation_complete(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def receive_cancel_single_workflow(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle single workflow cancellation request."""
        if self._accepting_requests:
            return await self._cancellation_handler.handle_cancel_single_workflow(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def state_sync(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle state sync request from peer gate."""
        if self._accepting_requests:
            return await self._state_sync_handler.handle_state_sync_request(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def lease_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle lease transfer during gate scaling."""
        if self._accepting_requests:
            return await self._state_sync_handler.handle_lease_transfer(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def job_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle a datacenter's final result for a job from its manager.

        One datacenter's result is not the job's: the client gets the
        global result, once every datacenter reported (``_complete_job``).
        Forwarded to a peer gate's client callback, it was taken for the
        whole job -- the message a gateless manager sends for its one
        datacenter.
        """
        if self._accepting_requests:
            return await self._state_sync_handler.handle_job_final_result(
                addr,
                data,
                self._complete_job,
                self.handle_exception,
                self._forward_job_final_result_to_peers,
            )
        return b"error"

    @tcp.receive()
    async def job_final_result_forwarded(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """A datacenter's final result a peer gate forwarded: applied here
        if this gate leads the job, and never forwarded again."""
        if self._accepting_requests:
            return await self._state_sync_handler.handle_job_final_result(
                addr,
                data,
                self._complete_job,
                self.handle_exception,
                None,
            )
        return b"error"

    @tcp.receive()
    async def receive_job_progress_report(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Receive progress report from manager (AD-34 multi-DC coordination)."""
        try:
            report = JobProgressReport.load(data)
            job = self._job_manager.get_job(report.job_id)
            if job and job.status in (
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
            ):
                await self._udp_logger.log(
                    ServerInfo(
                        message=(
                            "Discarding progress report for terminal job "
                            f"{report.job_id} (status={job.status})"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return b"ok"

            await self._job_timeout_tracker.record_progress(report)
            return b"ok"
        except Exception as error:
            await self.handle_exception(error, "receive_job_progress_report")
            return b""

    @tcp.receive()
    async def receive_job_timeout_report(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Receive DC-local timeout report from manager (AD-34 multi-DC coordination)."""
        try:
            report = JobTimeoutReport.load(data)
            await self._job_timeout_tracker.record_timeout(report)
            return b"ok"
        except Exception as error:
            await self.handle_exception(error, "receive_job_timeout_report")
            return b""

    @tcp.receive()
    async def receive_job_leader_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Receive manager leader transfer notification (AD-34 multi-DC coordination)."""
        try:
            report = JobLeaderTransfer.load(data)
            await self._job_timeout_tracker.record_leader_transfer(report)
            return b"ok"
        except Exception as error:
            await self.handle_exception(error, "receive_job_leader_transfer")
            return b""

    @tcp.receive()
    async def receive_job_final_status(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Receive final job status from manager (AD-34 lifecycle cleanup)."""
        try:
            report = JobFinalStatus.load(data)
            dedup_key = (report.job_id, report.datacenter)
            if dedup_key in self._job_final_statuses:
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            "Duplicate final status ignored for job "
                            f"{report.job_id} from DC {report.datacenter}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )
                return b"ok"

            self._job_final_statuses[dedup_key] = report.timestamp
            await self._job_timeout_tracker.handle_final_status(report)
            return b"ok"
        except Exception as error:
            await self.handle_exception(error, "receive_job_final_status")
            return b""

    @tcp.receive()
    async def workflow_result_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle workflow result push from manager."""
        try:
            push = WorkflowResultPush.load(data)
            callback = self._resolve_job_callback(push.job_id, push.callback_addr)

            stale_response = await self._validate_workflow_result_producer(push)
            if stale_response is not None:
                return stale_response

            if callback is not None:
                push.callback_addr = callback
                self._record_job_callback(push.job_id, callback)

            # Note: ``push.fence_token`` is the manager-side fence at
            # dispatch time, not a gate-leadership claim. Applying the
            # gate's per-job fence (which is bumped by orphan takeover —
            # see ``_commit_gate_job_leadership_takeover``) to reject
            # this push would silently drop legitimate completed work
            # whenever a takeover landed mid-flight before the manager
            # learned the new fence via ``job_leader_gate_transfer``.
            # Gate-leadership fencing belongs on gate-leader claims
            # (``GateJobReplica``, ``JobLeaderGateTransfer``), not on
            # manager-originated data-plane results.

            # Data-plane idempotency: a workflow has one aggregate. A push
            # for a workflow already claimed for it is acked without
            # re-aggregating or re-delivering (re-checked under the results
            # lock below, where the claim is made).
            if (push.job_id, push.workflow_id) in self._finalized_workflow_results:
                return b"ok"

            if push.is_client_ready:
                if callback is None:
                    return b"no_callback"
                delivered = await self._record_and_send_client_update(
                    push.job_id,
                    callback,
                    "workflow_result_push",
                    push.dump(),
                    timeout=self._tcp_timeout_standard,
                )
                return b"ok" if delivered else b"error"

            if not self._job_manager.has_job(push.job_id):
                if callback is None:
                    await self._udp_logger.log(
                        ServerWarning(
                            message=(
                                "Rejecting workflow result for unknown job "
                                f"{push.job_id}: no callback metadata"
                            ),
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        ),
                    )
                    return b"no_callback"
                self._recover_job_from_workflow_push(push, callback)

            await self._udp_logger.log(
                ServerDebug(
                    message=f"Received workflow result for {push.job_id}:{push.workflow_id} from DC {push.datacenter}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

            workflow_results: dict[str, WorkflowResultPush] = {}
            timeout_token: str | None = None
            should_schedule_timeout = False
            state_updated = False
            target_dcs_from_push = self._get_workflow_push_target_dcs(push)
            expected_dc_count = self._get_workflow_push_expected_dc_count(
                push,
                target_dcs_from_push,
            )

            async with self._workflow_dc_results_lock:
                if (push.job_id, push.workflow_id) in self._finalized_workflow_results:
                    return b"ok"
                # This gate's own placement of the job decides whose results
                # count. A push names the datacenters its manager was sent
                # the job for -- not where a fallback or a failover moved
                # it -- so it only seeds a gate that holds no placement of
                # the job (a survivor, recovering it from the push).
                if target_dcs_from_push and not self._job_manager.get_target_dcs(push.job_id):
                    self._job_manager.set_target_dcs(push.job_id, target_dcs_from_push)
                expected_dcs = self._job_manager.expected_workflow_datacenters(
                    push.job_id, push.workflow_id
                )
                if expected_dcs and push.datacenter not in expected_dcs:
                    # A datacenter the job moved off (lost, or released at
                    # dispatch), or a replacement re-running a workflow only
                    # for the context its dependents read: acked, not counted.
                    await self._udp_logger.log(
                        ServerDebug(
                            message=(
                                f"Dropped result of workflow {push.workflow_id} of job "
                                f"{push.job_id} from DC {push.datacenter}: its result "
                                f"comes from {sorted(expected_dcs)}"
                            ),
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        ),
                    )
                    return b"ok"
                if expected_dc_count > 0:
                    expected_counts = self._workflow_result_expected_dc_counts.setdefault(
                        push.job_id,
                        {},
                    )
                    previous_expected_count = expected_counts.get(push.workflow_id, 0)
                    if expected_dc_count > previous_expected_count:
                        expected_counts[push.workflow_id] = expected_dc_count

                if push.job_id not in self._workflow_dc_results:
                    self._workflow_dc_results[push.job_id] = {}
                if push.workflow_id not in self._workflow_dc_results[push.job_id]:
                    self._workflow_dc_results[push.job_id][push.workflow_id] = {}
                existing_result = self._workflow_dc_results[push.job_id][
                    push.workflow_id
                ].get(push.datacenter)
                if existing_result != push:
                    self._workflow_dc_results[push.job_id][push.workflow_id][
                        push.datacenter
                    ] = push
                    state_updated = True

                received_dcs = set(
                    self._workflow_dc_results[push.job_id][push.workflow_id].keys()
                )
                should_aggregate = bool(expected_dcs and received_dcs >= expected_dcs)
                if not should_aggregate and not expected_dcs and expected_dc_count > 0:
                    should_aggregate = len(received_dcs) >= expected_dc_count

                has_timeout = (
                    push.job_id in self._workflow_result_timeout_tokens
                    and push.workflow_id
                    in self._workflow_result_timeout_tokens[push.job_id]
                )

                if should_aggregate:
                    workflow_results, timeout_token = self._pop_workflow_results_locked(
                        push.job_id, push.workflow_id
                    )
                    if expected_dcs:
                        # Stored before the job moved off its datacenter.
                        workflow_results = {
                            datacenter: result
                            for datacenter, result in workflow_results.items()
                            if datacenter in expected_dcs
                        }
                elif (expected_dcs or expected_dc_count > 1) and not has_timeout:
                    should_schedule_timeout = True

            if state_updated:
                self._increment_version()

            if should_schedule_timeout:
                await self._schedule_workflow_result_timeout(
                    push.job_id, push.workflow_id
                )

            if timeout_token:
                await self._cancel_workflow_result_timeout(timeout_token)

            if workflow_results:
                # Accepted once recorded: the aggregate reaches the client
                # now or on its callback's (re)registration. Answered
                # "error", the manager re-sent its result, which came back
                # as a fresh partial -- the other datacenters' results were
                # taken -- and the timeout then delivered a second, wrong
                # aggregate calling them missing.
                await self._forward_aggregated_workflow_result(
                    push.job_id, push.workflow_id, workflow_results
                )
                return b"ok"

            return b"stored"

        except Exception as error:
            await self.handle_exception(error, "workflow_result_push")
            return b"error"

    @tcp.receive()
    async def register_callback(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle client callback registration for job reconnection."""
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit_for_operation(
                client_id, "reconnect"
            )
            if not allowed:
                return RateLimitResponse(
                    operation="reconnect",
                    retry_after_seconds=retry_after,
                ).dump()

            request = RegisterCallback.load(data)
            job_id = request.job_id

            job = self._job_manager.get_job(job_id)
            if not job:
                response = RegisterCallbackResponse(
                    job_id=job_id,
                    success=False,
                    error="Job not found",
                )
                return response.dump()

            self._record_job_callback(job_id, request.callback_addr)

            last_sequence = request.last_sequence
            if last_sequence <= 0:
                last_sequence = await self._modular_state.get_client_update_position(
                    job_id,
                    request.callback_addr,
                )

            await self._replay_job_status_to_callback(
                job_id,
                request.callback_addr,
                last_sequence,
            )

            elapsed = self._clock.monotonic() - job.timestamp if job.timestamp > 0 else 0.0

            await self._udp_logger.log(
                ServerInfo(
                    message=f"Client reconnected for job {job_id}, registered callback {request.callback_addr}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

            response = RegisterCallbackResponse(
                job_id=job_id,
                success=True,
                status=job.status,
                total_completed=job.total_completed,
                total_failed=job.total_failed,
                elapsed_seconds=elapsed,
            )

            return response.dump()

        except Exception as error:
            await self.handle_exception(error, "register_callback")
            return b"error"

    @tcp.receive()
    async def workflow_query(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle workflow status query from client."""
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit_for_operation(
                client_id, "workflow_query"
            )
            if not allowed:
                return RateLimitResponse(
                    operation="workflow_query",
                    retry_after_seconds=retry_after,
                ).dump()

            request = WorkflowQueryRequest.load(data)
            dc_results = await self._query_all_datacenters(request)

            datacenters = [
                DatacenterWorkflowStatus(dc_id=dc_id, workflows=workflows)
                for dc_id, workflows in dc_results.items()
            ]

            response = GateWorkflowQueryResponse(
                request_id=request.request_id,
                gate_id=self._node_id.full,
                datacenters=datacenters,
            )

            return response.dump()

        except Exception as error:
            await self.handle_exception(error, "workflow_query")
            return b"error"

    @tcp.receive()
    async def datacenter_list(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle datacenter list request from client."""
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit_for_operation(
                client_id, "datacenter_list"
            )
            if not allowed:
                return RateLimitResponse(
                    operation="datacenter_list",
                    retry_after_seconds=retry_after,
                ).dump()

            request = DatacenterListRequest.load(data)

            datacenters: list[DatacenterInfo] = []
            total_available_cores = 0
            healthy_datacenter_count = 0

            for dc_id in self._datacenter_managers.keys():
                status = self._classify_datacenter_health(dc_id)

                leader_addr: tuple[str, int] | None = None
                manager_statuses = self._modular_state.get_datacenter_manager_statuses(dc_id)
                for manager_addr, heartbeat in manager_statuses.items():
                    if heartbeat.is_leader:
                        leader_addr = (heartbeat.tcp_host, heartbeat.tcp_port)
                        break

                datacenters.append(
                    DatacenterInfo(
                        dc_id=dc_id,
                        health=status.health,
                        leader_addr=leader_addr,
                        available_cores=status.available_capacity,
                        manager_count=status.manager_count,
                        worker_count=status.worker_count,
                        resources=self._health_coordinator.datacenter_resource_view(dc_id),
                    )
                )

                total_available_cores += status.available_capacity
                if status.health == DatacenterHealth.HEALTHY.value:
                    healthy_datacenter_count += 1

            response = DatacenterListResponse(
                request_id=request.request_id,
                gate_id=self._node_id.full,
                datacenters=datacenters,
                total_available_cores=total_available_cores,
                healthy_datacenter_count=healthy_datacenter_count,
            )

            return response.dump()

        except Exception as error:
            await self.handle_exception(error, "datacenter_list")
            return b"error"

    @tcp.receive()
    async def gate_job_replica_prepare(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Receive a prepare for the 2PC gate-job-replication protocol."""
        if not self._accepting_requests:
            return b"error"
        try:
            return await self._replication_coordinator.handle_prepare(data)
        except Exception as error:
            await self.handle_exception(error, "gate_job_replica_prepare")
            return b"error"

    @tcp.receive()
    async def gate_job_replica_commit(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Promote a prepared replica into committed gate-job state."""
        if not self._accepting_requests:
            return b"error"
        try:
            return await self._replication_coordinator.handle_commit(data)
        except Exception as error:
            await self.handle_exception(error, "gate_job_replica_commit")
            return b"error"

    @tcp.receive()
    async def gate_job_replica_abort(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Drop a prepared replica on leader-side quorum failure."""
        if not self._accepting_requests:
            return b"error"
        try:
            return await self._replication_coordinator.handle_abort(data)
        except Exception as error:
            await self.handle_exception(error, "gate_job_replica_abort")
            return b"error"

    @tcp.receive()
    async def gate_job_replica_fetch(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Return the cached committed replica for orphan-state-repair."""
        if not self._accepting_requests:
            return b"error"
        try:
            return await self._replication_coordinator.handle_fetch(data)
        except Exception as error:
            await self.handle_exception(error, "gate_job_replica_fetch")
            return b"error"

    @tcp.receive()
    async def job_leadership_announcement(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job leadership announcement from peer gate."""
        try:
            announcement = JobLeadershipAnnouncement.load(data)

            accepted = self._job_leadership_tracker.process_leadership_claim(
                job_id=announcement.job_id,
                claimer_id=announcement.leader_id,
                claimer_addr=(announcement.leader_host, announcement.leader_tcp_port),
                fencing_token=announcement.fence_token or announcement.term,
                metadata=announcement.target_dc_count or announcement.workflow_count,
            )

            if accepted:
                callback = self._normalize_callback_addr(announcement.callback_addr)
                if callback is not None:
                    self._record_job_callback(announcement.job_id, callback)
                self._orphan_job_coordinator.clear_orphaned_job(announcement.job_id)

                await self._udp_logger.log(
                    ServerDebug(
                        message=f"Recorded job {announcement.job_id[:8]}... leader: {announcement.leader_id[:8]}...",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )

            return JobLeadershipAck(
                job_id=announcement.job_id,
                accepted=accepted,
                responder_id=self._node_id.full,
            ).dump()

        except Exception as error:
            await self.handle_exception(error, "job_leadership_announcement")
            return JobLeadershipAck(
                job_id="unknown",
                accepted=False,
                responder_id=self._node_id.full,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def dc_leader_announcement(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle DC leader announcement from peer gate."""
        try:
            announcement = DCLeaderAnnouncement.load(data)

            updated = self._dc_health_monitor.update_leader(
                datacenter=announcement.datacenter,
                leader_udp_addr=announcement.leader_udp_addr,
                leader_tcp_addr=announcement.leader_tcp_addr,
                leader_node_id=announcement.leader_node_id,
                leader_term=announcement.term,
            )

            if updated:
                await self._udp_logger.log(
                    ServerDebug(
                        message=(
                            f"Updated DC {announcement.datacenter} leader from peer: "
                            f"{announcement.leader_node_id[:8]}... (term {announcement.term})"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

            return b"ok"

        except Exception as error:
            await self.handle_exception(error, "dc_leader_announcement")
            return b"error"

    @tcp.receive()
    async def job_leader_manager_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job leadership manager transfer notification from manager (AD-31)."""
        try:
            transfer = JobLeaderManagerTransfer.load(data)

            job_known = (
                bool(self._modular_state.get_job_dc_managers(transfer.job_id))
                or transfer.job_id in self._job_leadership_tracker
            )
            if not job_known:
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            "Received manager transfer for unknown job "
                            f"{transfer.job_id[:8]}... from {transfer.new_manager_id[:8]}..."
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )
                return JobLeaderManagerTransferAck(
                    job_id=transfer.job_id,
                    gate_id=self._node_id.full,
                    accepted=False,
                ).dump()

            old_manager_addr = self._job_leadership_tracker.get_dc_manager(
                transfer.job_id, transfer.datacenter_id
            )
            if old_manager_addr is None:
                old_manager_addr = self._modular_state.get_job_dc_managers(
                    transfer.job_id
                ).get(transfer.datacenter_id)

            accepted = await self._job_leadership_tracker.update_dc_manager_async(
                job_id=transfer.job_id,
                dc_id=transfer.datacenter_id,
                manager_id=transfer.new_manager_id,
                manager_addr=transfer.new_manager_addr,
                fencing_token=transfer.fence_token,
            )

            if not accepted:
                current_fence = (
                    self._job_leadership_tracker.get_dc_manager_fencing_token(
                        transfer.job_id, transfer.datacenter_id
                    )
                )
                await self._udp_logger.log(
                    ServerDebug(
                        message=(
                            "Rejected stale manager transfer for job "
                            f"{transfer.job_id[:8]}... "
                            f"(fence {transfer.fence_token} <= {current_fence})"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )
                return JobLeaderManagerTransferAck(
                    job_id=transfer.job_id,
                    gate_id=self._node_id.full,
                    accepted=False,
                ).dump()

            self._modular_state.set_job_dc_manager(
                transfer.job_id, transfer.datacenter_id, transfer.new_manager_addr
            )

            await self._udp_logger.log(
                ServerInfo(
                    message=(
                        f"Updated job {transfer.job_id[:8]}... DC {transfer.datacenter_id} manager: "
                        f"{old_manager_addr} -> {transfer.new_manager_addr}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

            callback = self._modular_state._progress_callbacks.get(transfer.job_id)
            if callback:
                manager_transfer = ManagerJobLeaderTransfer(
                    job_id=transfer.job_id,
                    new_manager_id=transfer.new_manager_id,
                    new_manager_addr=transfer.new_manager_addr,
                    fence_token=transfer.fence_token,
                    datacenter_id=transfer.datacenter_id,
                    old_manager_id=transfer.old_manager_id,
                    old_manager_addr=old_manager_addr,
                )
                payload = manager_transfer.dump()
                delivered = await self._record_and_send_client_update(
                    transfer.job_id,
                    callback,
                    "receive_manager_job_leader_transfer",
                    payload,
                    timeout=self._tcp_timeout_standard,
                    log_failure=False,
                )
                if not delivered:
                    await self._udp_logger.log(
                        ServerWarning(
                            message=(
                                "Failed to deliver manager leader transfer to "
                                f"client {callback} for job {transfer.job_id[:8]}..."
                            ),
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            return JobLeaderManagerTransferAck(
                job_id=transfer.job_id,
                gate_id=self._node_id.full,
                accepted=True,
            ).dump()

        except Exception as error:
            await self.handle_exception(error, "job_leader_manager_transfer")
            return JobLeaderManagerTransferAck(
                job_id="unknown",
                gate_id=self._node_id.full,
                accepted=False,
            ).dump()

    @tcp.receive()
    async def job_leader_gate_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job leader gate transfer notification from peer gate."""
        if self._accepting_requests:
            return await self._job_handler.handle_job_leader_gate_transfer(addr, data)
        return JobLeaderGateTransferAck(
            job_id="unknown",
            manager_id=self._node_id.full,
            accepted=False,
        ).dump()

    @tcp.receive()
    async def windowed_stats_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle windowed stats push from Manager."""
        try:
            push: WindowedStatsPush = cloudpickle.loads(data)

            if not self._job_manager.has_job(push.job_id):
                await self._udp_logger.log(
                    ServerDebug(
                        message=(
                            "Discarding windowed stats for unknown job "
                            f"{push.job_id} from DC {push.datacenter}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )
                return b"discarded"

            job = self._job_manager.get_job(push.job_id)
            terminal_states = {
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
                JobStatus.TIMEOUT.value,
            }

            if not job or job.status in terminal_states:
                status = job.status if job else "missing"
                await self._udp_logger.log(
                    ServerDebug(
                        message=(
                            "Discarding windowed stats for job "
                            f"{push.job_id} in terminal state {status} "
                            f"from DC {push.datacenter}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )
                return b"discarded"

            for worker_stat in push.per_worker_stats:
                progress = WorkflowProgress(
                    job_id=push.job_id,
                    workflow_id=push.workflow_id,
                    workflow_name=push.workflow_name,
                    status="running",
                    completed_count=worker_stat.completed_count,
                    failed_count=worker_stat.failed_count,
                    rate_per_second=worker_stat.rate_per_second,
                    elapsed_seconds=push.window_end - push.window_start,
                    step_stats=worker_stat.step_stats,
                    avg_cpu_percent=worker_stat.avg_cpu_percent,
                    avg_memory_mb=worker_stat.avg_memory_mb,
                    collected_at=(push.window_start + push.window_end) / 2,
                )
                worker_key = f"{push.datacenter}:{worker_stat.worker_id}"
                await self._windowed_stats.add_progress(worker_key, progress)

            return b"ok"

        except Exception as error:
            await self.handle_exception(error, "windowed_stats_push")
            return b"error"

    @tcp.receive()
    async def job_status_push_forward(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle forwarded job status push from peer gate."""
        try:
            push = JobStatusPush.load(data)
            job_id = push.job_id

            callback = self._resolve_job_callback(job_id, push.callback_addr)
            if not callback:
                return b"no_callback"
            push.callback_addr = callback
            # One datacenter's terminal status is not the job's when the
            # job runs in several: the gate's global result (all DCs, or
            # the AD-44 best-effort decision) is. Relayed as final, it
            # ended the client's wait on the first DC to finish.
            if push.is_final and (
                datacenter_count := len(self._job_manager.get_target_dcs(job_id))
            ) > 1:
                push.is_final = False
                push.status = JobStatus.RUNNING.value
                push.message = (
                    f"{push.message} (one of {datacenter_count} datacenters; "
                    "the job continues)"
                )
            self._record_job_callback(job_id, callback)
            data = push.dump()

            delivered = await self._record_and_send_client_update(
                job_id,
                callback,
                "job_status_push",
                data,
                timeout=self._tcp_timeout_standard,
            )
            return b"ok" if delivered else b"error"

        except Exception as error:
            await self.handle_exception(error, "job_status_push_forward")
            return b"error"

    # =========================================================================
    # Helper Methods (Required by Handlers and Coordinators)
    # =========================================================================

    async def _send_tcp(
        self,
        addr: tuple[str, int],
        message_type: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[bytes | None, float]:
        """Send TCP message and return response."""
        return await self.send_tcp(addr, message_type, data, timeout=timeout)

    async def _deliver_client_update(
        self,
        job_id: str,
        callback: tuple[str, int],
        sequence: int,
        message_type: str,
        payload: bytes,
        timeout: float = 5.0,
        log_failure: bool = True,
    ) -> bool:
        last_error: Exception | None = None
        for attempt in range(GateStatsCoordinator.CALLBACK_PUSH_MAX_RETRIES):
            try:
                response, _ = await self._send_tcp(
                    callback,
                    message_type,
                    payload,
                    timeout=timeout,
                )
                if isinstance(response, Exception):
                    raise response
                if response not in (b"ok", None):
                    raise RuntimeError(
                        f"{message_type} rejected with {response!r}"
                    )
                await self._modular_state.set_client_update_position(
                    job_id,
                    callback,
                    sequence,
                )
                return True
            except Exception as error:
                last_error = error
                if attempt < GateStatsCoordinator.CALLBACK_PUSH_MAX_RETRIES - 1:
                    delay = min(
                        GateStatsCoordinator.CALLBACK_PUSH_BASE_DELAY_SECONDS
                        * (2**attempt),
                        GateStatsCoordinator.CALLBACK_PUSH_MAX_DELAY_SECONDS,
                    )
                    await self._clock.sleep(delay)

        if log_failure:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Failed to deliver {message_type} for job {job_id[:8]}... "
                        f"after {GateStatsCoordinator.CALLBACK_PUSH_MAX_RETRIES} retries: "
                        f"{last_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        return False

    async def _record_and_send_client_update(
        self,
        job_id: str,
        callback: tuple[str, int],
        message_type: str,
        payload: bytes,
        timeout: float = 5.0,
        log_failure: bool = True,
    ) -> bool:
        sequence = await self._modular_state.record_client_update(
            job_id,
            message_type,
            payload,
            self._clock.monotonic(),
        )
        return await self._deliver_client_update(
            job_id,
            callback,
            sequence,
            message_type,
            payload,
            timeout=timeout,
            log_failure=log_failure,
        )

    async def _confirm_peer(self, peer_addr: tuple[str, int]) -> None:
        """Confirm a peer via SWIM (AD-29 UNCONFIRMED→OK).

        ``HealthAwareServer.confirm_peer`` is a coroutine; the previous
        sync wrapper invoked it without awaiting, silently dropping the
        coroutine and leaving the peer's incarnation tracker entry in
        the UNCONFIRMED state — which then causes
        ``IncarnationTracker.can_suspect_node`` to refuse suspicion of
        the peer (AD-29 §"Task 12.3.4: UNCONFIRMED→SUSPECT forbidden").
        Detection of a genuinely-failed peer is silently delayed by
        the entire suspicion bracket budget while the SWIM layer
        retries from scratch through whatever path eventually
        confirms the peer.
        """
        await self.confirm_peer(peer_addr)

    async def _persist_accepted_job_durable(
        self,
        submission: "JobSubmission",
        successful_dcs: list[str],
        fence_token: int,
    ) -> None:
        """Durably record an accepted job (Phase 8 gate durable tier).

        Awaited from the dispatch coordinator at the acceptance point
        (after at least one datacenter took the dispatch), at the gate
        tier's AD-38 level for job creation (GLOBAL where the tier spans
        regions). No-op for volatile gates.
        """
        if self._job_ledger is None:
            return

        callback_addr = submission.callback_addr or self._job_manager.get_callback(
            submission.job_id
        )
        requestor_contact = (
            f"{callback_addr[0]}:{callback_addr[1]}" if callback_addr else ""
        )
        _ledger_job_id, create_result = await self._job_ledger.create_job(
            spec_hash=hashlib.sha256(submission.workflows).digest(),
            assigned_datacenters=tuple(successful_dcs),
            requestor_id=requestor_contact,
            durability=await self._ledger_target_durability(),
            job_id=submission.job_id,
            timeout_seconds=submission.timeout_seconds,
        )
        # A shortfall means the record IS durable here and applied to
        # ledger state, just not replicated that far: a durability
        # warning, not a reason to abort acceptance of a job that exists.
        await self._log_ledger_shortfall("JobCreated", submission.job_id, create_result)

    async def _record_cancellation_durable(
        self,
        job_id: str,
        reason: str,
        requester_id: str,
        confirmed_datacenters: list[tuple[str, int]],
    ) -> None:
        """Durably record a cancel the datacenters confirmed (AD-38).

        One ``JobCancellationRequested`` (the ledger ignores a repeat from
        a client retry) and one ``JobCancellationAcked`` per confirming
        datacenter (each datacenter acks once).
        """
        if self._job_ledger is None:
            return

        durability = await self._ledger_target_durability()
        await self._log_ledger_shortfall(
            "JobCancellationRequested",
            job_id,
            await self._job_ledger.request_cancellation(
                job_id,
                reason=reason,
                requestor_id=requester_id,
                durability=durability,
            ),
        )
        for datacenter_id, workflows_cancelled in confirmed_datacenters:
            await self._log_ledger_shortfall(
                "JobCancellationAcked",
                job_id,
                await self._job_ledger.acknowledge_cancellation(
                    job_id,
                    datacenter_id=datacenter_id,
                    workflows_cancelled=workflows_cancelled,
                    durability=durability,
                ),
            )

    async def _finalize_terminal_job(
        self,
        job_id: str,
        final_status: str,
        total_completed: int,
        total_failed: int,
        elapsed_seconds: float,
        reason: str = "",
        failed_datacenters: tuple[str, ...] = (),
    ) -> None:
        """Every terminal transition's single exit: record the AD-38
        terminal, then tell peer gates the job is terminal (which also
        retires its per-job Raft group everywhere). Order matters: the
        terminal record commits through that group first."""
        await self._record_job_terminal_durable(
            job_id,
            final_status=final_status,
            total_completed=total_completed,
            total_failed=total_failed,
            elapsed_seconds=elapsed_seconds,
            reason=reason,
            failed_datacenters=failed_datacenters,
        )
        await self._replicate_terminal_status(job_id)

    async def _finalize_failed_job(
        self,
        job_id: str,
        failed_datacenters: tuple[str, ...],
        reason: str,
    ) -> None:
        """Terminal hook for coordinator-side FAILED transitions."""
        job = self._job_manager.get_job(job_id)
        await self._finalize_terminal_job(
            job_id,
            final_status=JobStatus.FAILED.value,
            total_completed=getattr(job, "total_completed", 0),
            total_failed=getattr(job, "total_failed", 0),
            elapsed_seconds=getattr(job, "elapsed_seconds", 0.0),
            reason=reason,
            failed_datacenters=failed_datacenters,
        )

    async def _replicate_terminal_status(self, job_id: str) -> None:
        """Re-commit the job's replica to peer gates with its terminal status.

        Peers learned the job through the committed replica and never
        heard it end: their copy stayed SUBMITTED forever, so their
        cleanup sweep (terminal jobs only) never removed it and its Raft
        group never retired. The terminal status goes out as a revision of
        the committed replica, reusing the protocol that created it.
        """
        job = self._job_manager.get_job(job_id)
        if job is None or self._replication_coordinator.get_committed_replica(job_id) is None:
            return

        replicated = await self._replication_coordinator.revise_committed_replica(
            job_id,
            lambda committed: (
                None
                if committed.status_seed == job.status
                else dataclasses.replace(committed, status_seed=job.status)
            ),
            peer_addrs=list(self._modular_state.get_active_peers_list()),
            quorum_size=self._quorum_size(),
        )
        if not replicated:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Terminal status {job.status} for job {job_id[:8]}... "
                        "did not reach a quorum of peer gates"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _record_job_terminal_durable(
        self,
        job_id: str,
        final_status: str,
        total_completed: int,
        total_failed: int,
        elapsed_seconds: float,
        reason: str = "",
        failed_datacenters: tuple[str, ...] = (),
    ) -> None:
        """Durably record a job's terminal outcome (idempotent).

        Timeouts and failures get their AD-38 event types (``JobTimedOut``
        with the timeout reason, ``JobFailed`` naming the failed
        datacenters); everything else is ``JobCompleted``.
        """
        if self._job_ledger is None:
            return
        ledger_job = self._job_ledger.get_job(job_id)
        if ledger_job is None or ledger_job.is_terminal:
            return

        duration_ms = int(elapsed_seconds * 1000)
        durability = await self._ledger_target_durability()
        if final_status in (JobStatus.TIMEOUT.value, "timed_out"):
            await self._log_ledger_shortfall(
                "JobTimedOut",
                job_id,
                await self._job_ledger.time_out_job(
                    job_id,
                    timeout_type=reason or final_status,
                    total_completed=total_completed,
                    total_failed=total_failed,
                    duration_ms=duration_ms,
                    durability=durability,
                ),
            )
            return

        if final_status == JobStatus.FAILED.value:
            await self._log_ledger_shortfall(
                "JobFailed",
                job_id,
                await self._job_ledger.fail_job(
                    job_id,
                    error_message=reason or "job failed",
                    failed_datacenter=",".join(failed_datacenters),
                    total_completed=total_completed,
                    total_failed=total_failed,
                    duration_ms=duration_ms,
                    durability=durability,
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
                durability=durability,
            ),
        )

    async def _replicate_ledger_regional(self, entry: WALEntry) -> bool:
        """Ledger REGIONAL replicator: commit in the job's gate group."""
        if self._ledger_replicator is None:
            return False
        return await self._ledger_replicator.replicate(entry)

    async def _replicate_ledger_global(self, entry: WALEntry) -> bool:
        """Ledger GLOBAL replicator: holders span two regions."""
        return await self._ledger_region_span.replicate(entry)

    async def _ledger_target_durability(self) -> DurabilityLevel:
        """AD-38 level for the gate's job records: GLOBAL where the gate
        tier spans two or more regions, else REGIONAL (GLOBAL cannot be
        met by a single-region tier). Logs each change of that answer
        once rather than one shortfall per job."""
        spans_regions = self._ledger_region_span.tier_spans_regions()
        if spans_regions != self._ledger_tier_spans_regions:
            self._ledger_tier_spans_regions = spans_regions
            await self._udp_logger.log(
                ServerInfo(
                    message=(
                        "Gate ledger records are GLOBAL: the gate tier spans "
                        f"regions {sorted(self._gate_tier_regions())}"
                        if spans_regions
                        else "Gate ledger records are REGIONAL: GLOBAL needs gates "
                        f"in two regions, the tier spans {sorted(self._gate_tier_regions())}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        return DurabilityLevel.GLOBAL if spans_regions else DurabilityLevel.REGIONAL

    def _gate_region_of(self, gate_id: str) -> str | None:
        """Region (datacenter identity) of a gate by its full node id."""
        if gate_id == self._node_id.full:
            return self._node_id.datacenter
        gate_info = self._modular_state.get_known_gate(gate_id)
        return gate_info.datacenter if gate_info is not None else None

    def _gate_tier_regions(self) -> set[str]:
        """Regions this gate knows the gate tier to span (itself included)."""
        return {self._node_id.datacenter} | {
            gate_info.datacenter for _, gate_info in self._modular_state.iter_known_gates()
        }

    async def _send_to_gate_peer(
        self,
        addr: tuple[str, int],
        method: str,
        data: bytes,
        timeout: float,
    ) -> bytes | Exception | None:
        """send_tcp returning only the reply (or the transport error)."""
        response, _clock = await self.send_tcp(addr, method, data, timeout=timeout)
        return response

    async def _record_datacenter_reassignment_durable(
        self,
        job_id: str,
        substitution: DatacenterSubstitution,
    ) -> None:
        """Durably record that the job moved off a datacenter it lost
        mid-run (AD-38 ``JobDatacenterReassigned``, AD-36), at the gate
        tier's level for the job's record: a gate recovering the job from
        its ledger awaits its results where it runs now. No-op for
        volatile gates."""
        if self._job_ledger is None:
            return
        await self._log_ledger_shortfall(
            "JobDatacenterReassigned",
            job_id,
            await self._job_ledger.reassign_datacenter(
                job_id,
                DatacenterReassignment(
                    lost_datacenter=substitution.lost_datacenter,
                    replacement_datacenter=substitution.replacement_datacenter,
                    completed_workflow_ids=tuple(substitution.completed_workflow_ids),
                    total_completed=substitution.total_completed,
                    total_failed=substitution.total_failed,
                ),
                durability=await self._ledger_target_durability(),
            ),
        )

    async def _log_ledger_shortfall(
        self,
        event_name: str,
        job_id: str,
        result: CommitResult | None,
    ) -> None:
        """Log a job-ledger record that fell short of its requested level.

        The record is durable here and applied either way (the ledger's
        apply contract); ``None`` means the ledger appended nothing
        (unknown or already terminal job), which is not a shortfall.
        """
        if result is None or result.success:
            return
        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"Gate job ledger {event_name} for {job_id[:8]}... is "
                    f"{result.level_achieved.name}-durable only: {result.error}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _recover_durable_jobs(self) -> None:
        """Rebuild gate-side tracking for ledger-recovered jobs.

        For every NON-terminal job the WAL replay produced, the gate
        reconstructs enough state to (a) answer client status queries
        (the poll path — push registrations are connection state and
        die with the old process), (b) accept the completion the
        owning manager still OWES it (the manager's completion-notice
        obligation resends until this gate acks — the two halves of
        the durable story), and (c) resume AD-34 global-timeout
        tracking with the REMAINING budget, elapsed derived from the
        created HLC's embedded wall clock against the same clock axis.
        """
        recovered_count = 0
        current_wall_ms = self._hlc.now().wall_ms
        for job_state in self._job_ledger.get_all_jobs().values():
            if job_state.is_terminal:
                continue

            elapsed_seconds = max(
                (current_wall_ms - job_state.created_hlc.wall_ms)
                / 1000.0,
                0.0,
            )
            remaining_timeout = job_state.timeout_seconds - elapsed_seconds
            if job_state.timeout_seconds <= 0.0 or remaining_timeout <= 0.0:
                # One timeout check of grace. A pre-Phase-8 record with
                # no budget persisted is resolved loudly at the next check
                # rather than stranded silently; a budget that ran out
                # while this gate was down lets a completion already in
                # flight (the manager's owed notice) win the race before
                # the loud timeout fires.
                remaining_timeout = self.env.GATE_TIMEOUT_CHECK_INTERVAL

            job = GlobalJobStatus(
                job_id=job_state.job_id,
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=self._clock.monotonic() - elapsed_seconds,
                fence_token=job_state.fence_token,
            )
            self._job_manager.set_job(job_state.job_id, job)
            # Where the job runs now: a datacenter it lost mid-run was
            # replaced in its assigned datacenters (AD-36), keeping the
            # result slots of the workflows it delivered.
            self._job_manager.set_target_dcs(
                job_state.job_id, set(job_state.assigned_datacenters)
            )
            self._job_manager.set_datacenter_substitutions(
                job_state.job_id,
                [
                    DatacenterSubstitution(
                        lost_datacenter=reassignment.lost_datacenter,
                        replacement_datacenter=reassignment.replacement_datacenter,
                        completed_workflow_ids=list(reassignment.completed_workflow_ids),
                        total_completed=reassignment.total_completed,
                        total_failed=reassignment.total_failed,
                    )
                    for reassignment in job_state.datacenter_reassignments
                ],
            )
            self._job_manager.set_released_datacenters(
                job_state.job_id,
                {
                    reassignment.lost_datacenter
                    for reassignment in job_state.datacenter_reassignments
                },
            )
            self._job_manager.set_fence_token(
                job_state.job_id, job_state.fence_token
            )
            # Restore the client's push registration from the persisted
            # requestor contact — push registrations are connection
            # state and die with the old process, but the terminal
            # result the manager still owes this gate must reach the
            # CLIENT, not just the gate (measured: without this, gen-2
            # accepted the owed completion and pushed it into the void
            # while the client waited out its full budget).
            if job_state.requestor_id and ":" in job_state.requestor_id:
                callback_host, _, callback_port_text = (
                    job_state.requestor_id.rpartition(":")
                )
                if callback_port_text.isdigit():
                    self._job_manager.set_callback(
                        job_state.job_id,
                        (callback_host, int(callback_port_text)),
                    )
                else:
                    await self._udp_logger.log(
                        ServerWarning(
                            message=(
                                f"Recovered job {job_state.job_id[:8]}... has a "
                                f"malformed requestor contact "
                                f"{job_state.requestor_id!r}: its client is "
                                "reached only if it polls"
                            ),
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
            await self._job_timeout_tracker.start_tracking_job(
                job_id=job_state.job_id,
                timeout_seconds=remaining_timeout,
                target_dcs=list(job_state.assigned_datacenters),
            )
            recovered_count += 1

        if recovered_count:
            await self._udp_logger.log(
                ServerInfo(
                    message=(
                        f"Gate durable tier recovered {recovered_count} "
                        "in-flight job(s) from the ledger"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _complete_job(self, job_id: str, result: object) -> bool:
        """Complete a job and notify client."""
        if not isinstance(result, JobFinalResult):
            return False

        async with self._job_manager.lock_job(job_id):
            job = self._job_manager.get_job(job_id)
            if not job:
                # ``_udp_logger`` is the gate's logger; ``_logger`` has
                # never existed on this class, so BOTH cold branches of
                # this method (unknown job, duplicate terminal) raised
                # AttributeError instead of returning their verdict.
                # The handler's except turned that into ``b"error"``,
                # which the manager's completion-notice obligation
                # reads as "not delivered" — so a job whose terminal
                # HAD been applied kept getting resent up the backoff
                # ladder until the 1800s age ceiling dropped it loudly.
                # Reached for the first time by the gate durable-tier
                # restart scenario (the manager redelivers an owed
                # notice to a recovered gate that already applied it).
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            "Final result received for unknown job "
                            f"{job_id[:8]}...; skipping completion"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return False

            terminal_statuses = {
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
                JobStatus.TIMEOUT.value,
            }
            if job.status in terminal_statuses:
                await self._udp_logger.log(
                    ServerDebug(
                        message=(
                            "Duplicate final result for job "
                            f"{job_id[:8]}... ignored (status={job.status})"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return False

            previous_status = job.status

        global_result = await self._record_job_final_result(result)
        if global_result and await self._claim_job_completion(job_id):
            await self._finish_job_with_global_result(
                job_id, previous_status, global_result
            )

        return True

    async def _claim_job_completion(self, job_id: str) -> bool:
        """Claim the right to finish ``job_id``; True for exactly one caller."""
        async with self._job_manager.lock_job(job_id):
            if job_id in self._job_completion_claimed:
                return False
            self._job_completion_claimed.add(job_id)
            return True

    async def _finish_job_with_global_result(
        self,
        job_id: str,
        previous_status: str,
        global_result: GlobalJobResult,
    ) -> None:
        """Deliver a job's global result and make it terminal everywhere."""
        if global_result.unreported_datacenters:
            # AD-44: the per-workflow results that were waiting on the
            # datacenters this job completed without go out from those that
            # reported, ahead of the terminal result -- a client takes its
            # results from them.
            await self._release_workflow_results(job_id)

        await self._push_global_job_result(global_result)

        async with self._job_manager.lock_job(job_id):
            job = self._job_manager.get_job(job_id)
            if job:
                job.status = global_result.status
                job.total_completed = global_result.total_completed
                job.total_failed = global_result.total_failed
                job.completed_datacenters = global_result.successful_datacenters
                job.failed_datacenters = global_result.failed_datacenters
                job.errors = list(global_result.errors)
                job.elapsed_seconds = global_result.elapsed_seconds
                self._job_manager.set_job(job_id, job)

        self._handle_update_by_tier(
            job_id,
            previous_status,
            global_result.status,
            None,
        )

        await self._finalize_terminal_job(
            job_id,
            final_status=global_result.status,
            total_completed=global_result.total_completed,
            total_failed=global_result.total_failed,
            elapsed_seconds=global_result.elapsed_seconds,
            reason="; ".join(global_result.errors),
            failed_datacenters=tuple(
                sorted(
                    datacenter_id
                    for datacenter_id, datacenter_status in (
                        global_result.per_datacenter_statuses.items()
                    )
                    if datacenter_status == JobStatus.FAILED.value
                )
            ),
        )

        self._task_runner.run(
            self._dispatch_to_reporters,
            job_id,
            global_result,
        )

        await self._best_effort_manager.cleanup(job_id)
        if global_result.unreported_datacenters:
            # Cancelling may wait out unreachable datacenters (the likely
            # reason they never reported); it must not hold up the
            # completion this result delivers.
            self._task_runner.run(
                self._abandon_unreported_datacenters,
                job_id,
                list(global_result.unreported_datacenters),
                global_result.completion_reason,
            )

    async def _dispatch_to_reporters(
        self,
        job_id: str,
        global_result: GlobalJobResult,
    ) -> None:
        """
        Dispatch job results to configured reporters (Task 38).

        Creates reporter tasks for each configured reporter type
        and submits the results.
        """
        submission = self._modular_state._job_submissions.get(job_id)
        if not submission or not submission.reporting_configs:
            return

        try:
            # The client's payload: read through the restricted unpickler,
            # as its workflows are.
            reporter_configs = restricted_loads(submission.reporting_configs)
        except Exception as config_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to load reporter configs for job {job_id[:8]}...: {config_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return

        workflow_stats: WorkflowStats = {
            "workflow": job_id,
            "stats": {
                "total_completed": global_result.total_completed,
                "total_failed": global_result.total_failed,
                "successful_dcs": global_result.successful_datacenters,
                "failed_dcs": global_result.failed_datacenters,
            },
            "aps": global_result.total_completed
            / max(global_result.elapsed_seconds, 1.0),
            "elapsed": global_result.elapsed_seconds,
            "results": [],
        }

        # Each reporter in a run of its own: one that fails or hangs holds
        # up neither the job's completion nor any other reporter.
        for reporter_config in reporter_configs:
            reporter_type = getattr(reporter_config, "reporter_type", None)
            self._task_runner.run(
                self._submit_to_reporter,
                job_id,
                reporter_config,
                workflow_stats,
                reporter_type.name if reporter_type else "unknown",
            )

    async def _submit_to_reporter(
        self,
        job_id: str,
        reporter_config: object,
        workflow_stats: WorkflowStats,
        reporter_type_name: str,
    ) -> None:
        """
        Submit a job's results to one reporter: connect and submit within
        one ``REPORTER_SUBMISSION_TIMEOUT_SECONDS`` deadline, then close --
        however the submission ended -- within another. A failure or
        timeout is logged; it is the reporter's alone.

        A method, not a closure: the task runner keeps the first callable
        it is given under a name, so a closure over one job's id logged
        every later job's submissions under that first job.
        """
        deadline = self._clock.monotonic() + self._reporter_submission_timeout_seconds
        try:
            reporter = Reporter(reporter_config)
            try:
                # A connect that failed or timed out may have opened part of
                # its backend: it is closed all the same.
                await self._clock.wait_for(reporter.connect(), max(deadline - self._clock.monotonic(), 0.0))
                await self._clock.wait_for(
                    reporter.submit_workflow_results(workflow_stats),
                    max(deadline - self._clock.monotonic(), 0.0),
                )
            finally:
                # A deadline of its own: one spent by a hung submission
                # would cancel the close before it began.
                await self._clock.wait_for(reporter.close(), self._reporter_submission_timeout_seconds)
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Submitted results for job {job_id[:8]}... to {reporter_type_name}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        except Exception as submit_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to submit results for job {job_id[:8]}... to {reporter_type_name}: "
                    f"{type(submit_error).__name__}: {submit_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def handle_global_timeout(
        self,
        job_id: str,
        reason: str,
        target_dcs: list[str],
        manager_addrs: dict[str, tuple[str, int]],
    ) -> None:
        job = await self._mark_job_timeout(job_id, reason)
        if not job:
            await self._job_timeout_tracker.stop_tracking(job_id)
            return

        resolved_target_dcs = self._resolve_timeout_target_dcs(
            job_id, target_dcs, manager_addrs
        )
        await self._cancel_job_for_timeout(
            job_id,
            reason,
            resolved_target_dcs,
            manager_addrs,
        )
        timeout_result = self._build_timeout_global_result(
            job_id,
            job,
            resolved_target_dcs,
            reason,
        )
        await self._push_global_job_result(timeout_result)
        await self._job_timeout_tracker.stop_tracking(job_id)

    async def _mark_job_timeout(
        self,
        job_id: str,
        reason: str,
    ) -> GlobalJobStatus | None:
        async with self._job_manager.lock_job(job_id):
            job = self._job_manager.get_job(job_id)
            if not job:
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            f"Global timeout triggered for unknown job {job_id[:8]}..."
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return None

            terminal_statuses = {
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
                JobStatus.TIMEOUT.value,
            }
            if job.status in terminal_statuses:
                await self._udp_logger.log(
                    ServerInfo(
                        message=(
                            "Global timeout ignored for terminal job "
                            f"{job_id[:8]}... (status={job.status})"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return None

            aggregated = self._job_manager.aggregate_job_status(job_id)
            if aggregated is not None:
                job = aggregated

            job.status = JobStatus.TIMEOUT.value
            job.resolution_details = "global_timeout"
            reported_datacenters = self._job_manager.get_all_dc_results(job_id)
            errors = list(job.errors) + [
                f"{datacenter_id}: missing final result"
                for datacenter_id in sorted(self._job_manager.get_target_dcs(job_id))
                if datacenter_id not in reported_datacenters
            ]
            if reason and reason not in errors:
                errors.append(reason)
            job.errors = errors
            if job.timestamp > 0:
                job.elapsed_seconds = self._clock.monotonic() - job.timestamp

            self._job_manager.set_job(job_id, job)

        await self._finalize_terminal_job(
            job_id,
            final_status=JobStatus.TIMEOUT.value,
            total_completed=getattr(job, "total_completed", 0),
            total_failed=getattr(job, "total_failed", 0),
            elapsed_seconds=getattr(job, "elapsed_seconds", 0.0),
            reason=reason,
        )
        await self._modular_state.increment_state_version()
        await self._send_immediate_update(job_id, "timeout", None)
        return job

    def _resolve_timeout_target_dcs(
        self,
        job_id: str,
        target_dcs: list[str],
        manager_addrs: dict[str, tuple[str, int]],
    ) -> list[str]:
        resolved = list(target_dcs)
        if not resolved:
            resolved = list(self._job_manager.get_target_dcs(job_id))
        if not resolved:
            resolved = list(manager_addrs.keys())
        return resolved

    async def _cancel_job_for_timeout(
        self,
        job_id: str,
        reason: str,
        target_dcs: list[str],
        manager_addrs: dict[str, tuple[str, int]],
    ) -> None:
        if not target_dcs:
            return

        # Unfenced, as every cancel the gate forwards for a client is: a
        # manager checks ``fence_token`` against ITS lease fence for the
        # job, a counter the gate's lease fence (sent here before) has
        # nothing to do with, and an exact-match check refused almost every
        # timeout cancel -- the job ran on past its global timeout. A cancel
        # is fail-safe; no leadership epoch needs to authorize it.
        cancel_payload = CancelJob(
            job_id=job_id,
            reason=reason or "global_timeout",
            fence_token=0,
        ).dump()
        job_dc_managers = self._modular_state.get_job_dc_managers(job_id)
        errors: list[str] = []

        for dc_id in target_dcs:
            manager_addr = manager_addrs.get(dc_id) or job_dc_managers.get(dc_id)
            if not manager_addr:
                errors.append(f"No manager found for DC {dc_id}")
                continue

            try:
                response, _ = await self._send_tcp(
                    manager_addr,
                    "cancel_job",
                    cancel_payload,
                    timeout=self._tcp_timeout_standard,
                )
                # send_tcp returns transport errors rather than raising.
                if isinstance(response, Exception):
                    raise response
            except Exception as error:
                errors.append(f"DC {dc_id} cancel error: {error}")
                continue

            if not response:
                errors.append(f"No response from DC {dc_id}")
                continue

            try:
                ack = JobCancelResponse.load(response)
                if not ack.success:
                    errors.append(f"DC {dc_id} rejected cancellation: {ack.error}")
                continue
            except Exception as parse_error:
                await self._udp_logger.log(
                    ServerDebug(
                        message=(
                            f"JobCancelResponse parse failed for DC {dc_id}, "
                            f"falling back to CancelAck: {parse_error}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

            try:
                ack = CancelAck.load(response)
                if not ack.cancelled:
                    errors.append(f"DC {dc_id} rejected cancellation: {ack.error}")
            except Exception:
                errors.append(f"DC {dc_id} sent unrecognized cancel response")

        if errors:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        "Global timeout cancellation issues for job "
                        f"{job_id[:8]}...: {'; '.join(errors)}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _build_timeout_job_results(
        self,
        job_id: str,
        target_dcs: list[str],
        reason: str,
        elapsed_seconds: float,
    ) -> list[JobFinalResult]:
        existing_results = self._job_manager.get_all_dc_results(job_id)
        if not target_dcs:
            target_dcs = list(existing_results.keys())

        timeout_reason = reason or "Global timeout"
        fence_token = self._job_manager.get_fence_token(job_id)
        results: list[JobFinalResult] = []

        for dc_id in target_dcs:
            if dc_id in existing_results:
                results.append(existing_results[dc_id])
                continue

            results.append(
                JobFinalResult(
                    job_id=job_id,
                    datacenter=dc_id,
                    status="PARTIAL",
                    workflow_results=[],
                    total_completed=0,
                    total_failed=0,
                    errors=[timeout_reason],
                    elapsed_seconds=elapsed_seconds,
                    fence_token=fence_token,
                    **self._data_plane_provenance(job_id),
                )
            )

        return results

    def _build_timeout_global_result(
        self,
        job_id: str,
        job: GlobalJobStatus,
        target_dcs: list[str],
        reason: str,
    ) -> GlobalJobResult:
        elapsed_seconds = getattr(job, "elapsed_seconds", 0.0)
        per_dc_results = self._build_timeout_job_results(
            job_id,
            list(target_dcs),
            reason,
            elapsed_seconds,
        )
        total_completed = sum(result.total_completed for result in per_dc_results)
        total_failed = sum(result.total_failed for result in per_dc_results)
        errors: list[str] = []
        for result in per_dc_results:
            errors.extend(result.errors)
        if reason and reason not in errors:
            errors.append(reason)

        successful_dcs = sum(
            1
            for result in per_dc_results
            if result.status.lower() == JobStatus.COMPLETED.value
        )
        failed_dcs = len(per_dc_results) - successful_dcs

        aggregated = AggregatedJobStats(
            total_requests=total_completed + total_failed,
            successful_requests=total_completed,
            failed_requests=total_failed,
        )

        return GlobalJobResult(
            job_id=job_id,
            status=JobStatus.TIMEOUT.value,
            per_datacenter_results=per_dc_results,
            aggregated=aggregated,
            total_completed=total_completed,
            total_failed=total_failed,
            successful_datacenters=successful_dcs,
            failed_datacenters=failed_dcs,
            errors=errors,
            elapsed_seconds=elapsed_seconds,
        )

    async def _push_global_job_result(self, result: GlobalJobResult) -> None:
        """Deliver the aggregated global result to the submitting client.

        THE single definition. This method was defined twice in this
        class — an early ``(self, job_id, result)`` form and a later
        ``(self, result)`` form — and Python kept the later one, which
        meant two silent defects at once:

        * ``handle_global_timeout`` calls with ``(job_id, result)``,
          so the AD-34 global-timeout push raised ``TypeError`` — the
          loud terminal that exists for when everything else failed
          never reached the client.
        * the surviving body sent action ``"global_job_result"``,
          which NO endpoint implements; the client's receiver is
          ``receive_global_job_result`` (client.py), so the completion
          path's pushes went nowhere either.

        The merged body keeps the reachable wire action and the
        client-update recording from the shadowed definition, plus the
        ``_job_global_result_sent`` dedup bookkeeping the later one
        added (read by the aggregation guard, cleared on job cleanup).
        """
        job_id = result.job_id
        callback = self._job_manager.get_callback(job_id)
        if not callback:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Global result has no callback for job {job_id[:8]}..."
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return

        payload = result.dump()
        delivered = await self._record_and_send_client_update(
            job_id,
            callback,
            "receive_global_job_result",
            payload,
            timeout=self._tcp_timeout_standard,
            log_failure=False,
        )
        if delivered:
            self._job_global_result_sent.add(job_id)
            return

        await self._udp_logger.log(
            ServerWarning(
                message=(
                    f"Failed to deliver the global result of job {job_id[:8]}...: "
                    "it is replayed when its client's callback re-registers"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _answer_job_status_query(self, query: JobStatusQuery) -> bytes:
        """A status read at its consistency level (AD-38 Part 8). EVENTUAL
        reads, and any read of a terminal status (which never changes), are
        answered from what this gate holds. Otherwise the job's leader gate
        answers -- for STRONG, once a quorum of gates committed its replica
        again (they accept only the job's current fence: it still leads,
        and what it answers is held by that quorum). A gate that follows
        the job passes the read to its leader; it keeps no synced view to
        judge SESSION or BOUNDED_STALENESS reads by. Empty bytes: no answer."""
        consistency = ReadConsistency(query.consistency)
        job_id = query.job_id
        status = await self._gather_job_status(job_id)
        if status is not None and (
            consistency is ReadConsistency.EVENTUAL or JobStatusOrder().is_terminal(status.status)
        ):
            return status.dump()

        if self._job_leadership_tracker.is_leader(job_id):
            if status is None:
                return b""
            if consistency is ReadConsistency.STRONG and not await self._replication_coordinator.revise_committed_replica(
                job_id,
                lambda committed_replica: committed_replica,
                peer_addrs=list(self._modular_state.get_active_peers_list()),
                quorum_size=self._quorum_size(),
            ):
                return b""
            status.view_time = self._clock.monotonic()
            return status.dump()

        leader_addr = self._job_leadership_tracker.get_leader_addr(job_id)
        if query.forwarded or leader_addr is None or tuple(leader_addr) == (self._host, self._tcp_port):
            return b""
        query.forwarded = True
        response, _clock = await self._send_tcp(
            tuple(leader_addr),
            "job_status",
            query.dump(),
            timeout=self._tcp_timeout_standard,
        )
        return response if isinstance(response, bytes) else b""

    async def _gather_job_status(self, job_id: str) -> GlobalJobStatus | None:
        """A client's status poll: the job as this gate holds it, or None
        when it holds no such job. A read -- only the job's terminal paths
        decide its status."""
        async with self._job_manager.lock_job(job_id):
            status = self._job_manager.aggregate_job_status(job_id)
            if status is None:
                return None

            return GlobalJobStatus(
                job_id=status.job_id,
                status=status.status,
                total_completed=status.total_completed,
                total_failed=status.total_failed,
                elapsed_seconds=status.elapsed_seconds,
                overall_rate=status.overall_rate,
                datacenters=list(status.datacenters),
                timestamp=status.timestamp,
                completed_datacenters=status.completed_datacenters,
                failed_datacenters=status.failed_datacenters,
                errors=list(status.errors),
                resolution_details=status.resolution_details,
                fence_token=status.fence_token,
            )

    def _get_peer_state_lock(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
        """Get or create lock for a peer."""
        return self._modular_state.get_or_create_peer_lock_sync(peer_addr)

    def _build_gate_info_from_ping(
        self,
        udp_addr: tuple[str, int],
        response: GatePingResponse,
    ) -> GateInfo:
        """Build gate identity from a verified TCP ping response."""
        return GateInfo(
            node_id=response.gate_id,
            tcp_host=response.host,
            tcp_port=response.port,
            udp_host=udp_addr[0],
            udp_port=udp_addr[1],
            datacenter=response.datacenter,
            is_leader=response.is_leader,
        )

    async def _verify_gate_peer_rejoin(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> GateInfo | None:
        """Verify a configured gate peer is live at ``tcp_addr``."""
        # ``self._clock.monotonic_ns`` — NOT ``time.monotonic_ns``: this
        # module never imported ``time``, so the old form was a latent
        # NameError that would have crashed the first gate rejoin. The
        # clock seam both fixes that and makes the token deterministic
        # under SIM replay.
        request_id = f"{self._node_id.full}:gate-rejoin:{self._clock.monotonic_ns()}"
        request = PingRequest(request_id=request_id)
        try:
            response, _clock = await self._send_tcp(
                tcp_addr,
                "ping",
                request.dump(),
                timeout=self._tcp_timeout_short,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response, Exception):
                raise response
            if not response or response == b"error":
                return None

            parsed = GatePingResponse.load(response)
            if parsed.request_id != request_id:
                return None
            if (parsed.host, parsed.port) != tcp_addr:
                return None
            if parsed.gate_id == self._node_id.full:
                return None

            return self._build_gate_info_from_ping(udp_addr, parsed)

        except Exception as error:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Gate peer rejoin verification failed for {tcp_addr}: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return None

    async def authorize_rejoin_reset(
        self,
        target: tuple[str, int],
        role: str | None,
        source_addr: tuple[str, int],
    ) -> int | None:
        """Authorize configured gate-peer JOINs that prove liveness over TCP."""
        if role != NodeRole.GATE.value:
            return None
        if source_addr != target:
            return None
        if target not in self._gate_udp_peers:
            return None

        tcp_addr = self._modular_state.get_tcp_addr_for_udp(target)
        if tcp_addr is None:
            return None

        gate_info = await self._verify_gate_peer_rejoin(target, tcp_addr)
        if gate_info is None:
            return None

        await self._ingest_gate_peer_info(gate_info)
        return await self.reset_peer_for_rejoin(target)

    def _get_registered_node_id_for_addr(self, addr: tuple[str, int]) -> str | None:
        """Return the gate identity currently registered at ``addr``."""
        if heartbeat := self._modular_state.get_gate_peer_heartbeat(addr):
            return heartbeat.node_id

        tcp_addr = self._modular_state.get_tcp_addr_for_udp(addr)
        for gate_id, gate_info in self._modular_state.iter_known_gates():
            gate_tcp_addr = (gate_info.tcp_host, gate_info.tcp_port)
            gate_udp_addr = (gate_info.udp_host, gate_info.udp_port)
            if addr == gate_udp_addr or addr == gate_tcp_addr or tcp_addr == gate_tcp_addr:
                return gate_id

        return None

    async def _ingest_gate_peer_info(
        self,
        gate_info: GateInfo,
        heartbeat: GateHeartbeat | None = None,
    ) -> None:
        """Apply gate peer identity while removing stale same-address IDs."""
        if gate_info.node_id == self._node_id.full:
            return

        tcp_addr = (gate_info.tcp_host, gate_info.tcp_port)
        udp_addr = (gate_info.udp_host, gate_info.udp_port)
        stale_gate_ids: set[str] = set()

        existing_heartbeat = self._modular_state.get_gate_peer_heartbeat(udp_addr)
        if existing_heartbeat and existing_heartbeat.node_id != gate_info.node_id:
            stale_gate_ids.add(existing_heartbeat.node_id)

        for known_gate_id, known_gate in list(self._modular_state.iter_known_gates()):
            if known_gate_id == gate_info.node_id:
                continue
            known_tcp_addr = (known_gate.tcp_host, known_gate.tcp_port)
            known_udp_addr = (known_gate.udp_host, known_gate.udp_port)
            if known_tcp_addr == tcp_addr or known_udp_addr == udp_addr:
                stale_gate_ids.add(known_gate_id)

        for stale_gate_id in stale_gate_ids:
            self._modular_state.remove_known_gate(stale_gate_id)
            await self._versioned_clock.remove_entity(stale_gate_id)
            await self._job_hash_ring.remove_node(stale_gate_id)

        self._modular_state.set_udp_to_tcp_mapping(udp_addr, tcp_addr)
        self._modular_state.add_known_gate(gate_info.node_id, gate_info)
        self._modular_state.mark_peer_healthy(tcp_addr)
        self._dead_gate_addrs.discard(tcp_addr)
        self.record_peer_role(udp_addr, NodeRole.GATE.value)

        if heartbeat is not None:
            self._modular_state.set_gate_peer_heartbeat(udp_addr, heartbeat)

        await self._job_hash_ring.add_node(
            node_id=gate_info.node_id,
            tcp_host=gate_info.tcp_host,
            tcp_port=gate_info.tcp_port,
        )

    def _on_peer_confirmed(self, peer: tuple[str, int]) -> None:
        """Handle peer confirmation via SWIM (AD-29)."""
        tcp_addr = self._modular_state.get_tcp_addr_for_udp(peer)
        if tcp_addr:
            self._task_runner.run(self._modular_state.add_active_peer, tcp_addr)

    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """Handle node death via SWIM.

        Runs on every peer death — both peer gates (orphan-coordinator
        + hash-ring removal flow) and DC managers the gate watches
        (circuit-breaker cleanup). The dead-log emission and circuit-
        breaker removal were previously gated by the HFD
        ``on_global_death`` override; that override silently bypassed
        the canonical post-DEAD pipeline and prevented the orphan
        coordinator from ever firing. Both side effects now run
        unconditionally on the canonical pipeline (AD-31).
        """
        gate_tcp_addr = self._modular_state.get_tcp_addr_for_udp(node_addr)
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Peer {node_addr} globally dead",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )
        self._task_runner.run(
            self._circuit_breaker_manager.remove_circuit,
            node_addr,
        )
        if gate_tcp_addr:
            self._dead_gate_addrs.add(gate_tcp_addr)
            self._task_runner.run(
                self._handle_gate_peer_failure, node_addr, gate_tcp_addr
            )

    def _on_node_join(self, node_addr: tuple[str, int]) -> None:
        """Handle node join via SWIM."""
        gate_tcp_addr = self._modular_state.get_tcp_addr_for_udp(node_addr)
        if gate_tcp_addr:
            self._dead_gate_addrs.discard(gate_tcp_addr)
            self._task_runner.run(
                self._handle_gate_peer_recovery, node_addr, gate_tcp_addr
            )

    def _on_peer_coordinate_update(
        self,
        peer_id: str,
        peer_coordinate: NetworkCoordinate,
        rtt_ms: float,
    ) -> None:
        self._coordinate_tracker.update_peer_coordinate(
            peer_id, peer_coordinate, rtt_ms
        )

    def _get_datacenter_candidates_for_router(self) -> list[DatacenterCandidate]:
        return self._health_coordinator.build_datacenter_candidates(
            list(self._datacenter_managers.keys())
        )

    def _get_datacenter_coordinate(self, datacenter_id: str) -> NetworkCoordinate | None:
        """The Vivaldi coordinate a datacenter is reached at: that of its
        most authoritative manager (the leader, while its heartbeat is
        fresh). Coordinates are learned per manager node."""
        heartbeat, _, _ = self._health_coordinator.get_best_manager_heartbeat(datacenter_id)
        if heartbeat is None:
            return None
        return self._coordinate_tracker.get_peer_coordinate(heartbeat.node_id)

    async def _handle_gate_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer failure."""
        await self._peer_coordinator.handle_peer_failure(udp_addr, tcp_addr)

    async def _handle_gate_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer recovery."""
        await self._peer_coordinator.handle_peer_recovery(udp_addr, tcp_addr)

    async def _apply_committed_replica(self, replica) -> None:
        """Apply a committed ``GateJobReplica`` to local gate state.

        Called by the replication coordinator at commit time on both
        leader and peers. Idempotent — repeated commits at the same
        sequence overwrite identical values. Leadership is applied as the
        committed fenced value regardless of whether this gate is the leader
        or a peer, keeping local identity out of the commit path.
        """
        # The submission instant, on this gate's own monotonic clock.
        submitted_monotonic = self._clock.monotonic() - max(
            0.0, self._clock.time() - replica.submitted_wall_time
        )
        job = self._job_manager.get_job(replica.job_id)
        if job is None:
            job = GlobalJobStatus(
                job_id=replica.job_id,
                status=replica.status_seed,
                datacenters=[],
                timestamp=submitted_monotonic,
                fence_token=replica.fence_token,
            )
        else:
            job.status = replica.status_seed
            job.fence_token = replica.fence_token
            if job.timestamp <= 0:
                job.timestamp = submitted_monotonic
        self._job_manager.set_job(replica.job_id, job)
        self._job_manager.set_target_dcs(
            replica.job_id, set(replica.target_dcs)
        )
        self._job_manager.set_datacenter_substitutions(
            replica.job_id, list(replica.datacenter_substitutions)
        )
        self._job_manager.set_released_datacenters(
            replica.job_id, set(replica.released_datacenters)
        )
        self._job_manager.set_fence_token(replica.job_id, replica.fence_token)

        if replica.callback_addr:
            callback = tuple(replica.callback_addr)
            self._job_manager.set_callback(replica.job_id, callback)
            self._modular_state._progress_callbacks[replica.job_id] = callback

        self._modular_state._job_workflow_ids[replica.job_id] = set(
            replica.workflow_ids
        )

        # AD-40: the key is decided for this job on every gate that holds
        # its replica -- a retry at any of them is answered for it.
        if replica.idempotency_key:
            await self._idempotency_cache.adopt_committed(
                IdempotencyKey.parse(replica.idempotency_key), replica.job_id, replica.leader_id
            )

        if replica.submission_payload:
            try:
                submission = JobSubmission.load(replica.submission_payload)
                self._modular_state._job_submissions[replica.job_id] = submission
            except Exception as load_error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            f"Committed gate replica for job {replica.job_id[:8]}... "
                            f"has invalid submission payload: {load_error}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

        self._job_leadership_tracker.apply_leadership(
            job_id=replica.job_id,
            leader_id=replica.leader_id,
            leader_addr=tuple(replica.leader_addr),
            fencing_token=replica.fence_token,
            metadata=replica.target_dc_count,
        )

        # Every gate (leader and peers) runs this at commit, so this is
        # where each joins the job's Raft group -- and leaves it once the
        # replicated status is terminal (no ledger entries follow it).
        if JobStatusOrder().is_terminal(replica.status_seed):
            await self._raft.consensus.destroy_job_raft(replica.job_id)
        else:
            # With the voters the accepting gate decided (AD-52).
            await self._raft.consensus.create_job_raft(
                replica.job_id, frozenset(replica.raft_voters)
            )

        await self._modular_state.increment_state_version()

    async def _drop_committed_replica(self, job_id: str) -> None:
        """Remove all replicated state for ``job_id`` (failed-commit cleanup)."""
        await self._raft.consensus.destroy_job_raft(job_id)
        self._job_manager.delete_job(job_id)
        self._modular_state.clear_job(job_id)
        self._job_leadership_tracker.release_leadership(job_id)
        await self._modular_state.increment_state_version()

    async def _repair_orphan_job_state(self, job_id: str) -> bool:
        """Fetch and apply the committed replica for ``job_id`` from peers.

        Called by the orphan coordinator when local ``get_job(job_id)``
        returns ``None`` for a job whose leader has been declared dead.
        Returns ``True`` when a peer returned a committed replica and
        it was applied locally; ``False`` otherwise. A ``False`` return
        does not abandon the orphan — the coordinator keeps it marked
        and re-evaluates on the next scan tick until the orphan
        timeout elapses.
        """
        peer_addrs = list(self._modular_state.get_active_peers_list())
        return await self._replication_coordinator.repair_committed_replica_from_peers(
            job_id,
            peer_addrs,
        )

    async def _repair_orphan_jobs_for_dead_gate(
        self,
        leader_addr: tuple[str, int],
    ) -> list[str]:
        """Fetch committed replicas for jobs led by a SWIM-dead gate."""
        peer_addrs = list(self._modular_state.get_active_peers_list())
        return (
            await self._replication_coordinator.repair_committed_replicas_for_leader_from_peers(
                leader_addr,
                peer_addrs,
            )
        )

    async def _mark_confirmed_orphans_for_dead_gate(
        self,
        leader_addr: tuple[str, int],
    ) -> list[str]:
        """Mark and repair SWIM-confirmed orphans for a dead gate leader."""
        orphaned_job_ids = (
            self._orphan_job_coordinator.mark_jobs_confirmed_orphaned_by_gate(
                leader_addr
            )
        )
        if self.is_leader():
            repaired_job_ids = await self._repair_orphan_jobs_for_dead_gate(
                leader_addr
            )
            if repaired_job_ids:
                orphaned_job_ids = (
                    self._orphan_job_coordinator.mark_jobs_confirmed_orphaned_by_gate(
                        leader_addr
                    )
                )

        return orphaned_job_ids

    async def _commit_gate_job_leadership_takeover(self, job_id: str) -> int | None:
        """Quorum-commit a gate job leadership takeover by the SWIM leader.

        The takeover is built from the freshest replica a quorum of gates
        committed, adopted here first: this gate may have missed the old
        leader's last revision of the job. When that replica names a
        leader other than the one being replaced, the job is led already
        and nothing is taken over.
        """
        if not self.is_leader():
            return None

        if self._job_manager.get_job(job_id) is None:
            return None

        node_addr = (self._host, self._tcp_port)
        old_leader_id = self._job_leadership_tracker.get_leader(job_id)

        def build_takeover() -> GateJobReplica | None:
            job = self._job_manager.get_job(job_id)
            if job is None or self._job_leadership_tracker.get_leader(job_id) != old_leader_id:
                return None
            target_dcs = sorted(self._job_manager.get_target_dcs(job_id))
            next_fence_token = max(
                2,
                max(
                    self._job_manager.get_fence_token(job_id),
                    self._job_leadership_tracker.get_fencing_token(job_id),
                )
                + 1,
            )
            submission = self._modular_state._job_submissions.get(job_id)
            # The job's group keeps its voters: those of the replica this
            # gate adopted -- or, with no replica anywhere, the group is
            # founded here, with the gates live now (AD-52).
            committed = self._replication_coordinator.get_committed_replica(job_id)
            return GateJobReplica(
                job_id=job_id,
                # The coordinator orders it after every revision it knows.
                sequence=0,
                fence_token=next_fence_token,
                leader_id=self._node_id.full,
                leader_addr=node_addr,
                origin_gate_addr=node_addr,
                callback_addr=self._job_manager.get_callback(job_id),
                target_dcs=target_dcs,
                target_dc_count=len(target_dcs),
                status_seed=job.status,
                submitted_wall_time=(
                    self._clock.time() - (self._clock.monotonic() - job.timestamp)
                    if job.timestamp > 0
                    else self._clock.time()
                ),
                raft_voters=(
                    list(committed.raft_voters)
                    if committed is not None
                    else sorted(self._raft.consensus.current_members())
                ),
                workflow_ids=sorted(self._modular_state._job_workflow_ids.get(job_id, set())),
                submission_payload=submission.dump() if submission is not None else b"",
                datacenter_substitutions=self._job_manager.get_datacenter_substitutions(job_id),
                released_datacenters=sorted(self._job_manager.get_released_datacenters(job_id)),
                idempotency_key=(
                    committed.idempotency_key
                    if committed is not None
                    else (submission.idempotency_key or "") if submission is not None else ""
                ),
            )

        replica = await self._replication_coordinator.take_over_committed_replica(
            job_id,
            build_takeover,
            peer_addrs=list(self._modular_state.get_active_peers_list()),
            quorum_size=self._quorum_size(),
        )
        if replica is None:
            return None
        next_fence_token = replica.fence_token
        target_dcs = list(replica.target_dcs)
        submission = self._modular_state._job_submissions.get(job_id)

        await self._adopt_replicated_ledger_history(job_id, old_leader_id, next_fence_token)
        await self._resume_best_effort_tracking(job_id, submission, target_dcs)
        # AD-34: a job's global timeout is its leader gate's to track --
        # this gate's now, for what is left of the budget (one timeout check
        # of grace if it ran out meanwhile, so a completion in flight can
        # win). Untracked, a job whose leader gate died never timed out.
        if submission is not None:
            remaining_timeout = submission.timeout_seconds - max(
                0.0, self._clock.time() - replica.submitted_wall_time
            )
            await self._job_timeout_tracker.start_tracking_job(
                job_id=job_id,
                timeout_seconds=(
                    remaining_timeout
                    if remaining_timeout > 0.0
                    else self.env.GATE_TIMEOUT_CHECK_INTERVAL
                ),
                target_dcs=target_dcs,
            )
        else:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Took over job {job_id[:8]}... without its submission: "
                        "its global timeout budget is unknown, and it is bounded "
                        "by its managers' timeouts alone"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        self._task_runner.run(
            self._notify_managers_gate_job_leader_transfer,
            job_id,
            old_leader_id,
            next_fence_token,
            target_dcs,
        )
        return next_fence_token

    async def _adopt_replicated_ledger_history(
        self, job_id: str, previous_leader_id: str | None, lease_fence_token: int
    ) -> None:
        """AD-38: take over the job's ledger record along with the job.

        The previous leader gate's ledger held the job; this gate mirrored
        its replicated entries through the job's gate group. Without
        adopting them this ledger does not know the job, and its terminal
        would append nothing anywhere.
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
        # The takeover is recorded beside the history (AD-38
        # JobLeadershipAcquired).
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

    async def _notify_managers_gate_job_leader_transfer(
        self,
        job_id: str,
        old_gate_id: str | None,
        fence_token: int,
        target_dcs: list[str],
    ) -> None:
        """Notify relevant managers that this gate is the new job leader."""
        manager_addrs: list[tuple[str, int]] = []
        seen_manager_addrs: set[tuple[str, int]] = set()
        job_dc_managers = self._modular_state.get_job_dc_managers(job_id)

        for manager_addr in job_dc_managers.values():
            if manager_addr in seen_manager_addrs:
                continue
            manager_addrs.append(manager_addr)
            seen_manager_addrs.add(manager_addr)

        for datacenter_id in target_dcs:
            for manager_addr in self._datacenter_managers.get(datacenter_id, []):
                if manager_addr in seen_manager_addrs:
                    continue
                manager_addrs.append(manager_addr)
                seen_manager_addrs.add(manager_addr)

        if not manager_addrs:
            return

        transfer = JobLeaderGateTransfer(
            job_id=job_id,
            new_gate_id=self._node_id.full,
            new_gate_addr=(self._host, self._tcp_port),
            fence_token=fence_token,
            old_gate_id=old_gate_id,
        )
        await asyncio.gather(
            *[
                self._send_gate_job_leader_transfer_to_manager(manager_addr, transfer)
                for manager_addr in manager_addrs
            ]
        )

    async def _send_gate_job_leader_transfer_to_manager(
        self,
        manager_addr: tuple[str, int],
        transfer: JobLeaderGateTransfer,
    ) -> None:
        """Send one gate-leader-transfer notification to a manager."""
        try:
            response, _clock_time = await self._send_tcp(
                manager_addr,
                "job_leader_gate_transfer",
                transfer.dump(),
                timeout=self.env.GATE_TCP_TIMEOUT_STANDARD,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response, Exception):
                raise response
            if not response:
                return
            ack = JobLeaderGateTransferAck.load(response)
            if ack.accepted:
                return

            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Manager {manager_addr} rejected gate leader transfer "
                        f"for job {transfer.job_id[:8]}..."
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        except Exception as transfer_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Failed to notify manager {manager_addr} about gate leader "
                        f"transfer for job {transfer.job_id[:8]}...: {transfer_error}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _handle_job_leader_failure(self, tcp_addr: tuple[str, int]) -> None:
        orphaned_job_ids = await self._mark_confirmed_orphans_for_dead_gate(tcp_addr)
        if orphaned_job_ids:
            await self._udp_logger.log(
                ServerInfo(
                    message=f"Marked {len(orphaned_job_ids)} jobs as orphaned from failed gate {tcp_addr}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    def _on_gate_become_leader(self) -> None:
        """Called when this gate becomes the SWIM cluster leader."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message="This gate is now the LEADER",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )
        self._task_runner.run(self._scan_for_orphaned_gate_jobs)
        self._task_runner.run(self._orphan_job_coordinator.evaluate_confirmed_orphans)

    def _on_gate_lose_leadership(self) -> None:
        """Called when this gate loses cluster leadership."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message="This gate is no longer the leader",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    # =========================================================================
    # Per-Job Raft Leader Callbacks
    # =========================================================================

    def _on_job_raft_leader(self, job_id: str) -> None:
        """Called when this gate becomes the per-job Raft leader."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerDebug(
                message=(
                    f"Ignoring per-job Raft leadership for gate job {job_id[:8]}... "
                    "as a failover authority; SWIM leadership coordinates takeover"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _on_job_raft_lose_leader(self, job_id: str) -> None:
        """Called when this gate loses per-job Raft leadership."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Lost Raft leadership for gate job {job_id[:8]}...",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    async def _scan_for_orphaned_gate_jobs(self) -> None:
        """Scan for orphaned gate jobs from dead gate peers.

        Called when this gate becomes the SWIM cluster leader.
        Marks jobs whose leader is in the dead gate-address set so the orphan
        coordinator can quorum-commit the SWIM-leader takeover.
        """
        all_leaderships = self._job_leadership_tracker.get_all_leaderships()
        dead_leader_addrs: set[tuple[str, int]] = set()
        for _job_id, _leader_id, leader_addr, _fencing_token in all_leaderships:
            if leader_addr not in self._dead_gate_addrs:
                continue
            dead_leader_addrs.add(leader_addr)

        for leader_addr in dead_leader_addrs:
            await self._mark_confirmed_orphans_for_dead_gate(leader_addr)

    def _on_manager_dead_for_dc(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Handle manager death for specific DC (AD-30).

        Called synchronously from the HFD job-layer expiration handler;
        the circuit-breaker write is async (asyncio.Lock-guarded), so
        we route it through the TaskRunner per CLAUDE.md's no-orphan
        rule rather than dropping the coroutine.
        """
        self._task_runner.run(
            self._circuit_breaker_manager.record_failure,
            manager_addr,
        )

    def _get_dc_manager_count(self, dc_id: str) -> int:
        """Get manager count for a DC."""
        return len(self._datacenter_managers.get(dc_id, []))

    async def _suspect_manager_for_dc(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
    ) -> None:
        incarnation = 0
        health_state = self._modular_state.get_manager_status(dc_id, manager_addr)
        if health_state:
            incarnation = getattr(health_state, "incarnation", 0)

        detector = self.get_hierarchical_detector()
        if detector:
            await detector.suspect_job(
                job_id=dc_id,
                node=manager_addr,
                incarnation=incarnation,
                from_node=(self._host, self._udp_port),
            )

    async def _confirm_manager_for_dc(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
    ) -> None:
        incarnation = 0
        health_state = self._modular_state.get_manager_status(dc_id, manager_addr)
        if health_state:
            incarnation = getattr(health_state, "incarnation", 0)

        detector = self.get_hierarchical_detector()
        if detector:
            await detector.confirm_job(
                job_id=dc_id,
                node=manager_addr,
                incarnation=incarnation,
                from_node=(self._host, self._udp_port),
            )

    async def _handle_embedded_manager_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        await self._health_coordinator.handle_embedded_manager_heartbeat(
            heartbeat,
            source_addr,
        )

    async def _ingest_manager_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
        manager_addr: tuple[str, int] | None = None,
        *,
        use_version_clock: bool = True,
    ) -> tuple[str, tuple[str, int]] | None:
        ingested = await self._health_coordinator.ingest_manager_heartbeat(
            heartbeat,
            source_addr,
            manager_addr,
            use_version_clock=use_version_clock,
        )
        # A datacenter this gate learned from its managers -- one that
        # joined this gate, or registered with it -- has its membership
        # followed from here on, like one the gate joined (AD-52 section
        # 10); not after ``stop`` cleared the watches.
        if ingested is not None:
            self._watch_datacenter_membership(ingested[0])
        return ingested

    async def _handle_gate_peer_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        udp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer heartbeat from SWIM."""
        if heartbeat.tcp_host and heartbeat.tcp_port:
            self._modular_state.record_gate_peer_heartbeat((heartbeat.tcp_host, heartbeat.tcp_port))
        if heartbeat.node_id and heartbeat.tcp_host and heartbeat.tcp_port:
            gate_info = GateInfo(
                node_id=heartbeat.node_id,
                tcp_host=heartbeat.tcp_host,
                tcp_port=heartbeat.tcp_port,
                udp_host=udp_addr[0],
                udp_port=udp_addr[1],
                datacenter=heartbeat.datacenter,
                is_leader=heartbeat.is_leader,
            )
            await self._ingest_gate_peer_info(gate_info, heartbeat)
        else:
            self._modular_state.set_gate_peer_heartbeat(udp_addr, heartbeat)

        # AD-19 addendum (Phase D): peer gates report their LHM in
        # GateHeartbeat. Feed it into cross_dc_correlation so a
        # stressed gate-tier surfaces alongside manager- and worker-
        # tier LHM in correlation analysis. Gate's own DC is
        # ``heartbeat.datacenter`` — the reporting peer's home DC.
        if heartbeat.datacenter:
            self._health_coordinator.record_peer_lhm_score(
                datacenter_id=heartbeat.datacenter,
                lhm_score=getattr(heartbeat, "lhm_score", 0),
                node_type="gate",
            )

    def _get_known_gates_for_piggyback(self) -> dict[str, tuple[str, int, str, int]]:
        """Get known gates for SWIM piggyback."""
        return self._peer_coordinator.get_known_gates_for_piggyback()

    def _get_job_leaderships_for_piggyback(
        self,
    ) -> list[tuple[str, str, tuple[str, int], int]]:
        """Get job leaderships for SWIM piggyback."""
        return self._job_leadership_tracker.get_all_leaderships()

    def _count_active_datacenters(self) -> int:
        return self._health_coordinator.count_active_datacenters()

    def _get_forward_throughput(self) -> float:
        return self._modular_state.calculate_throughput(
            self._clock.monotonic(), self._forward_throughput_interval_seconds
        )

    def _get_expected_forward_throughput(self) -> float:
        """AD-19 progress: the dispatches this gate attempted per second,
        against which the accepted ones (its throughput) are judged."""
        return self._modular_state.get_forward_attempt_rate()

    def _record_forward_throughput_event(self) -> None:
        self._task_runner.run(self._modular_state.record_forward)

    def _record_forward_attempt_event(self) -> None:
        self._task_runner.run(self._modular_state.record_forward_attempt)

    def _classify_datacenter_health(self, dc_id: str) -> DatacenterStatus:
        return self._health_coordinator.classify_datacenter_health(dc_id)

    def _get_all_datacenter_health(self) -> dict[str, DatacenterStatus]:
        return self._health_coordinator.get_all_datacenter_health()

    async def _log_health_transitions(self) -> None:
        transitions = self._dc_health_manager.get_and_clear_health_transitions()
        for dc_id, previous_health, new_health in transitions:
            if new_health in ("degraded", "unhealthy"):
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"DC {dc_id} health changed: {previous_health} -> {new_health}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.full if self._node_id else "unknown",
                    ),
                )

                status = self._dc_health_manager.get_datacenter_health(dc_id)
                if getattr(status, "leader_overloaded", False):
                    await self._udp_logger.log(
                        ServerWarning(
                            message=f"ALERT: DC {dc_id} leader manager is OVERLOADED - control plane saturated",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.full if self._node_id else "unknown",
                        ),
                    )

    async def _select_datacenters_with_fallback(
        self,
        count: int,
        preferred: list[str] | None,
        job_id: str,
    ) -> tuple[list[str], list[str], str]:
        """The datacenters to place a job in: its primaries, fallbacks and
        the worst health bucket among the primaries.

        The AD-36 router decides alone. While a datacenter the job could
        use has not reported yet, a selection short of ``count`` is
        refused as "initializing" (the client retries): placing the job in
        fewer datacenters than it asked for, because one was still
        starting, would silently shrink it. With no eligible datacenter at
        all the job is refused as "unhealthy".
        """
        decision = self._job_router.route_job(
            job_id,
            count,
            set(preferred) if preferred else None,
        )
        if len(decision.primary_datacenters) < count and self._has_initializing_datacenter(
            preferred
        ):
            return ([], [], "initializing")
        if decision.worst_primary_health_bucket is None:
            return ([], [], "unhealthy")

        await self._udp_logger.log(
            ServerInfo(
                message=(
                    f"Routed job {job_id[:8]}... to DCs {decision.primary_datacenters} "
                    f"(worst bucket={decision.worst_primary_health_bucket}, "
                    f"fallbacks={decision.fallback_datacenters}, "
                    f"scores={ {datacenter_id: round(score.final_score, 3) for datacenter_id, score in decision.scores.items()} }, "
                    f"excluded={ {datacenter_id: reason.value for datacenter_id, reason in decision.exclusions.items()} }, "
                    f"cooling={sorted(decision.cooling_datacenters)})"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

        return (
            decision.primary_datacenters,
            decision.fallback_datacenters,
            decision.worst_primary_health_bucket.lower(),
        )

    def _has_initializing_datacenter(self, preferred: list[str] | None) -> bool:
        """Whether a datacenter in the job's scope has not reported yet."""
        in_scope = set(preferred) if preferred else None
        return any(
            status.health == DatacenterHealth.INITIALIZING.value
            for datacenter_id, status in self._get_all_datacenter_health().items()
            if in_scope is None or datacenter_id in in_scope
        )

    async def _check_rate_limit_for_operation(
        self,
        client_id: str,
        operation: str,
    ) -> tuple[bool, float]:
        """Check rate limit for an operation."""
        result = await self._rate_limiter.check_rate_limit(client_id, operation)
        return result.allowed, result.retry_after_seconds

    def _should_shed_request(self, request_type: str) -> bool:
        """Check if request should be shed due to load."""
        return self._load_shedder.should_shed_handler(request_type)

    def _has_quorum_available(self) -> bool:
        return self._leadership_coordinator.has_quorum(
            self._modular_state.get_gate_state().value
        )

    def _quorum_size(self) -> int:
        return self._leadership_coordinator.get_quorum_size()

    def _is_clock_fenced(self) -> bool:
        return self._clock_offset_monitor.is_fenced

    def _refuses_leadership_for_a_ready_peer(self) -> bool:
        """AD-19: a gate that cannot do a leader's work -- no reachable
        datacenter, or shedding load -- leaves leadership to a live peer
        whose latest heartbeat says it can. When no peer can, every gate
        stands as usual: refusing everywhere would leave the tier without
        a leader exactly when one is needed (a global datacenter outage).
        """
        connected_datacenter_count = self._count_active_datacenters()
        if GateHealthState(
            gate_id=self._node_id.full,
            has_dc_connectivity=connected_datacenter_count > 0,
            connected_dc_count=connected_datacenter_count,
            overload_state=self._gate_health_state,
        ).readiness:
            return False

        for _udp_addr, heartbeat in self._modular_state.iter_gate_peer_heartbeats():
            if not self._modular_state.is_peer_active((heartbeat.tcp_host, heartbeat.tcp_port)):
                continue
            if GateHealthState(
                gate_id=heartbeat.node_id,
                has_dc_connectivity=heartbeat.health_has_dc_connectivity,
                connected_dc_count=heartbeat.health_connected_dc_count,
                overload_state=heartbeat.health_overload_state,
            ).readiness:
                return True

        return False

    def _may_lead(self) -> bool:
        return not self._clock_offset_monitor.is_fenced

    async def _on_clock_fence_change(self, verdict: ClockFenceVerdict) -> None:
        """A fenced gate gives up cluster leadership (re-election is
        refused while fenced); Raft groups relinquish on their next tick."""
        if verdict.fenced and self.is_leader():
            self._task_runner.run(self._leader_election._step_down)

    def _declare_datacenter_managers(
        self,
        datacenter_id: str,
        manager_addrs: list[tuple[str, int]],
    ) -> None:
        """Record operator-declared managers, which the stale-manager reaper
        keeps in their datacenter's address lists."""
        self._declared_datacenter_managers[datacenter_id] = self._declared_datacenter_managers.get(
            datacenter_id, frozenset()
        ).union(manager_addrs)

    def _forget_learned_manager_addresses(self, manager_addr: tuple[str, int]) -> None:
        """Remove a retired, runtime-learned manager from its datacenter's
        address lists, which count the datacenter's expected managers.
        Declared managers stay; a manager that returns is re-learned
        from its next heartbeat."""
        for datacenter_id, manager_addrs in self._datacenter_managers.items():
            if manager_addr not in manager_addrs or manager_addr in (
                self._declared_datacenter_managers.get(datacenter_id, frozenset())
            ):
                continue

            heartbeat = self._modular_state.get_manager_status(datacenter_id, manager_addr)
            manager_addrs.remove(manager_addr)
            if heartbeat is None or not heartbeat.udp_host:
                continue

            udp_addrs = self._datacenter_manager_udp.get(datacenter_id, [])
            if (udp_addr := (heartbeat.udp_host, heartbeat.udp_port)) in udp_addrs:
                udp_addrs.remove(udp_addr)

    def _get_election_member_count(self) -> int:
        """The gate tier's SWIM election uses the one cluster size every
        gate quorum decision uses. The base class counts every same-role
        peer ever seen, a count that only grew."""
        return self._configured_gate_count()

    def _is_election_cohort_voter(self, voter_udp_address: tuple[str, int]) -> bool:
        """Only the gate cohort votes (Raft section 5.2): a gate outside it
        -- removed, or never in it -- cannot carry a minority of the cohort
        to a majority."""
        return self._modular_state.get_tcp_addr_for_udp(voter_udp_address) in self._cluster_membership.cohort

    def _configured_gate_count(self) -> int:
        """The gate cluster size every quorum decision uses: the gate
        cohort -- this gate and the peers it was configured with, until a
        committed resize changes it (AD-52) -- or more, while it knows of
        or holds active more peers than that."""
        return max(
            1,
            self._modular_state.get_known_gate_count() + 1,
            len(self._cluster_membership.cohort),
            self._modular_state.get_active_peer_count() + 1,
        )

    def _on_cohort_change(self, cohort: frozenset[tuple[str, int]]) -> None:
        """A committed resize changed the gate cohort (AD-52
        ``ResizeCluster``): job groups count its majority, and clock offsets
        are measured to its members. Quorum decisions read the cohort as
        they count."""
        self._raft.set_cohort_size(self._configured_gate_count())
        self._clock_probe_peers = {
            f"{peer_host}:{peer_port}": (peer_host, peer_port)
            for peer_host, peer_port in cohort
            if (peer_host, peer_port) != (self._host, self._tcp_port)
        }
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate cohort resized to {sorted(cohort)}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _get_healthy_gates(self) -> list[GateInfo]:
        return self._peer_coordinator.get_healthy_gates()

    def _get_progress_callback_for_job(self, job_id: str) -> tuple[str, int] | None:
        """Get the client callback address for a job."""
        return self._modular_state._progress_callbacks.get(job_id)

    def _normalize_callback_addr(
        self,
        callback_addr: tuple[str, int] | list[str | int] | None,
    ) -> tuple[str, int] | None:
        """Normalize serialized callback addresses into TCP address tuples."""
        if callback_addr is None:
            return None
        if len(callback_addr) != 2:
            return None
        return (str(callback_addr[0]), int(callback_addr[1]))

    def _record_job_callback(self, job_id: str, callback: tuple[str, int]) -> None:
        """Record a client callback in every gate-local callback store."""
        previous_callback = self._job_manager.get_callback(job_id)
        self._job_manager.set_callback(job_id, callback)
        self._modular_state._progress_callbacks[job_id] = callback
        if previous_callback != callback:
            self._increment_version()

    def _data_plane_provenance(
        self, job_id: str
    ) -> dict[str, str | int | tuple[str, int]]:
        """Producer identity + fence stamps for a gate-originated result.

        Mirrors the manager-side helper for the split-fence semantics:
        gates stamp ``producer_role='gate'`` and ``manager_fence_token=0``
        (gates never claim manager leadership); ``gate_fence_token`` is
        the gate's current per-job leadership fence — used by *peer*
        gate receivers to validate gate-originated forwards. The
        per-workflow result-sequence is not allocated here; gate-
        originated pushes are one-shot per workflow result, and any
        dedup at the receiving gate is keyed on
        ``(job_id, workflow_id, datacenter)``.
        """
        return {
            "producer_id": self._node_id.full,
            "producer_addr": (self._host, self._tcp_port),
            "producer_role": "gate",
            "manager_fence_token": 0,
            "gate_fence_token": self._job_manager.get_fence_token(job_id),
        }

    def _get_known_dc_manager_for_job(
        self,
        job_id: str,
        datacenter: str,
    ) -> tuple[str, int] | None:
        """Return the known manager leader for ``job_id`` in ``datacenter``."""
        manager_addr = self._job_leadership_tracker.get_dc_manager(
            job_id,
            datacenter,
        )
        if manager_addr is not None:
            return manager_addr

        return self._modular_state.get_job_dc_managers(job_id).get(datacenter)

    async def _validate_manager_result_producer(
        self,
        push: WorkflowResultPush,
    ) -> bytes | None:
        """Validate manager-originated workflow result provenance."""
        if push.producer_addr is not None:
            expected_manager_addr = self._get_known_dc_manager_for_job(
                push.job_id,
                push.datacenter,
            )
            producer_addr = tuple(push.producer_addr)
            if (
                expected_manager_addr is not None
                and producer_addr != expected_manager_addr
            ):
                await self._udp_logger.log(
                    ServerDebug(
                        message=(
                            f"Rejecting workflow result for {push.job_id}: "
                            f"producer {producer_addr} is not DC manager "
                            f"{expected_manager_addr}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )
                return b"stale_producer"

        if push.manager_fence_token <= 0:
            return None

        known_term = self._get_known_leader_manager_term_for_dc(push.datacenter)
        if known_term <= 0 or push.manager_fence_token >= known_term:
            return None

        await self._udp_logger.log(
            ServerDebug(
                message=(
                    f"Rejecting workflow result for {push.job_id}: producer "
                    f"{push.producer_id[:8]}... term {push.manager_fence_token} "
                    f"< DC leader manager term {known_term}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )
        return b"stale_producer"

    async def _validate_gate_result_producer(
        self,
        push: WorkflowResultPush,
    ) -> bytes | None:
        """Validate gate-originated workflow result provenance."""
        if push.gate_fence_token <= 0:
            return None

        current_fence = self._job_manager.get_fence_token(push.job_id)
        if push.gate_fence_token >= current_fence:
            return None

        await self._udp_logger.log(
            ServerDebug(
                message=(
                    f"Rejecting workflow result for {push.job_id}: gate "
                    f"producer {push.producer_id[:8]}... fence "
                    f"{push.gate_fence_token} < current gate fence {current_fence}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )
        return b"stale_producer"

    async def _validate_workflow_result_producer(
        self,
        push: WorkflowResultPush,
    ) -> bytes | None:
        """Validate producer provenance for a workflow result push."""
        if push.producer_role == "manager":
            return await self._validate_manager_result_producer(push)
        if push.producer_role == "gate":
            return await self._validate_gate_result_producer(push)
        return None

    def _resolve_job_callback(
        self,
        job_id: str,
        callback_addr: tuple[str, int] | list[str | int] | None,
    ) -> tuple[str, int] | None:
        """Resolve callback metadata from a push payload or local gate state."""
        callback = self._normalize_callback_addr(callback_addr)
        if callback is not None:
            return callback

        callback = self._job_manager.get_callback(job_id)
        if callback is not None:
            return callback

        return self._modular_state._progress_callbacks.get(job_id)

    def _get_workflow_push_target_dcs(self, push: WorkflowResultPush) -> set[str]:
        """Return the explicit target DCs carried by a workflow-result push."""
        return {datacenter for datacenter in push.target_dcs if datacenter}

    def _get_workflow_push_expected_dc_count(
        self,
        push: WorkflowResultPush,
        target_dcs: set[str],
    ) -> int:
        """Return the expected DC count represented by a workflow-result push."""
        expected_dc_count = max(push.target_dc_count, len(target_dcs))
        if expected_dc_count <= 0 and push.datacenter:
            return 1
        return expected_dc_count

    def _recover_job_from_workflow_push(
        self,
        push: WorkflowResultPush,
        callback: tuple[str, int],
    ) -> None:
        """Create enough job state for a survivor gate to own pushed results."""
        job = GlobalJobStatus(
            job_id=push.job_id,
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=self._clock.monotonic(),
            fence_token=push.fence_token,
        )
        self._job_manager.set_job(push.job_id, job)
        self._job_manager.set_fence_token(push.job_id, push.fence_token)
        self._record_job_callback(push.job_id, callback)

        target_dcs = self._get_workflow_push_target_dcs(push)
        expected_dc_count = self._get_workflow_push_expected_dc_count(push, target_dcs)
        if not target_dcs and expected_dc_count <= 1 and push.datacenter:
            target_dcs = {push.datacenter}
        if target_dcs:
            self._job_manager.set_target_dcs(push.job_id, target_dcs)

    async def _broadcast_job_leadership(
        self,
        job_id: str,
        target_dc_count: int,
        callback_addr: tuple[str, int] | None = None,
    ) -> None:
        if callback_addr is None:
            callback_addr = self._job_manager.get_callback(job_id)
        await self._leadership_coordinator.broadcast_leadership(
            job_id, target_dc_count, callback_addr
        )

    async def _dispatch_job_to_datacenters(
        self,
        submission: JobSubmission,
        target_dcs: list[str],
    ) -> None:
        await self._dispatch_coordinator.dispatch_job(submission, target_dcs)

    async def _forward_job_progress_to_peers(
        self,
        progress: JobProgress,
    ) -> bool:
        owner = await self._job_hash_ring.get_node(progress.job_id)
        if owner and owner.node_id != self._node_id.full:
            owner_addr = await self._job_hash_ring.get_node_addr(owner)
            if owner_addr:
                if await self._peer_gate_circuit_breaker.is_circuit_open(owner_addr):
                    return False

                circuit = await self._peer_gate_circuit_breaker.get_circuit(owner_addr)
                try:
                    response, _ = await self.send_tcp(
                        owner_addr,
                        "receive_job_progress",
                        progress.dump(),
                        timeout=self._tcp_timeout_forward,
                    )
                    # send_tcp returns transport errors rather than raising.
                    if isinstance(response, Exception):
                        raise response
                    circuit.record_success()
                    return True
                except Exception as forward_error:
                    circuit.record_failure()
                    await self._udp_logger.log(
                        ServerWarning(
                            message=f"Failed to forward progress to peer gate: {forward_error}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
        return False

    def _record_request_latency(self, latency_ms: float) -> None:
        """Record request latency for load shedding."""
        self._overload_detector.record_latency(latency_ms)

    async def _record_dc_job_stats(
        self,
        job_id: str,
        datacenter_id: str,
        completed: int,
        failed: int,
        rate: float,
        status: str,
    ) -> None:
        timestamp = int(self._clock.monotonic() * 1000)

        async with self._job_stats_crdt_lock:
            if job_id not in self._job_stats_crdt:
                self._job_stats_crdt[job_id] = JobStatsCRDT(job_id=job_id)

            crdt = self._job_stats_crdt[job_id]
            crdt.record_completed(datacenter_id, completed)
            crdt.record_failed(datacenter_id, failed)
            crdt.record_rate(datacenter_id, rate, timestamp)
            crdt.record_status(datacenter_id, status, timestamp)

    def _handle_update_by_tier(
        self,
        job_id: str,
        old_status: str | None,
        new_status: str,
        progress_data: bytes | None = None,
    ) -> None:
        """Handle update by tier (AD-15)."""
        tier = self._stats_coordinator.classify_update_tier(job_id, old_status, new_status)

        if tier == UpdateTier.IMMEDIATE.value:
            self._task_runner.run(
                self._send_immediate_update,
                job_id,
                f"status:{old_status}->{new_status}",
                progress_data,
            )

    async def _replay_job_status_to_callback(
        self,
        job_id: str,
        callback: tuple[str, int],
        last_sequence: int,
    ) -> None:
        if not self._job_manager.has_job(job_id):
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Skipped callback replay for missing job {job_id[:8]}..."
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return

        try:
            (
                updates,
                oldest_sequence,
                latest_sequence,
            ) = await self._modular_state.get_client_updates_since(
                job_id,
                last_sequence,
            )
            if updates:
                if last_sequence > 0 and oldest_sequence > 0:
                    if last_sequence < (oldest_sequence - 1):
                        await self._udp_logger.log(
                            ServerWarning(
                                message=(
                                    "Update history truncated for job "
                                    f"{job_id[:8]}...; replaying from {oldest_sequence}"
                                ),
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )
                for sequence, message_type, payload, _ in updates:
                    delivered = await self._deliver_client_update(
                        job_id,
                        callback,
                        sequence,
                        message_type,
                        payload,
                    )
                    if not delivered:
                        return
                await self._modular_state.set_client_update_position(
                    job_id,
                    callback,
                    latest_sequence,
                )

            await self._stats_coordinator.send_immediate_update(
                job_id,
                "reconnect",
                None,
            )
            await self._stats_coordinator.send_progress_replay(job_id)
            await self._stats_coordinator.push_windowed_stats_for_job(job_id)
        except Exception as error:
            await self.handle_exception(error, "replay_job_status_to_callback")

    async def _send_immediate_update(
        self,
        job_id: str,
        event_type: str,
        payload: bytes | None = None,
    ) -> None:
        """Send immediate update to client."""
        await self._stats_coordinator.send_immediate_update(
            job_id, event_type, payload
        )

    def _get_known_leader_manager_term_for_dc(self, dc_id: str) -> int:
        """Return the highest known leader-manager term for ``dc_id``.

        Looked up by ``GateStateSyncHandler.handle_job_final_result``
        (and the equivalent inline guard in ``workflow_result_push``)
        to validate ``manager_fence_token`` on manager-originated
        data-plane results against per-DC manager leadership.
        Returns 0 when no leader heartbeat exists for the DC — the
        caller treats 0 as "permissive: unknown" and accepts.
        """
        dc_state = self._dc_registration_states.get(dc_id)
        if dc_state is None:
            return 0
        return dc_state.get_known_leader_manager_term()

    def _record_manager_heartbeat(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
        node_id: str,
        generation: int,
        term: int = 0,
        is_leader: bool = False,
    ) -> None:
        """Record manager heartbeat.

        ``term`` and ``is_leader`` are threaded from
        ``ManagerHeartbeat`` so the gate can later validate
        manager-originated data-plane results against per-DC
        manager-leadership term.
        """
        now = self._clock.monotonic()

        self._circuit_breaker_manager.record_success(manager_addr)

        dc_state = self._dc_registration_states.setdefault(
            dc_id,
            DatacenterRegistrationState(
                dc_id=dc_id,
                configured_managers=[manager_addr],
            ),
        )
        if manager_addr not in dc_state.configured_managers:
            dc_state.configured_managers.append(manager_addr)

        dc_state.record_heartbeat(
            manager_addr,
            node_id,
            generation,
            now,
            term=term,
            is_leader=is_leader,
        )

    async def _handle_manager_backpressure_signal(
        self,
        manager_addr: tuple[str, int],
        dc_id: str,
        signal: BackpressureSignal,
    ) -> None:
        await self._modular_state.update_backpressure(
            manager_addr,
            dc_id,
            signal.level,
            signal.suggested_delay_ms,
            self._datacenter_managers,
        )

    async def _update_dc_backpressure(self, dc_id: str) -> None:
        await self._modular_state.recalculate_dc_backpressure(
            dc_id, self._datacenter_managers
        )

    async def _clear_manager_backpressure(self, manager_addr: tuple[str, int]) -> None:
        await self._modular_state.remove_manager_backpressure(manager_addr)

    async def _set_manager_backpressure_none(
        self, manager_addr: tuple[str, int], dc_id: str
    ) -> None:
        await self._modular_state.clear_manager_backpressure(
            manager_addr, dc_id, self._datacenter_managers
        )

    async def _broadcast_manager_discovery(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
        manager_udp_addr: tuple[str, int] | None,
        worker_count: int,
        healthy_worker_count: int,
        available_cores: int,
        total_cores: int,
    ) -> None:
        """Broadcast manager discovery to peer gates."""
        if not self._modular_state.has_active_peers():
            return

        broadcast = ManagerDiscoveryBroadcast(
            source_gate_id=self._node_id.full,
            datacenter=dc_id,
            manager_tcp_addr=list(manager_addr),
            manager_udp_addr=list(manager_udp_addr) if manager_udp_addr else None,
            worker_count=worker_count,
            healthy_worker_count=healthy_worker_count,
            available_cores=available_cores,
            total_cores=total_cores,
        )

        for peer_addr in self._modular_state.iter_active_peers():
            if await self._peer_gate_circuit_breaker.is_circuit_open(peer_addr):
                continue

            circuit = await self._peer_gate_circuit_breaker.get_circuit(peer_addr)
            try:
                response, _ = await self.send_tcp(
                    peer_addr,
                    "manager_discovery",
                    broadcast.dump(),
                    timeout=self._tcp_timeout_short,
                )
                # send_tcp returns transport errors rather than raising.
                if isinstance(response, Exception):
                    raise response
                circuit.record_success()
            except Exception as discovery_error:
                circuit.record_failure()
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to broadcast manager discovery to peer gate: {discovery_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    def _get_state_snapshot(self) -> GateStateSnapshot:
        job_leaders, job_leader_addrs, job_fencing_tokens = (
            self._job_leadership_tracker.to_snapshot()
        )
        progress_callbacks = dict(self._modular_state._progress_callbacks)
        workflow_dc_results = {
            job_id: {
                workflow_id: dict(dc_results)
                for workflow_id, dc_results in workflow_results.items()
            }
            for job_id, workflow_results in self._workflow_dc_results.items()
        }
        # ``is_leader`` and ``term`` are required: without them building the
        # snapshot raised TypeError, and every state-sync request a peer
        # gate made was answered with an error.
        return GateStateSnapshot(
            node_id=self._node_id.full,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            version=self._modular_state.get_state_version(),
            jobs={job_id: job for job_id, job in self._job_manager.items()},
            datacenter_managers=dict(self._datacenter_managers),
            datacenter_manager_udp=dict(self._datacenter_manager_udp),
            job_leaders=job_leaders,
            job_leader_addrs=job_leader_addrs,
            job_fencing_tokens=job_fencing_tokens,
            job_dc_managers=self._modular_state.copy_job_dc_managers(),
            workflow_dc_results=workflow_dc_results,
            progress_callbacks=progress_callbacks,
        )

    async def _apply_gate_state_snapshot(
        self,
        snapshot: GateStateSnapshot,
    ) -> None:
        """Apply state snapshot from peer gate."""
        for job_id, job_status in snapshot.jobs.items():
            if not self._job_manager.has_job(job_id):
                self._job_manager.set_job(job_id, job_status)

        for dc, manager_addrs in snapshot.datacenter_managers.items():
            dc_managers = self._datacenter_managers.setdefault(dc, [])
            for addr in manager_addrs:
                addr_tuple = tuple(addr) if isinstance(addr, list) else addr
                if addr_tuple not in dc_managers:
                    dc_managers.append(addr_tuple)

        async with self._workflow_dc_results_lock:
            for job_id, workflow_results in snapshot.workflow_dc_results.items():
                job_results = self._workflow_dc_results.setdefault(job_id, {})
                for workflow_id, dc_results in workflow_results.items():
                    workflow_entries = job_results.setdefault(workflow_id, {})
                    for dc_id, result in dc_results.items():
                        if dc_id not in workflow_entries:
                            workflow_entries[dc_id] = result

        # The DC managers running each job route status queries and the
        # takeover notices; a syncing gate keeps what it already knows.
        for job_id, datacenter_managers in snapshot.job_dc_managers.items():
            known_managers = self._modular_state.get_job_dc_managers(job_id)
            for datacenter_id, manager_addr in datacenter_managers.items():
                manager_addr_tuple = self._normalize_callback_addr(manager_addr)
                if manager_addr_tuple is not None and datacenter_id not in known_managers:
                    self._modular_state.set_job_dc_manager(
                        job_id, datacenter_id, manager_addr_tuple
                    )

        for job_id, callback_addr in snapshot.progress_callbacks.items():
            callback_tuple = self._normalize_callback_addr(callback_addr)
            if callback_tuple is not None:
                self._record_job_callback(job_id, callback_tuple)

        self._job_leadership_tracker.merge_from_snapshot(
            job_leaders=snapshot.job_leaders,
            job_leader_addrs=snapshot.job_leader_addrs,
            job_fencing_tokens=snapshot.job_fencing_tokens,
        )

        self._modular_state.adopt_state_version(snapshot.version)

    def _increment_version(self) -> None:
        """Increment state version."""
        self._modular_state.advance_state_version()

    async def _send_xprobe(self, target: tuple[str, int], data: bytes) -> bool:
        """Send cross-cluster probe."""
        try:
            response = await self.send(target, data, timeout=self.env.FEDERATED_PROBE_TIMEOUT)
            if not isinstance(response, bytes):
                return True

            if response.startswith(b"xack>"):
                await self._handle_xack_response(target, response.split(b">", 1)[1])
                return True

            if response.startswith(b"xnack>"):
                return False

            return True
        except Exception as probe_error:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Cross-cluster probe failed: {probe_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

    def _on_dc_health_change(self, datacenter: str, new_health: str) -> None:
        """Handle DC health change."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"DC {datacenter} health changed to {new_health}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _on_partition_detected(self, affected_datacenters: list[str]) -> None:
        self._task_runner.run(
            self._udp_logger.log,
            ServerWarning(
                message=f"Partition detected across datacenters: {affected_datacenters}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _on_partition_healed(self, healed_datacenters: list[str]) -> None:
        """Handle partition healed notifications."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=(
                    "Partition healed, routing restored for datacenters: "
                    f"{healed_datacenters}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _on_dc_latency(self, datacenter: str, latency_ms: float) -> None:
        self._cross_dc_correlation.record_latency(
            datacenter_id=datacenter,
            latency_ms=latency_ms,
            probe_type="federated",
        )

    async def _on_federated_probe_error(
        self,
        error_message: str,
        affected_datacenters: list[str],
    ) -> None:
        await self._udp_logger.log(
            ServerWarning(
                message=f"Federated health probe error: {error_message} "
                f"(DCs: {affected_datacenters})",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _on_cross_dc_callback_error(
        self,
        event_type: str,
        affected_datacenters: list[str],
        error: Exception,
    ) -> None:
        self._task_runner.run(
            self._udp_logger.log,
            ServerWarning(
                message=f"Cross-DC correlation callback error ({event_type}): {error} "
                f"(DCs: {affected_datacenters})",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _on_dc_leader_change(
        self,
        datacenter: str,
        leader_node_id: str,
        leader_tcp_addr: tuple[str, int],
        leader_udp_addr: tuple[str, int],
        term: int,
    ) -> None:
        """
        Handle DC leader change.

        Broadcasts the leadership change to all peer gates so they can update
        their FederatedHealthMonitor with the new leader information.
        """
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"DC {datacenter} leader changed to {leader_node_id} "
                f"at {leader_tcp_addr[0]}:{leader_tcp_addr[1]} (term {term})",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

        # Broadcast DC leader change to peer gates
        self._task_runner.run(
            self._broadcast_dc_leader_announcement,
            datacenter,
            leader_node_id,
            leader_tcp_addr,
            leader_udp_addr,
            term,
        )

    async def _broadcast_dc_leader_announcement(
        self,
        datacenter: str,
        leader_node_id: str,
        leader_tcp_addr: tuple[str, int],
        leader_udp_addr: tuple[str, int],
        term: int,
    ) -> None:
        """
        Broadcast a DC leader announcement to all peer gates.

        Ensures all gates in the cluster learn about DC leadership changes,
        even if they don't directly observe the change via probes.
        """
        if not self._modular_state.has_active_peers():
            return

        announcement = DCLeaderAnnouncement(
            datacenter=datacenter,
            leader_node_id=leader_node_id,
            leader_tcp_addr=leader_tcp_addr,
            leader_udp_addr=leader_udp_addr,
            term=term,
        )

        broadcast_count = 0
        for peer_addr in self._modular_state.iter_active_peers():
            if await self._peer_gate_circuit_breaker.is_circuit_open(peer_addr):
                await self._udp_logger.log(
                    ServerDebug(
                        message=f"Skipping DC leader announcement to peer {peer_addr} due to open circuit",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                continue

            circuit = await self._peer_gate_circuit_breaker.get_circuit(peer_addr)
            try:
                response, _ = await self.send_tcp(
                    peer_addr,
                    "dc_leader_announcement",
                    announcement.dump(),
                    timeout=self._tcp_timeout_short,
                )
                # send_tcp returns transport errors rather than raising.
                if isinstance(response, Exception):
                    raise response
                circuit.record_success()
                broadcast_count += 1
            except Exception as error:
                circuit.record_failure()
                await self._udp_logger.log(
                    ServerDebug(
                        message=f"Failed DC leader announcement to {peer_addr}: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )

        if broadcast_count > 0:
            await self._udp_logger.log(
                ServerInfo(
                    message=f"Broadcast DC {datacenter} leader change to {broadcast_count} peer gates",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    async def _forward_job_final_result_to_peers(self, data: bytes) -> bool:
        for gate_id, gate_info in list(self._modular_state.iter_known_gates()):
            if gate_id == self._node_id.full:
                continue

            gate_addr = (gate_info.tcp_host, gate_info.tcp_port)
            if await self._peer_gate_circuit_breaker.is_circuit_open(gate_addr):
                continue

            circuit = await self._peer_gate_circuit_breaker.get_circuit(gate_addr)
            try:
                response, _ = await self.send_tcp(
                    gate_addr,
                    "job_final_result_forwarded",
                    data,
                    timeout=self._tcp_timeout_forward,
                )
                # send_tcp returns transport errors rather than raising.
                if isinstance(response, Exception):
                    raise response
                if response in (b"ok", b"already_completed"):
                    circuit.record_success()
                    return True
            except Exception as forward_error:
                circuit.record_failure()
                await self._udp_logger.log(
                    ServerDebug(
                        message=f"Failed to forward job final result to gate: {forward_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                continue

        return False

    async def _forward_job_status_push_to_peers(
        self,
        job_id: str,
        push_data: bytes,
    ) -> bool:
        """
        Forward job status push to peer gates for delivery reliability.

        Used when direct client delivery fails after retries. Peers may have
        a better route to the client or can store-and-forward when the client
        reconnects.
        """
        for gate_id, gate_info in list(self._modular_state.iter_known_gates()):
            if gate_id == self._node_id.full:
                continue

            gate_addr = (gate_info.tcp_host, gate_info.tcp_port)
            if await self._peer_gate_circuit_breaker.is_circuit_open(gate_addr):
                continue

            circuit = await self._peer_gate_circuit_breaker.get_circuit(gate_addr)
            try:
                response, _ = await self.send_tcp(
                    gate_addr,
                    "job_status_push_forward",
                    push_data,
                    timeout=self._tcp_timeout_forward,
                )
                # send_tcp returns transport errors rather than raising.
                if isinstance(response, Exception):
                    raise response
                if response in (b"ok", None):
                    circuit.record_success()
                    return True
            except Exception as forward_error:
                circuit.record_failure()
                await self._udp_logger.log(
                    ServerDebug(
                        message=f"Failed to forward job status push for {job_id} to gate {gate_id}: {forward_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                continue

        return False

    async def _schedule_workflow_result_timeout(
        self,
        job_id: str,
        workflow_id: str,
    ) -> None:
        if self._workflow_result_timeout_seconds <= 0:
            return

        async with self._workflow_dc_results_lock:
            job_tokens = self._workflow_result_timeout_tokens.setdefault(job_id, {})
            if workflow_id in job_tokens:
                return

            run = self._task_runner.run(
                self._workflow_result_timeout_wait,
                job_id,
                workflow_id,
                alias=f"workflow-result-timeout-{job_id}-{workflow_id}",
            )
            if run is None:
                return
            job_tokens[workflow_id] = run.token

    def _pop_workflow_timeout_token_locked(
        self,
        job_id: str,
        workflow_id: str,
    ) -> str | None:
        job_tokens = self._workflow_result_timeout_tokens.get(job_id)
        if not job_tokens:
            return None

        token = job_tokens.pop(workflow_id, None)
        if not job_tokens:
            self._workflow_result_timeout_tokens.pop(job_id, None)
        return token

    async def _cancel_workflow_result_timeout(self, token: str) -> None:
        try:
            await self._task_runner.cancel(token)
        except Exception as cancel_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to cancel workflow result timeout: {cancel_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _cancel_workflow_result_timeouts(
        self, tokens: dict[str, str] | None
    ) -> None:
        if not tokens:
            return

        for token in tokens.values():
            await self._cancel_workflow_result_timeout(token)

    async def _workflow_result_timeout_wait(
        self,
        job_id: str,
        workflow_id: str,
    ) -> None:
        try:
            await self._clock.sleep(self._workflow_result_timeout_seconds)
        except asyncio.CancelledError:
            return

        await self._udp_logger.log(
            ServerWarning(
                message=(
                    "Workflow result timeout expired for job "
                    f"{job_id} workflow {workflow_id}"
                ),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        await self._handle_workflow_result_timeout(job_id, workflow_id)

    def _build_missing_workflow_result(
        self,
        job_id: str,
        workflow_id: str,
        workflow_name: str,
        datacenter: str,
        fence_token: int,
        is_test_workflow: bool,
    ) -> WorkflowResultPush:
        return WorkflowResultPush(
            job_id=job_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            datacenter=datacenter,
            status="FAILED",
            fence_token=fence_token,
            results=[],
            error=f"Timed out waiting for workflow result from DC {datacenter}",
            elapsed_seconds=0.0,
            completed_at=self._clock.time(),
            is_test=is_test_workflow,
            **self._data_plane_provenance(job_id),
        )

    async def _handle_workflow_result_timeout(
        self,
        job_id: str,
        workflow_id: str,
    ) -> None:
        async with self._workflow_dc_results_lock:
            expected_dc_count = self._workflow_result_expected_dc_counts.get(
                job_id,
                {},
            ).get(workflow_id, 0)
            workflow_results, _ = self._pop_workflow_results_locked(
                job_id,
                workflow_id,
            )
        if not workflow_results:
            return

        # Any stored result names the workflow, counted or not.
        stored_results = workflow_results
        expected_dcs = self._job_manager.expected_workflow_datacenters(job_id, workflow_id)
        if expected_dcs:
            # Stored before the job moved off its datacenter.
            workflow_results = {
                datacenter: result
                for datacenter, result in stored_results.items()
                if datacenter in expected_dcs
            }
        missing_dcs = set(expected_dcs) - set(workflow_results.keys())
        if not missing_dcs and expected_dc_count > len(workflow_results):
            missing_count = expected_dc_count - len(workflow_results)
            missing_dcs = {
                f"unknown-dc-{ordinal}"
                for ordinal in range(1, missing_count + 1)
            }

        if missing_dcs:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Workflow results timed out for job {job_id} workflow {workflow_id}; "
                        f"missing DCs: {sorted(missing_dcs)}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            first_push = next(iter(stored_results.values()))
            fence_token = max(
                dc_push.fence_token for dc_push in stored_results.values()
            )
            for datacenter in missing_dcs:
                workflow_results[datacenter] = self._build_missing_workflow_result(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    workflow_name=first_push.workflow_name,
                    datacenter=datacenter,
                    fence_token=fence_token,
                    is_test_workflow=first_push.is_test,
                )

        await self._forward_aggregated_workflow_result(
            job_id, workflow_id, workflow_results
        )

    def _pop_workflow_results_locked(
        self,
        job_id: str,
        workflow_id: str,
    ) -> tuple[dict[str, WorkflowResultPush], str | None]:
        """Take a workflow's per-datacenter results for its one aggregate:
        the workflow is finalized here, under the results lock, so a push
        arriving while the aggregate is recorded is acked, not stored to be
        aggregated a second time. Caller holds ``_workflow_dc_results_lock``."""
        self._finalized_workflow_results.add((job_id, workflow_id))
        job_results = self._workflow_dc_results.get(job_id, {})
        workflow_results = job_results.pop(workflow_id, {})
        if not job_results and job_id in self._workflow_dc_results:
            del self._workflow_dc_results[job_id]

        expected_counts = self._workflow_result_expected_dc_counts.get(job_id, {})
        expected_counts.pop(workflow_id, None)
        if not expected_counts and job_id in self._workflow_result_expected_dc_counts:
            del self._workflow_result_expected_dc_counts[job_id]

        timeout_token = self._pop_workflow_timeout_token_locked(job_id, workflow_id)
        return workflow_results, timeout_token

    async def _pop_workflow_results(
        self, job_id: str, workflow_id: str
    ) -> tuple[dict[str, WorkflowResultPush], str | None]:
        async with self._workflow_dc_results_lock:
            return self._pop_workflow_results_locked(job_id, workflow_id)

    def _build_per_dc_result(
        self,
        datacenter: str,
        dc_push: WorkflowResultPush,
        is_test_workflow: bool,
        workflow_stats: list[WorkflowStats],
        rerun_of: str,
    ) -> WorkflowDCResult:
        if is_test_workflow:
            dc_aggregated_stats: WorkflowStats | None = None
            if len(workflow_stats) > 1:
                dc_aggregated_stats = Results().merge_results(workflow_stats)
            elif workflow_stats:
                dc_aggregated_stats = workflow_stats[0]

            return WorkflowDCResult(
                datacenter=datacenter,
                status=dc_push.status,
                stats=dc_aggregated_stats,
                error=dc_push.error,
                elapsed_seconds=dc_push.elapsed_seconds,
                rerun_of=rerun_of,
            )

        return WorkflowDCResult(
            datacenter=datacenter,
            status=dc_push.status,
            stats=None,
            error=dc_push.error,
            elapsed_seconds=dc_push.elapsed_seconds,
            raw_results=workflow_stats,
            rerun_of=rerun_of,
        )

    def _normalize_workflow_result_stats(self, results: object) -> list[WorkflowStats]:
        if isinstance(results, bytes):
            if not results:
                return []
            raise ValueError(
                "WorkflowResultPush.results must be list[WorkflowStats], "
                "got non-empty bytes"
            )

        if not isinstance(results, list):
            raise TypeError(
                "WorkflowResultPush.results must be list[WorkflowStats], "
                f"got {type(results).__name__}"
            )

        return results

    def _aggregate_workflow_results(
        self,
        job_id: str,
        workflow_results: dict[str, WorkflowResultPush],
        is_test_workflow: bool,
    ) -> tuple[
        list[WorkflowStats],
        list[WorkflowDCResult],
        str,
        bool,
        list[str],
        float,
        int,
        int,
    ]:
        all_workflow_stats: list[WorkflowStats] = []
        per_dc_results: list[WorkflowDCResult] = []
        workflow_name = ""
        has_failure = False
        error_messages: list[str] = []
        max_elapsed = 0.0
        completed_datacenters = 0
        failed_datacenters = 0

        for datacenter, dc_push in workflow_results.items():
            workflow_name = dc_push.workflow_name
            dc_workflow_stats = self._normalize_workflow_result_stats(dc_push.results)
            all_workflow_stats.extend(dc_workflow_stats)

            per_dc_results.append(
                self._build_per_dc_result(
                    datacenter,
                    dc_push,
                    is_test_workflow,
                    dc_workflow_stats,
                    # A replacement holds only the slots its lost datacenter
                    # passed on: each result it delivers re-ran that share.
                    self._job_manager.rerun_origin(job_id, datacenter) or "",
                )
            )

            status_value = dc_push.status.upper()
            if status_value == "COMPLETED":
                completed_datacenters += 1
            else:
                failed_datacenters += 1
                has_failure = True
                if dc_push.error:
                    error_messages.append(f"{datacenter}: {dc_push.error}")

            if dc_push.elapsed_seconds > max_elapsed:
                max_elapsed = dc_push.elapsed_seconds

        return (
            all_workflow_stats,
            per_dc_results,
            workflow_name,
            has_failure,
            error_messages,
            max_elapsed,
            completed_datacenters,
            failed_datacenters,
        )

    def _prepare_final_results(
        self, all_workflow_stats: list[WorkflowStats], is_test_workflow: bool
    ) -> list[WorkflowStats]:
        if not all_workflow_stats:
            return []

        if is_test_workflow:
            aggregator = Results()
            if len(all_workflow_stats) > 1:
                return [aggregator.merge_results(all_workflow_stats)]
            return [all_workflow_stats[0]]
        return all_workflow_stats

    def _collect_job_workflow_stats(
        self, per_dc_results: list[JobFinalResult]
    ) -> list[WorkflowStats]:
        workflow_stats: list[WorkflowStats] = []
        for dc_result in per_dc_results:
            for workflow_result in dc_result.workflow_results:
                workflow_stats.extend(workflow_result.results)
        return workflow_stats

    def _collect_timing_stats(
        self, workflow_stats: list[WorkflowStats]
    ) -> list[dict[str, float | int]]:
        timing_stats: list[dict[str, float | int]] = []
        for workflow_stat in workflow_stats:
            results = workflow_stat.get("results")
            if not isinstance(results, list):
                continue
            for result_set in results:
                if not isinstance(result_set, dict):
                    continue
                timings = result_set.get("timings")
                if not isinstance(timings, dict):
                    continue
                for timing_stat in timings.values():
                    if isinstance(timing_stat, dict):
                        timing_stats.append(timing_stat)
        return timing_stats

    def _extract_timing_metric(
        self,
        timing_stats: dict[str, float | int],
        keys: tuple[str, ...],
    ) -> float | None:
        for key in keys:
            if isinstance((value := timing_stats.get(key)), (int, float)):
                return float(value)
        return None

    def _median_timing_metric(
        self,
        timing_stats: list[dict[str, float | int]],
        keys: tuple[str, ...],
    ) -> float:
        values = [
            value
            for timing_stat in timing_stats
            if (value := self._extract_timing_metric(timing_stat, keys)) is not None
        ]
        if not values:
            return 0.0
        return float(statistics.median(values))

    def _build_aggregated_job_stats(
        self, per_dc_results: list[JobFinalResult]
    ) -> AggregatedJobStats:
        total_completed = sum(result.total_completed for result in per_dc_results)
        total_failed = sum(result.total_failed for result in per_dc_results)
        total_requests = total_completed + total_failed

        all_workflow_stats = self._collect_job_workflow_stats(per_dc_results)
        timing_stats = self._collect_timing_stats(all_workflow_stats)

        average_latency_ms = self._median_timing_metric(
            timing_stats,
            ("mean", "avg", "average"),
        )
        p50_latency_ms = self._median_timing_metric(
            timing_stats,
            ("p50", "med", "median"),
        )
        p95_latency_ms = self._median_timing_metric(timing_stats, ("p95",))
        p99_latency_ms = self._median_timing_metric(timing_stats, ("p99",))
        if average_latency_ms <= 0.0 and p50_latency_ms > 0.0:
            average_latency_ms = p50_latency_ms

        overall_rate = sum(
            float(workflow_stat["aps"])
            for workflow_stat in all_workflow_stats
            if isinstance(workflow_stat.get("aps"), (int, float))
        )

        return AggregatedJobStats(
            total_requests=total_requests,
            successful_requests=total_completed,
            failed_requests=total_failed,
            overall_rate=overall_rate,
            avg_latency_ms=average_latency_ms,
            p50_latency_ms=p50_latency_ms,
            p95_latency_ms=p95_latency_ms,
            p99_latency_ms=p99_latency_ms,
        )

    def _normalize_final_status(self, status: str) -> str:
        normalized = status.strip().lower()
        if normalized in ("timeout", "timed_out"):
            return JobStatus.TIMEOUT.value
        if normalized in ("cancelled", "canceled"):
            return JobStatus.CANCELLED.value
        if normalized in (JobStatus.COMPLETED.value, JobStatus.FAILED.value):
            return normalized
        return JobStatus.FAILED.value

    def _should_finalize_partial_results(self, normalized_statuses: list[str]) -> bool:
        terminal_overrides = {
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }
        return any(status in terminal_overrides for status in normalized_statuses)

    def _resolve_global_result_status(self, normalized_statuses: list[str]) -> str:
        if JobStatus.FAILED.value in normalized_statuses:
            return JobStatus.FAILED.value
        if JobStatus.CANCELLED.value in normalized_statuses:
            return JobStatus.CANCELLED.value
        if JobStatus.TIMEOUT.value in normalized_statuses:
            return JobStatus.TIMEOUT.value
        if normalized_statuses and all(
            status == JobStatus.COMPLETED.value for status in normalized_statuses
        ):
            return JobStatus.COMPLETED.value
        return JobStatus.FAILED.value

    def _build_missing_dc_result(
        self, job_id: str, datacenter: str, fence_token: int
    ) -> JobFinalResult:
        return JobFinalResult(
            job_id=job_id,
            datacenter=datacenter,
            status=JobStatus.TIMEOUT.value,
            workflow_results=[],
            total_completed=0,
            total_failed=0,
            errors=[f"Missing final result from DC {datacenter}"],
            elapsed_seconds=0.0,
            fence_token=fence_token,
            **self._data_plane_provenance(job_id),
        )

    def _build_global_job_result(
        self,
        job_id: str,
        per_dc_results: dict[str, JobFinalResult],
        target_dcs: set[str],
    ) -> GlobalJobResult:
        expected_dcs = target_dcs or set(per_dc_results.keys())
        missing_dcs = expected_dcs - set(per_dc_results.keys())
        max_fence_token = max(
            (result.fence_token for result in per_dc_results.values()),
            default=0,
        )

        ordered_results: list[JobFinalResult] = []
        errors: list[str] = []
        successful_datacenters = 0
        failed_datacenters = 0
        max_elapsed = 0.0
        normalized_statuses: list[str] = []

        for datacenter in sorted(per_dc_results.keys()):
            dc_result = per_dc_results[datacenter]
            ordered_results.append(dc_result)

            status_value = self._normalize_final_status(dc_result.status)
            normalized_statuses.append(status_value)
            if status_value == JobStatus.COMPLETED.value:
                successful_datacenters += 1
            else:
                failed_datacenters += 1
                if dc_result.errors:
                    errors.extend(
                        [f"{datacenter}: {error}" for error in dc_result.errors]
                    )
                else:
                    errors.append(
                        f"{datacenter}: reported status {dc_result.status} "
                        "without error details"
                    )

            if dc_result.elapsed_seconds > max_elapsed:
                max_elapsed = dc_result.elapsed_seconds

        for datacenter in sorted(missing_dcs):
            missing_result = self._build_missing_dc_result(
                job_id, datacenter, max_fence_token
            )
            ordered_results.append(missing_result)
            failed_datacenters += 1
            errors.append(f"{datacenter}: missing final result")
            normalized_statuses.append(
                self._normalize_final_status(missing_result.status)
            )

        # AD-36: a datacenter lost mid-job did its work until it was lost
        # -- counted with the rest -- and its replacement's final result
        # stands among the others for its share.
        substitutions = self._job_manager.get_datacenter_substitutions(job_id)
        total_completed = sum(result.total_completed for result in ordered_results) + sum(
            substitution.total_completed for substitution in substitutions
        )
        total_failed = sum(result.total_failed for result in ordered_results) + sum(
            substitution.total_failed for substitution in substitutions
        )

        status = self._resolve_global_result_status(normalized_statuses)

        aggregated_stats = self._build_aggregated_job_stats(ordered_results)
        per_datacenter_statuses = {
            result.datacenter: result.status for result in ordered_results
        }

        return GlobalJobResult(
            job_id=job_id,
            status=status,
            per_datacenter_results=ordered_results,
            per_datacenter_statuses=per_datacenter_statuses,
            aggregated=aggregated_stats,
            total_completed=total_completed,
            total_failed=total_failed,
            successful_datacenters=successful_datacenters,
            failed_datacenters=failed_datacenters,
            errors=errors,
            elapsed_seconds=max_elapsed,
            datacenter_substitutions=substitutions,
        )

    async def _on_job_dispatched(
        self,
        submission: JobSubmission,
        dispatched_datacenters: list[str],
    ) -> None:
        """The job is placed. Its datacenters -- those that took it, which
        a fallback or spillover may have changed -- and those its dispatch
        released go to peer gates in its replica, so a gate taking it over
        awaits results from where it runs; per-workflow results a dropped
        slot left complete are aggregated; AD-44 tracking starts."""
        job_id = submission.job_id
        await self._replicate_job_placement(
            job_id,
            sorted(self._job_manager.get_target_dcs(job_id)),
            self._job_manager.get_datacenter_substitutions(job_id),
            sorted(self._job_manager.get_released_datacenters(job_id)),
        )
        await self._aggregate_complete_workflow_results(job_id)
        await self._start_best_effort_tracking(submission, dispatched_datacenters)

    async def _replicate_job_placement(
        self,
        job_id: str,
        target_dcs: list[str],
        substitutions: list[DatacenterSubstitution],
        released_datacenters: list[str],
    ) -> bool:
        """Commit a placement of the job -- its datacenters, its lost
        datacenters' substitutions, the datacenters it released -- to a
        quorum of peer gates (True when they hold it already). Built on
        the replica committed last, with the job's current status (the
        replica's own would roll this gate's back); committed, it is this
        gate's placement too."""
        job = self._job_manager.get_job(job_id)
        if job is None or self._replication_coordinator.get_committed_replica(job_id) is None:
            return False

        def revise(committed: GateJobReplica) -> GateJobReplica | None:
            if (
                sorted(committed.target_dcs),
                committed.datacenter_substitutions,
                sorted(committed.released_datacenters),
            ) == (target_dcs, substitutions, released_datacenters):
                return None
            return dataclasses.replace(
                committed,
                target_dcs=target_dcs,
                target_dc_count=len(target_dcs),
                datacenter_substitutions=substitutions,
                released_datacenters=released_datacenters,
                status_seed=job.status,
            )

        replicated = await self._replication_coordinator.revise_committed_replica(
            job_id,
            revise,
            peer_addrs=list(self._modular_state.get_active_peers_list()),
            quorum_size=self._quorum_size(),
        )
        if not replicated:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Placement of job {job_id[:8]}... (datacenters {target_dcs}) "
                        "did not reach a quorum of peer gates"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        return replicated

    async def _delivered_workflow_ids(self, job_id: str, datacenter: str) -> set[str]:
        """The job's workflows whose results need nothing more from
        ``datacenter``: those it delivered a result for, and those already
        aggregated (with or without it)."""
        async with self._workflow_dc_results_lock:
            return {
                workflow_id
                for workflow_id, datacenter_results in self._workflow_dc_results.get(
                    job_id, {}
                ).items()
                if datacenter in datacenter_results
            } | {
                workflow_id
                for finalized_job_id, workflow_id in self._finalized_workflow_results
                if finalized_job_id == job_id
            }

    async def _release_workflow_result_timeouts(
        self, job_id: str, workflow_ids: set[str]
    ) -> None:
        """Stop the per-workflow result timeouts of these workflows of the
        job: their result slots moved to a datacenter re-running them."""
        async with self._workflow_dc_results_lock:
            timeout_tokens = [
                timeout_token
                for workflow_id in workflow_ids
                if (
                    timeout_token := self._pop_workflow_timeout_token_locked(
                        job_id, workflow_id
                    )
                )
                is not None
            ]
        for timeout_token in timeout_tokens:
            await self._cancel_workflow_result_timeout(timeout_token)

    async def _aggregate_complete_workflow_results(self, job_id: str) -> None:
        """Aggregate each of the job's stored workflow results whose
        datacenters have all reported. After the job's placement changed,
        a workflow's last awaited result may be one stored already, or
        from a slot that is gone -- and no push is coming to aggregate it
        but its timeout's."""
        ready_workflows: list[tuple[str, dict[str, WorkflowResultPush], str | None]] = []
        async with self._workflow_dc_results_lock:
            for workflow_id, datacenter_results in list(
                self._workflow_dc_results.get(job_id, {}).items()
            ):
                expected_dcs = self._job_manager.expected_workflow_datacenters(
                    job_id, workflow_id
                )
                if not expected_dcs or not set(datacenter_results) >= expected_dcs:
                    continue
                workflow_results, timeout_token = self._pop_workflow_results_locked(
                    job_id, workflow_id
                )
                ready_workflows.append(
                    (
                        workflow_id,
                        {
                            datacenter: result
                            for datacenter, result in workflow_results.items()
                            if datacenter in expected_dcs
                        },
                        timeout_token,
                    )
                )

        for workflow_id, workflow_results, timeout_token in ready_workflows:
            if timeout_token:
                await self._cancel_workflow_result_timeout(timeout_token)
            await self._forward_aggregated_workflow_result(
                job_id, workflow_id, workflow_results
            )

    async def _start_best_effort_tracking(
        self,
        submission: JobSubmission,
        dispatched_datacenters: list[str],
    ) -> None:
        """AD-44: track a best-effort job over the datacenters that accepted it."""
        if not submission.best_effort or not dispatched_datacenters:
            return
        await self._best_effort_manager.create_state(
            job_id=submission.job_id,
            min_dcs=submission.best_effort_min_dcs,
            deadline_seconds=submission.best_effort_deadline_seconds,
            target_dcs=set(dispatched_datacenters),
        )

    async def _resume_best_effort_tracking(
        self,
        job_id: str,
        submission: JobSubmission | None,
        target_dcs: list[str],
    ) -> None:
        """AD-44 on gate takeover: keep a best-effort job best-effort.

        The new leader gate holds the replicated submission but neither the
        previous leader's tracking nor the datacenter results it received
        (those are not replicated). It tracks the job afresh over its
        datacenters: results arriving from here on count, and the deadline
        runs from the takeover -- never shorter than the job asked for.
        """
        if submission is None or self._best_effort_manager.has_state(job_id):
            return
        await self._start_best_effort_tracking(submission, target_dcs)

    def _build_best_effort_global_result(
        self,
        job_id: str,
        per_dc_results: dict[str, JobFinalResult],
        reason: str,
        success: bool,
    ) -> GlobalJobResult:
        """AD-44: the result of a best-effort job from the datacenters that
        reported. It is COMPLETED when the policy judged it a success --
        even with a failed datacenter among them -- and FAILED otherwise;
        datacenters that never reported are listed, not counted as
        timeouts."""
        reported_result = self._build_global_job_result(
            job_id, per_dc_results, set(per_dc_results)
        )
        unreported = sorted(
            (
                self._best_effort_manager.get_target_dcs(job_id)
                or set(self._job_manager.get_target_dcs(job_id))
            )
            - set(per_dc_results)
        )
        return dataclasses.replace(
            reported_result,
            status=JobStatus.COMPLETED.value if success else JobStatus.FAILED.value,
            completion_reason=f"best_effort: {reason}",
            unreported_datacenters=unreported,
        )

    async def _complete_best_effort_job(
        self,
        job_id: str,
        reason: str,
        success: bool,
    ) -> None:
        """AD-44 deadline: complete a best-effort job with what reported."""
        async with self._job_manager.lock_job(job_id):
            job = self._job_manager.get_job(job_id)
            if (
                job is None
                or JobStatusOrder().is_terminal(job.status)
                or job_id in self._job_completion_claimed
            ):
                await self._best_effort_manager.cleanup(job_id)
                return
            self._job_completion_claimed.add(job_id)
            previous_status = job.status
            global_result = self._build_best_effort_global_result(
                job_id,
                self._job_manager.get_all_dc_results(job_id),
                reason,
                success,
            )

        await self._finish_job_with_global_result(job_id, previous_status, global_result)

    async def _abandon_unreported_datacenters(
        self,
        job_id: str,
        datacenters: list[str],
        reason: str,
    ) -> None:
        """AD-44: the job completed without these datacenters -- cancel what
        they still run (the per-workflow results that were waiting on them
        were released when the job completed)."""
        manager_addresses = dict(self._modular_state.get_job_dc_managers(job_id))
        await self._cancel_job_for_timeout(job_id, reason, datacenters, manager_addresses)

    async def _release_workflow_results(self, job_id: str) -> None:
        """Deliver every per-workflow result of the job still waiting on a
        datacenter, from the datacenters that reported."""
        for workflow_id in list(self._workflow_dc_results.get(job_id, {})):
            await self._aggregate_and_forward_workflow_result(job_id, workflow_id)

    async def _record_job_final_result(
        self, result: JobFinalResult
    ) -> GlobalJobResult | None:
        async with self._job_manager.lock_job(result.job_id):
            if not self._job_manager.has_job(result.job_id):
                return None

            # Gate-leader fence does NOT apply to manager-originated
            # final results. The takeover bump exists to fence stale
            # gate-leader claims; data-plane terminal results from the
            # manager are validated separately. Mixing the two drops
            # legitimate completed work whenever a gate takeover
            # landed before the manager learned the new fence.

            target_dcs = set(self._job_manager.get_target_dcs(result.job_id))
            if target_dcs and result.datacenter not in target_dcs:
                # A datacenter the job moved off -- lost and replaced, or
                # released at dispatch: its result is not the job's (the
                # replacement's is), and it was told to stop.
                await self._udp_logger.log(
                    ServerInfo(
                        message=(
                            f"Dropped final result of job {result.job_id[:8]}... from "
                            f"DC {result.datacenter}: the job runs in {sorted(target_dcs)}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return None

            self._job_manager.set_dc_result(result.job_id, result.datacenter, result)

            if result.job_id in self._job_global_result_sent:
                return None

            per_dc_results = self._job_manager.get_all_dc_results(result.job_id)
            # AD-44: a best-effort job completes on its own policy -- a
            # failed datacenter does not end it while others may still
            # complete it.
            if (
                decision := await self._best_effort_manager.record_result(
                    result.job_id,
                    result.datacenter,
                    self._normalize_final_status(result.status) == JobStatus.COMPLETED.value,
                )
            ) is not None:
                should_complete, reason, success = decision
                if not should_complete:
                    return None
                return self._build_best_effort_global_result(
                    result.job_id, per_dc_results, reason, success
                )

            missing_dcs = target_dcs - set(per_dc_results.keys())
            if target_dcs and missing_dcs:
                normalized_statuses = [
                    self._normalize_final_status(dc_result.status)
                    for dc_result in per_dc_results.values()
                ]
                if not self._should_finalize_partial_results(normalized_statuses):
                    return None

        return self._build_global_job_result(result.job_id, per_dc_results, target_dcs)

    async def _aggregate_and_forward_workflow_result(
        self,
        job_id: str,
        workflow_id: str,
    ) -> None:
        workflow_results, timeout_token = await self._pop_workflow_results(
            job_id, workflow_id
        )
        if timeout_token:
            await self._cancel_workflow_result_timeout(timeout_token)
        if not workflow_results:
            return

        await self._forward_aggregated_workflow_result(
            job_id, workflow_id, workflow_results
        )

    async def _forward_aggregated_workflow_result(
        self,
        job_id: str,
        workflow_id: str,
        workflow_results: dict[str, WorkflowResultPush],
    ) -> None:
        """Record a workflow's one aggregate for the job's client and send
        it on. The record is what is owed: it is replayed when the client's
        callback (re)registers, so a send that fails -- or a job with no
        callback yet -- loses nothing (the results it was built from are
        already taken)."""
        first_dc_push = next(iter(workflow_results.values()))
        is_test_workflow = first_dc_push.is_test
        fence_token = max(dc_push.fence_token for dc_push in workflow_results.values())

        (
            all_workflow_stats,
            per_dc_results,
            workflow_name,
            has_failure,
            error_messages,
            max_elapsed,
            completed_datacenters,
            failed_datacenters,
        ) = self._aggregate_workflow_results(job_id, workflow_results, is_test_workflow)

        status = JobStatus.FAILED.value if has_failure else JobStatus.COMPLETED.value
        if (
            self._allow_partial_workflow_results
            and has_failure
            and completed_datacenters > 0
            and failed_datacenters > 0
        ):
            status = "partial"
        error = "; ".join(error_messages) if error_messages else None
        results_to_send = self._prepare_final_results(
            all_workflow_stats, is_test_workflow
        )
        callback = self._resolve_job_callback(job_id, first_dc_push.callback_addr)

        client_push = WorkflowResultPush(
            job_id=job_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            datacenter="aggregated",
            status=status,
            fence_token=fence_token,
            results=results_to_send,
            error=error,
            elapsed_seconds=max_elapsed,
            per_dc_results=per_dc_results,
            completed_at=self._clock.time(),
            is_test=is_test_workflow,
            callback_addr=callback,
            is_client_ready=True,
            **self._data_plane_provenance(job_id),
        )

        payload = client_push.dump()
        sequence = await self._modular_state.record_client_update(
            job_id,
            "workflow_result_push",
            payload,
            self._clock.monotonic(),
        )
        if callback is None:
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Workflow result for {job_id} recorded with no callback "
                        "to send it to: it goes out when one registers"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return

        self._record_job_callback(job_id, callback)
        if not await self._deliver_client_update(
            job_id,
            callback,
            sequence,
            "workflow_result_push",
            payload,
            timeout=self._tcp_timeout_standard,
            log_failure=False,
        ):
            await self._udp_logger.log(
                ServerWarning(
                    message=(
                        f"Failed to send workflow result to client {callback}: "
                        "it is replayed when its callback re-registers"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _query_all_datacenters(
        self,
        request: WorkflowQueryRequest,
    ) -> dict[str, list[WorkflowStatusInfo]]:
        """Query all datacenter managers for workflow status."""
        dc_results: dict[str, list[WorkflowStatusInfo]] = {}

        async def query_dc(dc_id: str, manager_addr: tuple[str, int]) -> None:
            try:
                response_data, _ = await self.send_tcp(
                    manager_addr,
                    "workflow_query",
                    request.dump(),
                    timeout=self._tcp_timeout_standard,
                )
                # send_tcp returns transport errors rather than raising.
                if isinstance(response_data, Exception):
                    raise response_data
                if response_data == b"error":
                    return

                manager_response = WorkflowQueryResponse.load(response_data)
                dc_results[dc_id] = manager_response.workflows

            except Exception as query_error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to query workflows from manager: {query_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

        job_dc_managers = (
            self._modular_state.get_job_dc_managers(request.job_id) if request.job_id else {}
        )

        query_tasks = []
        for dc_id in self._datacenter_managers.keys():
            target_addr = self._get_dc_query_target(dc_id, job_dc_managers)
            if target_addr:
                query_tasks.append(query_dc(dc_id, target_addr))

        if query_tasks:
            await asyncio.gather(*query_tasks, return_exceptions=True)

        return dc_results

    def _get_dc_query_target(
        self,
        dc_id: str,
        job_dc_managers: dict[str, tuple[str, int]],
    ) -> tuple[str, int] | None:
        """Get the best manager address to query for a datacenter."""
        if dc_id in job_dc_managers:
            return job_dc_managers[dc_id]

        manager_statuses = self._modular_state.get_datacenter_manager_statuses(dc_id)
        fallback_addr: tuple[str, int] | None = None

        for manager_addr, heartbeat in manager_statuses.items():
            if fallback_addr is None:
                fallback_addr = (heartbeat.tcp_host, heartbeat.tcp_port)

            if heartbeat.is_leader:
                return (heartbeat.tcp_host, heartbeat.tcp_port)

        return fallback_addr

    async def _wait_for_cluster_stabilization(self) -> None:
        """Wait for SWIM cluster to stabilize."""
        expected_peers = len(self._gate_udp_peers)
        if expected_peers == 0:
            return

        timeout = self.env.CLUSTER_STABILIZATION_TIMEOUT
        poll_interval = self.env.CLUSTER_STABILIZATION_POLL_INTERVAL
        start_time = self._clock.monotonic()

        while True:
            self_addr = (self._host, self._udp_port)
            visible_peers = len(
                [
                    n
                    for n in self._incarnation_tracker.node_states.keys()
                    if n != self_addr
                ]
            )

            if visible_peers >= expected_peers:
                return

            if self._clock.monotonic() - start_time >= timeout:
                return

            await self._clock.sleep(poll_interval)

    async def _complete_startup_sync(self) -> None:
        """Complete startup sync and transition to ACTIVE."""
        if self.is_leader():
            self._modular_state.set_gate_state(GateState.ACTIVE)
            return

        leader_addr = self.get_current_leader()
        if leader_addr:
            leader_tcp_addr = self._modular_state.get_tcp_addr_for_udp(leader_addr)
            if leader_tcp_addr:
                await self._sync_state_from_peer(leader_tcp_addr)

        self._modular_state.set_gate_state(GateState.ACTIVE)

    async def _sync_state_from_peer(
        self,
        peer_tcp_addr: tuple[str, int],
    ) -> bool:
        """Sync state from peer gate."""
        if await self._peer_gate_circuit_breaker.is_circuit_open(peer_tcp_addr):
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Skip state sync to peer gate {peer_tcp_addr} due to open circuit",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

        circuit = await self._peer_gate_circuit_breaker.get_circuit(peer_tcp_addr)
        try:
            request = GateStateSyncRequest(
                requester_id=self._node_id.full,
                known_version=self._modular_state.get_state_version(),
            )

            result, _ = await self.send_tcp(
                peer_tcp_addr,
                "state_sync",
                request.dump(),
                timeout=self._tcp_timeout_standard,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(result, Exception):
                raise result

            if isinstance(result, bytes) and len(result) > 0:
                response = GateStateSyncResponse.load(result)
                if response.error:
                    circuit.record_failure()
                    return False
                if response.snapshot:
                    await self._apply_gate_state_snapshot(response.snapshot)
                    circuit.record_success()
                    return True
                if response.state_version <= self._modular_state.get_state_version():
                    circuit.record_success()
                    return True
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            "State sync response missing snapshot despite newer version "
                            f"{response.state_version} > {self._modular_state.get_state_version()}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                circuit.record_failure()
                return False

            circuit.record_failure()
            return False

        except Exception as sync_error:
            circuit.record_failure()
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to sync state from peer: {sync_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

    async def _join_node(self, target_addr: tuple[str, int]) -> None:
        """Operator join: register this gate with the manager at ``target_addr``.

        Uses the manager's existing ``gate_register`` endpoint (isolation,
        protocol and role validation happen there). The accepted manager
        tracks this gate as healthy and its heartbeat loop starts sending
        ``manager_status_update`` here, which the gate ingests through the
        same canonical path as a manager registration — adding the
        manager's datacenter. ``gate_register`` exists only on managers,
        so any other target is refused by the target itself.
        """
        registration = await self._register_with_manager(target_addr)
        if not registration.accepted:
            raise ClusterJoinError(
                f"manager {target_addr[0]}:{target_addr[1]} refused registration: "
                f"{registration.error}"
            )

        await self._join_datacenter_managers(target_addr, registration)

    async def _restore_joined_managers(self) -> None:
        """Add the managers saved by earlier runs' joins to their
        datacenters' address lists."""
        if self._wal_data_dir is None:
            return

        self._joined_peer_store = JoinedPeerStore(
            Path(self._wal_data_dir),
            _DEFAULT_FILESYSTEM,
            self._udp_logger,
            self._host,
            self._tcp_port,
        )
        for joined_manager in await self._joined_peer_store.load():
            self._declare_datacenter_managers(joined_manager.datacenter, [joined_manager.tcp_address])
            manager_addrs = self._datacenter_managers.setdefault(joined_manager.datacenter, [])
            if joined_manager.tcp_address not in manager_addrs:
                manager_addrs.append(joined_manager.tcp_address)

            udp_addrs = self._datacenter_manager_udp.setdefault(joined_manager.datacenter, [])
            if joined_manager.udp_address not in udp_addrs:
                udp_addrs.append(joined_manager.udp_address)

    async def _join_datacenter_managers(
        self,
        joined_manager_addr: tuple[str, int],
        registration: GateRegistrationResponse,
    ) -> None:
        """Learn every healthy manager the joined manager reported for its
        datacenter, and register with each one not yet known, so every
        manager of the datacenter heartbeats this gate -- not only the
        one the operator named. The joined managers are declared: they
        stay expected members of the datacenter, across restarts too."""
        datacenter_id = registration.datacenter
        # Its membership is followed from here on (AD-52 section 10).
        self._watch_datacenter_membership(datacenter_id)
        manager_addrs = self._datacenter_managers.setdefault(datacenter_id, [])
        udp_addrs = self._datacenter_manager_udp.setdefault(datacenter_id, [])
        self._declare_datacenter_managers(
            datacenter_id,
            [(manager_info.tcp_host, manager_info.tcp_port) for manager_info in registration.healthy_managers],
        )

        if self._joined_peer_store is not None:
            await self._joined_peer_store.add(
                [
                    JoinedPeer(
                        datacenter=datacenter_id,
                        tcp_address=(manager_info.tcp_host, manager_info.tcp_port),
                        udp_address=(manager_info.udp_host, manager_info.udp_port),
                    )
                    for manager_info in registration.healthy_managers
                ]
            )

        for manager_info in registration.healthy_managers:
            manager_addr = (manager_info.tcp_host, manager_info.tcp_port)
            if (udp_addr := (manager_info.udp_host, manager_info.udp_port)) not in udp_addrs:
                udp_addrs.append(udp_addr)

            if manager_addr in manager_addrs:
                continue

            manager_addrs.append(manager_addr)
            if manager_addr == joined_manager_addr:
                continue

            try:
                peer_registration = await self._register_with_manager(manager_addr)
                if not peer_registration.accepted:
                    raise ClusterJoinError(
                        f"registration refused: {peer_registration.error}"
                    )

            except ClusterJoinError as register_error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            f"Joined {datacenter_id} through {joined_manager_addr}, "
                            f"but its manager {manager_addr} did not take this "
                            f"gate's registration: {register_error}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    def _watch_datacenter_membership(self, datacenter_id: str) -> None:
        """Follow ``datacenter_id``'s manager membership (AD-52 sections
        9-10) into its soft-state cache, from the managers this gate knows
        there; each change reconciles the datacenter's manager lists. Not
        once ``stop`` cleared the watches: a late heartbeat starts none."""
        if datacenter_id in self._datacenter_watches or not self._running:
            return
        request_timeout_seconds = float(self.env.GATE_TCP_TIMEOUT_STANDARD)
        cache = ClusterViewCache(
            poll_wait_seconds=self.env.CLUSTER_WATCH_WAIT_SECONDS,
            request_timeout_seconds=request_timeout_seconds,
        )
        follower = ClusterWatchFollower(
            cache,
            seeds=lambda watched_datacenter=datacenter_id: self._datacenter_managers.get(watched_datacenter, []),
            send_watch=self._send_datacenter_watch,
            clock=self._clock,
            poll_wait_seconds=self.env.CLUSTER_WATCH_WAIT_SECONDS,
            request_timeout_seconds=request_timeout_seconds,
            on_view_changed=lambda view, watched_datacenter=datacenter_id: self._task_runner.run(
                self._reconcile_datacenter_managers, watched_datacenter, view
            ),
            on_disconnected_changed=lambda disconnected, watched_datacenter=datacenter_id: self._udp_logger.log(
                ClusterWatchConnectivityChanged(
                    message=(
                        f"Lost {watched_datacenter}'s manager membership: routing it by the view last observed"
                        if disconnected
                        else f"Following {watched_datacenter}'s manager membership"
                    ),
                    node_id=self._node_id.short,
                    watched=watched_datacenter,
                    disconnected=disconnected,
                    staleness_seconds=cache.read(self._clock.monotonic())[1],
                    level=LogLevel.WARN if disconnected else LogLevel.INFO,
                )
            ),
        )
        self._datacenter_views[datacenter_id] = cache
        self._datacenter_watches[datacenter_id] = follower
        self._task_runner.run(follower.run, alias=f"datacenter-watch-{datacenter_id}")

    async def _send_datacenter_watch(
        self,
        manager_addr: tuple[str, int],
        payload: bytes,
        timeout: float,
    ) -> bytes | Exception | None:
        response, _clock = await self._send_tcp(manager_addr, "cluster_watch", payload, timeout=timeout)
        return response

    async def _reconcile_datacenter_managers(self, datacenter_id: str, view: ClusterView) -> None:
        """A datacenter's watched membership changed: its cohort -- what the
        datacenter's managers committed -- becomes the managers this gate
        routes to there. An address the cohort dropped is forgotten (here
        and for restarts); one it added is registered with, so it
        heartbeats this gate. Answering as a different cluster than before
        -- the datacenter was founded again -- forgets what this gate
        learned of its old incarnation."""
        previous_cluster_uuid = self._datacenter_cluster_uuids.get(datacenter_id)
        if view.cluster_uuid is not None:
            # Recorded before any wait, so of two reconciliations of the
            # new cluster's views only the first forgets.
            self._datacenter_cluster_uuids[datacenter_id] = view.cluster_uuid
        if previous_cluster_uuid is not None and view.cluster_uuid not in (None, previous_cluster_uuid):
            await self._observed_latency_tracker.remove_datacenter(datacenter_id)
            self._slo_health_classifier.forget(datacenter_id)
            await self._udp_logger.log(
                DatacenterRegenerated(
                    message=(
                        f"{datacenter_id} answers as cluster {view.cluster_uuid}, not {previous_cluster_uuid}: "
                        "forgot its observed latency and SLO violations"
                    ),
                    node_id=self._node_id.short,
                    datacenter_id=datacenter_id,
                    previous_cluster_uuid=previous_cluster_uuid,
                    cluster_uuid=view.cluster_uuid,
                )
            )
        if not view.cohort:
            return
        manager_addrs = self._datacenter_managers.setdefault(datacenter_id, [])
        departed = [manager_addr for manager_addr in manager_addrs if manager_addr not in view.cohort]
        arrived = [manager_addr for manager_addr in sorted(view.cohort) if manager_addr not in manager_addrs]
        self._declared_datacenter_managers[datacenter_id] = view.cohort
        if departed or arrived:
            await self._udp_logger.log(
                ServerInfo(
                    message=(
                        f"{datacenter_id}'s manager membership changed: joined "
                        f"{[f'{host}:{port}' for host, port in arrived]}, left "
                        f"{[f'{host}:{port}' for host, port in departed]}"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        for manager_addr in departed:
            self._forget_learned_manager_addresses(manager_addr)
        if departed and self._joined_peer_store is not None:
            await self._joined_peer_store.remove(departed)

        udp_addrs = self._datacenter_manager_udp.setdefault(datacenter_id, [])
        for manager_addr in arrived:
            manager_addrs.append(manager_addr)
            try:
                registration = await self._register_with_manager(manager_addr)
                if not registration.accepted:
                    raise ClusterJoinError(f"registration refused: {registration.error}")
            except ClusterJoinError as register_error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            f"{datacenter_id}'s membership added manager {manager_addr}, which did not "
                            f"take this gate's registration: {register_error}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                continue
            for manager_info in registration.healthy_managers:
                if (manager_info.tcp_host, manager_info.tcp_port) == manager_addr and (
                    udp_addr := (manager_info.udp_host, manager_info.udp_port)
                ) not in udp_addrs:
                    udp_addrs.append(udp_addr)

    async def _register_with_manager(
        self,
        manager_addr: tuple[str, int],
    ) -> GateRegistrationResponse:
        """Send this gate's registration to one manager and decode the reply.

        Raises ``ClusterJoinError`` when the manager is unreachable or its
        reply is not a registration response.
        """
        request = GateRegistrationRequest(
            node_id=self._node_id.full,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            udp_host=self._host,
            udp_port=self._udp_port,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            state=self._modular_state.get_gate_state().value,
            datacenter=self._node_id.datacenter,
            cluster_id=self.env.CLUSTER_ID,
            environment_id=self.env.ENVIRONMENT_ID,
            active_jobs=self._job_manager.job_count(),
            manager_count=sum(
                len(addrs) for addrs in self._datacenter_managers.values()
            ),
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            capabilities=",".join(
                sorted(self._node_capabilities.capabilities)
            ),
        )

        response, _ = await self.send_tcp(
            manager_addr,
            "gate_register",
            request.dump(),
            timeout=self._tcp_timeout_standard,
        )
        if isinstance(response, Exception):
            raise ClusterJoinError(
                f"manager {manager_addr[0]}:{manager_addr[1]} is unreachable: "
                f"{type(response).__name__}: {response}"
            )

        return decode_join_message(
            response,
            GateRegistrationResponse,
            f"gate registration reply from {manager_addr[0]}:{manager_addr[1]} "
            "(gates can only join managers)",
        )

    async def _register_with_managers(self) -> None:
        """Register with all managers."""
        for dc_id, manager_addrs in self._datacenter_managers.items():
            for manager_addr in manager_addrs:
                try:
                    registration = await self._register_with_manager(manager_addr)
                    if not registration.accepted:
                        raise ClusterJoinError(
                            f"registration refused: {registration.error}"
                        )

                except Exception as register_error:
                    await self._udp_logger.log(
                        ServerWarning(
                            message=f"Failed to register with manager {manager_addr}: {register_error}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

    # =========================================================================
    # Background Tasks
    # =========================================================================

    async def _lease_cleanup_loop(self) -> None:
        """Periodically clean up expired leases."""
        while self._running:
            try:
                await self._clock.sleep(self._lease_timeout / 2)
                self._dc_lease_manager.cleanup_expired()

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "lease_cleanup_loop")

    def _get_expired_terminal_jobs(self, now: float) -> list[str]:
        # Shared terminal predicate: it also knows the gate timeout
        # tracker's ``timed_out`` spelling, which a hand-written set of
        # the four enum values missed.
        status_order = JobStatusOrder()

        jobs_to_remove = []
        for job_id, job in list(self._job_manager.items()):
            if not status_order.is_terminal(job.status):
                continue
            # Retained for the max age after it ended -- measured from when
            # this gate first found it terminal, however it ended (here, or
            # learned from a peer): its timestamp is its submission.
            if now - self._job_manager.terminal_since(job_id, now) > self._job_max_age:
                jobs_to_remove.append(job_id)

        return jobs_to_remove

    async def _cleanup_single_job(self, job_id: str) -> None:
        await self._raft.consensus.destroy_job_raft(job_id)
        self._job_manager.delete_job(job_id)
        # Never released, every job's lock stayed for the gate's lifetime.
        self._job_manager.cleanup_job_lock(job_id)
        workflow_timeout_tokens: dict[str, str] | None = None
        async with self._workflow_dc_results_lock:
            self._workflow_dc_results.pop(job_id, None)
            self._workflow_result_expected_dc_counts.pop(job_id, None)
            workflow_timeout_tokens = self._workflow_result_timeout_tokens.pop(
                job_id, None
            )
            # Drop the job's finalized workflows (one row per workflow).
            for finalized_workflow in [
                finalized_workflow
                for finalized_workflow in self._finalized_workflow_results
                if finalized_workflow[0] == job_id
            ]:
                self._finalized_workflow_results.discard(finalized_workflow)
        if workflow_timeout_tokens:
            await self._cancel_workflow_result_timeouts(workflow_timeout_tokens)
        if self._job_final_statuses:
            keys_to_remove = [
                key for key in self._job_final_statuses.keys() if key[0] == job_id
            ]
            for key in keys_to_remove:
                self._job_final_statuses.pop(key, None)
        self._job_global_result_sent.discard(job_id)
        self._job_completion_claimed.discard(job_id)
        self._modular_state.clear_job(job_id)
        self._job_leadership_tracker.release_leadership(job_id)
        await self._best_effort_manager.cleanup(job_id)

        self._job_stats_crdt.pop(job_id, None)

        self._task_runner.run(self._windowed_stats.cleanup_job_windows, job_id)
        self._job_router.cleanup_job_state(job_id)

        self._modular_state.cleanup_job_progress_tracking(job_id)
        await self._modular_state.cleanup_job_update_state(job_id)
        self._replication_coordinator.clear_for_job(job_id)
        self._job_failover_coordinator.forget_job(job_id)

    async def _job_cleanup_loop(self) -> None:
        while self._running:
            try:
                await self._clock.sleep(self._job_cleanup_interval)

                now = self._clock.monotonic()
                jobs_to_remove = self._get_expired_terminal_jobs(now)

                for job_id in jobs_to_remove:
                    await self._cleanup_single_job(job_id)

                # Prepared replicas whose leader died mid-prepare (no
                # commit or abort will come) and expired commit-rollback
                # records: retained at most TTL + one cleanup interval.
                await self._replication_coordinator.reap_expired_prepared()

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "job_cleanup_loop")

    async def _rate_limit_cleanup_loop(self) -> None:
        """Periodically clean up rate limiter."""
        while self._running:
            try:
                await self._clock.sleep(self._rate_limit_cleanup_interval)
                await self._rate_limiter.cleanup_inactive_clients()
            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "rate_limit_cleanup_loop")

    async def _batch_stats_loop(self) -> None:
        """Background loop for batch stats updates."""
        while self._running:
            try:
                await self._clock.sleep(self._batch_stats_interval)
                if not self._running:
                    break
                await self._batch_stats_update()
            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "batch_stats_loop")

    async def _batch_stats_update(self) -> None:
        """Process batch stats update."""
        await self._stats_coordinator.batch_stats_update()

    async def _windowed_stats_push_loop(self) -> None:
        """Background loop for windowed stats push."""
        while self._running:
            try:
                await self._clock.sleep(self._stats_push_interval_ms / 1000.0)
                if not self._running:
                    break
                await self._stats_coordinator.push_windowed_stats()
            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "windowed_stats_push_loop")

    async def _resource_sampling_loop(self) -> None:
        """
        Background loop for periodic CPU/memory sampling.

        Samples gate resource usage and feeds HybridOverloadDetector for overload
        state classification, every OVERLOAD_SAMPLE_INTERVAL_SECONDS.
        """
        sample_interval = self.env.OVERLOAD_SAMPLE_INTERVAL_SECONDS

        while self._running:
            try:
                await self._clock.sleep(sample_interval)

                metrics = await self._resource_monitor.sample()
                self._last_resource_metrics = metrics

                new_state = self._overload_detector.get_state(
                    metrics.cpu_percent,
                    metrics.memory_percent,
                )
                new_state_str = new_state.value

                if new_state_str != self._gate_health_state:
                    self._previous_gate_health_state = self._gate_health_state
                    self._gate_health_state = new_state_str
                    await self._log_gate_health_transition(
                        self._previous_gate_health_state,
                        new_state_str,
                    )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Resource sampling error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _log_gate_health_transition(self, previous_state: str, new_state: str) -> None:
        state_severity = {"healthy": 0, "busy": 1, "stressed": 2, "overloaded": 3}
        previous_severity = state_severity.get(previous_state, 0)
        new_severity = state_severity.get(new_state, 0)
        is_degradation = new_severity > previous_severity

        if is_degradation:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Gate health degraded: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
        else:
            await self._udp_logger.log(
                ServerDebug(
                    message=f"Gate health improved: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    def _decay_discovery_failures(self) -> None:
        self._manager_selector.decay_failures()
        self._peer_discovery.decay_failures()

    async def _cleanup_stale_manager(self, manager_addr: tuple[str, int]) -> None:
        # Before its heartbeat is dropped: it names the manager's UDP address.
        self._forget_learned_manager_addresses(manager_addr)
        await self._modular_state.remove_manager(manager_addr)
        self._manager_selector.forget_manager(manager_addr)
        await self._clear_manager_backpressure(manager_addr)
        await self._circuit_breaker_manager.remove_circuit(manager_addr)

    async def _discovery_maintenance_loop(self) -> None:
        # A silent manager is retained as long as a dead gate peer is
        # (the operator's retention for departed members), then forgotten.
        stale_manager_threshold = self._dead_peer_reap_interval
        while self._running:
            try:
                await self._clock.sleep(self._discovery_failure_decay_interval)

                self._decay_discovery_failures()

                now = self._clock.monotonic()
                stale_cutoff = now - stale_manager_threshold
                stale_manager_addrs = self._modular_state.get_stale_manager_addrs(
                    stale_cutoff
                )

                for manager_addr in stale_manager_addrs:
                    await self._cleanup_stale_manager(manager_addr)

                if forgotten_datacenters := await self._observed_latency_tracker.cleanup_stale_entries():
                    await self._udp_logger.log(
                        StaleObservationsDecayed(
                            message=(
                                f"Observed latency of {', '.join(forgotten_datacenters)} decayed to no "
                                "confidence: routing them by prediction alone"
                            ),
                            datacenter_ids=forgotten_datacenters,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "discovery_maintenance_loop")

    async def _datacenter_correlation_loop(self) -> None:
        """AD-33 Part 6: every datacenter's health, sampled into the
        cross-datacenter correlation detector each heartbeat interval --
        the fastest a datacenter's classification can change."""
        while self._running:
            try:
                await self._clock.sleep(self.env.MANAGER_HEARTBEAT_INTERVAL)
                self._health_coordinator.sample_datacenter_correlation()
            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "datacenter_correlation_loop")

    async def _dead_peer_reap_loop(self) -> None:
        while self._running:
            try:
                await self._clock.sleep(self._dead_peer_check_interval)

                now = self._clock.monotonic()
                reap_threshold = now - self._dead_peer_reap_interval

                peers_to_reap = [
                    peer_addr
                    for peer_addr, unhealthy_since in self._modular_state.get_unhealthy_peers().items()
                    if unhealthy_since < reap_threshold
                ]

                for peer_addr in peers_to_reap:
                    # Stop treating the peer as active and start its dead
                    # clock. Its UDP mapping and last heartbeat stay: the
                    # cleanup pass below needs them to resolve the peer's
                    # gate id (known gates, versioned clock, hash ring,
                    # discovery are keyed by it).
                    await self._modular_state.remove_active_peer(peer_addr)
                    self._modular_state.mark_peer_dead(peer_addr, now)

                    await self._udp_logger.log(
                        ServerInfo(
                            message=f"Reaped dead gate peer {peer_addr[0]}:{peer_addr[1]}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        ),
                    )

                    await self._udp_logger.log(
                        ServerDebug(
                            message=(
                                "Removed gate peer from unhealthy tracking during reap: "
                                f"{peer_addr[0]}:{peer_addr[1]}"
                            ),
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        ),
                    )

                cleanup_threshold = now - (self._dead_peer_reap_interval * 2)
                peers_to_cleanup = [
                    peer_addr
                    for peer_addr, dead_since in self._modular_state.get_dead_peer_timestamps().items()
                    if dead_since < cleanup_threshold
                ]

                for peer_addr in peers_to_cleanup:
                    gate_ids_to_remove = (
                        await self._peer_coordinator.cleanup_dead_peer(peer_addr)
                    )

                    for gate_id in gate_ids_to_remove:
                        await self._versioned_clock.remove_entity(gate_id)
                    await self._peer_gate_circuit_breaker.remove_circuit(peer_addr)

                    await self._udp_logger.log(
                        ServerDebug(
                            message=(
                                "Completed dead peer cleanup for gate "
                                f"{peer_addr[0]}:{peer_addr[1]}"
                            ),
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        ),
                    )

                await self._check_quorum_status()

                await self._log_health_transitions()

                await self._checkpoint_ledger_if_due()

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "dead_peer_reap_loop")

    async def _checkpoint_ledger_if_due(self) -> None:
        """AD-38's compaction cadence, driven by the reap loop's tick.

        The ledger owns the policy and contains its own disk failures,
        so this is a no-op when the durable tier is unconfigured and
        two counter reads when a checkpoint is not yet due. Without a
        caller the WAL never compacts: pending entries accumulate for
        the process lifetime and a restarted gate replays its whole
        history from LSN 0 instead of resuming from a snapshot.
        """
        if self._job_ledger is not None:
            await self._job_ledger.maybe_checkpoint()

    async def _gate_peer_readmission_loop(self) -> None:
        """Re-admit configured gate peers evicted by false SWIM death.

        A partition longer than the suspicion bound makes each side
        declare the other DEAD: the tracker pins the peer at its death
        incarnation, the probe scheduler drops it, and — because a
        DEAD entry outranks ALIVE at the same incarnation — no amount
        of post-heal traffic from the (never-restarted, never-bumped)
        peer can revive it. The evicted peer never learns it was
        declared dead, so the refutation path never fires and the
        registries stay decayed forever (probed: every gate ends the
        total-isolation scenario with active-peer count 1, and the
        kill+partition composite leaves the survivors leaderless with
        no quorum to re-elect).

        This is the gate-tier analog of the worker tier's rejoin
        machinery (manager-rejoin watch + eviction-notice nudge): on
        the dead-peer check cadence, every CONFIGURED peer this gate
        holds DEAD is liveness-verified over TCP (the same
        ``ping`` proof ``authorize_rejoin_reset`` trusts
        for inbound JOINs); on proof of life the peer is re-admitted
        through the established rejoin composite and a fresh JOIN is
        sent so a one-sidedly evicted peer re-admits US symmetrically.
        A genuinely dead peer costs one short failed ping per tick and
        nothing else.
        """
        while self._running:
            try:
                await self._clock.sleep(self._dead_peer_check_interval)
                await self._readmit_partition_evicted_peers()
            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "gate_peer_readmission_loop")

    async def _readmit_partition_evicted_peers(self) -> None:
        """TCP-verify each DEAD-marked configured peer; re-admit on proof."""
        for peer_index, udp_addr in enumerate(self._gate_udp_peers):
            if peer_index >= len(self._gate_peers):
                continue

            node_state = self._incarnation_tracker.get_node_state(udp_addr)
            if node_state is not None and node_state.status != b"DEAD":
                continue

            tcp_addr = self._gate_peers[peer_index]
            gate_info = await self._verify_gate_peer_rejoin(udp_addr, tcp_addr)
            if gate_info is None:
                continue

            # The same composite the manager's gate_register rejoin
            # branch runs: refresh identity, wipe the death record and
            # seed OK at the rejoin incarnation (also re-gossips ALIVE
            # so third parties supersede their stale DEAD entries),
            # re-enrol in the probe rotation, and drive the canonical
            # join pipeline (dead-addr clear, peer recovery -> active-peer
            # restoration + state sync). Its Raft membership is the gate
            # tier's membership group's to change (AD-52), not this path's.
            await self._ingest_gate_peer_info(gate_info)
            await self.reset_peer_for_rejoin(udp_addr)
            self._probe_scheduler.add_member(udp_addr)
            self._on_node_join(udp_addr)

            # Announce OURSELVES to the recovered peer with a freshly
            # bumped incarnation: if the eviction was mutual (or only
            # on the peer's side), its zombie check needs a claim above
            # the incarnation it recorded for our death — join_cluster
            # bumps past the rejoin threshold by contract.
            await self.join_cluster(udp_addr, seed_role="gate")

            await self._udp_logger.log(
                ServerInfo(
                    message=(
                        f"Re-admitted gate peer {tcp_addr[0]}:{tcp_addr[1]} "
                        "after partition-driven eviction (TCP liveness "
                        "proof + rejoin reset)"
                    ),
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _check_quorum_status(self) -> None:
        active_peer_count = self._modular_state.get_active_peer_count() + 1
        known_gate_count = self._configured_gate_count()
        quorum_size = self._quorum_size()

        if active_peer_count < quorum_size:
            self._consecutive_quorum_failures += 1

            if (
                self._consecutive_quorum_failures
                >= self._quorum_stepdown_consecutive_failures
                and self._leader_election.state.is_leader()
            ):
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Quorum lost ({active_peer_count}/{known_gate_count} active, "
                        f"need {quorum_size}). Stepping down as leader after "
                        f"{self._consecutive_quorum_failures} consecutive failures.",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )
                await self._leader_election._step_down()
        else:
            self._consecutive_quorum_failures = 0

    # =========================================================================
    # Coordinator Accessors
    # =========================================================================

    @property
    def stats_coordinator(self) -> GateStatsCoordinator | None:
        """Get the stats coordinator."""
        return self._stats_coordinator

    @property
    def dispatch_coordinator(self) -> GateDispatchCoordinator | None:
        """Get the dispatch coordinator."""
        return self._dispatch_coordinator

    @property
    def leadership_coordinator(self) -> GateLeadershipCoordinator | None:
        """Get the leadership coordinator."""
        return self._leadership_coordinator

    @property
    def peer_coordinator(self) -> GatePeerCoordinator | None:
        """Get the peer coordinator."""
        return self._peer_coordinator

    @property
    def health_coordinator(self) -> GateHealthCoordinator | None:
        """Get the health coordinator."""
        return self._health_coordinator


    # =========================================================================
    # Raft TCP Handlers
    # =========================================================================

    @tcp.receive()
    async def gate_raft_request_vote(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle incoming Raft RequestVote RPC from a gate peer."""
        if not self._accepting_requests:
            return b""
        response = await self._raft.handle_request_vote(data)
        return response if response is not None else b""

    @tcp.receive()
    async def gate_raft_request_vote_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle incoming Raft RequestVoteResponse from a gate peer."""
        if self._accepting_requests:
            await self._raft.handle_request_vote_response(data)
        return b""

    @tcp.receive()
    async def gate_raft_ledger_proposal(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Propose a peer's forwarded AD-38 ledger entry (if Raft leader)."""
        if self._ledger_replicator is None:
            return b""
        result = await self._ledger_replicator.handle_forwarded(LedgerProposal.load(data))
        return result.dump()

    @tcp.receive()
    async def gate_raft_ledger_placement(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Answer which gates hold a ledger entry (AD-38 GLOBAL check)."""
        if not self._accepting_requests:
            return b""
        result = await self._ledger_region_span.handle_query(LedgerPlacementQuery.load(data))
        return result.dump()

    @tcp.receive()
    async def gate_raft_append_entries(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle incoming Raft AppendEntries RPC from a gate peer."""
        if not self._accepting_requests:
            return b""
        response = await self._raft.handle_append_entries(data)
        return response if response is not None else b""

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
    async def gate_raft_append_entries_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle incoming Raft AppendEntriesResponse from a gate peer."""
        if self._accepting_requests:
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
        cluster's formation. Nothing
        answers before the gate accepts requests: its peers' rounds wait."""
        if not self._accepting_requests:
            return b""
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
        if not self._accepting_requests:
            return b""
        return await self._cluster_membership.handle_found(data)

    @tcp.receive()
    async def cluster_join(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Take a node of the cohort into the formed cluster (leader only)."""
        if not self._accepting_requests:
            return b""
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
        if not self._accepting_requests:
            return b""
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
        if not self._accepting_requests:
            return b""
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
        if not self._accepting_requests:
            return b""
        return await self._cluster_membership.handle_status(data)

    @tcp.receive()
    async def cluster_watch(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """A membership watch's long poll (AD-52 section 9)."""
        if not self._accepting_requests:
            return b""
        return await self._cluster_membership.handle_watch(data)

    @tcp.receive()
    async def cluster_metrics(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """This node's metrics of its cluster's membership (AD-52 section
        18), with its AD-45 route learning per datacenter."""
        if not self._accepting_requests:
            return b""
        if not (membership_metrics := await self._cluster_membership.handle_metrics(data)):
            return membership_metrics
        reply = ClusterMetricsReply.load(membership_metrics)
        observed_by_datacenter = self._observed_latency_tracker.get_metrics()["per_dc"]
        blended_by_datacenter = self._latency_estimator.estimate(self._datacenter_managers.keys())
        for datacenter_id, blended_latency_ms in blended_by_datacenter.items():
            observed_latency_ms, confidence = self._blended_scorer.get_observed_latency(datacenter_id)
            reply.route_learning[datacenter_id] = {
                "observed_latency_ms": observed_latency_ms,
                "blended_latency_ms": blended_latency_ms,
                "confidence": confidence,
                "sample_count": float(observed_by_datacenter.get(datacenter_id, {}).get("sample_count", 0)),
            }
        reply.routing = self._job_router.get_metrics()
        now = self._clock.monotonic()
        for datacenter_id, cache in self._datacenter_views.items():
            watched_view, staleness_seconds = cache.read(now)
            reply.datacenter_watches[datacenter_id] = {
                "staleness_seconds": staleness_seconds,
                "disconnected": float(cache.is_disconnected(now)),
                "applied_index": float(watched_view.applied_index),
            }
        return reply.dump()

    @tcp.receive()
    async def cluster_leave(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Release a member's address: a member draining itself, or an
        operator removing one that is gone (AD-52 section 13)."""
        if not self._accepting_requests:
            return b""
        return await self._cluster_membership.handle_leave(data)

    @tcp.receive()
    async def cluster_raft_request_vote(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle the membership group's RequestVote."""
        if not self._accepting_requests:
            return b""
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
        if not self._accepting_requests:
            return b""
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
        if not self._accepting_requests:
            return b""
        response = await self._cluster_membership.handle_install_snapshot(data)
        return response if response is not None else b""


__all__ = [
    "GateServer",
    "GateRuntimeState",
    "GateStatsCoordinator",
    "GateDispatchCoordinator",
    "GateLeadershipCoordinator",
    "GatePeerCoordinator",
    "GateHealthCoordinator",
    "GatePingHandler",
    "GateJobHandler",
    "GateManagerHandler",
    "GateCancellationHandler",
    "GateStateSyncHandler",
]
