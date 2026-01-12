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
- Config: GateConfig for immutable configuration
"""

import asyncio
import random
import time
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import cloudpickle

from hyperscale.distributed.server import tcp
from hyperscale.distributed.leases import LeaseManager as JobLeaseManager
from hyperscale.reporting.results import Results
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.distributed.server.events import VersionedStateClock
from hyperscale.distributed.swim import HealthAwareServer, GateStateEmbedder
from hyperscale.distributed.swim.health import (
    FederatedHealthMonitor,
    DCLeaderAnnouncement,
)
from hyperscale.distributed.models import (
    NodeInfo,
    NodeRole,
    GateInfo,
    GateState,
    GateHeartbeat,
    ManagerRegistrationResponse,
    GateRegistrationRequest,
    GateRegistrationResponse,
    ManagerDiscoveryBroadcast,
    JobProgressAck,
    ManagerHeartbeat,
    JobSubmission,
    JobAck,
    JobStatus,
    JobProgress,
    GlobalJobStatus,
    JobStatusPush,
    DCStats,
    JobBatchPush,
    JobFinalResult,
    GlobalJobResult,
    AggregatedJobStats,
    StateSyncRequest,
    StateSyncResponse,
    GateStateSnapshot,
    CancelJob,
    CancelAck,
    JobCancelRequest,
    JobCancelResponse,
    JobCancellationComplete,
    SingleWorkflowCancelRequest,
    SingleWorkflowCancelResponse,
    WorkflowCancellationStatus,
    DatacenterLease,
    LeaseTransfer,
    LeaseTransferAck,
    DatacenterHealth,
    DatacenterRegistrationStatus,
    DatacenterRegistrationState,
    DatacenterStatus,
    UpdateTier,
    PingRequest,
    DatacenterInfo,
    GatePingResponse,
    DatacenterListRequest,
    DatacenterListResponse,
    WorkflowQueryRequest,
    WorkflowStatusInfo,
    WorkflowQueryResponse,
    DatacenterWorkflowStatus,
    GateWorkflowQueryResponse,
    RegisterCallback,
    RegisterCallbackResponse,
    RateLimitResponse,
    ReporterResultPush,
    WorkflowResultPush,
    WorkflowDCResult,
    JobLeadershipAnnouncement,
    JobLeadershipAck,
    JobLeaderGateTransfer,
    JobLeaderGateTransferAck,
    JobLeaderManagerTransfer,
    JobLeaderManagerTransferAck,
    JobLeadershipNotification,
    GateStateSyncRequest,
    GateStateSyncResponse,
    restricted_loads,
    JobStatsCRDT,
    JobProgressReport,
    JobTimeoutReport,
    JobGlobalTimeout,
    JobLeaderTransfer,
    JobFinalStatus,
)
from hyperscale.distributed.swim.core import (
    QuorumError,
    QuorumUnavailableError,
    QuorumCircuitOpenError,
    ErrorStats,
    CircuitState,
)
from hyperscale.distributed.swim.detection import HierarchicalConfig
from hyperscale.distributed.health import (
    ManagerHealthState,
    ManagerHealthConfig,
    GateHealthState,
    GateHealthConfig,
    RoutingDecision,
    CircuitBreakerManager,
    LatencyTracker,
)
from hyperscale.distributed.reliability import (
    HybridOverloadDetector,
    LoadShedder,
    ServerRateLimiter,
    RetryExecutor,
    RetryConfig,
    JitterStrategy,
    BackpressureLevel,
    BackpressureSignal,
)
from hyperscale.distributed.jobs.gates import (
    GateJobManager,
    JobForwardingTracker,
    ConsistentHashRing,
    GateJobTimeoutTracker,
)
from hyperscale.distributed.jobs import (
    WindowedStatsCollector,
    WindowedStatsPush,
    JobLeadershipTracker,
)
from hyperscale.distributed.ledger import JobLedger
from hyperscale.distributed.idempotency import (
    GateIdempotencyCache,
    IdempotencyKey,
    IdempotencyStatus,
    create_idempotency_config_from_env,
)
from hyperscale.distributed.datacenters import (
    DatacenterHealthManager,
    ManagerDispatcher,
    LeaseManager as DatacenterLeaseManager,
    CrossDCCorrelationDetector,
    CorrelationSeverity,
)
from hyperscale.distributed.protocol.version import (
    ProtocolVersion,
    NodeCapabilities,
    NegotiatedCapabilities,
    negotiate_capabilities,
    CURRENT_PROTOCOL_VERSION,
    get_features_for_version,
)
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.discovery.security.role_validator import (
    RoleValidator,
    CertificateClaims,
    NodeRole as SecurityNodeRole,
)
from hyperscale.distributed.routing import (
    GateJobRouter,
    GateJobRouterConfig,
    RoutingDecision as VivaldiRoutingDecision,
    DatacenterCandidate,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerWarning,
    ServerError,
    ServerDebug,
)

from .stats_coordinator import GateStatsCoordinator
from .cancellation_coordinator import GateCancellationCoordinator
from .dispatch_coordinator import GateDispatchCoordinator
from .leadership_coordinator import GateLeadershipCoordinator
from .peer_coordinator import GatePeerCoordinator
from .health_coordinator import GateHealthCoordinator
from .config import GateConfig, create_gate_config
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


class GateServer(HealthAwareServer):
    """
    Gate node in the distributed Hyperscale system.

    This is the composition root that wires together all gate modules:
    - Configuration (GateConfig)
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
        ledger_data_dir: Path | None = None,
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
        )

        # Store reference to env
        self.env = env

        # Job ledger configuration (AD-38)
        self._ledger_data_dir = ledger_data_dir
        self._job_ledger: JobLedger | None = None

        # Create modular runtime state
        self._modular_state = GateRuntimeState()

        # Datacenter -> manager addresses mapping
        self._datacenter_managers = datacenter_managers or {}
        self._datacenter_manager_udp = datacenter_manager_udp or {}

        # Per-DC registration state tracking (AD-27)
        self._dc_registration_states: dict[str, DatacenterRegistrationState] = {}
        for datacenter_id, manager_addrs in self._datacenter_managers.items():
            self._dc_registration_states[datacenter_id] = DatacenterRegistrationState(
                dc_id=datacenter_id,
                configured_managers=list(manager_addrs),
            )

        # Per-manager circuit breakers
        self._circuit_breaker_manager = CircuitBreakerManager(env)

        # Gate peers
        self._gate_peers = gate_peers or []
        self._gate_udp_peers = gate_udp_peers or []

        # UDP -> TCP mapping for peers
        self._gate_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}
        for idx, tcp_addr in enumerate(self._gate_peers):
            if idx < len(self._gate_udp_peers):
                self._gate_udp_to_tcp[self._gate_udp_peers[idx]] = tcp_addr

        # Active gate peers (AD-29: start empty)
        self._active_gate_peers: set[tuple[str, int]] = set()

        # Per-peer locks and epochs
        self._peer_state_locks: dict[tuple[str, int], asyncio.Lock] = {}
        self._peer_state_epoch: dict[tuple[str, int], int] = {}

        # Gate peer info from heartbeats
        self._gate_peer_info: dict[tuple[str, int], GateHeartbeat] = {}

        # Known gates
        self._known_gates: dict[str, GateInfo] = {}

        # Datacenter manager status
        self._datacenter_manager_status: dict[
            str, dict[tuple[str, int], ManagerHeartbeat]
        ] = {}
        self._manager_last_status: dict[tuple[str, int], float] = {}

        # Health state tracking (AD-19)
        self._manager_health: dict[tuple[str, tuple[str, int]], ManagerHealthState] = {}
        self._manager_health_config = ManagerHealthConfig()
        self._gate_peer_health: dict[str, GateHealthState] = {}
        self._gate_health_config = GateHealthConfig()

        # Latency tracking
        self._peer_gate_latency_tracker = LatencyTracker(
            sample_max_age=60.0,
            sample_max_count=30,
        )

        # Load shedding (AD-22)
        self._overload_detector = HybridOverloadDetector()
        self._load_shedder = LoadShedder(self._overload_detector)

        # Backpressure tracking (AD-37)
        self._manager_backpressure: dict[tuple[str, int], BackpressureLevel] = {}
        self._backpressure_delay_ms: int = 0
        self._dc_backpressure: dict[str, BackpressureLevel] = {}

        # Throughput tracking
        self._forward_throughput_count: int = 0
        self._forward_throughput_interval_start: float = time.monotonic()
        self._forward_throughput_last_value: float = 0.0
        self._forward_throughput_interval_seconds: float = getattr(
            env, "GATE_THROUGHPUT_INTERVAL_SECONDS", 10.0
        )

        # Rate limiting (AD-24)
        self._rate_limiter = ServerRateLimiter(inactive_cleanup_seconds=300.0)

        # Protocol version (AD-25)
        self._node_capabilities = NodeCapabilities.current(node_version=f"gate-{dc_id}")
        self._manager_negotiated_caps: dict[
            tuple[str, int], NegotiatedCapabilities
        ] = {}

        # Versioned state clock
        self._versioned_clock = VersionedStateClock()

        # Job management
        self._job_manager = GateJobManager()

        # Consistent hash ring
        self._job_hash_ring = ConsistentHashRing(replicas=150)

        # Workflow results tracking
        self._workflow_dc_results: dict[
            str, dict[str, dict[str, WorkflowResultPush]]
        ] = {}
        self._job_workflow_ids: dict[str, set[str]] = {}

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
        )

        # Per-job per-DC manager tracking
        self._job_dc_managers: dict[str, dict[str, tuple[str, int]]] = {}

        # Cancellation tracking
        self._cancellation_completion_events: dict[str, asyncio.Event] = {}
        self._cancellation_errors: dict[str, list[str]] = defaultdict(list)

        # Progress callbacks
        self._progress_callbacks: dict[str, tuple[str, int]] = {}

        # Windowed stats
        self._windowed_stats = WindowedStatsCollector(
            window_size_ms=env.STATS_WINDOW_SIZE_MS,
            drift_tolerance_ms=env.STATS_DRIFT_TOLERANCE_MS,
            max_window_age_ms=env.STATS_MAX_WINDOW_AGE_MS,
        )
        self._stats_push_interval_ms: float = env.STATS_PUSH_INTERVAL_MS

        # Job submissions
        self._job_submissions: dict[str, JobSubmission] = {}

        # Reporter tasks
        self._job_reporter_tasks: dict[str, dict[str, asyncio.Task]] = {}

        # CRDT stats (AD-14)
        self._job_stats_crdt: dict[str, JobStatsCRDT] = {}
        self._job_stats_crdt_lock = asyncio.Lock()

        # Datacenter health manager (AD-16)
        self._dc_health_manager = DatacenterHealthManager(
            heartbeat_timeout=30.0,
            get_configured_managers=lambda dc: self._datacenter_managers.get(dc, []),
        )
        for datacenter_id in self._datacenter_managers.keys():
            self._dc_health_manager.add_datacenter(datacenter_id)

        # Manager dispatcher
        self._manager_dispatcher = ManagerDispatcher(
            dispatch_timeout=5.0,
            max_retries_per_dc=2,
        )
        for datacenter_id, manager_addrs in self._datacenter_managers.items():
            self._manager_dispatcher.add_datacenter(datacenter_id, manager_addrs)

        # Datacenter lease manager
        self._dc_lease_manager = DatacenterLeaseManager(
            node_id="",
            lease_timeout=lease_timeout,
        )

        # Job forwarding tracker
        self._job_forwarding_tracker = JobForwardingTracker(
            local_gate_id="",
            forward_timeout=3.0,
            max_forward_attempts=3,
        )

        # Legacy leases
        self._leases: dict[str, DatacenterLease] = {}
        self._fence_token = 0

        # Orphan job tracking
        self._dead_job_leaders: set[tuple[str, int]] = set()
        self._orphaned_jobs: dict[str, float] = {}
        self._orphan_grace_period: float = env.GATE_ORPHAN_GRACE_PERIOD
        self._orphan_check_interval: float = env.GATE_ORPHAN_CHECK_INTERVAL
        self._orphan_check_task: asyncio.Task | None = None

        # Job timeout tracker (AD-34)
        self._job_timeout_tracker = GateJobTimeoutTracker(
            gate=self,
            check_interval=getattr(env, "GATE_TIMEOUT_CHECK_INTERVAL", 15.0),
            stuck_threshold=getattr(env, "GATE_ALL_DC_STUCK_THRESHOLD", 180.0),
        )

        # Job router (AD-36) - initialized in start()
        self._job_router: GateJobRouter | None = None

        # Idempotency cache (AD-40) - initialized in start() after task_runner is available
        self._idempotency_cache: GateIdempotencyCache[bytes] | None = None
        self._idempotency_config = create_idempotency_config_from_env(env)

        # State version
        self._state_version = 0

        # Gate state
        self._gate_state = GateState.SYNCING

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
        self._job_max_age: float = 3600.0
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
                get_state_version=lambda: self._state_version,
                get_gate_state=lambda: self._gate_state.value,
                get_active_jobs=lambda: self._job_manager.job_count(),
                get_active_datacenters=lambda: self._count_active_datacenters(),
                get_manager_count=lambda: sum(
                    len(managers) for managers in self._datacenter_managers.values()
                ),
                get_tcp_host=lambda: self._host,
                get_tcp_port=lambda: self._tcp_port,
                on_manager_heartbeat=self._handle_embedded_manager_heartbeat,
                on_gate_heartbeat=self._handle_gate_peer_heartbeat,
                get_known_managers=self._get_known_managers_for_piggyback,
                get_known_gates=self._get_known_gates_for_piggyback,
                get_job_leaderships=self._get_job_leaderships_for_piggyback,
                get_job_dc_managers=self._get_job_dc_managers_for_piggyback,
                get_health_has_dc_connectivity=lambda: len(self._datacenter_managers)
                > 0,
                get_health_connected_dc_count=self._count_active_datacenters,
                get_health_throughput=self._get_forward_throughput,
                get_health_expected_throughput=self._get_expected_forward_throughput,
                get_health_overload_state=lambda: self._overload_detector.get_state(
                    0.0, 0.0
                ),
            )
        )

        # Register callbacks
        self.register_on_node_dead(self._on_node_dead)
        self.register_on_node_join(self._on_node_join)
        self.register_on_become_leader(self._on_gate_become_leader)
        self.register_on_lose_leadership(self._on_gate_lose_leadership)
        self.register_on_peer_confirmed(self._on_peer_confirmed)

        # Initialize hierarchical failure detector (AD-30)
        self.init_hierarchical_detector(
            config=HierarchicalConfig(
                global_min_timeout=30.0,
                global_max_timeout=120.0,
                job_min_timeout=5.0,
                job_max_timeout=30.0,
            ),
            on_global_death=self._on_manager_globally_dead,
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
        )

        # Cross-DC correlation detector
        self._cross_dc_correlation = CrossDCCorrelationDetector(
            config=env.get_cross_dc_correlation_config()
        )
        for datacenter_id in self._datacenter_managers.keys():
            self._cross_dc_correlation.add_datacenter(datacenter_id)

        # Discovery services (AD-28)
        self._dc_manager_discovery: dict[str, DiscoveryService] = {}
        self._discovery_failure_decay_interval: float = (
            env.DISCOVERY_FAILURE_DECAY_INTERVAL
        )
        self._discovery_maintenance_task: asyncio.Task | None = None

        for datacenter_id, manager_addrs in self._datacenter_managers.items():
            static_seeds = [f"{host}:{port}" for host, port in manager_addrs]
            dc_discovery_config = env.get_discovery_config(
                node_role="gate",
                static_seeds=static_seeds,
            )
            dc_discovery = DiscoveryService(dc_discovery_config)
            for host, port in manager_addrs:
                dc_discovery.add_peer(
                    peer_id=f"{host}:{port}",
                    host=host,
                    port=port,
                    role="manager",
                    datacenter_id=datacenter_id,
                )
            self._dc_manager_discovery[datacenter_id] = dc_discovery

        # Peer discovery
        peer_static_seeds = [f"{host}:{port}" for host, port in self._gate_peers]
        peer_discovery_config = env.get_discovery_config(
            node_role="gate",
            static_seeds=peer_static_seeds,
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
            cluster_id=env.get("CLUSTER_ID", "hyperscale"),
            environment_id=env.get("ENVIRONMENT_ID", "default"),
            strict_mode=env.get("MTLS_STRICT_MODE", "false").lower() == "true",
        )

        # Coordinators (initialized in _init_coordinators)
        self._stats_coordinator: GateStatsCoordinator | None = None
        self._cancellation_coordinator: GateCancellationCoordinator | None = None
        self._dispatch_coordinator: GateDispatchCoordinator | None = None
        self._leadership_coordinator: GateLeadershipCoordinator | None = None
        self._peer_coordinator: GatePeerCoordinator | None = None
        self._health_coordinator: GateHealthCoordinator | None = None

        # Handlers (initialized in _init_handlers)
        self._ping_handler: GatePingHandler | None = None
        self._job_handler: GateJobHandler | None = None
        self._manager_handler: GateManagerHandler | None = None
        self._cancellation_handler: GateCancellationHandler | None = None
        self._state_sync_handler: GateStateSyncHandler | None = None

    # =========================================================================
    # Coordinator and Handler Initialization
    # =========================================================================

    def _init_coordinators(self) -> None:
        """Initialize coordinator instances with dependencies."""
        self._stats_coordinator = GateStatsCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            windowed_stats=self._windowed_stats,
            get_job_callback=self._job_manager.get_callback,
            get_job_status=self._job_manager.get_job,
            send_tcp=self._send_tcp,
            stats_push_interval_ms=self._stats_push_interval_ms,
        )

        self._cancellation_coordinator = GateCancellationCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            get_job_target_dcs=self._job_manager.get_target_dcs,
            get_dc_manager_addr=lambda job_id, dc_id: self._job_dc_managers.get(
                job_id, {}
            ).get(dc_id),
            send_tcp=self._send_tcp,
            is_job_leader=self._job_leadership_tracker.is_leader,
        )

        self._leadership_coordinator = GateLeadershipCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            leadership_tracker=self._job_leadership_tracker,
            get_node_id=lambda: self._node_id,
            get_node_addr=lambda: (self._host, self._tcp_port),
            send_tcp=self._send_tcp,
            get_active_peers=lambda: list(self._active_gate_peers),
        )

        self._dispatch_coordinator = GateDispatchCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_router=self._job_router,
            check_rate_limit=self._check_rate_limit_for_operation,
            should_shed_request=self._should_shed_request,
            has_quorum_available=self._has_quorum_available,
            quorum_size=self._quorum_size,
            quorum_circuit=self._quorum_circuit,
            select_datacenters=self._select_datacenters_with_fallback,
            assume_leadership=self._job_leadership_tracker.assume_leadership,
            broadcast_leadership=self._broadcast_job_leadership,
            dispatch_to_dcs=self._dispatch_job_to_datacenters,
        )

        self._peer_coordinator = GatePeerCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            peer_discovery=self._peer_discovery,
            job_hash_ring=self._job_hash_ring,
            job_forwarding_tracker=self._job_forwarding_tracker,
            job_leadership_tracker=self._job_leadership_tracker,
            versioned_clock=self._versioned_clock,
            gate_health_config=vars(self._gate_health_config),
            recovery_semaphore=self._recovery_semaphore,
            recovery_jitter_min=0.0,
            recovery_jitter_max=getattr(self.env, "GATE_RECOVERY_JITTER_MAX", 1.0),
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_udp_port=lambda: self._udp_port,
            confirm_peer=self._confirm_peer,
            handle_job_leader_failure=self._handle_job_leader_failure,
        )

        self._health_coordinator = GateHealthCoordinator(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            dc_health_manager=self._dc_health_manager,
            dc_health_monitor=self._dc_health_monitor,
            cross_dc_correlation=self._cross_dc_correlation,
            dc_manager_discovery=self._dc_manager_discovery,
            versioned_clock=self._versioned_clock,
            manager_dispatcher=self._manager_dispatcher,
            manager_health_config=vars(self._manager_health_config),
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            confirm_manager_for_dc=self._confirm_manager_for_dc,
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
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_router=self._job_router,
            job_leadership_tracker=self._job_leadership_tracker,
            quorum_circuit=self._quorum_circuit,
            load_shedder=self._load_shedder,
            job_lease_manager=self._job_lease_manager,
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
        )

        self._manager_handler = GateManagerHandler(
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
            broadcast_manager_discovery=self._broadcast_manager_discovery,
        )

        self._cancellation_handler = GateCancellationHandler(
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
            get_available_datacenters=self._get_available_datacenters,
        )

        self._state_sync_handler = GateStateSyncHandler(
            state=self._modular_state,
            logger=self._udp_logger,
            task_runner=self._task_runner,
            job_manager=self._job_manager,
            job_leadership_tracker=self._job_leadership_tracker,
            versioned_clock=self._versioned_clock,
            get_node_id=lambda: self._node_id,
            get_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            is_leader=self.is_leader,
            get_term=lambda: self._leader_election.state.current_term,
            get_state_snapshot=self._get_state_snapshot,
            apply_state_snapshot=self._apply_gate_state_snapshot,
        )

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    async def start(self) -> None:
        """
        Start the gate server.

        Initializes coordinators, wires handlers, and starts background tasks.
        """
        await self.start_server(init_context=self.env.get_swim_init_context())

        # Set node_id on trackers
        self._job_leadership_tracker.node_id = self._node_id.full
        self._job_leadership_tracker.node_addr = (self._host, self._tcp_port)
        self._job_lease_manager._node_id = self._node_id.full
        self._dc_lease_manager.set_node_id(self._node_id.full)
        self._job_forwarding_tracker.set_local_gate_id(self._node_id.full)

        if self._ledger_data_dir is not None:
            self._job_ledger = await JobLedger.open(
                wal_path=self._ledger_data_dir / "wal",
                checkpoint_dir=self._ledger_data_dir / "checkpoints",
                archive_dir=self._ledger_data_dir / "archive",
                region_code=self._node_id.datacenter,
                gate_id=self._node_id.full,
                node_id=hash(self._node_id.full) & 0xFFFF,
                logger=self._udp_logger,
            )

        # Add this gate to hash ring
        self._job_hash_ring.add_node(
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

        # Join SWIM cluster
        for peer_udp in self._gate_udp_peers:
            await self.join_cluster(peer_udp)

        # Start SWIM probe cycle
        self._task_runner.run(self.start_probe_cycle)

        # Wait for cluster stabilization
        await self._wait_for_cluster_stabilization()

        # Leader election jitter
        jitter_max = self.env.LEADER_ELECTION_JITTER_MAX
        if jitter_max > 0 and len(self._gate_udp_peers) > 0:
            jitter = random.uniform(0, jitter_max)
            await asyncio.sleep(jitter)

        # Start leader election
        await self.start_leader_election()

        # Wait for election to stabilize
        await asyncio.sleep(self.env.MANAGER_STARTUP_SYNC_DELAY)

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
        await self._job_lease_manager.start_cleanup_task()

        # Start background tasks
        self._task_runner.run(self._lease_cleanup_loop)
        self._task_runner.run(self._job_cleanup_loop)
        self._task_runner.run(self._rate_limit_cleanup_loop)
        self._task_runner.run(self._batch_stats_loop)
        self._task_runner.run(self._windowed_stats_push_loop)

        # Discovery maintenance (AD-28)
        self._discovery_maintenance_task = asyncio.create_task(
            self._discovery_maintenance_loop()
        )

        # Start timeout tracker (AD-34)
        await self._job_timeout_tracker.start()

        # Initialize job router (AD-36)
        self._job_router = GateJobRouter(
            coordinate_tracker=self._coordinate_tracker,
            get_datacenter_candidates=self._build_datacenter_candidates,
        )

        self._idempotency_cache = GateIdempotencyCache(
            config=self._idempotency_config,
            task_runner=self._task_runner,
            logger=self._udp_logger,
        )
        await self._idempotency_cache.start()

        # Initialize coordinators and handlers
        self._init_coordinators()
        self._init_handlers()

        # Register with managers
        if self._datacenter_managers:
            await self._register_with_managers()

        await self._udp_logger.log(
            ServerInfo(
                message=f"Gate started with {len(self._datacenter_managers)} DCs, "
                f"state={self._gate_state.value}",
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
        self._running = False

        if (
            self._discovery_maintenance_task
            and not self._discovery_maintenance_task.done()
        ):
            self._discovery_maintenance_task.cancel()
            try:
                await self._discovery_maintenance_task
            except asyncio.CancelledError:
                pass

        await self._dc_health_monitor.stop()
        await self._job_timeout_tracker.stop()

        if self._idempotency_cache is not None:
            await self._idempotency_cache.close()

        if self._job_ledger is not None:
            await self._job_ledger.close()

        await super().stop(
            drain_timeout=drain_timeout,
            broadcast_leave=broadcast_leave,
        )

    # =========================================================================
    # TCP Handlers - Delegating to Handler Classes
    # =========================================================================

    @tcp.receive()
    async def manager_status_update(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle manager status update via TCP."""
        if self._manager_handler:
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
        transport: asyncio.Transport,
    ):
        """Handle manager registration."""
        if self._manager_handler:
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
        transport: asyncio.Transport,
    ):
        """Handle manager discovery broadcast from peer gate."""
        if self._manager_handler:
            return await self._manager_handler.handle_discovery(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def job_submission(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle job submission from client."""
        if self._job_handler:
            return await self._job_handler.handle_submission(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def receive_job_status_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle job status request from client."""
        if self._job_handler:
            return await self._job_handler.handle_status_request(
                addr, data, self.handle_exception
            )
        return b""

    @tcp.receive()
    async def receive_job_progress(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle job progress update from manager."""
        if self._job_handler:
            return await self._job_handler.handle_progress(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def receive_gate_ping(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle ping request."""
        if self._ping_handler:
            return await self._ping_handler.handle_ping(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def receive_cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle job cancellation request."""
        if self._cancellation_handler:
            return await self._cancellation_handler.handle_cancel_job(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def receive_job_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle job cancellation complete notification."""
        if self._cancellation_handler:
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
        transport: asyncio.Transport,
    ):
        """Handle single workflow cancellation request."""
        if self._cancellation_handler:
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
        transport: asyncio.Transport,
    ):
        """Handle state sync request from peer gate."""
        if self._state_sync_handler:
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
        transport: asyncio.Transport,
    ):
        """Handle lease transfer during gate scaling."""
        if self._state_sync_handler:
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
        transport: asyncio.Transport,
    ):
        """Handle job final result from manager."""
        if self._state_sync_handler:
            return await self._state_sync_handler.handle_job_final_result(
                addr, data, self._complete_job, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def job_leadership_notification(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle job leadership notification from peer gate."""
        if self._state_sync_handler:
            return await self._state_sync_handler.handle_job_leadership_notification(
                addr, data, self.handle_exception
            )
        return b"error"

    @tcp.receive()
    async def receive_job_progress_report(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Receive progress report from manager (AD-34 multi-DC coordination)."""
        try:
            report = JobProgressReport.load(data)
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
        transport: asyncio.Transport,
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
        transport: asyncio.Transport,
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
        transport: asyncio.Transport,
    ):
        """Receive final job status from manager (AD-34 lifecycle cleanup)."""
        try:
            report = JobFinalStatus.load(data)
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
        transport: asyncio.Transport,
    ):
        """Handle workflow result push from manager."""
        try:
            push = WorkflowResultPush.load(data)

            if not self._job_manager.has_job(push.job_id):
                await self._forward_workflow_result_to_peers(push)
                return b"ok"

            self._task_runner.run(
                self._udp_logger.log,
                ServerDebug(
                    message=f"Received workflow result for {push.job_id}:{push.workflow_id} from DC {push.datacenter}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

            if push.job_id not in self._workflow_dc_results:
                self._workflow_dc_results[push.job_id] = {}
            if push.workflow_id not in self._workflow_dc_results[push.job_id]:
                self._workflow_dc_results[push.job_id][push.workflow_id] = {}
            self._workflow_dc_results[push.job_id][push.workflow_id][
                push.datacenter
            ] = push

            target_dcs = self._job_manager.get_target_dcs(push.job_id)
            received_dcs = set(
                self._workflow_dc_results[push.job_id][push.workflow_id].keys()
            )

            if target_dcs and received_dcs >= target_dcs:
                await self._aggregate_and_forward_workflow_result(
                    push.job_id, push.workflow_id
                )

            return b"ok"

        except Exception as error:
            await self.handle_exception(error, "workflow_result_push")
            return b"error"

    @tcp.receive()
    async def register_callback(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle client callback registration for job reconnection."""
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit_for_operation(
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

            self._job_manager.set_callback(job_id, request.callback_addr)
            self._progress_callbacks[job_id] = request.callback_addr

            elapsed = time.monotonic() - job.timestamp if job.timestamp > 0 else 0.0

            self._task_runner.run(
                self._udp_logger.log,
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
        transport: asyncio.Transport,
    ):
        """Handle workflow status query from client."""
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit_for_operation(
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
        transport: asyncio.Transport,
    ):
        """Handle datacenter list request from client."""
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit_for_operation(
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
                manager_statuses = self._datacenter_manager_status.get(dc_id, {})
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
    async def job_leadership_announcement(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle job leadership announcement from peer gate."""
        try:
            announcement = JobLeadershipAnnouncement.load(data)

            accepted = self._job_leadership_tracker.process_leadership_claim(
                job_id=announcement.job_id,
                claimer_id=announcement.leader_id,
                claimer_addr=(announcement.leader_host, announcement.leader_tcp_port),
                fencing_token=announcement.term,
                metadata=announcement.workflow_count,
            )

            if accepted:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerDebug(
                        message=f"Recorded job {announcement.job_id[:8]}... leader: {announcement.leader_id[:8]}...",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )

            return JobLeadershipAck(
                job_id=announcement.job_id,
                accepted=True,
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
        transport: asyncio.Transport,
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
        transport: asyncio.Transport,
    ):
        """Handle job leadership manager transfer notification from manager (AD-31)."""
        try:
            transfer = JobLeaderManagerTransfer.load(data)

            job_known = (
                transfer.job_id in self._job_dc_managers
                or transfer.job_id in self._job_leadership_tracker
            )
            if not job_known:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Received manager transfer for unknown job {transfer.job_id[:8]}... from {transfer.new_manager_id[:8]}...",
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
            if old_manager_addr is None and transfer.job_id in self._job_dc_managers:
                old_manager_addr = self._job_dc_managers[transfer.job_id].get(
                    transfer.datacenter_id
                )

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
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerDebug(
                        message=f"Rejected stale manager transfer for job {transfer.job_id[:8]}... (fence {transfer.fence_token} <= {current_fence})",
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

            if transfer.job_id not in self._job_dc_managers:
                self._job_dc_managers[transfer.job_id] = {}
            self._job_dc_managers[transfer.job_id][transfer.datacenter_id] = (
                transfer.new_manager_addr
            )

            self._clear_orphaned_job(transfer.job_id, transfer.new_manager_addr)

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Updated job {transfer.job_id[:8]}... DC {transfer.datacenter_id} manager: {old_manager_addr} -> {transfer.new_manager_addr}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
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
    async def windowed_stats_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: asyncio.Transport,
    ):
        """Handle windowed stats push from Manager."""
        try:
            push: WindowedStatsPush = cloudpickle.loads(data)

            from hyperscale.distributed.models import WorkflowProgress

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

    def _confirm_peer(self, peer_addr: tuple[str, int]) -> None:
        """Confirm a peer via SWIM."""
        self.confirm_peer(peer_addr)

    async def _complete_job(self, job_id: str, result: object) -> None:
        """Complete a job and notify client."""
        job = self._job_manager.get_job(job_id)
        if job:
            job.status = JobStatus.COMPLETED.value
            self._job_manager.set_job(job_id, job)

        await self._send_immediate_update(job_id, "completed", None)

    def _get_peer_state_lock(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
        """Get or create lock for a peer."""
        return self._peer_state_locks.setdefault(peer_addr, asyncio.Lock())

    def _on_peer_confirmed(self, peer: tuple[str, int]) -> None:
        """Handle peer confirmation via SWIM (AD-29)."""
        tcp_addr = self._gate_udp_to_tcp.get(peer)
        if tcp_addr:
            self._active_gate_peers.add(tcp_addr)

    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """Handle node death via SWIM."""
        gate_tcp_addr = self._gate_udp_to_tcp.get(node_addr)
        if gate_tcp_addr:
            self._task_runner.run(
                self._handle_gate_peer_failure, node_addr, gate_tcp_addr
            )

    def _on_node_join(self, node_addr: tuple[str, int]) -> None:
        """Handle node join via SWIM."""
        gate_tcp_addr = self._gate_udp_to_tcp.get(node_addr)
        if gate_tcp_addr:
            self._task_runner.run(
                self._handle_gate_peer_recovery, node_addr, gate_tcp_addr
            )

    async def _handle_gate_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer failure."""
        if self._peer_coordinator:
            await self._peer_coordinator.handle_peer_failure(udp_addr, tcp_addr)
        else:
            self._active_gate_peers.discard(tcp_addr)

    async def _handle_gate_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer recovery."""
        if self._peer_coordinator:
            await self._peer_coordinator.handle_peer_recovery(udp_addr, tcp_addr)
        else:
            self._active_gate_peers.add(tcp_addr)

    async def _handle_job_leader_failure(self, tcp_addr: tuple[str, int]) -> None:
        """Handle job leader failure - takeover orphaned jobs."""
        if self._peer_coordinator:
            await self._peer_coordinator.handle_job_leader_failure(tcp_addr)

    def _on_gate_become_leader(self) -> None:
        """Called when this gate becomes the cluster leader."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message="This gate is now the LEADER",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

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

    def _on_manager_globally_dead(
        self,
        manager_addr: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Handle manager global death (AD-30)."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager {manager_addr} globally dead",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    def _on_manager_dead_for_dc(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Handle manager death for specific DC (AD-30)."""
        self._circuit_breaker_manager.record_failure(manager_addr)

    def _get_dc_manager_count(self, dc_id: str) -> int:
        """Get manager count for a DC."""
        return len(self._datacenter_managers.get(dc_id, []))

    async def _confirm_manager_for_dc(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
    ) -> None:
        """Confirm manager is alive for a DC."""
        incarnation = 0
        health_state = self._datacenter_manager_status.get(dc_id, {}).get(manager_addr)
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
        """Handle embedded manager heartbeat from SWIM."""
        if self._health_coordinator:
            self._health_coordinator.handle_embedded_manager_heartbeat(
                heartbeat.datacenter,
                source_addr,
                heartbeat.node_id,
                heartbeat.is_leader,
                heartbeat.term,
                heartbeat.worker_count,
                heartbeat.available_cores,
            )

    async def _handle_gate_peer_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        udp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer heartbeat from SWIM."""
        self._gate_peer_info[udp_addr] = heartbeat

        if heartbeat.node_id and heartbeat.tcp_host and heartbeat.tcp_port:
            await self._job_hash_ring.add_node(
                node_id=heartbeat.node_id,
                tcp_host=heartbeat.tcp_host,
                tcp_port=heartbeat.tcp_port,
            )

    def _get_known_managers_for_piggyback(
        self,
    ) -> list[tuple[str, tuple[str, int], int, int]]:
        """Get known managers for SWIM piggyback."""
        result = []
        for dc_id, managers in self._datacenter_manager_status.items():
            for addr, status in managers.items():
                result.append(
                    (dc_id, addr, status.worker_count, status.available_cores)
                )
        return result

    def _get_known_gates_for_piggyback(self) -> list[GateInfo]:
        """Get known gates for SWIM piggyback."""
        return list(self._known_gates.values())

    def _get_job_leaderships_for_piggyback(
        self,
    ) -> list[tuple[str, str, tuple[str, int], int]]:
        """Get job leaderships for SWIM piggyback."""
        return self._job_leadership_tracker.get_all_leaderships()

    def _get_job_dc_managers_for_piggyback(
        self,
    ) -> dict[str, dict[str, tuple[str, int]]]:
        """Get job DC managers for SWIM piggyback."""
        return dict(self._job_dc_managers)

    def _count_active_datacenters(self) -> int:
        """Count active datacenters."""
        count = 0
        for dc_id in self._datacenter_managers.keys():
            status = self._classify_datacenter_health(dc_id)
            if status.health != DatacenterHealth.UNHEALTHY.value:
                count += 1
        return count

    def _get_forward_throughput(self) -> float:
        """Get current forward throughput."""
        now = time.monotonic()
        elapsed = now - self._forward_throughput_interval_start
        if elapsed >= self._forward_throughput_interval_seconds:
            throughput = (
                self._forward_throughput_count / elapsed if elapsed > 0 else 0.0
            )
            self._forward_throughput_last_value = throughput
            self._forward_throughput_count = 0
            self._forward_throughput_interval_start = now
        return self._forward_throughput_last_value

    def _get_expected_forward_throughput(self) -> float:
        """Get expected forward throughput."""
        return 100.0

    def _record_forward_throughput_event(self) -> None:
        """Record a forward throughput event."""
        self._forward_throughput_count += 1

    def _classify_datacenter_health(self, dc_id: str) -> DatacenterStatus:
        """Classify datacenter health."""
        return self._dc_health_manager.classify_health(dc_id)

    def _get_all_datacenter_health(self) -> dict[str, DatacenterStatus]:
        """Get health status for all datacenters."""
        return self._dc_health_manager.get_all_health()

    def _get_available_datacenters(self) -> list[str]:
        """Get list of available datacenters."""
        healthy = []
        for dc_id in self._datacenter_managers.keys():
            status = self._classify_datacenter_health(dc_id)
            if status.health != DatacenterHealth.UNHEALTHY.value:
                healthy.append(dc_id)
        return healthy

    def _select_datacenters_with_fallback(
        self,
        count: int,
        preferred: list[str] | None = None,
        job_id: str | None = None,
    ) -> tuple[list[str], list[str], str]:
        """Select datacenters with fallback (AD-36)."""
        if self._job_router:
            decision = self._job_router.route_job(
                job_id=job_id or f"temp-{time.monotonic()}",
                preferred_datacenters=set(preferred) if preferred else None,
            )
            primary_dcs = (
                decision.primary_datacenters[:count]
                if decision.primary_datacenters
                else []
            )
            fallback_dcs = (
                decision.fallback_datacenters + decision.primary_datacenters[count:]
            )

            if not decision.primary_bucket:
                dc_health = self._get_all_datacenter_health()
                if len(dc_health) == 0 and len(self._datacenter_managers) > 0:
                    return ([], [], "initializing")
                return ([], [], "unhealthy")

            return (primary_dcs, fallback_dcs, decision.primary_bucket.lower())

        return self._legacy_select_datacenters(count, preferred)

    def _legacy_select_datacenters(
        self,
        count: int,
        preferred: list[str] | None = None,
    ) -> tuple[list[str], list[str], str]:
        """Legacy datacenter selection."""
        dc_health = self._get_all_datacenter_health()
        if not dc_health:
            if len(self._datacenter_managers) > 0:
                return ([], [], "initializing")
            return ([], [], "unhealthy")

        healthy = [
            dc
            for dc, status in dc_health.items()
            if status.health == DatacenterHealth.HEALTHY.value
        ]
        busy = [
            dc
            for dc, status in dc_health.items()
            if status.health == DatacenterHealth.BUSY.value
        ]
        degraded = [
            dc
            for dc, status in dc_health.items()
            if status.health == DatacenterHealth.DEGRADED.value
        ]

        if healthy:
            worst_health = "healthy"
        elif busy:
            worst_health = "busy"
        elif degraded:
            worst_health = "degraded"
        else:
            return ([], [], "unhealthy")

        all_usable = healthy + busy + degraded
        primary = all_usable[:count]
        fallback = all_usable[count:]

        return (primary, fallback, worst_health)

    def _build_datacenter_candidates(self) -> list[DatacenterCandidate]:
        """Build datacenter candidates for job router."""
        candidates = []
        for dc_id in self._datacenter_managers.keys():
            status = self._classify_datacenter_health(dc_id)
            candidates.append(
                DatacenterCandidate(
                    datacenter_id=dc_id,
                    health=status.health,
                    available_capacity=status.available_capacity,
                )
            )
        return candidates

    def _check_rate_limit_for_operation(
        self,
        client_id: str,
        operation: str,
    ) -> tuple[bool, float]:
        """Check rate limit for an operation."""
        result = self._rate_limiter.check_rate_limit(client_id, operation)
        return result.allowed, result.retry_after_seconds

    def _should_shed_request(self, request_type: str) -> bool:
        """Check if request should be shed due to load."""
        return self._load_shedder.should_shed_handler(request_type)

    def _has_quorum_available(self) -> bool:
        """Check if quorum is available."""
        if self._gate_state != GateState.ACTIVE:
            return False
        active_count = len(self._active_gate_peers) + 1
        return active_count >= self._quorum_size()

    def _quorum_size(self) -> int:
        """Calculate quorum size."""
        total_gates = len(self._active_gate_peers) + 1
        return (total_gates // 2) + 1

    def _get_healthy_gates(self) -> list[GateInfo]:
        """Get list of healthy gates."""
        gates = [
            GateInfo(
                gate_id=self._node_id.full,
                tcp_host=self._host,
                tcp_port=self._tcp_port,
                udp_host=self._host,
                udp_port=self._udp_port,
                is_leader=self.is_leader(),
                term=self._leader_election.state.current_term,
                state=self._gate_state.value,
            )
        ]

        for peer_addr in self._active_gate_peers:
            for udp_addr, tcp_addr in self._gate_udp_to_tcp.items():
                if tcp_addr == peer_addr:
                    heartbeat = self._gate_peer_info.get(udp_addr)
                    if heartbeat:
                        gates.append(
                            GateInfo(
                                gate_id=heartbeat.node_id,
                                tcp_host=heartbeat.tcp_host,
                                tcp_port=heartbeat.tcp_port,
                                udp_host=udp_addr[0],
                                udp_port=udp_addr[1],
                                is_leader=heartbeat.is_leader,
                                term=heartbeat.term,
                                state=heartbeat.state,
                            )
                        )
                    break

        return gates

    async def _broadcast_job_leadership(
        self,
        job_id: str,
        target_dc_count: int,
    ) -> None:
        """Broadcast job leadership to peer gates."""
        if self._leadership_coordinator:
            await self._leadership_coordinator.broadcast_job_leadership(
                job_id, target_dc_count
            )

    async def _dispatch_job_to_datacenters(
        self,
        submission: JobSubmission,
        target_dcs: list[str],
    ) -> None:
        """Dispatch job to datacenters."""
        if self._dispatch_coordinator:
            await self._dispatch_coordinator.dispatch_job(submission, target_dcs)

    async def _forward_job_progress_to_peers(
        self,
        progress: JobProgress,
    ) -> bool:
        """Forward job progress to peer gates."""
        owner = self._job_hash_ring.get_node(progress.job_id)
        if owner and owner != self._node_id.full:
            owner_addr = self._job_hash_ring.get_node_addr(owner)
            if owner_addr:
                try:
                    await self.send_tcp(
                        owner_addr,
                        "receive_job_progress",
                        progress.dump(),
                        timeout=3.0,
                    )
                    return True
                except Exception as forward_error:
                    await self._udp_logger.log(
                        ServerWarning(
                            message=f"Failed to forward progress to manager: {forward_error}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
        return False

    def _record_request_latency(self, latency_ms: float) -> None:
        """Record request latency for load shedding."""
        self._overload_detector.record_latency(latency_ms)

    def _record_dc_job_stats(self, dc_id: str, job_id: str, stats: dict) -> None:
        """Record DC job stats."""
        pass

    def _handle_update_by_tier(
        self,
        job_id: str,
        old_status: str | None,
        new_status: str,
        progress_data: bytes | None = None,
    ) -> None:
        """Handle update by tier (AD-15)."""
        tier = self._classify_update_tier(job_id, old_status, new_status)

        if tier == UpdateTier.IMMEDIATE.value:
            self._task_runner.run(
                self._send_immediate_update,
                job_id,
                f"status:{old_status}->{new_status}",
                progress_data,
            )

    def _classify_update_tier(
        self,
        job_id: str,
        old_status: str | None,
        new_status: str,
    ) -> str:
        """Classify update tier."""
        terminal_states = {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
        }

        if new_status in terminal_states:
            return UpdateTier.IMMEDIATE.value

        if old_status is None and new_status == JobStatus.RUNNING.value:
            return UpdateTier.IMMEDIATE.value

        if old_status != new_status:
            return UpdateTier.IMMEDIATE.value

        return UpdateTier.PERIODIC.value

    async def _send_immediate_update(
        self,
        job_id: str,
        event_type: str,
        payload: bytes | None = None,
    ) -> None:
        """Send immediate update to client."""
        if self._stats_coordinator:
            await self._stats_coordinator.send_immediate_update(
                job_id, event_type, payload
            )

    def _record_manager_heartbeat(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
        node_id: str,
        generation: int,
    ) -> None:
        """Record manager heartbeat."""
        now = time.monotonic()

        if dc_id not in self._dc_registration_states:
            self._dc_registration_states[dc_id] = DatacenterRegistrationState(
                dc_id=dc_id,
                configured_managers=[manager_addr],
            )
        else:
            dc_state = self._dc_registration_states[dc_id]
            if manager_addr not in dc_state.configured_managers:
                dc_state.configured_managers.append(manager_addr)

        dc_state = self._dc_registration_states[dc_id]
        dc_state.record_heartbeat(manager_addr, node_id, generation, now)

    def _handle_manager_backpressure_signal(
        self,
        manager_addr: tuple[str, int],
        dc_id: str,
        signal: BackpressureSignal,
    ) -> None:
        """Handle backpressure signal from manager."""
        self._manager_backpressure[manager_addr] = signal.level
        self._backpressure_delay_ms = max(
            self._backpressure_delay_ms,
            signal.suggested_delay_ms,
        )
        self._update_dc_backpressure(dc_id)

    def _update_dc_backpressure(self, dc_id: str) -> None:
        """Update DC backpressure level."""
        manager_addrs = self._datacenter_managers.get(dc_id, [])
        if not manager_addrs:
            return

        max_level = BackpressureLevel.NONE
        for manager_addr in manager_addrs:
            level = self._manager_backpressure.get(manager_addr, BackpressureLevel.NONE)
            if level > max_level:
                max_level = level

        self._dc_backpressure[dc_id] = max_level

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
        if not self._active_gate_peers:
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

        for peer_addr in self._active_gate_peers:
            try:
                await self.send_tcp(
                    peer_addr,
                    "manager_discovery",
                    broadcast.dump(),
                    timeout=2.0,
                )
            except Exception as discovery_error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to send manager discovery broadcast: {discovery_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    def _get_state_snapshot(self) -> GateStateSnapshot:
        """Get gate state snapshot."""
        return GateStateSnapshot(
            node_id=self._node_id.full,
            version=self._state_version,
            jobs={job_id: job for job_id, job in self._job_manager.items()},
            datacenter_managers=dict(self._datacenter_managers),
            datacenter_manager_udp=dict(self._datacenter_manager_udp),
            job_leaders=self._job_leadership_tracker.get_all_leaders(),
            job_leader_addrs=self._job_leadership_tracker.get_all_leader_addrs(),
            job_fencing_tokens=self._job_leadership_tracker.get_all_fence_tokens(),
            job_dc_managers=dict(self._job_dc_managers),
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
            if dc not in self._datacenter_managers:
                self._datacenter_managers[dc] = []
            for addr in manager_addrs:
                addr_tuple = tuple(addr) if isinstance(addr, list) else addr
                if addr_tuple not in self._datacenter_managers[dc]:
                    self._datacenter_managers[dc].append(addr_tuple)

        self._job_leadership_tracker.merge_from_snapshot(
            job_leaders=snapshot.job_leaders,
            job_leader_addrs=snapshot.job_leader_addrs,
            job_fencing_tokens=snapshot.job_fencing_tokens,
        )

        if snapshot.version > self._state_version:
            self._state_version = snapshot.version

    def _increment_version(self) -> None:
        """Increment state version."""
        self._state_version += 1

    async def _send_xprobe(self, target: tuple[str, int], data: bytes) -> bool:
        """Send cross-cluster probe."""
        try:
            await self.send(target, data, timeout=5)
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

    def _on_dc_latency(self, datacenter: str, latency_ms: float) -> None:
        """Handle DC latency update."""
        self._cross_dc_correlation.record_latency(
            datacenter_id=datacenter,
            latency_ms=latency_ms,
            probe_type="federated",
        )

    def _on_dc_leader_change(
        self,
        datacenter: str,
        leader_node_id: str,
        leader_tcp_addr: tuple[str, int],
        leader_udp_addr: tuple[str, int],
        term: int,
    ) -> None:
        """Handle DC leader change."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"DC {datacenter} leader changed to {leader_node_id}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            ),
        )

    async def _forward_workflow_result_to_peers(self, push: WorkflowResultPush) -> bool:
        """Forward workflow result to the job owner gate using consistent hashing."""
        candidates = self._job_hash_ring.get_nodes(push.job_id, count=3)

        for candidate in candidates:
            if candidate.node_id == self._node_id.full:
                continue

            try:
                gate_addr = (candidate.tcp_host, candidate.tcp_port)
                await self.send_tcp(
                    gate_addr,
                    "workflow_result_push",
                    push.dump(),
                    timeout=3.0,
                )
                return True
            except Exception as push_error:
                await self._udp_logger.log(
                    ServerDebug(
                        message=f"Failed to push result to candidate gate: {push_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                continue

        for gate_id, gate_info in list(self._known_gates.items()):
            if gate_id == self._node_id.full:
                continue
            try:
                gate_addr = (gate_info.tcp_host, gate_info.tcp_port)
                await self.send_tcp(
                    gate_addr,
                    "workflow_result_push",
                    push.dump(),
                    timeout=3.0,
                )
                return True
            except Exception as fallback_push_error:
                await self._udp_logger.log(
                    ServerDebug(
                        message=f"Failed to push result to fallback gate: {fallback_push_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                continue

        return False

    async def _aggregate_and_forward_workflow_result(
        self,
        job_id: str,
        workflow_id: str,
    ) -> None:
        """Aggregate workflow results from all DCs and forward to client."""
        workflow_results = self._workflow_dc_results.get(job_id, {}).get(
            workflow_id, {}
        )
        if not workflow_results:
            return

        first_dc_push = next(iter(workflow_results.values()))
        is_test_workflow = first_dc_push.is_test

        all_workflow_stats: list[WorkflowStats] = []
        per_dc_results: list[WorkflowDCResult] = []
        workflow_name = ""
        has_failure = False
        error_messages: list[str] = []
        max_elapsed = 0.0

        for datacenter, dc_push in workflow_results.items():
            workflow_name = dc_push.workflow_name
            all_workflow_stats.extend(dc_push.results)

            if is_test_workflow:
                dc_aggregated_stats: WorkflowStats | None = None
                if dc_push.results:
                    if len(dc_push.results) > 1:
                        aggregator = Results()
                        dc_aggregated_stats = aggregator.merge_results(dc_push.results)
                    else:
                        dc_aggregated_stats = dc_push.results[0]

                per_dc_results.append(
                    WorkflowDCResult(
                        datacenter=datacenter,
                        status=dc_push.status,
                        stats=dc_aggregated_stats,
                        error=dc_push.error,
                        elapsed_seconds=dc_push.elapsed_seconds,
                    )
                )
            else:
                per_dc_results.append(
                    WorkflowDCResult(
                        datacenter=datacenter,
                        status=dc_push.status,
                        stats=None,
                        error=dc_push.error,
                        elapsed_seconds=dc_push.elapsed_seconds,
                        raw_results=dc_push.results,
                    )
                )

            if dc_push.status == "FAILED":
                has_failure = True
                if dc_push.error:
                    error_messages.append(f"{datacenter}: {dc_push.error}")

            if dc_push.elapsed_seconds > max_elapsed:
                max_elapsed = dc_push.elapsed_seconds

        if not all_workflow_stats:
            return

        status = "FAILED" if has_failure else "COMPLETED"
        error = "; ".join(error_messages) if error_messages else None

        if is_test_workflow:
            aggregator = Results()
            if len(all_workflow_stats) > 1:
                aggregated = aggregator.merge_results(all_workflow_stats)
            else:
                aggregated = all_workflow_stats[0]
            results_to_send = [aggregated]
        else:
            results_to_send = all_workflow_stats

        client_push = WorkflowResultPush(
            job_id=job_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            datacenter="aggregated",
            status=status,
            results=results_to_send,
            error=error,
            elapsed_seconds=max_elapsed,
            per_dc_results=per_dc_results,
            completed_at=time.time(),
            is_test=is_test_workflow,
        )

        callback = self._job_manager.get_callback(job_id)
        if callback:
            try:
                await self.send_tcp(
                    callback,
                    "workflow_result_push",
                    client_push.dump(),
                    timeout=5.0,
                )
            except Exception as error:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Failed to send workflow result to client {callback}: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )

        if job_id in self._workflow_dc_results:
            self._workflow_dc_results[job_id].pop(workflow_id, None)

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
                    timeout=5.0,
                )
                if isinstance(response_data, Exception) or response_data == b"error":
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
            self._job_dc_managers.get(request.job_id, {}) if request.job_id else {}
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

        manager_statuses = self._datacenter_manager_status.get(dc_id, {})
        fallback_addr: tuple[str, int] | None = None

        for manager_addr, heartbeat in manager_statuses.items():
            if fallback_addr is None:
                fallback_addr = (heartbeat.tcp_host, heartbeat.tcp_port)

            if heartbeat.is_leader:
                return (heartbeat.tcp_host, heartbeat.tcp_port)

        return fallback_addr

    def _clear_orphaned_job(
        self,
        job_id: str,
        new_manager_addr: tuple[str, int],
    ) -> None:
        """Clear orphaned status when a new manager takes over a job."""
        self._orphaned_jobs.pop(job_id, None)

    async def _wait_for_cluster_stabilization(self) -> None:
        """Wait for SWIM cluster to stabilize."""
        expected_peers = len(self._gate_udp_peers)
        if expected_peers == 0:
            return

        timeout = self.env.CLUSTER_STABILIZATION_TIMEOUT
        poll_interval = self.env.CLUSTER_STABILIZATION_POLL_INTERVAL
        start_time = time.monotonic()

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

            if time.monotonic() - start_time >= timeout:
                return

            await asyncio.sleep(poll_interval)

    async def _complete_startup_sync(self) -> None:
        """Complete startup sync and transition to ACTIVE."""
        if self.is_leader():
            self._gate_state = GateState.ACTIVE
            return

        leader_addr = self.get_current_leader()
        if leader_addr:
            leader_tcp_addr = self._gate_udp_to_tcp.get(leader_addr)
            if leader_tcp_addr:
                await self._sync_state_from_peer(leader_tcp_addr)

        self._gate_state = GateState.ACTIVE

    async def _sync_state_from_peer(
        self,
        peer_tcp_addr: tuple[str, int],
    ) -> bool:
        """Sync state from peer gate."""
        try:
            request = GateStateSyncRequest(
                requester_id=self._node_id.full,
                known_version=self._state_version,
            )

            result, _ = await self.send_tcp(
                peer_tcp_addr,
                "state_sync",
                request.dump(),
                timeout=5.0,
            )

            if isinstance(result, bytes) and len(result) > 0:
                response = GateStateSyncResponse.load(result)
                if not response.error and response.snapshot:
                    await self._apply_gate_state_snapshot(response.snapshot)
                    return True

            return False

        except Exception as sync_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to sync state from peer: {sync_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False

    async def _register_with_managers(self) -> None:
        """Register with all managers."""
        for dc_id, manager_addrs in self._datacenter_managers.items():
            for manager_addr in manager_addrs:
                try:
                    request = GateRegistrationRequest(
                        node_id=self._node_id.full,
                        tcp_host=self._host,
                        tcp_port=self._tcp_port,
                        udp_host=self._host,
                        udp_port=self._udp_port,
                        is_leader=self.is_leader(),
                        term=self._leader_election.state.current_term,
                        state=self._gate_state.value,
                        cluster_id=self.env.CLUSTER_ID,
                        environment_id=self.env.ENVIRONMENT_ID,
                        active_jobs=self._job_manager.count_active_jobs(),
                        manager_count=sum(
                            len(addrs) for addrs in self._datacenter_managers.values()
                        ),
                        protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                        protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                        capabilities=",".join(
                            sorted(self._node_capabilities.capabilities)
                        ),
                    )

                    await self.send_tcp(
                        manager_addr,
                        "gate_register",
                        request.dump(),
                        timeout=5.0,
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
                await asyncio.sleep(self._lease_timeout / 2)
                self._dc_lease_manager.cleanup_expired()

                now = time.monotonic()
                expired = [
                    key for key, lease in self._leases.items() if lease.expires_at < now
                ]
                for key in expired:
                    self._leases.pop(key, None)

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "lease_cleanup_loop")

    async def _job_cleanup_loop(self) -> None:
        """Periodically clean up completed jobs."""
        terminal_states = {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }

        while self._running:
            try:
                await asyncio.sleep(self._job_cleanup_interval)

                now = time.monotonic()
                jobs_to_remove = []

                for job_id, job in list(self._job_manager.items()):
                    if job.status in terminal_states:
                        age = now - getattr(job, "timestamp", now)
                        if age > self._job_max_age:
                            jobs_to_remove.append(job_id)

                for job_id in jobs_to_remove:
                    self._job_manager.delete_job(job_id)
                    self._workflow_dc_results.pop(job_id, None)
                    self._job_workflow_ids.pop(job_id, None)
                    self._progress_callbacks.pop(job_id, None)
                    self._job_leadership_tracker.release_leadership(job_id)
                    self._job_dc_managers.pop(job_id, None)

                    reporter_tasks = self._job_reporter_tasks.pop(job_id, None)
                    if reporter_tasks:
                        for task in reporter_tasks.values():
                            if task and not task.done():
                                task.cancel()

                    self._job_stats_crdt.pop(job_id, None)

                    state_reporter_tasks = self._state._job_reporter_tasks.pop(
                        job_id, None
                    )
                    if state_reporter_tasks:
                        for task in state_reporter_tasks.values():
                            if task and not task.done():
                                task.cancel()

                    if self._job_router:
                        self._job_router.cleanup_job_state(job_id)

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "job_cleanup_loop")

    async def _rate_limit_cleanup_loop(self) -> None:
        """Periodically clean up rate limiter."""
        while self._running:
            try:
                await asyncio.sleep(self._rate_limit_cleanup_interval)
                self._rate_limiter.cleanup_inactive_clients()
            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "rate_limit_cleanup_loop")

    async def _batch_stats_loop(self) -> None:
        """Background loop for batch stats updates."""
        while self._running:
            try:
                await asyncio.sleep(self._batch_stats_interval)
                if not self._running:
                    break
                await self._batch_stats_update()
            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "batch_stats_loop")

    async def _batch_stats_update(self) -> None:
        """Process batch stats update."""
        if self._stats_coordinator:
            await self._stats_coordinator.batch_stats_update()

    async def _windowed_stats_push_loop(self) -> None:
        """Background loop for windowed stats push."""
        while self._running:
            try:
                await asyncio.sleep(self._stats_push_interval_ms / 1000.0)
                if not self._running:
                    break
                if self._stats_coordinator:
                    await self._stats_coordinator.push_windowed_stats()
            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "windowed_stats_push_loop")

    async def _discovery_maintenance_loop(self) -> None:
        """Discovery maintenance loop (AD-28)."""
        while self._running:
            try:
                await asyncio.sleep(self._discovery_failure_decay_interval)

                for dc_discovery in self._dc_manager_discovery.values():
                    dc_discovery.decay_failures()

                self._peer_discovery.decay_failures()

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self.handle_exception(error, "discovery_maintenance_loop")

    # =========================================================================
    # Coordinator Accessors
    # =========================================================================

    @property
    def stats_coordinator(self) -> GateStatsCoordinator | None:
        """Get the stats coordinator."""
        return self._stats_coordinator

    @property
    def cancellation_coordinator(self) -> GateCancellationCoordinator | None:
        """Get the cancellation coordinator."""
        return self._cancellation_coordinator

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


__all__ = [
    "GateServer",
    "GateConfig",
    "create_gate_config",
    "GateRuntimeState",
    "GateStatsCoordinator",
    "GateCancellationCoordinator",
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
