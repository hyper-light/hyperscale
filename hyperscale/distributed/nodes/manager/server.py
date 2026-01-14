"""
Manager server composition root.

Thin orchestration layer that wires all manager modules together.
All business logic is delegated to specialized coordinators.
"""

import asyncio
import random
import time
import cloudpickle
from pathlib import Path

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.swim import HealthAwareServer, ManagerStateEmbedder
from hyperscale.distributed.swim.core import ErrorStats
from hyperscale.distributed.swim.detection import HierarchicalConfig
from hyperscale.distributed.swim.health import FederatedHealthMonitor
from hyperscale.distributed.env import Env
from hyperscale.distributed.server import tcp
from hyperscale.distributed.idempotency import (
    IdempotencyKey,
    IdempotencyStatus,
    ManagerIdempotencyLedger,
    create_idempotency_config_from_env,
)

from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.distributed.models import (
    NodeInfo,
    NodeRole,
    ManagerInfo,
    ManagerState as ManagerStateEnum,
    ManagerHeartbeat,
    ManagerStateSnapshot,
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
    JobCancellationComplete,
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkflowProgress,
    WorkflowProgressAck,
    WorkflowFinalResult,
    WorkflowResult,
    WorkflowStatus,
    StateSyncRequest,
    StateSyncResponse,
    JobCancelRequest,
    JobCancelResponse,
    CancelJob,
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    WorkflowCancellationComplete,
    WorkflowCancellationQuery,
    WorkflowCancellationResponse,
    WorkflowCancellationStatus,
    SingleWorkflowCancelRequest,
    SingleWorkflowCancelResponse,
    WorkflowCancellationPeerNotification,
    CancelledWorkflowInfo,
    HealthcheckExtensionRequest,
    HealthcheckExtensionResponse,
    WorkerDiscoveryBroadcast,
    ContextForward,
    ContextLayerSync,
    ContextLayerSyncAck,
    JobLeadershipAnnouncement,
    JobLeadershipAck,
    JobStateSyncMessage,
    JobStateSyncAck,
    JobLeaderGateTransfer,
    JobLeaderGateTransferAck,
    ProvisionRequest,
    ProvisionConfirm,
    ProvisionCommit,
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
    restricted_loads,
    JobInfo,
    WorkflowInfo,
)
from hyperscale.distributed.models.worker_state import (
    WorkerStateUpdate,
    WorkerListResponse,
    WorkflowReassignmentBatch,
)
from hyperscale.distributed.reliability import (
    HybridOverloadDetector,
    ServerRateLimiter,
    StatsBuffer,
    StatsBufferConfig,
)
from hyperscale.distributed.resources import ProcessResourceMonitor, ResourceMetrics
from hyperscale.distributed.health import WorkerHealthManager, WorkerHealthManagerConfig
from hyperscale.distributed.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    NodeCapabilities,
    ProtocolVersion,
    negotiate_capabilities,
    get_features_for_version,
)
from hyperscale.distributed.discovery.security.role_validator import RoleValidator
from hyperscale.distributed.nodes.manager.health import NodeStatus
from hyperscale.distributed.jobs import (
    JobManager,
    WorkerPool,
    WorkflowDispatcher,
    WindowedStatsCollector,
    WindowedStatsPush,
)
from hyperscale.distributed.ledger.wal import NodeWAL
from hyperscale.logging.lsn import HybridLamportClock
from hyperscale.distributed.jobs.timeout_strategy import (
    TimeoutStrategy,
    LocalAuthorityTimeout,
    GateCoordinatedTimeout,
)
from hyperscale.distributed.workflow import (
    WorkflowStateMachine as WorkflowLifecycleStateMachine,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerWarning,
    ServerError,
    ServerDebug,
)

from .config import create_manager_config_from_env
from .state import ManagerState
from .registry import ManagerRegistry
from .dispatch import ManagerDispatchCoordinator
from .cancellation import ManagerCancellationCoordinator
from .leases import ManagerLeaseCoordinator
from .health import ManagerHealthMonitor, HealthcheckExtensionManager
from .sync import ManagerStateSync
from .leadership import ManagerLeadershipCoordinator
from .stats import ManagerStatsCoordinator
from .discovery import ManagerDiscoveryCoordinator
from .load_shedding import ManagerLoadShedder

from .workflow_lifecycle import ManagerWorkflowLifecycle
from .worker_dissemination import WorkerDisseminator
from hyperscale.distributed.swim.gossip.worker_state_gossip_buffer import (
    WorkerStateGossipBuffer,
)


class ManagerServer(HealthAwareServer):
    """
    Manager node composition root.

    Orchestrates workflow execution within a datacenter by:
    - Receiving jobs from gates (or directly from clients)
    - Dispatching workflows to workers
    - Aggregating status updates from workers
    - Reporting to gates (if present)
    - Participating in leader election among managers
    - Handling quorum-based confirmation for workflow provisioning
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
        max_workflow_retries: int = 3,
        workflow_timeout: float = 300.0,
        wal_data_dir: Path | None = None,
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
            max_workflow_retries: Maximum retry attempts per workflow
            workflow_timeout: Workflow execution timeout in seconds
        """
        from .config import ManagerConfig

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
            max_workflow_retries=max_workflow_retries,
            workflow_timeout=workflow_timeout,
            wal_data_dir=wal_data_dir,
        )

        self._node_wal: NodeWAL | None = None

        self._env: Env = env
        self._seed_gates: list[tuple[str, int]] = gate_addrs or []
        self._gate_udp_addrs: list[tuple[str, int]] = gate_udp_addrs or []
        self._seed_managers: list[tuple[str, int]] = (
            seed_managers or manager_peers or []
        )
        self._manager_udp_peers: list[tuple[str, int]] = manager_udp_peers or []
        self._max_workflow_retries: int = max_workflow_retries
        self._workflow_timeout: float = workflow_timeout

        self._manager_state: ManagerState = ManagerState()
        self._idempotency_config = create_idempotency_config_from_env(env)
        self._idempotency_ledger: ManagerIdempotencyLedger[bytes] | None = None

        # Initialize parent HealthAwareServer
        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
            node_role="manager",
        )

        # Wire logger to modules
        self._init_modules()

        # Initialize address mappings for SWIM callbacks
        self._init_address_mappings()

        # Register callbacks
        self._register_callbacks()

    def _init_modules(self) -> None:
        """Initialize all modular coordinators."""
        # Registry for workers, gates, peers
        self._registry = ManagerRegistry(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Lease coordinator for fencing tokens and job leadership
        self._leases = ManagerLeaseCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Health monitor for worker health tracking
        self._health_monitor = ManagerHealthMonitor(
            state=self._manager_state,
            config=self._config,
            registry=self._registry,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Extension manager for AD-26 deadline extensions
        self._extension_manager = HealthcheckExtensionManager(
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Dispatch coordinator for workflow dispatch
        self._dispatch = ManagerDispatchCoordinator(
            state=self._manager_state,
            config=self._config,
            registry=self._registry,
            leases=self._leases,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            send_to_worker=self._send_to_worker,
            send_to_peer=self._send_to_peer,
        )

        # Cancellation coordinator for AD-20
        self._cancellation = ManagerCancellationCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            send_to_worker=self._send_to_worker,
            send_to_client=self._send_to_client,
        )

        # State sync coordinator
        self._state_sync = ManagerStateSync(
            state=self._manager_state,
            config=self._config,
            registry=self._registry,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            send_tcp=self._send_to_peer,
        )

        # Leadership coordinator
        self._leadership = ManagerLeadershipCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            is_leader_fn=self.is_leader,
            get_term_fn=lambda: self._leader_election.state.current_term
            if hasattr(self, "_leader_election")
            else 0,
        )

        # Discovery coordinator
        self._discovery = ManagerDiscoveryCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
            env=self._env,
        )

        # Load shedding (AD-22)
        self._overload_detector = HybridOverloadDetector()
        self._resource_monitor = ProcessResourceMonitor()
        self._last_resource_metrics: "ResourceMetrics | None" = None
        self._manager_health_state: str = "healthy"
        self._manager_health_state_snapshot: str = "healthy"
        self._previous_manager_health_state: str = "healthy"
        self._manager_health_state_lock: asyncio.Lock = asyncio.Lock()
        self._load_shedder = ManagerLoadShedder(
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # JobManager for race-safe job/workflow state
        self._job_manager = JobManager(
            datacenter=self._node_id.datacenter,
            manager_id=self._node_id.short,
        )

        self._worker_pool = WorkerPool(
            health_grace_period=30.0,
            get_swim_status=self._get_swim_status_for_worker,
            manager_id=self._node_id.short,
            datacenter=self._node_id.datacenter,
        )

        self._registry.set_worker_pool(self._worker_pool)

        # Workflow lifecycle state machine (AD-33)
        self._workflow_lifecycle = ManagerWorkflowLifecycle(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # Rate limiting (AD-24)
        self._rate_limiter = ServerRateLimiter(inactive_cleanup_seconds=300.0)

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
        )

        # Worker health manager (AD-26)
        self._worker_health_manager = WorkerHealthManager(
            WorkerHealthManagerConfig(
                base_deadline=30.0,
                min_grant=1.0,
                max_extensions=5,
                eviction_threshold=3,
            )
        )

        # WorkflowDispatcher (initialized in start())
        self._workflow_dispatcher: WorkflowDispatcher | None = None

        # WorkflowLifecycleStateMachine (initialized in start())
        self._workflow_lifecycle_states: WorkflowLifecycleStateMachine | None = None

        # WorkerDisseminator (AD-48, initialized in start())
        self._worker_disseminator: "WorkerDisseminator | None" = None

        # Federated health monitor for gate probing
        fed_config = self._env.get_federated_health_config()
        self._gate_health_monitor = FederatedHealthMonitor(
            probe_interval=fed_config["probe_interval"],
            probe_timeout=fed_config["probe_timeout"],
            suspicion_timeout=fed_config["suspicion_timeout"],
            max_consecutive_failures=fed_config["max_consecutive_failures"],
        )

        # Gate circuit breaker
        cb_config = self._env.get_circuit_breaker_config()
        self._gate_circuit = ErrorStats(
            max_errors=cb_config["max_errors"],
            window_seconds=cb_config["window_seconds"],
            half_open_after=cb_config["half_open_after"],
        )

        # Quorum circuit breaker
        self._quorum_circuit = ErrorStats(
            window_seconds=30.0,
            max_errors=3,
            half_open_after=10.0,
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

        # Protocol capabilities
        self._node_capabilities = NodeCapabilities.current(node_version="")

        # Background tasks
        self._dead_node_reap_task: asyncio.Task | None = None
        self._orphan_scan_task: asyncio.Task | None = None
        self._discovery_maintenance_task: asyncio.Task | None = None
        self._job_responsiveness_task: asyncio.Task | None = None
        self._stats_push_task: asyncio.Task | None = None
        self._windowed_stats_flush_task: asyncio.Task | None = None
        self._gate_heartbeat_task: asyncio.Task | None = None
        self._rate_limit_cleanup_task: asyncio.Task | None = None
        self._job_cleanup_task: asyncio.Task | None = None
        self._unified_timeout_task: asyncio.Task | None = None
        self._deadline_enforcement_task: asyncio.Task | None = None
        self._peer_job_state_sync_task: asyncio.Task | None = None
        self._resource_sample_task: asyncio.Task | None = None

    def _init_address_mappings(self) -> None:
        """Initialize UDP to TCP address mappings."""
        # Gate UDP to TCP mapping
        for idx, tcp_addr in enumerate(self._seed_gates):
            if idx < len(self._gate_udp_addrs):
                self._manager_state.set_gate_udp_to_tcp_mapping(
                    self._gate_udp_addrs[idx], tcp_addr
                )

        # Manager UDP to TCP mapping
        for idx, tcp_addr in enumerate(self._seed_managers):
            if idx < len(self._manager_udp_peers):
                self._manager_state.set_manager_udp_to_tcp_mapping(
                    self._manager_udp_peers[idx], tcp_addr
                )

    def _register_callbacks(self) -> None:
        """Register SWIM and leadership callbacks."""
        self.register_on_become_leader(self._on_manager_become_leader)
        self.register_on_lose_leadership(self._on_manager_lose_leadership)
        self.register_on_node_dead(self._on_node_dead)
        self.register_on_node_join(self._on_node_join)
        self.register_on_peer_confirmed(self._on_peer_confirmed)

        # Initialize hierarchical failure detector (AD-30)
        self.init_hierarchical_detector(
            config=HierarchicalConfig(
                global_min_timeout=10.0,
                global_max_timeout=60.0,
                job_min_timeout=2.0,
                job_max_timeout=15.0,
            ),
            on_global_death=self._on_worker_globally_dead,
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
            get_health_accepting_jobs=lambda: self._manager_state.manager_state_enum
            == ManagerStateEnum.ACTIVE,
            get_health_has_quorum=self._has_quorum_available,
            get_health_throughput=self._get_dispatch_throughput,
            get_health_expected_throughput=self._get_expected_dispatch_throughput,
            get_health_overload_state=lambda: self._manager_health_state,
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

    @property
    def _quorum_size(self) -> int:
        """Calculate required quorum size."""
        return (self._manager_state.get_active_peer_count() // 2) + 1

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    async def start(self, timeout: float | None = None) -> None:
        """Start the manager server."""
        # Initialize locks (requires async context)
        self._manager_state.initialize_locks()

        # Start the underlying server
        await self.start_server(init_context=self._env.get_swim_init_context())

        if self._config.wal_data_dir is not None:
            wal_clock = HybridLamportClock(node_id=hash(self._node_id.full) & 0xFFFF)
            self._node_wal = await NodeWAL.open(
                path=self._config.wal_data_dir / "wal",
                clock=wal_clock,
                logger=self._udp_logger,
            )

        ledger_base_dir = (
            self._config.wal_data_dir
            if self._config.wal_data_dir is not None
            else Path(self._env.MERCURY_SYNC_LOGS_DIRECTORY)
        )
        ledger_path = ledger_base_dir / f"manager-idempotency-{self._node_id.short}.wal"
        self._idempotency_ledger = ManagerIdempotencyLedger(
            config=self._idempotency_config,
            wal_path=ledger_path,
            task_runner=self._task_runner,
            logger=self._udp_logger,
        )
        await self._idempotency_ledger.start()

        # Update node capabilities with proper version
        self._node_capabilities = NodeCapabilities.current(
            node_version=f"manager-{self._node_id.short}"
        )

        # Initialize workflow lifecycle state machine (AD-33)
        self._workflow_lifecycle_states = WorkflowLifecycleStateMachine()

        self._workflow_dispatcher = WorkflowDispatcher(
            job_manager=self._job_manager,
            worker_pool=self._worker_pool,
            manager_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            send_dispatch=self._send_workflow_dispatch,
            env=self.env,
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

        # Request worker lists from peer managers (AD-48)
        if self._worker_disseminator:
            await self._worker_disseminator.request_worker_list_from_peers()

        # Start SWIM probe cycle
        self._task_runner.run(self.start_probe_cycle)

        # Start background tasks
        self._start_background_tasks()

        manager_count = self._manager_state.get_known_manager_peer_count() + 1
        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager started, {manager_count} managers in cluster",
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
        """Stop the manager server."""
        if not self._running and not hasattr(self, "_started"):
            return

        self._running = False
        self._manager_state.set_manager_state_enum(ManagerStateEnum.DRAINING)

        # Cancel background tasks
        await self._cancel_background_tasks()

        if self._idempotency_ledger is not None:
            await self._idempotency_ledger.close()

        if self._node_wal is not None:
            await self._node_wal.close()

        # Graceful shutdown
        await super().stop(
            drain_timeout=drain_timeout,
            broadcast_leave=broadcast_leave,
        )

    def abort(self) -> None:
        """Abort the manager server immediately."""
        self._running = False
        self._manager_state.set_manager_state_enum(ManagerStateEnum.OFFLINE)

        # Cancel all background tasks synchronously
        for task in self._get_background_tasks():
            if task and not task.done():
                task.cancel()

        super().abort()

    def _get_background_tasks(self) -> list[asyncio.Task | None]:
        """Get list of background tasks."""
        return [
            self._dead_node_reap_task,
            self._orphan_scan_task,
            self._discovery_maintenance_task,
            self._job_responsiveness_task,
            self._stats_push_task,
            self._windowed_stats_flush_task,
            self._gate_heartbeat_task,
            self._rate_limit_cleanup_task,
            self._job_cleanup_task,
            self._unified_timeout_task,
            self._deadline_enforcement_task,
            self._peer_job_state_sync_task,
        ]

    def _start_background_tasks(self) -> None:
        self._dead_node_reap_task = self._create_background_task(
            self._dead_node_reap_loop(), "dead_node_reap"
        )
        self._orphan_scan_task = self._create_background_task(
            self._orphan_scan_loop(), "orphan_scan"
        )
        self._discovery_maintenance_task = self._create_background_task(
            self._discovery.maintenance_loop(), "discovery_maintenance"
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
        self._peer_job_state_sync_task = self._create_background_task(
            self._peer_job_state_sync_loop(), "peer_job_state_sync"
        )
        self._resource_sample_task = self._create_background_task(
            self._resource_sample_loop(), "resource_sample"
        )

    async def _cancel_background_tasks(self) -> None:
        """Cancel all background tasks."""
        for task in self._get_background_tasks():
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    # =========================================================================
    # Registration
    # =========================================================================

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

    async def _register_with_manager(self, manager_addr: tuple[str, int]) -> bool:
        """Register with a single peer manager."""
        registration = ManagerPeerRegistration(
            node=self.node_info,
            manager_info=ManagerInfo(
                node_id=self._node_id.full,
                tcp_host=self._host,
                tcp_port=self._tcp_port,
                udp_host=self._host,
                udp_port=self._udp_port,
                datacenter=self._node_id.datacenter,
                is_leader=self.is_leader(),
            ),
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
        )

        try:
            response = await self.send_tcp(
                manager_addr,
                "manager_peer_register",
                registration.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )

            if response and not isinstance(response, Exception):
                parsed = ManagerPeerRegistrationResponse.load(response)
                if parsed.accepted:
                    for peer_info in parsed.known_peers:
                        self._registry.register_manager_peer(peer_info)
                    return True

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Manager registration error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        return False

    async def _join_swim_clusters(self) -> None:
        """Join SWIM clusters for managers, gates, and workers."""
        # Join manager SWIM cluster
        for udp_addr in self._manager_udp_peers:
            await self.join_cluster(udp_addr)

        # Join gate SWIM cluster if gates configured
        for udp_addr in self._gate_udp_addrs:
            await self.join_cluster(udp_addr)

    # =========================================================================
    # SWIM Callbacks
    # =========================================================================

    def _on_peer_confirmed(self, peer: tuple[str, int]) -> None:
        """Handle peer confirmation via SWIM (AD-29)."""
        # Check if manager peer
        tcp_addr = self._manager_state.get_manager_tcp_from_udp(peer)
        if tcp_addr:
            for peer_id, peer_info in self._manager_state.iter_known_manager_peers():
                if (peer_info.udp_host, peer_info.udp_port) == peer:
                    self._task_runner.run(
                        self._manager_state.add_active_peer, tcp_addr, peer_id
                    )
                    break

    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """Handle node death detected by SWIM."""
        worker_id = self._manager_state.get_worker_id_from_addr(node_addr)
        if worker_id:
            self._manager_state.setdefault_worker_unhealthy_since(
                worker_id, time.monotonic()
            )
            self._task_runner.run(self._handle_worker_failure, worker_id)
            return

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

        # Check if manager peer
        manager_tcp_addr = self._manager_state.get_manager_tcp_from_udp(node_addr)
        if manager_tcp_addr:
            dead_managers = self._manager_state.get_dead_managers()
            dead_managers.discard(manager_tcp_addr)
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
        self._task_runner.run(self._sync_state_from_workers)
        self._task_runner.run(self._sync_state_from_manager_peers)
        self._task_runner.run(self._scan_for_orphaned_jobs)
        self._task_runner.run(self._resume_timeout_tracking_for_all_jobs)

    def _on_manager_lose_leadership(self) -> None:
        """Handle losing SWIM cluster leadership."""
        pass

    def _on_worker_globally_dead(self, worker_id: str) -> None:
        """Handle worker global death (AD-30)."""
        self._health_monitor.on_global_death(worker_id)
        if self._worker_disseminator:
            self._task_runner.run(
                self._worker_disseminator.broadcast_worker_dead, worker_id, "dead"
            )

    def _on_worker_dead_for_job(self, job_id: str, worker_id: str) -> None:
        if not self._workflow_dispatcher or not self._job_manager:
            return

        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return

        sub_workflows_to_requeue = [
            sub.token_str
            for sub in job.sub_workflows.values()
            if sub.worker_id == worker_id and sub.result is None
        ]

        for sub_token in sub_workflows_to_requeue:
            self._task_runner.run(
                self._workflow_dispatcher.requeue_workflow,
                sub_token,
            )

    # =========================================================================
    # Failure/Recovery Handlers
    # =========================================================================

    async def _handle_worker_failure(self, worker_id: str) -> None:
        self._health_monitor.handle_worker_failure(worker_id)

        if self._workflow_dispatcher and self._job_manager:
            running_sub_workflows = (
                self._job_manager.get_running_sub_workflows_on_worker(worker_id)
            )

            for job_id, workflow_id, sub_token in running_sub_workflows:
                requeued = await self._workflow_dispatcher.requeue_workflow(sub_token)

                if requeued:
                    await self._udp_logger.log(
                        ServerInfo(
                            message=f"Requeued workflow {workflow_id[:8]}... from failed worker {worker_id[:8]}...",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                else:
                    await self._udp_logger.log(
                        ServerWarning(
                            message=f"Failed to requeue workflow {workflow_id[:8]}... from failed worker {worker_id[:8]}... - not found in pending",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            if running_sub_workflows and self._worker_disseminator:
                await self._worker_disseminator.broadcast_workflow_reassignments(
                    failed_worker_id=worker_id,
                    reason="worker_dead",
                    reassignments=running_sub_workflows,
                )

        self._manager_state.remove_worker_state(worker_id)

    async def _handle_manager_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        peer_lock = await self._manager_state.get_peer_state_lock(tcp_addr)
        async with peer_lock:
            self._manager_state.increment_peer_state_epoch(tcp_addr)
            self._manager_state.remove_active_manager_peer(tcp_addr)
            self._manager_state.add_dead_manager(tcp_addr, time.monotonic())

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager peer {tcp_addr} marked DEAD",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        await self._handle_job_leader_failure(tcp_addr)
        await self._check_quorum_status()

    async def _handle_manager_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        peer_lock = await self._manager_state.get_peer_state_lock(tcp_addr)

        async with peer_lock:
            initial_epoch = self._manager_state.get_peer_state_epoch(tcp_addr)

        async with self._recovery_semaphore:
            jitter = random.uniform(
                self._config.recovery_jitter_min_seconds,
                self._config.recovery_jitter_max_seconds,
            )
            await asyncio.sleep(jitter)

            async with peer_lock:
                current_epoch = self._manager_state.get_peer_state_epoch(tcp_addr)
                if current_epoch != initial_epoch:
                    return

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
                return

            async with peer_lock:
                current_epoch = self._manager_state.get_peer_state_epoch(tcp_addr)
                if current_epoch != initial_epoch:
                    return

                self._manager_state.add_active_manager_peer(tcp_addr)
                self._manager_state.remove_dead_manager(tcp_addr)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager peer {tcp_addr} REJOINED (verified)",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _verify_peer_recovery(self, tcp_addr: tuple[str, int]) -> bool:
        try:
            ping_request = PingRequest(requester_id=self._node_id.full)
            response = await asyncio.wait_for(
                self._send_to_peer(
                    tcp_addr,
                    "ping",
                    ping_request.dump(),
                    self._config.tcp_timeout_short_seconds,
                ),
                timeout=self._config.tcp_timeout_short_seconds + 1.0,
            )
            return response is not None and response != b"error"
        except (asyncio.TimeoutError, Exception):
            return False

    async def _handle_gate_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer failure."""
        # Find gate by address
        gate_node_id = None
        for gate_id, gate_info in self._manager_state.iter_known_gates():
            if (gate_info.tcp_host, gate_info.tcp_port) == tcp_addr:
                gate_node_id = gate_id
                break

        if gate_node_id:
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
            if (gate_info.tcp_host, gate_info.tcp_port) == tcp_addr:
                self._registry.mark_gate_healthy(gate_id)
                break

            elif (gate_info.udp_host, gate_info.udp_port) == udp_addr:
                self._registry.mark_gate_healthy(gate_id)
                break

    async def _handle_job_leader_failure(self, failed_addr: tuple[str, int]) -> None:
        """Handle job leader manager failure."""
        if not self.is_leader():
            return

        jobs_to_takeover = [
            job_id
            for job_id, leader_addr in self._manager_state.iter_job_leader_addrs()
            if leader_addr == failed_addr
        ]

        for job_id in jobs_to_takeover:
            self._leases.claim_job_leadership(
                job_id,
                (self._host, self._tcp_port),
                force_takeover=True,
            )
            await self._udp_logger.log(
                ServerInfo(
                    message=f"Took over leadership for job {job_id[:8]}...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _check_quorum_status(self) -> None:
        has_quorum = self._leadership.has_quorum()

        if has_quorum:
            self._manager_state.reset_quorum_failures()
            return

        failure_count = self._manager_state.increment_quorum_failures()

        if not self.is_leader():
            return

        max_quorum_failures = 3
        if failure_count >= max_quorum_failures:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Lost quorum for {failure_count} consecutive checks, stepping down",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            self._task_runner.run(self._leader_election._step_down)

    def _should_backup_orphan_scan(self) -> bool:
        if self.is_leader():
            return False

        leader_addr = self._leader_election.state.current_leader
        if leader_addr is None:
            return True

        leader_last_seen = self._leader_election.state.last_heartbeat_time
        leader_timeout = self._config.orphan_scan_interval_seconds * 3
        return (time.monotonic() - leader_last_seen) > leader_timeout

    # =========================================================================
    # Heartbeat Handlers
    # =========================================================================

    async def _handle_embedded_worker_heartbeat(
        self,
        heartbeat: WorkerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        self._health_monitor.handle_worker_heartbeat(heartbeat, source_addr)

        worker_id = heartbeat.node_id
        if self._manager_state.has_worker(worker_id):
            await self._worker_pool.process_heartbeat(worker_id, heartbeat)

    async def _handle_manager_peer_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        peer_id = heartbeat.node_id

        if not self._manager_state.has_known_manager_peer(peer_id):
            peer_info = ManagerInfo(
                node_id=peer_id,
                tcp_host=heartbeat.tcp_host or source_addr[0],
                tcp_port=heartbeat.tcp_port or source_addr[1] - 1,
                udp_host=source_addr[0],
                udp_port=source_addr[1],
                datacenter=heartbeat.datacenter,
                is_leader=heartbeat.is_leader,
            )
            self._registry.register_manager_peer(peer_info)

        if heartbeat.is_leader:
            self._manager_state.set_dc_leader_manager_id(peer_id)

        peer_health_state = getattr(heartbeat, "health_overload_state", "healthy")
        previous_peer_state = self._manager_state.get_peer_manager_health_state(peer_id)
        self._manager_state.set_peer_manager_health_state(peer_id, peer_health_state)

        if previous_peer_state and previous_peer_state != peer_health_state:
            self._log_peer_manager_health_transition(
                peer_id, previous_peer_state, peer_health_state
            )
            self._health_monitor.check_peer_manager_health_alerts()

        self.confirm_peer(source_addr)

    async def _handle_gate_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """Handle embedded gate heartbeat from SWIM."""
        gate_id = heartbeat.node_id

        # Register gate if not known
        if not self._manager_state.get_known_gate(gate_id):
            gate_info = GateInfo(
                node_id=gate_id,
                tcp_host=heartbeat.tcp_host or source_addr[0],
                tcp_port=heartbeat.tcp_port or source_addr[1] - 1,
                udp_host=source_addr[0],
                udp_port=source_addr[1],
                datacenter=heartbeat.datacenter,
                is_leader=heartbeat.is_leader,
            )
            self._registry.register_gate(gate_info)

        # Update gate leader tracking
        if heartbeat.is_leader:
            gate_info = self._manager_state.get_known_gate(gate_id)
            if gate_info:
                self._manager_state.set_current_gate_leader(
                    gate_id, (gate_info.tcp_host, gate_info.tcp_port)
                )
            else:
                self._manager_state.set_current_gate_leader(gate_id, None)

        # Confirm peer
        self.confirm_peer(source_addr)

    # =========================================================================
    # Background Loops
    # =========================================================================

    def _reap_dead_workers(self, now: float) -> None:
        worker_reap_threshold = now - self._config.dead_worker_reap_interval_seconds
        workers_to_reap = [
            worker_id
            for worker_id, unhealthy_since in self._manager_state.iter_worker_unhealthy_since()
            if unhealthy_since < worker_reap_threshold
        ]
        for worker_id in workers_to_reap:
            self._registry.unregister_worker(worker_id)

    def _reap_dead_peers(self, now: float) -> None:
        peer_reap_threshold = now - self._config.dead_peer_reap_interval_seconds
        peers_to_reap = [
            peer_id
            for peer_id, unhealthy_since in self._manager_state.iter_manager_peer_unhealthy_since()
            if unhealthy_since < peer_reap_threshold
        ]
        for peer_id in peers_to_reap:
            self._registry.unregister_manager_peer(peer_id)

    def _reap_dead_gates(self, now: float) -> None:
        gate_reap_threshold = now - self._config.dead_gate_reap_interval_seconds
        gates_to_reap = [
            gate_id
            for gate_id, unhealthy_since in self._manager_state.iter_gate_unhealthy_since()
            if unhealthy_since < gate_reap_threshold
        ]
        for gate_id in gates_to_reap:
            self._registry.unregister_gate(gate_id)

    def _cleanup_stale_dead_manager_tracking(self, now: float) -> None:
        dead_manager_cleanup_threshold = now - (
            self._config.dead_peer_reap_interval_seconds * 2
        )
        dead_managers_to_cleanup = [
            tcp_addr
            for tcp_addr, dead_since in self._manager_state.iter_dead_manager_timestamps()
            if dead_since < dead_manager_cleanup_threshold
        ]
        for tcp_addr in dead_managers_to_cleanup:
            self._manager_state.remove_dead_manager(tcp_addr)
            self._manager_state.clear_dead_manager_timestamp(tcp_addr)
            self._manager_state.remove_peer_lock(tcp_addr)

    async def _dead_node_reap_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._config.dead_node_check_interval_seconds)

                now = time.monotonic()
                self._reap_dead_workers(now)
                self._reap_dead_peers(now)
                self._reap_dead_gates(now)
                self._cleanup_stale_dead_manager_tracking(now)

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Dead node reap error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    def _get_manager_tracked_workflow_ids_for_worker(self, worker_id: str) -> set[str]:
        """Get workflow tokens that the manager thinks are running on a specific worker."""
        tracked_ids: set[str] = set()

        for job in self._job_manager.iter_jobs():
            for sub_workflow_token, sub_workflow in job.sub_workflows.items():
                if sub_workflow.worker_id != worker_id:
                    continue

                parent_workflow = job.workflows.get(
                    sub_workflow.parent_token.workflow_token or ""
                )
                if parent_workflow and parent_workflow.status == WorkflowStatus.RUNNING:
                    tracked_ids.add(sub_workflow_token)

        return tracked_ids

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

        query_response = WorkflowQueryResponse.load(response)
        return {workflow.workflow_id for workflow in query_response.workflows}

    async def _handle_orphaned_workflows(
        self,
        orphaned_tokens: set[str],
        worker_id: str,
    ) -> None:
        for orphaned_token in orphaned_tokens:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Orphaned sub-workflow {orphaned_token[:8]}... detected on worker {worker_id[:8]}..., scheduling retry",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            if self._workflow_dispatcher:
                await self._workflow_dispatcher.requeue_workflow(orphaned_token)

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
        while self._running:
            try:
                await asyncio.sleep(self._config.orphan_scan_interval_seconds)

                should_scan = self.is_leader() or self._should_backup_orphan_scan()
                if not should_scan:
                    continue

                for worker_id, worker in self._manager_state.iter_workers():
                    try:
                        worker_addr = (worker.node.host, worker.node.tcp_port)
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

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Orphan scan error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _job_responsiveness_loop(self) -> None:
        """Check job responsiveness (AD-30)."""
        while self._running:
            try:
                await asyncio.sleep(
                    self._config.job_responsiveness_check_interval_seconds
                )

                # Check for expired job suspicions
                expired = self._health_monitor.check_job_suspicion_expiry()

                for job_id, worker_id in expired:
                    self._on_worker_dead_for_job(job_id, worker_id)

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Job responsiveness check error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _stats_push_loop(self) -> None:
        """Periodically push stats to gates/clients."""
        while self._running:
            try:
                await asyncio.sleep(self._config.batch_push_interval_seconds)

                # Push aggregated stats
                await self._stats.push_batch_stats()

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Stats push error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _windowed_stats_flush_loop(self) -> None:
        flush_interval = self._config.stats_push_interval_ms / 1000.0

        while self._running:
            try:
                await asyncio.sleep(flush_interval)
                if not self._running:
                    break
                await self._flush_windowed_stats()
            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Windowed stats flush error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _flush_windowed_stats(self) -> None:
        windowed_stats = await self._windowed_stats.flush_closed_windows(
            aggregate=False
        )
        if not windowed_stats:
            return

        for stats_push in windowed_stats:
            await self._push_windowed_stats_to_gate(stats_push)

    async def _push_windowed_stats_to_gate(
        self,
        stats_push: WindowedStatsPush,
    ) -> None:
        origin_gate_addr = self._manager_state.get_job_origin_gate(stats_push.job_id)
        if not origin_gate_addr:
            return

        stats_push.datacenter = self._node_id.datacenter

        try:
            await self._send_to_peer(
                origin_gate_addr,
                "windowed_stats_push",
                stats_push.dump(),
                timeout=self._config.tcp_timeout_short_seconds,
            )
        except Exception as error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to send windowed stats to gate: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _gate_heartbeat_loop(self) -> None:
        """
        Periodically send ManagerHeartbeat to gates via TCP.

        This supplements the Serf-style SWIM embedding for reliability.
        Gates use this for datacenter health classification.
        """
        heartbeat_interval = self._config.gate_heartbeat_interval_seconds

        await self._udp_logger.log(
            ServerInfo(
                message="Gate heartbeat loop started",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        while self._running:
            try:
                await asyncio.sleep(heartbeat_interval)

                heartbeat = self._build_manager_heartbeat()

                # Send to all healthy gates (use known gates if available, else seed gates)
                gate_addrs = self._get_healthy_gate_tcp_addrs() or self._seed_gates

                sent_count = 0
                for gate_addr in gate_addrs:
                    try:
                        response = await self.send_tcp(
                            gate_addr,
                            "manager_status_update",
                            heartbeat.dump(),
                            timeout=2.0,
                        )
                        if not isinstance(response, Exception):
                            sent_count += 1
                    except Exception as heartbeat_error:
                        await self._udp_logger.log(
                            ServerWarning(
                                message=f"Failed to send heartbeat to gate: {heartbeat_error}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

                if sent_count > 0:
                    await self._udp_logger.log(
                        ServerDebug(
                            message=f"Sent heartbeat to {sent_count}/{len(gate_addrs)} gates",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Gate heartbeat error: {error}",
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

        while self._running:
            try:
                await asyncio.sleep(cleanup_interval)

                cleaned = self._cleanup_inactive_rate_limit_clients()

                if cleaned > 0:
                    await self._udp_logger.log(
                        ServerDebug(
                            message=f"Rate limiter: cleaned up {cleaned} inactive clients",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Rate limit cleanup error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _job_cleanup_loop(self) -> None:
        """
        Periodically clean up completed/failed jobs and their associated state.

        Runs at JOB_CLEANUP_INTERVAL (default 60s).
        Jobs are eligible for cleanup when:
        - Status is COMPLETED or FAILED
        - More than JOB_RETENTION_SECONDS have elapsed since completion
        """
        cleanup_interval = self._config.job_cleanup_interval_seconds
        retention_seconds = self._config.job_retention_seconds

        while self._running:
            try:
                await asyncio.sleep(cleanup_interval)

                current_time = time.monotonic()
                jobs_cleaned = 0

                for job in list(self._job_manager.iter_jobs()):
                    is_terminal = job.status in (
                        JobStatus.COMPLETED.value,
                        JobStatus.FAILED.value,
                        JobStatus.CANCELLED.value,
                    )
                    if not is_terminal or job.completed_at <= 0:
                        continue
                    time_since_completion = current_time - job.completed_at
                    if time_since_completion > retention_seconds:
                        self._cleanup_job(job.job_id)
                        jobs_cleaned += 1

                if jobs_cleaned > 0:
                    await self._udp_logger.log(
                        ServerInfo(
                            message=f"Cleaned up {jobs_cleaned} completed jobs",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Job cleanup error: {error}",
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

        while self._running:
            try:
                await asyncio.sleep(check_interval)

                # Only leader checks timeouts
                if not self.is_leader():
                    continue

                for job_id, strategy in list(
                    self._manager_state.iter_job_timeout_strategies()
                ):
                    try:
                        timed_out, reason = await strategy.check_timeout(job_id)
                        if timed_out:
                            await self._udp_logger.log(
                                ServerWarning(
                                    message=f"Job {job_id[:8]}... timed out: {reason}",
                                    node_host=self._host,
                                    node_port=self._tcp_port,
                                    node_id=self._node_id.short,
                                )
                            )
                            job = self._job_manager.get_job(job_id)
                            if job and job.status not in (
                                JobStatus.COMPLETED.value,
                                JobStatus.FAILED.value,
                                JobStatus.CANCELLED.value,
                            ):
                                job.status = JobStatus.FAILED.value
                                job.completed_at = time.monotonic()
                                await self._manager_state.increment_state_version()
                    except Exception as check_error:
                        await self._udp_logger.log(
                            ServerError(
                                message=f"Timeout check error for job {job_id[:8]}...: {check_error}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Unified timeout loop error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _deadline_enforcement_loop(self) -> None:
        """
        Background loop for worker deadline enforcement (AD-26 Issue 2).

        Checks worker deadlines every 5 seconds and takes action:
        - If deadline expired but within grace period: mark worker as SUSPECTED
        - If deadline expired beyond grace period: evict worker
        """
        check_interval = 5.0

        while self._running:
            try:
                await asyncio.sleep(check_interval)

                current_time = time.monotonic()
                grace_period = self._worker_health_manager.base_deadline

                deadlines_snapshot = self._manager_state.iter_worker_deadlines()

                for worker_id, deadline in deadlines_snapshot:
                    if current_time <= deadline:
                        continue

                    time_since_deadline = current_time - deadline

                    if time_since_deadline <= grace_period:
                        await self._suspect_worker_deadline_expired(worker_id)
                    else:
                        await self._evict_worker_deadline_expired(worker_id)

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Deadline enforcement error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    def _build_job_state_sync_message(
        self, job_id: str, job: JobInfo
    ) -> JobStateSyncMessage:
        elapsed_seconds = time.monotonic() - job.started_at if job.started_at else 0.0
        origin_gate_addr = job.submission.origin_gate_addr if job.submission else None
        return JobStateSyncMessage(
            leader_id=self._node_id.full,
            job_id=job_id,
            status=job.status,
            fencing_token=self._leases.get_fence_token(job_id),
            workflows_total=job.workflows_total,
            workflows_completed=job.workflows_completed,
            workflows_failed=job.workflows_failed,
            workflow_statuses={
                wf_id: wf.status.value for wf_id, wf in job.workflows.items()
            },
            elapsed_seconds=elapsed_seconds,
            timestamp=time.monotonic(),
            origin_gate_addr=origin_gate_addr,
            context_snapshot=job.context.dict(),
            layer_version=job.layer_version,
        )

    async def _sync_job_state_to_peers(self, job_id: str, job: JobInfo) -> None:
        sync_msg = self._build_job_state_sync_message(job_id, job)

        for peer_addr in self._manager_state.get_active_manager_peers():
            try:
                await self._send_to_peer(
                    peer_addr,
                    "job_state_sync",
                    sync_msg.dump(),
                    timeout=2.0,
                )
            except Exception as sync_error:
                await self._udp_logger.log(
                    ServerDebug(
                        message=f"Peer job state sync to {peer_addr} failed: {sync_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _peer_job_state_sync_loop(self) -> None:
        """
        Background loop for periodic job state sync to peer managers.

        Syncs job state (leadership, fencing tokens, context versions)
        to ensure consistency across manager cluster.
        """
        sync_interval = self._config.peer_job_sync_interval_seconds

        while self._running:
            try:
                await asyncio.sleep(sync_interval)

                if not self.is_leader():
                    continue

                led_jobs = self._leases.get_led_job_ids()
                if not led_jobs:
                    continue

                for job_id in led_jobs:
                    if (job := self._job_manager.get_job_by_id(job_id)) is None:
                        continue
                    await self._sync_job_state_to_peers(job_id, job)

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._udp_logger.log(
                    ServerError(
                        message=f"Peer job state sync error: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _resource_sample_loop(self) -> None:
        """
        Background loop for periodic CPU/memory sampling.

        Samples manager's own resource usage and feeds to HybridOverloadDetector
        for overload state classification. Runs at 1s cadence for responsive
        detection while balancing overhead.
        """
        sample_interval = 1.0

        while self._running:
            try:
                await asyncio.sleep(sample_interval)

                metrics = await self._resource_monitor.sample()
                self._last_resource_metrics = metrics

                new_state = self._overload_detector.get_state(
                    metrics.cpu_percent,
                    metrics.memory_percent,
                )
                new_state_str = new_state.value

                if new_state_str != self._manager_health_state:
                    self._previous_manager_health_state = self._manager_health_state
                    self._manager_health_state = new_state_str
                    self._log_manager_health_transition(
                        self._previous_manager_health_state,
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

    def _log_manager_health_transition(
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
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Manager health degraded: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
        else:
            self._task_runner.run(
                self._udp_logger.log,
                ServerDebug(
                    message=f"Manager health improved: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

    def _log_peer_manager_health_transition(
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
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Peer manager {peer_id[:8]}... health degraded: {previous_state} -> {new_state}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
        else:
            self._task_runner.run(
                self._udp_logger.log,
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

    async def _sync_state_from_workers(self) -> None:
        """Sync state from all workers."""
        for worker_id, worker in self._manager_state.iter_workers():
            try:
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_version=self._manager_state.state_version,
                )

                worker_addr = (worker.node.host, worker.node.port)
                response = await self.send_tcp(
                    worker_addr,
                    "state_sync_request",
                    request.dump(),
                    timeout=self._config.state_sync_timeout_seconds,
                )

                if response and not isinstance(response, Exception):
                    sync_response = StateSyncResponse.load(response)
                    if sync_response.worker_state and sync_response.responder_ready:
                        worker_snapshot = sync_response.worker_state
                        if self._manager_state.has_worker(worker_id):
                            worker_reg = self._manager_state.get_worker(worker_id)
                            if worker_reg:
                                worker_reg.available_cores = (
                                    worker_snapshot.available_cores
                                )

            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"State sync from worker {worker_id[:8]}... failed: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _sync_state_from_manager_peers(self) -> None:
        """Sync state from peer managers."""
        for peer_addr in self._manager_state.get_active_manager_peers():
            try:
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_version=self._manager_state.state_version,
                )

                response = await self.send_tcp(
                    peer_addr,
                    "manager_state_sync_request",
                    request.dump(),
                    timeout=self._config.state_sync_timeout_seconds,
                )

                if response and not isinstance(response, Exception):
                    sync_response = StateSyncResponse.load(response)
                    if sync_response.manager_state and sync_response.responder_ready:
                        peer_snapshot = sync_response.manager_state
                        self._manager_state.update_job_leaders(
                            peer_snapshot.job_leaders
                        )
                        self._manager_state.update_job_leader_addrs(
                            peer_snapshot.job_leader_addrs
                        )

            except Exception as error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"State sync from peer {peer_addr} failed: {error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _scan_for_orphaned_jobs(self) -> None:
        """Scan for orphaned jobs from dead managers."""
        dead_managers_snapshot = self._manager_state.get_dead_managers()
        job_leader_addrs_snapshot = self._manager_state.iter_job_leader_addrs()

        for dead_addr in dead_managers_snapshot:
            jobs_to_takeover = [
                job_id
                for job_id, leader_addr in job_leader_addrs_snapshot
                if leader_addr == dead_addr
            ]

            for job_id in jobs_to_takeover:
                self._leases.claim_job_leadership(
                    job_id,
                    (self._host, self._tcp_port),
                    force_takeover=True,
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

    def _get_swim_status_for_worker(self, worker_id: str) -> str:
        """Get SWIM status for a worker."""
        if self._manager_state.has_worker_unhealthy_since(worker_id):
            return "unhealthy"
        return "healthy"

    def _get_active_workflow_count(self) -> int:
        """Get count of active workflows."""
        return sum(
            len(
                [
                    w
                    for w in job.workflows.values()
                    if w.status == WorkflowStatus.RUNNING
                ]
            )
            for job in self._job_manager.iter_jobs()
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
        worker_ids = {
            sub_wf.token.worker_id
            for sub_wf in job.sub_workflows.values()
            if sub_wf.token.worker_id
        }
        return len(worker_ids)

    def _has_quorum_available(self) -> bool:
        """Check if quorum is available."""
        active_count = self._manager_state.get_active_peer_count()
        return active_count >= self._quorum_size

    def _get_dispatch_throughput(self) -> float:
        """Get current dispatch throughput."""
        current_time = time.monotonic()
        elapsed = current_time - self._manager_state.dispatch_throughput_interval_start

        if elapsed >= self._config.throughput_interval_seconds and elapsed > 0:
            throughput = self._manager_state.dispatch_throughput_count / elapsed
            self._manager_state.reset_dispatch_throughput(current_time, throughput)
            return throughput

        if elapsed > 0:
            return self._manager_state.dispatch_throughput_count / elapsed
        return self._manager_state.dispatch_throughput_last_value

    def _get_expected_dispatch_throughput(self) -> float:
        """Get expected dispatch throughput."""
        worker_count = len(self._registry.get_healthy_worker_ids())
        if worker_count == 0:
            return 0.0
        # Assume 1 workflow per second per worker as baseline
        return float(worker_count)

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
    ) -> tuple[bool, float]:
        """
        Check if a client request is within rate limits for a specific operation.

        Args:
            client_id: Identifier for the client (typically addr as string)
            operation: Type of operation being performed

        Returns:
            Tuple of (allowed, retry_after_seconds). If not allowed,
            retry_after_seconds indicates when client can retry.
        """
        result = await self._rate_limiter.check_rate_limit(client_id, operation)
        return result.allowed, result.retry_after_seconds

    def _cleanup_inactive_rate_limit_clients(self) -> int:
        """
        Clean up inactive clients from rate limiter.

        Returns:
            Number of clients cleaned up
        """
        return self._rate_limiter.cleanup_inactive_clients()

    def _build_cancel_response(
        self,
        job_id: str,
        success: bool,
        error: str | None = None,
        cancelled_count: int = 0,
        already_cancelled: bool = False,
        already_completed: bool = False,
    ) -> bytes:
        """Build cancel response in AD-20 format."""
        return JobCancelResponse(
            job_id=job_id,
            success=success,
            error=error,
            cancelled_workflow_count=cancelled_count,
            already_cancelled=already_cancelled,
            already_completed=already_completed,
        ).dump()

    def _build_manager_heartbeat(self) -> ManagerHeartbeat:
        health_state_counts = self._health_monitor.get_worker_health_state_counts()
        return ManagerHeartbeat(
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
            state=self._manager_state.manager_state_enum.value,
            worker_count=self._manager_state.get_worker_count(),
            healthy_worker_count=len(self._registry.get_healthy_worker_ids()),
            available_cores=self._get_available_cores_for_healthy_workers(),
            total_cores=self._get_total_cores(),
            active_job_count=self._job_manager.job_count,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            udp_host=self._host,
            udp_port=self._udp_port,
            overloaded_worker_count=health_state_counts.get("overloaded", 0),
            stressed_worker_count=health_state_counts.get("stressed", 0),
            busy_worker_count=health_state_counts.get("busy", 0),
            health_overload_state=self._manager_health_state,
        )

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

    async def _push_cancellation_complete_to_origin(
        self,
        job_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        """Push cancellation complete notification to origin gate/client."""
        callback_addr = self._manager_state.get_job_callback(job_id)
        if not callback_addr:
            callback_addr = self._manager_state.get_client_callback(job_id)

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
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to push cancellation complete to {callback_addr}: {error}",
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
            job_worker_ids = {
                sub_wf.worker_id
                for sub_wf in job.sub_workflows.values()
                if sub_wf.worker_id
            }
            if worker_id in job_worker_ids:
                strategy = self._manager_state.get_job_timeout_strategy(job.job_id)
                if strategy and hasattr(strategy, "record_extension"):
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

        await self._handle_worker_failure(worker_id)
        self._manager_state.clear_worker_deadline(worker_id)

        if self._worker_disseminator:
            await self._worker_disseminator.broadcast_worker_dead(worker_id, "evicted")

    def _cleanup_job(self, job_id: str) -> None:
        """
        Clean up all state associated with a job.

        Removes job from tracking dictionaries, cleans up workflow state,
        and notifies relevant systems.
        """
        self._task_runner.run(self._job_manager.complete_job, job_id)
        self._manager_state.clear_job_state(job_id)

        if self._workflow_dispatcher:
            self._task_runner.run(
                self._workflow_dispatcher.cleanup_job,
                job_id,
            )

        self._manager_state.remove_workflow_retries_for_job(job_id)
        self._manager_state.remove_workflow_completion_events_for_job(job_id)

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
        return await self.send_tcp(
            addr,
            method,
            data,
            timeout=timeout or self._config.tcp_timeout_standard_seconds,
        )

    async def _send_to_peer(
        self,
        addr: tuple[str, int],
        method: str,
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | Exception | None:
        """Send TCP message to peer manager."""
        return await self.send_tcp(
            addr,
            method,
            data,
            timeout=timeout or self._config.tcp_timeout_standard_seconds,
        )

    async def _send_to_client(
        self,
        addr: tuple[str, int],
        method: str,
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | Exception | None:
        """Send TCP message to client."""
        return await self.send_tcp(
            addr,
            method,
            data,
            timeout=timeout or self._config.tcp_timeout_standard_seconds,
        )

    async def _send_workflow_dispatch(
        self,
        worker_addr: tuple[str, int],
        dispatch: WorkflowDispatch,
    ) -> WorkflowDispatchAck | None:
        """Send workflow dispatch to worker."""
        try:
            response = await self.send_tcp(
                worker_addr,
                "workflow_dispatch",
                dispatch.dump(),
                timeout=self._config.tcp_timeout_standard_seconds,
            )

            if response and not isinstance(response, Exception):
                return WorkflowDispatchAck.load(response)

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow dispatch error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        return None

    # =========================================================================
    # TCP Handlers
    # =========================================================================

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

            # Register worker
            self._registry.register_worker(registration)

            # Add to worker pool
            self._worker_pool.register_worker(
                worker_id=registration.node.node_id,
                total_cores=registration.total_cores,
                available_cores=registration.available_cores,
                tcp_addr=(registration.node.host, registration.node.port),
            )

            # Add to SWIM
            worker_udp_addr = (registration.node.host, registration.node.udp_port)
            self._manager_state.set_worker_addr_mapping(
                worker_udp_addr, registration.node.node_id
            )
            self._probe_scheduler.add_member(worker_udp_addr)

            if self._worker_disseminator:
                await self._worker_disseminator.broadcast_worker_registered(
                    registration
                )

            # Build response with known managers
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

            response = RegistrationResponse(
                accepted=True,
                manager_id=self._node_id.full,
                healthy_managers=healthy_managers,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            )

            return response.dump()

        except Exception as error:
            return RegistrationResponse(
                accepted=False,
                manager_id=self._node_id.full,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def manager_peer_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle manager peer registration."""
        try:
            registration = ManagerPeerRegistration.load(data)

            self._registry.register_manager_peer(registration.node)

            # Add to SWIM
            peer_udp_addr = (
                registration.node.udp_host,
                registration.node.udp_port,
            )
            self._manager_state.set_manager_udp_to_tcp_mapping(
                peer_udp_addr, (registration.node.tcp_host, registration.node.tcp_port)
            )
            self._probe_scheduler.add_member(peer_udp_addr)

            response = ManagerPeerRegistrationResponse(
                accepted=True,
                manager_id=self._node_id.full,
                is_leader=self.is_leader(),
                term=self._leader_election.state.current_term,
                known_peers=self._manager_state.get_known_manager_peer_values(),
            )

            return response.dump()

        except Exception as error:
            return ManagerPeerRegistrationResponse(
                accepted=False,
                error=str(error),
            ).dump()

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

            # Record job progress for AD-30 responsiveness tracking
            worker_id = self._manager_state.get_worker_id_from_addr(addr)
            if worker_id:
                self._health_monitor.record_job_progress(progress.job_id, worker_id)

            # Update job manager
            self._job_manager.update_workflow_progress(
                job_id=progress.job_id,
                workflow_id=progress.workflow_id,
                completed_count=progress.completed_count,
                failed_count=progress.failed_count,
            )

            stats_worker_id = worker_id or f"{addr[0]}:{addr[1]}"
            await self._stats.record_progress_update(stats_worker_id, progress)

            # Get backpressure signal
            backpressure = self._stats.get_backpressure_signal()

            ack = WorkflowProgressAck(
                workflow_id=progress.workflow_id,
                received=True,
                backpressure_level=backpressure.level.value,
                backpressure_delay_ms=backpressure.delay_ms,
            )

            return ack.dump()

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
                workflow_id="",
                received=False,
                error=str(error),
            ).dump()

    def _record_workflow_latency_from_results(self, results: list[dict]) -> None:
        for stats in results:
            if not (stats and isinstance(stats, dict) and "elapsed" in stats):
                continue
            elapsed_seconds = stats.get("elapsed", 0)
            if isinstance(elapsed_seconds, (int, float)) and elapsed_seconds > 0:
                self._manager_state.record_workflow_latency(elapsed_seconds * 1000.0)

    async def _handle_parent_workflow_completion(
        self,
        result: WorkflowFinalResult,
        result_recorded: bool,
        parent_complete: bool,
    ) -> None:
        if not (result_recorded and parent_complete):
            return

        sub_token = TrackingToken.parse(result.workflow_id)
        parent_workflow_token = sub_token.workflow_token
        if not parent_workflow_token:
            return

        if result.status == WorkflowStatus.COMPLETED.value:
            await self._job_manager.mark_workflow_completed(parent_workflow_token)
        elif result.error:
            await self._job_manager.mark_workflow_failed(
                parent_workflow_token, result.error
            )

    def _is_job_complete(self, job_id: str) -> bool:
        job = self._job_manager.get_job(job_id)
        if not job:
            return False
        return job.workflows_completed + job.workflows_failed >= job.workflows_total

    @tcp.receive()
    async def workflow_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        try:
            result = WorkflowFinalResult.load(data)

            self._record_workflow_latency_from_results(result.results)

            if result.context_updates:
                await self._job_manager.apply_workflow_context(
                    job_id=result.job_id,
                    workflow_name=result.workflow_name,
                    context_updates_bytes=result.context_updates,
                )

            (
                result_recorded,
                parent_complete,
            ) = await self._job_manager.record_sub_workflow_result(
                sub_workflow_token=result.workflow_id,
                result=result,
            )

            await self._handle_parent_workflow_completion(
                result, result_recorded, parent_complete
            )

            if self._is_job_complete(result.job_id):
                await self._handle_job_completion(result.job_id)

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow result error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    def _parse_cancel_request(
        self,
        data: bytes,
        addr: tuple[str, int],
    ) -> tuple[str, int, str, float, str]:
        """Parse cancel request from either JobCancelRequest or legacy CancelJob format."""
        try:
            cancel_request = JobCancelRequest.load(data)
            return (
                cancel_request.job_id,
                cancel_request.fence_token,
                cancel_request.requester_id,
                cancel_request.timestamp,
                cancel_request.reason,
            )
        except Exception:
            # Normalize legacy CancelJob format to AD-20 fields
            cancel = CancelJob.load(data)
            return (
                cancel.job_id,
                cancel.fence_token,
                f"{addr[0]}:{addr[1]}",
                time.monotonic(),
                "Legacy cancel request",
            )

    async def _cancel_pending_workflows(
        self,
        job_id: str,
        timestamp: float,
        reason: str,
    ) -> list[str]:
        """Cancel and remove all pending workflows from the dispatch queue."""
        if not self._workflow_dispatcher:
            return []

        removed_pending = await self._workflow_dispatcher.cancel_pending_workflows(
            job_id
        )

        for workflow_id in removed_pending:
            self._manager_state.set_cancelled_workflow(
                workflow_id,
                CancelledWorkflowInfo(
                    workflow_id=workflow_id,
                    job_id=job_id,
                    cancelled_at=timestamp,
                    reason=reason,
                ),
            )

        return removed_pending

    async def _cancel_running_workflow_on_worker(
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
                    self._manager_state.set_cancelled_workflow(
                        workflow_id,
                        CancelledWorkflowInfo(
                            workflow_id=workflow_id,
                            job_id=job_id,
                            cancelled_at=timestamp,
                            reason=reason,
                        ),
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

    def _get_running_workflows_to_cancel(
        self,
        job: JobInfo,
        pending_cancelled: list[str],
    ) -> list[tuple[str, str, tuple[str, int]]]:
        """Get list of (workflow_id, worker_id, worker_addr) for running workflows to cancel."""
        workflows_to_cancel: list[tuple[str, str, tuple[str, int]]] = []

        for workflow_id, workflow_info in job.workflows.items():
            if workflow_id in pending_cancelled:
                continue
            if workflow_info.status != WorkflowStatus.RUNNING:
                continue

            for sub_workflow_token in workflow_info.sub_workflow_tokens:
                sub_workflow = job.sub_workflows.get(sub_workflow_token)
                if not (sub_workflow and sub_workflow.token.worker_id):
                    continue

                worker = self._manager_state.get_worker(sub_workflow.token.worker_id)
                if worker:
                    worker_addr = (worker.node.host, worker.node.port)
                    workflows_to_cancel.append(
                        (workflow_id, sub_workflow.token.worker_id, worker_addr)
                    )

        return workflows_to_cancel

    async def _cancel_running_workflows(
        self,
        job: JobInfo,
        pending_cancelled: list[str],
        requester_id: str,
        timestamp: float,
        reason: str,
    ) -> tuple[list[str], dict[str, str]]:
        """Cancel all running workflows on workers. Returns (cancelled_list, errors_dict)."""
        running_cancelled: list[str] = []
        workflow_errors: dict[str, str] = {}

        workflows_to_cancel = self._get_running_workflows_to_cancel(
            job, pending_cancelled
        )

        for workflow_id, worker_id, worker_addr in workflows_to_cancel:
            success, error_msg = await self._cancel_running_workflow_on_worker(
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

    @tcp.receive()
    async def job_cancel(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle job cancellation request (AD-20).

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

            job_id, fence_token, requester_id, timestamp, reason = (
                self._parse_cancel_request(data, addr)
            )

            job = self._job_manager.get_job(job_id)
            if not job:
                return self._build_cancel_response(
                    job_id, success=False, error="Job not found"
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

            pending_cancelled = await self._cancel_pending_workflows(
                job_id, timestamp, reason
            )

            running_cancelled, workflow_errors = await self._cancel_running_workflows(
                job, pending_cancelled, requester_id, timestamp, reason
            )

            strategy = self._manager_state.get_job_timeout_strategy(job_id)
            if strategy:
                await strategy.stop_tracking(job_id, "cancelled")

            job.status = JobStatus.CANCELLED.value
            job.completed_at = time.monotonic()
            await self._manager_state.increment_state_version()

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

            return self._build_cancel_response(
                job_id,
                success=overall_success,
                cancelled_count=total_cancelled,
                error=error_str,
            )

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job cancel error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return JobCancelResponse(
                job_id="",
                success=False,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def workflow_cancellation_complete(
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

            await self._udp_logger.log(
                ServerInfo(
                    message=f"Received workflow cancellation complete for {workflow_id[:8]}... "
                    f"(job {job_id[:8]}..., success={completion.success})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Track this workflow as complete
            pending = self._manager_state.get_cancellation_pending_workflows(job_id)
            if workflow_id in pending:
                self._manager_state.remove_cancellation_pending_workflow(
                    job_id, workflow_id
                )

                # Collect any errors
                if not completion.success and completion.errors:
                    for error in completion.errors:
                        self._manager_state.add_cancellation_error(
                            job_id, f"Workflow {workflow_id[:8]}...: {error}"
                        )

                # Check if all workflows for this job have reported
                remaining_pending = (
                    self._manager_state.get_cancellation_pending_workflows(job_id)
                )
                if not remaining_pending:
                    # All workflows cancelled - fire completion event and push to origin
                    event = self._manager_state.get_cancellation_completion_event(
                        job_id
                    )
                    if event:
                        event.set()

                    errors = self._manager_state.get_cancellation_errors(job_id)
                    success = len(errors) == 0

                    # Push completion notification to origin gate/client
                    self._task_runner.run(
                        self._push_cancellation_complete_to_origin,
                        job_id,
                        success,
                        errors,
                    )

                    # Cleanup tracking structures
                    self._manager_state.clear_cancellation_pending_workflows(job_id)
                    self._manager_state.clear_cancellation_completion_events(job_id)
                    self._manager_state.clear_cancellation_initiated_at(job_id)

            # Also delegate to cancellation coordinator for additional handling
            await self._cancellation.handle_workflow_cancelled(completion)

            return b"OK"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Cancellation complete error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"ERROR"

    @tcp.receive()
    async def state_sync_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle state sync request from peer managers or workers."""
        try:
            request = StateSyncRequest.load(data)

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"State sync request from {request.requester_id[:8]}... role={request.requester_role} since_version={request.since_version}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )

            current_version = self._manager_state.state_version
            is_ready = (
                self._manager_state.manager_state_enum != ManagerStateEnum.INITIALIZING
            )

            if request.since_version >= current_version:
                return StateSyncResponse(
                    responder_id=self._node_id.full,
                    current_version=current_version,
                    responder_ready=is_ready,
                ).dump()

            snapshot = ManagerStateSnapshot(
                node_id=self._node_id.full,
                datacenter=self._config.datacenter_id,
                is_leader=self._leadership_coordinator.is_leader(),
                term=self._leadership_coordinator._get_term(),
                version=current_version,
                workers=self._build_worker_snapshots(),
                jobs=dict(self._manager_state._job_progress),
                job_leaders=dict(self._manager_state._job_leaders),
                job_leader_addrs=dict(self._manager_state._job_leader_addrs),
                job_fence_tokens=dict(self._manager_state._job_fencing_tokens),
                job_layer_versions=dict(self._manager_state._job_layer_version),
                job_contexts=self._serialize_job_contexts(),
            )

            return StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=current_version,
                responder_ready=is_ready,
                manager_state=snapshot,
            ).dump()

        except Exception as error:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"State sync request failed: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
            return StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=0,
                responder_ready=False,
            ).dump()

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
            request = HealthcheckExtensionRequest.load(data)

            # Rate limit check (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = await self._check_rate_limit_for_operation(
                client_id, "extension"
            )
            if not allowed:
                return HealthcheckExtensionResponse(
                    granted=False,
                    extension_seconds=0.0,
                    new_deadline=0.0,
                    remaining_extensions=0,
                    denial_reason=f"Rate limited, retry after {retry_after:.1f}s",
                ).dump()

            # Check if worker is registered
            worker_id = request.worker_id
            if not worker_id:
                worker_id = self._manager_state.get_worker_id_from_addr(addr)

            if not worker_id:
                return HealthcheckExtensionResponse(
                    granted=False,
                    extension_seconds=0.0,
                    new_deadline=0.0,
                    remaining_extensions=0,
                    denial_reason="Worker not registered",
                ).dump()

            worker = self._manager_state.get_worker(worker_id)
            if not worker:
                return HealthcheckExtensionResponse(
                    granted=False,
                    extension_seconds=0.0,
                    new_deadline=0.0,
                    remaining_extensions=0,
                    denial_reason="Worker not found",
                ).dump()

            # Get current deadline (or set default)
            current_deadline = self._manager_state.get_worker_deadline(worker_id)
            if current_deadline is None:
                current_deadline = time.monotonic() + 30.0

            # Handle extension request via worker health manager
            response = self._worker_health_manager.handle_extension_request(
                request=request,
                current_deadline=current_deadline,
            )

            # Update stored deadline if granted
            if response.granted:
                self._manager_state.set_worker_deadline(
                    worker_id, response.new_deadline
                )

                # AD-26 Issue 3: Integrate with SWIM timing wheels (SWIM as authority)
                hierarchical_detector = self.get_hierarchical_detector()
                if hierarchical_detector:
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

                # Notify timeout strategies of extension (AD-34 Part 10.4.7)
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
            else:
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

            return response.dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Extension request error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return HealthcheckExtensionResponse(
                granted=False,
                extension_seconds=0.0,
                new_deadline=0.0,
                remaining_extensions=0,
                denial_reason=str(error),
            ).dump()

    @tcp.receive()
    async def ping(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle ping request."""
        try:
            request = PingRequest.load(data)

            # Build worker status list
            worker_statuses = [
                WorkerStatus(
                    worker_id=worker_id,
                    state=self._health_monitor.get_worker_health_status(worker_id),
                    available_cores=worker.available_cores,
                    total_cores=worker.total_cores,
                )
                for worker_id, worker in self._manager_state.iter_workers()
            ]

            response = ManagerPingResponse(
                manager_id=self._node_id.full,
                is_leader=self.is_leader(),
                state=self._manager_state.manager_state_enum.value,
                state_version=self._manager_state.state_version,
                worker_count=self._manager_state.get_worker_count(),
                healthy_worker_count=self._health_monitor.get_healthy_worker_count(),
                active_job_count=self._job_manager.job_count,
                workers=worker_statuses,
            )

            return response.dump()

        except Exception as error:
            return ManagerPingResponse(
                manager_id=self._node_id.full,
                is_leader=False,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def gate_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle gate registration via TCP."""
        try:
            registration = GateRegistrationRequest.load(data)

            # Cluster isolation validation (AD-28)
            if registration.cluster_id != self._env.CLUSTER_ID:
                return GateRegistrationResponse(
                    accepted=False,
                    manager_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    healthy_managers=[],
                    error=f"Cluster isolation violation: gate cluster_id '{registration.cluster_id}' does not match",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            if registration.environment_id != self._env.ENVIRONMENT_ID:
                return GateRegistrationResponse(
                    accepted=False,
                    manager_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    healthy_managers=[],
                    error="Environment isolation violation: gate environment_id mismatch",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            # Protocol version validation (AD-25)
            gate_version = ProtocolVersion(
                registration.protocol_version_major,
                registration.protocol_version_minor,
            )
            gate_caps_set = (
                set(registration.capabilities.split(","))
                if registration.capabilities
                else set()
            )
            gate_caps = NodeCapabilities(
                protocol_version=gate_version,
                capabilities=gate_caps_set,
            )
            local_caps = NodeCapabilities.current()
            negotiated = negotiate_capabilities(local_caps, gate_caps)

            if not negotiated.compatible:
                return GateRegistrationResponse(
                    accepted=False,
                    manager_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    healthy_managers=[],
                    error=f"Incompatible protocol version: {gate_version}",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            # Store gate info
            gate_info = GateInfo(
                node_id=registration.node_id,
                tcp_host=registration.tcp_host,
                tcp_port=registration.tcp_port,
                udp_host=registration.udp_host,
                udp_port=registration.udp_port,
            )

            self._registry.register_gate(gate_info)

            # Track gate addresses
            gate_tcp_addr = (registration.tcp_host, registration.tcp_port)
            gate_udp_addr = (registration.udp_host, registration.udp_port)
            self._manager_state.set_gate_udp_to_tcp_mapping(
                gate_udp_addr, gate_tcp_addr
            )

            # Add to SWIM probing
            self.add_unconfirmed_peer(gate_udp_addr)
            self._probe_scheduler.add_member(gate_udp_addr)

            # Store negotiated capabilities
            self._manager_state.set_gate_negotiated_caps(
                registration.node_id, negotiated
            )

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

        except Exception as error:
            return GateRegistrationResponse(
                accepted=False,
                manager_id=self._node_id.full,
                datacenter=self._node_id.datacenter,
                healthy_managers=[],
                error=str(error),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()

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

            # Process heartbeat via WorkerPool
            await self._worker_pool.process_heartbeat(heartbeat.node_id, heartbeat)

            # Trigger dispatch for active jobs
            if self._workflow_dispatcher:
                for job_id, submission in self._manager_state.iter_job_submissions():
                    await self._workflow_dispatcher.try_dispatch(job_id, submission)

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

            if self._worker_disseminator is None:
                return b"not_ready"

            accepted = await self._worker_disseminator.handle_worker_state_update(
                update, addr
            )

            return b"accepted" if accepted else b"rejected"

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
            if batch is None:
                return b"invalid"

            if batch.originating_manager_id == self._node_id.full:
                return b"self"

            await self._udp_logger.log(
                ServerDebug(
                    message=f"Received {len(batch.reassignments)} workflow reassignments from {batch.originating_manager_id[:8]}... (worker {batch.failed_worker_id[:8]}... {batch.reason})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

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

    @tcp.receive()
    async def context_forward(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle context forwarded from non-leader manager."""
        try:
            forward = ContextForward.load(data)

            # Verify we are the job leader
            if not self._is_job_leader(forward.job_id):
                return b"not_leader"

            # Apply context updates
            await self._apply_context_updates(
                forward.job_id,
                forward.workflow_id,
                forward.context_updates,
                forward.context_timestamps,
            )

            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Context forward error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def context_layer_sync(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle context layer sync from job leader."""
        try:
            sync = ContextLayerSync.load(data)

            # Check if this is a newer layer version
            current_version = self._manager_state.get_job_layer_version(
                sync.job_id, default=-1
            )
            if sync.layer_version <= current_version:
                return ContextLayerSyncAck(
                    job_id=sync.job_id,
                    layer_version=sync.layer_version,
                    applied=False,
                    responder_id=self._node_id.full,
                ).dump()

            # Apply context snapshot
            context_dict = cloudpickle.loads(sync.context_snapshot)

            context = self._manager_state.get_or_create_job_context(sync.job_id)
            for workflow_name, values in context_dict.items():
                await context.from_dict(workflow_name, values)

            # Update layer version
            self._manager_state.set_job_layer_version(sync.job_id, sync.layer_version)

            # Update job leader if not set
            if not self._manager_state.has_job_leader(sync.job_id):
                self._manager_state.set_job_leader(sync.job_id, sync.source_node_id)

            return ContextLayerSyncAck(
                job_id=sync.job_id,
                layer_version=sync.layer_version,
                applied=True,
                responder_id=self._node_id.full,
            ).dump()

        except Exception as context_sync_error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Context layer sync failed: {context_sync_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return ContextLayerSyncAck(
                job_id="unknown",
                layer_version=-1,
                applied=False,
                responder_id=self._node_id.full,
            ).dump()

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

        try:
            # Rate limit check (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            rate_limit_result = await self._rate_limiter.check_rate_limit(
                client_id, "job_submit"
            )
            if not rate_limit_result.allowed:
                return RateLimitResponse(
                    operation="job_submit",
                    retry_after_seconds=rate_limit_result.retry_after_seconds,
                ).dump()

            if self._load_shedder.should_shed("JobSubmission"):
                # get_current_state() returns the same state should_shed() just computed
                # (both use same default args and HybridOverloadDetector tracks _current_state)
                overload_state = self._load_shedder.get_current_state()
                return JobAck(
                    job_id="",
                    accepted=False,
                    error=f"System under load ({overload_state}), please retry later",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            submission = JobSubmission.load(data)

            # Protocol version negotiation (AD-25)
            client_version = ProtocolVersion(
                major=getattr(submission, "protocol_version_major", 1),
                minor=getattr(submission, "protocol_version_minor", 0),
            )

            if client_version.major != CURRENT_PROTOCOL_VERSION.major:
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Incompatible protocol version: {client_version}",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            # Negotiate capabilities
            client_caps_str = getattr(submission, "capabilities", "")
            client_features = (
                set(client_caps_str.split(",")) if client_caps_str else set()
            )
            our_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
            negotiated_features = client_features & our_features
            negotiated_caps_str = ",".join(sorted(negotiated_features))

            if submission.idempotency_key and self._idempotency_ledger is not None:
                try:
                    idempotency_key = IdempotencyKey.parse(submission.idempotency_key)
                except ValueError as error:
                    return JobAck(
                        job_id=submission.job_id,
                        accepted=False,
                        error=str(error),
                    ).dump()

                existing_entry = self._idempotency_ledger.get_by_key(idempotency_key)
                if existing_entry is not None:
                    if existing_entry.result_serialized is not None:
                        return existing_entry.result_serialized
                    if existing_entry.status in (
                        IdempotencyStatus.COMMITTED,
                        IdempotencyStatus.REJECTED,
                    ):
                        return JobAck(
                            job_id=submission.job_id,
                            accepted=(
                                existing_entry.status == IdempotencyStatus.COMMITTED
                            ),
                            error="Duplicate request"
                            if existing_entry.status == IdempotencyStatus.REJECTED
                            else None,
                        ).dump()
                    return JobAck(
                        job_id=submission.job_id,
                        accepted=False,
                        error="Request pending, please retry",
                    ).dump()

            # Only active managers accept jobs
            if self._manager_state.manager_state_enum != ManagerStateEnum.ACTIVE:
                return JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Manager is {self._manager_state.manager_state_enum.value}, not accepting jobs",
                ).dump()

            if idempotency_key is not None and self._idempotency_ledger is not None:
                found, entry = await self._idempotency_ledger.check_or_reserve(
                    idempotency_key,
                    submission.job_id,
                )
                if found and entry is not None:
                    if entry.result_serialized is not None:
                        return entry.result_serialized
                    if entry.status in (
                        IdempotencyStatus.COMMITTED,
                        IdempotencyStatus.REJECTED,
                    ):
                        return JobAck(
                            job_id=submission.job_id,
                            accepted=entry.status == IdempotencyStatus.COMMITTED,
                            error="Duplicate request"
                            if entry.status == IdempotencyStatus.REJECTED
                            else None,
                        ).dump()
                    return JobAck(
                        job_id=submission.job_id,
                        accepted=False,
                        error="Request pending, please retry",
                    ).dump()
                idempotency_reserved = True

            # Unpickle workflows
            workflows: list[tuple[str, list[str], Workflow]] = restricted_loads(
                submission.workflows
            )

            # Create job using JobManager
            callback_addr = None
            if submission.callback_addr:
                callback_addr = (
                    tuple(submission.callback_addr)
                    if isinstance(submission.callback_addr, list)
                    else submission.callback_addr
                )

            job_info = await self._job_manager.create_job(
                submission=submission,
                callback_addr=callback_addr,
            )

            job_info.leader_node_id = self._node_id.full
            job_info.leader_addr = (self._host, self._tcp_port)
            job_info.fencing_token = 1

            # Store submission for dispatch
            self._manager_state.set_job_submission(submission.job_id, submission)

            # Start timeout tracking (AD-34)
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

            self._leases.claim_job_leadership(
                job_id=submission.job_id,
                tcp_addr=(self._host, self._tcp_port),
            )
            self._leases.initialize_job_context(submission.job_id)

            # Store callbacks
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

            await self._manager_state.increment_state_version()

            # Broadcast job leadership to peers
            workflow_names = [wf.name for _, _, wf in workflows]
            await self._broadcast_job_leadership(
                submission.job_id,
                len(workflows),
                workflow_names,
            )

            # Dispatch workflows
            await self._dispatch_job_workflows(submission, workflows)

            ack_response = JobAck(
                job_id=submission.job_id,
                accepted=True,
                queued_position=self._job_manager.job_count,
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            ).dump()

            if (
                idempotency_reserved
                and idempotency_key is not None
                and self._idempotency_ledger is not None
            ):
                await self._idempotency_ledger.commit(idempotency_key, ack_response)

            return ack_response

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Job submission error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            job_id = submission.job_id if submission is not None else "unknown"
            error_ack = JobAck(
                job_id=job_id,
                accepted=False,
                error=str(error),
            ).dump()
            if (
                idempotency_reserved
                and idempotency_key is not None
                and self._idempotency_ledger is not None
            ):
                await self._idempotency_ledger.reject(idempotency_key, error_ack)
            return error_ack

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

            strategy = self._manager_state.get_job_timeout_strategy(timeout_msg.job_id)
            if not strategy:
                return b""

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

            return b""

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

    @tcp.receive()
    async def provision_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle provision request from leader for quorum."""
        try:
            request = ProvisionRequest.load(data)

            # Check if we can confirm
            worker = self._worker_pool.get_worker(request.target_worker)
            can_confirm = (
                worker is not None
                and self._worker_pool.is_worker_healthy(request.target_worker)
                and (worker.available_cores - worker.reserved_cores)
                >= request.cores_required
            )

            return ProvisionConfirm(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                confirming_node=self._node_id.full,
                confirmed=can_confirm,
                version=self._manager_state.state_version,
                error=None if can_confirm else "Worker not available",
            ).dump()

        except Exception as error:
            return ProvisionConfirm(
                job_id="unknown",
                workflow_id="unknown",
                confirming_node=self._node_id.full,
                confirmed=False,
                version=self._manager_state.state_version,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def provision_commit(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle provision commit from leader."""
        try:
            ProvisionCommit.load(data)  # Validate message format
            await self._manager_state.increment_state_version()
            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Provision commit error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def workflow_cancellation_query(
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

    @tcp.receive()
    async def receive_cancel_single_workflow(
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
            existing = self._manager_state.get_cancelled_workflow(request.workflow_id)
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
            self._manager_state.set_cancelled_workflow(
                request.workflow_id,
                CancelledWorkflowInfo(
                    job_id=request.job_id,
                    workflow_id=request.workflow_id,
                    cancelled_at=time.monotonic(),
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

    @tcp.receive()
    async def receive_workflow_cancellation_peer_notification(
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
                if not self._manager_state.has_cancelled_workflow(wf_id):
                    self._manager_state.set_cancelled_workflow(
                        wf_id,
                        CancelledWorkflowInfo(
                            job_id=notification.job_id,
                            workflow_id=wf_id,
                            cancelled_at=notification.timestamp or time.monotonic(),
                            request_id=notification.request_id,
                            dependents=[],
                        ),
                    )

            return b"OK"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Workflow cancellation peer notification error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"ERROR"

    @tcp.receive()
    async def job_leadership_announcement(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job leadership announcement from another manager."""
        try:
            announcement = JobLeadershipAnnouncement.load(data)

            # Don't accept if we're already the leader
            if self._is_job_leader(announcement.job_id):
                return JobLeadershipAck(
                    job_id=announcement.job_id,
                    accepted=False,
                    responder_id=self._node_id.full,
                ).dump()

            # Record job leadership
            self._manager_state.set_job_leader(
                announcement.job_id, announcement.leader_id
            )
            self._manager_state.set_job_leader_addr(
                announcement.job_id,
                (announcement.leader_host, announcement.leader_tcp_port),
            )

            # Initialize context for this job
            self._manager_state.get_or_create_job_context(announcement.job_id)

            self._manager_state.setdefault_job_layer_version(announcement.job_id, 0)

            # Track remote job
            await self._job_manager.track_remote_job(
                job_id=announcement.job_id,
                leader_node_id=announcement.leader_id,
                leader_addr=(announcement.leader_host, announcement.leader_tcp_port),
            )

            return JobLeadershipAck(
                job_id=announcement.job_id,
                accepted=True,
                responder_id=self._node_id.full,
            ).dump()

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
            current_leader = self._manager_state.get_job_leader(sync_msg.job_id)
            if current_leader and current_leader != sync_msg.leader_id:
                return JobStateSyncAck(
                    job_id=sync_msg.job_id,
                    responder_id=self._node_id.full,
                    accepted=False,
                ).dump()

            if job := self._job_manager.get_job(sync_msg.job_id):
                job.status = sync_msg.status
                job.workflows_total = sync_msg.workflows_total
                job.workflows_completed = sync_msg.workflows_completed
                job.workflows_failed = sync_msg.workflows_failed
                job.timestamp = time.monotonic()

                if (
                    sync_msg.context_snapshot
                    and sync_msg.layer_version > job.layer_version
                ):
                    async with job.lock:
                        for workflow_name, values in sync_msg.context_snapshot.items():
                            await job.context.from_dict(workflow_name, values)
                        job.layer_version = sync_msg.layer_version

            self._leases.update_fence_token_if_higher(
                sync_msg.job_id, sync_msg.fencing_token
            )

            if sync_msg.origin_gate_addr:
                self._manager_state.set_job_origin_gate(
                    sync_msg.job_id, sync_msg.origin_gate_addr
                )

            return JobStateSyncAck(
                job_id=sync_msg.job_id,
                responder_id=self._node_id.full,
                accepted=True,
            ).dump()

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

    @tcp.receive()
    async def job_leader_gate_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job leader gate transfer notification from gate."""
        try:
            transfer = JobLeaderGateTransfer.load(data)

            current_fence = self._leases.get_fence_token(transfer.job_id)
            if transfer.fence_token < current_fence:
                return JobLeaderGateTransferAck(
                    job_id=transfer.job_id,
                    manager_id=self._node_id.full,
                    accepted=False,
                ).dump()

            self._manager_state.set_job_origin_gate(
                transfer.job_id, transfer.new_gate_addr
            )

            self._leases.update_fence_token_if_higher(
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

            # Calculate elapsed time
            elapsed = time.monotonic() - job.timestamp if job.timestamp > 0 else 0.0

            # Aggregate completed/failed from sub-workflows (WorkflowInfo has no counts;
            # they live on SubWorkflowInfo.progress)
            total_completed = 0
            total_failed = 0
            for workflow_info in job.workflows.values():
                for sub_workflow_token in workflow_info.sub_workflow_tokens:
                    sub_workflow_info = job.sub_workflows.get(sub_workflow_token)
                    if sub_workflow_info and (progress := sub_workflow_info.progress):
                        total_completed += progress.completed_count
                        total_failed += progress.failed_count

            return RegisterCallbackResponse(
                job_id=job_id,
                success=True,
                status=job.status,
                total_completed=total_completed,
                total_failed=total_failed,
                elapsed_seconds=elapsed,
            ).dump()

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

    @tcp.receive()
    async def workflow_query(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow status query from client."""
        try:
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
            if job is None:
                return WorkflowQueryResponse(
                    request_id=request.request_id,
                    manager_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    workflows=workflows,
                ).dump()

            # Find matching workflows
            for wf_info in job.workflows.values():
                if wf_info.name in request.workflow_names:
                    workflow_id = wf_info.token.workflow_id or ""
                    status = wf_info.status.value
                    is_enqueued = wf_info.status == WorkflowStatus.PENDING

                    # Aggregate from sub-workflows
                    assigned_workers: list[str] = []
                    provisioned_cores = 0
                    completed_count = 0
                    failed_count = 0
                    rate_per_second = 0.0

                    for sub_token_str in wf_info.sub_workflow_tokens:
                        sub_info = job.sub_workflows.get(sub_token_str)
                        if not sub_info:
                            continue
                        if sub_info.worker_id:
                            assigned_workers.append(sub_info.worker_id)
                        provisioned_cores += sub_info.cores_allocated
                        if progress := sub_info.progress:
                            completed_count += progress.completed_count
                            failed_count += progress.failed_count
                            rate_per_second += progress.rate_per_second

                    workflows.append(
                        WorkflowStatusInfo(
                            workflow_id=workflow_id,
                            workflow_name=wf_info.name,
                            status=status,
                            is_enqueued=is_enqueued,
                            queue_position=0,
                            provisioned_cores=provisioned_cores,
                            completed_count=completed_count,
                            failed_count=failed_count,
                            rate_per_second=rate_per_second,
                            assigned_workers=assigned_workers,
                        )
                    )

            return WorkflowQueryResponse(
                request_id=request.request_id,
                manager_id=self._node_id.full,
                datacenter=self._node_id.datacenter,
                workflows=workflows,
            ).dump()

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

    # =========================================================================
    # Helper Methods - Job Submission
    # =========================================================================

    async def _broadcast_job_leadership(
        self,
        job_id: str,
        workflow_count: int,
        workflow_names: list[str],
    ) -> None:
        """Broadcast job leadership to peer managers."""
        announcement = JobLeadershipAnnouncement(
            job_id=job_id,
            leader_id=self._node_id.full,
            leader_host=self._host,
            leader_tcp_port=self._tcp_port,
            workflow_count=workflow_count,
            workflow_names=workflow_names,
        )

        for peer_addr in self._manager_state.get_active_manager_peers():
            try:
                await self.send_tcp(
                    peer_addr,
                    "job_leadership_announcement",
                    announcement.dump(),
                    timeout=2.0,
                )
            except Exception as announcement_error:
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to send leadership announcement to peer {peer_addr}: {announcement_error}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _dispatch_job_workflows(
        self,
        submission: JobSubmission,
        workflows: list[tuple[str, list[str], Workflow]],
    ) -> None:
        """Dispatch workflows respecting dependencies."""
        if self._workflow_dispatcher:
            registered = await self._workflow_dispatcher.register_workflows(
                submission,
                workflows,
            )
            if registered:
                await self._workflow_dispatcher.start_job_dispatch(
                    submission.job_id, submission
                )
                await self._workflow_dispatcher.try_dispatch(
                    submission.job_id, submission
                )

        job = self._job_manager.get_job(submission.job_id)
        if job:
            job.status = JobStatus.RUNNING.value
            await self._manager_state.increment_state_version()

    async def _register_with_discovered_worker(
        self,
        worker_snapshot: WorkerStateSnapshot,
    ) -> None:
        """Register a discovered worker from peer manager gossip."""
        worker_id = worker_snapshot.node_id
        if self._manager_state.has_worker(worker_id):
            return

        node_info = NodeInfo(
            node_id=worker_id,
            host=worker_snapshot.host,
            tcp_port=worker_snapshot.tcp_port,
            udp_port=worker_snapshot.udp_port,
            role=NodeRole.WORKER,
        )

        registration = WorkerRegistration(
            node=node_info,
            total_cores=worker_snapshot.total_cores,
            available_cores=worker_snapshot.available_cores,
            memory_mb=0,
        )

        self._registry.register_worker(registration)

        self._worker_pool.register_worker(
            worker_id=worker_id,
            total_cores=worker_snapshot.total_cores,
            available_cores=worker_snapshot.available_cores,
            tcp_addr=(worker_snapshot.host, worker_snapshot.tcp_port),
            is_remote=True,
        )

    def _is_job_leader(self, job_id: str) -> bool:
        """Check if this manager is the leader for a job."""
        leader_id = self._manager_state.get_job_leader(job_id)
        return leader_id == self._node_id.full

    async def _apply_context_updates(
        self,
        job_id: str,
        workflow_id: str,
        updates_bytes: bytes,
        timestamps_bytes: bytes,
    ) -> None:
        """Apply context updates from workflow completion."""
        context = self._manager_state.get_or_create_job_context(job_id)

        updates = cloudpickle.loads(updates_bytes)
        timestamps = cloudpickle.loads(timestamps_bytes) if timestamps_bytes else {}

        for key, value in updates.items():
            timestamp = timestamps.get(
                key, await self._manager_state.increment_context_lamport_clock()
            )
            await context.update(
                workflow_id,
                key,
                value,
                timestamp=timestamp,
                source_node=self._node_id.full,
            )

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

    async def _handle_job_completion(self, job_id: str) -> None:
        """Handle job completion with notification and cleanup."""
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return await self._send_job_completion_to_gate(
                job_id, JobStatus.COMPLETED.value, [], [], 0, 0, 0.0
            )

        async with job.lock:
            job.status = JobStatus.COMPLETED.value
            job.completed_at = time.monotonic()
            elapsed_seconds = job.elapsed_seconds()
            final_status = self._determine_final_job_status(job)
            workflow_results, errors, total_completed, total_failed = (
                self._aggregate_workflow_results(job)
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
                WorkflowResult(
                    workflow_id=workflow_info.token.workflow_id or workflow_token,
                    workflow_name=workflow_info.name,
                    status=workflow_info.status.value,
                    results=stats,
                    error=workflow_info.error,
                )
            )
            if workflow_info.error:
                errors.append(f"{workflow_info.name}: {workflow_info.error}")

        return workflow_results, errors, total_completed, total_failed

    def _aggregate_sub_workflow_stats(
        self, job: JobInfo, workflow_info: WorkflowInfo
    ) -> tuple[list[WorkflowStats], int, int]:
        stats: list[WorkflowStats] = []
        completed = 0
        failed = 0

        for sub_wf_token in workflow_info.sub_workflow_tokens:
            sub_wf = job.sub_workflows.get(sub_wf_token)
            if not sub_wf:
                continue
            if sub_wf.result:
                stats.extend(sub_wf.result.results)
            if progress := sub_wf.progress:
                completed += progress.completed_count
                failed += progress.failed_count

        return stats, completed, failed

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
        )

        try:
            await self._send_to_peer(
                origin_gate_addr, "job_final_result", final_result.dump(), timeout=5.0
            )
        except Exception as send_error:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to send job completion to gate: {send_error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _cleanup_job_state(self, job_id: str) -> None:
        self._leases.clear_job_leases(job_id)
        self._health_monitor.cleanup_job_progress(job_id)
        self._health_monitor.clear_job_suspicions(job_id)
        self._manager_state.clear_job_state(job_id)
        job_token = self._job_manager.create_job_token(job_id)
        await self._job_manager.remove_job(job_token)

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


__all__ = ["ManagerServer"]
