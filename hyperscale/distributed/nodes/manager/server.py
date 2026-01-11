"""
Manager server composition root.

Thin orchestration layer that wires all manager modules together.
All business logic is delegated to specialized coordinators.
"""

import asyncio
import random
import time
from typing import TYPE_CHECKING

from hyperscale.distributed.swim import HealthAwareServer, ManagerStateEmbedder
from hyperscale.distributed.swim.core import ErrorStats, CircuitState
from hyperscale.distributed.swim.detection import HierarchicalConfig
from hyperscale.distributed.swim.health import FederatedHealthMonitor
from hyperscale.distributed.env import Env
from hyperscale.distributed.server import tcp
from hyperscale.distributed.server.events import VersionedStateClock
from hyperscale.distributed.models import (
    NodeInfo,
    NodeRole,
    ManagerInfo,
    ManagerState as ManagerStateEnum,
    ManagerHeartbeat,
    ManagerStateSnapshot,
    GateInfo,
    GateHeartbeat,
    WorkerRegistration,
    WorkerHeartbeat,
    WorkerState,
    RegistrationResponse,
    ManagerPeerRegistration,
    ManagerPeerRegistrationResponse,
    JobSubmission,
    JobAck,
    JobStatus,
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkflowProgress,
    WorkflowFinalResult,
    WorkflowStatus,
    StateSyncRequest,
    StateSyncResponse,
    JobCancelRequest,
    JobCancelResponse,
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    WorkflowCancellationComplete,
    HealthcheckExtensionRequest,
    HealthcheckExtensionResponse,
    ManagerToWorkerRegistration,
    ManagerToWorkerRegistrationAck,
    PingRequest,
    ManagerPingResponse,
    WorkerStatus,
)
from hyperscale.distributed.reliability import (
    HybridOverloadDetector,
    LoadShedder,
    ServerRateLimiter,
    StatsBuffer,
    StatsBufferConfig,
)
from hyperscale.distributed.health import WorkerHealthManager, WorkerHealthManagerConfig
from hyperscale.distributed.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    NodeCapabilities,
    NegotiatedCapabilities,
    ProtocolVersion,
    negotiate_capabilities,
)
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.discovery.security.role_validator import RoleValidator
from hyperscale.distributed.jobs import (
    JobManager,
    WorkerPool,
    WorkflowDispatcher,
    WindowedStatsCollector,
)
from hyperscale.distributed.workflow import WorkflowStateMachine as WorkflowLifecycleStateMachine
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerWarning,
    ServerError,
    ServerDebug,
)

from .config import ManagerConfig, create_manager_config_from_env
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
from .in_flight import InFlightTracker, BoundedRequestExecutor
from .workflow_lifecycle import ManagerWorkflowLifecycle

if TYPE_CHECKING:
    from hyperscale.logging import Logger


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
        # Build configuration from environment
        self._config = create_manager_config_from_env(
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
        )

        self._env = env
        self._seed_gates = gate_addrs or []
        self._gate_udp_addrs = gate_udp_addrs or []
        self._seed_managers = seed_managers or manager_peers or []
        self._manager_udp_peers = manager_udp_peers or []
        self._max_workflow_retries = max_workflow_retries
        self._workflow_timeout = workflow_timeout

        # Initialize centralized runtime state
        self._manager_state = ManagerState()

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
            get_term_fn=lambda: self._leader_election.state.current_term if hasattr(self, '_leader_election') else 0,
        )

        # Stats coordinator
        self._stats = ManagerStatsCoordinator(
            state=self._manager_state,
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
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
        self._load_shedder = ManagerLoadShedder(
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # In-flight tracking (AD-32)
        self._in_flight = InFlightTracker(
            config=self._config,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )
        self._bounded_executor = BoundedRequestExecutor(
            in_flight=self._in_flight,
            load_shedder=self._load_shedder,
            logger=self._udp_logger,
            node_id=self._node_id.short,
            task_runner=self._task_runner,
        )

        # JobManager for race-safe job/workflow state
        self._job_manager = JobManager(
            datacenter=self._node_id.datacenter,
            manager_id=self._node_id.short,
        )

        # WorkerPool for worker registration and resource tracking
        self._worker_pool = WorkerPool(
            health_grace_period=30.0,
            get_swim_status=self._get_swim_status_for_worker,
            manager_id=self._node_id.short,
            datacenter=self._node_id.datacenter,
        )

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

        # Federated health monitor for gate probing
        fed_config = self._env.get_federated_health_config()
        self._gate_health_monitor = FederatedHealthMonitor(
            probe_interval=fed_config['probe_interval'],
            probe_timeout=fed_config['probe_timeout'],
            suspicion_timeout=fed_config['suspicion_timeout'],
            max_consecutive_failures=fed_config['max_consecutive_failures'],
        )

        # Gate circuit breaker
        cb_config = self._env.get_circuit_breaker_config()
        self._gate_circuit = ErrorStats(
            max_errors=cb_config['max_errors'],
            window_seconds=cb_config['window_seconds'],
            half_open_after=cb_config['half_open_after'],
        )

        # Quorum circuit breaker
        self._quorum_circuit = ErrorStats(
            window_seconds=30.0,
            max_errors=3,
            half_open_after=10.0,
        )

        # Recovery semaphore
        self._recovery_semaphore = asyncio.Semaphore(self._config.recovery_max_concurrent)

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

    def _init_address_mappings(self) -> None:
        """Initialize UDP to TCP address mappings."""
        # Gate UDP to TCP mapping
        for idx, tcp_addr in enumerate(self._seed_gates):
            if idx < len(self._gate_udp_addrs):
                self._manager_state._gate_udp_to_tcp[self._gate_udp_addrs[idx]] = tcp_addr

        # Manager UDP to TCP mapping
        for idx, tcp_addr in enumerate(self._seed_managers):
            if idx < len(self._manager_udp_peers):
                self._manager_state._manager_udp_to_tcp[self._manager_udp_peers[idx]] = tcp_addr

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
            get_state_version=lambda: self._manager_state._state_version,
            get_active_jobs=lambda: self._job_manager.job_count,
            get_active_workflows=self._get_active_workflow_count,
            get_worker_count=lambda: len(self._manager_state._workers),
            get_healthy_worker_count=lambda: len(self._registry.get_healthy_worker_ids()),
            get_available_cores=self._get_available_cores_for_healthy_workers,
            get_total_cores=self._get_total_cores,
            on_worker_heartbeat=self._handle_embedded_worker_heartbeat,
            on_manager_heartbeat=self._handle_manager_peer_heartbeat,
            on_gate_heartbeat=self._handle_gate_heartbeat,
            get_manager_state=lambda: self._manager_state._manager_state.value,
            get_tcp_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_udp_host=lambda: self._host,
            get_udp_port=lambda: self._udp_port,
            get_health_accepting_jobs=lambda: self._manager_state._manager_state == ManagerStateEnum.ACTIVE,
            get_health_has_quorum=self._has_quorum_available,
            get_health_throughput=self._get_dispatch_throughput,
            get_health_expected_throughput=self._get_expected_dispatch_throughput,
            get_health_overload_state=lambda: self._overload_detector.get_state(0.0, 0.0),
            get_current_gate_leader_id=lambda: self._manager_state._current_gate_leader_id,
            get_current_gate_leader_host=lambda: (
                self._manager_state._current_gate_leader_addr[0]
                if self._manager_state._current_gate_leader_addr else None
            ),
            get_current_gate_leader_port=lambda: (
                self._manager_state._current_gate_leader_addr[1]
                if self._manager_state._current_gate_leader_addr else None
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
            version=self._manager_state._state_version,
            udp_port=self._udp_port,
        )

    @property
    def _quorum_size(self) -> int:
        """Calculate required quorum size."""
        total_managers = len(self._manager_state._active_manager_peers) + 1
        return (total_managers // 2) + 1

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    async def start(self, timeout: float | None = None) -> None:
        """Start the manager server."""
        # Initialize locks (requires async context)
        self._manager_state.initialize_locks()

        # Start the underlying server
        await self.start_server(init_context=self._env.get_swim_init_context())

        # Update node capabilities with proper version
        self._node_capabilities = NodeCapabilities.current(
            node_version=f"manager-{self._node_id.short}"
        )

        # Initialize workflow lifecycle state machine (AD-33)
        self._workflow_lifecycle_states = WorkflowLifecycleStateMachine()

        # Initialize workflow dispatcher
        self._workflow_dispatcher = WorkflowDispatcher(
            job_manager=self._job_manager,
            worker_pool=self._worker_pool,
            manager_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            dispatch_semaphore=asyncio.Semaphore(100),
            send_dispatch=self._send_workflow_dispatch,
            get_fence_token=lambda job_id: self._leases.increment_fence_token(job_id),
        )

        # Mark as started
        self._started = True
        self._manager_state._manager_state = ManagerStateEnum.ACTIVE

        # Register with seed managers
        await self._register_with_peer_managers()

        # Join SWIM clusters
        await self._join_swim_clusters()

        # Start SWIM probe cycle
        self._task_runner.run(self.start_probe_cycle)

        # Start background tasks
        self._start_background_tasks()

        manager_count = len(self._manager_state._known_manager_peers) + 1
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
        if not self._running and not hasattr(self, '_started'):
            return

        self._running = False
        self._manager_state._manager_state = ManagerStateEnum.DRAINING

        # Cancel background tasks
        await self._cancel_background_tasks()

        # Graceful shutdown
        await super().stop(
            drain_timeout=drain_timeout,
            broadcast_leave=broadcast_leave,
        )

    def abort(self) -> None:
        """Abort the manager server immediately."""
        self._running = False
        self._manager_state._manager_state = ManagerStateEnum.OFFLINE

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
        ]

    def _start_background_tasks(self) -> None:
        """Start all background tasks."""
        self._dead_node_reap_task = asyncio.create_task(self._dead_node_reap_loop())
        self._orphan_scan_task = asyncio.create_task(self._orphan_scan_loop())
        self._discovery_maintenance_task = asyncio.create_task(
            self._discovery.maintenance_loop()
        )
        self._job_responsiveness_task = asyncio.create_task(
            self._job_responsiveness_loop()
        )
        self._stats_push_task = asyncio.create_task(self._stats_push_loop())

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
                    self._registry.register_manager_peer(parsed.manager_info)
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
        tcp_addr = self._manager_state._manager_udp_to_tcp.get(peer)
        if tcp_addr:
            for peer_id, peer_info in self._manager_state._known_manager_peers.items():
                if (peer_info.udp_host, peer_info.udp_port) == peer:
                    self._manager_state._active_manager_peer_ids.add(peer_id)
                    self._manager_state._active_manager_peers.add(tcp_addr)
                    break

    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """Handle node death detected by SWIM."""
        # Check if worker
        worker_id = self._manager_state._worker_addr_to_id.get(node_addr)
        if worker_id:
            if worker_id not in self._manager_state._worker_unhealthy_since:
                self._manager_state._worker_unhealthy_since[worker_id] = time.monotonic()
            self._task_runner.run(self._handle_worker_failure, worker_id)
            return

        # Check if manager peer
        manager_tcp_addr = self._manager_state._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            self._manager_state._dead_managers.add(manager_tcp_addr)
            self._task_runner.run(
                self._handle_manager_peer_failure, node_addr, manager_tcp_addr
            )
            return

        # Check if gate
        gate_tcp_addr = self._manager_state._gate_udp_to_tcp.get(node_addr)
        if gate_tcp_addr:
            self._task_runner.run(
                self._handle_gate_peer_failure, node_addr, gate_tcp_addr
            )

    def _on_node_join(self, node_addr: tuple[str, int]) -> None:
        """Handle node join detected by SWIM."""
        # Check if worker
        worker_id = self._manager_state._worker_addr_to_id.get(node_addr)
        if worker_id:
            self._manager_state._worker_unhealthy_since.pop(worker_id, None)
            return

        # Check if manager peer
        manager_tcp_addr = self._manager_state._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            self._manager_state._dead_managers.discard(manager_tcp_addr)
            self._task_runner.run(
                self._handle_manager_peer_recovery, node_addr, manager_tcp_addr
            )
            return

        # Check if gate
        gate_tcp_addr = self._manager_state._gate_udp_to_tcp.get(node_addr)
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

    def _on_worker_dead_for_job(self, job_id: str, worker_id: str) -> None:
        """Handle worker death for specific job (AD-30)."""
        # This would trigger workflow reschedule
        pass

    # =========================================================================
    # Failure/Recovery Handlers
    # =========================================================================

    async def _handle_worker_failure(self, worker_id: str) -> None:
        """Handle worker failure."""
        self._health_monitor.handle_worker_failure(worker_id)

        # Trigger workflow retry for workflows on this worker
        # Implementation delegated to workflow lifecycle coordinator

    async def _handle_manager_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle manager peer failure."""
        peer_lock = self._manager_state.get_peer_state_lock(tcp_addr)
        async with peer_lock:
            self._manager_state._peer_state_epoch[tcp_addr] = (
                self._manager_state._peer_state_epoch.get(tcp_addr, 0) + 1
            )
            self._manager_state._active_manager_peers.discard(tcp_addr)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager peer {tcp_addr} marked DEAD",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Handle job leader failure
        await self._handle_job_leader_failure(tcp_addr)

    async def _handle_manager_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle manager peer recovery."""
        peer_lock = self._manager_state.get_peer_state_lock(tcp_addr)

        async with peer_lock:
            initial_epoch = self._manager_state._peer_state_epoch.get(tcp_addr, 0)

        async with self._recovery_semaphore:
            jitter = random.uniform(
                self._config.recovery_jitter_min_seconds,
                self._config.recovery_jitter_max_seconds,
            )
            await asyncio.sleep(jitter)

            async with peer_lock:
                current_epoch = self._manager_state._peer_state_epoch.get(tcp_addr, 0)
                if current_epoch != initial_epoch:
                    return

                self._manager_state._active_manager_peers.add(tcp_addr)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Manager peer {tcp_addr} REJOINED",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

    async def _handle_gate_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer failure."""
        # Find gate by address
        gate_node_id = None
        for gate_id, gate_info in self._manager_state._known_gates.items():
            if (gate_info.tcp_host, gate_info.tcp_port) == tcp_addr:
                gate_node_id = gate_id
                break

        if gate_node_id:
            self._registry.mark_gate_unhealthy(gate_node_id)

            if self._manager_state._primary_gate_id == gate_node_id:
                self._manager_state._primary_gate_id = None
                for healthy_id in self._manager_state._healthy_gate_ids:
                    self._manager_state._primary_gate_id = healthy_id
                    break

    async def _handle_gate_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """Handle gate peer recovery."""
        for gate_id, gate_info in self._manager_state._known_gates.items():
            if (gate_info.tcp_host, gate_info.tcp_port) == tcp_addr:
                self._registry.mark_gate_healthy(gate_id)
                break

    async def _handle_job_leader_failure(self, failed_addr: tuple[str, int]) -> None:
        """Handle job leader manager failure."""
        if not self.is_leader():
            return

        # Find jobs led by the failed manager and take them over
        jobs_to_takeover = []
        for job_id, leader_addr in self._manager_state._job_leader_addrs.items():
            if leader_addr == failed_addr:
                jobs_to_takeover.append(job_id)

        for job_id in jobs_to_takeover:
            self._leases.claim_job_leadership(job_id, (self._host, self._tcp_port))
            await self._udp_logger.log(
                ServerInfo(
                    message=f"Took over leadership for job {job_id[:8]}...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    # =========================================================================
    # Heartbeat Handlers
    # =========================================================================

    def _handle_embedded_worker_heartbeat(
        self,
        heartbeat: WorkerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """Handle embedded worker heartbeat from SWIM."""
        self._health_monitor.handle_worker_heartbeat(heartbeat, source_addr)

        # Update worker pool if worker is registered
        worker_id = heartbeat.node_id
        if worker_id in self._manager_state._workers:
            self._worker_pool.update_worker_capacity(
                worker_id=worker_id,
                available_cores=heartbeat.available_cores,
                queue_depth=heartbeat.queue_depth,
            )

    def _handle_manager_peer_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """Handle embedded manager heartbeat from SWIM."""
        peer_id = heartbeat.node_id

        # Register peer if not known
        if peer_id not in self._manager_state._known_manager_peers:
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

        # Confirm peer
        self.confirm_peer(source_addr)

    def _handle_gate_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """Handle embedded gate heartbeat from SWIM."""
        gate_id = heartbeat.node_id

        # Register gate if not known
        if gate_id not in self._manager_state._known_gates:
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
            self._manager_state._current_gate_leader_id = gate_id
            gate_info = self._manager_state._known_gates.get(gate_id)
            if gate_info:
                self._manager_state._current_gate_leader_addr = (
                    gate_info.tcp_host,
                    gate_info.tcp_port,
                )

        # Confirm peer
        self.confirm_peer(source_addr)

    # =========================================================================
    # Background Loops
    # =========================================================================

    async def _dead_node_reap_loop(self) -> None:
        """Periodically reap dead nodes."""
        while self._running:
            try:
                await asyncio.sleep(self._config.dead_node_check_interval_seconds)

                now = time.monotonic()

                # Reap dead workers
                worker_reap_threshold = (
                    now - self._config.dead_worker_reap_interval_seconds
                )
                workers_to_reap = [
                    worker_id
                    for worker_id, unhealthy_since in self._manager_state._worker_unhealthy_since.items()
                    if unhealthy_since < worker_reap_threshold
                ]
                for worker_id in workers_to_reap:
                    self._registry.unregister_worker(worker_id)

                # Reap dead peers
                peer_reap_threshold = (
                    now - self._config.dead_peer_reap_interval_seconds
                )
                peers_to_reap = [
                    peer_id
                    for peer_id, unhealthy_since in self._manager_state._manager_peer_unhealthy_since.items()
                    if unhealthy_since < peer_reap_threshold
                ]
                for peer_id in peers_to_reap:
                    self._registry.unregister_manager_peer(peer_id)

                # Reap dead gates
                gate_reap_threshold = (
                    now - self._config.dead_gate_reap_interval_seconds
                )
                gates_to_reap = [
                    gate_id
                    for gate_id, unhealthy_since in self._manager_state._gate_unhealthy_since.items()
                    if unhealthy_since < gate_reap_threshold
                ]
                for gate_id in gates_to_reap:
                    self._registry.unregister_gate(gate_id)

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

    async def _orphan_scan_loop(self) -> None:
        """Periodically scan for orphaned workflows."""
        while self._running:
            try:
                await asyncio.sleep(self._config.orphan_scan_interval_seconds)

                if not self.is_leader():
                    continue

                # Implementation: Scan workers for workflows not tracked by JobManager
                # and trigger cleanup or takeover

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
                    # Trigger workflow reschedule for expired suspicions
                    pass

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
                await asyncio.sleep(
                    self._config.batch_push_interval_seconds
                )

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

    # =========================================================================
    # State Sync
    # =========================================================================

    async def _sync_state_from_workers(self) -> None:
        """Sync state from all workers."""
        for worker_id, worker in self._manager_state._workers.items():
            try:
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_version=self._manager_state._state_version,
                )

                worker_addr = (worker.node.host, worker.node.tcp_port)
                response = await self.send_tcp(
                    worker_addr,
                    "state_sync_request",
                    request.dump(),
                    timeout=self._config.state_sync_timeout_seconds,
                )

                if response and not isinstance(response, Exception):
                    # Process worker state
                    pass

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
        for peer_addr in self._manager_state._active_manager_peers:
            try:
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_version=self._manager_state._state_version,
                )

                response = await self.send_tcp(
                    peer_addr,
                    "manager_state_sync_request",
                    request.dump(),
                    timeout=self._config.state_sync_timeout_seconds,
                )

                if response and not isinstance(response, Exception):
                    # Process peer state
                    pass

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
        for dead_addr in self._manager_state._dead_managers:
            jobs_to_takeover = [
                job_id
                for job_id, leader_addr in self._manager_state._job_leader_addrs.items()
                if leader_addr == dead_addr
            ]

            for job_id in jobs_to_takeover:
                self._leases.claim_job_leadership(
                    job_id, (self._host, self._tcp_port)
                )

    async def _resume_timeout_tracking_for_all_jobs(self) -> None:
        """Resume timeout tracking for all jobs as new leader."""
        for job_id in self._leases.get_led_job_ids():
            # Re-initialize timeout strategy if needed
            pass

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _get_swim_status_for_worker(self, worker_id: str) -> str:
        """Get SWIM status for a worker."""
        if worker_id in self._manager_state._worker_unhealthy_since:
            return "unhealthy"
        return "healthy"

    def _get_active_workflow_count(self) -> int:
        """Get count of active workflows."""
        return sum(
            len([
                w for w in job.workflows.values()
                if w.status == WorkflowStatus.RUNNING
            ])
            for job in self._job_manager.iter_jobs()
        )

    def _get_available_cores_for_healthy_workers(self) -> int:
        """Get total available cores across healthy workers."""
        total = 0
        healthy_ids = self._registry.get_healthy_worker_ids()
        for worker_id in healthy_ids:
            worker = self._manager_state._workers.get(worker_id)
            if worker:
                total += worker.available_cores
        return total

    def _get_total_cores(self) -> int:
        """Get total cores across all workers."""
        return sum(
            w.total_cores for w in self._manager_state._workers.values()
        )

    def _get_job_worker_count(self, job_id: str) -> int:
        """Get number of workers for a job."""
        job = self._job_manager.get_job(job_id)
        if job:
            return len(job.workers)
        return 0

    def _has_quorum_available(self) -> bool:
        """Check if quorum is available."""
        active_count = len(self._manager_state._active_manager_peers) + 1
        return active_count >= self._quorum_size

    def _get_dispatch_throughput(self) -> float:
        """Get current dispatch throughput."""
        current_time = time.monotonic()
        elapsed = current_time - self._manager_state._dispatch_throughput_interval_start

        if elapsed >= self._config.throughput_interval_seconds:
            if elapsed > 0:
                self._manager_state._dispatch_throughput_last_value = (
                    self._manager_state._dispatch_throughput_count / elapsed
                )
            self._manager_state._dispatch_throughput_count = 0
            self._manager_state._dispatch_throughput_interval_start = current_time
            return self._manager_state._dispatch_throughput_last_value

        if elapsed > 0:
            return self._manager_state._dispatch_throughput_count / elapsed
        return self._manager_state._dispatch_throughput_last_value

    def _get_expected_dispatch_throughput(self) -> float:
        """Get expected dispatch throughput."""
        worker_count = len(self._registry.get_healthy_worker_ids())
        if worker_count == 0:
            return 0.0
        # Assume 1 workflow per second per worker as baseline
        return float(worker_count)

    def _get_known_gates_for_heartbeat(self) -> list[GateInfo]:
        """Get known gates for heartbeat embedding."""
        return list(self._manager_state._known_gates.values())

    def _get_job_leaderships_for_heartbeat(self) -> list[str]:
        """Get job leaderships for heartbeat embedding."""
        return self._leases.get_led_job_ids()

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
                tcp_addr=(registration.node.host, registration.node.tcp_port),
            )

            # Add to SWIM
            worker_udp_addr = (registration.node.host, registration.node.udp_port)
            self._manager_state._worker_addr_to_id[worker_udp_addr] = (
                registration.node.node_id
            )
            self._probe_scheduler.add_member(worker_udp_addr)

            # Build response with known managers
            healthy_managers = [
                self._manager_state._known_manager_peers[peer_id]
                for peer_id in self._manager_state._active_manager_peer_ids
                if peer_id in self._manager_state._known_manager_peers
            ]
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

            # Register peer
            self._registry.register_manager_peer(registration.manager_info)

            # Add to SWIM
            peer_udp_addr = (
                registration.manager_info.udp_host,
                registration.manager_info.udp_port,
            )
            self._manager_state._manager_udp_to_tcp[peer_udp_addr] = (
                registration.manager_info.tcp_host,
                registration.manager_info.tcp_port,
            )
            self._probe_scheduler.add_member(peer_udp_addr)

            response = ManagerPeerRegistrationResponse(
                accepted=True,
                manager_info=ManagerInfo(
                    node_id=self._node_id.full,
                    tcp_host=self._host,
                    tcp_port=self._tcp_port,
                    udp_host=self._host,
                    udp_port=self._udp_port,
                    datacenter=self._node_id.datacenter,
                    is_leader=self.is_leader(),
                ),
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
            worker_id = self._manager_state._worker_addr_to_id.get(addr)
            if worker_id:
                self._health_monitor.record_job_progress(progress.job_id, worker_id)

            # Update job manager
            self._job_manager.update_workflow_progress(
                job_id=progress.job_id,
                workflow_id=progress.workflow_id,
                completed_count=progress.completed_count,
                failed_count=progress.failed_count,
            )

            # Record in windowed stats
            self._windowed_stats.record(progress)

            # Get backpressure signal
            backpressure = self._stats_buffer.get_backpressure_signal()

            from hyperscale.distributed.models import WorkflowProgressAck

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
            from hyperscale.distributed.models import WorkflowProgressAck

            return WorkflowProgressAck(
                workflow_id="",
                received=False,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def workflow_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow final result from worker."""
        try:
            result = WorkflowFinalResult.load(data)

            # Update job manager
            self._job_manager.complete_workflow(
                job_id=result.job_id,
                workflow_id=result.workflow_id,
                success=result.status == WorkflowStatus.COMPLETED.value,
                results=result.results,
            )

            # Check if job is complete
            job = self._job_manager.get_job(result.job_id)
            if job and job.is_complete:
                # Handle job completion
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

    @tcp.receive()
    async def job_cancel(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job cancellation request (AD-20)."""
        try:
            request = JobCancelRequest.load(data)
            return await self._cancellation.cancel_job(request, addr)

        except Exception as error:
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
        """Handle workflow cancellation complete notification."""
        try:
            notification = WorkflowCancellationComplete.load(data)
            await self._cancellation.handle_workflow_cancelled(notification)
            return b"ok"

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Cancellation complete error: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return b"error"

    @tcp.receive()
    async def state_sync_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle state sync request."""
        try:
            request = StateSyncRequest.load(data)

            # Build state snapshot
            snapshot = ManagerStateSnapshot(
                node_id=self._node_id.full,
                state_version=self._manager_state._state_version,
                manager_state=self._manager_state._manager_state.value,
                job_count=self._job_manager.job_count,
                worker_count=len(self._manager_state._workers),
            )

            return StateSyncResponse(
                responder_id=self._node_id.full,
                version=self._manager_state._state_version,
                snapshot=snapshot.dump(),
            ).dump()

        except Exception as error:
            return StateSyncResponse(
                responder_id=self._node_id.full,
                version=0,
                error=str(error),
            ).dump()

    @tcp.receive()
    async def extension_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle healthcheck extension request (AD-26)."""
        try:
            request = HealthcheckExtensionRequest.load(data)

            worker_id = self._manager_state._worker_addr_to_id.get(addr)
            if not worker_id:
                return HealthcheckExtensionResponse(
                    granted=False,
                    denial_reason="Unknown worker",
                ).dump()

            granted, extension_seconds, new_deadline, remaining, denial_reason = (
                self._extension_manager.handle_extension_request(
                    worker_id=worker_id,
                    reason=request.reason,
                    current_progress=request.current_progress,
                    estimated_completion=request.estimated_completion,
                )
            )

            return HealthcheckExtensionResponse(
                granted=granted,
                extension_seconds=extension_seconds,
                new_deadline=new_deadline,
                remaining_extensions=remaining,
                denial_reason=denial_reason,
            ).dump()

        except Exception as error:
            return HealthcheckExtensionResponse(
                granted=False,
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
                for worker_id, worker in self._manager_state._workers.items()
            ]

            response = ManagerPingResponse(
                manager_id=self._node_id.full,
                is_leader=self.is_leader(),
                state=self._manager_state._manager_state.value,
                state_version=self._manager_state._state_version,
                worker_count=len(self._manager_state._workers),
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

    # =========================================================================
    # Job Completion
    # =========================================================================

    async def _handle_job_completion(self, job_id: str) -> None:
        """Handle job completion."""
        # Clear job state
        self._leases.clear_job_leases(job_id)
        self._health_monitor.cleanup_job_progress(job_id)
        self._health_monitor.clear_job_suspicions(job_id)
        self._manager_state.clear_job_state(job_id)

        await self._udp_logger.log(
            ServerInfo(
                message=f"Job {job_id[:8]}... completed",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )


__all__ = ["ManagerServer"]
