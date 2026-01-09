"""
Gate Node Server.

Gates coordinate job execution across datacenters. They:
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
"""

import asyncio
import secrets
import statistics
import time
from collections import defaultdict
from typing import Any

import cloudpickle

from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.reporting.results import Results
from hyperscale.reporting.reporter import Reporter
from hyperscale.reporting.common import ReporterTypes
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.distributed_rewrite.server.events import VersionedStateClock
from hyperscale.distributed_rewrite.swim import HealthAwareServer, GateStateEmbedder
from hyperscale.distributed_rewrite.swim.health import (
    FederatedHealthMonitor,
    CrossClusterAck,
    DCLeaderAnnouncement,
)
from hyperscale.distributed_rewrite.models import (
    NodeInfo,
    NodeRole,
    GateInfo,
    GateState,
    GateHeartbeat,
    ManagerRegistrationResponse,
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
    DatacenterLease,
    LeaseTransfer,
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
    restricted_loads,
)
from hyperscale.distributed_rewrite.swim.core import (
    QuorumError,
    QuorumUnavailableError,
    QuorumTimeoutError,
    QuorumCircuitOpenError,
    ErrorStats,
    CircuitState,
)
from hyperscale.distributed_rewrite.health import (
    ManagerHealthState,
    ManagerHealthConfig,
    GateHealthState,
    GateHealthConfig,
    RoutingDecision,
)
from hyperscale.distributed_rewrite.reliability import (
    HybridOverloadDetector,
    LoadShedder,
    ServerRateLimiter,
    RateLimitConfig,
    RetryExecutor,
    RetryConfig,
    JitterStrategy,
)
from hyperscale.distributed_rewrite.jobs.gates import (
    GateJobManager,
    JobForwardingTracker,
    ConsistentHashRing,
)
from hyperscale.distributed_rewrite.jobs import (
    WindowedStatsCollector,
    WindowedStatsPush,
    JobLeadershipTracker,
)
from hyperscale.distributed_rewrite.datacenters import (
    DatacenterHealthManager,
    ManagerDispatcher,
    LeaseManager,
    CrossDCCorrelationDetector,
    CorrelationSeverity,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.protocol.version import (
    ProtocolVersion,
    NodeCapabilities,
    NegotiatedCapabilities,
    negotiate_capabilities,
    CURRENT_PROTOCOL_VERSION,
    get_features_for_version,
)
from hyperscale.distributed_rewrite.discovery import DiscoveryService
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning, ServerError, ServerDebug


class GateServer(HealthAwareServer):
    """
    Gate node in the distributed Hyperscale system.
    
    Gates:
    - Form a gossip cluster for leader election (UDP SWIM)
    - Accept job submissions from clients (TCP)
    - Dispatch jobs to managers in target datacenters (TCP)
    - Probe managers via UDP to detect DC failures (SWIM)
    - Aggregate global job status across DCs (TCP)
    - Manage leases for at-most-once semantics
    
    Healthchecks (UDP - SWIM protocol):
        Gates form a SWIM cluster with other gates for leader election.
        Gates also probe datacenter managers via UDP to detect DC
        availability. DC health is determined by SWIM probes, not TCP.
    
    Status Updates (TCP):
        Managers send status updates via TCP containing job progress.
        These are distinct from healthchecks - a DC might have stale
        status but still be reachable (detected via UDP probes).
    """
    
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "global",  # Gates typically span DCs
        datacenter_managers: dict[str, list[tuple[str, int]]] | None = None,  # TCP
        datacenter_manager_udp: dict[str, list[tuple[str, int]]] | None = None,  # UDP for SWIM
        gate_peers: list[tuple[str, int]] | None = None,  # TCP
        gate_udp_peers: list[tuple[str, int]] | None = None,  # UDP for SWIM cluster
        lease_timeout: float = 30.0,
    ):
        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
        )
        
        # Datacenter -> manager addresses mapping
        self._datacenter_managers = datacenter_managers or {}  # TCP
        self._datacenter_manager_udp = datacenter_manager_udp or {}  # UDP for SWIM

        # Per-DC registration state tracking (AD-27: Explicit Registration with Readiness Gating)
        # Tracks which managers have sent heartbeats and quorum status per DC.
        # Health classification only applies to DCs with READY registration status.
        self._dc_registration_states: dict[str, DatacenterRegistrationState] = {}
        for dc_id, manager_addrs in self._datacenter_managers.items():
            self._dc_registration_states[dc_id] = DatacenterRegistrationState(
                dc_id=dc_id,
                configured_managers=list(manager_addrs),
            )

        # Per-manager circuit breakers for dispatch failures
        # Key is manager TCP address tuple, value is ErrorStats
        self._manager_circuits: dict[tuple[str, int], ErrorStats] = {}
        
        # Gate peers for clustering
        self._gate_peers = gate_peers or []  # TCP
        self._gate_udp_peers = gate_udp_peers or []  # UDP for SWIM cluster

        # DEBUG: Track initialization

        # Track gate peer addresses for failure detection (same pattern as managers)
        # Maps UDP addr -> TCP addr for peer gates
        self._gate_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}
        for i, tcp_addr in enumerate(self._gate_peers):
            if i < len(self._gate_udp_peers):
                self._gate_udp_to_tcp[self._gate_udp_peers[i]] = tcp_addr

        # Track active gate peers (removed when SWIM marks as dead)
        # AD-29: Start empty - peers become active ONLY after we receive their heartbeat
        # This prevents false failure detection during cluster formation
        self._active_gate_peers: set[tuple[str, int]] = set()

        # Per-peer locks protecting _active_gate_peers modifications to prevent race conditions
        # between concurrent failure/recovery handlers for the SAME peer (asyncio task interleaving)
        # Using per-peer locks allows concurrent operations on different peers without serialization
        self._peer_state_locks: dict[tuple[str, int], asyncio.Lock] = {}

        # Monotonic epoch per peer address to detect stale failure/recovery operations
        # Incremented on each state change; handlers check epoch hasn't changed after await
        self._peer_state_epoch: dict[tuple[str, int], int] = {}
        
        # Track gate peer info from GateHeartbeat (proper node_ids, leadership, etc)
        # Maps UDP addr -> GateHeartbeat for peers we've heard from via SWIM
        self._gate_peer_info: dict[tuple[str, int], GateHeartbeat] = {}

        # Known gates discovered via piggybacking or direct announcement
        # Maps gate_id -> GateInfo for cross-gate job forwarding and discovery
        self._known_gates: dict[str, GateInfo] = {}
        
        # Known datacenters and their status (from TCP updates)
        # Stored per-datacenter, per-manager for proper aggregation
        self._datacenter_manager_status: dict[str, dict[tuple[str, int], ManagerHeartbeat]] = {}  # dc -> {manager_addr -> heartbeat}
        self._manager_last_status: dict[tuple[str, int], float] = {}  # manager_addr -> timestamp

        # Three-signal health state for managers (AD-19)
        # Maps (dc, manager_addr) -> ManagerHealthState
        self._manager_health: dict[tuple[str, tuple[str, int]], ManagerHealthState] = {}
        self._manager_health_config = ManagerHealthConfig()

        # Three-signal health state for peer gates (AD-19)
        # Maps gate_id -> GateHealthState
        self._gate_peer_health: dict[str, GateHealthState] = {}
        self._gate_health_config = GateHealthConfig()

        # Latency tracking for peer gates
        # Used to detect network degradation within the gate cluster
        # High latency to all peers indicates network issues vs specific gate failures
        self._peer_gate_latency_samples: dict[str, list[tuple[float, float]]] = {}  # gate_id -> [(timestamp, latency_ms)]
        self._latency_sample_max_age: float = 60.0  # Keep samples for 60 seconds
        self._latency_sample_max_count: int = 30  # Keep at most 30 samples per peer

        # Load shedding infrastructure (AD-22)
        # Tracks latency and sheds low-priority requests under load
        self._overload_detector = HybridOverloadDetector()
        self._load_shedder = LoadShedder(self._overload_detector)

        # Rate limiting infrastructure (AD-24)
        # Per-client rate limiting with automatic cleanup
        self._rate_limiter = ServerRateLimiter(
            inactive_cleanup_seconds=300.0,  # Cleanup after 5 minutes
        )

        # Protocol version negotiation (AD-25)
        # Our capabilities for negotiation with managers
        self._node_capabilities = NodeCapabilities.current(node_version=f"gate-{self._node_id.short}")
        # Negotiated capabilities per manager
        # Maps manager_addr -> NegotiatedCapabilities
        self._manager_negotiated_caps: dict[tuple[str, int], NegotiatedCapabilities] = {}

        # Versioned state clock for rejecting stale updates
        # Tracks per-datacenter versions using Lamport timestamps
        self._versioned_clock = VersionedStateClock()
        
        # Global job state
        self._jobs: dict[str, GlobalJobStatus] = {}  # job_id -> status
        
        # Per-DC final results for job completion aggregation
        # job_id -> {datacenter -> JobFinalResult}
        self._job_dc_results: dict[str, dict[str, JobFinalResult]] = {}

        # Per-workflow results from all DCs for cross-DC aggregation
        # job_id -> workflow_id -> datacenter -> WorkflowResultPush
        self._workflow_dc_results: dict[str, dict[str, dict[str, WorkflowResultPush]]] = {}

        # Track which DCs were assigned for each job (to know when complete)
        # job_id -> set of datacenter IDs
        self._job_target_dcs: dict[str, set[str]] = {}

        # Track expected workflow IDs per job (client-generated, globally unique)
        # job_id -> set of workflow IDs
        # Used to verify all expected workflows are reported from each DC
        self._job_workflow_ids: dict[str, set[str]] = {}

        # Per-job leader tracking (Context Consistency Protocol)
        # Each job has one leader gate responsible for aggregation and client communication
        # Any gate can accept a job and become its leader (independent of SWIM cluster leadership)
        # Uses JobLeadershipTracker for clean, modular implementation with fencing tokens
        # Metadata type is int (target_dc_count) for gates
        self._job_leadership_tracker: JobLeadershipTracker[int] = JobLeadershipTracker(
            node_id="",  # Set properly in start() when node_id is available
            node_addr=("", 0),  # Set properly in start()
        )

        # Per-job per-DC manager leader tracking
        # Tracks which manager accepted each job in each datacenter
        # Used for routing queries to the authoritative manager for each job
        # job_id -> {dc_id -> (manager_host, manager_tcp_port)}
        self._job_dc_managers: dict[str, dict[str, tuple[str, int]]] = {}

        # Client push notification callbacks
        # job_id -> callback address for push notifications
        self._job_callbacks: dict[str, tuple[str, int]] = {}

        # Cancellation completion tracking (AD-20 push notifications from managers)
        # job_id -> asyncio.Event (set when cancellation complete notification received)
        self._cancellation_completion_events: dict[str, asyncio.Event] = {}
        # job_id -> list of errors from cancelled workflows
        self._cancellation_errors: dict[str, list[str]] = defaultdict(list)

        # Progress update callbacks (for streaming windowed stats)
        # job_id -> callback address for progress updates
        self._progress_callbacks: dict[str, tuple[str, int]] = {}

        # Time-windowed stats collector for cross-DC aggregation
        # Receives unaggregated stats from Managers, aggregates across DCs
        self._windowed_stats = WindowedStatsCollector(
            window_size_ms=env.STATS_WINDOW_SIZE_MS,
            drift_tolerance_ms=env.STATS_DRIFT_TOLERANCE_MS,
            max_window_age_ms=env.STATS_MAX_WINDOW_AGE_MS,
        )

        # Stats push interval (from env config)
        self._stats_push_interval_ms: float = env.STATS_PUSH_INTERVAL_MS

        # Job submissions for reporting configs
        # job_id -> JobSubmission (needed for reporting_configs after aggregation)
        self._job_submissions: dict[str, JobSubmission] = {}

        # Background reporter tasks per job
        # Maps job_id -> dict[reporter_type -> asyncio.Task]
        # Tasks are tracked for cleanup when job is cleaned up
        self._job_reporter_tasks: dict[str, dict[str, asyncio.Task]] = {}

        # Lease management for at-most-once
        self._leases: dict[str, DatacenterLease] = {}  # job_id:dc -> lease
        self._fence_token = 0

        # Per-job fence token tracking for rejecting stale updates
        # job_id -> highest fence_token seen for this job
        self._job_fence_tokens: dict[str, int] = {}

        # State versioning (local gate state version)
        self._state_version = 0
        
        # Gate state for new gate join process
        # Gates start in SYNCING and transition to ACTIVE after state sync
        self._gate_state = GateState.SYNCING
        
        # Quorum circuit breaker
        # Tracks quorum operation failures and implements fail-fast
        cb_config = env.get_circuit_breaker_config()
        self._quorum_circuit = ErrorStats(
            max_errors=cb_config['max_errors'],
            window_seconds=cb_config['window_seconds'],
            half_open_after=cb_config['half_open_after'],
        )

        # Recovery semaphore - limits concurrent recovery operations to prevent thundering herd
        self._recovery_semaphore = asyncio.Semaphore(env.RECOVERY_MAX_CONCURRENT)

        # Configuration
        self._lease_timeout = lease_timeout

        # Job cleanup configuration
        self._job_max_age: float = 3600.0  # 1 hour max age for completed jobs
        self._job_cleanup_interval: float = env.GATE_JOB_CLEANUP_INTERVAL
        self._rate_limit_cleanup_interval: float = env.GATE_RATE_LIMIT_CLEANUP_INTERVAL
        self._batch_stats_interval: float = env.GATE_BATCH_STATS_INTERVAL
        self._tcp_timeout_short: float = env.GATE_TCP_TIMEOUT_SHORT
        self._tcp_timeout_standard: float = env.GATE_TCP_TIMEOUT_STANDARD
        self._tcp_timeout_forward: float = env.GATE_TCP_TIMEOUT_FORWARD
        
        # Inject state embedder for Serf-style heartbeat embedding in SWIM messages
        self.set_state_embedder(GateStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_datacenter=lambda: self._node_id.datacenter,
            is_leader=self.is_leader,
            get_term=lambda: self._leader_election.state.current_term,
            get_state_version=lambda: self._state_version,
            get_gate_state=lambda: self._gate_state.value,
            get_active_jobs=lambda: len(self._jobs),
            get_active_datacenters=lambda: self._count_active_datacenters(),
            get_manager_count=lambda: sum(
                len(managers) for managers in self._datacenter_managers.values()
            ),
            get_tcp_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            on_manager_heartbeat=self._handle_embedded_manager_heartbeat,
            on_gate_heartbeat=self._handle_gate_peer_heartbeat,
            # Piggybacking for discovery
            get_known_managers=self._get_known_managers_for_piggyback,
            get_known_gates=self._get_known_gates_for_piggyback,
            # Job leadership piggybacking (Serf-style like managers)
            get_job_leaderships=self._get_job_leaderships_for_piggyback,
            get_job_dc_managers=self._get_job_dc_managers_for_piggyback,
            # Health piggyback fields (AD-19)
            get_health_has_dc_connectivity=lambda: len(self._datacenter_managers) > 0,
            get_health_connected_dc_count=self._count_active_datacenters,
            get_health_throughput=lambda: 0.0,  # Actual throughput tracking deferred
            get_health_expected_throughput=lambda: 0.0,  # Expected throughput calculation deferred
            get_health_overload_state=lambda: self._overload_detector.get_state(0.0, 0.0),
        ))
        
        # Register node death and join callbacks for failure/recovery handling
        # (Same pattern as ManagerServer for split-brain prevention)
        self.register_on_node_dead(self._on_node_dead)
        self.register_on_node_join(self._on_node_join)
        
        # Register leadership callbacks for state sync
        self.register_on_become_leader(self._on_gate_become_leader)
        self.register_on_lose_leadership(self._on_gate_lose_leadership)
        
        # Federated Health Monitor for cross-DC probing (Gate -> DC Leader)
        # Uses configurable settings tuned for high-latency global links
        fed_config = env.get_federated_health_config()
        self._dc_health_monitor = FederatedHealthMonitor(
            probe_interval=fed_config['probe_interval'],
            probe_timeout=fed_config['probe_timeout'],
            suspicion_timeout=fed_config['suspicion_timeout'],
            max_consecutive_failures=fed_config['max_consecutive_failures'],
        )

        # Cross-DC correlation detector for eviction decisions (Phase 7)
        # Prevents cascade evictions when multiple DCs fail simultaneously
        # (likely network partition, not actual DC failures)
        # Configuration is user-configurable via Env
        self._cross_dc_correlation = CrossDCCorrelationDetector(
            config=env.get_cross_dc_correlation_config()
        )
        # Register known DCs with correlation detector
        for dc_id in self._datacenter_managers.keys():
            self._cross_dc_correlation.add_datacenter(dc_id)

        # Discovery services for adaptive manager selection per datacenter (AD-28)
        # Each datacenter has its own DiscoveryService for locality-aware selection
        self._dc_manager_discovery: dict[str, DiscoveryService] = {}
        self._discovery_failure_decay_interval: float = env.DISCOVERY_FAILURE_DECAY_INTERVAL
        self._discovery_maintenance_task: asyncio.Task | None = None

        # Initialize discovery service per datacenter
        for datacenter_id, manager_addrs in self._datacenter_managers.items():
            static_seeds = [f"{host}:{port}" for host, port in manager_addrs]
            dc_discovery_config = env.get_discovery_config(
                node_role="gate",
                static_seeds=static_seeds,
            )
            dc_discovery = DiscoveryService(dc_discovery_config)
            # Pre-register configured managers
            for host, port in manager_addrs:
                dc_discovery.add_peer(
                    peer_id=f"{host}:{port}",  # Use addr as initial ID until heartbeat received
                    host=host,
                    port=port,
                    role="manager",
                    datacenter_id=datacenter_id,
                )
            self._dc_manager_discovery[datacenter_id] = dc_discovery

        # Discovery service for peer gate selection (AD-28)
        # Used for quorum operations, job leadership, and state sync
        peer_static_seeds = [f"{host}:{port}" for host, port in self._gate_peers]
        peer_discovery_config = env.get_discovery_config(
            node_role="gate",
            static_seeds=peer_static_seeds,
        )
        self._peer_discovery = DiscoveryService(peer_discovery_config)
        # Pre-register seed gate peers
        for host, port in self._gate_peers:
            self._peer_discovery.add_peer(
                peer_id=f"{host}:{port}",  # Use addr as initial ID until heartbeat
                host=host,
                port=port,
                role="gate",
            )

    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """
        Called when a node is marked as DEAD via SWIM.

        Handles gate peer failures (for split-brain awareness).
        Datacenter manager failures are handled via DC availability checks.
        """

        # Check if this is a gate peer
        gate_tcp_addr = self._gate_udp_to_tcp.get(node_addr)
        if gate_tcp_addr:
            self._task_runner.run(self._handle_gate_peer_failure, node_addr, gate_tcp_addr)

    def _on_node_join(self, node_addr: tuple[str, int]) -> None:
        """
        Called when a node joins or rejoins the SWIM cluster.

        Handles gate peer recovery.
        """

        # Check if this is a gate peer
        gate_tcp_addr = self._gate_udp_to_tcp.get(node_addr)
        if gate_tcp_addr:
            self._task_runner.run(self._handle_gate_peer_recovery, node_addr, gate_tcp_addr)
    
    def _get_peer_state_lock(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
        """
        Get or create a lock for a specific peer address.

        Per-peer locks allow concurrent failure/recovery operations on different peers
        while ensuring serialization for operations on the same peer.
        """
        if peer_addr not in self._peer_state_locks:
            self._peer_state_locks[peer_addr] = asyncio.Lock()
        return self._peer_state_locks[peer_addr]

    async def _handle_gate_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """
        Handle a gate peer becoming unavailable (detected via SWIM).

        This is important for split-brain awareness:
        - If we lose contact with majority of peers, we should be cautious
        - Leadership re-election is automatic via LocalLeaderElection

        Also handles per-job leadership takeover when the failed gate was leading jobs.

        Thread safety:
        - Uses per-peer lock to coordinate with recovery handler for same peer
        - Increments epoch to invalidate any in-flight recovery operations
        """

        peer_lock = self._get_peer_state_lock(tcp_addr)
        async with peer_lock:
            # Increment epoch to invalidate any pending recovery operations
            self._peer_state_epoch[tcp_addr] = self._peer_state_epoch.get(tcp_addr, 0) + 1

            # Remove from active peers
            self._active_gate_peers.discard(tcp_addr)

            # Remove from peer discovery service (AD-28)
            peer_host, peer_port = tcp_addr
            peer_id = f"{peer_host}:{peer_port}"
            self._peer_discovery.remove_peer(peer_id)

        # Check if this was the leader
        current_leader = self.get_current_leader()
        was_leader = current_leader == udp_addr

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate peer at {tcp_addr} (UDP: {udp_addr}) marked as DEAD" +
                        (" - was LEADER, re-election will occur" if was_leader else ""),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Handle job leadership takeover for jobs led by the failed gate
        await self._handle_job_leader_failure(tcp_addr)

        # Log quorum status (gates don't use quorum for operations, but useful for monitoring)
        active_count = len(self._active_gate_peers) + 1  # Include self
        total_gates = len(self._gate_peers) + 1

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate cluster: {active_count}/{total_gates} active",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def _handle_gate_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """
        Handle a gate peer recovering/rejoining the cluster.

        Actions:
        1. Capture current epoch before any await
        2. Acquire recovery semaphore (limits concurrent recovery operations)
        3. Apply jitter delay to prevent thundering herd on mass recovery
        4. Verify epoch hasn't changed (peer wasn't marked dead during jitter)
        5. Re-add to active peers set
        6. Add to peer discovery with synthetic peer_id (real NodeId comes via heartbeat)

        Thread safety:
        - Uses epoch checking to detect if failure handler ran during our jitter
        - Uses per-peer lock to coordinate state changes for same peer
        """

        peer_lock = self._get_peer_state_lock(tcp_addr)

        # Capture epoch BEFORE any await points
        async with peer_lock:
            initial_epoch = self._peer_state_epoch.get(tcp_addr, 0)

        # Limit concurrent recovery operations to prevent thundering herd
        async with self._recovery_semaphore:
            # Apply jitter before recovery actions to prevent thundering herd
            # when multiple gates detect recovery simultaneously
            import random
            jitter_min = self.env.RECOVERY_JITTER_MIN
            jitter_max = self.env.RECOVERY_JITTER_MAX
            if jitter_max > 0:
                jitter = random.uniform(jitter_min, jitter_max)
                await asyncio.sleep(jitter)

            # After jitter, check if peer was marked dead during our sleep
            async with peer_lock:
                current_epoch = self._peer_state_epoch.get(tcp_addr, 0)
                if current_epoch != initial_epoch:
                    # Epoch changed - a failure was detected during our jitter
                    # Don't add peer back as it's now considered dead
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerDebug(
                            message=f"Gate peer recovery for {tcp_addr} aborted: epoch changed "
                                    f"({initial_epoch} -> {current_epoch}) during jitter",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                    return

                # Epoch unchanged - safe to add peer back
                self._active_gate_peers.add(tcp_addr)
                # Add to peer discovery with synthetic peer_id based on address
                # The real NodeId will be updated when we receive the peer's heartbeat
                peer_host, peer_port = tcp_addr
                synthetic_peer_id = f"{peer_host}:{peer_port}"
                self._peer_discovery.add_peer(
                    peer_id=synthetic_peer_id,
                    host=peer_host,
                    port=peer_port,
                    role="gate",
                )

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate peer at {tcp_addr} (UDP: {udp_addr}) has REJOINED the cluster",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Log cluster status
        active_count = len(self._active_gate_peers) + 1  # Include self
        total_gates = len(self._gate_peers) + 1

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate cluster: {active_count}/{total_gates} active",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    def _handle_embedded_manager_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Handle ManagerHeartbeat received via SWIM message embedding.
        
        Uses versioned clock to reject stale updates - if the incoming
        heartbeat has a version <= our tracked version for this DC, it's discarded.
        """
        # Check if update is stale using versioned clock
        dc_key = f"dc:{heartbeat.datacenter}"
        if self._versioned_clock.is_entity_stale(dc_key, heartbeat.version):
            # Stale update - discard
            return
        
        # Store per-datacenter, per-manager using heartbeat's self-reported address
        dc = heartbeat.datacenter
        manager_addr = (heartbeat.tcp_host, heartbeat.tcp_port) if heartbeat.tcp_host else source_addr

        if dc not in self._datacenter_manager_status:
            self._datacenter_manager_status[dc] = {}
        self._datacenter_manager_status[dc][manager_addr] = heartbeat
        self._manager_last_status[manager_addr] = time.monotonic()

        # Update discovery service with manager info (AD-28)
        if dc in self._dc_manager_discovery:
            discovery = self._dc_manager_discovery[dc]
            # Use actual node_id from heartbeat (better than synthetic addr-based ID)
            peer_id = heartbeat.node_id if heartbeat.node_id else f"{manager_addr[0]}:{manager_addr[1]}"
            discovery.add_peer(
                peer_id=peer_id,
                host=manager_addr[0],
                port=manager_addr[1],
                role="manager",
                datacenter_id=dc,
            )

        # Update three-signal health state (AD-19)
        manager_key = (dc, manager_addr)
        health_state = self._manager_health.get(manager_key)
        if not health_state:
            health_state = ManagerHealthState(
                manager_id=heartbeat.node_id,
                datacenter_id=dc,
                config=self._manager_health_config,
            )
            self._manager_health[manager_key] = health_state

        # Update signals from heartbeat
        health_state.update_liveness(success=True)
        health_state.update_readiness(
            has_quorum=heartbeat.has_quorum,
            accepting=heartbeat.accepting_jobs,
            worker_count=heartbeat.healthy_worker_count,
        )
        # Progress is updated from throughput metrics if available

        # Record extension and LHM data for cross-DC correlation (Phase 7)
        # This helps distinguish load from failures - high extensions + high LHM
        # across DCs indicates load spike, not health issues
        if heartbeat.workers_with_extensions > 0:
            # Record extension activity for this DC
            # We track at DC level (aggregated from manager heartbeats)
            self._cross_dc_correlation.record_extension(
                datacenter_id=dc,
                worker_id=f"{dc}:{heartbeat.node_id}",  # Use manager as proxy
                extension_count=heartbeat.workers_with_extensions,
                reason="aggregated from manager heartbeat",
            )
        if heartbeat.lhm_score > 0:
            # Record LHM score for this DC
            self._cross_dc_correlation.record_lhm_score(
                datacenter_id=dc,
                lhm_score=heartbeat.lhm_score,
            )

        # Update version tracking via TaskRunner
        self._task_runner.run(
            self._versioned_clock.update_entity, dc_key, heartbeat.version
        )
    
    def _handle_gate_peer_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Handle GateHeartbeat received from peer gates via SWIM.

        This enables:
        1. Proper node_id tracking for peers (instead of synthetic IDs)
        2. Leader tracking across the gate cluster
        3. Version-based stale update rejection
        4. Job leadership propagation (Serf-style piggybacking)
        5. Per-DC manager tracking for job queries
        """

        # Check if update is stale using versioned clock
        if self._versioned_clock.is_entity_stale(heartbeat.node_id, heartbeat.version):
            return

        # Store peer info keyed by UDP address (source_addr is the SWIM UDP address)
        self._gate_peer_info[source_addr] = heartbeat

        # Get peer TCP address for discovery tracking
        # Note: TCP and UDP addresses can be completely different - use heartbeat fields
        peer_tcp_host = heartbeat.tcp_host if heartbeat.tcp_host else source_addr[0]
        peer_tcp_port = heartbeat.tcp_port if heartbeat.tcp_port else source_addr[1]
        peer_tcp_addr = (peer_tcp_host, peer_tcp_port)

        # AD-29: Confirm this peer in the SWIM layer since we received their heartbeat
        # This allows the suspicion subprotocol to function properly
        self.confirm_peer(source_addr)

        # Update UDP to TCP mapping for failure/recovery callbacks
        # source_addr is the UDP address from SWIM, peer_tcp_addr is from heartbeat
        # This mapping is critical: without it, _on_node_join/_on_node_dead
        # cannot find the TCP address for dynamically discovered gates
        udp_addr = source_addr  # SWIM source address is always UDP
        if udp_addr not in self._gate_udp_to_tcp:
            self._gate_udp_to_tcp[udp_addr] = peer_tcp_addr
            # Also add to active peers since this is a new discovery via heartbeat
            self._active_gate_peers.add(peer_tcp_addr)
        elif self._gate_udp_to_tcp[udp_addr] != peer_tcp_addr:
            # TCP address changed (rare but possible) - update mapping
            old_tcp_addr = self._gate_udp_to_tcp[udp_addr]
            self._active_gate_peers.discard(old_tcp_addr)
            self._gate_udp_to_tcp[udp_addr] = peer_tcp_addr
            self._active_gate_peers.add(peer_tcp_addr)

        # Update peer discovery service (AD-28)
        self._peer_discovery.add_peer(
            peer_id=heartbeat.node_id,
            host=peer_tcp_host,
            port=peer_tcp_port,
            role="gate",
        )

        # Update three-signal health state for peer gate (AD-19)
        gate_id = heartbeat.node_id
        health_state = self._gate_peer_health.get(gate_id)
        if not health_state:
            health_state = GateHealthState(
                gate_id=gate_id,
                config=self._gate_health_config,
            )
            self._gate_peer_health[gate_id] = health_state

        # Update signals from heartbeat
        health_state.update_liveness(success=True)
        health_state.update_readiness(
            has_dc_connectivity=heartbeat.connected_dc_count > 0,
            connected_dc_count=heartbeat.connected_dc_count,
            overload_state=getattr(heartbeat, 'overload_state', 'healthy'),
        )

        # Process job leadership claims (Serf-style UDP piggybacking)
        # peer_tcp_addr was computed earlier for UDP-to-TCP mapping
        self._process_job_leadership_heartbeat(heartbeat, peer_tcp_addr)

        # Process per-DC manager tracking for jobs led by this peer
        self._process_job_dc_managers_heartbeat(heartbeat)

        # Update version tracking
        self._task_runner.run(
            self._versioned_clock.update_entity, heartbeat.node_id, heartbeat.version
        )

    def _process_job_leadership_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        peer_tcp_addr: tuple[str, int],
    ) -> None:
        """
        Process job leadership claims from a peer gate's heartbeat.

        Uses fencing tokens for consistency:
        - Accept leadership claim only if fencing token is higher than what we have
        - This prevents stale leaders from reasserting leadership after recovery

        This is the UDP-based job leadership protocol (Serf-style piggybacking),
        mirroring the manager implementation for architectural consistency.
        """
        for job_id, (fencing_token, target_dc_count) in heartbeat.job_leaderships.items():
            # Use tracker's process_leadership_claim (handles fencing token comparison)
            self._job_leadership_tracker.process_leadership_claim(
                job_id=job_id,
                claimer_id=heartbeat.node_id,
                claimer_addr=peer_tcp_addr,
                fencing_token=fencing_token,
                metadata=target_dc_count,
            )

    def _process_job_dc_managers_heartbeat(
        self,
        heartbeat: GateHeartbeat,
    ) -> None:
        """
        Process per-DC manager tracking from a peer gate's heartbeat.

        This enables non-leader gates to know which manager to query
        for each job's results in each datacenter. When a job leader
        fails, this information allows the new leader to route queries
        correctly.
        """
        for job_id, dc_managers in heartbeat.job_dc_managers.items():
            # Only accept if this peer is the job leader (has authority)
            peer_is_leader = self._job_leadership_tracker.get_leader(job_id) == heartbeat.node_id

            if peer_is_leader:
                # Merge DC manager info - peer's data is authoritative for jobs they lead
                if job_id not in self._job_dc_managers:
                    self._job_dc_managers[job_id] = {}

                for dc_id, manager_addr in dc_managers.items():
                    # Only update if we don't have info for this DC yet
                    # (prevent overwrites during failover transitions)
                    if dc_id not in self._job_dc_managers[job_id]:
                        self._job_dc_managers[job_id][dc_id] = manager_addr

    def _get_healthy_gates(self) -> list[GateInfo]:
        """
        Build list of all known healthy gates for manager discovery.
        
        Includes self and all active peer gates. Managers use this
        to maintain redundant communication channels.
        
        Uses real node_ids from GateHeartbeat when available (received via SWIM),
        falling back to synthetic IDs for peers we haven't heard from yet.
        """
        gates: list[GateInfo] = []
        
        # Add self
        gates.append(GateInfo(
            node_id=self._node_id.full,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            udp_host=self._host,
            udp_port=self._udp_port,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
        ))
        
        # Add active peer gates
        for tcp_addr in self._active_gate_peers:
            # Find UDP addr for this peer
            udp_addr: tuple[str, int] | None = None
            for udp, tcp in list(self._gate_udp_to_tcp.items()):
                if tcp == tcp_addr:
                    udp_addr = udp
                    break
            
            if udp_addr is None:
                udp_addr = tcp_addr  # Fallback
            
            # Check if we have real peer info from GateHeartbeat
            peer_heartbeat = self._gate_peer_info.get(udp_addr)
            
            if peer_heartbeat:
                # Use real info from SWIM heartbeat
                gates.append(GateInfo(
                    node_id=peer_heartbeat.node_id,
                    tcp_host=tcp_addr[0],
                    tcp_port=tcp_addr[1],
                    udp_host=udp_addr[0],
                    udp_port=udp_addr[1],
                    datacenter=peer_heartbeat.datacenter,
                    is_leader=peer_heartbeat.is_leader,
                ))
            else:
                # Fallback to synthetic ID (peer hasn't sent heartbeat yet)
                gates.append(GateInfo(
                    node_id=f"gate-{tcp_addr[0]}:{tcp_addr[1]}",
                    tcp_host=tcp_addr[0],
                    tcp_port=tcp_addr[1],
                    udp_host=udp_addr[0],
                    udp_port=udp_addr[1],
                    datacenter=self._node_id.datacenter,
                    is_leader=False,
                ))
        
        return gates
    
    @property
    def node_info(self) -> NodeInfo:
        """Get this gate's node info."""
        return NodeInfo(
            node_id=self._node_id.full,
            role=NodeRole.GATE.value,
            host=self._host,
            port=self._tcp_port,
            datacenter=self._node_id.datacenter,
            version=self._state_version,
        )
    
    def _increment_version(self) -> int:
        """Increment and return the state version."""
        self._state_version += 1
        return self._state_version
    
    def _get_fence_token(self) -> int:
        """Generate a new fencing token."""
        self._fence_token += 1
        return self._fence_token

    # =========================================================================
    # Per-Job Leader Helpers (independent of SWIM cluster leadership)
    # =========================================================================

    def _is_job_leader(self, job_id: str) -> bool:
        """Check if this gate is the leader for the given job."""
        return self._job_leadership_tracker.is_leader(job_id)

    def _get_job_leader(self, job_id: str) -> str | None:
        """Get the node_id of the job leader, or None if unknown."""
        return self._job_leadership_tracker.get_leader(job_id)

    def _get_job_leader_addr(self, job_id: str) -> tuple[str, int] | None:
        """Get the TCP address of the job leader, or None if unknown."""
        return self._job_leadership_tracker.get_leader_addr(job_id)

    async def _handle_job_leader_failure(
        self,
        failed_gate_addr: tuple[str, int],
    ) -> None:
        """
        Handle job leadership takeover when a gate fails.

        When a gate that was leading jobs fails, another gate takes over
        leadership for those jobs. This ensures jobs continue to be monitored
        and results are properly aggregated.

        Only takes over jobs that are not yet in a terminal state
        (COMPLETED, FAILED, CANCELLED).
        """
        # Find all jobs led by the failed gate (using tracker's helper)
        candidate_jobs = self._job_leadership_tracker.get_jobs_led_by_addr(failed_gate_addr)

        # Filter to only active (non-terminal) jobs
        orphaned_jobs: list[str] = []
        for job_id in candidate_jobs:
            job = self._jobs.get(job_id)
            if job and job.status not in (
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
            ):
                orphaned_jobs.append(job_id)

        if not orphaned_jobs:
            return

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Taking over {len(orphaned_jobs)} jobs from failed gate at {failed_gate_addr}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Take over leadership for each orphaned job
        for job_id in orphaned_jobs:
            # Get old leader ID before takeover (for manager notification)
            old_gate_id = self._job_leadership_tracker.get_leader(job_id)

            # Use tracker's takeover method (handles fencing token increment)
            target_dc_count = len(self._job_target_dcs.get(job_id, set()))
            self._job_leadership_tracker.takeover_leadership(job_id, metadata=target_dc_count)

            # Broadcast new leadership to peer gates
            await self._broadcast_job_leadership(job_id, target_dc_count)

            # AD-31: Notify managers of the leadership transfer so they update
            # their _job_origin_gates mapping and route results to new leader
            await self._notify_managers_of_leadership_transfer(job_id, old_gate_id)

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Assumed leadership for job {job_id[:8]}...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        self._increment_version()

    async def _broadcast_job_leadership(
        self,
        job_id: str,
        datacenter_count: int,
    ) -> None:
        """
        Broadcast job leadership announcement to all peer gates.

        This ensures all gates in the cluster know who is leading
        a specific job, enabling proper routing of DC results
        and allowing non-leaders to forward requests to the leader.
        """
        announcement = JobLeadershipAnnouncement(
            job_id=job_id,
            leader_id=self._node_id.full,
            leader_host=self._host,
            leader_tcp_port=self._tcp_port,
            term=self._leader_election.state.current_term,
            workflow_count=datacenter_count,  # Repurposed for DC count at gate level
            timestamp=time.monotonic(),
            workflow_names=[],  # Not applicable for gate-level leadership
        )

        # Get all active peer gate addresses
        for peer_addr in self._active_gate_peers:
            try:
                response, _ = await self.send_tcp(
                    peer_addr,
                    action='job_leadership_announcement',
                    data=announcement.dump(),
                    timeout=2.0,
                )

                if response and isinstance(response, bytes) and response != b'error':
                    ack = JobLeadershipAck.load(response)
                    if ack.accepted:
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerDebug(
                                message=f"Job {job_id[:8]}... leadership accepted by {ack.responder_id[:8]}...",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Failed to announce job {job_id[:8]}... leadership to {peer_addr}: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

    async def _notify_managers_of_leadership_transfer(
        self,
        job_id: str,
        old_gate_id: str | None,
    ) -> None:
        """
        Notify all managers assigned to a job that leadership has transferred to this gate.

        Part of AD-31: When a gate takes over job leadership from a failed gate,
        managers need to update their _job_origin_gates mapping so they route
        job results to the new leader gate.

        Args:
            job_id: The job whose leadership transferred
            old_gate_id: Node ID of the previous leader (if known)
        """
        # Get managers assigned to this job
        dc_managers = self._job_dc_managers.get(job_id, {})
        if not dc_managers:
            return

        fence_token = self._job_leadership_tracker.get_fencing_token(job_id)

        transfer_msg = JobLeaderGateTransfer(
            job_id=job_id,
            new_gate_id=self._node_id.full,
            new_gate_addr=(self._host, self._tcp_port),
            fence_token=fence_token,
            old_gate_id=old_gate_id,
        )

        notified_count = 0
        failed_count = 0

        # Notify each manager in each DC assigned to this job
        for datacenter_id, manager_addr in dc_managers.items():
            try:
                response, _ = await self.send_tcp(
                    manager_addr,
                    action='job_leader_gate_transfer',
                    data=transfer_msg.dump(),
                    timeout=2.0,
                )

                if response and isinstance(response, bytes) and response != b'error':
                    ack = JobLeaderGateTransferAck.load(response)
                    if ack.accepted:
                        notified_count += 1
                    else:
                        failed_count += 1
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerWarning(
                                message=f"Manager {ack.manager_id[:8]}... rejected job {job_id[:8]}... leadership transfer",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )
                else:
                    failed_count += 1

            except Exception as e:
                failed_count += 1
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Failed to notify manager at {manager_addr} of job {job_id[:8]}... leadership transfer: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

        if notified_count > 0 or failed_count > 0:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Job {job_id[:8]}... leadership transfer notifications: {notified_count} accepted, {failed_count} failed",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _get_state_snapshot(self) -> GateStateSnapshot:
        """Get a complete state snapshot for state sync."""
        # Get job leadership snapshot once (efficient)
        job_leaders, job_leader_addrs, job_fencing_tokens = self._job_leadership_tracker.to_snapshot()

        return GateStateSnapshot(
            node_id=self._node_id.full,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            version=self._state_version,
            jobs=dict(self._jobs),
            datacenter_status={
                dc: self._classify_datacenter_health(dc)
                for dc in self._datacenter_managers.keys()
            },
            leases=dict(self._leases),
            # Include manager discovery info for cross-gate sync
            datacenter_managers={dc: list(addrs) for dc, addrs in self._datacenter_managers.items()},
            datacenter_manager_udp={dc: list(addrs) for dc, addrs in self._datacenter_manager_udp.items()},
            # Include per-job leadership tracking for cross-gate sync (via tracker)
            job_leaders=job_leaders,
            job_leader_addrs=job_leader_addrs,
            job_fencing_tokens=job_fencing_tokens,
            # Include per-job per-DC manager leaders for query routing
            job_dc_managers={job_id: dict(dc_mgrs) for job_id, dc_mgrs in self._job_dc_managers.items()},
        )
    
    def _on_gate_become_leader(self) -> None:
        """
        Called when this gate becomes the leader.
        
        Triggers state sync from other gate peers to ensure the new
        leader has complete global job state.
        """
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message="Gate became leader, initiating state sync from peers",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        self._task_runner.run(self._sync_state_from_gate_peers)
    
    def _on_gate_lose_leadership(self) -> None:
        """Called when this gate loses leadership."""
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message="Gate lost leadership",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def _sync_state_from_gate_peers(self) -> None:
        """
        Sync state from active gate peers when becoming leader.

        Uses exponential backoff for retries to handle transient failures.
        Handles the case where peers are not ready (still in SYNCING state)
        by retrying until the peer becomes ACTIVE or retries are exhausted.
        """
        if not self._active_gate_peers:
            return

        request = StateSyncRequest(
            requester_id=self._node_id.full,
            requester_role=NodeRole.GATE.value,
            since_version=0,  # Get all state
        )

        synced_count = 0
        max_retries = 3

        for peer_addr in self._active_gate_peers:
            for attempt in range(max_retries):
                try:
                    response, _ = await self.send_tcp(
                        peer_addr,
                        "gate_state_sync_request",
                        request.dump(),
                        timeout=5.0 * (attempt + 1),  # Exponential backoff
                    )

                    if isinstance(response, bytes) and response:
                        sync_response = StateSyncResponse.load(response)

                        # Check if peer is ready to serve state
                        if not sync_response.responder_ready:
                            # Peer is alive but not ready yet - retry
                            if attempt < max_retries - 1:
                                await asyncio.sleep(0.5 * (2 ** attempt))
                                continue
                            # Last attempt - log warning and move on
                            await self._udp_logger.log(
                                ServerWarning(
                                    message=f"Gate peer {peer_addr} not ready for state sync after {max_retries} attempts",
                                    node_host=self._host,
                                    node_port=self._tcp_port,
                                    node_id=self._node_id.short,
                                )
                            )
                            break

                        if sync_response.gate_state:
                            self._apply_gate_state_snapshot(sync_response.gate_state)
                            synced_count += 1
                    break  # Success or no state available

                except Exception as e:
                    if attempt == max_retries - 1:
                        await self.handle_exception(e, f"state_sync_from_{peer_addr}")
                    else:
                        await asyncio.sleep(0.5 * (2 ** attempt))  # Backoff

        await self._udp_logger.log(
            ServerInfo(
                message=f"State sync complete: synced from {synced_count}/{len(self._active_gate_peers)} peers",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    def _apply_gate_state_snapshot(self, snapshot: GateStateSnapshot) -> None:
        """
        Apply a state snapshot from another gate.

        Merges job state, preferring entries with higher versions.
        """
        # Merge jobs - keep newer versions
        for job_id, job in snapshot.jobs.items():
            existing = self._jobs.get(job_id)
            if not existing or getattr(job, 'timestamp', 0) > getattr(existing, 'timestamp', 0):
                self._jobs[job_id] = job

        # Merge leases - keep ones with higher fence tokens
        for lease_key, lease in snapshot.leases.items():
            existing = self._leases.get(lease_key)
            if not existing or lease.fence_token > existing.fence_token:
                self._leases[lease_key] = lease

        # Merge per-job leadership tracking via tracker
        # Uses fencing tokens for proper consistency
        self._job_leadership_tracker.merge_from_snapshot(
            job_leaders=snapshot.job_leaders,
            job_leader_addrs=snapshot.job_leader_addrs,
            job_fencing_tokens=snapshot.job_fencing_tokens,
        )

        # Merge per-job per-DC manager leaders
        # Only add jobs we don't already have DC manager info for
        for job_id, dc_managers in snapshot.job_dc_managers.items():
            if job_id not in self._job_dc_managers:
                self._job_dc_managers[job_id] = dict(dc_managers)
            else:
                # Merge DC managers we don't already have
                for dc_id, manager_addr in dc_managers.items():
                    if dc_id not in self._job_dc_managers[job_id]:
                        self._job_dc_managers[job_id][dc_id] = manager_addr

        self._increment_version()
    
    async def _broadcast_manager_discovery(
        self,
        datacenter: str,
        manager_tcp_addr: tuple[str, int],
        manager_udp_addr: tuple[str, int] | None = None,
        worker_count: int = 0,
        healthy_worker_count: int = 0,
        available_cores: int = 0,
        total_cores: int = 0,
    ) -> None:
        """
        Broadcast a newly discovered manager to all peer gates.
        
        Called when a manager registers with this gate. Ensures all gates
        learn about the manager even if they don't receive direct registration.
        Includes manager status so peer gates can update their datacenter health.
        """
        if not self._active_gate_peers:
            return
        
        broadcast = ManagerDiscoveryBroadcast(
            datacenter=datacenter,
            manager_tcp_addr=manager_tcp_addr,
            manager_udp_addr=manager_udp_addr,
            source_gate_id=self._node_id.full,
            worker_count=worker_count,
            healthy_worker_count=healthy_worker_count,
            available_cores=available_cores,
            total_cores=total_cores,
        )
        
        broadcast_count = 0
        for peer_addr in self._active_gate_peers:
            try:
                await self.send_tcp(
                    peer_addr,
                    "manager_discovery",
                    broadcast.dump(),
                    timeout=2.0,
                )
                broadcast_count += 1
            except Exception:
                # Best effort - peer may be down
                pass
        
        if broadcast_count > 0:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Broadcast manager {manager_tcp_addr} in DC {datacenter} to {broadcast_count} peer gates",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
    
    def _get_manager_circuit(self, manager_addr: tuple[str, int]) -> ErrorStats:
        """
        Get or create a circuit breaker for a specific manager.
        
        Each manager has its own circuit breaker so that failures to one
        manager don't affect dispatch to other managers.
        """
        if manager_addr not in self._manager_circuits:
            cb_config = self.env.get_circuit_breaker_config()
            self._manager_circuits[manager_addr] = ErrorStats(
                max_errors=cb_config['max_errors'],
                window_seconds=cb_config['window_seconds'],
                half_open_after=cb_config['half_open_after'],
            )
        return self._manager_circuits[manager_addr]
    
    def _is_manager_circuit_open(self, manager_addr: tuple[str, int]) -> bool:
        """Check if a manager's circuit breaker is open."""
        circuit = self._manager_circuits.get(manager_addr)
        if not circuit:
            return False
        return circuit.circuit_state == CircuitState.OPEN
    
    def get_manager_circuit_status(self, manager_addr: tuple[str, int]) -> dict | None:
        """
        Get circuit breaker status for a specific manager.
        
        Returns None if manager has no circuit breaker (never had failures).
        """
        circuit = self._manager_circuits.get(manager_addr)
        if not circuit:
            return None
        return {
            "manager_addr": f"{manager_addr[0]}:{manager_addr[1]}",
            "circuit_state": circuit.circuit_state.name,
            "error_count": circuit.error_count,
            "error_rate": circuit.error_rate,
        }
    
    def get_all_manager_circuit_status(self) -> dict:
        """Get circuit breaker status for all managers."""
        return {
            "managers": {
                f"{addr[0]}:{addr[1]}": self.get_manager_circuit_status(addr)
                for addr in self._manager_circuits.keys()
            },
            "open_circuits": [
                f"{addr[0]}:{addr[1]}" for addr in self._manager_circuits.keys()
                if self._is_manager_circuit_open(addr)
            ],
        }
    
    def _count_active_datacenters(self) -> int:
        """
        Count datacenters with at least one fresh manager heartbeat.

        A datacenter is active if any manager has sent a heartbeat in the last 60s.
        """
        now = time.monotonic()
        active_count = 0
        for dc_id in self._datacenter_manager_status:
            for manager_addr in self._datacenter_manager_status[dc_id]:
                if now - self._manager_last_status.get(manager_addr, 0) < 60.0:
                    active_count += 1
                    break  # Only count DC once
        return active_count

    def _get_known_managers_for_piggyback(self) -> dict[str, tuple[str, int, str, int, str]]:
        """
        Get known managers for piggybacking in SWIM heartbeats.

        Returns: dict mapping manager_id -> (tcp_host, tcp_port, udp_host, udp_port, datacenter)
        """
        result: dict[str, tuple[str, int, str, int, str]] = {}
        for dc_id, manager_status in self._datacenter_manager_status.items():
            for manager_addr, heartbeat in manager_status.items():
                if heartbeat.node_id:
                    tcp_host = heartbeat.tcp_host or manager_addr[0]
                    tcp_port = heartbeat.tcp_port or manager_addr[1]
                    udp_host = heartbeat.udp_host or manager_addr[0]
                    udp_port = heartbeat.udp_port or manager_addr[1]
                    result[heartbeat.node_id] = (tcp_host, tcp_port, udp_host, udp_port, dc_id)
        return result

    def _get_known_gates_for_piggyback(self) -> dict[str, tuple[str, int, str, int]]:
        """
        Get known gates for piggybacking in SWIM heartbeats.

        Returns: dict mapping gate_id -> (tcp_host, tcp_port, udp_host, udp_port)
        """
        result: dict[str, tuple[str, int, str, int]] = {}
        for gate_id, gate_info in self._known_gates.items():
            result[gate_id] = (
                gate_info.tcp_host,
                gate_info.tcp_port,
                gate_info.udp_host,
                gate_info.udp_port,
            )
        return result

    def _get_job_leaderships_for_piggyback(self) -> dict[str, tuple[int, int]]:
        """
        Get job leadership info for piggybacking in SWIM heartbeats.

        Only includes jobs where this gate is the leader. This enables
        Serf-style distributed consistency - other gates learn about
        job leadership via UDP heartbeats (passive propagation).

        Returns: dict mapping job_id -> (fencing_token, target_dc_count)
        """
        # Get claims from tracker (job_id -> (fencing_token, metadata))
        # Metadata is target_dc_count for gates
        claims = self._job_leadership_tracker.get_leadership_claims()

        # Convert to expected format, using stored metadata or computing from _job_target_dcs
        result: dict[str, tuple[int, int]] = {}
        for job_id, (fencing_token, metadata) in claims.items():
            target_dc_count = metadata if metadata is not None else len(self._job_target_dcs.get(job_id, set()))
            result[job_id] = (fencing_token, target_dc_count)
        return result

    def _get_job_dc_managers_for_piggyback(self) -> dict[str, dict[str, tuple[str, int]]]:
        """
        Get per-job per-DC manager leader info for piggybacking in SWIM heartbeats.

        Only includes jobs where this gate is the leader. This enables
        other gates to know which manager to query for each job's
        results in each datacenter.

        Returns: dict mapping job_id -> {dc_id -> (manager_host, manager_port)}
        """
        result: dict[str, dict[str, tuple[str, int]]] = {}
        # Get jobs we lead from the tracker
        for job_id in self._job_leadership_tracker.get_leadership_claims().keys():
            dc_managers = self._job_dc_managers.get(job_id)
            if dc_managers:
                result[job_id] = dict(dc_managers)
        return result

    def _get_best_manager_heartbeat(self, dc_id: str) -> tuple[ManagerHeartbeat | None, int, int]:
        """
        Get the most authoritative manager heartbeat for a datacenter.
        
        Strategy:
        1. Prefer the LEADER's heartbeat if fresh (within 30s)
        2. Fall back to any fresh manager heartbeat
        3. Return None if no fresh heartbeats
        
        Returns:
            tuple of (best_heartbeat, alive_manager_count, total_manager_count)
        """
        manager_statuses = self._datacenter_manager_status.get(dc_id, {})
        now = time.monotonic()
        heartbeat_timeout = 30.0  # Heartbeats older than 30s are considered stale
        
        best_heartbeat: ManagerHeartbeat | None = None
        leader_heartbeat: ManagerHeartbeat | None = None
        alive_count = 0
        
        for manager_addr, heartbeat in manager_statuses.items():
            last_seen = self._manager_last_status.get(manager_addr, 0)
            is_fresh = (now - last_seen) < heartbeat_timeout
            
            if is_fresh:
                alive_count += 1
                
                # Track leader heartbeat separately
                if heartbeat.is_leader:
                    leader_heartbeat = heartbeat
                
                # Keep any fresh heartbeat as fallback
                if best_heartbeat is None:
                    best_heartbeat = heartbeat
        
        # Prefer leader if available
        if leader_heartbeat is not None:
            best_heartbeat = leader_heartbeat
        
        total_managers = len(self._datacenter_managers.get(dc_id, []))
        return best_heartbeat, alive_count, total_managers
    
    def _classify_datacenter_health(self, dc_id: str) -> DatacenterStatus:
        """
        Classify datacenter health based on TCP heartbeats from managers.
        
        Health States (evaluated in order):
        1. UNHEALTHY: No managers registered OR no workers registered
        2. DEGRADED: Majority of workers unhealthy OR majority of managers unhealthy
        3. BUSY: NOT degraded AND available_cores == 0 (transient, will clear)
        4. HEALTHY: NOT degraded AND available_cores > 0
        
        Key insight: BUSY ≠ UNHEALTHY
        - BUSY = transient, will clear → accept job (queued)
        - DEGRADED = structural problem, reduced capacity → may need intervention
        - UNHEALTHY = severe problem → try fallback datacenter
        
        Note: Gates and managers are in different SWIM clusters, so we can't use
        SWIM probes for cross-cluster health. We use TCP heartbeats instead.
        Manager liveness is determined by recent TCP heartbeats per-manager.
        
        Uses the LEADER's heartbeat as the authoritative source for worker info.
        Falls back to any fresh manager heartbeat if leader is stale.
        
        See AD-16 in docs/architecture.md.
        """
        # Get best manager heartbeat (prefers leader, falls back to any fresh)
        status, alive_managers, total_managers = self._get_best_manager_heartbeat(dc_id)
        
        # === UNHEALTHY: No managers registered ===
        if total_managers == 0:
            return DatacenterStatus(
                dc_id=dc_id,
                health=DatacenterHealth.UNHEALTHY.value,
                available_capacity=0,
                queue_depth=0,
                manager_count=0,
                worker_count=0,
                last_update=time.monotonic(),
            )
        
        # === UNHEALTHY: No fresh heartbeats or no workers registered ===
        if not status or status.worker_count == 0:
            return DatacenterStatus(
                dc_id=dc_id,
                health=DatacenterHealth.UNHEALTHY.value,
                available_capacity=0,
                queue_depth=0,
                manager_count=alive_managers,
                worker_count=0,
                last_update=time.monotonic(),
            )
        
        # Extract worker health info from status
        # ManagerHeartbeat includes healthy_worker_count (workers responding to SWIM)
        total_workers = status.worker_count
        healthy_workers = getattr(status, 'healthy_worker_count', total_workers)
        available_cores = status.available_cores
        
        # === Check for DEGRADED state ===
        is_degraded = False
        
        # Majority of managers unhealthy?
        manager_quorum = total_managers // 2 + 1
        if total_managers > 0 and alive_managers < manager_quorum:
            is_degraded = True
        
        # Majority of workers unhealthy?
        worker_quorum = total_workers // 2 + 1
        if total_workers > 0 and healthy_workers < worker_quorum:
            is_degraded = True
        
        # === Determine final health state ===
        if is_degraded:
            health = DatacenterHealth.DEGRADED
        elif available_cores == 0:
            # Not degraded, but no capacity = BUSY (transient)
            health = DatacenterHealth.BUSY
        else:
            # Not degraded, has capacity = HEALTHY
            health = DatacenterHealth.HEALTHY
        
        return DatacenterStatus(
            dc_id=dc_id,
            health=health.value,
            available_capacity=available_cores,
            queue_depth=getattr(status, 'queue_depth', 0),
            manager_count=alive_managers,
            worker_count=healthy_workers,  # Report healthy workers, not total
            last_update=time.monotonic(),
        )
    
    def _get_all_datacenter_health(self) -> dict[str, DatacenterStatus]:
        """
        Get health classification for all registered datacenters.

        Only classifies DCs that have achieved READY or PARTIAL registration
        status (AD-27). DCs that are still AWAITING_INITIAL or INITIALIZING
        are excluded from health classification to prevent false UNHEALTHY
        classifications during startup.
        """
        result: dict[str, DatacenterStatus] = {}
        for dc_id in self._datacenter_managers.keys():
            if self._is_dc_ready_for_health_classification(dc_id):
                result[dc_id] = self._classify_datacenter_health(dc_id)
        return result

    # =========================================================================
    # Three-Signal Manager Health (AD-19)
    # =========================================================================

    def _get_manager_health_state(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
    ) -> ManagerHealthState | None:
        """Get the three-signal health state for a manager."""
        manager_key = (dc_id, manager_addr)
        return self._manager_health.get(manager_key)

    def _get_manager_routing_decision(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
    ) -> RoutingDecision | None:
        """Get routing decision for a manager based on three-signal health."""
        health_state = self._get_manager_health_state(dc_id, manager_addr)
        if health_state:
            return health_state.get_routing_decision()
        return None

    def _get_routable_managers_in_dc(self, dc_id: str) -> list[tuple[str, int]]:
        """
        Get list of managers in a DC that can receive new jobs.

        Returns managers where routing decision is ROUTE.
        """
        routable: list[tuple[str, int]] = []
        for manager_addr in self._datacenter_managers.get(dc_id, []):
            decision = self._get_manager_routing_decision(dc_id, manager_addr)
            # If no health state yet, consider routable (optimistic)
            if decision is None or decision == RoutingDecision.ROUTE:
                routable.append(manager_addr)
        return routable

    def _get_dc_health_from_managers(self, dc_id: str) -> DatacenterHealth:
        """
        Classify DC health based on manager health signals (AD-19).

        Rules:
        - ALL managers NOT liveness → DC = UNHEALTHY
        - MAJORITY managers NOT readiness → DC = DEGRADED
        - ANY manager progress == "stuck" → DC = DEGRADED
        - Otherwise → HEALTHY
        """
        manager_addrs = self._datacenter_managers.get(dc_id, [])
        if not manager_addrs:
            return DatacenterHealth.UNHEALTHY

        live_count = 0
        ready_count = 0
        has_stuck = False
        total = len(manager_addrs)

        for manager_addr in manager_addrs:
            health_state = self._get_manager_health_state(dc_id, manager_addr)
            if health_state:
                if health_state.liveness:
                    live_count += 1
                if health_state.readiness:
                    ready_count += 1
                if health_state.progress_state.value == "stuck":
                    has_stuck = True
            else:
                # No health state yet - assume live for new managers
                live_count += 1

        # ALL managers NOT liveness → UNHEALTHY
        if live_count == 0:
            return DatacenterHealth.UNHEALTHY

        # MAJORITY managers NOT readiness → DEGRADED
        quorum = total // 2 + 1
        if ready_count < quorum:
            return DatacenterHealth.DEGRADED

        # ANY manager stuck → DEGRADED
        if has_stuck:
            return DatacenterHealth.DEGRADED

        return DatacenterHealth.HEALTHY

    def _get_managers_to_evict(self, dc_id: str) -> list[tuple[str, int]]:
        """Get list of managers that should be evicted based on health signals."""
        evict: list[tuple[str, int]] = []
        for manager_addr in self._datacenter_managers.get(dc_id, []):
            decision = self._get_manager_routing_decision(dc_id, manager_addr)
            if decision == RoutingDecision.EVICT:
                evict.append(manager_addr)
        return evict

    def _get_manager_health_diagnostics(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
    ) -> dict | None:
        """Get diagnostic information for a manager's health state."""
        health_state = self._get_manager_health_state(dc_id, manager_addr)
        if health_state:
            return health_state.get_diagnostics()
        return None

    # =========================================================================
    # Three-Signal Gate Peer Health (AD-19)
    # =========================================================================

    def _get_gate_peer_health_state(self, gate_id: str) -> GateHealthState | None:
        """Get the three-signal health state for a peer gate."""
        return self._gate_peer_health.get(gate_id)

    def _get_gate_peer_routing_decision(self, gate_id: str) -> RoutingDecision | None:
        """Get routing decision for a peer gate based on three-signal health."""
        health_state = self._get_gate_peer_health_state(gate_id)
        if health_state:
            return health_state.get_routing_decision()
        return None

    def _get_routable_peer_gates(self) -> list[str]:
        """
        Get list of peer gates that can receive forwarded jobs.

        Returns gate IDs where routing decision is ROUTE.
        """
        return [
            gate_id
            for gate_id, health_state in self._gate_peer_health.items()
            if health_state.get_routing_decision() == RoutingDecision.ROUTE
        ]

    def _get_gates_eligible_for_election(self) -> list[str]:
        """
        Get list of peer gates eligible for leader election.

        Returns gate IDs where should_participate_in_election is True.
        """
        eligible: list[str] = []
        for gate_id, health_state in self._gate_peer_health.items():
            if health_state.should_participate_in_election():
                eligible.append(gate_id)
        return eligible

    def _get_gates_to_evict(self) -> list[str]:
        """Get list of peer gates that should be evicted based on health signals."""
        return [
            gate_id
            for gate_id, health_state in self._gate_peer_health.items()
            if health_state.get_routing_decision() == RoutingDecision.EVICT
        ]

    def _get_gate_peer_health_diagnostics(self, gate_id: str) -> dict | None:
        """Get diagnostic information for a peer gate's health state."""
        health_state = self._get_gate_peer_health_state(gate_id)
        if health_state:
            return health_state.get_diagnostics()
        return None

    # =========================================================================
    # Load Shedding (AD-22)
    # =========================================================================

    def _should_shed_request(self, message_type: str) -> bool:
        """
        Check if a request should be shed based on current load.

        Uses the HybridOverloadDetector to determine current state and
        LoadShedder to decide based on message priority.

        Args:
            message_type: The type of message being processed

        Returns:
            True if request should be shed, False to process normally
        """
        return self._load_shedder.should_shed(message_type)

    def _record_request_latency(self, latency_ms: float) -> None:
        """
        Record request processing latency for overload detection.

        Should be called after processing each request to update
        the overload detector's latency model.

        Args:
            latency_ms: Request processing time in milliseconds
        """
        self._overload_detector.record_latency(latency_ms)

    def _record_manager_heartbeat(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
        node_id: str,
        generation: int,
    ) -> None:
        """
        Record a manager heartbeat for DC registration state tracking (AD-27).

        This updates the per-DC registration state to track which managers
        have sent heartbeats. DCs transition through registration states:
        - AWAITING_INITIAL → INITIALIZING (first heartbeat)
        - INITIALIZING → READY (quorum of managers)
        - READY → PARTIAL (below quorum)
        - PARTIAL → UNAVAILABLE (all stale)

        Args:
            dc_id: Datacenter ID
            manager_addr: Manager TCP address tuple
            node_id: Manager's node ID (for detecting restarts)
            generation: Manager's generation/version (for detecting restarts)
        """
        now = time.monotonic()

        # Ensure DC registration state exists (for dynamically discovered DCs)
        if dc_id not in self._dc_registration_states:
            self._dc_registration_states[dc_id] = DatacenterRegistrationState(
                dc_id=dc_id,
                configured_managers=[manager_addr],
            )
        else:
            # Add manager to configured list if not already present
            dc_state = self._dc_registration_states[dc_id]
            if manager_addr not in dc_state.configured_managers:
                dc_state.configured_managers.append(manager_addr)

        # Record the heartbeat
        dc_state = self._dc_registration_states[dc_id]
        is_restart = dc_state.record_heartbeat(manager_addr, node_id, generation, now)

        if is_restart:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Manager restart detected: {node_id} in DC {dc_id} (gen={generation})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _get_dc_registration_status(self, dc_id: str) -> DatacenterRegistrationStatus:
        """
        Get the current registration status for a datacenter.

        Returns AWAITING_INITIAL if DC is not in registration states.
        """
        if dc_id not in self._dc_registration_states:
            return DatacenterRegistrationStatus.AWAITING_INITIAL
        return self._dc_registration_states[dc_id].get_registration_status(time.monotonic())

    def _is_dc_ready_for_health_classification(self, dc_id: str) -> bool:
        """
        Check if a datacenter is ready for health classification.

        A DC is ready when it has achieved READY registration status,
        meaning a quorum of configured managers have sent heartbeats.
        """
        status = self._get_dc_registration_status(dc_id)
        return status in (
            DatacenterRegistrationStatus.READY,
            DatacenterRegistrationStatus.PARTIAL,
        )

    def _get_load_shedding_metrics(self) -> dict:
        """Get load shedding metrics for monitoring."""
        return {
            "overload_state": self._load_shedder.get_current_state().value,
            **self._load_shedder.get_metrics(),
        }

    # =========================================================================
    # Rate Limiting (AD-24)
    # =========================================================================

    async def _check_rate_limit(self, addr: tuple[str, int]) -> bool:
        """
        Check if a sender is within rate limits.

        Overrides base class to use ServerRateLimiter which provides
        per-client per-operation rate limiting with configurable limits.

        Args:
            addr: Source address tuple (host, port)

        Returns:
            True if allowed, False if rate limited
        """
        # Use the .check() compatibility method on ServerRateLimiter
        return self._rate_limiter.check(addr)

    def _check_rate_limit_for_operation(
        self,
        client_id: str,
        operation: str,
    ) -> tuple[bool, float]:
        """
        Check if a client request is within rate limits for a specific operation.

        Args:
            client_id: Client identifier (e.g., from address or auth)
            operation: Type of operation being performed

        Returns:
            Tuple of (allowed, retry_after_seconds)
        """
        result = self._rate_limiter.check_rate_limit(client_id, operation)
        return result.allowed, result.retry_after_seconds

    def _get_rate_limit_metrics(self) -> dict:
        """Get rate limiting metrics for monitoring."""
        return self._rate_limiter.get_metrics()

    def _cleanup_inactive_rate_limit_clients(self) -> int:
        """
        Cleanup rate limit buckets for inactive clients.

        Should be called periodically to prevent memory leaks.

        Returns:
            Number of clients cleaned up
        """
        return self._rate_limiter.cleanup_inactive_clients()

    def _get_available_datacenters(self) -> list[str]:
        """
        Get list of healthy datacenters (for backwards compatibility).
        
        A datacenter is healthy if:
        1. Its manager(s) are alive per SWIM UDP probes
        2. It has workers available (from TCP status updates)
        """
        healthy = []
        for dc_id in list(self._datacenter_managers.keys()):
            status = self._classify_datacenter_health(dc_id)
            if status.health != DatacenterHealth.UNHEALTHY.value:
                healthy.append(dc_id)
        return healthy
    
    def _select_datacenters_with_fallback(
        self,
        count: int,
        preferred: list[str] | None = None,
    ) -> tuple[list[str], list[str], str]:
        """
        Select datacenters with fallback list for resilient routing.

        Routing Rules (evaluated in order):
        - UNHEALTHY: Fallback to non-UNHEALTHY DC, else fail job with error
        - DEGRADED: Fallback to non-DEGRADED DC, else queue with warning
        - BUSY: Fallback to HEALTHY DC, else queue
        - HEALTHY: Enqueue (preferred)

        Args:
            count: Number of primary DCs to select
            preferred: Optional list of preferred DCs

        Returns:
            (primary_dcs, fallback_dcs, worst_health)
            worst_health indicates the worst state we had to accept:
            - "healthy": All selected DCs are healthy
            - "busy": Had to accept BUSY DCs (no HEALTHY available)
            - "degraded": Had to accept DEGRADED DCs (no HEALTHY/BUSY available)
            - "unhealthy": All registered DCs are unhealthy (job should fail)
            - "initializing": No DCs have completed registration yet (retry later)
        """
        # Classify all registered DCs (AD-27: only DCs with READY/PARTIAL status)
        dc_health = self._get_all_datacenter_health()

        # Check if we have any configured DCs that are still initializing
        # This distinguishes "no healthy DCs" from "DCs still starting up"
        configured_dc_count = len(self._datacenter_managers)
        registered_dc_count = len(dc_health)

        # Bucket by health
        healthy: list[tuple[str, DatacenterStatus]] = []
        busy: list[tuple[str, DatacenterStatus]] = []
        degraded: list[tuple[str, DatacenterStatus]] = []
        unhealthy_count = 0

        for dc_id, status in dc_health.items():
            if status.health == DatacenterHealth.HEALTHY.value:
                healthy.append((dc_id, status))
            elif status.health == DatacenterHealth.BUSY.value:
                busy.append((dc_id, status))
            elif status.health == DatacenterHealth.DEGRADED.value:
                degraded.append((dc_id, status))
            else:  # UNHEALTHY
                unhealthy_count += 1

        # Sort healthy by capacity (highest first)
        healthy.sort(key=lambda x: x[1].available_capacity, reverse=True)

        # Extract just DC IDs
        healthy_ids = [dc for dc, _ in healthy]
        busy_ids = [dc for dc, _ in busy]
        degraded_ids = [dc for dc, _ in degraded]

        # Respect preferences within healthy
        if preferred:
            preferred_healthy = [dc for dc in preferred if dc in healthy_ids]
            other_healthy = [dc for dc in healthy_ids if dc not in preferred]
            healthy_ids = preferred_healthy + other_healthy

        # Determine worst health we need to accept
        if healthy_ids:
            worst_health = "healthy"
        elif busy_ids:
            worst_health = "busy"
        elif degraded_ids:
            worst_health = "degraded"
        else:
            worst_health = "unhealthy"

        # Build selection: HEALTHY first, then BUSY, then DEGRADED
        all_usable = healthy_ids + busy_ids + degraded_ids

        if len(all_usable) == 0:
            # No usable DCs - determine why
            if registered_dc_count == 0 and configured_dc_count > 0:
                # DCs are configured but none have completed registration
                # This is a startup scenario - client should retry
                return ([], [], "initializing")
            # All registered DCs are UNHEALTHY - job should fail
            return ([], [], "unhealthy")

        # Primary = first `count` DCs
        primary = all_usable[:count]
        # Fallback = remaining usable DCs
        fallback = all_usable[count:]

        return (primary, fallback, worst_health)
    
    def _select_datacenters(
        self,
        count: int,
        preferred: list[str] | None = None,
    ) -> list[str]:
        """
        Select datacenters for job execution (backwards compatible).
        
        Uses cryptographically secure random selection for HEALTHY DCs,
        with fallback to BUSY and DEGRADED DCs.
        """
        primary, _, _ = self._select_datacenters_with_fallback(count, preferred)
        return primary
    
    async def _try_dispatch_to_manager(
        self,
        manager_addr: tuple[str, int],
        submission: JobSubmission,
        max_retries: int = 2,
        base_delay: float = 0.3,
    ) -> tuple[bool, str | None]:
        """
        Try to dispatch job to a single manager with retries.
        
        Uses retries with exponential backoff:
        - Attempt 1: immediate
        - Attempt 2: 0.3s delay
        - Attempt 3: 0.6s delay
        
        Args:
            manager_addr: (host, port) of the manager
            submission: Job submission to dispatch
            max_retries: Maximum retry attempts (default 2)
            base_delay: Base delay for exponential backoff (default 0.3s)
            
        Returns:
            (success: bool, error: str | None)
        """
        # Check circuit breaker first
        if self._is_manager_circuit_open(manager_addr):
            return (False, "Circuit breaker is OPEN")
        
        circuit = self._get_manager_circuit(manager_addr)
        
        for attempt in range(max_retries + 1):
            try:
                response, _ = await self.send_tcp(
                    manager_addr,
                    "job_submission",
                    submission.dump(),
                    timeout=5.0,
                )

                if isinstance(response, bytes):
                    ack = JobAck.load(response)
                    if ack.accepted:
                        circuit.record_success()
                        return (True, None)
                    # Check if it's a capacity issue vs unhealthy
                    if ack.error:
                        error_lower = ack.error.lower()
                        if "no capacity" in error_lower or "busy" in error_lower:
                            # BUSY is still acceptable - job will be queued
                            circuit.record_success()
                            return (True, None)
                    # Manager rejected - don't retry
                    circuit.record_error()
                    return (False, ack.error)

            except Exception as e:
                # Connection error - retry
                if attempt == max_retries:
                    circuit.record_error()
                    return (False, str(e))
            
            # Exponential backoff before retry
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
        
        # Should not reach here
        circuit.record_error()
        return (False, "Unknown error")
    
    async def _try_dispatch_to_dc(
        self,
        job_id: str,
        dc: str,
        submission: JobSubmission,
    ) -> tuple[bool, str | None, tuple[str, int] | None]:
        """
        Try to dispatch job to a single datacenter.

        Iterates through managers in the DC, using _try_dispatch_to_manager
        which handles retries and circuit breakers.

        Returns:
            (success: bool, error: str | None, accepting_manager: tuple[str, int] | None)
            - True if DC accepted (even if queued), with the accepting manager address
            - False only if DC is UNHEALTHY (should try fallback)
        """
        managers = self._datacenter_managers.get(dc, [])

        for manager_addr in managers:
            success, error = await self._try_dispatch_to_manager(
                manager_addr, submission
            )
            if success:
                # Return the accepting manager address for job leader tracking
                return (True, None, manager_addr)
            # Continue to next manager

        # All managers failed = DC is UNHEALTHY for this dispatch
        return (False, f"All managers in {dc} failed to accept job", None)
    
    async def _dispatch_job_with_fallback(
        self,
        submission: JobSubmission,
        primary_dcs: list[str],
        fallback_dcs: list[str],
    ) -> tuple[list[str], list[str]]:
        """
        Dispatch job to datacenters with automatic fallback.

        Priority: HEALTHY > BUSY > DEGRADED
        Only fails if ALL DCs are UNHEALTHY.

        Also records per-DC job leader (the manager that accepted the job)
        for routing queries to the authoritative manager.

        Args:
            submission: The job submission
            primary_dcs: Primary target DCs
            fallback_dcs: Fallback DCs to try if primary fails

        Returns:
            (successful_dcs, failed_dcs)
        """
        successful = []
        failed = []
        fallback_queue = list(fallback_dcs)
        job_id = submission.job_id

        # Initialize job DC managers tracking if needed
        if job_id not in self._job_dc_managers:
            self._job_dc_managers[job_id] = {}

        for dc in primary_dcs:
            success, error, accepting_manager = await self._try_dispatch_to_dc(
                job_id, dc, submission
            )

            if success:
                successful.append(dc)
                # Record the accepting manager as job leader for this DC
                if accepting_manager:
                    self._job_dc_managers[job_id][dc] = accepting_manager
            else:
                # Try fallback
                fallback_success = False
                while fallback_queue:
                    fallback_dc = fallback_queue.pop(0)
                    fb_success, fb_error, fb_manager = await self._try_dispatch_to_dc(
                        job_id, fallback_dc, submission
                    )
                    if fb_success:
                        successful.append(fallback_dc)
                        # Record the accepting manager as job leader for fallback DC
                        if fb_manager:
                            self._job_dc_managers[job_id][fallback_dc] = fb_manager
                        fallback_success = True
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerInfo(
                                message=f"Job {job_id}: Fallback from {dc} to {fallback_dc}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )
                        break

                if not fallback_success:
                    # No fallback worked
                    failed.append(dc)

        return (successful, failed)
    
    # =========================================================================
    # Tiered Update Strategy (AD-15)
    # =========================================================================
    
    def _classify_update_tier(
        self,
        job_id: str,
        old_status: str | None,
        new_status: str,
    ) -> str:
        """
        Classify which tier an update belongs to.
        
        Tier 1 (Immediate): Job completion, failure, critical alerts
        Tier 2 (Periodic): Workflow progress, aggregate rates
        Tier 3 (On-Demand): Step-level stats, historical data
        
        Returns UpdateTier value.
        """
        # Critical state transitions = Immediate
        if new_status in (JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value):
            return UpdateTier.IMMEDIATE.value
        
        # New job start = Immediate
        if old_status is None and new_status == JobStatus.RUNNING.value:
            return UpdateTier.IMMEDIATE.value
        
        # Status transitions = Immediate
        if old_status != new_status:
            return UpdateTier.IMMEDIATE.value
        
        # Regular progress updates = Periodic (batched)
        return UpdateTier.PERIODIC.value
    
    async def _send_immediate_update(
        self,
        job_id: str,
        event_type: str,
        payload: bytes | None = None,
    ) -> None:
        """
        Send a Tier 1 (Immediate) update to subscribed clients.
        
        Used for critical events that clients need to know about immediately:
        - Job completion
        - Job failure
        - Critical alerts
        
        If client provided a callback_addr at submission time, pushes
        JobStatusPush to that address via TCP.
        """
        job = self._jobs.get(job_id)
        if not job:
            return
        
        callback = self._job_callbacks.get(job_id)
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Job {job_id}: Immediate update - {event_type}" +
                        (f" (pushing to {callback})" if callback else " (no callback)"),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Push to client if callback is registered
        if callback:
            is_final = job.status in (
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
            )
            
            # Build per-DC stats for granular visibility
            per_dc_stats = [
                DCStats(
                    datacenter=dc_prog.datacenter,
                    status=dc_prog.status,
                    completed=dc_prog.total_completed,
                    failed=dc_prog.total_failed,
                    rate=dc_prog.overall_rate,
                )
                for dc_prog in job.datacenters
            ]
            
            push = JobStatusPush(
                job_id=job_id,
                status=job.status,
                message=event_type,
                total_completed=job.total_completed,
                total_failed=job.total_failed,
                overall_rate=job.overall_rate,
                elapsed_seconds=job.elapsed_seconds,
                is_final=is_final,
                per_dc_stats=per_dc_stats,
            )
            
            try:
                await self.send_tcp(
                    callback,
                    "job_status_push",
                    push.dump(),
                    timeout=2.0,
                )
            except Exception:
                # Client unreachable - don't block on this
                pass
            
            # Clean up callbacks and windowed stats if job is final
            if is_final:
                # Flush any remaining windowed stats before cleanup
                final_pushes = await self._windowed_stats.flush_job_windows(
                    job_id,
                    aggregate=True,  # Gate always aggregates for clients
                )
                for push in final_pushes:
                    await self._push_windowed_stats_to_client(push)

                self._job_callbacks.pop(job_id, None)
                self._progress_callbacks.pop(job_id, None)
    
    async def _batch_stats_update(self) -> None:
        """
        Process a batch of Tier 2 (Periodic) updates.
        
        Aggregates pending progress updates and pushes to clients
        that have registered callbacks. This is more efficient than
        sending each update individually.
        """
        # Collect running jobs with callbacks
        jobs_with_callbacks = []
        for job_id, job in list(self._jobs.items()):
            if job.status == JobStatus.RUNNING.value:
                callback = self._job_callbacks.get(job_id)
                if callback:
                    jobs_with_callbacks.append((job_id, job, callback))
        
        if not jobs_with_callbacks:
            return
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Batch stats update: pushing to {len(jobs_with_callbacks)} clients",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Push batched stats to each client
        for job_id, job, callback in jobs_with_callbacks:
            # Aggregate step stats from all DC progress
            all_step_stats = []
            for dc_progress in job.datacenters:
                if hasattr(dc_progress, 'step_stats') and dc_progress.step_stats:
                    all_step_stats.extend(dc_progress.step_stats)
            
            # Build per-DC stats for granular visibility
            per_dc_stats = [
                DCStats(
                    datacenter=dc_prog.datacenter,
                    status=dc_prog.status,
                    completed=dc_prog.total_completed,
                    failed=dc_prog.total_failed,
                    rate=dc_prog.overall_rate,
                )
                for dc_prog in job.datacenters
            ]
            
            batch_push = JobBatchPush(
                job_id=job_id,
                status=job.status,
                step_stats=all_step_stats,
                total_completed=job.total_completed,
                total_failed=job.total_failed,
                overall_rate=job.overall_rate,
                elapsed_seconds=job.elapsed_seconds,
                per_dc_stats=per_dc_stats,
            )
            
            try:
                await self.send_tcp(
                    callback,
                    "job_batch_push",
                    batch_push.dump(),
                    timeout=2.0,
                )
            except Exception:
                # Client unreachable - continue with others
                pass
    
    async def _batch_stats_loop(self) -> None:
        """
        Background loop for Tier 2 (Periodic) updates.
        
        Runs every 1-5 seconds (configurable) to batch and send progress updates.
        This reduces network overhead compared to sending each update immediately.
        """
        batch_interval = self._batch_stats_interval
        
        while self._running:
            try:
                await asyncio.sleep(batch_interval)
                if not self._running:
                    break
                await self._batch_stats_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Log but continue
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Batch stats loop error: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                await asyncio.sleep(batch_interval)
    
    def _handle_update_by_tier(
        self,
        job_id: str,
        old_status: str | None,
        new_status: str,
        progress_data: bytes | None = None,
    ) -> None:
        """
        Route an update through the appropriate tier.
        
        Tier 1 → immediate TCP push
        Tier 2 → batched periodic update
        Tier 3 → stored for on-demand retrieval
        """
        tier = self._classify_update_tier(job_id, old_status, new_status)
        
        if tier == UpdateTier.IMMEDIATE.value:
            self._task_runner.run(
                self._send_immediate_update,
                job_id,
                f"status:{old_status}->{new_status}",
                progress_data,
            )
        # Tier 2 and 3 are handled by batch loop and on-demand requests
    
    # =========================================================================
    # Gate State and Quorum Management
    # =========================================================================
    
    def _quorum_size(self) -> int:
        """
        Calculate required quorum size for gate operations.
        
        Quorum = (total_gates // 2) + 1 (simple majority)
        
        Returns at least 1 for single-gate deployments.
        """
        total_gates = len(self._active_gate_peers) + 1  # Include self
        return (total_gates // 2) + 1
    
    def _has_quorum_available(self) -> bool:
        """
        Check if we have enough active gates to achieve quorum.
        
        Returns True if:
        1. This gate is ACTIVE (SYNCING gates don't participate in quorum)
        2. The number of active gates (including self) >= required quorum size
        """
        # SYNCING gates don't participate in quorum operations
        if self._gate_state != GateState.ACTIVE:
            return False
        
        active_count = len(self._active_gate_peers) + 1  # Include self
        return active_count >= self._quorum_size()
    
    def get_quorum_status(self) -> dict:
        """
        Get current quorum and circuit breaker status.
        
        Returns a dict with:
        - active_gates: Number of active gates
        - required_quorum: Quorum size needed
        - quorum_available: Whether quorum is achievable
        - circuit_state: Current circuit breaker state
        - circuit_failures: Recent failure count
        - circuit_error_rate: Error rate over window
        - gate_state: Current gate state (syncing/active/draining)
        """
        active_count = len(self._active_gate_peers) + 1
        required_quorum = self._quorum_size()
        
        return {
            "active_gates": active_count,
            "required_quorum": required_quorum,
            "quorum_available": self._has_quorum_available(),
            "circuit_state": self._quorum_circuit.circuit_state.name,
            "circuit_failures": self._quorum_circuit.error_count,
            "circuit_error_rate": self._quorum_circuit.error_rate,
            "gate_state": self._gate_state.value,
        }
    
    async def _complete_startup_sync(self) -> None:
        """
        Complete the startup state sync and transition to ACTIVE.
        
        If this gate is the leader, it becomes ACTIVE immediately.
        
        If not leader, requests state sync from the current leader,
        then transitions to ACTIVE.
        """
        if self.is_leader():
            # Leader becomes ACTIVE immediately
            self._gate_state = GateState.ACTIVE
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message="Gate is LEADER, transitioning to ACTIVE state",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return
        
        # Not leader - request state sync from leader
        leader_addr = self.get_current_leader()
        
        if leader_addr:
            # Find TCP address for leader (UDP -> TCP mapping)
            leader_tcp_addr = self._gate_udp_to_tcp.get(leader_addr)
            
            if leader_tcp_addr:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Gate is SYNCING, requesting state from leader {leader_tcp_addr}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                
                # Request state sync with retry
                sync_success = await self._sync_state_from_gate_peer(leader_tcp_addr)
                
                if sync_success:
                    self._gate_state = GateState.ACTIVE
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message="Gate synced state from leader, transitioning to ACTIVE",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                else:
                    # Sync failed but we can still become active
                    # (We'll get state updates via SWIM and progress reports)
                    self._gate_state = GateState.ACTIVE
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message="Gate sync from leader failed, becoming ACTIVE anyway (will sync via updates)",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
            else:
                # No TCP address for leader - become active anyway
                self._gate_state = GateState.ACTIVE
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"No TCP address for leader {leader_addr}, becoming ACTIVE",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
        else:
            # No leader yet - become active (we might be the first gate)
            self._gate_state = GateState.ACTIVE
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message="No leader elected yet, becoming ACTIVE",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
    
    async def _sync_state_from_gate_peer(
        self,
        peer_tcp_addr: tuple[str, int],
    ) -> bool:
        """
        Request and apply state snapshot from a peer gate.
        
        Uses exponential backoff for retries.
        
        Returns True if sync succeeded, False otherwise.
        """
        max_retries = 3
        base_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_role=NodeRole.GATE.value,
                    since_version=self._state_version,
                )
                
                result, _ = await self.send_tcp(
                    peer_tcp_addr,
                    "state_sync",
                    request.dump(),
                    timeout=5.0,
                )
                
                if isinstance(result, bytes) and len(result) > 0:
                    response = StateSyncResponse.load(result)
                    if response.success and response.snapshot:
                        snapshot = GateStateSnapshot.load(response.snapshot)
                        await self._apply_gate_state_snapshot(snapshot)
                        return True
                        
            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"State sync attempt {attempt + 1} failed: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            
            # Exponential backoff
            delay = base_delay * (2 ** attempt)
            await asyncio.sleep(delay)
        
        return False
    
    async def _apply_gate_state_snapshot(
        self,
        snapshot: GateStateSnapshot,
    ) -> None:
        """
        Apply a state snapshot received from a peer gate.
        
        Merges job state and manager discovery that we don't already have.
        """
        # Merge jobs we don't have
        for job_id, job_status in snapshot.jobs.items():
            if job_id not in self._jobs:
                self._jobs[job_id] = job_status
        
        # Merge manager discovery - add any managers we don't know about
        new_managers_count = 0
        for dc, manager_addrs in snapshot.datacenter_managers.items():
            if dc not in self._datacenter_managers:
                self._datacenter_managers[dc] = []
            for addr in manager_addrs:
                # Convert list to tuple if needed
                addr_tuple = tuple(addr) if isinstance(addr, list) else addr
                if addr_tuple not in self._datacenter_managers[dc]:
                    self._datacenter_managers[dc].append(addr_tuple)
                    new_managers_count += 1
        
        # Merge manager UDP addresses
        for dc, udp_addrs in snapshot.datacenter_manager_udp.items():
            if dc not in self._datacenter_manager_udp:
                self._datacenter_manager_udp[dc] = []
            for addr in udp_addrs:
                addr_tuple = tuple(addr) if isinstance(addr, list) else addr
                if addr_tuple not in self._datacenter_manager_udp[dc]:
                    self._datacenter_manager_udp[dc].append(addr_tuple)

        # Merge per-job leadership tracking via tracker
        # Uses fencing tokens for proper consistency
        self._job_leadership_tracker.merge_from_snapshot(
            job_leaders=snapshot.job_leaders,
            job_leader_addrs=snapshot.job_leader_addrs,
            job_fencing_tokens=snapshot.job_fencing_tokens,
        )

        # Merge per-job per-DC manager leaders
        for job_id, dc_managers in snapshot.job_dc_managers.items():
            if job_id not in self._job_dc_managers:
                self._job_dc_managers[job_id] = dict(dc_managers)
            else:
                # Merge DC managers we don't already have
                for dc_id, manager_addr in dc_managers.items():
                    if dc_id not in self._job_dc_managers[job_id]:
                        self._job_dc_managers[job_id][dc_id] = manager_addr

        # Update state version if snapshot is newer
        if snapshot.version > self._state_version:
            self._state_version = snapshot.version

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Applied state snapshot from {snapshot.node_id}: {len(snapshot.jobs)} jobs, {new_managers_count} new managers",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def start(self) -> None:
        """
        Start the gate server.
        
        New Gate Join Process:
        1. Start TCP/UDP server
        2. Join SWIM cluster with other gates
        3. Start probe cycle
        4. Start leader election
        5. Complete startup sync and transition to ACTIVE
        
        SYNCING gates are NOT counted in quorum.
        """
        # Start the underlying server (TCP/UDP listeners, task runner, etc.)
        # Uses SWIM settings from Env configuration
        await self.start_server(init_context=self.env.get_swim_init_context())

        # Now that node_id is available, initialize the job leadership tracker
        self._job_leadership_tracker.node_id = self._node_id.full
        self._job_leadership_tracker.node_addr = (self._host, self._tcp_port)

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate starting in SYNCING state (not in quorum yet)",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Join SWIM cluster with other gates (UDP healthchecks)
        for peer_udp in self._gate_udp_peers:
            await self.join_cluster(peer_udp)
        
        # NOTE: Managers are NOT added to gate's SWIM probe scheduler.
        # Managers are in their own SWIM cluster (per-datacenter).
        # Gate-to-manager health is monitored via FederatedHealthMonitor (xprobe/xack).
        
        # Start SWIM probe cycle (UDP healthchecks for gates only)
        self._task_runner.run(self.start_probe_cycle)
        
        # Start leader election (uses SWIM membership info)
        await self.start_leader_election()
        
        # Wait a short time for leader election to stabilize
        await asyncio.sleep(0.5)
        
        # Sync state and transition to ACTIVE
        await self._complete_startup_sync()
        
        # Initialize and start Federated Health Monitor for DC leader probing
        self._dc_health_monitor.set_callbacks(
            send_udp=self._send_xprobe,
            cluster_id=f"gate-{self._node_id.datacenter}",
            node_id=self._node_id.full,
            on_dc_health_change=self._on_dc_health_change,
            on_dc_latency=self._on_dc_latency,
        )
        
        # Add known DC leaders to monitor (will be updated via TCP registrations)
        for dc, manager_udp_addrs in list(self._datacenter_manager_udp.items()):
            if manager_udp_addrs:
                # Start with first known manager - will update when leader is discovered
                self._dc_health_monitor.add_datacenter(dc, manager_udp_addrs[0])
        
        await self._dc_health_monitor.start()
        
        # Start background cleanup tasks via TaskRunner
        self._task_runner.run(self._lease_cleanup_loop)
        self._task_runner.run(self._job_cleanup_loop)
        self._task_runner.run(self._rate_limit_cleanup_loop)

        # Start Tier 2 (periodic) batch stats loop
        self._task_runner.run(self._batch_stats_loop)

        # Start windowed stats push loop for streaming progress to clients
        self._task_runner.run(self._windowed_stats_push_loop)

        # Start discovery maintenance loop (AD-28)
        self._discovery_maintenance_task = asyncio.create_task(self._discovery_maintenance_loop())

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate started with {len(self._datacenter_managers)} configured DCs, " +
                        f"state={self._gate_state.value}, SWIM healthcheck active, " +
                        f"federated DC monitoring active",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def stop(
        self,
        drain_timeout: float = 5,
        broadcast_leave: bool = True
    ) -> None:
        """Stop the gate server."""
        # Set _running to False early to stop all background loops
        self._running = False

        # Cancel discovery maintenance loop (AD-28)
        if self._discovery_maintenance_task and not self._discovery_maintenance_task.done():
            self._discovery_maintenance_task.cancel()
            try:
                await self._discovery_maintenance_task
            except asyncio.CancelledError:
                pass

        # Stop federated health monitor
        await self._dc_health_monitor.stop()

        await super().stop(
            drain_timeout=drain_timeout,
            broadcast_leave=broadcast_leave,
        )
    
    async def _send_xprobe(self, target: tuple[str, int], data: bytes) -> bool:
        """
        Send a cross-cluster probe to a DC leader.
        
        Used by FederatedHealthMonitor for DC health checking.
        """
        try:
            await self.send(target, data, timeout=5)
            return True
        except Exception:
            return False
    
    def _on_dc_health_change(self, datacenter: str, new_health: str) -> None:
        """
        Called when a datacenter's health status changes.

        Logs the change and updates internal tracking.
        Uses cross-DC correlation detection to prevent cascade evictions
        when multiple DCs fail simultaneously (likely network issue).
        """
        # Register DC with correlation detector if not known
        self._cross_dc_correlation.add_datacenter(datacenter)

        # Record failure or recovery with correlation detector
        if new_health in ("unhealthy", "degraded"):
            # Count affected managers for this DC
            manager_count = len(self._datacenter_managers.get(datacenter, []))
            self._cross_dc_correlation.record_failure(
                datacenter_id=datacenter,
                failure_type=new_health,
                manager_count_affected=manager_count,
            )

            # Check for correlated failures before taking action
            correlation = self._cross_dc_correlation.check_correlation(datacenter)

            if correlation.should_delay_eviction:
                # High/medium correlation - likely network issue, don't evict
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=(
                            f"DC {datacenter} health changed to {new_health}, "
                            f"but CORRELATION DETECTED ({correlation.severity.value}): "
                            f"{correlation.reason}. Affected DCs: {correlation.affected_datacenters}. "
                            f"Recommendation: {correlation.recommendation}"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            elif correlation.severity == CorrelationSeverity.LOW:
                # Low correlation - proceed cautiously with warning
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=(
                            f"DC {datacenter} health changed to {new_health} "
                            f"(low correlation with {len(correlation.affected_datacenters)} other DCs)"
                        ),
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            else:
                # No correlation - normal health change handling
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"DC {datacenter} health changed to {new_health}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
        else:
            # DC recovered (healthy or busy)
            self._cross_dc_correlation.record_recovery(datacenter)
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"DC {datacenter} health changed to {new_health}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _on_dc_latency(self, datacenter: str, latency_ms: float) -> None:
        """
        Called when a latency measurement is received from a DC probe.

        Records latency for cross-DC correlation detection (Phase 7).
        High latency across multiple DCs indicates network degradation
        rather than individual DC failures.

        Args:
            datacenter: The datacenter that was probed.
            latency_ms: Round-trip latency in milliseconds.
        """
        self._cross_dc_correlation.record_latency(
            datacenter_id=datacenter,
            latency_ms=latency_ms,
            probe_type="federated",
        )

    def _record_peer_gate_latency(self, gate_id: str, latency_ms: float) -> None:
        """
        Record latency measurement from a peer gate healthcheck.

        Used to detect network degradation within the gate cluster.
        High latency to all peers indicates network issues vs specific
        gate failures.

        Args:
            gate_id: The peer gate's node ID.
            latency_ms: Round-trip latency in milliseconds.
        """
        now = time.monotonic()
        if gate_id not in self._peer_gate_latency_samples:
            self._peer_gate_latency_samples[gate_id] = []

        samples = self._peer_gate_latency_samples[gate_id]
        samples.append((now, latency_ms))

        # Prune old samples
        cutoff = now - self._latency_sample_max_age
        self._peer_gate_latency_samples[gate_id] = [
            (ts, lat) for ts, lat in samples
            if ts > cutoff
        ][-self._latency_sample_max_count:]

    def get_average_peer_gate_latency(self) -> float | None:
        """
        Get average latency to peer gates.

        Returns None if no samples available.
        """
        all_latencies = [
            lat for samples in self._peer_gate_latency_samples.values()
            for _, lat in samples
        ]
        if not all_latencies:
            return None
        return sum(all_latencies) / len(all_latencies)

    def get_peer_gate_latency(self, gate_id: str) -> float | None:
        """
        Get average latency to a specific peer gate.

        Args:
            gate_id: The peer gate's node ID.

        Returns None if no samples available.
        """
        samples = self._peer_gate_latency_samples.get(gate_id)
        if not samples:
            return None
        return sum(lat for _, lat in samples) / len(samples)

    async def _handle_xack_response(
        self,
        source_addr: tuple[str, int] | bytes,
        ack_data: bytes,
    ) -> None:
        """
        Handle a cross-cluster health acknowledgment from a DC leader.

        Passes the ack to the FederatedHealthMonitor for processing.
        """
        try:
            ack = CrossClusterAck.load(ack_data)
            self._dc_health_monitor.handle_ack(ack)
            
            # Also update DC leader info if this is a leader response
            if ack.is_leader:
                addr = source_addr if isinstance(source_addr, tuple) else None
                if addr:
                    self._dc_health_monitor.update_leader(
                        datacenter=ack.datacenter,
                        leader_udp_addr=addr,
                        leader_node_id=ack.node_id,
                        leader_term=ack.leader_term,
                    )
        except Exception as e:
            await self.handle_exception(e, "handle_xack_response")
    
    async def _build_xprobe_response(
        self,
        source_addr: tuple[str, int] | bytes,
        probe_data: bytes,
    ) -> bytes | None:
        """
        Build response to cross-cluster health probe from a manager.
        
        Returns aggregate gate cluster health for the manager to track.
        Only responds if we are the gate cluster leader.
        """
        # Only gate cluster leader responds to xprobes
        if not self.is_leader():
            return None
        
        # Get gate cluster health metrics
        nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        cluster_size = 1  # Self
        healthy_gates = 1  # Self
        
        if nodes:
            for node_addr, data in nodes.items():
                if node_addr != self_addr:
                    cluster_size += 1
                    if isinstance(data, tuple) and len(data) >= 2:
                        _, status = data[:2]
                        if status == b'OK':
                            healthy_gates += 1
        
        # Count tracked DCs and their managers
        dc_count = len(self._datacenter_manager_status)
        total_managers = sum(
            len(managers) for managers in self._datacenter_manager_status.values()
        )
        
        # Count active jobs
        active_jobs = len(self._jobs)
        
        # Determine gate cluster health
        gate_health = "HEALTHY"
        if healthy_gates < (cluster_size / 2):
            gate_health = "DEGRADED"
        
        ack = CrossClusterAck(
            datacenter="gate-cluster",
            node_id=self._node_id.full,
            incarnation=self._state_version,  # Use state version as incarnation
            is_leader=True,
            leader_term=self._leader_election.state.current_term,
            cluster_size=cluster_size,
            healthy_managers=healthy_gates,  # For gates, this is healthy_gates
            worker_count=dc_count,  # Reuse field: number of DCs tracked
            healthy_workers=total_managers,  # Reuse field: total managers tracked
            total_cores=0,  # N/A for gates
            available_cores=0,  # N/A for gates
            active_jobs=active_jobs,
            active_workflows=0,  # N/A for gates
            dc_health=gate_health,
        )
        
        return ack.dump()
    
    async def _lease_cleanup_loop(self) -> None:
        """Periodically clean up expired leases."""
        while self._running:
            try:
                await asyncio.sleep(self._lease_timeout / 2)
                
                now = time.monotonic()
                expired = []
                for key, lease in list(self._leases.items()):
                    if lease.expires_at < now:
                        expired.append(key)
                
                for key in expired:
                    self._leases.pop(key, None)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.handle_exception(e, "lease_cleanup_loop")
    
    async def _job_cleanup_loop(self) -> None:
        """
        Periodically clean up completed/failed jobs.
        
        Removes jobs that have been in a terminal state for longer than _job_max_age.
        """
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

                for job_id, job in list(self._jobs.items()):
                    if job.status in terminal_states:
                        # Check age - use elapsed_seconds as relative timestamp
                        # or timestamp if available
                        age = now - getattr(job, 'timestamp', now)
                        if age > self._job_max_age:
                            jobs_to_remove.append(job_id)
                
                for job_id in jobs_to_remove:
                    self._jobs.pop(job_id, None)
                    # Also clean up related tracking dicts
                    self._job_fence_tokens.pop(job_id, None)
                    self._job_dc_results.pop(job_id, None)
                    self._workflow_dc_results.pop(job_id, None)
                    self._job_target_dcs.pop(job_id, None)
                    self._job_workflow_ids.pop(job_id, None)
                    self._job_callbacks.pop(job_id, None)
                    self._progress_callbacks.pop(job_id, None)
                    # Clean up per-job leadership tracking
                    self._job_leadership_tracker.release_leadership(job_id)
                    self._job_dc_managers.pop(job_id, None)
                    # Flush and clean up windowed stats for this job
                    final_pushes = await self._windowed_stats.flush_job_windows(
                        job_id,
                        aggregate=True,
                    )
                    for push in final_pushes:
                        await self._push_windowed_stats_to_client(push)
                    # Clean up reporter tasks and submissions
                    self._cleanup_reporter_tasks(job_id)
                    # Clean up any leases for this job
                    lease_keys_to_remove = [
                        key for key in self._leases
                        if key.startswith(f"{job_id}:")
                    ]
                    for key in lease_keys_to_remove:
                        self._leases.pop(key, None)
                
                if jobs_to_remove:
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"Cleaned up {len(jobs_to_remove)} completed jobs",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.handle_exception(e, "job_cleanup_loop")

    async def _rate_limit_cleanup_loop(self) -> None:
        """
        Periodically clean up inactive clients from the rate limiter.

        Removes token buckets for clients that haven't made requests
        within the inactive_cleanup_seconds window to prevent memory leaks.
        """
        while self._running:
            try:
                await asyncio.sleep(self._rate_limit_cleanup_interval)

                cleaned = self._cleanup_inactive_rate_limit_clients()

                if cleaned > 0:
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerDebug(
                            message=f"Rate limiter: cleaned up {cleaned} inactive clients",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.handle_exception(e, "rate_limit_cleanup_loop")

    def _create_lease(self, job_id: str, datacenter: str) -> DatacenterLease:
        """Create a new lease for a job in a datacenter."""
        lease = DatacenterLease(
            job_id=job_id,
            datacenter=datacenter,
            lease_holder=self._node_id.full,
            fence_token=self._get_fence_token(),
            expires_at=time.monotonic() + self._lease_timeout,
            version=self._state_version,
        )
        self._leases[f"{job_id}:{datacenter}"] = lease
        return lease
    
    def _get_lease(self, job_id: str, datacenter: str) -> DatacenterLease | None:
        """Get existing lease if valid."""
        key = f"{job_id}:{datacenter}"
        lease = self._leases.get(key)
        if lease and lease.expires_at > time.monotonic():
            return lease
        return None
    
    async def _dispatch_job_to_datacenter(
        self,
        job_id: str,
        datacenter: str,
        submission: JobSubmission,
    ) -> bool:
        """
        Dispatch a job to a datacenter with lease.
        
        Returns True on success, False on failure.
        """
        # Get or create lease
        lease = self._get_lease(job_id, datacenter)
        if not lease:
            lease = self._create_lease(job_id, datacenter)
        
        # Get manager addresses for this DC
        managers = self._datacenter_managers.get(datacenter, [])
        if not managers:
            return False
        
        # Try each manager until one accepts
        for manager_addr in managers:
            try:
                response, _ = await self.send_tcp(
                    manager_addr,
                    "job_submission",
                    submission.dump(),
                    timeout=5.0,
                )
                
                if isinstance(response, bytes):
                    ack = JobAck.load(response)
                    if ack.accepted:
                        return True
                    # If not leader, try another
                    
            except Exception as e:
                await self.handle_exception(e, f"dispatch_to_dc_{datacenter}")
        
        return False
    
    async def _gather_job_status(self, job_id: str) -> GlobalJobStatus:
        """Gather and aggregate job status from all DCs."""
        job = self._jobs.get(job_id)
        if not job:
            return GlobalJobStatus(
                job_id=job_id,
                status=JobStatus.FAILED.value,
            )
        
        # Request status from each DC with active workflows
        dc_progress = []
        for dc in self._get_available_datacenters():
            managers = self._datacenter_managers.get(dc, [])
            if not managers:
                continue
            
            # Try first available manager
            for manager_addr in managers:
                try:
                    response, _ = await self.send_tcp(
                        manager_addr,
                        "job_status_request",
                        job_id.encode(),
                        timeout=2.0,
                    )
                    
                    if isinstance(response, bytes) and response:
                        progress = JobProgress.load(response)
                        dc_progress.append(progress)
                        break
                        
                except Exception:
                    continue
        
        # Aggregate
        job.datacenters = dc_progress
        job.total_completed = sum(p.total_completed for p in dc_progress)
        job.total_failed = sum(p.total_failed for p in dc_progress)
        job.overall_rate = sum(p.overall_rate for p in dc_progress)
        job.completed_datacenters = sum(
            1 for p in dc_progress if p.status == JobStatus.COMPLETED.value
        )
        job.failed_datacenters = sum(
            1 for p in dc_progress if p.status == JobStatus.FAILED.value
        )
        job.timestamp = time.monotonic()
        
        # Determine overall status
        if job.failed_datacenters > 0 and job.completed_datacenters == 0:
            job.status = JobStatus.FAILED.value
        elif job.completed_datacenters == len(dc_progress):
            job.status = JobStatus.COMPLETED.value
        else:
            job.status = JobStatus.RUNNING.value
        
        return job
    
    # =========================================================================
    # TCP Handlers - Manager Status Updates (NOT healthchecks)
    # =========================================================================
    
    @tcp.send('manager_status_ack')
    async def send_manager_status_ack(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send manager status ack."""
        return (addr, data, timeout)
    
    @tcp.handle('manager_status_ack')
    async def handle_manager_status_ack_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw manager status ack."""
        return data
    
    @tcp.receive()
    async def manager_status_update(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle manager status update via TCP.

        This is NOT a healthcheck - DC liveness is tracked via per-manager heartbeat freshness.
        This contains job progress and worker capacity information.

        Stored per-datacenter, per-manager to enable proper aggregation.

        Also updates DC registration state for registration status tracking (AD-27).
        """
        try:
            status = ManagerHeartbeat.load(data)

            # Store per-datacenter, per-manager using manager's self-reported address
            # (TCP source addr is ephemeral, not the manager's listening address)
            dc = status.datacenter
            manager_addr = (status.tcp_host, status.tcp_port)

            if dc not in self._datacenter_manager_status:
                self._datacenter_manager_status[dc] = {}
            self._datacenter_manager_status[dc][manager_addr] = status
            self._manager_last_status[manager_addr] = time.monotonic()

            # Update DC registration state (AD-27)
            # Use version as generation proxy - detects restarts via node_id change
            self._record_manager_heartbeat(dc, manager_addr, status.node_id, status.version)

            return b'ok'

        except Exception as e:
            await self.handle_exception(e, "manager_status_update")
            return b'error'
    
    @tcp.receive()
    async def manager_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle manager registration.

        Managers register with gates at startup to discover all healthy gates.
        This is analogous to Workers registering with Managers.

        Protocol Negotiation (AD-25):
        - Extracts manager's protocol version and capabilities from heartbeat
        - Performs capability negotiation
        - Returns negotiated capabilities in response
        - Rejects registration if protocol versions are incompatible
        """
        try:
            heartbeat = ManagerHeartbeat.load(data)

            # Store per-datacenter, per-manager using manager's self-reported address
            dc = heartbeat.datacenter
            manager_addr = (heartbeat.tcp_host, heartbeat.tcp_port)

            # Protocol version negotiation (AD-25)
            manager_version = ProtocolVersion(
                major=getattr(heartbeat, 'protocol_version_major', 1),
                minor=getattr(heartbeat, 'protocol_version_minor', 0),
            )
            manager_caps_str = getattr(heartbeat, 'capabilities', '')
            manager_capabilities = set(manager_caps_str.split(',')) if manager_caps_str else set()

            manager_node_caps = NodeCapabilities(
                protocol_version=manager_version,
                capabilities=manager_capabilities,
                node_version=heartbeat.node_id,
            )

            # Negotiate capabilities
            negotiated = negotiate_capabilities(self._node_capabilities, manager_node_caps)

            if not negotiated.compatible:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Manager registration rejected: incompatible protocol version "
                                f"{manager_version} (we are {CURRENT_PROTOCOL_VERSION})",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                response = ManagerRegistrationResponse(
                    accepted=False,
                    gate_id=self._node_id.full,
                    healthy_gates=[],
                    error=f"Incompatible protocol version: {manager_version} vs {CURRENT_PROTOCOL_VERSION}",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                )
                return response.dump()

            # Store negotiated capabilities for this manager
            self._manager_negotiated_caps[manager_addr] = negotiated

            if dc not in self._datacenter_manager_status:
                self._datacenter_manager_status[dc] = {}
            self._datacenter_manager_status[dc][manager_addr] = heartbeat
            self._manager_last_status[manager_addr] = time.monotonic()

            # Add manager address to datacenter managers (if not already tracked)
            if dc not in self._datacenter_managers:
                self._datacenter_managers[dc] = []
            if manager_addr not in self._datacenter_managers[dc]:
                self._datacenter_managers[dc].append(manager_addr)

            # Update DC registration state (AD-27)
            # Use version as generation proxy - detects restarts via node_id change
            self._record_manager_heartbeat(dc, manager_addr, heartbeat.node_id, heartbeat.version)

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Manager registered: {heartbeat.node_id} from DC {dc} "
                            f"({heartbeat.worker_count} workers, protocol {manager_version}, "
                            f"{len(negotiated.common_features)} features)",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Return ack with all healthy gates and negotiated capabilities
            negotiated_caps_str = ','.join(sorted(negotiated.common_features))
            response = ManagerRegistrationResponse(
                accepted=True,
                gate_id=self._node_id.full,
                healthy_gates=self._get_healthy_gates(),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            )

            # Broadcast this manager discovery to peer gates (include status info)
            self._task_runner.run(
                self._broadcast_manager_discovery,
                dc,
                manager_addr,
                None,  # manager_udp_addr not available from heartbeat
                heartbeat.worker_count,
                getattr(heartbeat, 'healthy_worker_count', heartbeat.worker_count),
                heartbeat.available_cores,
                getattr(heartbeat, 'total_cores', 0),
            )

            return response.dump()
            
        except Exception as e:
            await self.handle_exception(e, "manager_register")
            response = ManagerRegistrationResponse(
                accepted=False,
                gate_id=self._node_id.full,
                healthy_gates=[],
                error=str(e),
            )
            return response.dump()
    
    @tcp.receive()
    async def manager_discovery(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle manager discovery broadcast from a peer gate.
        
        When another gate receives a manager registration, it broadcasts
        to all peers. This handler adds the manager to our tracking and
        updates datacenter status from the included manager heartbeat info.
        """
        try:
            broadcast = ManagerDiscoveryBroadcast.load(data)
            
            dc = broadcast.datacenter
            manager_addr = tuple(broadcast.manager_tcp_addr)
            
            # Add manager if not already tracked
            if dc not in self._datacenter_managers:
                self._datacenter_managers[dc] = []
            
            if manager_addr not in self._datacenter_managers[dc]:
                self._datacenter_managers[dc].append(manager_addr)
                
                # Also add UDP address if provided
                if broadcast.manager_udp_addr:
                    if dc not in self._datacenter_manager_udp:
                        self._datacenter_manager_udp[dc] = []
                    udp_addr = tuple(broadcast.manager_udp_addr)
                    if udp_addr not in self._datacenter_manager_udp[dc]:
                        self._datacenter_manager_udp[dc].append(udp_addr)
                
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Discovered manager {manager_addr} in DC {dc} via gate {broadcast.source_gate_id}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            
            # Store per-datacenter, per-manager status
            # Create a synthetic ManagerHeartbeat for the discovered manager
            if dc not in self._datacenter_manager_status:
                self._datacenter_manager_status[dc] = {}
            
            synthetic_heartbeat = ManagerHeartbeat(
                node_id=f"discovered-via-{broadcast.source_gate_id}",
                datacenter=dc,
                is_leader=False,  # Unknown from broadcast
                term=0,
                version=0,
                active_jobs=0,
                active_workflows=0,
                worker_count=broadcast.worker_count,
                healthy_worker_count=broadcast.healthy_worker_count,
                available_cores=broadcast.available_cores,
                total_cores=broadcast.total_cores,
                state="active",
            )
            self._datacenter_manager_status[dc][manager_addr] = synthetic_heartbeat
            self._manager_last_status[manager_addr] = time.monotonic()
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "manager_discovery")
            return b'error'
    
    # =========================================================================
    # TCP Handlers - Job Submission (from Client)
    # =========================================================================
    
    @tcp.send('job_ack')
    async def send_job_ack(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send job ack."""
        return (addr, data, timeout)
    
    @tcp.handle('job_ack')
    async def handle_job_ack_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw job ack."""
        return data
    
    @tcp.receive()
    async def job_submission(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job submission from client.

        Any gate can accept a job and become its leader. Per-job leadership
        is independent of SWIM cluster leadership - each job has exactly one
        leader gate that handles aggregation and client communication.
        """
        try:
            # Check rate limit first (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit_for_operation(client_id, "job_submit")
            if not allowed:
                return RateLimitResponse(
                    operation="job_submit",
                    retry_after_seconds=retry_after,
                ).dump()

            # Backpressure/load shedding check (AD-22)
            # Reject new job submissions when system is overloaded
            if self._should_shed_request("JobSubmission"):
                overload_state = self._load_shedder.get_current_state()
                return JobAck(
                    job_id="",  # No job_id yet
                    accepted=False,
                    error=f"System under load ({overload_state.value}), please retry later",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            submission = JobSubmission.load(data)

            # Protocol version negotiation (AD-25)
            client_version = ProtocolVersion(
                major=getattr(submission, 'protocol_version_major', 1),
                minor=getattr(submission, 'protocol_version_minor', 0),
            )

            # Check version compatibility - reject if major version differs
            if client_version.major != CURRENT_PROTOCOL_VERSION.major:
                ack = JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Incompatible protocol version: {client_version} (requires major version {CURRENT_PROTOCOL_VERSION.major})",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                )
                return ack.dump()

            # Negotiate capabilities
            client_caps_str = getattr(submission, 'capabilities', '')
            client_features = set(client_caps_str.split(',')) if client_caps_str else set()
            our_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
            negotiated_features = client_features & our_features
            negotiated_caps_str = ','.join(sorted(negotiated_features))

            # Check quorum circuit breaker (fail-fast)
            if self._quorum_circuit.circuit_state == CircuitState.OPEN:
                # Calculate retry_after from half_open_after setting
                retry_after = self._quorum_circuit.half_open_after
                raise QuorumCircuitOpenError(
                    recent_failures=self._quorum_circuit.error_count,
                    window_seconds=self._quorum_circuit.window_seconds,
                    retry_after_seconds=retry_after,
                )
            
            # Check if quorum is available (multi-gate deployments)
            if len(self._active_gate_peers) > 0 and not self._has_quorum_available():
                active_gates = len(self._active_gate_peers) + 1  # +1 for self
                raise QuorumUnavailableError(
                    active_managers=active_gates,  # Using same field name for consistency
                    required_quorum=self._quorum_size(),
                )
            
            # Select datacenters with fallback support
            primary_dcs, fallback_dcs, worst_health = self._select_datacenters_with_fallback(
                submission.datacenter_count,
                submission.datacenters if submission.datacenters else None,
            )

            # If DCs are still initializing (no manager heartbeats yet), return retryable error
            if worst_health == "initializing":
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Job {submission.job_id}: Datacenters still initializing - client should retry",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                ack = JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error="initializing",  # Client will retry
                )
                return ack.dump()

            # Use primary_dcs as target_dcs
            target_dcs = primary_dcs

            if not target_dcs:
                # All DCs are unhealthy (not initializing, actually unhealthy)
                ack = JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error="No available datacenters - all unhealthy",
                )
                return ack.dump()
            
            # Create global job tracking
            job = GlobalJobStatus(
                job_id=submission.job_id,
                status=JobStatus.SUBMITTED.value,
                datacenters=[],
                timestamp=time.monotonic(),
            )
            self._jobs[submission.job_id] = job
            
            # Track which DCs this job targets (for completion detection)
            self._job_target_dcs[submission.job_id] = set(target_dcs)

            # Extract and track workflow IDs from submission (client-generated)
            # Format: list[tuple[str, list[str], Workflow]] - (workflow_id, dependencies, workflow)
            try:
                workflows: list[tuple[str, list[str], object]] = cloudpickle.loads(submission.workflows)
                workflow_ids = {wf_id for wf_id, _, _ in workflows}
                self._job_workflow_ids[submission.job_id] = workflow_ids
            except Exception:
                # If unpickling fails, we can still proceed but won't have workflow ID tracking
                self._job_workflow_ids[submission.job_id] = set()

            # Store callback for push notifications (if provided)
            if submission.callback_addr:
                self._job_callbacks[submission.job_id] = submission.callback_addr
                # Also register for progress updates (same address, different message type)
                self._progress_callbacks[submission.job_id] = submission.callback_addr

            # Store submission for reporter configs access after aggregation
            if submission.reporting_configs:
                self._job_submissions[submission.job_id] = submission

            # Set this gate as job leader (first to accept = job leader)
            # Per-job leadership is independent of SWIM cluster leadership
            self._job_leadership_tracker.assume_leadership(
                job_id=submission.job_id,
                metadata=len(target_dcs),  # Store target_dc_count as metadata
            )

            self._increment_version()

            # Broadcast job leadership to peer gates
            await self._broadcast_job_leadership(
                submission.job_id,
                len(target_dcs),
            )

            # Record success for circuit breaker
            self._quorum_circuit.record_success()

            # Dispatch to each DC (in background via TaskRunner)
            self._task_runner.run(
                self._dispatch_job_to_datacenters, submission, target_dcs
            )
            
            ack = JobAck(
                job_id=submission.job_id,
                accepted=True,
                queued_position=len(self._jobs),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            )
            return ack.dump()

        except QuorumCircuitOpenError as e:
            # Circuit already open - don't record another error (would extend open state)
            ack = JobAck(
                job_id=submission.job_id if 'submission' in dir() else "unknown",
                accepted=False,
                error=str(e),
            )
            return ack.dump()
        except QuorumError as e:
            # Record error for circuit breaker (QuorumUnavailableError, etc.)
            self._quorum_circuit.record_error()
            ack = JobAck(
                job_id=submission.job_id if 'submission' in dir() else "unknown",
                accepted=False,
                error=str(e),
            )
            return ack.dump()
        except Exception as e:
            await self.handle_exception(e, "job_submission")
            ack = JobAck(
                job_id="unknown",
                accepted=False,
                error=str(e),
            )
            return ack.dump()
    
    async def _dispatch_job_to_datacenters(
        self,
        submission: JobSubmission,
        target_dcs: list[str],
    ) -> None:
        """
        Dispatch job to all target datacenters with fallback support.

        Uses _select_datacenters_with_fallback to get primary and fallback DCs,
        then uses _dispatch_job_with_fallback for resilient dispatch.

        Routing Rules:
        - UNHEALTHY: Fallback to non-UNHEALTHY DC, else fail job with error
        - DEGRADED: Fallback to non-DEGRADED DC, else queue with warning
        - BUSY: Fallback to HEALTHY DC, else queue
        - HEALTHY: Enqueue (preferred)

        Direct DC-to-Job-Leader Routing:
        - Sets origin_gate_addr so managers send results directly to this gate
        - This gate is the job leader for this job
        """
        job = self._jobs.get(submission.job_id)
        if not job:
            return

        # Set origin gate address for direct DC-to-Job-Leader routing
        # Managers will send JobFinalResult/JobProgress directly to this gate
        submission.origin_gate_addr = (self._host, self._tcp_port)

        job.status = JobStatus.DISPATCHING.value
        self._increment_version()
        
        # Get primary and fallback DCs based on health classification
        # Note: "initializing" case is normally handled in job_submission before this method is called.
        # However, if DC state changes between job acceptance and dispatch, we handle it here too.
        primary_dcs, fallback_dcs, worst_health = self._select_datacenters_with_fallback(
            len(target_dcs),
            target_dcs if target_dcs else None,
        )

        # If DCs regressed to initializing (rare race condition), mark job pending
        if worst_health == "initializing":
            job.status = JobStatus.PENDING.value
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Job {submission.job_id}: DCs became initializing after acceptance (race) - waiting",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            # Don't fail - the job was accepted, we'll retry dispatch when DCs are ready
            return

        # If ALL DCs are UNHEALTHY, fail immediately
        if worst_health == "unhealthy":
            job.status = JobStatus.FAILED.value
            job.failed_datacenters = len(target_dcs)
            self._quorum_circuit.record_error()
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Job {submission.job_id}: All datacenters are UNHEALTHY - job failed",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            self._increment_version()
            return
        
        # Log warning if we had to accept DEGRADED DCs
        if worst_health == "degraded":
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Job {submission.job_id}: No HEALTHY or BUSY DCs available, "
                            f"routing to DEGRADED DCs: {primary_dcs}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        elif worst_health == "busy":
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Job {submission.job_id}: No HEALTHY DCs available, "
                            f"routing to BUSY DCs: {primary_dcs}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        
        # Dispatch with fallback support
        successful_dcs, failed_dcs = await self._dispatch_job_with_fallback(
            submission,
            primary_dcs,
            fallback_dcs,
        )
        
        if not successful_dcs:
            # All DCs failed (all UNHEALTHY) - record for circuit breaker
            self._quorum_circuit.record_error()
            job.status = JobStatus.FAILED.value
            job.failed_datacenters = len(failed_dcs)
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Job {submission.job_id}: Failed to dispatch to any datacenter",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        else:
            # Successful dispatch - record success for circuit breaker
            self._quorum_circuit.record_success()
            job.status = JobStatus.RUNNING.value
            job.completed_datacenters = 0
            job.failed_datacenters = len(failed_dcs)
            
            if failed_dcs:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Job {submission.job_id}: Dispatched to {len(successful_dcs)} DCs, "
                                f"{len(failed_dcs)} DCs failed (all UNHEALTHY)",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
        
        self._increment_version()
    
    # =========================================================================
    # TCP Handlers - Job Status (for Client)
    # =========================================================================
    
    @tcp.send('job_status')
    async def send_job_status(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send job status."""
        return (addr, data, timeout)
    
    @tcp.handle('job_status')
    async def handle_job_status_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw job status."""
        return data
    
    @tcp.receive()
    async def receive_job_status_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job status request from client."""
        start_time = time.monotonic()
        try:
            # Rate limit check (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit_for_operation(client_id, "job_status")
            if not allowed:
                return RateLimitResponse(
                    operation="job_status",
                    retry_after_seconds=retry_after,
                ).dump()

            # Load shedding check (AD-22)
            if self._should_shed_request("JobStatusRequest"):
                return b''  # Shed request under load

            job_id = data.decode()
            status = await self._gather_job_status(job_id)
            return status.dump()

        except Exception as e:
            await self.handle_exception(e, "receive_job_status_request")
            return b''
        finally:
            latency_ms = (time.monotonic() - start_time) * 1000
            self._record_request_latency(latency_ms)
    
    # =========================================================================
    # TCP Handlers - Job Progress (from Manager)
    # =========================================================================
    
    @tcp.receive()
    async def receive_job_progress(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle job progress update from manager.

        Uses tiered update strategy (AD-15):
        - Tier 1 (Immediate): Critical state changes → push immediately
        - Tier 2 (Periodic): Regular progress → batched

        Validates fence tokens to reject stale updates from old job owners.

        Forwarding: If we don't own this job (not in _jobs), forward to peer gates
        since we may have received this due to stale origin_gate_addr in manager.
        """
        start_time = time.monotonic()
        try:
            # Load shedding check (AD-22) - JobProgress is NORMAL priority
            if self._should_shed_request("JobProgress"):
                # Return minimal ack even when shedding to prevent retries
                ack = JobProgressAck(
                    gate_id=self._node_id.full,
                    is_leader=self.is_leader(),
                    healthy_gates=self._get_healthy_gates(),
                )
                return ack.dump()

            progress = JobProgress.load(data)

            # Check if we own this job - if not, forward to peers
            if progress.job_id not in self._jobs:
                # We don't own this job - forward to peer gates
                forwarded = await self._forward_job_progress_to_peers(progress)
                if forwarded:
                    # Still return ack with topology info
                    ack = JobProgressAck(
                        gate_id=self._node_id.full,
                        is_leader=self.is_leader(),
                        healthy_gates=self._get_healthy_gates(),
                    )
                    return ack.dump()
                # No peers to forward to - continue processing locally

            # Validate fence token - reject stale updates
            current_fence = self._job_fence_tokens.get(progress.job_id, 0)
            if progress.fence_token < current_fence:
                # Stale update from old owner - reject silently
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerDebug(
                        message=f"Rejecting stale job progress for {progress.job_id}: "
                                f"fence_token {progress.fence_token} < {current_fence}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                # Still return ack to avoid retries
                ack = JobProgressAck(
                    gate_id=self._node_id.full,
                    is_leader=self.is_leader(),
                    healthy_gates=self._get_healthy_gates(),
                )
                return ack.dump()

            # Update fence token if higher
            if progress.fence_token > current_fence:
                self._job_fence_tokens[progress.job_id] = progress.fence_token

            job = self._jobs.get(progress.job_id)
            if job:
                old_status = job.status

                # Update DC progress
                for i, dc_prog in enumerate(job.datacenters):
                    if dc_prog.datacenter == progress.datacenter:
                        job.datacenters[i] = progress
                        break
                else:
                    job.datacenters.append(progress)

                # Recalculate aggregates
                job.total_completed = sum(p.total_completed for p in job.datacenters)
                job.total_failed = sum(p.total_failed for p in job.datacenters)
                job.overall_rate = sum(p.overall_rate for p in job.datacenters)
                job.timestamp = time.monotonic()

                # Check if all DCs are done to update job status
                completed_dcs = sum(
                    1 for p in job.datacenters
                    if p.status in (JobStatus.COMPLETED.value, JobStatus.FAILED.value)
                )
                if completed_dcs == len(job.datacenters):
                    failed_dcs = sum(
                        1 for p in job.datacenters
                        if p.status == JobStatus.FAILED.value
                    )
                    if failed_dcs > 0:
                        job.status = JobStatus.FAILED.value
                    else:
                        job.status = JobStatus.COMPLETED.value
                    job.completed_datacenters = len(job.datacenters) - failed_dcs
                    job.failed_datacenters = failed_dcs

                # Route through tiered update strategy
                self._handle_update_by_tier(
                    progress.job_id,
                    old_status,
                    job.status,
                    data,
                )

                self._increment_version()

            # Return ack with current gate topology for manager to update
            ack = JobProgressAck(
                gate_id=self._node_id.full,
                is_leader=self.is_leader(),
                healthy_gates=self._get_healthy_gates(),
            )
            return ack.dump()

        except Exception as e:
            await self.handle_exception(e, "receive_job_progress")
            return b'error'
        finally:
            latency_ms = (time.monotonic() - start_time) * 1000
            self._record_request_latency(latency_ms)

    # =========================================================================
    # TCP Handlers - Cancellation (AD-20)
    # =========================================================================

    @tcp.receive()
    async def receive_cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle job cancellation from client (AD-20).

        Supports both legacy CancelJob and new JobCancelRequest formats.
        Uses retry logic with exponential backoff when forwarding to managers.
        """
        try:
            # Rate limit check (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit_for_operation(client_id, "cancel")
            if not allowed:
                return RateLimitResponse(
                    operation="cancel",
                    retry_after_seconds=retry_after,
                ).dump()

            # Try to parse as JobCancelRequest first (AD-20), fall back to CancelJob
            try:
                cancel_request = JobCancelRequest.load(data)
                job_id = cancel_request.job_id
                fence_token = cancel_request.fence_token
                requester_id = cancel_request.requester_id
                reason = cancel_request.reason
                use_ad20_response = True
            except Exception:
                # Fall back to legacy CancelJob format
                cancel = CancelJob.load(data)
                job_id = cancel.job_id
                fence_token = cancel.fence_token
                requester_id = f"{addr[0]}:{addr[1]}"
                reason = cancel.reason
                use_ad20_response = False

            job = self._jobs.get(job_id)
            if not job:
                if use_ad20_response:
                    return JobCancelResponse(
                        job_id=job_id,
                        success=False,
                        error="Job not found",
                    ).dump()
                else:
                    return CancelAck(
                        job_id=job_id,
                        cancelled=False,
                        error="Job not found",
                    ).dump()

            # Check fence token if provided (prevents cancelling restarted jobs)
            if fence_token > 0 and hasattr(job, 'fence_token'):
                if job.fence_token != fence_token:
                    error_msg = f"Fence token mismatch: expected {job.fence_token}, got {fence_token}"
                    if use_ad20_response:
                        return JobCancelResponse(
                            job_id=job_id,
                            success=False,
                            error=error_msg,
                        ).dump()
                    else:
                        return CancelAck(
                            job_id=job_id,
                            cancelled=False,
                            error=error_msg,
                        ).dump()

            # Check if already cancelled (idempotency)
            if job.status == JobStatus.CANCELLED.value:
                if use_ad20_response:
                    return JobCancelResponse(
                        job_id=job_id,
                        success=True,
                        already_cancelled=True,
                        cancelled_workflow_count=0,
                    ).dump()
                else:
                    return CancelAck(
                        job_id=job_id,
                        cancelled=True,
                        workflows_cancelled=0,
                    ).dump()

            # Check if already completed (cannot cancel)
            if job.status == JobStatus.COMPLETED.value:
                if use_ad20_response:
                    return JobCancelResponse(
                        job_id=job_id,
                        success=False,
                        already_completed=True,
                        error="Job already completed",
                    ).dump()
                else:
                    return CancelAck(
                        job_id=job_id,
                        cancelled=False,
                        error="Job already completed",
                    ).dump()

            # Create retry executor with exponential backoff for DC communication
            retry_config = RetryConfig(
                max_attempts=3,
                base_delay=0.5,
                max_delay=5.0,
                jitter=JitterStrategy.FULL,
                retryable_exceptions=(ConnectionError, TimeoutError, OSError),
            )

            # Cancel in all DCs with retry logic
            cancelled_workflows = 0
            errors: list[str] = []

            for dc in self._get_available_datacenters():
                managers = self._datacenter_managers.get(dc, [])
                dc_cancelled = False

                for manager_addr in managers:
                    if dc_cancelled:
                        break

                    # Use RetryExecutor for reliable DC communication
                    retry_executor = RetryExecutor(retry_config)

                    async def send_cancel_to_manager():
                        # Build the cancel request for the manager
                        if use_ad20_response:
                            cancel_data = JobCancelRequest(
                                job_id=job_id,
                                requester_id=requester_id,
                                timestamp=cancel_request.timestamp,
                                fence_token=fence_token,
                                reason=reason,
                            ).dump()
                        else:
                            cancel_data = CancelJob(
                                job_id=job_id,
                                reason=reason,
                                fence_token=fence_token,
                            ).dump()

                        response, _ = await self.send_tcp(
                            manager_addr,
                            "cancel_job",
                            cancel_data,
                            timeout=5.0,
                        )
                        return response

                    try:
                        response = await retry_executor.execute(
                            send_cancel_to_manager,
                            operation_name=f"cancel_job_dc_{dc}",
                        )

                        if isinstance(response, bytes):
                            # Try parsing as AD-20 response first
                            try:
                                dc_response = JobCancelResponse.load(response)
                                cancelled_workflows += dc_response.cancelled_workflow_count
                                dc_cancelled = True
                            except Exception:
                                # Fall back to legacy format
                                dc_ack = CancelAck.load(response)
                                cancelled_workflows += dc_ack.workflows_cancelled
                                dc_cancelled = True
                    except Exception as e:
                        errors.append(f"DC {dc}: {str(e)}")
                        continue

            # Update job status
            job.status = JobStatus.CANCELLED.value
            self._increment_version()

            # Build response
            if use_ad20_response:
                return JobCancelResponse(
                    job_id=job_id,
                    success=True,
                    cancelled_workflow_count=cancelled_workflows,
                    error="; ".join(errors) if errors else None,
                ).dump()
            else:
                return CancelAck(
                    job_id=job_id,
                    cancelled=True,
                    workflows_cancelled=cancelled_workflows,
                ).dump()

        except Exception as e:
            await self.handle_exception(e, "receive_cancel_job")
            # Return error in appropriate format
            try:
                # Try to parse to determine format
                JobCancelRequest.load(data)
                return JobCancelResponse(
                    job_id="unknown",
                    success=False,
                    error=str(e),
                ).dump()
            except Exception:
                return CancelAck(
                    job_id="unknown",
                    cancelled=False,
                    error=str(e),
                ).dump()

    @tcp.receive()
    async def receive_job_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle job cancellation completion push from manager (AD-20).

        Managers push this notification after all workflows in a job have
        reported cancellation completion. The gate:
        1. Records any errors from failed cancellations
        2. Fires the completion event for await_job_cancellation callers
        3. Pushes notification to the client callback if registered
        """
        try:
            completion = JobCancellationComplete.load(data)
            job_id = completion.job_id

            await self._udp_logger.log(
                ServerInfo(
                    message=f"Received job cancellation complete for {job_id[:8]}... "
                            f"(success={completion.success}, errors={len(completion.errors)})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Store errors for await_job_cancellation
            if completion.errors:
                self._cancellation_errors[job_id].extend(completion.errors)

            # Fire completion event
            event = self._cancellation_completion_events.get(job_id)
            if event:
                event.set()

            # Push notification to client callback if registered
            callback = self._job_callbacks.get(job_id)
            if callback:
                self._task_runner.run(
                    self._push_cancellation_complete_to_client,
                    job_id,
                    completion,
                    callback,
                )

            return b"OK"

        except Exception as e:
            await self.handle_exception(e, "receive_job_cancellation_complete")
            return b"ERROR"

    async def _push_cancellation_complete_to_client(
        self,
        job_id: str,
        completion: JobCancellationComplete,
        callback: tuple[str, int],
    ) -> None:
        """Push job cancellation completion to client callback."""
        try:
            await self.send_tcp(
                callback,
                "receive_job_cancellation_complete",
                completion.dump(),
                timeout=2.0,
            )
        except Exception as e:
            await self._udp_logger.log(
                ServerError(
                    message=f"Failed to push cancellation complete to client {callback}: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        # Cleanup tracking after push
        self._cancellation_completion_events.pop(job_id, None)
        self._cancellation_errors.pop(job_id, None)

    # =========================================================================
    # TCP Handlers - Lease Transfer (for Gate Scaling)
    # =========================================================================
    
    @tcp.send('lease_transfer_ack')
    async def send_lease_transfer_ack(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send lease transfer ack."""
        return (addr, data, timeout)
    
    @tcp.handle('lease_transfer_ack')
    async def handle_lease_transfer_ack_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw lease transfer ack."""
        return data
    
    @tcp.receive()
    async def receive_lease_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle lease transfer during gate scaling."""
        try:
            transfer = LeaseTransfer.load(data)
            
            # Accept the lease
            lease = DatacenterLease(
                job_id=transfer.job_id,
                datacenter=transfer.datacenter,
                lease_holder=transfer.to_gate,
                fence_token=transfer.new_fence_token,
                expires_at=time.monotonic() + self._lease_timeout,
                version=transfer.version,
            )
            self._leases[f"{transfer.job_id}:{transfer.datacenter}"] = lease
            self._increment_version()
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "receive_lease_transfer")
            return b'error'
    
    # =========================================================================
    # TCP Handlers - State Sync (between Gates)
    # =========================================================================
    
    @tcp.send('gate_state_sync_response')
    async def send_gate_state_sync_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send state sync response."""
        return (addr, data, timeout)
    
    @tcp.handle('gate_state_sync_response')
    async def handle_gate_state_sync_response_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw state sync response."""
        return data
    
    @tcp.receive()
    async def receive_gate_state_sync_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle state sync request from another gate (usually new leader).

        Returns this gate's complete state snapshot for merging.
        Only returns full state if this gate is ACTIVE. If still SYNCING,
        returns responder_ready=False to indicate the requester should retry.
        """
        try:
            request = StateSyncRequest.load(data)

            # Only serve state if we're ACTIVE (completed our own startup)
            is_ready = self._gate_state == GateState.ACTIVE

            response = StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=self._state_version,
                responder_ready=is_ready,
                # Only include state if we're ready
                gate_state=self._get_state_snapshot() if is_ready else None,
            )
            return response.dump()

        except Exception as e:
            await self.handle_exception(e, "receive_gate_state_sync_request")
            return b''
    
    # =========================================================================
    # Job Final Result Handling (Manager -> Gate -> Client)
    # =========================================================================
    
    @tcp.receive()
    async def job_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle final result from a manager for a datacenter.

        Aggregates results from all DCs and sends GlobalJobResult to client.
        Validates fence tokens to reject stale results from old job owners.

        Forwarding: If we don't own this job (not in _jobs), forward to peer gates
        since we may have received this due to stale origin_gate_addr in manager.
        """
        try:
            result = JobFinalResult.load(data)

            # Check if we own this job - if not, forward to peers
            if result.job_id not in self._jobs:
                # We don't own this job - forward to peer gates
                forwarded = await self._forward_job_result_to_peers(result)
                if forwarded:
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerDebug(
                            message=f"Forwarded job final result for {result.job_id} to peer gates",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                    return b'ok'
                # No peers to forward to, or we're the leader - process locally
                # This can happen during startup or single-gate deployments

            # Validate fence token - reject stale results
            current_fence = self._job_fence_tokens.get(result.job_id, 0)
            if result.fence_token < current_fence:
                # Stale result from old owner - reject silently
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerDebug(
                        message=f"Rejecting stale job final result for {result.job_id}: "
                                f"fence_token {result.fence_token} < {current_fence}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return b'ok'  # Ack to avoid retries

            # Update fence token if higher
            if result.fence_token > current_fence:
                self._job_fence_tokens[result.job_id] = result.fence_token

            self._task_runner.run(
                self._udp_logger.log,
                ServerDebug(
                    message=f"Received job final result for {result.job_id} from DC {result.datacenter}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Store per-DC result
            if result.job_id not in self._job_dc_results:
                self._job_dc_results[result.job_id] = {}
            self._job_dc_results[result.job_id][result.datacenter] = result

            # Check if we have results from all target DCs
            target_dcs = self._job_target_dcs.get(result.job_id, set())
            received_dcs = set(self._job_dc_results.get(result.job_id, {}).keys())

            if target_dcs and received_dcs >= target_dcs:
                # All DCs reported - aggregate and send to client
                await self._send_global_job_result(result.job_id)

            return b'ok'

        except Exception as e:
            await self.handle_exception(e, "job_final_result")
            return b'error'

    @tcp.receive()
    async def workflow_result_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle workflow result push from manager.

        Managers send raw per-core WorkflowStats for each completed workflow.
        Gate aggregates results from all DCs using Results.merge_results()
        and forwards to client.
        """
        try:
            push = WorkflowResultPush.load(data)

            # Check if we own this job
            if push.job_id not in self._jobs:
                # Forward to peer gates
                await self._forward_workflow_result_to_peers(push)
                return b'ok'

            self._task_runner.run(
                self._udp_logger.log,
                ServerDebug(
                    message=f"Received workflow result for {push.job_id}:{push.workflow_id} from DC {push.datacenter}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Store per-DC workflow result
            if push.job_id not in self._workflow_dc_results:
                self._workflow_dc_results[push.job_id] = {}
            if push.workflow_id not in self._workflow_dc_results[push.job_id]:
                self._workflow_dc_results[push.job_id][push.workflow_id] = {}
            self._workflow_dc_results[push.job_id][push.workflow_id][push.datacenter] = push

            # Check if we have results from all target DCs for this workflow
            target_dcs = self._job_target_dcs.get(push.job_id, set())
            received_dcs = set(self._workflow_dc_results[push.job_id][push.workflow_id].keys())

            if target_dcs and received_dcs >= target_dcs:
                # All DCs reported for this workflow - aggregate and send to client
                await self._aggregate_and_forward_workflow_result(push.job_id, push.workflow_id)

            return b'ok'

        except Exception as e:
            await self.handle_exception(e, "workflow_result_push")
            return b'error'

    async def _aggregate_and_forward_workflow_result(
        self,
        job_id: str,
        workflow_id: str,
    ) -> None:
        """
        Aggregate workflow results from all DCs and forward to client.

        For test workflows: Uses Results.merge_results() to combine all WorkflowStats.
        For non-test workflows: Returns per-DC raw results without aggregation.
        Includes per-DC breakdown for client visibility.
        """
        workflow_results = self._workflow_dc_results.get(job_id, {}).get(workflow_id, {})
        if not workflow_results:
            return

        # Determine if this is a test workflow from any DC push (all should match)
        first_dc_push = next(iter(workflow_results.values()))
        is_test_workflow = first_dc_push.is_test

        # Collect all WorkflowStats from all DCs and build per-DC results
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
                # Test workflow: aggregate this DC's results for per-DC breakdown
                dc_aggregated_stats: WorkflowStats | None = None
                if dc_push.results:
                    if len(dc_push.results) > 1:
                        aggregator = Results()
                        dc_aggregated_stats = aggregator.merge_results(dc_push.results)
                    else:
                        dc_aggregated_stats = dc_push.results[0]

                # Build per-DC result entry with aggregated stats
                per_dc_results.append(WorkflowDCResult(
                    datacenter=datacenter,
                    status=dc_push.status,
                    stats=dc_aggregated_stats,
                    error=dc_push.error,
                    elapsed_seconds=dc_push.elapsed_seconds,
                ))
            else:
                # Non-test workflow: include raw results list per DC
                per_dc_results.append(WorkflowDCResult(
                    datacenter=datacenter,
                    status=dc_push.status,
                    stats=None,  # No aggregated stats for non-test workflows
                    error=dc_push.error,
                    elapsed_seconds=dc_push.elapsed_seconds,
                    raw_results=dc_push.results,  # Raw unaggregated results
                ))

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
            # Test workflow: aggregate cross-DC using Results.merge_results()
            aggregator = Results()
            if len(all_workflow_stats) > 1:
                aggregated = aggregator.merge_results(all_workflow_stats)
            else:
                aggregated = all_workflow_stats[0]
            results_to_send = [aggregated]
        else:
            # Non-test workflow: return all raw stats without aggregation
            results_to_send = all_workflow_stats

        # Build push for client with per-DC breakdown
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

        # Send to client
        callback = self._job_callbacks.get(job_id)
        if callback:
            try:
                await self.send_tcp(
                    callback,
                    "workflow_result_push",
                    client_push.dump(),
                    timeout=5.0,
                )
            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Failed to send workflow result to client {callback}: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

        # Clean up this workflow's DC results
        if job_id in self._workflow_dc_results:
            self._workflow_dc_results[job_id].pop(workflow_id, None)

    async def _forward_workflow_result_to_peers(self, push: WorkflowResultPush) -> bool:
        """Forward workflow result to peer gates that may own the job."""
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
            except Exception:
                continue
        return False

    async def _forward_job_result_to_peers(self, result: JobFinalResult) -> bool:
        """
        Forward a job final result to peer gates that may own the job.

        Returns True if forwarded to at least one peer.
        """
        forwarded = False
        for gate_id, gate_info in list(self._known_gates.items()):
            if gate_id == self._node_id.full:
                continue  # Don't forward to self
            try:
                gate_addr = (gate_info.tcp_host, gate_info.tcp_port)
                await self.send_tcp(
                    gate_addr,
                    "job_final_result",
                    result.dump(),
                    timeout=3.0,
                )
                forwarded = True
                break  # Only forward to one peer, they'll handle routing
            except Exception:
                continue  # Try next peer
        return forwarded

    async def _forward_job_progress_to_peers(self, progress: JobProgress) -> bool:
        """
        Forward job progress to peer gates that may own the job.

        Returns True if forwarded to at least one peer.
        """
        forwarded = False
        for gate_id, gate_info in list(self._known_gates.items()):
            if gate_id == self._node_id.full:
                continue  # Don't forward to self
            try:
                gate_addr = (gate_info.tcp_host, gate_info.tcp_port)
                await self.send_tcp(
                    gate_addr,
                    "job_progress",
                    progress.dump(),
                    timeout=2.0,
                )
                forwarded = True
                break  # Only forward to one peer, they'll handle routing
            except Exception:
                continue  # Try next peer
        return forwarded
    
    async def _send_global_job_result(self, job_id: str) -> None:
        """
        Aggregate DC results and send GlobalJobResult to client.
        
        Uses Results.merge_results() to properly aggregate WorkflowStats
        from all datacenters, including timing percentiles (p50, p95, p99).
        """
        dc_results = self._job_dc_results.get(job_id, {})
        if not dc_results:
            return
        
        # Aggregate across DCs
        all_dc_results = list(dc_results.values())
        total_completed = sum(r.total_completed for r in all_dc_results)
        total_failed = sum(r.total_failed for r in all_dc_results)
        all_errors: list[str] = []
        max_elapsed = 0.0
        successful_dcs = 0
        failed_dcs = 0
        
        for dc_result in all_dc_results:
            all_errors.extend(dc_result.errors)
            if dc_result.elapsed_seconds > max_elapsed:
                max_elapsed = dc_result.elapsed_seconds
            if dc_result.status == JobStatus.COMPLETED.value:
                successful_dcs += 1
            else:
                failed_dcs += 1
        
        # Determine overall status
        if failed_dcs == 0:
            overall_status = JobStatus.COMPLETED.value
        elif successful_dcs == 0:
            overall_status = JobStatus.FAILED.value
        else:
            overall_status = "PARTIAL"
        
        # =================================================================
        # Aggregate WorkflowStats using Results.merge_results()
        # =================================================================

        # 1. Collect all WorkflowStats from all DCs, grouped by workflow name
        # Manager sends list[WorkflowStats] (raw per-core results from all workers)
        all_workflow_stats: dict[str, list[WorkflowStats]] = defaultdict(list)

        for dc_result in all_dc_results:
            for wf_result in dc_result.workflow_results:
                # wf_result.results is list[WorkflowStats] - extend to flatten all per-core stats
                all_workflow_stats[wf_result.workflow_name].extend(wf_result.results)

        # 2. Merge WorkflowStats per workflow using Results.merge_results()
        merged_workflow_stats: list[WorkflowStats] = []
        aggregator = Results()

        for workflow_name, stats_list in all_workflow_stats.items():
            if len(stats_list) > 1:
                # Multiple workers/DCs ran this workflow - merge their stats
                merged = aggregator.merge_results(stats_list)
            elif len(stats_list) == 1:
                merged = stats_list[0]
            else:
                continue
            merged_workflow_stats.append(merged)
        
        # 3. Extract aggregated latency stats from merged results
        avg_latencies: list[float] = []
        p50_latencies: list[float] = []
        p95_latencies: list[float] = []
        p99_latencies: list[float] = []
        total_aps: float = 0.0
        
        for ws in merged_workflow_stats:
            # Accumulate actions per second
            total_aps += ws.get("aps", 0.0)
            
            # Extract timing stats from test results
            for result_set in ws.get("results", []):
                timings = result_set.get("timings", {})
                total_timing = timings.get("total", {})
                
                if total_timing:
                    if "mean" in total_timing:
                        avg_latencies.append(total_timing["mean"])
                    if "med" in total_timing:
                        p50_latencies.append(total_timing["med"])
                    if "95th_quantile" in total_timing:
                        p95_latencies.append(total_timing["95th_quantile"])
                    if "99th_quantile" in total_timing:
                        p99_latencies.append(total_timing["99th_quantile"])
        
        # 4. Calculate aggregated latencies (median of medians for percentiles)
        avg_latency_ms = statistics.mean(avg_latencies) * 1000 if avg_latencies else 0.0
        p50_latency_ms = statistics.median(p50_latencies) * 1000 if p50_latencies else 0.0
        p95_latency_ms = statistics.median(p95_latencies) * 1000 if p95_latencies else 0.0
        p99_latency_ms = statistics.median(p99_latencies) * 1000 if p99_latencies else 0.0
        
        # Ensure percentiles are monotonically increasing (p50 <= p95 <= p99)
        # If any percentile is missing (0.0), interpolate from available data
        if p95_latency_ms == 0.0 and (p50_latency_ms > 0 or p99_latency_ms > 0):
            # Interpolate p95 as midpoint between p50 and p99, or use the non-zero value
            if p50_latency_ms > 0 and p99_latency_ms > 0:
                p95_latency_ms = (p50_latency_ms + p99_latency_ms) / 2
            elif p99_latency_ms > 0:
                p95_latency_ms = p99_latency_ms * 0.95  # Estimate p95 from p99
            else:
                p95_latency_ms = p50_latency_ms * 1.5  # Estimate p95 from p50
        
        if p99_latency_ms == 0.0 and p95_latency_ms > 0:
            p99_latency_ms = p95_latency_ms * 1.1  # Estimate p99 from p95
        
        # Final sanity check: ensure monotonic order
        if p95_latency_ms < p50_latency_ms:
            p95_latency_ms = p50_latency_ms
        if p99_latency_ms < p95_latency_ms:
            p99_latency_ms = p95_latency_ms
        
        # 5. Build aggregated stats with real values
        aggregated = AggregatedJobStats(
            total_requests=total_completed + total_failed,
            successful_requests=total_completed,
            failed_requests=total_failed,
            overall_rate=total_aps,
            avg_latency_ms=avg_latency_ms,
            p50_latency_ms=p50_latency_ms,
            p95_latency_ms=p95_latency_ms,
            p99_latency_ms=p99_latency_ms,
        )
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Aggregated job {job_id}: {len(merged_workflow_stats)} workflows, "
                        f"rate={total_aps:.2f}/s, p50={p50_latency_ms:.2f}ms, p99={p99_latency_ms:.2f}ms",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Build GlobalJobResult
        global_result = GlobalJobResult(
            job_id=job_id,
            status=overall_status,
            per_datacenter_results=all_dc_results,
            aggregated=aggregated,
            total_completed=total_completed,
            total_failed=total_failed,
            successful_datacenters=successful_dcs,
            failed_datacenters=failed_dcs,
            errors=all_errors,
            elapsed_seconds=max_elapsed,
        )
        
        # Send to client
        callback = self._job_callbacks.get(job_id)
        if callback:
            try:
                await self.send_tcp(
                    callback,
                    "global_job_result",
                    global_result.dump(),
                    timeout=5.0,
                )
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Sent global job result for {job_id} to client {callback}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Failed to send global job result to client {callback}: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
        
        # Update job status
        if job_id in self._jobs:
            self._jobs[job_id].status = overall_status

        # Start background reporter submission after DC aggregation
        # Pass the merged workflow stats for reporting
        if merged_workflow_stats:
            self._start_background_reporter_submission(
                job_id=job_id,
                aggregated_stats=merged_workflow_stats,
                callback_addr=callback,
            )

        # Clean up DC results (but not job submission - needed for reporter tasks)
        self._job_dc_results.pop(job_id, None)
        self._workflow_dc_results.pop(job_id, None)

    # =========================================================================
    # Background Reporter Submission
    # =========================================================================

    def _start_background_reporter_submission(
        self,
        job_id: str,
        aggregated_stats: list[WorkflowStats],
        callback_addr: tuple[str, int] | None,
    ) -> None:
        """
        Start background tasks to submit results to configured reporters.

        Each reporter config gets its own background task that:
        1. Connects to the reporter
        2. Submits workflow and step results
        3. Closes the reporter
        4. Sends success/failure notification to client

        Tasks are tracked per job for cleanup.

        Args:
            job_id: The job ID for tracking
            aggregated_stats: List of aggregated WorkflowStats from all DCs
            callback_addr: Client callback address for push notifications
        """
        submission = self._job_submissions.get(job_id)
        if not submission:
            return

        reporter_configs = self._get_reporter_configs(job_id, submission)

        # No remote-capable reporters configured - skip submission
        # File-based reporters (JSON, CSV, XML) are handled client-side
        if not reporter_configs:
            return

        # Initialize task tracking for this job
        if job_id not in self._job_reporter_tasks:
            self._job_reporter_tasks[job_id] = {}

        # Start a background task for each reporter
        for config in reporter_configs:
            reporter_type = config.reporter_type.value
            token = self._task_runner.run(
                self._submit_to_reporter,
                job_id,
                config,
                aggregated_stats,
                callback_addr,
            )
            self._job_reporter_tasks[job_id][reporter_type] = token

    def _get_reporter_configs(self, job_id: str, submission: JobSubmission) -> list:
        """
        Extract remote-capable reporter configs from job submission.

        Filters out file-based reporters (JSON, CSV, XML) since gates
        cannot write to the client's local filesystem. Returns only reporters
        that can submit to remote destinations.

        Returns empty list if no remote-capable reporters are configured.
        """
        file_based_reporter_types = {
            ReporterTypes.JSON,
            ReporterTypes.CSV,
            ReporterTypes.XML,
        }

        if not submission.reporting_configs:
            return []

        try:
            reporter_configs = restricted_loads(submission.reporting_configs)
        except Exception as e:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Failed to unpickle reporter configs for job {job_id}: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return []

        if not reporter_configs:
            return []

        if not isinstance(reporter_configs, list):
            reporter_configs = [reporter_configs]

        # Filter out file-based reporters - they can't write to client's filesystem
        remote_configs = [
            config for config in reporter_configs
            if config.reporter_type not in file_based_reporter_types
        ]

        return remote_configs

    def _cleanup_reporter_task(self, job_id: str, reporter_type: str) -> None:
        """Remove completed reporter task from tracking."""
        job_tasks = self._job_reporter_tasks.get(job_id)
        if not job_tasks or reporter_type not in job_tasks:
            return

        del job_tasks[reporter_type]

        if job_tasks:
            return

        # No more reporter tasks for this job - clean up
        del self._job_reporter_tasks[job_id]
        self._job_submissions.pop(job_id, None)

    async def _submit_to_reporter(
        self,
        job_id: str,
        reporter_config,
        aggregated_stats: list[WorkflowStats],
        callback_addr: tuple[str, int] | None,
    ) -> None:
        """
        Submit aggregated results to a single reporter.

        Runs as a background task. Sends push notification to client
        on success or failure.

        For gates, we submit each workflow's merged stats. The reporter
        receives multiple calls (one per workflow) with cross-DC aggregated data.

        Args:
            job_id: The job ID
            reporter_config: The ReporterConfig instance
            aggregated_stats: List of merged WorkflowStats (one per workflow)
            callback_addr: Client callback for push notification
        """
        reporter_type = reporter_config.reporter_type.value
        start_time = time.monotonic()
        success = False
        error_message: str | None = None

        try:
            reporter = Reporter(reporter_config)
            await reporter.connect()

            try:
                # Submit each workflow's aggregated stats
                for workflow_stats in aggregated_stats:
                    if workflow_stats is None:
                        continue
                    await reporter.submit_workflow_results(workflow_stats)
                    await reporter.submit_step_results(workflow_stats)
                success = True
            finally:
                await reporter.close()

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Successfully submitted job {job_id} results to {reporter_type} ({len(aggregated_stats)} workflows)",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        except Exception as e:
            error_message = str(e)
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Failed to submit job {job_id} results to {reporter_type}: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        elapsed = time.monotonic() - start_time

        # Send push notification to client
        if callback_addr:
            await self._send_reporter_result_push(
                job_id=job_id,
                reporter_type=reporter_type,
                success=success,
                error=error_message,
                elapsed_seconds=elapsed,
                callback_addr=callback_addr,
            )

        # Cleanup task tracking
        self._cleanup_reporter_task(job_id, reporter_type)

    async def _send_reporter_result_push(
        self,
        job_id: str,
        reporter_type: str,
        success: bool,
        error: str | None,
        elapsed_seconds: float,
        callback_addr: tuple[str, int],
    ) -> None:
        """Send ReporterResultPush notification to client."""
        push = ReporterResultPush(
            job_id=job_id,
            reporter_type=reporter_type,
            success=success,
            error=error,
            elapsed_seconds=elapsed_seconds,
            source="gate",
            datacenter="",  # Gates span DCs, no single DC
        )

        try:
            await self.send_tcp(
                callback_addr,
                "reporter_result_push",
                push.dump(),
                timeout=5.0,
            )
        except Exception as e:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Failed to send reporter result push to client {callback_addr}: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    def _cleanup_reporter_tasks(self, job_id: str) -> None:
        """Cancel and clean up any pending reporter tasks for a job."""
        job_tasks = self._job_reporter_tasks.get(job_id)
        if job_tasks:
            for reporter_type, task in list(job_tasks.items()):
                if not task.done():
                    task.cancel()
            del self._job_reporter_tasks[job_id]
        # Also clean up submission
        self._job_submissions.pop(job_id, None)

    # =========================================================================
    # TCP Handlers - Ping/Health Check
    # =========================================================================

    @tcp.receive()
    async def ping(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle ping request from client.

        Returns comprehensive gate status including:
        - Gate identity and leadership status
        - Per-datacenter health and leader info
        - Active jobs
        - Peer gate addresses
        """
        try:
            request = PingRequest.load(data)

            # Build per-datacenter info
            datacenters: list[DatacenterInfo] = []

            for dc_id in self._datacenter_managers.keys():
                status = self._classify_datacenter_health(dc_id)

                # Find the DC leader address
                leader_addr: tuple[str, int] | None = None
                manager_statuses = self._datacenter_manager_status.get(dc_id, {})
                for manager_addr, heartbeat in manager_statuses.items():
                    if heartbeat.is_leader:
                        leader_addr = (heartbeat.tcp_host, heartbeat.tcp_port)
                        break

                datacenters.append(DatacenterInfo(
                    dc_id=dc_id,
                    health=status.health,
                    leader_addr=leader_addr,
                    available_cores=status.available_capacity,
                    manager_count=status.manager_count,
                    worker_count=status.worker_count,
                ))

            # Get active job IDs
            active_job_ids = list(self._jobs.keys())

            # Get peer gate addresses
            peer_gates = list(self._active_gate_peers)

            response = GatePingResponse(
                request_id=request.request_id,
                gate_id=self._node_id.full,
                datacenter=self._node_id.datacenter,
                host=self._host,
                port=self._tcp_port,
                is_leader=self.is_leader(),
                state=self._gate_state.value,
                term=self._leader_election.state.current_term,
                datacenters=datacenters,
                active_datacenter_count=self._count_active_datacenters(),
                active_job_ids=active_job_ids,
                active_job_count=len(active_job_ids),
                peer_gates=peer_gates,
            )

            return response.dump()

        except Exception as e:
            await self.handle_exception(e, "ping")
            return b'error'

    @tcp.receive()
    async def register_callback(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle client callback registration for job reconnection.

        Called when a client wants to re-subscribe to push notifications
        for an existing job (e.g., after disconnect/reconnect).

        Returns current job status so client can sync immediately.
        If this gate doesn't own the job, returns success=False with
        error="Job not found".
        """
        try:
            # Rate limit check (AD-24) - using reconnect limits
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit_for_operation(client_id, "reconnect")
            if not allowed:
                return RateLimitResponse(
                    operation="reconnect",
                    retry_after_seconds=retry_after,
                ).dump()

            request = RegisterCallback.load(data)
            job_id = request.job_id

            # Check if we own this job
            job = self._jobs.get(job_id)
            if not job:
                # Job not found on this gate
                response = RegisterCallbackResponse(
                    job_id=job_id,
                    success=False,
                    error="Job not found",
                )
                return response.dump()

            # Register the callback address for both status and progress updates
            self._job_callbacks[job_id] = request.callback_addr
            self._progress_callbacks[job_id] = request.callback_addr

            # Calculate elapsed time
            elapsed = time.monotonic() - job.timestamp if job.timestamp > 0 else 0.0

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Client reconnected for job {job_id}, registered callback {request.callback_addr}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
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

        except Exception as e:
            await self.handle_exception(e, "register_callback")
            return b'error'

    @tcp.receive()
    async def workflow_query(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle workflow status query from client.

        Queries all datacenter managers and aggregates results by datacenter.
        Returns status for requested workflows grouped by DC.

        Unknown workflow names are silently ignored.
        """
        try:
            # Rate limit check (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit_for_operation(client_id, "workflow_query")
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

        except Exception as e:
            await self.handle_exception(e, "workflow_query")
            return b'error'

    async def _query_all_datacenters(
        self,
        request: WorkflowQueryRequest,
    ) -> dict[str, list[WorkflowStatusInfo]]:
        """
        Query all datacenter managers for workflow status.

        Returns dict mapping DC ID to list of workflow status info.
        """
        dc_results: dict[str, list[WorkflowStatusInfo]] = {}

        async def query_dc(dc_id: str, manager_addr: tuple[str, int]) -> None:
            try:
                response_data, _ = await self.send_tcp(
                    manager_addr,
                    "workflow_query",
                    request.dump(),
                    timeout=5.0,
                )
                if isinstance(response_data, Exception) or response_data == b'error':
                    return

                manager_response = WorkflowQueryResponse.load(response_data)
                dc_results[dc_id] = manager_response.workflows

            except Exception:
                pass  # DC query failed - skip this DC

        # Get per-DC job leaders if this query has a job_id
        job_dc_managers = self._job_dc_managers.get(request.job_id, {}) if request.job_id else {}

        # Build query tasks for each datacenter
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
        """
        Get the best manager address to query for a datacenter.

        Priority: job leader > cluster leader > any healthy manager.
        """
        # First priority: use job leader for this DC if known
        if dc_id in job_dc_managers:
            return job_dc_managers[dc_id]

        # Fall back to cluster leader or any healthy manager
        manager_statuses = self._datacenter_manager_status.get(dc_id, {})
        fallback_addr: tuple[str, int] | None = None

        for manager_addr, heartbeat in manager_statuses.items():
            if fallback_addr is None:
                fallback_addr = (heartbeat.tcp_host, heartbeat.tcp_port)

            if heartbeat.is_leader:
                return (heartbeat.tcp_host, heartbeat.tcp_port)

        return fallback_addr

    @tcp.receive()
    async def datacenter_list(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle datacenter list request from client.

        Returns a lightweight list of registered datacenters with their
        health status and capacity information. This allows clients to
        discover available datacenters before submitting jobs.
        """
        try:
            # Rate limit check (AD-24)
            client_id = f"{addr[0]}:{addr[1]}"
            allowed, retry_after = self._check_rate_limit_for_operation(client_id, "datacenter_list")
            if not allowed:
                return RateLimitResponse(
                    operation="datacenter_list",
                    retry_after_seconds=retry_after,
                ).dump()

            request = DatacenterListRequest.load(data)

            # Build per-datacenter info
            datacenters: list[DatacenterInfo] = []
            total_available_cores = 0
            healthy_datacenter_count = 0

            for dc_id in self._datacenter_managers.keys():
                status = self._classify_datacenter_health(dc_id)

                # Find the DC leader address
                leader_addr: tuple[str, int] | None = None
                manager_statuses = self._datacenter_manager_status.get(dc_id, {})
                for manager_addr, heartbeat in manager_statuses.items():
                    if heartbeat.is_leader:
                        leader_addr = (heartbeat.tcp_host, heartbeat.tcp_port)
                        break

                datacenters.append(DatacenterInfo(
                    dc_id=dc_id,
                    health=status.health,
                    leader_addr=leader_addr,
                    available_cores=status.available_capacity,
                    manager_count=status.manager_count,
                    worker_count=status.worker_count,
                ))

                total_available_cores += status.available_capacity
                if status.health == DatacenterHealth.HEALTHY:
                    healthy_datacenter_count += 1

            response = DatacenterListResponse(
                request_id=request.request_id,
                gate_id=self._node_id.full,
                datacenters=datacenters,
                total_available_cores=total_available_cores,
                healthy_datacenter_count=healthy_datacenter_count,
            )

            return response.dump()

        except Exception as e:
            await self.handle_exception(e, "datacenter_list")
            return b'error'

    @tcp.receive()
    async def job_leadership_announcement(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle job leadership announcement from peer gate.

        When a gate accepts a job, it broadcasts leadership to peers.
        Peers record the leader for that job to enable proper routing
        of DC results and client requests.
        """
        try:
            announcement = JobLeadershipAnnouncement.load(data)

            # Use tracker to process claim - it will only accept if we don't already know
            # or if the fencing token is higher (TCP announcements use term as a proxy)
            accepted = self._job_leadership_tracker.process_leadership_claim(
                job_id=announcement.job_id,
                claimer_id=announcement.leader_id,
                claimer_addr=(announcement.leader_host, announcement.leader_tcp_port),
                fencing_token=announcement.term,  # Use term as fencing token for TCP
                metadata=announcement.workflow_count,  # workflow_count is DC count for gates
            )

            if accepted:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerDebug(
                        message=f"Recorded job {announcement.job_id[:8]}... leader: {announcement.leader_id[:8]}...",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

            return JobLeadershipAck(
                job_id=announcement.job_id,
                accepted=True,
                responder_id=self._node_id.full,
            ).dump()

        except Exception as e:
            await self.handle_exception(e, "job_leadership_announcement")
            return JobLeadershipAck(
                job_id="unknown",
                accepted=False,
                responder_id=self._node_id.full,
                error=str(e),
            ).dump()

    @tcp.receive()
    async def job_leader_manager_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle job leadership manager transfer notification from manager (AD-31).

        When a manager takes over job leadership from a failed manager within a DC,
        it notifies the origin gate so the gate can update its tracking of which
        manager leads the job in that datacenter.

        This ensures the gate routes subsequent job instructions to the correct manager.
        Uses JobLeadershipTracker.update_dc_manager_async for asyncio-safe updates
        with fencing token consistency.
        """
        try:
            transfer = JobLeaderManagerTransfer.load(data)

            # Verify this is for a job we're tracking (check both old dict and tracker)
            # Note: During migration, we check both. After full migration, only tracker is needed.
            job_known = (
                transfer.job_id in self._job_dc_managers or
                transfer.job_id in self._job_leadership_tracker
            )
            if not job_known:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Received manager transfer for unknown job {transfer.job_id[:8]}... from {transfer.new_manager_id[:8]}...",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return JobLeaderManagerTransferAck(
                    job_id=transfer.job_id,
                    gate_id=self._node_id.full,
                    accepted=False,
                ).dump()

            # Get current manager address for logging
            old_manager_addr = self._job_leadership_tracker.get_dc_manager(
                transfer.job_id, transfer.datacenter_id
            )
            # Also check legacy dict
            if old_manager_addr is None and transfer.job_id in self._job_dc_managers:
                old_manager_addr = self._job_dc_managers[transfer.job_id].get(transfer.datacenter_id)

            # Use tracker's async method - handles fencing token checks internally
            accepted = await self._job_leadership_tracker.update_dc_manager_async(
                job_id=transfer.job_id,
                dc_id=transfer.datacenter_id,
                manager_id=transfer.new_manager_id,
                manager_addr=transfer.new_manager_addr,
                fencing_token=transfer.fence_token,
            )

            if not accepted:
                current_fence = self._job_leadership_tracker.get_dc_manager_fencing_token(
                    transfer.job_id, transfer.datacenter_id
                )
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerDebug(
                        message=f"Rejected stale manager transfer for job {transfer.job_id[:8]}... (fence {transfer.fence_token} <= {current_fence})",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return JobLeaderManagerTransferAck(
                    job_id=transfer.job_id,
                    gate_id=self._node_id.full,
                    accepted=False,
                ).dump()

            # Also update legacy dict for backwards compatibility during migration
            if transfer.job_id not in self._job_dc_managers:
                self._job_dc_managers[transfer.job_id] = {}
            self._job_dc_managers[transfer.job_id][transfer.datacenter_id] = transfer.new_manager_addr

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Updated job {transfer.job_id[:8]}... DC {transfer.datacenter_id} manager: {old_manager_addr} -> {transfer.new_manager_addr}",
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
    async def windowed_stats_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle windowed stats push from Manager.

        Managers send unaggregated per-worker stats within time windows.
        Gate aggregates these across all DCs and forwards to clients.

        The stats include a datacenter field to enable cross-DC aggregation.
        """
        try:
            push: WindowedStatsPush = cloudpickle.loads(data)

            # Add to windowed stats collector using datacenter as worker_id
            # This aggregates stats from the same time window across DCs
            from hyperscale.distributed_rewrite.models import WorkflowProgress

            # For each worker stat from the DC, add to our collector
            for worker_stat in push.per_worker_stats:
                progress = WorkflowProgress(
                    job_id=push.job_id,
                    workflow_id=push.workflow_id,
                    workflow_name=push.workflow_name,
                    status="running",
                    completed_count=worker_stat.completed_count,
                    failed_count=worker_stat.failed_count,
                    rate_per_second=worker_stat.rate_per_second,
                    elapsed_seconds=push.window_end - push.window_start,  # Window duration
                    step_stats=worker_stat.step_stats,
                    avg_cpu_percent=worker_stat.avg_cpu_percent,
                    avg_memory_mb=worker_stat.avg_memory_mb,
                    collected_at=(push.window_start + push.window_end) / 2,
                )
                # Use DC:worker_id as the key so we track individual workers across DCs
                worker_key = f"{push.datacenter}:{worker_stat.worker_id}"
                await self._windowed_stats.add_progress(worker_key, progress)

            return b'ok'

        except Exception as e:
            await self.handle_exception(e, "windowed_stats_push")
            return b'error'

    async def _windowed_stats_push_loop(self) -> None:
        """
        Background loop for time-windowed stats streaming to clients.

        Flushes closed time windows and pushes aggregated stats to clients.
        Gate aggregates stats from all DCs before forwarding.

        Runs at STATS_PUSH_INTERVAL_MS (default 100ms) for low-latency streaming.
        """
        interval_seconds = self._stats_push_interval_ms / 1000.0

        while self._running:
            try:
                await asyncio.sleep(interval_seconds)
                if not self._running:
                    break

                # Flush closed windows with aggregation (Gate always aggregates for clients)
                pushes = await self._windowed_stats.flush_closed_windows(aggregate=True)

                if not pushes:
                    continue

                # Push aggregated stats to clients
                for push in pushes:
                    await self._push_windowed_stats_to_client(push)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Windowed stats push loop error: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                await asyncio.sleep(interval_seconds)

    async def _push_windowed_stats_to_client(self, push: WindowedStatsPush) -> None:
        """Push aggregated windowed stats to client callback."""
        callback = self._progress_callbacks.get(push.job_id)
        if not callback:
            return

        try:
            await self.send_tcp(
                callback,
                "windowed_stats_push",
                cloudpickle.dumps(push),
                timeout=1.0,
            )
        except Exception:
            # Client unreachable - continue, will retry next window
            pass

    async def _discovery_maintenance_loop(self) -> None:
        """
        Background loop for discovery service maintenance (AD-28).

        Periodically:
        - Decays failure counts to allow managers to recover
        - Cleans up expired DNS cache entries
        """
        while self._running:
            try:
                await asyncio.sleep(self._discovery_failure_decay_interval)

                # Decay failure counts for all DC discovery services
                for discovery in self._dc_manager_discovery.values():
                    discovery.decay_failures()
                    discovery.cleanup_expired_dns()

                # Decay failure counts for peer discovery service
                self._peer_discovery.decay_failures()
                self._peer_discovery.cleanup_expired_dns()

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def _select_best_manager_for_dc(self, datacenter_id: str, key: str) -> tuple[str, int] | None:
        """
        Select the best manager in a datacenter using adaptive selection (AD-28).

        Uses Power of Two Choices with EWMA for load-aware selection.

        Args:
            datacenter_id: The datacenter to select from
            key: Key for consistent selection (e.g., job_id)

        Returns:
            Tuple of (host, port) for the selected manager, or None if no managers available
        """
        discovery = self._dc_manager_discovery.get(datacenter_id)
        if discovery is None:
            return None

        # Only consider healthy managers (via three-signal health)
        def is_healthy(peer_id: str) -> bool:
            addr = discovery.get_peer_address(peer_id)
            if addr is None:
                return False
            manager_key = (datacenter_id, addr)
            health_state = self._manager_health.get(manager_key)
            if health_state is None:
                return True  # Assume healthy if not yet tracked
            routing = health_state.get_routing_decision()
            return routing.should_route

        selection = discovery.select_peer_with_filter(key, is_healthy)
        if selection is not None:
            return discovery.get_peer_address(selection.peer_id)
        return None

    def _record_manager_success(self, datacenter_id: str, manager_id: str, latency_ms: float) -> None:
        """
        Record a successful request to a manager (AD-28).

        Args:
            datacenter_id: The datacenter the manager belongs to
            manager_id: The manager that handled the request
            latency_ms: Request latency in milliseconds
        """
        discovery = self._dc_manager_discovery.get(datacenter_id)
        if discovery is not None:
            discovery.record_success(manager_id, latency_ms)

    def _record_manager_failure(self, datacenter_id: str, manager_id: str) -> None:
        """
        Record a failed request to a manager (AD-28).

        Args:
            datacenter_id: The datacenter the manager belongs to
            manager_id: The manager that failed
        """
        discovery = self._dc_manager_discovery.get(datacenter_id)
        if discovery is not None:
            discovery.record_failure(manager_id)

    def _select_best_peer(self, key: str) -> tuple[str, int] | None:
        """
        Select the best peer gate using adaptive selection (AD-28).

        Uses Power of Two Choices with EWMA for load-aware selection.

        Args:
            key: Key for consistent selection (e.g., request_id)

        Returns:
            Tuple of (host, port) for the selected peer, or None if no peers available
        """
        # Only consider active peers
        def is_active(peer_id: str) -> bool:
            addr = self._peer_discovery.get_peer_address(peer_id)
            if addr is None:
                return False
            return addr in self._active_gate_peers

        selection = self._peer_discovery.select_peer_with_filter(key, is_active)
        if selection is not None:
            return self._peer_discovery.get_peer_address(selection.peer_id)
        return None

    def _record_peer_success(self, peer_id: str, latency_ms: float) -> None:
        """
        Record a successful request to a peer gate (AD-28).

        Args:
            peer_id: The peer that handled the request
            latency_ms: Request latency in milliseconds
        """
        self._peer_discovery.record_success(peer_id, latency_ms)

    def _record_peer_failure(self, peer_id: str) -> None:
        """
        Record a failed request to a peer gate (AD-28).

        Args:
            peer_id: The peer that failed
        """
        self._peer_discovery.record_failure(peer_id)
