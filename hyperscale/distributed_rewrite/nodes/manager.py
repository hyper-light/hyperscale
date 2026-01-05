"""
Manager Node Server.

Managers orchestrate workflow execution within a datacenter. They:
- Receive jobs from gates (or directly from clients)
- Dispatch workflows to workers
- Aggregate status updates from workers
- Report to gates (if present)
- Participate in leader election among managers
- Handle quorum-based confirmation for workflow provisioning

Protocols:
- UDP: SWIM healthchecks (inherited from HealthAwareServer)
  - Managers probe workers to detect failures
  - Managers form a gossip cluster with other managers
  - Leader election uses SWIM membership info
- TCP: Data operations
  - Job submission from gates/clients
  - Workflow dispatch to workers
  - Status updates from workers
  - Quorum confirmation between managers
  - State sync for new leaders
"""

import asyncio
import secrets
import time
import inspect
from typing import Any

import cloudpickle
from collections import defaultdict

from hyperscale.core.hooks import Hook
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.graph.dependent_workflow import DependentWorkflow
from hyperscale.core.state.context import Context
from hyperscale.core.jobs.workers.stage_priority import StagePriority
from hyperscale.core.hooks import HookType
from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.distributed_rewrite.server.events import VersionedStateClock
from hyperscale.distributed_rewrite.swim import HealthAwareServer, ManagerStateEmbedder
from hyperscale.distributed_rewrite.swim.health import (
    FederatedHealthMonitor,
    CrossClusterAck,
)
from hyperscale.distributed_rewrite.swim.core import (
    ErrorStats,
    CircuitState,
    QuorumUnavailableError,
    QuorumTimeoutError,
    QuorumCircuitOpenError,
)
from hyperscale.distributed_rewrite.models import (
    NodeInfo,
    NodeRole,
    ManagerInfo,
    ManagerPeerRegistration,
    ManagerPeerRegistrationResponse,
    ManagerState,
    RegistrationResponse,
    WorkflowProgressAck,
    GateInfo,
    GateHeartbeat,
    ManagerRegistrationResponse,
    JobProgressAck,
    WorkerRegistration,
    WorkerHeartbeat,
    WorkerState,
    WorkerStateSnapshot,
    ManagerHeartbeat,
    ManagerStateSnapshot,
    JobSubmission,
    JobAck,
    JobStatus,
    JobStatusPush,
    JobBatchPush,
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkflowProgress,
    WorkflowFinalResult,
    WorkflowResult,
    WorkflowStatus,
    JobProgress,
    JobFinalResult,
    StepStats,
    StateSyncRequest,
    StateSyncResponse,
    ProvisionRequest,
    ProvisionConfirm,
    ProvisionCommit,
    CancelJob,
    CancelAck,
    WorkerDiscoveryBroadcast,
    ContextForward,
    ContextLayerSync,
    ContextLayerSyncAck,
    JobLeadershipAnnouncement,
    JobLeadershipAck,
    ManagerToWorkerRegistration,
    ManagerToWorkerRegistrationAck,
    PingRequest,
    WorkerStatus,
    ManagerPingResponse,
    WorkflowQueryRequest,
    WorkflowStatusInfo,
    WorkflowQueryResponse,
    EagerWorkflowEntry,
    restricted_loads,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning, ServerError, ServerDebug
from hyperscale.reporting.results import Results

# New modular classes for job/workflow management
from hyperscale.distributed_rewrite.jobs import (
    JobManager,
    TrackingToken,
    WorkflowStateMachine,
    JobInfo,
    WorkflowInfo,
    SubWorkflowInfo,
    WorkerPool,
    WorkerInfo,
    WorkerHealth,
    WorkflowDispatcher,
)
from hyperscale.distributed_rewrite.models import PendingWorkflow


class ManagerServer(HealthAwareServer):
    """
    Manager node in the distributed Hyperscale system.
    
    Managers:
    - Form a gossip cluster for leader election (UDP SWIM)
    - Track registered workers and their capacity
    - Probe workers for liveness via UDP (SWIM protocol)
    - Dispatch workflows to workers with quorum confirmation (TCP)
    - Aggregate workflow progress from workers (TCP)
    - Report job status to gates if present (TCP)
    
    Healthchecks (UDP - SWIM protocol):
        Managers form a SWIM cluster with other managers for leader
        election. They also add workers to their SWIM membership and
        probe them to detect failures. When a worker fails probes,
        the suspicion subprotocol kicks in.
    
    Status Updates (TCP):
        Workers send status updates via TCP containing capacity and
        progress. These are distinct from healthchecks - a worker
        might have stale status but still be alive (detected via UDP).
    """
    
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "default",
        gate_addrs: list[tuple[str, int]] | None = None,
        gate_udp_addrs: list[tuple[str, int]] | None = None,  # For SWIM if gates exist
        seed_managers: list[tuple[str, int]] | None = None,  # TCP seed addresses for peer discovery
        manager_peers: list[tuple[str, int]] | None = None,  # DEPRECATED: use seed_managers
        manager_udp_peers: list[tuple[str, int]] | None = None,  # UDP for initial SWIM cluster join
        quorum_timeout: float = 5.0,
        max_workflow_retries: int = 3,  # Max retry attempts per workflow
        workflow_timeout: float = 300.0,  # Workflow timeout in seconds
    ):
        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
        )
        
        # Gate discovery (optional) - seed addresses from config
        self._seed_gates = gate_addrs or []  # TCP seed addresses
        self._gate_udp_addrs = gate_udp_addrs or []  # UDP for SWIM
        
        # Gate tracking (similar to Worker's manager tracking)
        self._known_gates: dict[str, GateInfo] = {}  # node_id -> GateInfo
        self._healthy_gate_ids: set[str] = set()  # Currently healthy gate node_ids
        self._primary_gate_id: str | None = None  # Primary gate (prefer leader)
        
        # Circuit breaker for gate communication
        # Tracks failures and implements fail-fast when gates are unreachable
        cb_config = env.get_circuit_breaker_config()
        self._gate_circuit = ErrorStats(
            max_errors=cb_config['max_errors'],
            window_seconds=cb_config['window_seconds'],
            half_open_after=cb_config['half_open_after'],
        )
        
        # Backwards compat: keep for initial iteration through seed addresses
        self._gate_addrs = gate_addrs or []  # TCP
        self._current_gate: tuple[str, int] | None = None
        
        # Seed managers for peer discovery (like workers have seed_managers)
        # Backwards compat: accept manager_peers as alias for seed_managers
        self._seed_managers = seed_managers or manager_peers or []  # TCP
        self._manager_udp_peers = manager_udp_peers or []  # UDP for initial SWIM join

        # Known manager peers (discovered dynamically, like worker's _known_managers)
        # Maps node_id -> ManagerInfo
        self._known_manager_peers: dict[str, ManagerInfo] = {}

        # Track manager peer addresses for failure detection
        # Maps UDP addr -> TCP addr for peer managers
        self._manager_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}
        for i, tcp_addr in enumerate(self._seed_managers):
            if i < len(self._manager_udp_peers):
                self._manager_udp_to_tcp[self._manager_udp_peers[i]] = tcp_addr

        # Track active manager peers by node_id (removed when SWIM marks as dead)
        self._active_manager_peer_ids: set[str] = set()

        # Legacy: Track active peers by TCP addr for backwards compat during transition
        self._active_manager_peers: set[tuple[str, int]] = set(self._seed_managers)

        # Track manager peer info from ManagerHeartbeat (proper node_ids, leadership, etc)
        # Maps UDP addr -> ManagerHeartbeat for peers we've heard from via SWIM
        self._manager_peer_info: dict[tuple[str, int], ManagerHeartbeat] = {}

        # Set of manager node_ids we've already registered with (avoid duplicate registrations)
        self._registered_with_managers: set[str] = set()
        
        # Registered workers (indexed by node_id)
        self._workers: dict[str, WorkerRegistration] = {}  # node_id -> registration
        self._worker_addr_to_id: dict[tuple[str, int], str] = {}  # (host, port) -> node_id (reverse mapping)
        
        # Per-worker circuit breakers for dispatch failures
        # Tracks failures per-worker to avoid dispatching to failing workers
        self._worker_circuits: dict[str, ErrorStats] = {}  # node_id -> ErrorStats
        
        # Versioned state clock for rejecting stale updates
        # Tracks per-worker and per-job versions using Lamport timestamps
        self._versioned_clock = VersionedStateClock()

        # Quorum protocol state (temporary, scoped to quorum request execution)
        self._pending_provisions: dict[str, ProvisionRequest] = {}  # workflow_id -> request
        self._provision_confirmations: dict[str, set[str]] = {}  # workflow_id -> confirming nodes

        # Job leader tracking (Context Consistency Protocol)
        # Each job has one leader manager responsible for context consistency
        self._job_leaders: dict[str, str] = {}  # job_id -> leader_node_id
        self._job_leader_addrs: dict[str, tuple[str, int]] = {}  # job_id -> (host, tcp_port)
        self._job_fencing_tokens: dict[str, int] = {}  # job_id -> monotonic fencing token
        self._job_layer_version: dict[str, int] = {}  # job_id -> monotonic layer version
        self._job_contexts: dict[str, Context] = {}  # job_id -> Context for dependent workflows
        self._context_lamport_clock: int = 0  # For generating timestamps on context updates
        
        # Client push notification callbacks (when gates not present)
        # job_id -> callback address for push notifications
        self._job_callbacks: dict[str, tuple[str, int]] = {}
        self._client_callbacks: dict[str, tuple[str, int]] = {}  # Alias for backwards compat

        # Job submissions for eager dispatch (need access to submission params)
        self._job_submissions: dict[str, JobSubmission] = {}  # job_id -> submission
        
        # Workflow retry tracking
        # Maps workflow_id -> (retry_count, original_dispatch, failed_workers)
        self._workflow_retries: dict[str, tuple[int, bytes, set[str]]] = {}
        self._max_workflow_retries = max_workflow_retries
        
        # External incarnation for cross-cluster probes (xprobe)
        # Separate from SWIM cluster incarnation - used by gates for staleness detection
        self._external_incarnation: int = 0
        self._workflow_timeout = workflow_timeout
        
        # Federated Health Monitor for cross-cluster gate probing
        # Uses xprobe/xack protocol to probe gate cluster leader
        # This is separate from SWIM - gates are in a different SWIM cluster
        fed_config = env.get_federated_health_config()
        self._gate_health_monitor = FederatedHealthMonitor(
            probe_interval=fed_config['probe_interval'],
            probe_timeout=fed_config['probe_timeout'],
            suspicion_timeout=fed_config['suspicion_timeout'],
            max_consecutive_failures=fed_config['max_consecutive_failures'],
        )
        
        # Workflow completion events for dependency tracking
        # Maps workflow_id -> asyncio.Event (set when workflow completes)
        self._workflow_completion_events: dict[str, asyncio.Event] = {}

        # Core availability event - signaled when cores become available
        # Waiting workflows can wait on this instead of polling
        self._cores_available_event: asyncio.Event = asyncio.Event()

        # Lock for atomic core selection and reservation
        # Prevents race conditions when multiple workflows dispatch concurrently
        self._core_allocation_lock: asyncio.Lock | None = None

        # Lock for dispatch synchronization (used by WorkflowDispatcher)
        self._eager_dispatch_lock: asyncio.Lock | None = None
        self._workflow_results_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # Fencing tokens for at-most-once
        self._fence_token = 0
        
        # State versioning (local manager state version)
        self._state_version = 0
        
        # Manager state (SYNCING until state sync completes)
        # SYNCING managers are NOT counted in quorum calculations
        self._manager_state = ManagerState.SYNCING
        
        # Quorum settings
        self._quorum_timeout = quorum_timeout
        
        # Quorum circuit breaker - prevents repeated attempts when quorum unavailable
        # Opens after 3 failures within 30 seconds, recovers after 10 seconds
        self._quorum_circuit = ErrorStats(
            window_seconds=30.0,
            max_errors=3,
            half_open_after=10.0,
        )
        
        # Job cleanup configuration
        self._job_max_age: float = 3600.0  # 1 hour max age for completed jobs
        self._job_cleanup_interval: float = 60.0  # Check every minute

        # =======================================================================
        # New Modular Classes - Gradual Migration
        # These classes will progressively replace the direct dict-based tracking
        # above. During migration, both systems may coexist.
        # =======================================================================

        # JobManager for race-safe job/workflow state with TrackingToken support
        # Uses per-job locks and globally unique tracking tokens
        # NOTE: Use self._node_id.datacenter to ensure consistency with WorkflowDispatcher
        self._job_manager = JobManager(
            datacenter=self._node_id.datacenter,
            manager_id=self._node_id.short,
        )

        # WorkerPool for worker registration and resource tracking
        # Integrates with SWIM for health monitoring
        self._worker_pool = WorkerPool(
            health_grace_period=30.0,
            get_swim_status=self._get_swim_status_for_worker,
            manager_id=self._node_id.short,
            datacenter=dc_id,
        )

        # WorkflowDispatcher for dependency-aware workflow dispatch
        # Coordinates with JobManager and WorkerPool for allocation
        # Initialized lazily after start() when we have full context
        self._workflow_dispatcher: WorkflowDispatcher | None = None

        # Inject state embedder for Serf-style heartbeat embedding in SWIM messages
        self.set_state_embedder(ManagerStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_datacenter=lambda: self._node_id.datacenter,
            is_leader=self.is_leader,
            get_term=lambda: self._leader_election.state.current_term,
            get_state_version=lambda: self._state_version,
            get_active_jobs=lambda: self._job_manager.job_count,
            get_active_workflows=lambda: sum(
                len([w for w in job.workflows.values() if w.status == WorkflowStatus.RUNNING])
                for job in self._job_manager.iter_jobs()
            ),
            get_worker_count=lambda: len(self._workers),
            get_healthy_worker_count=lambda: len(self._get_healthy_worker_ids()),
            get_available_cores=lambda: self._get_available_cores_for_healthy_workers(),
            get_total_cores=self._get_total_cores,
            on_worker_heartbeat=self._handle_embedded_worker_heartbeat,
            on_manager_heartbeat=self._handle_manager_peer_heartbeat,
            on_gate_heartbeat=self._handle_gate_heartbeat,
            get_manager_state=lambda: self._manager_state.value,
            get_tcp_host=lambda: self._host,
            get_tcp_port=lambda: self._tcp_port,
            get_udp_host=lambda: self._host,
            get_udp_port=lambda: self._udp_port,
        ))
        
        # Register leadership callbacks (composition pattern - no override)
        self.register_on_become_leader(self._on_manager_become_leader)
        self.register_on_lose_leadership(self._on_manager_lose_leadership)
        
        # Register node death and join callbacks for failure/recovery handling
        self.register_on_node_dead(self._on_node_dead)
        self.register_on_node_join(self._on_node_join)
    
    def _on_manager_become_leader(self) -> None:
        """
        Called when this manager becomes the leader.
        
        Triggers state sync from:
        1. All known workers to get workflow state (workers are source of truth)
        2. Peer managers to get job-level metadata (retry counts, etc.)
        """
        # Schedule async state sync via task runner
        self._task_runner.run(self._sync_state_from_workers)
        self._task_runner.run(self._sync_state_from_manager_peers)
    
    def _on_manager_lose_leadership(self) -> None:
        """Called when this manager loses leadership."""
        # Currently no special cleanup needed
        pass
    
    def _on_node_dead(self, node_addr: tuple[str, int]) -> None:
        """
        Called when a node is marked as DEAD via SWIM.
        
        Handles both worker and manager peer failures:
        - Worker death → triggers workflow retry on other workers
        - Manager peer death → updates quorum tracking, logs for debugging
        
        Note: Leadership handling is automatic via lease expiry in LocalLeaderElection.
        If the dead manager was the leader, lease will expire and trigger re-election.
        """
        # Check if this is a worker
        worker_node_id = self._worker_addr_to_id.get(node_addr)
        if worker_node_id:
            # This is a worker - trigger failure handling
            self._task_runner.run(self._handle_worker_failure, worker_node_id)
            return
        
        # Check if this is a manager peer
        manager_tcp_addr = self._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            self._task_runner.run(self._handle_manager_peer_failure, node_addr, manager_tcp_addr)
    
    def _on_node_join(self, node_addr: tuple[str, int]) -> None:
        """
        Called when a node joins or rejoins the SWIM cluster.
        
        Handles manager peer recovery:
        - Manager peer rejoin → adds back to active peers set for quorum
        
        Worker joins are handled via register_worker TCP flow, not here.
        """
        # Check if this is a manager peer
        manager_tcp_addr = self._manager_udp_to_tcp.get(node_addr)
        if manager_tcp_addr:
            self._task_runner.run(self._handle_manager_peer_recovery, node_addr, manager_tcp_addr)
    
    async def _handle_manager_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """
        Handle a manager peer recovering/rejoining the cluster.
        
        Actions:
        1. Re-add to active peers set (restores quorum capacity)
        2. Log the recovery for debugging
        """
        # Add back to active peers
        self._active_manager_peers.add(tcp_addr)
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager peer at {tcp_addr} (UDP: {udp_addr}) has REJOINED the cluster",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Log quorum status
        active_count = len(self._active_manager_peers) + 1  # Include self
        required_quorum = self._quorum_size
        have_quorum = active_count >= required_quorum
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager cluster: {active_count} active, quorum={required_quorum}, have_quorum={have_quorum}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def _handle_manager_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """
        Handle a manager peer becoming unavailable (detected via SWIM).
        
        Actions:
        1. Remove from active peers set (affects quorum calculation)
        2. Log the failure for debugging
        3. If we were waiting on quorum from this peer, those requests will timeout
        
        Note: Leadership re-election is automatic via LocalLeaderElection
        when the leader's heartbeats stop (lease expiry).
        """
        # Remove from active peers
        self._active_manager_peers.discard(tcp_addr)
        
        # Check if this was the leader
        current_leader = self.get_current_leader()
        was_leader = current_leader == udp_addr
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager peer at {tcp_addr} (UDP: {udp_addr}) marked as DEAD" +
                        (" - was LEADER, re-election will occur" if was_leader else ""),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Log quorum status
        active_count = len(self._active_manager_peers) + 1  # Include self
        required_quorum = self._quorum_size
        have_quorum = active_count >= required_quorum
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager cluster: {active_count} active, quorum={required_quorum}, have_quorum={have_quorum}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Check if the dead manager was leading any jobs
        # If we're the cluster leader, take over those jobs
        await self._handle_job_leader_failure(tcp_addr)

    async def _handle_job_leader_failure(
        self,
        failed_manager_addr: tuple[str, int],
    ) -> None:
        """
        Handle job leadership takeover when a job leader manager fails.

        When a manager fails, the cluster leader takes over leadership
        for any jobs that the failed manager was leading. This provides
        automatic failover with the cluster leader acting as the
        "leader of last resort" for orphaned jobs.

        The cluster leader already has:
        - Lease-based leadership (provides fencing)
        - Term tracking (provides monotonic ordering)
        - Quorum-based election (provides consistency)

        By piggybacking on cluster leadership, we get these guarantees
        for job leadership failover without a separate per-job election.
        """
        # Only cluster leader performs job takeover
        if not self.is_leader():
            return

        # Find jobs led by the failed manager
        orphaned_jobs: list[str] = []
        for job_id, leader_addr in list(self._job_leader_addrs.items()):
            if leader_addr == failed_manager_addr:
                orphaned_jobs.append(job_id)

        if not orphaned_jobs:
            return

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Cluster leader taking over {len(orphaned_jobs)} jobs from failed manager at {failed_manager_addr}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        # Take over leadership of each orphaned job
        for job_id in orphaned_jobs:
            # Update job leadership to self
            old_leader = self._job_leaders.get(job_id)
            old_token = self._job_fencing_tokens.get(job_id, 0)
            new_token = old_token + 1  # Increment fencing token for new epoch

            self._job_leaders[job_id] = self._node_id.full
            self._job_leader_addrs[job_id] = (self._host, self._tcp_port)
            self._job_fencing_tokens[job_id] = new_token

            # Increment state version
            self._increment_version()

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Took over job {job_id[:8]}... leadership (was: {old_leader[:8] if old_leader else 'unknown'}..., token: {old_token} -> {new_token})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Note: Job leadership will propagate via UDP heartbeats (Serf-style)
            # The heartbeat includes job_leaderships with fencing tokens

    async def _sync_state_from_workers(self) -> None:
        """
        Request current state from all registered workers.
        
        Called when this manager becomes leader to ensure we have
        the freshest state from all workers.
        """
        if not self._workers:
            return
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"New leader syncing state from {len(self._workers)} workers",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Request state from each registered worker
        request = StateSyncRequest(
            requester_id=self._node_id.full,
            requester_role=NodeRole.MANAGER.value,
            since_version=0,  # Request full state
        )
        
        sync_tasks = []
        # Snapshot to avoid dict mutation during iteration
        for node_id, worker_reg in list(self._workers.items()):
            worker_addr = (worker_reg.node.host, worker_reg.node.port)
            sync_tasks.append(
                self._request_worker_state(worker_addr, request)
            )
        
        if sync_tasks:
            results = await asyncio.gather(*sync_tasks, return_exceptions=True)
            
            success_count = sum(
                1 for r in results
                if r is not None and not isinstance(r, Exception)
            )
            
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Worker state sync complete: {success_count}/{len(sync_tasks)} workers responded",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
    
    async def _sync_state_from_manager_peers(self) -> None:
        """
        Request job state from peer managers.
        
        Called when this manager becomes leader to get job-level metadata
        (retry counts, assignments, completion status) that workers don't have.
        """
        peer_addrs = self._get_active_peer_tcp_addrs()
        if not peer_addrs:
            return

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"New leader syncing job state from {len(peer_addrs)} peer managers",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        request = StateSyncRequest(
            requester_id=self._node_id.full,
            requester_role=NodeRole.MANAGER.value,
            since_version=0,  # Request full state
        )

        sync_tasks = []
        for peer_addr in peer_addrs:
            sync_tasks.append(
                self._request_manager_peer_state(peer_addr, request)
            )
        
        if sync_tasks:
            results = await asyncio.gather(*sync_tasks, return_exceptions=True)
            
            success_count = sum(
                1 for r in results
                if r is not None and not isinstance(r, Exception)
            )
            
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"State sync complete: {success_count}/{len(sync_tasks)} workers responded",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
    
    async def _request_worker_state(
        self,
        worker_addr: tuple[str, int],
        request: StateSyncRequest,
        max_retries: int = 3,
        base_delay: float = 0.5,
    ) -> WorkerStateSnapshot | None:
        """
        Request state from a single worker with retries.
        
        Uses exponential backoff: delay = base_delay * (2 ** attempt)
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                response, _ = await self.send_tcp(
                    worker_addr,
                    action='state_sync_request',
                    data=request.dump(),
                    timeout=5.0,
                )
                
                if response and not isinstance(response, Exception):
                    sync_response = StateSyncResponse.load(response)
                    if sync_response.worker_state:
                        return await self._process_worker_state_response(sync_response.worker_state)
                
                # No valid response, will retry
                last_error = "Empty or invalid response"
                
            except Exception as e:
                last_error = str(e)
            
            # Don't sleep after last attempt
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
        
        # All retries failed
        self._task_runner.run(
            self._udp_logger.log,
            ServerError(
                message=f"State sync failed for {worker_addr} after {max_retries} attempts: {last_error}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        return None
    
    async def _process_worker_state_response(
        self,
        worker_state: WorkerStateSnapshot,
    ) -> WorkerStateSnapshot | None:
        """Process a worker state response and update local tracking."""
        # Only accept if fresher than what we have
        if self._versioned_clock.should_accept_update(
            worker_state.node_id,
            worker_state.version,
        ):
            # Convert to heartbeat format and update WorkerPool
            heartbeat = WorkerHeartbeat(
                node_id=worker_state.node_id,
                state=worker_state.state,
                available_cores=worker_state.available_cores,
                queue_depth=0,  # Not in snapshot
                cpu_percent=0.0,
                memory_percent=0.0,
                version=worker_state.version,
                active_workflows={
                    wf_id: progress.status
                    for wf_id, progress in worker_state.active_workflows.items()
                },
            )
            await self._worker_pool.update_heartbeat(worker_state.node_id, heartbeat)

            return worker_state
        return None
    
    async def _request_manager_peer_state(
        self,
        peer_addr: tuple[str, int],
        request: StateSyncRequest,
        max_retries: int | None = None,
        base_delay: float = 0.5,
    ) -> ManagerStateSnapshot | None:
        """
        Request state from a peer manager with retries.

        Uses exponential backoff: delay = base_delay * (2 ** attempt)
        Timeout and retries are configurable via Env.

        Handles the case where the peer is not ready (still in SYNCING state)
        by retrying until the peer becomes ACTIVE or retries are exhausted.
        """
        if max_retries is None:
            max_retries = self.env.MANAGER_STATE_SYNC_RETRIES

        sync_timeout = self.env.MANAGER_STATE_SYNC_TIMEOUT
        last_error = None

        for attempt in range(max_retries):
            try:
                response, _ = await self.send_tcp(
                    peer_addr,
                    action='state_sync_request',
                    data=request.dump(),
                    timeout=sync_timeout,
                )

                if response and not isinstance(response, Exception):
                    sync_response = StateSyncResponse.load(response)

                    # Check if peer is ready to serve state
                    if not sync_response.responder_ready:
                        last_error = "Peer not ready (still syncing)"
                        # Retry - peer is alive but not ready yet
                    elif sync_response.manager_state:
                        return await self._process_manager_state_response(sync_response.manager_state)
                    else:
                        # Peer is ready but no state (fresh cluster)
                        last_error = "Peer ready but no state available"
                        return None
                else:
                    # No valid response, will retry
                    last_error = "Empty or invalid response"

            except Exception as e:
                last_error = str(e)

            # Don't sleep after last attempt
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)

        # All retries failed - log at warning level (expected during startup races)
        await self._udp_logger.log(
            ServerWarning(
                message=f"Manager peer state sync incomplete for {peer_addr} after {max_retries} attempts: {last_error}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        return None
    
    async def _process_manager_state_response(
        self,
        manager_state: ManagerStateSnapshot,
    ) -> ManagerStateSnapshot | None:
        """
        Process a manager state response and merge state.

        Merges:
        - Workers: If peer has workers we don't know, register with them
        - Job leaders, layer versions, contexts (for routing)

        Note: Job state is managed by JobManager, not merged from peers.
        """
        # Check version for staleness
        peer_key = f"manager:{manager_state.node_id}"
        if self._versioned_clock.is_entity_stale(peer_key, manager_state.version):
            return None

        # Merge workers - if peer knows workers we don't, register with them
        workers_discovered = 0
        for worker_snapshot in manager_state.workers:
            # Check WorkerPool instead of legacy _workers
            if self._worker_pool.get_worker(worker_snapshot.node_id) is None:
                # Only process if we have full connection info
                if worker_snapshot.host and worker_snapshot.tcp_port:
                    workers_discovered += 1
                    # Schedule registration with this worker
                    self._task_runner.run(
                        self._register_with_discovered_worker,
                        worker_snapshot,
                    )

        if workers_discovered > 0:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Discovered {workers_discovered} workers from peer {manager_state.node_id}, registering...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

        # Merge job leader tracking (Context Consistency Protocol)
        # These are used for routing, not job state management
        for job_id, leader_id in manager_state.job_leaders.items():
            if job_id not in self._job_leaders:
                self._job_leaders[job_id] = leader_id

        # Merge job leader addresses
        for job_id, leader_addr in manager_state.job_leader_addrs.items():
            if job_id not in self._job_leader_addrs:
                self._job_leader_addrs[job_id] = leader_addr

        for job_id, layer_version in manager_state.job_layer_versions.items():
            # Accept higher layer versions
            current = self._job_layer_version.get(job_id, -1)
            if layer_version > current:
                self._job_layer_version[job_id] = layer_version

        # Deserialize and merge job contexts
        if manager_state.job_contexts:
            try:
                contexts_data = cloudpickle.loads(manager_state.job_contexts)
                for job_id, context_dict in contexts_data.items():
                    if job_id not in self._job_contexts:
                        self._job_contexts[job_id] = Context()
                    # Apply context values (from_dict is async, run in task)
                    for workflow, values in context_dict.items():
                        self._task_runner.run(
                            self._job_contexts[job_id].from_dict, workflow, values
                        )
            except Exception:
                pass  # Ignore deserialization errors

        return manager_state

    async def _register_with_discovered_worker(
        self,
        worker_snapshot: WorkerStateSnapshot,
    ) -> None:
        """
        Register with a worker discovered via state sync from another manager.

        This ensures bidirectional consistency - if a follower has a worker
        registration that the leader doesn't, the leader will register with
        that worker to establish a direct connection.
        """
        worker_addr = (worker_snapshot.host, worker_snapshot.tcp_port)

        # Don't re-register if we already know this worker (check WorkerPool)
        if self._worker_pool.get_worker(worker_snapshot.node_id) is not None:
            return

        try:
            # Build manager info for registration
            manager_info = ManagerInfo(
                node_id=self._node_id.full,
                host=self._host,
                tcp_port=self._tcp_port,
                udp_port=self._udp_port,
                datacenter=self._node_id.datacenter,
            )

            registration = ManagerToWorkerRegistration(
                manager=manager_info,
                is_leader=self.is_leader(),
                term=self._leader_election.state.current_term,
                known_managers=self._get_known_peer_managers(),
            )

            response, _ = await self.send_tcp(
                worker_addr,
                action='manager_register',
                data=registration.dump(),
                timeout=2.0,
            )

            if response and isinstance(response, bytes) and response != b'error':
                ack = ManagerToWorkerRegistrationAck.load(response)
                if ack.accepted:
                    # Use data from the worker's response, not the snapshot
                    # This ensures we have accurate, up-to-date info from the worker
                    worker_reg = WorkerRegistration(
                        node=NodeInfo(
                            node_id=ack.worker_id,
                            host=worker_snapshot.host,
                            port=worker_snapshot.tcp_port,
                            udp_port=worker_snapshot.udp_port,
                        ),
                        total_cores=ack.total_cores,
                        available_cores=ack.available_cores,
                        memory_mb=0,  # Unknown from this flow
                        available_memory_mb=0,
                    )

                    # Register with WorkerPool
                    await self._worker_pool.register_worker(worker_reg)

                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"Registered with discovered worker {ack.worker_id[:8]}... at {worker_addr}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

        except Exception as e:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Failed to register with discovered worker {worker_snapshot.node_id[:8]}...: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
    
    def _handle_embedded_worker_heartbeat(
        self,
        heartbeat: WorkerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Handle WorkerHeartbeat received via SWIM message embedding.

        Uses versioned clock to reject stale updates - if the incoming
        heartbeat has a version <= our tracked version, it's discarded.
        """
        # Check if update is stale using versioned clock
        if self._versioned_clock.is_entity_stale(heartbeat.node_id, heartbeat.version):
            # Stale update - discard
            return

        # Process heartbeat in WorkerPool
        self._task_runner.run(
            self._worker_pool.process_heartbeat,
            heartbeat.node_id,
            heartbeat,
        )

        # Update version tracking (fire-and-forget, no await needed for sync operation)
        # We track the worker's version so future updates with same/lower version are rejected
        self._task_runner.run(
            self._versioned_clock.update_entity, heartbeat.node_id, heartbeat.version
        )
    
    def _handle_manager_peer_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Handle ManagerHeartbeat received from peer managers via SWIM.

        This enables:
        1. Proper node_id tracking for peers (instead of synthetic IDs)
        2. Leader tracking across the manager cluster
        3. Version-based stale update rejection
        4. Dynamic peer discovery - register with newly discovered peers
        5. Per-job leadership tracking via UDP (Serf-style)
        6. Continuous refresh of _known_manager_peers from heartbeats
        """
        # Don't process our own heartbeat
        if heartbeat.node_id == self._node_id.full:
            return

        # Check if update is stale using versioned clock
        if self._versioned_clock.is_entity_stale(heartbeat.node_id, heartbeat.version):
            return

        # Store peer info keyed by UDP address
        self._manager_peer_info[source_addr] = heartbeat

        # Update version tracking
        self._task_runner.run(
            self._versioned_clock.update_entity, heartbeat.node_id, heartbeat.version
        )

        # Use addresses from heartbeat if available, fallback to source_addr/convention
        tcp_host = heartbeat.tcp_host if heartbeat.tcp_host else source_addr[0]
        tcp_port = heartbeat.tcp_port if heartbeat.tcp_port else source_addr[1] - 1
        tcp_addr = (tcp_host, tcp_port)

        udp_host = heartbeat.udp_host if heartbeat.udp_host else source_addr[0]
        udp_port = heartbeat.udp_port if heartbeat.udp_port else source_addr[1]
        udp_addr = (udp_host, udp_port)

        # Process job leadership claims from this peer (UDP-based consistency)
        self._process_job_leadership_heartbeat(heartbeat, tcp_addr)

        # Always update _known_manager_peers to keep it fresh from heartbeats
        # This ensures leadership status and other info stays current
        is_new_peer = heartbeat.node_id not in self._known_manager_peers

        peer_info = ManagerInfo(
            node_id=heartbeat.node_id,
            tcp_host=tcp_host,
            tcp_port=tcp_port,
            udp_host=udp_host,
            udp_port=udp_port,
            datacenter=heartbeat.datacenter,
            is_leader=heartbeat.is_leader,
        )
        self._known_manager_peers[heartbeat.node_id] = peer_info
        self._active_manager_peer_ids.add(heartbeat.node_id)
        self._manager_udp_to_tcp[source_addr] = tcp_addr
        self._active_manager_peers.add(tcp_addr)

        if is_new_peer:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Discovered new peer manager via SWIM: {heartbeat.node_id} (leader={heartbeat.is_leader})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Register with the newly discovered peer for consistency
            # This ensures bidirectional relationship is established
            if heartbeat.node_id not in self._registered_with_managers:
                self._task_runner.run(
                    self._register_with_peer_manager,
                    tcp_addr,
                )

    def _process_job_leadership_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        peer_tcp_addr: tuple[str, int],
    ) -> None:
        """
        Process job leadership claims from a peer's heartbeat.

        Uses fencing tokens for consistency:
        - Accept leadership claim only if fencing token is higher than what we have
        - This prevents stale leaders from reasserting leadership after recovery

        This is the UDP-based job leadership protocol (Serf-style piggybacking).
        """
        for job_id, (fencing_token, layer_version) in heartbeat.job_leaderships.items():
            current_leader = self._job_leaders.get(job_id)
            current_token = self._job_fencing_tokens.get(job_id, -1)

            # Accept if:
            # 1. We don't know about this job yet, OR
            # 2. The fencing token is higher (newer leadership epoch)
            if current_leader is None or fencing_token > current_token:
                # Update job leadership
                self._job_leaders[job_id] = heartbeat.node_id
                self._job_leader_addrs[job_id] = peer_tcp_addr
                self._job_fencing_tokens[job_id] = fencing_token

                # Update layer version if higher
                current_layer = self._job_layer_version.get(job_id, -1)
                if layer_version > current_layer:
                    self._job_layer_version[job_id] = layer_version

                # Initialize context if needed
                if job_id not in self._job_contexts:
                    self._job_contexts[job_id] = Context()
    
    def _handle_gate_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Handle GateHeartbeat received from gates via SWIM.
        
        This enables managers to track gate leadership changes in real-time
        without waiting for TCP ack responses.
        """
        gate_id = heartbeat.node_id
        
        # Check if this is a known gate
        existing_gate = self._known_gates.get(gate_id)
        
        if existing_gate:
            # Update is_leader status if it changed
            old_is_leader = existing_gate.is_leader
            if heartbeat.is_leader != old_is_leader:
                # Update the gate info with new leadership status
                self._known_gates[gate_id] = GateInfo(
                    node_id=existing_gate.node_id,
                    tcp_host=existing_gate.tcp_host,
                    tcp_port=existing_gate.tcp_port,
                    udp_host=existing_gate.udp_host,
                    udp_port=existing_gate.udp_port,
                    datacenter=heartbeat.datacenter,
                    is_leader=heartbeat.is_leader,
                )
                
                # If this gate became the leader, switch primary
                if heartbeat.is_leader and self._primary_gate_id != gate_id:
                    old_primary = self._primary_gate_id
                    self._primary_gate_id = gate_id
                    
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"Gate leadership change via SWIM: {old_primary} -> {gate_id}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
        else:
            # New gate discovered via SWIM - create entry
            self._known_gates[gate_id] = GateInfo(
                node_id=gate_id,
                tcp_host=source_addr[0],
                tcp_port=source_addr[1] - 1,  # Convention: TCP = UDP - 1
                udp_host=source_addr[0],
                udp_port=source_addr[1],
                datacenter=heartbeat.datacenter,
                is_leader=heartbeat.is_leader,
            )
            self._healthy_gate_ids.add(gate_id)
            
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Discovered new gate via SWIM: {gate_id} (leader={heartbeat.is_leader})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            
            # If this is a leader and we don't have one, use it
            if heartbeat.is_leader and not self._primary_gate_id:
                self._primary_gate_id = gate_id
    
    def _update_known_gates(self, gates: list[GateInfo]) -> None:
        """
        Update the known gates from a list received via TCP ack.
        
        This is called when processing JobProgressAck from gates.
        """
        for gate in gates:
            self._known_gates[gate.node_id] = gate
            self._healthy_gate_ids.add(gate.node_id)
    
    def _process_job_progress_ack(self, data: bytes) -> None:
        """
        Process JobProgressAck to update gate topology.
        
        This enables continuous gate list refresh - every ack includes
        the current list of healthy gates and leadership status.
        """
        try:
            ack = JobProgressAck.load(data)
            
            # Update known gates from ack
            self._update_known_gates(ack.healthy_gates)
            
            # Update primary gate if leadership changed
            if ack.is_leader and self._primary_gate_id != ack.gate_id:
                old_primary = self._primary_gate_id
                self._primary_gate_id = ack.gate_id
                
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Gate leadership change: {old_primary} -> {ack.gate_id}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                
        except Exception:
            # Backwards compatibility: ignore parse errors for old b'ok' responses
            pass
    
    def _get_primary_gate_tcp_addr(self) -> tuple[str, int] | None:
        """Get TCP address of the primary gate."""
        if not self._primary_gate_id:
            return None
        gate = self._known_gates.get(self._primary_gate_id)
        if gate:
            return (gate.tcp_host, gate.tcp_port)
        return None
    
    def _get_healthy_gate_tcp_addrs(self) -> list[tuple[str, int]]:
        """Get TCP addresses of all healthy gates."""
        addrs = []
        for gate_id in self._healthy_gate_ids:
            gate = self._known_gates.get(gate_id)
            if gate:
                addrs.append((gate.tcp_host, gate.tcp_port))
        return addrs
    
    @property
    def node_info(self) -> NodeInfo:
        """Get this manager's node info."""
        return NodeInfo(
            node_id=self._node_id.full,
            role=NodeRole.MANAGER.value,
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
    
    @property
    def _quorum_size(self) -> int:
        """
        Calculate quorum size (majority of managers).
        
        Quorum is based on *known* cluster size, not just active size.
        This prevents split-brain where a partition thinks it has quorum
        because it only sees its own subset of members.

        Uses the larger of: seed managers or discovered peers.
        """
        # Use max of seeds and known peers for quorum calculation
        # This handles both initial startup (only seeds known) and
        # dynamic discovery (more peers discovered than seeds)
        known_peer_count = len(self._known_manager_peers)
        seed_count = len(self._seed_managers)
        peer_count = max(known_peer_count, seed_count)
        total_managers = peer_count + 1  # Include self
        return (total_managers // 2) + 1
    
    def _has_quorum_available(self) -> bool:
        """
        Check if we have enough active managers to achieve quorum.
        
        Returns True if:
        1. This manager is ACTIVE (SYNCING managers don't participate in quorum)
        2. The number of active managers (including self) is >= required quorum size
        """
        # SYNCING managers don't participate in quorum operations
        if self._manager_state != ManagerState.ACTIVE:
            return False
        
        active_count = len(self._active_manager_peers) + 1  # Include self
        return active_count >= self._quorum_size
    
    def get_quorum_status(self) -> dict:
        """
        Get current quorum and circuit breaker status.
        
        Returns a dict with:
        - active_managers: Number of active managers
        - required_quorum: Number needed for quorum
        - quorum_available: Whether quorum operations can proceed
        - circuit_state: Current circuit breaker state (CLOSED/OPEN/HALF_OPEN)
        - circuit_failures: Number of recent failures in window
        - circuit_error_rate: Errors per second in window
        
        This is useful for monitoring and debugging cluster health.
        """
        active_count = len(self._active_manager_peers) + 1
        required = self._quorum_size
        circuit_state = self._quorum_circuit.circuit_state
        
        return {
            "active_managers": active_count,
            "required_quorum": required,
            "quorum_available": self._has_quorum_available(),
            "circuit_state": circuit_state.name,
            "circuit_failures": self._quorum_circuit.error_count,
            "circuit_error_rate": self._quorum_circuit.error_rate,
            "manager_state": self._manager_state.value,
        }
    
    def _get_healthy_managers(self) -> list[ManagerInfo]:
        """
        Build list of all known healthy managers for worker discovery.
        
        Includes self and all active peer managers. Workers use this
        to maintain redundant communication channels.
        
        Uses real node_ids from ManagerHeartbeat when available (received via SWIM),
        falling back to synthetic IDs for peers we haven't heard from yet.
        """
        managers: list[ManagerInfo] = []
        
        # Add self
        managers.append(ManagerInfo(
            node_id=self._node_id.full,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            udp_host=self._host,
            udp_port=self._udp_port,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
        ))
        
        # Add active peer managers
        for tcp_addr in self._active_manager_peers:
            # Find UDP addr for this peer
            udp_addr: tuple[str, int] | None = None
            for udp, tcp in list(self._manager_udp_to_tcp.items()):
                if tcp == tcp_addr:
                    udp_addr = udp
                    break
            
            if udp_addr is None:
                udp_addr = tcp_addr  # Fallback
            
            # Check if we have real peer info from ManagerHeartbeat
            peer_heartbeat = self._manager_peer_info.get(udp_addr)
            
            if peer_heartbeat:
                # Use real info from SWIM heartbeat
                managers.append(ManagerInfo(
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
                managers.append(ManagerInfo(
                    node_id=f"manager-{tcp_addr[0]}:{tcp_addr[1]}",
                    tcp_host=tcp_addr[0],
                    tcp_port=tcp_addr[1],
                    udp_host=udp_addr[0],
                    udp_port=udp_addr[1],
                    datacenter=self._node_id.datacenter,
                    is_leader=False,
                ))
        
        return managers

    def _get_self_manager_info(self) -> ManagerInfo:
        """Get ManagerInfo for this manager."""
        return ManagerInfo(
            node_id=self._node_id.full,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            udp_host=self._host,
            udp_port=self._udp_port,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
        )

    def _get_known_peer_managers(self) -> list[ManagerInfo]:
        """Get list of all known peer managers (excluding self)."""
        return list(self._known_manager_peers.values())

    def _get_active_peer_tcp_addrs(self) -> list[tuple[str, int]]:
        """
        Get TCP addresses of all active peer managers.

        Prefers known peers (with proper node_ids) but falls back to
        seed managers during initial startup before peers are discovered.
        """
        # If we have known peers, use them
        if self._known_manager_peers:
            return [
                (peer.tcp_host, peer.tcp_port)
                for peer in self._known_manager_peers.values()
                if peer.node_id in self._active_manager_peer_ids
            ]
        # Fallback to active manager peers (set during init from seeds)
        return list(self._active_manager_peers)

    async def _register_with_peer_manager(
        self,
        peer_addr: tuple[str, int],
        max_retries: int = 3,
        base_delay: float = 0.5,
    ) -> bool:
        """
        Register this manager with a peer manager.

        Similar to worker registration - establishes bidirectional relationship
        and discovers the full cluster topology.

        Args:
            peer_addr: (host, port) TCP tuple of peer manager
            max_retries: Maximum number of retry attempts
            base_delay: Base delay for exponential backoff

        Returns:
            True if registration succeeded, False otherwise
        """
        registration = ManagerPeerRegistration(
            node=self._get_self_manager_info(),
            term=self._leader_election.state.current_term,
            is_leader=self.is_leader(),
        )

        for attempt in range(max_retries + 1):
            try:
                result, _ = await self.send_manager_peer_register(
                    peer_addr,
                    registration.dump(),
                    timeout=5.0,
                )

                if isinstance(result, Exception):
                    raise result

                response = ManagerPeerRegistrationResponse.load(result)

                if response.accepted:
                    # Add to known peers
                    self._registered_with_managers.add(response.manager_id)

                    # Learn about other peers from response
                    for peer_info in response.known_peers:
                        if peer_info.node_id != self._node_id.full:
                            self._known_manager_peers[peer_info.node_id] = peer_info
                            self._active_manager_peer_ids.add(peer_info.node_id)

                            # Update UDP -> TCP mapping
                            udp_addr = (peer_info.udp_host, peer_info.udp_port)
                            tcp_addr = (peer_info.tcp_host, peer_info.tcp_port)
                            self._manager_udp_to_tcp[udp_addr] = tcp_addr
                            self._active_manager_peers.add(tcp_addr)

                            # Also populate _manager_peer_info for _get_active_manager_peer_addrs()
                            # Create initial heartbeat that will be updated by SWIM
                            if udp_addr not in self._manager_peer_info:
                                initial_heartbeat = ManagerHeartbeat(
                                    node_id=peer_info.node_id,
                                    datacenter=peer_info.datacenter,
                                    is_leader=(peer_info.node_id == response.manager_id and response.is_leader),
                                    term=response.term,
                                    version=0,
                                    active_jobs=0,
                                    active_workflows=0,
                                    worker_count=0,
                                    healthy_worker_count=0,
                                    available_cores=0,
                                    total_cores=0,
                                    state=ManagerState.ACTIVE.value,
                                    tcp_host=peer_info.tcp_host,
                                    tcp_port=peer_info.tcp_port,
                                    udp_host=peer_info.udp_host,
                                    udp_port=peer_info.udp_port,
                                )
                                self._manager_peer_info[udp_addr] = initial_heartbeat

                    if attempt > 0:
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerInfo(
                                message=f"Registered with peer manager {peer_addr} after {attempt + 1} attempts",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )
                    return True

            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Peer registration attempt {attempt + 1}/{max_retries + 1} failed for {peer_addr}: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

            # Exponential backoff before retry
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)

        return False

    async def _register_with_seed_managers(self) -> None:
        """
        Register with all seed managers on startup.

        Like workers, managers register with all known seed managers
        to establish the full cluster topology.
        """
        if not self._seed_managers:
            return

        successful = 0
        for seed_addr in self._seed_managers:
            success = await self._register_with_peer_manager(seed_addr)
            if success:
                successful += 1

        if successful == 0:
            await self._udp_logger.log(
                ServerWarning(
                    message=f"Failed to register with any seed manager: {self._seed_managers}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        else:
            await self._udp_logger.log(
                ServerInfo(
                    message=f"Registered with {successful}/{len(self._seed_managers)} seed managers, "
                            f"discovered {len(self._known_manager_peers)} total peers",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _broadcast_worker_discovery(
        self,
        worker_id: str,
        worker_tcp_addr: tuple[str, int],
        worker_udp_addr: tuple[str, int],
        available_cores: int,
    ) -> None:
        """
        Broadcast a newly discovered worker to all peer managers.

        Called when a worker registers with this manager. Ensures all managers
        learn about the worker even if they don't receive direct registration.
        """
        peer_addrs = self._get_active_peer_tcp_addrs()
        if not peer_addrs:
            return

        broadcast = WorkerDiscoveryBroadcast(
            worker_id=worker_id,
            worker_tcp_addr=worker_tcp_addr,
            worker_udp_addr=worker_udp_addr,
            datacenter=self._node_id.datacenter,
            available_cores=available_cores,
            source_manager_id=self._node_id.full,
        )

        broadcast_count = 0
        for peer_addr in peer_addrs:
            try:
                await self.send_tcp(
                    peer_addr,
                    "worker_discovery",
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
                    message=f"Broadcast worker {worker_id} to {broadcast_count} peer managers",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
    
    async def start(self) -> None:
        """
        Start the manager server.
        
        New Manager Join Process:
        1. Start TCP/UDP server
        2. Join SWIM cluster with other managers
        3. Start probe cycle
        4. Start leader election
        5. Complete startup sync and transition to ACTIVE
        
        SYNCING managers are NOT counted in quorum.
        """
        # Start the underlying server (TCP/UDP listeners, task runner, etc.)
        # Uses SWIM settings from Env configuration
        await self.start_server(init_context=self.env.get_swim_init_context())

        if self._core_allocation_lock is None:
            self._core_allocation_lock = asyncio.Lock()

        if self._eager_dispatch_lock is None:
            self._eager_dispatch_lock = asyncio.Lock()

        # Initialize WorkflowDispatcher now that we have full context
        if self._workflow_dispatcher is None:
            self._workflow_dispatcher = WorkflowDispatcher(
                job_manager=self._job_manager,
                worker_pool=self._worker_pool,
                send_dispatch=self._send_workflow_dispatch,
                datacenter=self._node_id.datacenter,
                manager_id=self._node_id.short,
            )

            # Wire up event-driven dispatch: when a workflow completes in JobManager,
            # notify WorkflowDispatcher so it can trigger dependent workflows
            self._job_manager.set_on_workflow_completed(
                self._workflow_dispatcher.mark_workflow_completed
            )

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager starting in SYNCING state (not in quorum yet)",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Join SWIM cluster with other managers (UDP healthchecks)
        for peer_udp in self._manager_udp_peers:
            await self.join_cluster(peer_udp)

        # Start SWIM probe cycle (UDP healthchecks for managers + workers)
        self._task_runner.run(self.start_probe_cycle)

        # Register with seed managers to discover cluster topology
        # Like workers, managers register with all seeds to establish relationships
        if self._seed_managers:
            await self._register_with_seed_managers()

        # Wait for cluster to stabilize before starting leader election
        # This ensures all peers are visible before voting begins
        await self._wait_for_cluster_stabilization()

        # Add random jitter before starting leader election to prevent
        # simultaneous elections when managers start concurrently.
        # This is a standard Raft technique - each node waits a random
        # amount of time before starting its first election.
        jitter_max = self.env.LEADER_ELECTION_JITTER_MAX
        if jitter_max > 0 and len(self._manager_udp_peers) > 0:
            import random
            jitter = random.uniform(0, jitter_max)
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Waiting {jitter:.2f}s jitter before starting leader election",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            await asyncio.sleep(jitter)

        # Start leader election (uses SWIM membership info)
        await self.start_leader_election()

        # Wait for leader election to stabilize before state sync
        startup_sync_delay = self.env.MANAGER_STARTUP_SYNC_DELAY
        await asyncio.sleep(startup_sync_delay)

        # Sync state and transition to ACTIVE
        await self._complete_startup_sync()
        
        # Start background cleanup for completed jobs
        self._task_runner.run(self._job_cleanup_loop)
        
        # Register with gates (similar to Worker registering with Managers)
        if self._seed_gates:
            await self._register_with_gates()
        
        # Initialize Federated Health Monitor for gate probing
        # Uses xprobe/xack protocol instead of SWIM (gates are in separate cluster)
        self._gate_health_monitor.set_callbacks(
            send_udp=self._send_xprobe_to_gate,
            cluster_id=f"manager-{self._node_id.datacenter}",
            node_id=self._node_id.full,
            on_dc_health_change=self._on_gate_health_change,
        )
        
        # Add known gate addresses to the federated health monitor
        for gate_id, gate_info in list(self._known_gates.items()):
            gate_udp_addr = (gate_info.udp_host, gate_info.udp_port)
            self._gate_health_monitor.add_datacenter(
                datacenter="gate-cluster",  # Gates are a single cluster
                leader_udp_addr=gate_udp_addr,
                leader_node_id=gate_id,
            )
        
        # Start federated health monitor if we have gates
        if self._known_gates or self._gate_udp_addrs:
            await self._gate_health_monitor.start()
        
        # Start TCP heartbeat loop to gates (supplements federated health probing)
        # TCP provides reliability for critical status updates
        if self._gate_addrs or self._known_gates:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Starting gate heartbeat loop with {len(self._gate_addrs)} seed gates and {len(self._known_gates)} known gates",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            self._task_runner.run(self._gate_heartbeat_loop)
        else:
            # No gates - start batch push loop for direct client connections
            self._task_runner.run(self._client_batch_push_loop)
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager started in DC {self._node_id.datacenter}, state={self._manager_state.value}" +
                        (f", primary gate: {self._primary_gate_id}" if self._primary_gate_id else "") +
                        (", client push notifications enabled" if not (self._gate_addrs or self._known_gates) else ""),
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def _wait_for_cluster_stabilization(self) -> None:
        """
        Wait for the SWIM cluster to stabilize before starting leader election.

        This ensures all configured manager peers are visible in the cluster
        before any node attempts to become leader. This prevents the race
        condition where a manager becomes leader with only 1 vote (itself)
        because it started election before other peers joined.

        The method waits until:
        - All expected peers are in the nodes dict, OR
        - The stabilization timeout is reached

        With sequential starts, this allows later-starting managers to join
        before election begins. With concurrent starts, this ensures all
        managers see each other.
        """
        expected_peers = len(self._manager_udp_peers)
        if expected_peers == 0:
            # Single manager, no cluster to stabilize
            return

        timeout = self.env.CLUSTER_STABILIZATION_TIMEOUT
        poll_interval = self.env.CLUSTER_STABILIZATION_POLL_INTERVAL
        start_time = time.monotonic()

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Waiting for cluster stabilization (expecting {expected_peers} peers, timeout={timeout}s)",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )

        while True:
            # Check how many peers we can see
            nodes = self._context.read('nodes')
            self_addr = (self._host, self._udp_port)
            visible_peers = len([n for n in nodes.keys() if n != self_addr])

            if visible_peers >= expected_peers:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Cluster stabilized: {visible_peers}/{expected_peers} peers visible",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return

            # Check timeout
            elapsed = time.monotonic() - start_time
            if elapsed >= timeout:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Cluster stabilization timeout: only {visible_peers}/{expected_peers} peers visible after {timeout}s",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return

            await asyncio.sleep(poll_interval)

    async def _complete_startup_sync(self) -> None:
        """
        Complete the startup state sync and transition to ACTIVE.

        If this manager is the leader, it becomes ACTIVE immediately
        (leader sync happens in _on_manager_become_leader callback).

        If not leader, requests state sync from the current leader,
        then transitions to ACTIVE.
        """
        if self.is_leader():
            # Leader becomes ACTIVE immediately
            # State sync from workers/peers happens in _on_manager_become_leader
            self._manager_state = ManagerState.ACTIVE
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message="Manager is LEADER, transitioning to ACTIVE state",
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
            leader_tcp_addr = self._manager_udp_to_tcp.get(leader_addr)

            if not leader_tcp_addr:
                # Log the mismatch for debugging
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Leader UDP addr {leader_addr} not in UDP->TCP map. Map keys: {list(self._manager_udp_to_tcp.keys())}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

            if leader_tcp_addr:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Requesting state sync from leader at {leader_tcp_addr}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                
                # Request state sync from leader
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_role=NodeRole.MANAGER.value,
                    since_version=0,  # Request full state
                )
                
                state = await self._request_manager_peer_state(leader_tcp_addr, request)
                
                if state:
                    self._process_manager_state_response(state)
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"State sync from leader complete, transitioning to ACTIVE",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                else:
                    # Expected during startup races - leader may not be ready yet
                    await self._udp_logger.log(
                        ServerWarning(
                            message="State sync from leader incomplete, transitioning to ACTIVE anyway (fresh cluster or leader still starting)",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
        else:
            # No leader available - we might be the first manager
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message="No leader available for state sync (first manager?), transitioning to ACTIVE",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        
        # Transition to ACTIVE
        self._manager_state = ManagerState.ACTIVE
    
    async def _register_with_gates(self) -> None:
        """
        Register this manager with gates.
        
        Try each seed gate until one responds with a ManagerRegistrationResponse
        containing the list of all healthy gates.
        """
        for gate_addr in self._seed_gates:
            response = await self._try_register_with_gate(gate_addr)
            if response and response.accepted:
                self._current_gate = gate_addr
                self._primary_gate_id = response.gate_id
                
                # Populate known gates from response
                for gate_info in response.healthy_gates:
                    self._known_gates[gate_info.node_id] = gate_info
                    self._healthy_gate_ids.add(gate_info.node_id)
                    
                    # Track gate's UDP address for federated health monitoring
                    # NOTE: We do NOT add gates to our SWIM probe scheduler.
                    # Gates are in a separate SWIM cluster - we use xprobe/xack
                    # protocol via FederatedHealthMonitor instead.
                    gate_udp_addr = (gate_info.udp_host, gate_info.udp_port)
                    if gate_udp_addr not in self._gate_udp_addrs:
                        self._gate_udp_addrs.append(gate_udp_addr)
                    
                    # Add to federated health monitor (will be started in start())
                    # The monitor isn't set up yet at registration time, so we
                    # just store the addresses - start() will add them
                
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Registered with gate {response.gate_id}, discovered {len(response.healthy_gates)} gates",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return
        
        # Failed to register with any gate
        self._task_runner.run(
            self._udp_logger.log,
            ServerError(
                message="Failed to register with any gate - manager will operate without gate coordination",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def _try_register_with_gate(
        self,
        gate_addr: tuple[str, int],
        max_retries: int = 3,
        base_delay: float = 0.5,
    ) -> ManagerRegistrationResponse | None:
        """
        Try to register with a single gate.
        
        Uses retries with exponential backoff:
        - Attempt 1: immediate
        - Attempt 2: 0.5s delay
        - Attempt 3: 1.0s delay
        - Attempt 4: 2.0s delay
        
        Also respects the circuit breaker - if open, fails fast.
        
        Args:
            gate_addr: (host, port) tuple of gate
            max_retries: Maximum retry attempts (default 3)
            base_delay: Base delay for exponential backoff (default 0.5s)
            
        Returns:
            ManagerRegistrationResponse if successful, None otherwise
        """
        # Check circuit breaker first
        if self._is_gate_circuit_open():
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Cannot register with gate {gate_addr}: circuit breaker is OPEN",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return None
        
        heartbeat = self._build_manager_heartbeat()
        
        for attempt in range(max_retries + 1):
            try:
                response, _ = await self.send_tcp(
                    gate_addr,
                    "manager_register",
                    heartbeat.dump(),
                    timeout=5.0,
                )
                
                if isinstance(response, Exception):
                    raise response
                
                result = ManagerRegistrationResponse.load(response)
                if result.accepted:
                    self._gate_circuit.record_success()
                    if attempt > 0:
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerInfo(
                                message=f"Registered with gate {gate_addr} after {attempt + 1} attempts",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )
                    return result
                else:
                    # Gate rejected registration - don't retry
                    self._gate_circuit.record_error()
                    return result
                    
            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Gate registration attempt {attempt + 1}/{max_retries + 1} to {gate_addr} failed: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            
            # Exponential backoff before retry (except after last attempt)
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
        
        # All retries exhausted
        self._gate_circuit.record_error()
        return None
    
    async def stop(
        self,
        drain_timeout: float = 5,
        broadcast_leave: bool = True
    ) -> None:
        """Stop the manager server."""
        # Set _running to False early to stop all background loops
        self._running = False

        # Shutdown WorkflowDispatcher to cancel all dispatch loop tasks
        if self._workflow_dispatcher:
            await self._workflow_dispatcher.shutdown()

        # Stop federated health monitor
        await self._gate_health_monitor.stop()
        await super().stop(
            drain_timeout=drain_timeout,
            broadcast_leave=broadcast_leave,
        )
    
    async def _send_xprobe_to_gate(self, target: tuple[str, int], data: bytes) -> bool:
        """
        Send a cross-cluster probe to a gate.
        
        Used by FederatedHealthMonitor for gate health checking.
        """
        try:
            await self.send(target, data, timeout=5)
            return True
        except Exception:
            return False
    
    def _on_gate_health_change(self, datacenter: str, new_health: str) -> None:
        """
        Called when gate cluster health status changes.
        
        Logs the change and updates internal tracking.
        """
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate cluster health changed to {new_health}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def _handle_xack_response(
        self,
        source_addr: tuple[str, int] | bytes,
        ack_data: bytes,
    ) -> None:
        """
        Handle a cross-cluster health acknowledgment from a gate.
        
        Passes the ack to the FederatedHealthMonitor for processing.
        """
        try:
            ack = CrossClusterAck.load(ack_data)
            self._gate_health_monitor.handle_ack(ack)
            
            # Update gate leader info if this is a leader response
            if ack.is_leader:
                addr = source_addr if isinstance(source_addr, tuple) else None
                if addr:
                    self._gate_health_monitor.update_leader(
                        datacenter="gate-cluster",
                        leader_udp_addr=addr,
                        leader_node_id=ack.node_id,
                        leader_term=ack.leader_term,
                    )
        except Exception as e:
            await self.handle_exception(e, "handle_xack_response")
    
    def _is_gate_circuit_open(self) -> bool:
        """Check if gate circuit breaker is open (fail-fast mode)."""
        return self._gate_circuit.circuit_state == CircuitState.OPEN
    
    def get_gate_circuit_status(self) -> dict:
        """
        Get current gate circuit breaker status.
        
        Returns a dict with:
        - circuit_state: Current state (CLOSED, OPEN, HALF_OPEN)
        - error_count: Recent error count
        - error_rate: Error rate over window
        - healthy_gates: Count of healthy gates
        - primary_gate: Current primary gate ID
        """
        return {
            "circuit_state": self._gate_circuit.circuit_state.name,
            "error_count": self._gate_circuit.error_count,
            "error_rate": self._gate_circuit.error_rate,
            "healthy_gates": len(self._healthy_gate_ids),
            "primary_gate": self._primary_gate_id,
        }

    def _get_swim_status_for_worker(self, addr: tuple[str, int]) -> str | None:
        """
        Get SWIM health status for a worker by UDP address.

        This callback is used by WorkerPool to integrate with SWIM health tracking.

        Args:
            addr: (host, udp_port) tuple for the worker

        Returns:
            'OK' if healthy, 'SUSPECT' if suspect, 'DEAD' if dead, None if unknown
        """
        node_state = self._incarnation_tracker.get_node_state(addr)
        if not node_state:
            return None

        status = node_state.status
        if isinstance(status, bytes):
            status = status.decode('utf-8', errors='replace')

        return status

    def _get_healthy_worker_ids(self) -> list[str]:
        """
        Get list of worker IDs that are healthy according to WorkerPool.

        A worker is healthy if:
        1. SWIM reports it as 'OK' (alive), OR
        2. It was recently registered (within grace period) and hasn't been marked dead

        The grace period handles the startup race where workers register but SWIM
        probing hasn't completed yet.
        """
        return self._worker_pool.get_healthy_worker_ids()
    
    def _get_total_cores(self) -> int:
        """Get total cores across all registered workers."""
        return sum(worker.total_cores for worker in self._worker_pool.iter_workers())
    
    def _get_available_cores_for_healthy_workers(self) -> int:
        """
        Get available cores only from healthy workers.

        This is the source of truth for datacenter "BUSY" state:
        - If this returns 0 but we have healthy workers → BUSY
        - If we have no healthy workers → DEGRADED/UNHEALTHY
        """
        return self._worker_pool.get_total_available_cores()
    
    def _get_total_available_cores(self) -> int:
        """Get total available cores across all healthy workers for priority calculation."""
        return self._get_available_cores_for_healthy_workers()
    
    async def _build_xprobe_response(
        self,
        source_addr: tuple[str, int] | bytes,
        probe_data: bytes,
    ) -> bytes | None:
        """
        Build response to cross-cluster health probe from a gate.
        
        Returns aggregate datacenter health for the gate to track.
        Only responds if we are the DC leader.
        """
        from hyperscale.distributed_rewrite.swim.health import CrossClusterAck
        
        # Only DC leader responds to xprobes
        if not self.is_leader():
            return None
        
        # Get health metrics
        healthy_worker_ids = self._get_healthy_worker_ids()
        healthy_workers = len(healthy_worker_ids)
        total_workers = len(self._workers)
        total_cores = self._get_total_cores()
        available_cores = self._get_available_cores_for_healthy_workers()
        
        # Count active jobs/workflows
        active_jobs = self._job_manager.job_count
        active_workflows = sum(
            len(job.workflows) for job in self._job_manager.iter_jobs()
        )
        
        # Determine DC health status
        dc_health = self._classify_dc_health(
            healthy_workers, total_workers, available_cores, total_cores
        )
        
        # Count healthy managers in cluster (from SWIM)
        nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        cluster_size = 1  # Self
        healthy_managers = 1  # Self
        
        if nodes:
            for node_addr, data in nodes.items():
                if node_addr != self_addr:
                    cluster_size += 1
                    if isinstance(data, tuple) and len(data) >= 2:
                        _, status = data[:2]
                        if status == b'OK':
                            healthy_managers += 1
        
        ack = CrossClusterAck(
            datacenter=self._node_id.datacenter,
            node_id=self._node_id.full,
            incarnation=self._external_incarnation,
            is_leader=True,
            leader_term=self._leader_election.state.current_term,
            cluster_size=cluster_size,
            healthy_managers=healthy_managers,
            worker_count=total_workers,
            healthy_workers=healthy_workers,
            total_cores=total_cores,
            available_cores=available_cores,
            active_jobs=active_jobs,
            active_workflows=active_workflows,
            dc_health=dc_health,
        )
        
        return ack.dump()
    
    def _classify_dc_health(
        self,
        healthy_workers: int,
        total_workers: int,
        available_cores: int,
        total_cores: int,
    ) -> str:
        """Classify datacenter health based on worker status."""
        if total_workers == 0:
            return "UNHEALTHY"
        
        if healthy_workers == 0:
            return "UNHEALTHY"
        
        # Majority workers unhealthy = DEGRADED
        if healthy_workers < (total_workers / 2):
            return "DEGRADED"
        
        # No available cores = BUSY
        if available_cores == 0 and healthy_workers > 0:
            return "BUSY"
        
        return "HEALTHY"
    
    def _get_workflow_priority(self, workflow) -> StagePriority:
        """
        Get the priority of a workflow.
        
        Workflows can specify priority via a 'priority' attribute.
        If not specified, defaults to AUTO.
        """
        priority_attr = getattr(workflow, 'priority', None)
        if priority_attr is None:
            return StagePriority.AUTO
        
        if isinstance(priority_attr, StagePriority):
            return priority_attr
        
        if isinstance(priority_attr, str):
            return StagePriority.map(priority_attr.lower())
        
        return StagePriority.AUTO
    
    def _is_test_workflow(self, workflow) -> bool:
        """
        Determine if a workflow is a test workflow.
        
        A workflow is considered a test workflow if it has any hooks
        with hook_type == HookType.TEST.
        """
        import inspect
        from hyperscale.core.hooks import Hook
        
        for name, member in inspect.getmembers(workflow):
            if isinstance(member, Hook) and member.hook_type == HookType.TEST:
                return True
        return False
    
    def _calculate_layer_cores(
        self,
        layer_workflows: list[str],
        workflow_by_name: dict[str, tuple[int, Any]],
        workflow_priorities: dict[str, StagePriority],
        workflow_is_test: dict[str, bool],
        total_pool: int,
    ) -> tuple[dict[str, int], list[str]]:
        """
        Calculate cores for workflows in a single layer based on priority.

        Priority allocation rules:
        1. EXCLUSIVE workflows get 100% of pool and run sequentially (first-come first-serve)
        2. Specific priority workflows (HIGH, NORMAL, LOW) get allocated first based on ranges
        3. AUTO workflows split remaining cores evenly
        4. If all workflows are AUTO, split cores evenly among them
        5. Non-test workflows always get 1 core (they don't parallelize)

        Args:
            layer_workflows: Names of workflows in this layer
            workflow_by_name: Map of name -> (index, workflow)
            workflow_priorities: Map of name -> StagePriority
            workflow_is_test: Map of name -> is_test_workflow
            total_pool: Total available cores

        Returns:
            Tuple of:
            - workflow_cores: Map of name -> cores allocated (for concurrent dispatch)
            - exclusive_order: List of EXCLUSIVE workflow names to run sequentially
        """
        workflow_cores: dict[str, int] = {}
        exclusive_order: list[str] = []

        if not layer_workflows:
            return workflow_cores, exclusive_order

        # Categorize workflows
        exclusive_workflows: list[str] = []
        specific_priority_workflows: list[str] = []  # HIGH, NORMAL, LOW
        auto_workflows: list[str] = []
        non_test_workflows: list[str] = []

        for name in layer_workflows:
            if not workflow_is_test.get(name, False):
                non_test_workflows.append(name)
                continue

            priority = workflow_priorities.get(name, StagePriority.AUTO)
            if priority == StagePriority.EXCLUSIVE:
                exclusive_workflows.append(name)
            elif priority == StagePriority.AUTO:
                auto_workflows.append(name)
            else:
                specific_priority_workflows.append(name)

        # Non-test workflows always get 1 core
        for name in non_test_workflows:
            workflow_cores[name] = 1

        # EXCLUSIVE workflows run sequentially with full pool
        # Return them in exclusive_order for sequential dispatch
        if exclusive_workflows:
            exclusive_order = exclusive_workflows
            # Each EXCLUSIVE workflow gets full pool when it runs
            for name in exclusive_workflows:
                workflow_cores[name] = total_pool
            # Other workflows in this layer must wait - don't allocate cores
            # (They'll be dispatched after EXCLUSIVE workflows complete)
            return workflow_cores, exclusive_order

        # Calculate remaining pool after non-test allocations
        remaining_pool = total_pool - len(non_test_workflows)
        if remaining_pool <= 0:
            remaining_pool = 1

        # Allocate specific priority workflows first (HIGH > NORMAL > LOW)
        # Sort by priority descending
        specific_priority_workflows.sort(
            key=lambda n: workflow_priorities.get(n, StagePriority.AUTO).value,
            reverse=True
        )

        for name in specific_priority_workflows:
            priority = workflow_priorities.get(name, StagePriority.AUTO)
            min_cores, max_cores = StagePriority.get_worker_allocation_range(priority, total_pool)
            # Allocate up to max, but leave at least 1 core for remaining workflows
            others_remaining = len(specific_priority_workflows) + len(auto_workflows) - len(workflow_cores) - 1
            reserved_for_others = max(others_remaining, 0)
            available = remaining_pool - reserved_for_others
            cores = max(min(available, max_cores), min_cores, 1)
            workflow_cores[name] = cores
            remaining_pool -= cores

        # Divide remaining cores evenly among AUTO workflows
        if auto_workflows:
            if remaining_pool <= 0:
                remaining_pool = len(auto_workflows)  # At least 1 core each

            cores_per_auto = remaining_pool // len(auto_workflows)
            extra_cores = remaining_pool % len(auto_workflows)

            for i, name in enumerate(auto_workflows):
                # Distribute extra cores to first few workflows
                cores = cores_per_auto + (1 if i < extra_cores else 0)
                workflow_cores[name] = max(cores, 1)

        return workflow_cores, exclusive_order
    
    # =========================================================================
    # Job Leader Helpers (Context Consistency Protocol)
    # =========================================================================
    
    def _is_job_leader(self, job_id: str) -> bool:
        """Check if this manager is the leader for the given job."""
        return self._job_leaders.get(job_id) == self._node_id.full
    
    def _get_job_leader(self, job_id: str) -> str | None:
        """Get the node_id of the job leader, or None if unknown."""
        return self._job_leaders.get(job_id)

    def _get_job_leader_addr(self, job_id: str) -> tuple[str, int] | None:
        """Get the TCP address of the job leader, or None if unknown."""
        return self._job_leader_addrs.get(job_id)

    async def _broadcast_job_leadership(
        self,
        job_id: str,
        workflow_count: int,
        workflow_names: list[str] | None = None,
    ) -> None:
        """
        Broadcast job leadership announcement to all peer managers.

        This ensures all managers in the cluster know who is leading
        a specific job, enabling proper routing of workflow results
        and allowing non-leaders to respond to workflow queries.
        """
        announcement = JobLeadershipAnnouncement(
            job_id=job_id,
            leader_id=self._node_id.full,
            leader_host=self._host,
            leader_tcp_port=self._tcp_port,
            term=self._leader_election.state.current_term,
            workflow_count=workflow_count,
            timestamp=time.monotonic(),
            workflow_names=workflow_names or [],
        )

        # Get all peer manager addresses
        peer_addrs = self._get_active_peer_tcp_addrs()

        for peer_addr in peer_addrs:
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

    def _get_job_context(self, job_id: str) -> Context | None:
        """Get the context for a job, or None if job unknown."""
        return self._job_contexts.get(job_id)
    
    def _get_next_context_timestamp(self) -> int:
        """Get the next Lamport timestamp for context updates."""
        self._context_lamport_clock += 1
        return self._context_lamport_clock
    
    def _build_manager_heartbeat(self) -> ManagerHeartbeat:
        """Build a ManagerHeartbeat with current state."""
        healthy_worker_ids = self._worker_pool.get_healthy_worker_ids()
        all_workers = self._worker_pool.iter_workers()

        # Build job leadership info for jobs we lead
        # Maps job_id -> (fencing_token, layer_version)
        job_leaderships: dict[str, tuple[int, int]] = {}
        for job_id, leader_id in self._job_leaders.items():
            if leader_id == self._node_id.full:
                fencing_token = self._job_fencing_tokens.get(job_id, 0)
                layer_version = self._job_layer_version.get(job_id, 0)
                job_leaderships[job_id] = (fencing_token, layer_version)

        return ManagerHeartbeat(
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            version=self._state_version,
            active_jobs=self._job_manager.job_count,
            active_workflows=sum(
                len(job.workflows) for job in self._job_manager.iter_jobs()
            ),
            worker_count=len(all_workers),
            healthy_worker_count=len(healthy_worker_ids),
            available_cores=self._worker_pool.get_total_available_cores(),
            total_cores=sum(worker.total_cores for worker in all_workers),
            state=self._manager_state.value,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
            job_leaderships=job_leaderships,
        )
    
    async def _gate_heartbeat_loop(self) -> None:
        """
        Periodically send ManagerHeartbeat to gates via TCP.

        This supplements the Serf-style SWIM embedding for reliability.
        Gates use this for datacenter health classification.

        Heartbeat interval is configurable via Env.MANAGER_HEARTBEAT_INTERVAL.
        """
        heartbeat_interval = self.env.MANAGER_HEARTBEAT_INTERVAL

        self._task_runner.run(
            self._udp_logger.log,
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
                gate_addrs = self._get_healthy_gate_tcp_addrs() or self._gate_addrs
                
                sent_count = 0
                for gate_addr in gate_addrs:
                    try:
                        response, _ = await self.send_tcp(
                            gate_addr,
                            "manager_status_update",
                            heartbeat.dump(),
                            timeout=2.0,
                        )
                        if isinstance(response, Exception):
                            self._task_runner.run(
                                self._udp_logger.log,
                                ServerWarning(
                                    message=f"Heartbeat to gate {gate_addr} failed: {response}",
                                    node_host=self._host,
                                    node_port=self._tcp_port,
                                    node_id=self._node_id.short,
                                )
                            )
                        else:
                            sent_count += 1
                    except Exception as e:
                        # Gate might be down - continue to others
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerWarning(
                                message=f"Heartbeat to gate {gate_addr} exception: {e}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )
                
                if sent_count > 0:
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"Sent heartbeat to {sent_count}/{len(gate_addrs)} gates (workers={heartbeat.worker_count}, cores={heartbeat.available_cores})",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.handle_exception(e, "gate_heartbeat_loop")
    
    async def _send_job_progress_to_gate(
        self,
        job: JobProgress,
        max_retries: int = 2,
        base_delay: float = 0.2,
    ) -> None:
        """
        Send job progress to the primary gate and process ack.
        
        Uses limited retries with exponential backoff:
        - Progress updates can be frequent, so we keep retries short
        - Attempt 1: immediate
        - Attempt 2: 0.2s delay
        - Attempt 3: 0.4s delay
        
        The gate responds with JobProgressAck containing updated
        gate topology which we use to maintain redundant channels.
        
        Args:
            job: Job progress to send
            max_retries: Maximum retry attempts (default 2)
            base_delay: Base delay for exponential backoff (default 0.2s)
        """
        # Check circuit breaker first
        if self._is_gate_circuit_open():
            return  # Fail fast
        
        gate_addr = self._get_primary_gate_tcp_addr()
        if not gate_addr:
            # Fallback to first seed gate
            if self._gate_addrs:
                gate_addr = self._gate_addrs[0]
            else:
                return
        
        for attempt in range(max_retries + 1):
            try:
                response, _ = await self.send_tcp(
                    gate_addr,
                    "job_progress",
                    job.dump(),
                    timeout=2.0,
                )
                
                # Process ack to update gate topology
                if response and isinstance(response, bytes) and response != b'error':
                    self._process_job_progress_ack(response)
                    self._gate_circuit.record_success()
                    return  # Success
                    
            except Exception:
                pass
            
            # Exponential backoff before retry (except after last attempt)
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
        
        # All retries exhausted
        self._gate_circuit.record_error()
    
    async def _send_job_progress_to_all_gates(self, job: JobProgress) -> None:
        """
        Send job progress to ALL healthy gates and process acks.
        
        Used for critical updates to ensure all gates receive the update.
        """
        gate_addrs = self._get_healthy_gate_tcp_addrs() or self._gate_addrs
        
        for gate_addr in gate_addrs:
            try:
                response, _ = await self.send_tcp(
                    gate_addr,
                    "job_progress",
                    job.dump(),
                    timeout=2.0,
                )
                
                # Process ack to update gate topology
                if response and isinstance(response, bytes) and response != b'error':
                    self._process_job_progress_ack(response)
                    
            except Exception:
                pass
    
    def _get_state_snapshot(self) -> ManagerStateSnapshot:
        """Get a complete state snapshot."""
        worker_snapshots = []
        for worker in self._worker_pool.iter_workers():
            if worker.registration:
                heartbeat_version = worker.heartbeat.version if worker.heartbeat else 0
                worker_snapshots.append(WorkerStateSnapshot(
                    node_id=worker.node_id,
                    state=worker.state,
                    total_cores=worker.total_cores,
                    available_cores=worker.available_cores,
                    version=heartbeat_version,
                    # Include host/port for registration reconstruction
                    host=worker.registration.node.host,
                    tcp_port=worker.registration.node.port,
                    udp_port=worker.registration.node.udp_port,
                    active_workflows={},  # Could populate from tracking
                ))
        
        # Serialize job contexts for state sync
        contexts_data = {}
        # Snapshot to avoid dict mutation during iteration
        for job_id, context in list(self._job_contexts.items()):
            contexts_data[job_id] = context.dict()
        
        return ManagerStateSnapshot(
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            version=self._state_version,
            workers=worker_snapshots,
            jobs=self._job_manager.get_jobs_as_wire_progress(),
            job_leaders=dict(self._job_leaders),
            job_leader_addrs=dict(self._job_leader_addrs),
            job_layer_versions=dict(self._job_layer_version),
            job_contexts=cloudpickle.dumps(contexts_data),
        )
    
    def _get_worker_circuit(self, worker_id: str) -> ErrorStats:
        """
        Get or create a circuit breaker for a specific worker.
        
        Each worker has its own circuit breaker so that failures to one
        worker don't affect dispatch to other workers.
        """
        if worker_id not in self._worker_circuits:
            cb_config = self.env.get_circuit_breaker_config()
            self._worker_circuits[worker_id] = ErrorStats(
                max_errors=cb_config['max_errors'],
                window_seconds=cb_config['window_seconds'],
                half_open_after=cb_config['half_open_after'],
            )
        return self._worker_circuits[worker_id]
    
    def _is_worker_circuit_open(self, worker_id: str) -> bool:
        """Check if a worker's circuit breaker is open."""
        circuit = self._worker_circuits.get(worker_id)
        if not circuit:
            return False
        return circuit.circuit_state == CircuitState.OPEN
    
    def get_worker_circuit_status(self, worker_id: str) -> dict | None:
        """
        Get circuit breaker status for a specific worker.
        
        Returns None if worker has no circuit breaker (never had failures).
        """
        circuit = self._worker_circuits.get(worker_id)
        if not circuit:
            return None
        return {
            "worker_id": worker_id,
            "circuit_state": circuit.circuit_state.name,
            "error_count": circuit.error_count,
            "error_rate": circuit.error_rate,
        }
    
    def get_all_worker_circuit_status(self) -> dict:
        """Get circuit breaker status for all workers."""
        return {
            "workers": {
                worker_id: self.get_worker_circuit_status(worker_id)
                for worker_id in self._worker_circuits.keys()
            },
            "open_circuits": [
                worker_id for worker_id in self._worker_circuits.keys()
                if self._is_worker_circuit_open(worker_id)
            ],
        }
    
    def _get_fence_token(self) -> int:
        """
        Generate a fence token for at-most-once delivery.
        
        Uses monotonic increasing state version as the token.
        """
        return self._state_version
    
    async def _extract_dependency_context(
        self,
        job_id: str,
        workflow: Any,
    ) -> bytes:
        """
        Extract context values for workflow dependencies.
        
        Returns cloudpickled dict of context values that this workflow
        may need from its dependencies.
        """
        
        job_context = self._job_contexts.get(job_id)
        if not job_context:
            return cloudpickle.dumps({})
        
        # For now, return the full context dict
        # A more sophisticated approach would filter based on @state() decorators
        try:
            context_dict = job_context.dict()
            return cloudpickle.dumps(context_dict)
        except Exception:
            return cloudpickle.dumps({})
    
    def _select_worker_for_workflow(self, vus_needed: int) -> str | None:
        """
        Select a worker with sufficient capacity for a workflow.

        Uses cryptographically secure random selection among eligible workers.
        Also checks SWIM membership - only select workers that are ALIVE.
        Skips workers with open circuit breakers.
        """
        eligible = []
        for worker in self._worker_pool.iter_workers():
            node_id = worker.node_id

            # Check circuit breaker - skip workers with open circuits
            if self._is_worker_circuit_open(node_id):
                continue

            # Check capacity (available minus already reserved)
            effective_available = worker.available_cores - worker.reserved_cores
            if effective_available < vus_needed:
                continue

            # Check health via WorkerPool
            if not self._worker_pool.is_worker_healthy(node_id):
                continue

            eligible.append(node_id)

        if not eligible:
            return None

        # Cryptographically secure selection
        return secrets.choice(eligible)

    async def _send_workflow_dispatch(
        self,
        worker_node_id: str,
        dispatch: WorkflowDispatch,
    ) -> bool:
        """
        Send a workflow dispatch to a worker and return success status.

        This is a simple wrapper around _dispatch_workflow_to_worker that
        returns True/False for use by the WorkflowDispatcher callback.

        Args:
            worker_node_id: Target worker node ID
            dispatch: WorkflowDispatch message to send

        Returns:
            True if the worker accepted the dispatch, False otherwise
        """
        ack = await self._dispatch_workflow_to_worker(worker_node_id, dispatch)
        return ack is not None and ack.accepted

    async def _dispatch_workflow_to_worker(
        self,
        worker_node_id: str,
        dispatch: WorkflowDispatch,
        max_retries: int = 2,
        base_delay: float = 0.3,
    ) -> WorkflowDispatchAck | None:
        """
        Dispatch a workflow to a specific worker.

        Uses retries with exponential backoff:
        - Attempt 1: immediate
        - Attempt 2: 0.3s delay
        - Attempt 3: 0.6s delay
        
        Checks and updates the per-worker circuit breaker.
        
        Args:
            worker_node_id: Target worker node ID
            dispatch: Workflow dispatch message
            max_retries: Maximum retry attempts (default 2)
            base_delay: Base delay for exponential backoff (default 0.3s)
            
        Returns:
            WorkflowDispatchAck if accepted, None otherwise
        """
        # Check circuit breaker first
        if self._is_worker_circuit_open(worker_node_id):
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Cannot dispatch to worker {worker_node_id}: circuit breaker is OPEN",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return None

        # =================================================================
        # Get worker address from WorkerPool (new system) or legacy dict
        # =================================================================
        worker_addr = None
        worker_pool_info = self._worker_pool.get_worker(worker_node_id)
        if worker_pool_info:
            worker_addr = (
                worker_pool_info.registration.node.host,
                worker_pool_info.registration.node.port,
            )
        else:
            # Legacy fallback
            worker = self._workers.get(worker_node_id)
            if worker:
                worker_addr = (worker.node.host, worker.node.port)

        if not worker_addr:
            return None

        circuit = self._get_worker_circuit(worker_node_id)
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Sending TCP to worker at {worker_addr}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        for attempt in range(max_retries + 1):
            try:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"TCP send attempt {attempt + 1} to {worker_addr}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                response, _ = await self.send_tcp(
                    worker_addr,
                    "workflow_dispatch",
                    dispatch.dump(),
                    timeout=5.0,
                )
                
                if isinstance(response, bytes):
                    ack = WorkflowDispatchAck.load(response)
                    if ack.accepted:
                        circuit.record_success()
                        if attempt > 0:
                            self._task_runner.run(
                                self._udp_logger.log,
                                ServerInfo(
                                    message=f"Dispatched to worker {worker_node_id} after {attempt + 1} attempts",
                                    node_host=self._host,
                                    node_port=self._tcp_port,
                                    node_id=self._node_id.short,
                                )
                            )
                        return ack
                    else:
                        # Worker rejected - don't retry (not a transient error)
                        circuit.record_error()
                        return ack
                        
            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Dispatch attempt {attempt + 1}/{max_retries + 1} to {worker_node_id} failed: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            
            # Exponential backoff before retry (except after last attempt)
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
        
        # All retries exhausted
        circuit.record_error()
        return None
    
    async def _request_quorum_confirmation(
        self,
        provision: ProvisionRequest,
    ) -> bool:
        """
        Request quorum confirmation for a provisioning decision.
        
        Uses circuit breaker pattern to fail fast when quorum is repeatedly
        unavailable. This prevents cascading failures when the cluster is
        in a degraded state.
        
        Returns True if quorum is achieved, False otherwise.
        
        Raises:
            QuorumCircuitOpenError: Circuit breaker is open due to repeated failures
            QuorumUnavailableError: Not enough active managers for quorum
        """
        # Check circuit breaker first - fail fast if too many recent failures
        circuit_state = self._quorum_circuit.circuit_state
        if circuit_state == CircuitState.OPEN:
            # Calculate retry time
            retry_after = self._quorum_circuit.half_open_after
            if self._quorum_circuit._circuit_opened_at:
                elapsed = time.monotonic() - self._quorum_circuit._circuit_opened_at
                retry_after = max(0.0, self._quorum_circuit.half_open_after - elapsed)
            
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Quorum circuit breaker OPEN - failing fast (retry in {retry_after:.1f}s)",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            raise QuorumCircuitOpenError(
                recent_failures=self._quorum_circuit.error_count,
                window_seconds=self._quorum_circuit.window_seconds,
                retry_after_seconds=retry_after,
            )
        
        # Check if quorum is even possible
        if not self._has_quorum_available():
            active_count = len(self._active_manager_peers) + 1
            required = self._quorum_size
            
            # Record failure for circuit breaker
            self._quorum_circuit.record_error()
            
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Quorum unavailable: {active_count} active, need {required}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            raise QuorumUnavailableError(
                active_managers=active_count,
                required_quorum=required,
            )
        
        self._pending_provisions[provision.workflow_id] = provision
        self._provision_confirmations[provision.workflow_id] = {self._node_id.full}  # Self-confirm

        # Send to all peers
        peer_addrs = self._get_active_peer_tcp_addrs()
        confirm_tasks = []
        for peer in peer_addrs:
            confirm_tasks.append(
                self._request_confirmation_from_peer(peer, provision)
            )
        
        # Wait for responses with timeout
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*confirm_tasks, return_exceptions=True),
                timeout=self._quorum_timeout,
            )
            
            # Check if we have quorum
            confirmed = self._provision_confirmations.get(provision.workflow_id, set())
            quorum_achieved = len(confirmed) >= self._quorum_size
            
            if quorum_achieved:
                # Success - record for circuit breaker recovery
                self._quorum_circuit.record_success()
                return True
            else:
                # Failed to get quorum
                self._quorum_circuit.record_error()
                raise QuorumTimeoutError(
                    confirmations_received=len(confirmed),
                    required_quorum=self._quorum_size,
                    timeout=self._quorum_timeout,
                )
            
        except asyncio.TimeoutError:
            confirmed = self._provision_confirmations.get(provision.workflow_id, set())
            quorum_achieved = len(confirmed) >= self._quorum_size
            
            if quorum_achieved:
                self._quorum_circuit.record_success()
                return True
            else:
                self._quorum_circuit.record_error()
                raise QuorumTimeoutError(
                    confirmations_received=len(confirmed),
                    required_quorum=self._quorum_size,
                    timeout=self._quorum_timeout,
                )
        finally:
            # Cleanup
            self._pending_provisions.pop(provision.workflow_id, None)
            self._provision_confirmations.pop(provision.workflow_id, None)
    
    async def _request_confirmation_from_peer(
        self,
        peer: tuple[str, int],
        provision: ProvisionRequest,
    ) -> bool:
        """Request confirmation from a single peer."""
        try:
            response, _ = await self.send_tcp(
                peer,
                "provision_request",
                provision.dump(),
                timeout=self._quorum_timeout / 2,
            )
            
            if isinstance(response, bytes):
                confirm = ProvisionConfirm.load(response)
                if confirm.confirmed:
                    self._provision_confirmations[provision.workflow_id].add(confirm.confirming_node)
                    return True
            return False
            
        except Exception as e:
            await self.handle_exception(e, f"confirm_from_peer_{peer}")
            return False
    
    async def _send_provision_commit(
        self,
        provision: ProvisionRequest,
    ) -> None:
        """Send commit message to all managers after quorum achieved."""
        commit = ProvisionCommit(
            job_id=provision.job_id,
            workflow_id=provision.workflow_id,
            target_worker=provision.target_worker,
            cores_assigned=provision.cores_required,
            fence_token=provision.fence_token,
            committed_version=self._state_version,
        )

        for peer in self._get_active_peer_tcp_addrs():
            try:
                await self.send_tcp(
                    peer,
                    "provision_commit",
                    commit.dump(),
                    timeout=2.0,
                )
            except Exception:
                # Commit is best-effort after quorum
                pass
    
    # =========================================================================
    # TCP Handlers - Worker Registration and Heartbeats
    # =========================================================================
    
    @tcp.send('worker_register_ack')
    async def send_worker_register_ack(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send worker registration ack."""
        return (addr, data, timeout)
    
    @tcp.handle('worker_register_ack')
    async def handle_worker_register_ack_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw worker register ack."""
        return data
    
    @tcp.send('worker_discovery')
    async def send_worker_discovery(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send worker discovery broadcast to peer manager."""
        return (addr, data, timeout)
    
    @tcp.handle('worker_discovery')
    async def handle_worker_discovery_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw worker discovery response."""
        return data

    @tcp.send('manager_peer_register')
    async def send_manager_peer_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send manager peer registration to another manager."""
        return (addr, data, timeout)

    @tcp.handle('manager_peer_register')
    async def handle_manager_peer_register_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle manager peer registration response."""
        return data

    @tcp.receive()
    async def worker_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle worker registration via TCP."""
        try:
            registration = WorkerRegistration.load(data)

            # Register with WorkerPool
            worker_info = await self._worker_pool.register_worker(registration)

            self._increment_version()

            # Signal that cores are available - wake up any waiting workflows
            if registration.available_cores > 0:
                self._cores_available_event.set()
                # Also notify WorkflowDispatcher for event-driven dispatch
                if self._workflow_dispatcher:
                    self._workflow_dispatcher.signal_cores_available()

            # Add worker to SWIM cluster for UDP healthchecks
            worker_udp_addr = (registration.node.host, registration.node.port)
            self._probe_scheduler.add_member(worker_udp_addr)

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Worker registered: {worker_info.node_id} with {worker_info.total_cores} cores (SWIM probe added)",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Return response with list of all healthy managers
            response = RegistrationResponse(
                accepted=True,
                manager_id=self._node_id.full,
                healthy_managers=self._get_healthy_managers(),
            )

            # Broadcast this worker discovery to peer managers
            worker_addr = (registration.node.host, registration.node.port)
            self._task_runner.run(
                self._broadcast_worker_discovery,
                registration.node.node_id,
                worker_addr,
                worker_addr,  # UDP addr same as TCP for workers
                registration.total_cores,
            )

            return response.dump()
            
        except Exception as e:
            await self.handle_exception(e, "worker_register")
            # Return error response
            response = RegistrationResponse(
                accepted=False,
                manager_id=self._node_id.full,
                healthy_managers=[],
                error=str(e),
            )
            return response.dump()

    @tcp.receive()
    async def manager_peer_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle registration from a peer manager.

        When another manager discovers us (via seed list or SWIM),
        it sends a registration to establish bidirectional relationship.
        """
        try:
            registration = ManagerPeerRegistration.load(data)
            peer_info = registration.node

            # Add to known peers if not already tracked
            if peer_info.node_id not in self._known_manager_peers:
                self._known_manager_peers[peer_info.node_id] = peer_info
                self._active_manager_peer_ids.add(peer_info.node_id)

                # Update mappings
                udp_addr = (peer_info.udp_host, peer_info.udp_port)
                tcp_addr = (peer_info.tcp_host, peer_info.tcp_port)
                self._manager_udp_to_tcp[udp_addr] = tcp_addr
                self._active_manager_peers.add(tcp_addr)

                # Add to SWIM probing
                self._probe_scheduler.add_member(udp_addr)

                # Also populate _manager_peer_info so _get_active_manager_peer_addrs() works
                # This creates an initial heartbeat entry that will be updated by SWIM
                initial_heartbeat = ManagerHeartbeat(
                    node_id=peer_info.node_id,
                    datacenter=peer_info.datacenter,
                    is_leader=registration.is_leader,
                    term=registration.term,
                    version=0,  # Will be updated by real heartbeats
                    active_jobs=0,
                    active_workflows=0,
                    worker_count=0,
                    healthy_worker_count=0,
                    available_cores=0,
                    total_cores=0,
                    state=ManagerState.ACTIVE.value,  # Assume active since they're registering
                    tcp_host=peer_info.tcp_host,
                    tcp_port=peer_info.tcp_port,
                    udp_host=peer_info.udp_host,
                    udp_port=peer_info.udp_port,
                )
                self._manager_peer_info[udp_addr] = initial_heartbeat

                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Peer manager registered: {peer_info.node_id} (leader={registration.is_leader})",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

            # Build response with all known peers (including self and the registrant)
            all_peers = [self._get_self_manager_info()] + self._get_known_peer_managers()

            response = ManagerPeerRegistrationResponse(
                accepted=True,
                manager_id=self._node_id.full,
                is_leader=self.is_leader(),
                term=self._leader_election.state.current_term,
                known_peers=all_peers,
            )
            return response.dump()

        except Exception as e:
            await self.handle_exception(e, "manager_peer_register")
            response = ManagerPeerRegistrationResponse(
                accepted=False,
                manager_id=self._node_id.full,
                is_leader=self.is_leader(),
                term=self._leader_election.state.current_term,
                known_peers=[],
                error=str(e),
            )
            return response.dump()

    @tcp.receive()
    async def worker_discovery(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle worker discovery broadcast from a peer manager.

        When another manager receives a worker registration, it broadcasts
        to all peers. This handler schedules direct registration with the
        worker to get accurate, up-to-date info.
        """
        try:
            broadcast = WorkerDiscoveryBroadcast.load(data)

            worker_id = broadcast.worker_id
            worker_tcp_addr = tuple(broadcast.worker_tcp_addr)
            worker_udp_addr = tuple(broadcast.worker_udp_addr)

            # Skip if already registered - direct registration takes precedence
            if worker_id in self._workers:
                return b'ok'

            # Schedule registration with the worker to get accurate info
            # Don't blindly trust broadcast data - reach out to the worker directly
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

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Scheduling registration with worker {worker_id[:8]}... (discovered via {broadcast.source_manager_id[:8]}...)",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            return b'ok'

        except Exception as e:
            await self.handle_exception(e, "worker_discovery")
            return b'error'
    
    @tcp.receive()
    async def receive_worker_status_update(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle worker status update via TCP.

        This is NOT a healthcheck - liveness is tracked via SWIM UDP probes.
        This contains capacity and workflow progress information.
        """
        try:
            heartbeat = WorkerHeartbeat.load(data)

            # Process heartbeat via WorkerPool
            await self._worker_pool.process_heartbeat(heartbeat.node_id, heartbeat)

            return b'ok'

        except Exception as e:
            await self.handle_exception(e, "receive_worker_status_update")
            return b'error'
    
    @tcp.receive()
    async def workflow_progress(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle workflow progress update from worker.

        Key feature: Uses cores_completed to enable faster provisioning.
        When a worker reports that some cores have finished their portion
        of a workflow, we can immediately consider those cores available
        for new workflows, without waiting for the entire workflow to complete.

        Multi-worker dispatch: When a workflow is split across multiple workers,
        each worker sends progress with a sub-workflow ID (job_id:workflow_idx:worker_idx).
        We aggregate these into unified parent workflow progress before forwarding.
        """
        try:
            progress = WorkflowProgress.load(data)

            # =================================================================
            # Forward to job leader if we're not the leader
            # =================================================================
            # Progress updates should be processed by the job leader who has the state
            if not self._is_job_leader(progress.job_id):
                leader_addr = self._get_job_leader_addr(progress.job_id)
                if leader_addr:
                    try:
                        response, _ = await self.send_tcp(
                            leader_addr,
                            "workflow_progress",
                            data,  # Forward the raw data
                            timeout=2.0,
                        )
                        return response if response else b'ok'
                    except Exception:
                        pass  # Fall through to process locally as best effort
                # Fall through - maybe we have the job locally anyway

            # Check if this is a sub-workflow (dispatched to multiple workers)
            parent_workflow_id = self._get_parent_workflow_id(progress.workflow_id)

            # Log progress for debugging
            await self._udp_logger.log(
                ServerError(
                    message=f"[workflow_progress] Processing: workflow_id={progress.workflow_id} status={progress.status}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            if parent_workflow_id is not None:
                # This is a sub-workflow - update SubWorkflowInfo.progress in JobManager
                await self._job_manager.update_workflow_progress(progress.workflow_id, progress)

                # Update worker available cores based on cores_completed
                await self._update_worker_cores_from_progress(progress, None)

                # Aggregate progress from all sub-workflows
                aggregated_progress = self._aggregate_sub_workflow_progress(parent_workflow_id)
                if aggregated_progress is None:
                    # No progress to aggregate yet
                    ack = WorkflowProgressAck(
                        manager_id=self._node_id.full,
                        is_leader=self.is_leader(),
                        healthy_managers=self._get_healthy_managers(),
                    )
                    return ack.dump()

                # Use aggregated progress for job updates
                progress = aggregated_progress

            # Update job state via JobManager
            job = self._job_manager.get_job_by_id(progress.job_id)
            if job:
                # Update WorkflowInfo state based on progress status
                # Extract workflow_id from progress.workflow_id which may be a 5-part token
                # Format: DC:manager:job_id:workflow_id:worker_id
                parts = progress.workflow_id.split(":")
                if len(parts) >= 5:
                    workflow_id = parts[3]  # Extract just the workflow_id component (e.g., "wf-0001")
                else:
                    workflow_id = progress.workflow_id
                wf_info = job.workflows.get(
                    str(self._job_manager.create_workflow_token(progress.job_id, workflow_id))
                )
                if wf_info:
                    # Convert progress status string to WorkflowStatus enum
                    try:
                        new_status = WorkflowStatus(progress.status)
                    except ValueError:
                        new_status = WorkflowStatus.RUNNING
                    # Use state machine to advance status (prevents regression)
                    wf_info.status = WorkflowStateMachine.advance_state(wf_info.status, new_status)

                job.timestamp = time.monotonic()

                # Update worker available cores based on cores_completed (for single-worker case)
                if parent_workflow_id is None:
                    await self._update_worker_cores_from_progress(progress, None)

                self._increment_version()

                # Handle workflow completion states
                if progress.status == WorkflowStatus.FAILED.value:
                    # Check if workflow should be retried
                    await self._handle_workflow_failure(progress)
                elif progress.status == WorkflowStatus.COMPLETED.value:
                    # Clean up retry tracking on success
                    self._workflow_retries.pop(progress.workflow_id, None)

                    # Signal completion for dependency tracking
                    completion_event = self._workflow_completion_events.get(progress.workflow_id)
                    if completion_event:
                        completion_event.set()

                    # Notify WorkflowDispatcher of completion for dependency tracking
                    if self._workflow_dispatcher:
                        # Parse job_id from workflow_id (format: DC:manager:job_id:workflow_id:worker_id)
                        parts = progress.workflow_id.split(":")
                        if len(parts) >= 5:
                            jm_job_id = parts[2]  # job_id is the 3rd component
                            # Find matching workflow_id in JobManager
                            # Note: Use get_job_by_id(), not get_job() - the latter expects a full token string
                            job_info = self._job_manager.get_job_by_id(jm_job_id)
                            if job_info:
                                for wf_id, wf_info in job_info.workflows.items():
                                    if wf_info.name == progress.workflow_name:
                                        self._task_runner.run(
                                            self._workflow_dispatcher.mark_workflow_completed,
                                            jm_job_id,
                                            wf_id,
                                        )
                                        # Try to dispatch newly ready workflows
                                        submission = self._job_submissions.get(jm_job_id)
                                        if submission:
                                            self._task_runner.run(
                                                self._workflow_dispatcher.try_dispatch,
                                                jm_job_id,
                                                submission,
                                            )
                                        break

                # Forward job progress to gates (if connected)
                if self._known_gates or self._gate_addrs:
                    self._task_runner.run(self._send_job_progress_to_gate, job)

                # Check for job completion and push to client (if no gates)
                if not (self._known_gates or self._gate_addrs):
                    self._check_job_completion(progress.job_id)
            
            # Return ack with current manager topology for worker to update
            ack = WorkflowProgressAck(
                manager_id=self._node_id.full,
                is_leader=self.is_leader(),
                healthy_managers=self._get_healthy_managers(),
            )
            return ack.dump()
            
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            await self.handle_exception(e, "receive_workflow_progress")
            return b'error'
    
    @tcp.receive()
    async def workflow_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle workflow final result from worker.

        This is the critical path for workflow completion:
        1. Store the final result
        2. Process context updates for dependent workflows
        3. Check job completion
        4. Forward to gates or clients if appropriate

        Multi-worker dispatch: When a workflow is split across multiple workers,
        each worker sends a final result with a sub-workflow ID. We aggregate
        these using Results.merge_results() when all sub-workflows complete.
        """
        try:
            result = WorkflowFinalResult.load(data)

            await self._udp_logger.log(
                ServerError(
                    message=f"[workflow_final_result] RECEIVED: workflow_id={result.workflow_id} status={result.status}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # =================================================================
            # Forward to job leader if we're not the leader
            # =================================================================
            # The job state (workflows, sub-workflows) only exists on the job leader.
            # If a worker sends a result to the wrong manager, forward it.
            if not self._is_job_leader(result.job_id):
                leader_addr = self._get_job_leader_addr(result.job_id)
                if leader_addr:
                    await self._udp_logger.log(
                        ServerError(
                            message=f"[workflow_final_result] Forwarding to job leader at {leader_addr} (we are not leader for job {result.job_id})",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                    try:
                        response, _ = await self.send_tcp(
                            leader_addr,
                            "workflow_final_result",
                            data,  # Forward the raw data
                            timeout=5.0,
                        )
                        return response if response else b'ok'
                    except Exception as forward_err:
                        await self._udp_logger.log(
                            ServerError(
                                message=f"[workflow_final_result] Failed to forward to leader: {forward_err}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )
                        return b'error'
                else:
                    await self._udp_logger.log(
                        ServerError(
                            message=f"[workflow_final_result] Not job leader and no leader addr known for job {result.job_id}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                    # Fall through - maybe we have the job locally anyway

            # =================================================================
            # Record result in JobManager (new system)
            # =================================================================
            # Parse the workflow_id to extract job_id and workflow components
            # Format: DC:manager:job_id:workflow_id:worker_id (5 parts)
            parts = result.workflow_id.split(":")
            if len(parts) >= 5:
                jm_job_id = parts[2]  # job_id is the 3rd component
                jm_workflow_id = parts[3]  # workflow_id is the 4th component (e.g., "wf-0001")
                # Try to find the workflow in JobManager by job_id
                # Note: Use get_job_by_id(), not get_job() - the latter expects a full token string
                job_info = self._job_manager.get_job_by_id(jm_job_id)
                if job_info:
                    # Determine status based on result status
                    new_status = WorkflowStatus.COMPLETED if result.status == WorkflowStatus.COMPLETED.value else WorkflowStatus.FAILED

                    # Find matching workflow by workflow_id (parts[3] is workflow_id like "wf-0001")
                    workflow_token_str = str(self._job_manager.create_workflow_token(jm_job_id, jm_workflow_id))
                    wf_info = job_info.workflows.get(workflow_token_str)
                    if wf_info:
                        await self._job_manager.update_workflow_status(
                            jm_job_id, workflow_token_str, new_status
                        )
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerDebug(
                                message=f"JobManager: Updated workflow {workflow_token_str} to status {new_status.value}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            )
                        )

            # Check if this is a sub-workflow (dispatched to multiple workers)
            parent_workflow_id = self._get_parent_workflow_id(result.workflow_id)

            # Use try/finally to ensure lock is always released
            # This prevents lock leaks from early returns
            await self._workflow_results_locks[parent_workflow_id].acquire()
            try:
                # Update worker's available cores via WorkerPool
                if result.worker_id and result.worker_available_cores >= 0:
                    updated = await self._worker_pool.update_worker_cores_from_progress(
                        result.worker_id, result.worker_available_cores
                    )
                    if updated and result.worker_available_cores > 0:
                        self._cores_available_event.set()
                        if self._workflow_dispatcher:
                            self._workflow_dispatcher.signal_cores_available()

                # Store final result in JobManager first
                recorded, parent_complete = await self._job_manager.record_sub_workflow_result(result.workflow_id, result)
                await self._udp_logger.log(
                    ServerError(
                        message=f"[workflow_final_result] record_sub_workflow_result returned: recorded={recorded}, parent_complete={parent_complete}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

                if parent_workflow_id is not None:
                    # This is a sub-workflow - check if parent is complete

                    # Handle context updates from sub-workflow
                    if result.context_updates and len(result.context_updates) > 0:
                        if self._is_job_leader(result.job_id):
                            await self._apply_context_updates_from_result(result)
                        else:
                            await self._forward_context_from_result(result)

                    # Check if all sub-workflows have completed
                    if not self._is_parent_workflow_complete(parent_workflow_id):
                        # More sub-workflows pending - just ack
                        return b'ok'

                # Handle context updates (for dependent workflows) - only for non-sub-workflows
                # Sub-workflows already had context applied above
                if parent_workflow_id is None and result.context_updates and len(result.context_updates) > 0:
                    if self._is_job_leader(result.job_id):
                        # We are job leader - apply context directly
                        await self._apply_context_updates_from_result(result)
                    else:
                        # Forward context to job leader
                        await self._forward_context_from_result(result)

                # Clean up retry tracking on any final result
                self._workflow_retries.pop(result.workflow_id, None)

                # Signal completion for dependency tracking
                completion_event = self._workflow_completion_events.get(result.workflow_id)
                if completion_event:
                    completion_event.set()

                # Update job progress status via JobManager
                # Parse the workflow_id from the sub-workflow token
                parts = result.workflow_id.split(":")
                if len(parts) >= 5:
                    jm_job_id = parts[2]  # job_id is the 3rd component
                    jm_workflow_id = parts[3]  # workflow_id is the 4th component (e.g., "wf-0001")

                    job = self._job_manager.get_job_by_id(jm_job_id)
                    if job:
                        # Find workflow by constructing the proper token
                        workflow_token_str = str(self._job_manager.create_workflow_token(jm_job_id, jm_workflow_id))
                        wf_info = job.workflows.get(workflow_token_str)
                        if wf_info:
                            # Convert result status to WorkflowStatus
                            try:
                                new_status = WorkflowStatus(result.status)
                                wf_info.status = new_status
                                await self._udp_logger.log(
                                    ServerError(
                                        message=f"Updated workflow status: {jm_workflow_id} -> {result.status}",
                                        node_host=self._host,
                                        node_port=self._tcp_port,
                                        node_id=self._node_id.short,
                                    )
                                )
                            except ValueError:
                                pass  # Invalid status, keep current

                        # Forward to gates (if connected)
                        if self._known_gates or self._gate_addrs:
                            self._task_runner.run(self._send_job_progress_to_gate, job)

                        # Notify WorkflowDispatcher of completion/failure for dependency tracking
                        if self._workflow_dispatcher:
                            if result.status == WorkflowStatus.COMPLETED.value:
                                # Workflow completed successfully - notify dependents
                                await self._workflow_dispatcher.mark_workflow_completed(
                                    jm_job_id, jm_workflow_id
                                )
                                # Try to dispatch newly ready workflows
                                submission = self._job_submissions.get(jm_job_id)
                                if submission:
                                    await self._workflow_dispatcher.try_dispatch(
                                        jm_job_id, submission
                                    )
                            elif result.status == WorkflowStatus.FAILED.value:
                                # Workflow failed - fail all dependents
                                await self._workflow_dispatcher.mark_workflow_failed(
                                    jm_job_id, jm_workflow_id
                                )

                # Check if job is complete
                if self._is_job_complete(result.job_id):
                    await self._handle_job_completion(result.job_id)

                self._increment_version()
                return b'ok'

            finally:
                # Always release the lock, even on early returns or exceptions
                self._workflow_results_locks[parent_workflow_id].release()

        except Exception as e:
            await self.handle_exception(e, "workflow_final_result")
            return b'error'
    
    async def _apply_context_updates_from_result(self, result: WorkflowFinalResult) -> None:
        """Apply context updates from a workflow final result."""
        try:
            context_dict = cloudpickle.loads(result.context_updates)
            if context_dict:
                context = self._get_job_context(result.job_id)
                if context is None:
                    context = Context()
                    self._job_contexts[result.job_id] = context
                
                for key, value in context_dict.items():
                    await context.update(
                        result.workflow_name,
                        key,
                        value,
                        timestamp=self._get_next_context_timestamp(),
                        source_node=self._node_id.full,
                    )
        except Exception as e:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Failed to apply context from result {result.workflow_id}: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _forward_context_from_result(self, result: WorkflowFinalResult) -> None:
        """Forward context updates to the job leader."""
        leader_addr = self._get_job_leader_addr(result.job_id)
        if not leader_addr:
            # Try to find leader by ID
            leader_id = self._get_job_leader(result.job_id)
            if leader_id:
                for manager in list(self._known_manager_peers.values()):
                    if manager.node_id == leader_id:
                        leader_addr = (manager.tcp_host, manager.tcp_port)
                        break

        if not leader_addr:
            # Check peers as fallback
            peer_addrs = self._get_active_peer_tcp_addrs()
            if peer_addrs:
                leader_addr = peer_addrs[0]

        if leader_addr:
            forward = ContextForward(
                job_id=result.job_id,
                workflow_id=result.workflow_id,
                context_updates=result.context_updates,
                context_timestamps=b'',  # Timestamps handled by leader on apply
                source_manager=self._node_id.full,
            )
            try:
                await self.send_tcp(
                    leader_addr,
                    "context_forward",
                    forward.dump(),
                    timeout=2.0,
                )
            except Exception:
                pass

    def _is_job_complete(self, job_id: str) -> bool:
        """Check if all workflows in a job have completed."""
        # Note: Use get_job_by_id(), not get_job() - the latter expects a full token string
        job_info = self._job_manager.get_job_by_id(job_id)
        if not job_info or not job_info.workflows:
            return False

        return all(
            wf.status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED,
                          WorkflowStatus.AGGREGATED, WorkflowStatus.AGGREGATION_FAILED)
            for wf in job_info.workflows.values()
        )

    def _get_parent_workflow_id(self, sub_workflow_id: str) -> str | None:
        """
        Extract parent workflow ID from a sub-workflow ID.

        Sub-workflow IDs have format: DC:manager:job_id:workflow_id:worker_id (5 parts)
        Parent workflow IDs have format: DC:manager:job_id:workflow_id (4 parts)

        Returns None if this is not a sub-workflow (fewer than 5 parts).
        """
        parts = sub_workflow_id.split(":")
        if len(parts) >= 5:
            # Has worker_id suffix (5 parts), return parent (4 parts, without worker_id)
            return ":".join(parts[:-1])
        return None

    def _is_parent_workflow_complete(self, parent_workflow_id: str) -> bool:
        """
        Check if all sub-workflows for a parent workflow have completed.

        Returns True if all sub-workflows have final results stored.
        """
        # Get job from workflow token
        job = self._job_manager.get_job_for_workflow(parent_workflow_id)
        if not job:
            return True

        # Find sub-workflows for this parent workflow
        parent_sub_workflows = [
            sub_wf for sub_wf in job.sub_workflows.values()
            if str(sub_wf.parent_token) == parent_workflow_id
        ]

        if not parent_sub_workflows:
            # No sub-workflows tracked - might be single-worker dispatch
            return True

        # Check if all have results
        return all(sub_wf.result is not None for sub_wf in parent_sub_workflows)

    def _aggregate_sub_workflow_progress(self, parent_workflow_id: str) -> WorkflowProgress | None:
        """
        Aggregate progress updates from all sub-workflows into a unified progress.

        Combines:
        - completed_count: sum across all sub-workflows
        - failed_count: sum across all sub-workflows
        - rate_per_second: sum of rates
        - cores_completed: sum of completed cores
        - step_stats: merged by step name
        - avg_cpu_percent: weighted average by cores
        - avg_memory_mb: sum across all

        Returns None if no progress available.

        Uses the new JobManager system to get sub-workflow data.
        """
        # Find job_id from parent workflow_id (format: job_id:workflow_idx)
        job_id = parent_workflow_id.rsplit(":", 1)[0] if ":" in parent_workflow_id else parent_workflow_id

        # Get job and workflow info from JobManager
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return None

        # Find the parent workflow by workflow_id
        workflow_token_str = str(self._job_manager.create_workflow_token(job_id, parent_workflow_id))
        wf_info = job.workflows.get(workflow_token_str)
        if not wf_info:
            return None

        # Get sub-workflow tokens from WorkflowInfo
        sub_workflow_tokens = wf_info.sub_workflow_tokens
        if not sub_workflow_tokens:
            return None

        # Collect progress from SubWorkflowInfo objects
        progress_updates = [
            job.sub_workflows[token].progress
            for token in sub_workflow_tokens
            if token in job.sub_workflows and job.sub_workflows[token].progress is not None
        ]

        if not progress_updates:
            return None

        # Aggregate counts
        total_completed = sum(p.completed_count for p in progress_updates)
        total_failed = sum(p.failed_count for p in progress_updates)
        total_rate = sum(p.rate_per_second for p in progress_updates)
        max_elapsed = max(p.elapsed_seconds for p in progress_updates)
        total_cores_completed = sum(p.cores_completed for p in progress_updates)

        # Aggregate CPU/memory (weighted by assigned cores)
        total_cores = sum(len(p.assigned_cores) for p in progress_updates if p.assigned_cores)
        if total_cores > 0:
            avg_cpu = sum(
                p.avg_cpu_percent * len(p.assigned_cores)
                for p in progress_updates
                if p.assigned_cores
            ) / total_cores
        else:
            avg_cpu = sum(p.avg_cpu_percent for p in progress_updates) / len(progress_updates)

        total_memory = sum(p.avg_memory_mb for p in progress_updates)

        # Merge step stats by step name
        step_stats_by_name: dict[str, StepStats] = {}
        for p in progress_updates:
            for step in p.step_stats:
                if step.step_name in step_stats_by_name:
                    existing = step_stats_by_name[step.step_name]
                    step_stats_by_name[step.step_name] = StepStats(
                        step_name=step.step_name,
                        completed_count=existing.completed_count + step.completed_count,
                        failed_count=existing.failed_count + step.failed_count,
                        total_count=existing.total_count + step.total_count,
                    )
                else:
                    step_stats_by_name[step.step_name] = StepStats(
                        step_name=step.step_name,
                        completed_count=step.completed_count,
                        failed_count=step.failed_count,
                        total_count=step.total_count,
                    )

        # Determine overall status (worst case wins)
        status = WorkflowStatus.RUNNING.value
        for p in progress_updates:
            if p.status == WorkflowStatus.FAILED.value:
                status = WorkflowStatus.FAILED.value
                break
            elif p.status == WorkflowStatus.COMPLETED.value:
                # Only set completed if all are completed
                if all(up.status == WorkflowStatus.COMPLETED.value for up in progress_updates):
                    status = WorkflowStatus.COMPLETED.value

        # Collect all assigned cores
        all_cores = []
        for p in progress_updates:
            all_cores.extend(p.assigned_cores)

        return WorkflowProgress(
            job_id=job_id,
            workflow_id=parent_workflow_id,
            workflow_name=progress_updates[0].workflow_name,
            status=status,
            completed_count=total_completed,
            failed_count=total_failed,
            rate_per_second=total_rate,
            elapsed_seconds=max_elapsed,
            step_stats=list(step_stats_by_name.values()),
            timestamp=max(p.timestamp for p in progress_updates),
            assigned_cores=all_cores,
            cores_completed=total_cores_completed,
            avg_cpu_percent=avg_cpu,
            avg_memory_mb=total_memory,
        )

    def _compute_job_overall_rate(self, job_id: str) -> float:
        """
        Compute the overall rate for a job by aggregating sub-workflow progress.

        Sums up rate_per_second from all sub-workflows belonging to this job.

        Uses the new JobManager system to get sub-workflow data.

        Args:
            job_id: The job identifier

        Returns:
            Aggregate rate (requests/second) across all workflows
        """
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return 0.0

        total_rate = 0.0
        for sub_wf in job.sub_workflows.values():
            if sub_wf.progress:
                total_rate += sub_wf.progress.rate_per_second
        return total_rate

    def _aggregate_sub_workflow_final_results(
        self,
        parent_workflow_id: str,
    ) -> WorkflowFinalResult | None:
        """
        Aggregate final results from all sub-workflows into a unified result.

        Uses Results.merge_results() to combine WorkflowResults from all sub-workflows.
        This follows the same pattern as RemoteGraphManager.

        Args:
            parent_workflow_id: 4-part workflow token (DC:manager:job_id:workflow_id)

        Returns None if aggregation fails.
        """
        try:
            # Get job from workflow token
            job = self._job_manager.get_job_for_workflow(parent_workflow_id)
            if not job:
                return None

            # Get workflow info to access the workflow instance
            wf_info = job.workflows.get(parent_workflow_id)
            if not wf_info:
                return None

            # Find sub-workflows for this parent workflow
            parent_sub_workflows = [
                sub_wf for sub_wf in job.sub_workflows.values()
                if str(sub_wf.parent_token) == parent_workflow_id
            ]

            if not parent_sub_workflows:
                return None

            # Collect all sub-workflow results
            sub_results = [
                sub_wf.result for sub_wf in parent_sub_workflows
                if sub_wf.result is not None
            ]

            if not sub_results or len(sub_results) != len(parent_sub_workflows):
                # Not all sub-workflows have completed
                return None

            # Determine overall status (any failure = failure)
            overall_status = WorkflowStatus.COMPLETED.value
            errors = []
            for r in sub_results:
                if r.status == WorkflowStatus.FAILED.value:
                    overall_status = WorkflowStatus.FAILED.value
                    if r.error:
                        errors.append(r.error)

            # Unpack and merge WorkflowResults from all sub-workflows
            workflow_stats_list = []
            for r in sub_results:
                # Skip empty results (e.g., from failed workflows)
                if not r.results or len(r.results) == 0:
                    continue
                try:
                    workflow_stats_list.extend(r.results.values())
                except Exception:
                    # Skip malformed results
                    pass

            # Get workflow instance for hooks
            workflow = wf_info.workflow
            if workflow is None:
                return None

            hooks: dict[str, Hook] = {
                name: hook
                for name, hook in inspect.getmembers(
                    workflow,
                    predicate=lambda member: isinstance(member, Hook),
                )
            }

            # Merge results using Results helper (same pattern as RemoteGraphManager)
            if len(workflow_stats_list) > 1:
                results_helper = Results(hooks)
                merged_stats = results_helper.merge_results(workflow_stats_list)
            elif len(workflow_stats_list) == 1:
                merged_stats = workflow_stats_list[0]
            else:
                # No valid stats - create empty result
                merged_stats = {
                    "workflow": sub_results[0].workflow_name,
                    "stats": {},
                    "results": [],
                    "checks": [],
                    "metrics": [],
                }

            # Merge context updates from all sub-workflows
            merged_context = {}
            for r in sub_results:
                if r.context_updates and len(r.context_updates) > 0:
                    try:
                        ctx = cloudpickle.loads(r.context_updates)
                        if ctx:
                            merged_context.update(ctx)
                    except Exception:
                        pass

            # Create aggregated final result
            return WorkflowFinalResult(
                job_id=job.job_id,
                workflow_id=parent_workflow_id,
                workflow_name=sub_results[0].workflow_name,
                status=overall_status,
                results=cloudpickle.dumps(merged_stats),
                context_updates=cloudpickle.dumps(merged_context) if merged_context else b'',
                error="; ".join(errors) if errors else None,
            )

        except Exception:
            import traceback
            print(traceback.format_exc())
            return None

    async def _handle_job_completion(self, job_id: str) -> None:
        """Handle job completion - build and send JobFinalResult."""
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return

        # Collect results from sub_workflows
        errors: list[str] = []
        has_failures = False
        max_elapsed = 0.0
        workflow_results: list[WorkflowResult] = []

        for sub_wf in job.sub_workflows.values():
            wf_result = sub_wf.result
            if wf_result:
                if wf_result.status == WorkflowStatus.FAILED.value:
                    has_failures = True
                    if wf_result.error:
                        errors.append(f"{wf_result.workflow_name}: {wf_result.error}")

                workflow_results.append(WorkflowResult(
                    workflow_id=str(sub_wf.token),
                    workflow_name=wf_result.workflow_name,
                    status=wf_result.status,
                    results=wf_result.results,
                    error=wf_result.error,
                ))

            # Calculate max elapsed from progress
            if sub_wf.progress and sub_wf.progress.elapsed_seconds > max_elapsed:
                max_elapsed = sub_wf.progress.elapsed_seconds

        # Determine final status
        result_count = len(workflow_results)
        if has_failures:
            job_status = JobStatus.FAILED.value if len(errors) == result_count else "PARTIAL"
        else:
            job_status = JobStatus.COMPLETED.value

        job.status = job_status
        job.elapsed_seconds = max_elapsed
        job.timestamp = time.monotonic()

        # Extract completion counts from WorkflowStats if progress-based counts are zero
        total_completed = job.workflows_completed
        total_failed = job.workflows_failed

        if total_completed == 0 and total_failed == 0:
            for sub_wf in job.sub_workflows.values():
                wf_result = sub_wf.result
                if wf_result and wf_result.results and len(wf_result.results) > 0:
                    try:
                        workflow_stats = cloudpickle.loads(wf_result.results)
                        if isinstance(workflow_stats, dict):
                            stats = workflow_stats.get("stats", {})
                            total_completed += stats.get("succeeded", 0) or 0
                            total_failed += stats.get("failed", 0) or 0
                    except Exception:
                        pass

        # Build JobFinalResult
        job_final = JobFinalResult(
            job_id=job_id,
            datacenter=self._node_id.datacenter,
            status=job_status,
            workflow_results=workflow_results,
            total_completed=total_completed,
            total_failed=total_failed,
            errors=errors,
            elapsed_seconds=max_elapsed,
        )
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Job {job_id} completed with status={job_status}, {len(workflow_results)} workflows",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Send to gates (if connected)
        if self._known_gates or self._gate_addrs:
            await self._send_job_final_result_to_gates(job_final)
        
        # Send directly to client (if no gates and callback registered)
        callback = self._job_callbacks.get(job_id)
        if callback and not (self._known_gates or self._gate_addrs):
            await self._send_job_final_result_to_client(job_final, callback)

    async def _send_job_final_result_to_gates(self, job_final: JobFinalResult) -> None:
        """Send JobFinalResult to all known gates."""
        for gate_addr in self._gate_addrs:
            try:
                await self.send_tcp(
                    gate_addr,
                    "job_final_result",
                    job_final.dump(),
                    timeout=5.0,
                )
            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Failed to send job final result to gate {gate_addr}: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
    
    async def _send_job_final_result_to_client(
        self,
        job_final: JobFinalResult,
        callback: tuple[str, int],
    ) -> None:
        """Send JobFinalResult directly to client (when no gates)."""
        try:
            await self.send_tcp(
                callback,
                "job_final_result",
                job_final.dump(),
                timeout=5.0,
            )
        except Exception as e:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Failed to send job final result to client {callback}: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
    
    # =========================================================================
    # Context Forwarding (Context Consistency Protocol)
    # =========================================================================
    
    @tcp.receive()
    async def context_forward(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle context forwarded from a non-leader manager.
        
        Only the job leader should receive these messages. The leader applies
        the context updates using LWW conflict resolution.
        """
        try:
            forward = ContextForward.load(data)
            
            # Verify we are the job leader
            if not self._is_job_leader(forward.job_id):
                # We're not the leader - this shouldn't happen normally
                # Log and return error
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Received context_forward but not job leader for {forward.job_id}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return b'not_leader'
            
            # Apply the context updates
            await self._apply_context_updates(
                forward.job_id,
                forward.workflow_id,
                forward.context_updates,
                forward.context_timestamps,
            )
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "context_forward")
            return b'error'
    
    async def _apply_context_updates(
        self,
        job_id: str,
        workflow_id: str,
        updates_bytes: bytes,
        timestamps_bytes: bytes,
    ) -> None:
        """
        Apply context updates from a completed workflow.
        
        Uses LWW conflict resolution with Lamport timestamps.
        Only the job leader should call this directly; non-leaders forward.
        """
        context = self._job_contexts.get(job_id)
        if not context:
            # Create context if missing (shouldn't happen normally)
            context = Context()
            self._job_contexts[job_id] = context
        
        # Deserialize updates
        updates = cloudpickle.loads(updates_bytes)
        timestamps = cloudpickle.loads(timestamps_bytes) if timestamps_bytes else {}
        
        # Get workflow name from ID (for context keying)
        workflow_name = self._get_workflow_name_from_id(workflow_id)
        
        # Apply each update with LWW
        for key, value in updates.items():
            timestamp = timestamps.get(key, self._get_next_context_timestamp())
            await context.update(
                workflow_name,
                key,
                value,
                timestamp=timestamp,
                source_node=self._node_id.full,
            )
    
    def _get_workflow_name_from_id(self, workflow_id: str) -> str:
        """
        Get the workflow name from a workflow ID.

        Workflow IDs are typically formatted as job_id:workflow_name or similar.
        This extracts the name portion for context keying.
        """
        # Try to find in JobInfo.workflows (dict[str, WorkflowInfo])
        for job in self._job_manager.iter_jobs():
            for wf_info in job.workflows.values():
                if wf_info.token.workflow_id == workflow_id:
                    return wf_info.name

        # Fallback: use the ID itself
        return workflow_id
    
    async def _extract_dependency_context(
        self,
        job_id: str,
        workflow: Any,
    ) -> bytes:
        """
        Extract context from workflow dependencies.
        
        For dependent workflows, this extracts only the context values
        from their dependencies, not the full job context.
        
        Args:
            job_id: The job ID
            workflow: The workflow object (may be DependentWorkflow)
        
        Returns:
            Serialized dependency context (cloudpickle bytes)
        """
        context = self._job_contexts.get(job_id)
        if not context:
            return b''
        
        # Check if workflow has dependencies
        dependencies = []
        if isinstance(workflow, DependentWorkflow):
            dependencies = [dep.__name__ for dep in workflow.dependencies]
        elif hasattr(workflow, 'dependencies') and workflow.dependencies:
            dependencies = [dep.__name__ for dep in workflow.dependencies]
        
        if not dependencies:
            # No dependencies - no context needed
            return b''
        
        # Extract context for each dependency
        relevant_context = {}
        for dep_name in dependencies:
            if dep_name in context:
                relevant_context[dep_name] = context[dep_name].dict()
        
        if not relevant_context:
            return b''
        
        return cloudpickle.dumps(relevant_context)
    
    def _get_manager_tcp_addr(self, node_id: str) -> tuple[str, int] | None:
        """Get the TCP address for a manager by node_id."""
        # Check _known_manager_peers first (keyed by node_id)
        peer_info = self._known_manager_peers.get(node_id)
        if peer_info:
            return (peer_info.tcp_host, peer_info.tcp_port)

        # Fallback: search _manager_peer_info (keyed by UDP addr) for matching node_id
        for udp_addr, heartbeat in list(self._manager_peer_info.items()):
            if heartbeat.node_id == node_id:
                return (heartbeat.tcp_host, heartbeat.tcp_port)

        return None
    
    async def _sync_context_and_advance(self, job_id: str) -> bool:
        """
        Sync context to peer managers and advance to next layer.
        
        Called by job leader when a layer completes. This:
        1. Increments the layer version
        2. Creates a context snapshot
        3. Broadcasts to all peer managers
        4. Waits for quorum confirmation
        5. Returns True if quorum reached, False otherwise
        
        IMPORTANT: Only call this when you are the job leader.
        """
        if not self._is_job_leader(job_id):
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"_sync_context_and_advance called but not job leader for {job_id}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False
        
        # Check circuit breaker
        if self._quorum_circuit.circuit_state == CircuitState.OPEN:
            raise QuorumCircuitOpenError("Context sync circuit breaker is open")
        
        # Increment layer version
        new_version = self._job_layer_version.get(job_id, 0) + 1
        self._job_layer_version[job_id] = new_version
        
        # Create context snapshot
        context = self._job_contexts.get(job_id)
        if not context:
            context = Context()
            self._job_contexts[job_id] = context
        
        context_snapshot = cloudpickle.dumps(context.dict())
        
        sync_msg = ContextLayerSync(
            job_id=job_id,
            layer_version=new_version,
            context_snapshot=context_snapshot,
            source_node_id=self._node_id.full,
        )
        
        # Get peer managers to sync with
        peer_addrs = self._get_active_manager_peer_addrs()
        if not peer_addrs:
            # No peers - we are the only manager, sync trivially succeeds
            return True
        
        # Calculate quorum (majority of active managers including self)
        total_managers = len(peer_addrs) + 1  # +1 for self
        quorum_needed = (total_managers // 2) + 1
        confirmations = 1  # Count self
        
        # Broadcast to peers with timeout
        sync_tasks = []
        for peer_addr in peer_addrs:
            sync_tasks.append(
                self._send_context_sync_to_peer(peer_addr, sync_msg)
            )
        
        # Wait for responses with timeout
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*sync_tasks, return_exceptions=True),
                timeout=self._quorum_timeout,
            )
            
            # Count successful confirmations
            for result in results:
                if isinstance(result, bool) and result:
                    confirmations += 1
            
        except asyncio.TimeoutError:
            # Partial results - count what we got
            pass
        
        # Check if quorum reached
        if confirmations >= quorum_needed:
            self._quorum_circuit.record_success()
            self._task_runner.run(
                self._udp_logger.log,
                ServerDebug(
                    message=f"Context sync quorum reached for job {job_id} layer {new_version}: {confirmations}/{total_managers}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return True
        else:
            self._quorum_circuit.record_error()
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Context sync quorum failed for job {job_id} layer {new_version}: {confirmations}/{quorum_needed} needed",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            raise QuorumTimeoutError(
                f"Context sync quorum failed: got {confirmations}, need {quorum_needed}"
            )
    
    async def _send_context_sync_to_peer(
        self,
        peer_addr: tuple[str, int],
        sync_msg: ContextLayerSync,
    ) -> bool:
        """Send context sync to a peer and return True if acked."""
        try:
            response, _ = await self.send_tcp(
                peer_addr,
                action='context_layer_sync',
                data=sync_msg.dump(),
                timeout=self._quorum_timeout / 2,  # Leave time for retries
            )
            
            if response and not isinstance(response, Exception):
                ack = ContextLayerSyncAck.load(response)
                return ack.applied
            return False
            
        except Exception:
            return False
    
    def _get_active_manager_peer_addrs(self) -> list[tuple[str, int]]:
        """Get TCP addresses of active peer managers."""
        addrs = []
        for udp_addr, heartbeat in list(self._manager_peer_info.items()):
            if heartbeat.node_id == self._node_id.full:
                continue  # Skip self
            # Only include active managers (not SYNCING)
            if heartbeat.state == ManagerState.ACTIVE.value:
                addrs.append((heartbeat.tcp_host, heartbeat.tcp_port))
        return addrs
    
    @tcp.receive()
    async def context_layer_sync(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle context layer sync from job leader.
        
        The job leader broadcasts this at layer completion to ensure all
        managers have the latest context before dependent workflows dispatch.
        """
        try:
            sync = ContextLayerSync.load(data)
            
            # Check if this is a newer layer version
            current_version = self._job_layer_version.get(sync.job_id, -1)
            if sync.layer_version <= current_version:
                # Stale sync - already have this or newer
                ack = ContextLayerSyncAck(
                    job_id=sync.job_id,
                    layer_version=sync.layer_version,
                    applied=False,
                    responder_id=self._node_id.full,
                )
                return ack.dump()
            
            # Apply the context snapshot
            context_dict = cloudpickle.loads(sync.context_snapshot)
            
            # Create or update context
            if sync.job_id not in self._job_contexts:
                self._job_contexts[sync.job_id] = Context()
            
            context = self._job_contexts[sync.job_id]
            for workflow_name, values in context_dict.items():
                await context.from_dict(workflow_name, values)
            
            # Update layer version
            self._job_layer_version[sync.job_id] = sync.layer_version
            
            # Update job leader if not set
            if sync.job_id not in self._job_leaders:
                self._job_leaders[sync.job_id] = sync.source_node_id
            
            ack = ContextLayerSyncAck(
                job_id=sync.job_id,
                layer_version=sync.layer_version,
                applied=True,
                responder_id=self._node_id.full,
            )
            return ack.dump()
            
        except Exception as e:
            await self.handle_exception(e, "context_layer_sync")
            ack = ContextLayerSyncAck(
                job_id="unknown",
                layer_version=-1,
                applied=False,
                responder_id=self._node_id.full,
            )
            return ack.dump()
    
    def _aggregate_step_stats(
        self,
        workflows: list[WorkflowProgress],
    ) -> list[StepStats]:
        """
        Aggregate step stats from all workflows in a job.
        
        Merges stats with the same step_name, summing counts.
        
        Args:
            workflows: List of workflow progress updates
            
        Returns:
            Aggregated list of StepStats
        """
        # Merge by step_name
        stats_by_name: dict[str, dict[str, int]] = {}
        
        for workflow in workflows:
            for step_stat in workflow.step_stats:
                if step_stat.step_name not in stats_by_name:
                    stats_by_name[step_stat.step_name] = {
                        "completed": 0,
                        "failed": 0,
                        "total": 0,
                    }
                stats_by_name[step_stat.step_name]["completed"] += step_stat.completed_count
                stats_by_name[step_stat.step_name]["failed"] += step_stat.failed_count
                stats_by_name[step_stat.step_name]["total"] += step_stat.total_count
        
        # Convert back to StepStats
        return [
            StepStats(
                step_name=name,
                completed_count=stats["completed"],
                failed_count=stats["failed"],
                total_count=stats["total"],
            )
            for name, stats in stats_by_name.items()
        ]
    
    async def _update_worker_cores_from_progress(
        self,
        progress: WorkflowProgress,
        old_progress: WorkflowProgress | None,
    ) -> None:
        """
        Update worker available cores based on workflow progress.

        Uses JobManager to look up the sub-workflow and get the worker_id,
        then updates WorkerPool with the worker's reported available cores.

        Args:
            progress: New progress update
            old_progress: Previous progress (if any)
        """
        workflow_id = progress.workflow_id

        # Look up the sub-workflow in JobManager to get the worker_id
        job = self._job_manager.get_job_for_sub_workflow(workflow_id)
        if not job:
            return

        sub_wf = job.sub_workflows.get(workflow_id)
        if not sub_wf or not sub_wf.worker_id:
            return

        worker_id = sub_wf.worker_id

        # Update WorkerPool with the worker's reported availability
        updated = await self._worker_pool.update_worker_cores_from_progress(
            worker_id,
            progress.worker_available_cores,
        )

        if updated and progress.worker_available_cores > 0:
            # Signal cores available for event-driven dispatch
            self._cores_available_event.set()
            if self._workflow_dispatcher:
                self._workflow_dispatcher.signal_cores_available()

    # =========================================================================
    # Client Push Notifications (when gates not present)
    # =========================================================================
    
    async def _push_job_status_to_client(
        self,
        job_id: str,
        event_type: str,
    ) -> None:
        """
        Push job status to client callback (Tier 1 immediate update).
        
        Used when manager receives jobs directly from clients (no gates).
        Pushes JobStatusPush for critical events like completion/failure.
        """
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return
        
        callback = self._job_callbacks.get(job_id)
        if not callback:
            return  # No callback registered
        
        is_final = job.status in (
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
        )
        
        push = JobStatusPush(
            job_id=job_id,
            status=job.status,
            message=event_type,
            total_completed=job.workflows_completed,
            total_failed=job.workflows_failed,
            overall_rate=self._compute_job_overall_rate(job_id),
            elapsed_seconds=time.monotonic() - job.timestamp,
            is_final=is_final,
        )
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Job {job_id}: pushing {event_type} to client {callback}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        try:
            await self.send_tcp(
                callback,
                "job_status_push",
                push.dump(),
                timeout=2.0,
            )
        except Exception:
            # Client unreachable - don't block
            pass
        
        # Clean up callback if job is final
        if is_final:
            self._job_callbacks.pop(job_id, None)
    
    async def _push_batch_stats_to_clients(self) -> None:
        """
        Push batched stats to all clients with callbacks (Tier 2 periodic update).
        
        Called periodically to send progress updates to clients.
        """
        # Collect running jobs with callbacks
        jobs_with_callbacks = []
        for job in self._job_manager.iter_jobs():
            if job.status == JobStatus.RUNNING.value:
                callback = self._job_callbacks.get(job.job_id)
                if callback:
                    jobs_with_callbacks.append((job.job_id, job, callback))
        
        if not jobs_with_callbacks:
            return
        
        for job_id, job, callback in jobs_with_callbacks:
            batch_push = JobBatchPush(
                job_id=job_id,
                status=job.status,
                step_stats=job.step_stats if hasattr(job, 'step_stats') else [],
                total_completed=job.workflows_completed,
                total_failed=job.workflows_failed,
                overall_rate=self._compute_job_overall_rate(job_id),
                elapsed_seconds=time.monotonic() - job.timestamp,
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
    
    def _check_job_completion(self, job_id: str) -> None:
        """
        Check if a job has completed and push status if callback registered.
        
        Called after workflow progress updates to detect job completion.
        """
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return
        
        # Check if all workflows are complete (JobInfo.workflows is dict[str, WorkflowInfo])
        # WorkflowInfo uses .status (WorkflowStatus enum)
        terminal_statuses = (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED,
                          WorkflowStatus.AGGREGATED, WorkflowStatus.AGGREGATION_FAILED)
        all_done = all(
            wf_info.status in terminal_statuses
            for wf_info in job.workflows.values()
        ) if job.workflows else False

        if all_done and job.status == JobStatus.RUNNING.value:
            # Determine final status
            failed_statuses = (WorkflowStatus.FAILED, WorkflowStatus.AGGREGATION_FAILED)
            any_failed = any(
                wf_info.status in failed_statuses
                for wf_info in job.workflows.values()
            )
            job.status = JobStatus.FAILED.value if any_failed else JobStatus.COMPLETED.value
            
            # Push final status to client
            if self._job_callbacks.get(job_id):
                self._task_runner.run(
                    self._push_job_status_to_client,
                    job_id,
                    f"Job {job.status}",
                )
    
    async def _client_batch_push_loop(self) -> None:
        """
        Background loop for Tier 2 (Periodic) client push updates.

        Only runs when manager operates without gates (direct client mode).
        Sends batched progress updates to clients every few seconds.
        """
        batch_interval = getattr(self, '_batch_push_interval', 2.0)

        while self._running:
            try:
                await asyncio.sleep(batch_interval)
                if not self._running:
                    break
                await self._push_batch_stats_to_clients()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Client batch push loop error: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                await asyncio.sleep(batch_interval)
    
    # =========================================================================
    # Workflow Failure Retry Logic
    # =========================================================================
    
    async def _handle_workflow_failure(
        self,
        progress: WorkflowProgress,
    ) -> None:
        """
        Handle a workflow failure and potentially retry on another worker.

        Called when a workflow reports FAILED status. Will attempt to
        reschedule on a different worker up to max_workflow_retries times.
        """
        workflow_id = progress.workflow_id
        job_id = progress.job_id

        # Get current assignment from JobManager
        job = self._job_manager.get_job_for_sub_workflow(workflow_id)
        if not job:
            return
        sub_wf = job.sub_workflows.get(workflow_id)
        if not sub_wf:
            return
        current_worker = sub_wf.worker_id
        if not current_worker:
            return
        
        # Get retry info (should have been stored on initial dispatch)
        if workflow_id not in self._workflow_retries:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"No retry info for failed workflow {workflow_id}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return
        
        retry_count, original_dispatch, failed_workers = self._workflow_retries[workflow_id]
        failed_workers.add(current_worker)
        # Update the retry info with the new failed worker
        self._workflow_retries[workflow_id] = (retry_count, original_dispatch, failed_workers)
        
        # Check if we've exceeded max retries
        if retry_count >= self._max_workflow_retries:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Workflow {workflow_id} failed after {retry_count} retries",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            # Clean up retry tracking
            del self._workflow_retries[workflow_id]
            return
        
        # Try to reschedule on a different worker
        await self._retry_workflow(
            workflow_id=workflow_id,
            job_id=job_id,
            failed_workers=failed_workers,
            retry_count=retry_count + 1,
        )
    
    async def _retry_workflow(
        self,
        workflow_id: str,
        job_id: str,
        failed_workers: set[str],
        retry_count: int,
    ) -> bool:
        """
        Attempt to retry a workflow on a different worker.
        
        Returns True if successfully rescheduled, False otherwise.
        Uses the correct number of VUs/cores from the original dispatch.
        """
        # Find eligible workers (not in failed set and have capacity)
        job = self._job_manager.get_job_by_id(job_id)
        if not job:
            return False

        # Find the workflow progress from JobManager
        sub_wf = job.sub_workflows.get(workflow_id)
        workflow_progress = sub_wf.progress if sub_wf else None
        if not workflow_progress:
            return False
        
        # Get stored dispatch data from retry info
        retry_info = self._workflow_retries.get(workflow_id)
        if not retry_info or not retry_info[1]:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"No dispatch data for workflow {workflow_id} retry",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False
        
        original_dispatch_bytes = retry_info[1]
        
        # Parse dispatch to get actual VUs needed
        try:
            original_dispatch = WorkflowDispatch.load(original_dispatch_bytes)
            vus_needed = original_dispatch.vus
        except Exception as e:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Failed to parse dispatch for workflow {workflow_id}: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False
        
        # Select a new worker with correct VU requirement
        new_worker = self._select_worker_for_workflow_excluding(
            vus_needed=vus_needed,
            exclude_workers=failed_workers,
        )
        
        if not new_worker:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"No eligible workers for workflow {workflow_id} retry (attempt {retry_count})",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False
        
        # Create new dispatch with new fence token
        new_fence_token = self._get_fence_token()
        
        # Update tracking - preserve original dispatch bytes
        self._workflow_retries[workflow_id] = (retry_count, original_dispatch_bytes, failed_workers)

        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Retrying workflow {workflow_id} ({vus_needed} VUs) on {new_worker} (attempt {retry_count}/{self._max_workflow_retries})",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Re-dispatch the workflow to the new worker
        try:
            # Create new dispatch with new fence token
            # (original_dispatch was already parsed above to get cores_needed)
            new_dispatch = WorkflowDispatch(
                job_id=original_dispatch.job_id,
                workflow_id=original_dispatch.workflow_id,
                workflow=original_dispatch.workflow,
                context=original_dispatch.context,
                vus=original_dispatch.vus,
                cores=original_dispatch.cores,
                timeout_seconds=original_dispatch.timeout_seconds,
                fence_token=new_fence_token,
                # Preserve context from original dispatch
                context_version=original_dispatch.context_version,
                dependency_context=original_dispatch.dependency_context,
            )
            
            # Get worker address
            worker_reg = self._workers.get(new_worker)
            if not worker_reg:
                return False
            
            worker_addr = (worker_reg.node.host, worker_reg.node.port)
            
            # Send dispatch
            response, _ = await self.send_tcp(
                worker_addr,
                "workflow_dispatch",
                new_dispatch.dump(),
                timeout=5.0,
            )
            
            if response and isinstance(response, bytes):
                ack = WorkflowDispatchAck.load(response)
                if ack.accepted:
                    return True
                else:
                    # Worker rejected, add to failed set
                    failed_workers.add(new_worker)
                    return False
            
            return False
            
        except Exception as e:
            await self.handle_exception(e, f"retry_workflow_{workflow_id}")
            return False
    
    def _select_worker_for_workflow_excluding(
        self,
        vus_needed: int,
        exclude_workers: set[str],
    ) -> str | None:
        """
        Select a worker with sufficient capacity, excluding specified workers.

        Used for retry logic to avoid workers that have already failed.
        Also skips workers with open circuit breakers.
        """
        eligible = []
        for worker in self._worker_pool.iter_workers():
            node_id = worker.node_id

            if node_id in exclude_workers:
                continue

            # Check circuit breaker - skip workers with open circuits
            if self._is_worker_circuit_open(node_id):
                continue

            # Check capacity (available minus already reserved)
            effective_available = worker.available_cores - worker.reserved_cores
            if effective_available < vus_needed:
                continue

            # Check health via WorkerPool
            if not self._worker_pool.is_worker_healthy(node_id):
                continue

            eligible.append(node_id)

        if not eligible:
            return None

        return secrets.choice(eligible)
    
    async def _handle_worker_failure(self, worker_node_id: str) -> None:
        """
        Handle a worker becoming unavailable (detected via SWIM).

        Reschedules all workflows assigned to that worker on other workers.
        The workflows must have been dispatched via _dispatch_single_workflow
        which stores the dispatch bytes in _workflow_retries for exactly this
        scenario.
        """
        # Clean up worker from WorkerPool
        await self._worker_pool.deregister_worker(worker_node_id)

        # Find all workflows assigned to this worker via JobManager
        workflows_to_retry: list[str] = []
        for job in self._job_manager.iter_jobs():
            for sub_wf in job.sub_workflows.values():
                if sub_wf.worker_id == worker_node_id and sub_wf.result is None:
                    workflows_to_retry.append(str(sub_wf.token))
        
        if not workflows_to_retry:
            return
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Worker {worker_node_id} failed, rescheduling {len(workflows_to_retry)} workflows",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # Mark each workflow as needing retry
        for workflow_id in workflows_to_retry:
            # Get the job for this workflow by searching all jobs
            job_id = None
            for job in self._job_manager.iter_jobs():
                for wf_info in job.workflows.values():
                    if wf_info.token.workflow_id == workflow_id:
                        job_id = job.job_id
                        break
                if job_id:
                    break

            if not job_id:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Cannot retry workflow {workflow_id} - job not found",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                continue
            
            # Dispatch bytes should have been stored when workflow was dispatched
            # via _dispatch_single_workflow. If not present, we cannot retry.
            if workflow_id not in self._workflow_retries:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Cannot retry workflow {workflow_id} - no dispatch data stored (workflow may have been dispatched through a different path)",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                continue
            
            # Update failed workers set
            count, data, failed = self._workflow_retries[workflow_id]
            if not data:
                # Dispatch bytes are empty - cannot retry
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Cannot retry workflow {workflow_id} - empty dispatch data",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                continue
            
            failed.add(worker_node_id)
            self._workflow_retries[workflow_id] = (count, data, failed)
            
            # Attempt retry
            await self._retry_workflow(
                workflow_id=workflow_id,
                job_id=job_id,
                failed_workers=failed,
                retry_count=count + 1,
            )
    
    # =========================================================================
    # Background Cleanup
    # =========================================================================
    
    async def _job_cleanup_loop(self) -> None:
        """
        Periodically clean up completed/failed jobs and their associated state.

        Removes jobs that have been in a terminal state for longer than _job_max_age.
        Also cleans up workflow_assignments and workflow_retries for those jobs.
        Also checks for workflow timeouts and dispatch failures.
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

                # Check for workflow timeouts and dispatch failures
                if self._workflow_dispatcher:
                    evicted_or_failed = await self._workflow_dispatcher.check_timeouts()
                    for job_id, workflow_id, reason in evicted_or_failed:
                        # Mark the workflow as failed in JobManager
                        workflow_token = self._job_manager.create_workflow_token(job_id, workflow_id)
                        await self._job_manager.mark_workflow_failed(workflow_token, reason)
                
                now = time.monotonic()
                jobs_to_remove = []

                for job in self._job_manager.iter_jobs():
                    if job.status not in terminal_states:
                        continue
                    age = now - job.timestamp
                    if age > self._job_max_age:
                        jobs_to_remove.append(job.job_id)
                
                for job_id in jobs_to_remove:
                    self._cleanup_job(job_id)
                
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
    
    def _cleanup_job(self, job_id: str) -> None:
        """
        Clean up all state associated with a job.

        Removes:
        - The job itself from _jobs
        - Job leadership tracking from _job_leaders
        - Job layer version from _job_layer_version
        - Job context from _job_contexts
        - Job callback from _job_callbacks
        - All workflow assignments for this job
        - All workflow retries for this job
        - All workflow completion events for this job
        """
        # Remove job from JobManager and all related tracking dictionaries
        # Note: complete_job is async but we're in sync context - use fire-and-forget
        self._task_runner.run(self._job_manager.complete_job, job_id)
        self._job_leaders.pop(job_id, None)
        self._job_leader_addrs.pop(job_id, None)
        self._job_fencing_tokens.pop(job_id, None)
        self._job_layer_version.pop(job_id, None)
        self._job_contexts.pop(job_id, None)
        self._job_callbacks.pop(job_id, None)
        self._job_submissions.pop(job_id, None)

        # Clean up WorkflowDispatcher tracking for this job
        if self._workflow_dispatcher:
            self._task_runner.run(
                self._workflow_dispatcher.cleanup_job,
                job_id,
            )

        # Clean up JobManager tracking for this job
        self._task_runner.run(
            self._job_manager.complete_job,
            job_id,
        )

        # Find and remove workflow retries and completion events for this job
        # These are keyed by workflow_id (format: "{job_id}:{idx}")
        workflow_ids_to_remove = [
            wf_id for wf_id in self._workflow_retries
            if wf_id.startswith(f"{job_id}:")
        ]
        for wf_id in workflow_ids_to_remove:
            self._workflow_retries.pop(wf_id, None)

        workflow_ids_to_remove = [
            wf_id for wf_id in self._workflow_completion_events
            if wf_id.startswith(f"{job_id}:")
        ]
        for wf_id in workflow_ids_to_remove:
            self._workflow_completion_events.pop(wf_id, None)
    
    # =========================================================================
    # TCP Handlers - Job Submission (from Gate or Client)
    # =========================================================================
    
    @tcp.send('job_ack')
    async def send_job_ack(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send job acknowledgment."""
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
        """
        Handle job submission from gate or client.

        Any active manager can accept a job and become the job leader.
        Job leadership is per-job, not tied to datacenter leadership.
        The accepting manager broadcasts leadership to peers so they
        know where to route workflow results.
        """
        try:
            print('GOT JOB')
            submission = JobSubmission.load(data)

            # Unpickle workflows
            workflows = restricted_loads(submission.workflows)

            # Only active managers accept jobs (not SYNCING)
            if self._manager_state != ManagerState.ACTIVE:
                ack = JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Manager is {self._manager_state.value}, not accepting jobs",
                )
                return ack.dump()

            # =================================================================
            # Create job using JobManager (new system with TrackingToken)
            # =================================================================
            callback_addr = None
            if submission.callback_addr:
                callback_addr = tuple(submission.callback_addr) if isinstance(submission.callback_addr, list) else submission.callback_addr

            job_info = await self._job_manager.create_job(
                submission=submission,
                callback_addr=callback_addr,
            )

            # Set job leadership info in JobInfo
            job_info.leader_node_id = self._node_id.full
            job_info.leader_addr = (self._host, self._tcp_port)
            job_info.fencing_token = 1

            # Log the tracking token
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Created job with tracking token: {job_info.token}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Store submission for eager dispatch
            self._job_submissions[submission.job_id] = submission

            # Set this manager as job leader (first to accept = job leader)
            self._job_leaders[submission.job_id] = self._node_id.full
            self._job_leader_addrs[submission.job_id] = (self._host, self._tcp_port)
            self._job_fencing_tokens[submission.job_id] = 1  # Initial fencing token
            self._job_layer_version[submission.job_id] = 0  # Start at layer 0
            self._job_contexts[submission.job_id] = Context()  # Empty context

            # Store callback for push notifications (if provided)
            if submission.callback_addr:
                self._job_callbacks[submission.job_id] = submission.callback_addr

            self._increment_version()


            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Job {submission.job_id} unpickled {len(workflows)} workflows, dispatching...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # Broadcast job leadership to peer managers
            # Include workflow names so non-leaders can respond to workflow queries
            workflow_names = [wf.dependent_workflow.__name__ if isinstance(wf, DependentWorkflow) else wf.__name__ for wf in workflows]

            await self._broadcast_job_leadership(
                submission.job_id,
                len(workflows),
                workflow_names,
            )

            # Dispatch workflows to workers via TaskRunner
            await self._dispatch_job_workflows(
                submission,
                workflows,
            )
            
            ack = JobAck(
                job_id=submission.job_id,
                accepted=True,
                queued_position=self._job_manager.job_count,
            )
            return ack.dump()
            
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            await self.handle_exception(e, "job_submission")
            ack = JobAck(
                job_id="unknown",
                accepted=False,
                error=str(e),
            )
            return ack.dump()
    
    async def _dispatch_job_workflows(
        self,
        submission: JobSubmission,
        workflows: list[type[Workflow] | DependentWorkflow],
    ) -> None:
        """
        Dispatch workflows respecting dependencies and resource constraints.

        Builds a DAG from DependentWorkflow dependencies and dispatches
        in topological order (layer by layer). Workflows in the same layer
        can run in parallel, but dependent workflows wait for their
        dependencies to complete before dispatching.
        """

        try:

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"_dispatch_job_workflows called for job {submission.job_id} with {len(workflows)} workflows",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            # =================================================================
            # Register workflows with WorkflowDispatcher (new system)
            # =================================================================
            if self._workflow_dispatcher:
                registered = await self._workflow_dispatcher.register_workflows(
                    submission, workflows
                )
                if registered:
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"Registered {len(workflows)} workflows with WorkflowDispatcher for job {submission.job_id}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

                    # Start event-driven dispatch loop for this job
                    # This continuously dispatches workflows as dependencies are satisfied
                    # and cores become available, without polling
                    await self._workflow_dispatcher.start_job_dispatch(
                        submission.job_id, submission
                    )

                    # Also do an immediate dispatch attempt for workflows with no dependencies
                    dispatched = await self._workflow_dispatcher.try_dispatch(
                        submission.job_id, submission
                    )
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerInfo(
                            message=f"WorkflowDispatcher initial dispatch: {dispatched} workflows dispatched",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )

            # Update job status
            job = self._job_manager.get_job_by_id(submission.job_id)
            if job:
                job.status = JobStatus.RUNNING.value
                self._increment_version()

        except Exception as e:
            import traceback
            print(traceback.format_exc())
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Workflow dispatch failed: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
            job = self._job_manager.get_job_by_id(submission.job_id)
            if job:
                job.status = JobStatus.FAILED.value
            self._increment_version()

    # =========================================================================
    # TCP Handlers - Quorum
    # =========================================================================
    
    @tcp.send('provision_confirm')
    async def send_provision_confirm(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send provision confirmation."""
        return (addr, data, timeout)
    
    @tcp.handle('provision_confirm')
    async def handle_provision_confirm_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw provision confirm."""
        return data
    
    @tcp.receive()
    async def provision_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle provision request from leader for quorum."""
        try:
            request = ProvisionRequest.load(data)

            # Check if we can confirm (worker exists and has capacity)
            worker = self._worker_pool.get_worker(request.target_worker)
            can_confirm = (
                worker is not None and
                self._worker_pool.is_worker_healthy(request.target_worker) and
                (worker.available_cores - worker.reserved_cores) >= request.cores_required
            )

            confirm = ProvisionConfirm(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                confirming_node=self._node_id.full,
                confirmed=can_confirm,
                version=self._state_version,
                error=None if can_confirm else "Worker not available",
            )
            return confirm.dump()
            
        except Exception as e:
            await self.handle_exception(e, "receive_provision_request")
            confirm = ProvisionConfirm(
                job_id="unknown",
                workflow_id="unknown",
                confirming_node=self._node_id.full,
                confirmed=False,
                version=self._state_version,
                error=str(e),
            )
            return confirm.dump()
    
    @tcp.receive()
    async def provision_commit(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle provision commit from leader."""
        try:
            commit = ProvisionCommit.load(data)

            # Workflow assignments are tracked in JobManager via sub_workflows
            self._increment_version()

            return b'ok'

        except Exception as e:
            await self.handle_exception(e, "receive_provision_commit")
            return b'error'
    
    # =========================================================================
    # TCP Handlers - State Sync
    # =========================================================================
    
    @tcp.send('state_sync_response')
    async def send_state_sync_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send state sync response."""
        return (addr, data, timeout)
    
    @tcp.handle('state_sync_response')
    async def handle_state_sync_response_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw state sync response."""
        return data
    
    @tcp.receive()
    async def receive_state_sync_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle state sync request (when new leader needs current state).

        Only returns full state if this manager is ACTIVE. If still SYNCING,
        returns responder_ready=False to indicate the requester should retry.
        """
        try:
            request = StateSyncRequest.load(data)

            # Only serve state if we're ACTIVE (completed our own startup)
            is_ready = self._manager_state == ManagerState.ACTIVE

            response = StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=self._state_version,
                responder_ready=is_ready,
                # Only include state if we're ready
                manager_state=self._get_state_snapshot() if is_ready else None,
            )
            return response.dump()

        except Exception as e:
            await self.handle_exception(e, "receive_state_sync_request")
            return b''
    
    # =========================================================================
    # TCP Handlers - Cancellation
    # =========================================================================
    
    @tcp.receive()
    async def receive_cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job cancellation (from gate or client)."""
        try:
            cancel = CancelJob.load(data)

            job = self._job_manager.get_job_by_id(cancel.job_id)
            if not job:
                ack = CancelAck(
                    job_id=cancel.job_id,
                    cancelled=False,
                    error="Job not found",
                )
                return ack.dump()

            # Cancel all workflows on workers via sub_workflows from JobManager
            cancelled_count = 0
            workers_notified: set[str] = set()
            for sub_wf in job.sub_workflows.values():
                worker_id = sub_wf.worker_id
                if worker_id and worker_id not in workers_notified:
                    worker = self._worker_pool.get_worker(worker_id)
                    if worker and worker.registration:
                        try:
                            await self.send_tcp(
                                (worker.registration.node.host, worker.registration.node.port),
                                "cancel_job",
                                cancel.dump(),
                                timeout=2.0,
                            )
                            cancelled_count += 1
                            workers_notified.add(worker_id)
                        except Exception:
                            pass

            job.status = JobStatus.CANCELLED.value
            self._increment_version()

            ack = CancelAck(
                job_id=cancel.job_id,
                cancelled=True,
                workflows_cancelled=cancelled_count,
            )
            return ack.dump()

        except Exception as e:
            await self.handle_exception(e, "receive_cancel_job")
            ack = CancelAck(
                job_id="unknown",
                cancelled=False,
                error=str(e),
            )
            return ack.dump()

    # =========================================================================
    # TCP Handlers - Job Leadership
    # =========================================================================

    @tcp.receive()
    async def job_leadership_announcement(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle job leadership announcement from another manager.

        When another manager accepts a job, it broadcasts leadership.
        We record this so we can properly route workflow results
        and forward context updates to the job leader.
        """
        try:
            announcement = JobLeadershipAnnouncement.load(data)
        
            # Don't accept if we're already the leader for this job
            if self._is_job_leader(announcement.job_id):
                ack = JobLeadershipAck(
                    job_id=announcement.job_id,
                    accepted=False,
                    responder_id=self._node_id.full,
                )
                return ack.dump()

            # Record job leadership
            self._job_leaders[announcement.job_id] = announcement.leader_id
            self._job_leader_addrs[announcement.job_id] = (
                announcement.leader_host,
                announcement.leader_tcp_port,
            )

            # Initialize empty context for this job if we don't have one
            if announcement.job_id not in self._job_contexts:
                self._job_contexts[announcement.job_id] = Context()

            if announcement.job_id not in self._job_layer_version:
                self._job_layer_version[announcement.job_id] = 0

            # Track the job in JobManager for query support
            # Non-leader managers track jobs with leader info for routing
            await self._job_manager.track_remote_job(
                job_id=announcement.job_id,
                leader_node_id=announcement.leader_id,
                leader_addr=(announcement.leader_host, announcement.leader_tcp_port),
            )

            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Accepted job {announcement.job_id[:8]}... leadership from {announcement.leader_id[:8]}...",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )

            ack = JobLeadershipAck(
                job_id=announcement.job_id,
                accepted=True,
                responder_id=self._node_id.full,
            )
            return ack.dump()

        except Exception as e:
            await self.handle_exception(e, "job_leadership_announcement")
            return b'error'

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

        Returns comprehensive manager status including:
        - Manager identity and leadership status
        - Capacity (total/available cores)
        - Worker health (per-worker breakdown)
        - Active jobs
        - Peer manager addresses
        """
        try:
            request = PingRequest.load(data)

            # Build per-worker status list from WorkerPool
            all_workers = self._worker_pool.iter_workers()
            healthy_worker_ids = set(self._worker_pool.get_healthy_worker_ids())
            workers: list[WorkerStatus] = []

            for worker in all_workers:
                # Get state from heartbeat if available, otherwise infer from health
                if worker.heartbeat:
                    state = worker.heartbeat.state
                    queue_depth = worker.heartbeat.queue_depth
                    cpu_percent = worker.heartbeat.cpu_percent
                    memory_percent = worker.heartbeat.memory_percent
                else:
                    state = WorkerState.HEALTHY.value if worker.node_id in healthy_worker_ids else WorkerState.OFFLINE.value
                    queue_depth = 0
                    cpu_percent = 0.0
                    memory_percent = 0.0

                workers.append(WorkerStatus(
                    worker_id=worker.node_id,
                    state=state,
                    available_cores=worker.available_cores,
                    total_cores=worker.total_cores,
                    queue_depth=queue_depth,
                    cpu_percent=cpu_percent,
                    memory_percent=memory_percent,
                ))

            # Get active job IDs
            active_job_ids = self._job_manager.get_all_job_ids()

            # Get peer manager addresses
            peer_managers = self._get_active_manager_peer_addrs()

            response = ManagerPingResponse(
                request_id=request.request_id,
                manager_id=self._node_id.full,
                datacenter=self._dc_id,
                host=self._host,
                port=self._tcp_port,
                is_leader=self.is_leader(),
                state=self._manager_state.value,
                term=self._leader_election.state.current_term,
                total_cores=self._get_total_cores(),
                available_cores=self._get_available_cores_for_healthy_workers(),
                worker_count=len(all_workers),
                healthy_worker_count=len(healthy_worker_ids),
                workers=workers,
                active_job_ids=active_job_ids,
                active_job_count=len(active_job_ids),
                active_workflow_count=sum(
                    len(job.workflows) for job in self._job_manager.iter_jobs()
                ),
                peer_managers=peer_managers,
            )

            return response.dump()

        except Exception as e:
            await self.handle_exception(e, "ping")
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

        Returns status for requested workflows by name, including:
        - Current status (pending, running, completed, etc.)
        - Provisioned cores and VUs
        - Progress stats (completed/failed counts, rate)
        - Queue position if enqueued
        - Assigned workers

        Unknown workflow names are silently ignored.
        """
        try:
            request = WorkflowQueryRequest.load(data)
            workflow_names_set = set(request.workflow_names)

            workflows: list[WorkflowStatusInfo] = []

            matching_job = self._job_manager.get_job_by_id(request.job_id)
            if matching_job is None:
                response = WorkflowQueryResponse(
                    request_id=request.request_id,
                    manager_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    workflows=workflows,
                )

                return response.dump()

            # JobInfo.workflows is dict[str, WorkflowInfo], iterate over values
            # WorkflowInfo has .name (not .workflow_name) and .state (not .status)
            matching_workflows = [
                wf_info for wf_info in matching_job.workflows.values()
                if wf_info.name in request.workflow_names
            ]

            # Build global queue of all PENDING workflows ordered by timestamp
            # Queue position is 1-indexed (1 = next to run, 0 = not queued)
            pending_queue: list[tuple[float, str]] = []  # (timestamp, workflow_id)
            for job in self._job_manager.iter_jobs():
                for wf_info in job.workflows.values():
                    if wf_info.status == WorkflowStatus.PENDING:
                        pending_queue.append((job.timestamp, wf_info.token.workflow_id or ""))
            # Sort by timestamp (earliest first = front of queue)
            pending_queue.sort(key=lambda x: x[0])
            # Map workflow_id -> queue position (1-indexed)
            queue_positions = {wf_id: idx + 1 for idx, (_, wf_id) in enumerate(pending_queue)}

            for wf_info in matching_workflows:
                # wf_info is WorkflowInfo with: token, name, status, sub_workflow_tokens
                workflow_id = wf_info.token.workflow_id or ""
                status = wf_info.status.value

                # Determine if this workflow is enqueued (PENDING status)
                is_enqueued = wf_info.status == WorkflowStatus.PENDING

                # Get assigned worker(s) and progress from sub-workflows (new JobManager system)
                # WorkflowInfo.sub_workflow_tokens contains token strings for dispatched sub-workflows
                # JobInfo.sub_workflows maps token string -> SubWorkflowInfo
                assigned_workers: list[str] = []
                provisioned_cores = 0
                completed_count = 0
                failed_count = 0
                rate_per_second = 0.0
                elapsed_seconds = 0.0

                # Iterate over sub-workflow tokens tracked in WorkflowInfo
                for sub_token_str in wf_info.sub_workflow_tokens:
                    sub_info = matching_job.sub_workflows.get(sub_token_str)
                    if sub_info:
                        # Get worker ID from SubWorkflowInfo (extracted from token)
                        if sub_info.worker_id:
                            assigned_workers.append(sub_info.worker_id)

                        # Add cores allocated to this sub-workflow
                        provisioned_cores += sub_info.cores_allocated

                        # Aggregate progress if available
                        if sub_info.progress:
                            completed_count += sub_info.progress.completed_count
                            failed_count += sub_info.progress.failed_count
                            rate_per_second += sub_info.progress.rate_per_second
                            elapsed_seconds = max(elapsed_seconds, sub_info.progress.elapsed_seconds)

                # Deduplicate workers (same worker may have multiple sub-workflows)
                assigned_workers = list(set(assigned_workers))

                # Build status info
                status_info = WorkflowStatusInfo(
                    workflow_name=wf_info.name,
                    workflow_id=workflow_id,
                    job_id=request.job_id,
                    status=status,
                    provisioned_cores=provisioned_cores,
                    vus=0,  # VUs not tracked in WorkflowInfo
                    completed_count=completed_count,
                    failed_count=failed_count,
                    rate_per_second=rate_per_second,
                    elapsed_seconds=elapsed_seconds,
                    is_enqueued=is_enqueued,
                    queue_position=queue_positions.get(workflow_id, 0),
                    assigned_workers=assigned_workers,
                )
                workflows.append(status_info)

            response = WorkflowQueryResponse(
                request_id=request.request_id,
                manager_id=self._node_id.full,
                datacenter=self._node_id.datacenter,
                workflows=workflows,
            )

            return response.dump()

        except Exception as e:
            await self.handle_exception(e, "workflow_query")
            return b'error'
