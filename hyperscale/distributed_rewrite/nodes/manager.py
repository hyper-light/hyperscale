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
from typing import Any

import cloudpickle
import networkx

from hyperscale.core.graph.dependent_workflow import DependentWorkflow
from hyperscale.core.state.context import Context
from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.distributed_rewrite.server.events import VersionedStateClock
from hyperscale.distributed_rewrite.swim import HealthAwareServer, ManagerStateEmbedder
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
    WorkflowStatus,
    JobProgress,
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
    restricted_loads,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning, ServerError, ServerDebug


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
        manager_peers: list[tuple[str, int]] | None = None,  # TCP addresses
        manager_udp_peers: list[tuple[str, int]] | None = None,  # UDP for SWIM cluster
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
        
        # Manager peers for quorum (TCP) and SWIM cluster (UDP)
        self._manager_peers = manager_peers or []  # TCP
        self._manager_udp_peers = manager_udp_peers or []  # UDP for SWIM
        
        # Track manager peer addresses for failure detection
        # Maps UDP addr -> TCP addr for peer managers
        self._manager_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}
        for i, tcp_addr in enumerate(self._manager_peers):
            if i < len(self._manager_udp_peers):
                self._manager_udp_to_tcp[self._manager_udp_peers[i]] = tcp_addr
        
        # Track active manager peers (removed when SWIM marks as dead)
        self._active_manager_peers: set[tuple[str, int]] = set(self._manager_peers)
        
        # Track manager peer info from ManagerHeartbeat (proper node_ids, leadership, etc)
        # Maps UDP addr -> ManagerHeartbeat for peers we've heard from via SWIM
        self._manager_peer_info: dict[tuple[str, int], ManagerHeartbeat] = {}
        
        # Registered workers (indexed by node_id)
        self._workers: dict[str, WorkerRegistration] = {}  # node_id -> registration
        self._worker_addr_to_id: dict[tuple[str, int], str] = {}  # (host, port) -> node_id (reverse mapping)
        self._worker_status: dict[str, WorkerHeartbeat] = {}  # node_id -> last status
        self._worker_last_status: dict[str, float] = {}  # node_id -> timestamp
        
        # Per-worker circuit breakers for dispatch failures
        # Tracks failures per-worker to avoid dispatching to failing workers
        self._worker_circuits: dict[str, ErrorStats] = {}  # node_id -> ErrorStats
        
        # Versioned state clock for rejecting stale updates
        # Tracks per-worker and per-job versions using Lamport timestamps
        self._versioned_clock = VersionedStateClock()
        
        # Job and workflow state
        self._jobs: dict[str, JobProgress] = {}  # job_id -> progress
        self._workflow_assignments: dict[str, str] = {}  # workflow_id -> worker_node_id
        self._pending_provisions: dict[str, ProvisionRequest] = {}  # workflow_id -> request
        self._provision_confirmations: dict[str, set[str]] = {}  # workflow_id -> confirming nodes
        
        # Job leader tracking (Context Consistency Protocol)
        # Each job has one leader manager responsible for context consistency
        self._job_leaders: dict[str, str] = {}  # job_id -> leader_node_id
        self._job_layer_version: dict[str, int] = {}  # job_id -> monotonic layer version
        self._job_contexts: dict[str, Context] = {}  # job_id -> Context for dependent workflows
        self._context_lamport_clock: int = 0  # For generating timestamps on context updates
        
        # Client push notification callbacks (when gates not present)
        # job_id -> callback address for push notifications
        self._job_callbacks: dict[str, tuple[str, int]] = {}
        
        # Workflow retry tracking
        # Maps workflow_id -> (retry_count, original_dispatch, failed_workers)
        self._workflow_retries: dict[str, tuple[int, bytes, set[str]]] = {}
        self._max_workflow_retries = max_workflow_retries
        self._workflow_timeout = workflow_timeout
        
        # Workflow completion events for dependency tracking
        # Maps workflow_id -> asyncio.Event (set when workflow completes)
        self._workflow_completion_events: dict[str, asyncio.Event] = {}
        
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
        
        # Inject state embedder for Serf-style heartbeat embedding in SWIM messages
        self.set_state_embedder(ManagerStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_datacenter=lambda: self._node_id.datacenter,
            is_leader=self.is_leader,
            get_term=lambda: self._leader_election.state.current_term,
            get_state_version=lambda: self._state_version,
            get_active_jobs=lambda: len(self._jobs),
            get_active_workflows=lambda: sum(
                len([w for w in job.workflows if w.status == WorkflowStatus.RUNNING.value])
                for job in self._jobs.values()
            ),
            get_worker_count=lambda: len(self._workers),
            get_healthy_worker_count=lambda: len(self._get_healthy_worker_ids()),
            get_available_cores=lambda: self._get_available_cores_for_healthy_workers(),
            get_total_cores=self._get_total_cores,
            on_worker_heartbeat=self._handle_embedded_worker_heartbeat,
            on_manager_heartbeat=self._handle_manager_peer_heartbeat,
            on_gate_heartbeat=self._handle_gate_heartbeat,
            get_manager_state=lambda: self._manager_state.value,
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
        for node_id, worker_reg in self._workers.items():
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
        if not self._manager_peers:
            return
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"New leader syncing job state from {len(self._manager_peers)} peer managers",
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
        for peer_addr in self._manager_peers:
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
            # Convert to heartbeat format for storage
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
            self._worker_status[worker_state.node_id] = heartbeat
            
            # Update workflow assignments from worker's state
            for wf_id, progress in worker_state.active_workflows.items():
                self._workflow_assignments[wf_id] = worker_state.node_id
            
            return worker_state
        return None
    
    async def _request_manager_peer_state(
        self,
        peer_addr: tuple[str, int],
        request: StateSyncRequest,
        max_retries: int = 3,
        base_delay: float = 0.5,
    ) -> ManagerStateSnapshot | None:
        """
        Request state from a peer manager with retries.
        
        Uses exponential backoff: delay = base_delay * (2 ** attempt)
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                response, _ = await self.send_tcp(
                    peer_addr,
                    action='state_sync_request',
                    data=request.dump(),
                    timeout=5.0,
                )
                
                if response and not isinstance(response, Exception):
                    sync_response = StateSyncResponse.load(response)
                    if sync_response.manager_state:
                        return await self._process_manager_state_response(sync_response.manager_state)
                
                # No valid response, will retry
                last_error = "Empty or invalid response"
                
            except Exception as e:
                last_error = str(e)
            
            # Don't sleep after last attempt
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
        
        # All retries failed - log but don't fail (peer may be dead)
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager peer state sync failed for {peer_addr} after {max_retries} attempts: {last_error}",
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
        Process a manager state response and merge job state.
        
        Only merges jobs we don't know about or that have higher versions.
        Does NOT override worker state - workers are source of truth for that.
        """
        # Check version for staleness
        peer_key = f"manager:{manager_state.node_id}"
        if self._versioned_clock.is_entity_stale(peer_key, manager_state.version):
            return None
        
        # Merge job state - add jobs we don't have
        jobs_merged = 0
        for job_id, job_progress in manager_state.jobs.items():
            if job_id not in self._jobs:
                # Create JobProgress for this job
                self._jobs[job_id] = job_progress
                jobs_merged += 1
        
        # Merge job leader tracking (Context Consistency Protocol)
        for job_id, leader_id in manager_state.job_leaders.items():
            if job_id not in self._job_leaders:
                self._job_leaders[job_id] = leader_id
        
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
        
        if jobs_merged > 0:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Merged {jobs_merged} jobs from peer {manager_state.node_id}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
        
        return manager_state
    
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
        
        # Accept update
        self._worker_status[heartbeat.node_id] = heartbeat
        self._worker_last_status[heartbeat.node_id] = time.monotonic()
        
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
        """
        # Check if update is stale using versioned clock
        if self._versioned_clock.is_entity_stale(heartbeat.node_id, heartbeat.version):
            return
        
        # Store peer info keyed by UDP address
        self._manager_peer_info[source_addr] = heartbeat
        
        # Update version tracking
        self._task_runner.run(
            self._versioned_clock.update_entity, heartbeat.node_id, heartbeat.version
        )
    
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
        
        Quorum is based on *configured* cluster size, not active size.
        This prevents split-brain where a partition thinks it has quorum
        because it only sees its own subset of members.
        """
        total_managers = len(self._manager_peers) + 1  # Include self
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
            for udp, tcp in self._manager_udp_to_tcp.items():
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
        if not self._manager_peers:
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
        for peer_addr in self._manager_peers:
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
        
        # Start leader election (uses SWIM membership info)
        await self.start_leader_election()
        
        # Wait a short time for leader election to stabilize
        await asyncio.sleep(0.5)
        
        # Sync state and transition to ACTIVE
        await self._complete_startup_sync()
        
        # Start background cleanup for completed jobs
        self._task_runner.run(self._job_cleanup_loop)
        
        # Register with gates (similar to Worker registering with Managers)
        if self._seed_gates:
            await self._register_with_gates()
        
        # Start TCP heartbeat loop to gates (supplements SWIM embedding)
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
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerError(
                            message=f"State sync from leader failed, transitioning to ACTIVE anyway",
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
                    
                    # Add gate's UDP address for SWIM (if not already configured)
                    gate_udp_addr = (gate_info.udp_host, gate_info.udp_port)
                    if gate_udp_addr not in self._gate_udp_addrs:
                        self._gate_udp_addrs.append(gate_udp_addr)
                        # Join SWIM cluster with this gate
                        await self.join_cluster(gate_udp_addr)
                
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
                import traceback
                print(traceback.format_exc())
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
    
    async def stop(self) -> None:
        """Stop the manager server."""
        # Graceful shutdown broadcasts leave via UDP (SWIM)
        await self.graceful_shutdown()
        
        await super().stop()
    
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
    
    def _get_healthy_worker_ids(self) -> list[str]:
        """
        Get list of worker IDs that are healthy according to SWIM probes.
        
        A worker is healthy if:
        1. SWIM reports it as 'OK' (alive), OR
        2. It was recently registered (within grace period) and hasn't been marked dead
        
        The grace period handles the startup race where workers register but SWIM
        probing hasn't completed yet.
        """
        healthy = []
        now = time.monotonic()
        grace_period = 30.0  # Consider workers healthy for 30s after registration
        
        for node_id, registration in self._workers.items():
            worker_addr = (registration.node.host, registration.node.port)
            node_state = self._incarnation_tracker.get_node_state(worker_addr)
            
            # Check if SWIM says healthy
            if node_state and node_state.status == b'OK':
                healthy.append(node_id)
                continue
            
            # Check if recently registered (grace period)
            last_seen = self._worker_last_status.get(node_id, 0)
            if (now - last_seen) < grace_period:
                # Not explicitly marked dead by SWIM - treat as healthy
                if not node_state or node_state.status != b'DEAD':
                    healthy.append(node_id)
        
        return healthy
    
    def _get_total_cores(self) -> int:
        """Get total cores across all registered workers."""
        return sum(
            registration.total_cores
            for registration in self._workers.values()
        )
    
    def _get_available_cores_for_healthy_workers(self) -> int:
        """
        Get available cores only from healthy workers.
        
        This is the source of truth for datacenter "BUSY" state:
        - If this returns 0 but we have healthy workers → BUSY
        - If we have no healthy workers → DEGRADED/UNHEALTHY
        """
        healthy_ids = set(self._get_healthy_worker_ids())
        return sum(
            status.available_cores
            for node_id, status in self._worker_status.items()
            if node_id in healthy_ids
        )
    
    # =========================================================================
    # Job Leader Helpers (Context Consistency Protocol)
    # =========================================================================
    
    def _is_job_leader(self, job_id: str) -> bool:
        """Check if this manager is the leader for the given job."""
        return self._job_leaders.get(job_id) == self._node_id.full
    
    def _get_job_leader(self, job_id: str) -> str | None:
        """Get the node_id of the job leader, or None if unknown."""
        return self._job_leaders.get(job_id)
    
    def _get_job_context(self, job_id: str) -> Context | None:
        """Get the context for a job, or None if job unknown."""
        return self._job_contexts.get(job_id)
    
    def _get_next_context_timestamp(self) -> int:
        """Get the next Lamport timestamp for context updates."""
        self._context_lamport_clock += 1
        return self._context_lamport_clock
    
    def _build_manager_heartbeat(self) -> ManagerHeartbeat:
        """Build a ManagerHeartbeat with current state."""
        healthy_worker_ids = self._get_healthy_worker_ids()
        healthy_ids_set = set(healthy_worker_ids)
        
        return ManagerHeartbeat(
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            version=self._state_version,
            active_jobs=len(self._jobs),
            active_workflows=sum(
                len(job.workflows) for job in self._jobs.values()
            ),
            worker_count=len(self._workers),
            healthy_worker_count=len(healthy_worker_ids),
            available_cores=sum(
                status.available_cores
                for node_id, status in self._worker_status.items()
                if node_id in healthy_ids_set
            ),
            total_cores=self._get_total_cores(),
            state=self._manager_state.value,
            tcp_host=self._host,
            tcp_port=self._tcp_port,
        )
    
    async def _gate_heartbeat_loop(self) -> None:
        """
        Periodically send ManagerHeartbeat to gates via TCP.
        
        This supplements the Serf-style SWIM embedding for reliability.
        Gates use this for datacenter health classification.
        """
        heartbeat_interval = 5.0  # Send every 5 seconds
        
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
                print(f"[{self._node_id.short}] Heartbeat loop sleeping for {heartbeat_interval}s...")
                await asyncio.sleep(heartbeat_interval)
                print(f"[{self._node_id.short}] Heartbeat loop woke up, building heartbeat")
                
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
        for node_id, reg in self._workers.items():
            status = self._worker_status.get(node_id)
            if status:
                worker_snapshots.append(WorkerStateSnapshot(
                    node_id=node_id,
                    state=status.state,
                    total_cores=reg.total_cores,
                    available_cores=status.available_cores,
                    version=status.version,
                    active_workflows={},  # Could populate from tracking
                ))
        
        # Serialize job contexts for state sync
        contexts_data = {}
        for job_id, context in self._job_contexts.items():
            contexts_data[job_id] = context.dict()
        
        return ManagerStateSnapshot(
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            version=self._state_version,
            workers=worker_snapshots,
            jobs=dict(self._jobs),
            job_leaders=dict(self._job_leaders),
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
    
    def _select_worker_for_workflow(self, vus_needed: int) -> str | None:
        """
        Select a worker with sufficient capacity for a workflow.
        
        Uses cryptographically secure random selection among eligible workers.
        Also checks SWIM membership - only select workers that are ALIVE.
        Skips workers with open circuit breakers.
        """
        eligible = []
        for node_id, status in self._worker_status.items():
            # Check circuit breaker - skip workers with open circuits
            if self._is_worker_circuit_open(node_id):
                continue
            
            # Check capacity from status update
            if status.available_cores < vus_needed:
                continue
            if status.state != WorkerState.HEALTHY.value:
                continue
            
            # Check SWIM liveness - worker must be alive in SWIM cluster
            worker_reg = self._workers.get(node_id)
            if worker_reg:
                worker_addr = (worker_reg.node.host, worker_reg.node.port)
                node_state = self._incarnation_tracker.get_node_state(worker_addr)
                if node_state and node_state.status != b'OK':
                    continue  # Worker is suspected or dead per SWIM
            
            eligible.append(node_id)
        
        if not eligible:
            return None
        
        # Cryptographically secure selection
        return secrets.choice(eligible)
    
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
        
        worker = self._workers.get(worker_node_id)
        if not worker:
            return None
        
        worker_addr = (worker.node.host, worker.node.port)
        circuit = self._get_worker_circuit(worker_node_id)
        
        for attempt in range(max_retries + 1):
            try:
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
        confirm_tasks = []
        for peer in self._manager_peers:
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
        
        for peer in self._manager_peers:
            try:
                await self.send_tcp(
                    peer,
                    "provision_commit",
                    commit.dump(),
                    timeout=2.0,
                )
            except Exception as e:
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
            
            # Store registration
            self._workers[registration.node.node_id] = registration
            # Maintain reverse mapping for O(1) address -> node_id lookups
            worker_addr = (registration.node.host, registration.node.port)
            self._worker_addr_to_id[worker_addr] = registration.node.node_id
            self._worker_last_status[registration.node.node_id] = time.monotonic()
            
            # Create initial worker status with all cores available
            # This prevents race condition where SWIM hasn't exchanged heartbeats yet
            # SWIM updates will overwrite this with real status as they arrive
            initial_status = WorkerHeartbeat(
                node_id=registration.node.node_id,
                state=WorkerState.HEALTHY.value,  # Assume healthy on registration
                available_cores=registration.available_cores,  # All cores available
                queue_depth=0,
                cpu_percent=0.0,
                memory_percent=0.0,
                version=0,  # Initial version - SWIM updates will have higher versions
                active_workflows={},
            )
            self._worker_status[registration.node.node_id] = initial_status
            
            self._increment_version()
            
            # Add worker to SWIM cluster for UDP healthchecks
            # The worker's UDP address is derived from registration
            worker_udp_addr = (registration.node.host, registration.node.port)
            self._probe_scheduler.add_member(worker_udp_addr)
            
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Worker registered: {registration.node.node_id} with {registration.total_cores} cores (SWIM probe added)",
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
    async def worker_discovery(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle worker discovery broadcast from a peer manager.
        
        When another manager receives a worker registration, it broadcasts
        to all peers. This handler adds the worker to our tracking.
        """
        try:
            broadcast = WorkerDiscoveryBroadcast.load(data)
            
            worker_id = broadcast.worker_id
            worker_tcp_addr = tuple(broadcast.worker_tcp_addr)
            worker_udp_addr = tuple(broadcast.worker_udp_addr)
            
            # Add worker if not already tracked
            if worker_id not in self._workers:
                # Create a minimal registration for tracking
                node_info = NodeInfo(
                    node_id=worker_id,
                    host=worker_tcp_addr[0],
                    port=worker_tcp_addr[1],
                    role=NodeRole.WORKER.value,
                    datacenter=broadcast.datacenter,
                )
                registration = WorkerRegistration(
                    node=node_info,
                    total_cores=broadcast.available_cores,
                    available_cores=broadcast.available_cores,
                    memory_mb=0,  # Unknown from broadcast
                    available_memory_mb=0,  # Unknown from broadcast
                )
                self._workers[worker_id] = registration
                self._worker_addr_to_id[worker_tcp_addr] = worker_id
                self._worker_last_status[worker_id] = time.monotonic()
                
                # Create initial worker status with all cores available
                # This prevents race condition where SWIM hasn't exchanged heartbeats yet
                initial_status = WorkerHeartbeat(
                    node_id=worker_id,
                    state=WorkerState.HEALTHY.value,  # Assume healthy on discovery
                    available_cores=broadcast.available_cores,  # All cores available
                    queue_depth=0,
                    cpu_percent=0.0,
                    memory_percent=0.0,
                    version=0,  # Initial version - SWIM updates will have higher versions
                    active_workflows={},
                )
                self._worker_status[worker_id] = initial_status
                
                # Add to SWIM probing
                self._probe_scheduler.add_member(worker_udp_addr)
                
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Discovered worker {worker_id} via manager {broadcast.source_manager_id}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "worker_discovery")
            return b'error'
    
    async def _broadcast_worker_discovery(
        self,
        worker_id: str,
        worker_tcp_addr: tuple[str, int],
        worker_udp_addr: tuple[str, int],
        available_cores: int,
    ) -> None:
        """
        Broadcast a newly discovered worker to all peer managers.
        
        This enables cross-manager synchronization of worker discovery.
        When a worker registers with one manager, it gets broadcast
        to all other managers so they can also track it.
        
        Args:
            worker_id: Worker's node_id
            worker_tcp_addr: Worker's TCP address
            worker_udp_addr: Worker's UDP address  
            available_cores: Worker's available cores
        """
        if not self._manager_peers:
            return
        
        broadcast = WorkerDiscoveryBroadcast(
            worker_id=worker_id,
            worker_tcp_addr=worker_tcp_addr,
            worker_udp_addr=worker_udp_addr,
            datacenter=self._node_id.datacenter,
            available_cores=available_cores,
            source_manager_id=self._node_id.full,
        )
        
        for peer_addr in self._manager_peers:
            try:
                response, _ = await self.send_worker_discovery(
                    peer_addr,
                    broadcast.dump(),
                    timeout=2.0,
                )
                if isinstance(response, Exception):
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerWarning(
                            message=f"Failed to broadcast worker {worker_id} to {peer_addr}: {response}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
            except Exception as e:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=f"Error broadcasting worker {worker_id} to {peer_addr}: {e}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
    
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
            status = WorkerHeartbeat.load(data)
            
            # Update status tracking
            self._worker_status[status.node_id] = status
            self._worker_last_status[status.node_id] = time.monotonic()
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "receive_worker_status_update")
            return b'error'
    
    @tcp.receive()
    async def receive_workflow_progress(
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
        """
        try:
            progress = WorkflowProgress.load(data)
            
            # Update job progress
            job = self._jobs.get(progress.job_id)
            if job:
                # Track previous cores_completed to detect newly freed cores
                old_progress: WorkflowProgress | None = None
                for i, wf in enumerate(job.workflows):
                    if wf.workflow_id == progress.workflow_id:
                        old_progress = wf
                        job.workflows[i] = progress
                        break
                else:
                    # New workflow progress
                    job.workflows.append(progress)
                
                # Recalculate aggregates
                job.total_completed = sum(w.completed_count for w in job.workflows)
                job.total_failed = sum(w.failed_count for w in job.workflows)
                job.overall_rate = sum(w.rate_per_second for w in job.workflows)
                job.timestamp = time.monotonic()
                
                # Aggregate step stats from all workflows
                job.step_stats = self._aggregate_step_stats(job.workflows)
                
                # Update worker available cores based on cores_completed
                # This enables faster provisioning - we don't need to wait for
                # the entire workflow to complete to start using freed cores
                await self._update_worker_cores_from_progress(progress, old_progress)
                
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
            await self.handle_exception(e, "receive_workflow_progress")
            return b'error'
    
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
    
    async def _forward_context_to_leader(
        self,
        job_id: str,
        workflow_id: str,
        context_updates: bytes,
        context_timestamps: bytes,
    ) -> bool:
        """
        Forward context updates to the job leader.
        
        Called by non-leader managers when they receive workflow completion
        with context updates. Returns True if forwarding succeeded.
        """
        leader_id = self._get_job_leader(job_id)
        if not leader_id:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Cannot forward context - no leader for job {job_id}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False
        
        # Find leader's address from peer info
        leader_addr = self._get_manager_tcp_addr(leader_id)
        if not leader_addr:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Cannot forward context - unknown address for leader {leader_id}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False
        
        forward = ContextForward(
            job_id=job_id,
            workflow_id=workflow_id,
            context_updates=context_updates,
            context_timestamps=context_timestamps,
            source_manager=self._node_id.full,
        )
        
        try:
            response, _ = await self.send_tcp(
                leader_addr,
                action='context_forward',
                data=forward.dump(),
                timeout=5.0,
            )
            return response == b'ok'
        except Exception as e:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Context forward to leader failed: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False
    
    def _get_workflow_name_from_id(self, workflow_id: str) -> str:
        """
        Get the workflow name from a workflow ID.
        
        Workflow IDs are typically formatted as job_id:workflow_name or similar.
        This extracts the name portion for context keying.
        """
        # Try to find in job progress
        for job in self._jobs.values():
            for wf in job.workflows:
                if wf.workflow_id == workflow_id:
                    return wf.workflow_name
        
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
        # Check peer info for TCP address
        peer_info = self._manager_peer_info.get(node_id)
        if peer_info:
            # ManagerHeartbeat has tcp_host and tcp_port
            return (peer_info.tcp_host, peer_info.tcp_port)
        
        # Check manager peers by matching node_id prefix
        for tcp_addr, udp_addr in self._manager_tcp_to_udp.items():
            # This is less reliable - would need node_id mapping
            pass
        
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
        for node_id, heartbeat in self._manager_peer_info.items():
            if node_id == self._node_id.full:
                continue  # Skip self
            # Only include active managers (not SYNCING)
            if heartbeat.manager_state == ManagerState.ACTIVE.value:
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
        Update worker available cores based on cores_completed from progress.
        
        When cores_completed increases, we can mark those cores as available
        for new workflows. This allows for more aggressive provisioning.
        
        Args:
            progress: New progress update
            old_progress: Previous progress (if any)
        """
        # Find the worker for this workflow
        worker_id = self._workflow_assignments.get(progress.workflow_id)
        if not worker_id:
            return
        
        # Get worker status
        worker_status = self._worker_status.get(worker_id)
        if not worker_status:
            return
        
        # Calculate newly completed cores
        old_cores_completed = old_progress.cores_completed if old_progress else 0
        new_cores_completed = progress.cores_completed
        
        if new_cores_completed > old_cores_completed:
            # Cores have been freed - update worker's available count
            cores_freed = new_cores_completed - old_cores_completed
            
            # Create updated heartbeat with incremented available cores
            # Note: This is an optimistic update that may be superseded by
            # the next heartbeat from the worker. That's OK - if we overestimate
            # available cores, workflow dispatch will fail and retry.
            updated_status = WorkerHeartbeat(
                node_id=worker_status.node_id,
                state=worker_status.state,
                available_cores=worker_status.available_cores + cores_freed,
                queue_depth=worker_status.queue_depth,
                cpu_percent=worker_status.cpu_percent,
                memory_percent=worker_status.memory_percent,
                version=worker_status.version,  # Keep same version - worker heartbeat will update
                active_workflows=worker_status.active_workflows,
            )
            self._worker_status[worker_id] = updated_status
            
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Worker {worker_id} freed {cores_freed} cores (now {updated_status.available_cores} available)",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
    
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
        job = self._jobs.get(job_id)
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
            total_completed=job.total_completed,
            total_failed=job.total_failed,
            overall_rate=job.overall_rate,
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
        for job_id, job in self._jobs.items():
            if job.status == JobStatus.RUNNING.value:
                callback = self._job_callbacks.get(job_id)
                if callback:
                    jobs_with_callbacks.append((job_id, job, callback))
        
        if not jobs_with_callbacks:
            return
        
        for job_id, job, callback in jobs_with_callbacks:
            batch_push = JobBatchPush(
                job_id=job_id,
                status=job.status,
                step_stats=job.step_stats if hasattr(job, 'step_stats') else [],
                total_completed=job.total_completed,
                total_failed=job.total_failed,
                overall_rate=job.overall_rate,
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
        job = self._jobs.get(job_id)
        if not job:
            return
        
        # Check if all workflows are complete
        all_done = all(
            w.status in (WorkflowStatus.COMPLETED.value, WorkflowStatus.FAILED.value)
            for w in job.workflows
        ) if job.workflows else False
        
        if all_done and job.status == JobStatus.RUNNING.value:
            # Determine final status
            any_failed = any(
                w.status == WorkflowStatus.FAILED.value
                for w in job.workflows
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
        
        while True:
            try:
                await asyncio.sleep(batch_interval)
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
        
        # Get current assignment
        current_worker = self._workflow_assignments.get(workflow_id)
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
        job = self._jobs.get(job_id)
        if not job:
            return False
        
        # Find the workflow progress to get VUs needed
        workflow_progress = None
        for wf in job.workflows:
            if wf.workflow_id == workflow_id:
                workflow_progress = wf
                break
        
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
        self._workflow_assignments[workflow_id] = new_worker
        
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
            # (original_dispatch was already parsed above to get vus_needed)
            new_dispatch = WorkflowDispatch(
                job_id=original_dispatch.job_id,
                workflow_id=original_dispatch.workflow_id,
                workflow=original_dispatch.workflow,
                context=original_dispatch.context,
                vus=original_dispatch.vus,
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
        for node_id, status in self._worker_status.items():
            if node_id in exclude_workers:
                continue
            
            # Check circuit breaker - skip workers with open circuits
            if self._is_worker_circuit_open(node_id):
                continue
            
            if status.state != WorkerState.HEALTHY.value:
                continue
            if status.available_cores < vus_needed:
                continue
            
            # Check worker registration exists
            worker_reg = self._workers.get(node_id)
            if not worker_reg:
                continue
            
            # Check SWIM membership - only select workers that are ALIVE
            node_state = self._incarnation_tracker.get_node_state((
                worker_reg.node.host,
                worker_reg.node.port,
            ))
            if node_state and node_state.status == b'OK':
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
        # Clean up worker from registration mappings
        worker_reg = self._workers.pop(worker_node_id, None)
        if worker_reg:
            worker_addr = (worker_reg.node.host, worker_reg.node.port)
            self._worker_addr_to_id.pop(worker_addr, None)
        self._worker_status.pop(worker_node_id, None)
        self._worker_last_status.pop(worker_node_id, None)
        
        # Find all workflows assigned to this worker
        workflows_to_retry = [
            wf_id for wf_id, assigned_worker in self._workflow_assignments.items()
            if assigned_worker == worker_node_id
        ]
        
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
            # Get the job for this workflow
            job_id = None
            for jid, job in self._jobs.items():
                for wf in job.workflows:
                    if wf.workflow_id == workflow_id:
                        job_id = jid
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
                
                for job_id, job in self._jobs.items():
                    if job.status in terminal_states:
                        # Check age based on timestamp
                        age = now - job.timestamp
                        if age > self._job_max_age:
                            jobs_to_remove.append(job_id)
                
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
        - All workflow assignments for this job
        - All workflow retries for this job
        - All workflow completion events for this job
        """
        # Remove job
        self._jobs.pop(job_id, None)
        
        # Find and remove workflow assignments for this job
        workflow_ids_to_remove = [
            wf_id for wf_id in self._workflow_assignments
            if wf_id.startswith(f"{job_id}:")
        ]
        for wf_id in workflow_ids_to_remove:
            self._workflow_assignments.pop(wf_id, None)
            self._workflow_retries.pop(wf_id, None)
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
        """Handle job submission from gate or client."""
        try:
            submission = JobSubmission.load(data)
            
            # Only leader accepts new jobs
            if not self.is_leader():
                leader = self.get_current_leader()
                ack = JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error="Not leader" if leader else "No leader elected",
                    leader_addr=leader,
                )
                return ack.dump()
            
            # Create job progress tracker
            job = JobProgress(
                job_id=submission.job_id,
                datacenter=self._node_id.datacenter,
                status=JobStatus.SUBMITTED.value,
                workflows=[],
                timestamp=time.monotonic(),
            )
            self._jobs[submission.job_id] = job
            
            # Set this manager as job leader (first to accept = job leader)
            self._job_leaders[submission.job_id] = self._node_id.full
            self._job_layer_version[submission.job_id] = 0  # Start at layer 0
            self._job_contexts[submission.job_id] = Context()  # Empty context
            
            # Store callback for push notifications (if provided)
            if submission.callback_addr:
                self._job_callbacks[submission.job_id] = submission.callback_addr
            
            self._increment_version()
            
            # Unpickle workflows
            workflows = restricted_loads(submission.workflows)
            
            # Dispatch workflows to workers via TaskRunner
            self._task_runner.run(
                self._dispatch_job_workflows, submission, workflows
            )
            
            ack = JobAck(
                job_id=submission.job_id,
                accepted=True,
                queued_position=len(self._jobs),
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
    
    async def _dispatch_job_workflows(
        self,
        submission: JobSubmission,
        workflows: list,
    ) -> None:
        """
        Dispatch workflows respecting dependencies and resource constraints.
        
        Builds a DAG from DependentWorkflow dependencies and dispatches
        in topological order (layer by layer). Workflows in the same layer
        can run in parallel, but dependent workflows wait for their
        dependencies to complete before dispatching.
        """
        import cloudpickle
        
        job = self._jobs.get(submission.job_id)
        if not job:
            return
        
        job.status = JobStatus.DISPATCHING.value
        self._increment_version()
        
        # Build dependency graph
        workflow_graph = networkx.DiGraph()
        workflow_by_name: dict[str, tuple[int, Any]] = {}  # name -> (index, workflow)
        workflow_cores: dict[str, int] = {}  # name -> cores needed
        sources: list[str] = []  # Workflows with no dependencies
        
        for i, workflow in enumerate(workflows):
            if isinstance(workflow, DependentWorkflow) and len(workflow.dependencies) > 0:
                # DependentWorkflow wraps the actual workflow
                name = workflow.dependent_workflow.name
                workflow_by_name[name] = (i, workflow.dependent_workflow)
                # Use workflow's vus if specified, otherwise use submission default
                workflow_cores[name] = getattr(workflow.dependent_workflow, 'vus', submission.vus)
                workflow_graph.add_node(name)
                for dep in workflow.dependencies:
                    workflow_graph.add_edge(dep, name)
            else:
                # Regular workflow (no dependencies)
                name = workflow.name
                workflow_by_name[name] = (i, workflow)
                workflow_cores[name] = getattr(workflow, 'vus', submission.vus)
                workflow_graph.add_node(name)
                sources.append(name)
        
        # If no sources, all workflows have dependencies - find roots
        if not sources:
            # Find nodes with no incoming edges
            for node in workflow_graph.nodes():
                if workflow_graph.in_degree(node) == 0:
                    sources.append(node)
        
        # If still no sources, we have a cycle - fail the job
        if not sources:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Job {submission.job_id} has circular workflow dependencies",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
            job.status = JobStatus.FAILED.value
            self._increment_version()
            return
        
        # Create completion events for all workflows
        for name in workflow_by_name:
            idx, _ = workflow_by_name[name]
            workflow_id = f"{submission.job_id}:{idx}"
            self._workflow_completion_events[workflow_id] = asyncio.Event()
        
        try:
            # Dispatch in dependency order using BFS layers
            for layer in networkx.bfs_layers(workflow_graph, sources):
                # Wait for dependencies of this layer to complete
                for wf_name in layer:
                    deps = list(workflow_graph.predecessors(wf_name))
                    for dep in deps:
                        dep_idx, _ = workflow_by_name.get(dep, (None, None))
                        if dep_idx is not None:
                            dep_workflow_id = f"{submission.job_id}:{dep_idx}"
                            dep_event = self._workflow_completion_events.get(dep_workflow_id)
                            if dep_event:
                                # Wait for dependency to complete
                                try:
                                    await asyncio.wait_for(
                                        dep_event.wait(),
                                        timeout=submission.timeout_seconds,
                                    )
                                except asyncio.TimeoutError:
                                    self._task_runner.run(
                                        self._udp_logger.log,
                                        ServerError(
                                            message=f"Timeout waiting for dependency {dep} to complete",
                                            node_host=self._host,
                                            node_port=self._tcp_port,
                                            node_id=self._node_id.short,
                                        ),
                                    )
                                    job.status = JobStatus.TIMEOUT.value
                                    self._increment_version()
                                    return
                
                # Dispatch all workflows in this layer (can run in parallel)
                dispatch_tasks = []
                for wf_name in layer:
                    idx, wf = workflow_by_name[wf_name]
                    cores_needed = workflow_cores[wf_name]
                    dispatch_tasks.append(
                        self._dispatch_single_workflow(
                            submission, idx, wf, cores_needed, cloudpickle
                        )
                    )
                
                # Wait for all dispatches in this layer
                results = await asyncio.gather(*dispatch_tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerError(
                                message=f"Workflow dispatch failed: {result}",
                                node_host=self._host,
                                node_port=self._tcp_port,
                                node_id=self._node_id.short,
                            ),
                        )
                        job.status = JobStatus.FAILED.value
                        self._increment_version()
                        return
                    elif result is False:
                        # Dispatch failed
                        job.status = JobStatus.FAILED.value
                        self._increment_version()
                        return
            
            job.status = JobStatus.RUNNING.value
            self._increment_version()
            
        finally:
            # Cleanup will happen when job completes
            pass
    
    async def _dispatch_single_workflow(
        self,
        submission: JobSubmission,
        idx: int,
        workflow: Any,
        cores_needed: int,
        cloudpickle,
    ) -> bool:
        """
        Dispatch a single workflow to a worker with resource-aware waiting.
        
        If no worker has sufficient capacity, waits with exponential backoff
        until resources become available or timeout is reached.
        
        Returns True if dispatch succeeded, False otherwise.
        """
        workflow_id = f"{submission.job_id}:{idx}"
        
        # Resource-aware waiting with exponential backoff
        max_wait = submission.timeout_seconds
        waited = 0.0
        backoff = 0.5  # Start with 500ms
        max_backoff = 5.0  # Cap at 5 seconds
        
        worker_id = None
        while waited < max_wait:
            # Try to select a worker with sufficient capacity
            worker_id = self._select_worker_for_workflow(cores_needed)
            if worker_id:
                break
            
            # Log that we're waiting for resources
            if waited == 0:  # Only log on first attempt
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfo(
                        message=f"Waiting for {cores_needed} cores for {workflow_id} (none available)",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    ),
                )
            
            # Wait with exponential backoff
            await asyncio.sleep(backoff)
            waited += backoff
            backoff = min(backoff * 1.5, max_backoff)
        
        if not worker_id:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Timeout waiting for {cores_needed} cores for {workflow_id} after {waited:.1f}s",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
            return False
        
        if waited > 0:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Found worker for {workflow_id} after {waited:.1f}s wait",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                ),
            )
        
        # Create provision request for quorum
        provision = ProvisionRequest(
            job_id=submission.job_id,
            workflow_id=workflow_id,
            target_worker=worker_id,
            cores_required=cores_needed,
            fence_token=self._get_fence_token(),
            version=self._state_version,
        )
        
        # Request quorum (skip if only one manager)
        if self._manager_peers:
            try:
                await self._request_quorum_confirmation(provision)
                # Send commit to all managers on success
                await self._send_provision_commit(provision)
            except (QuorumCircuitOpenError, QuorumUnavailableError, QuorumTimeoutError) as e:
                # Log the specific quorum failure and return False
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=f"Quorum failed for {workflow_id}: {e.message}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )
                return False
        
        # Extract dependency context for this workflow
        dependency_context = await self._extract_dependency_context(
            submission.job_id, workflow
        )
        context_version = self._job_layer_version.get(submission.job_id, 0)
        
        # Dispatch to worker
        dispatch = WorkflowDispatch(
            job_id=submission.job_id,
            workflow_id=workflow_id,
            workflow=cloudpickle.dumps(workflow),
            context=b'{}',  # Legacy field, kept for compatibility
            vus=cores_needed,
            timeout_seconds=submission.timeout_seconds,
            fence_token=provision.fence_token,
            context_version=context_version,
            dependency_context=dependency_context,
        )
        
        # Store dispatch bytes for potential retry
        dispatch_bytes = dispatch.dump()
        self._workflow_retries[workflow_id] = (0, dispatch_bytes, set())
        
        ack = await self._dispatch_workflow_to_worker(worker_id, dispatch)
        if not ack or not ack.accepted:
            return False
        
        self._workflow_assignments[workflow_id] = worker_id
        return True
    
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
            worker_hb = self._worker_status.get(request.target_worker)
            can_confirm = (
                worker_hb is not None and
                worker_hb.available_cores >= request.cores_required and
                worker_hb.state == WorkerState.HEALTHY.value
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
            
            # Update our tracking
            self._workflow_assignments[commit.workflow_id] = commit.target_worker
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
        """Handle state sync request (when new leader needs current state)."""
        try:
            request = StateSyncRequest.load(data)
            
            response = StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=self._state_version,
                manager_state=self._get_state_snapshot(),
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
            
            job = self._jobs.get(cancel.job_id)
            if not job:
                ack = CancelAck(
                    job_id=cancel.job_id,
                    cancelled=False,
                    error="Job not found",
                )
                return ack.dump()
            
            # Cancel all workflows on workers
            cancelled_count = 0
            for workflow_id, worker_id in list(self._workflow_assignments.items()):
                if workflow_id.startswith(cancel.job_id + ":"):
                    worker = self._workers.get(worker_id)
                    if worker:
                        try:
                            await self.send_tcp(
                                (worker.node.host, worker.node.port),
                                "cancel_job",
                                cancel.dump(),
                                timeout=2.0,
                            )
                            cancelled_count += 1
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

