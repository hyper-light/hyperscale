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

import networkx

from hyperscale.core.graph.dependent_workflow import DependentWorkflow
from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.distributed_rewrite.server.events import VersionedStateClock
from hyperscale.distributed_rewrite.swim import HealthAwareServer, ManagerStateEmbedder
from hyperscale.distributed_rewrite.models import (
    NodeInfo,
    NodeRole,
    ManagerInfo,
    RegistrationResponse,
    WorkerRegistration,
    WorkerHeartbeat,
    WorkerState,
    WorkerStateSnapshot,
    ManagerHeartbeat,
    ManagerStateSnapshot,
    JobSubmission,
    JobAck,
    JobStatus,
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
    restricted_loads,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError


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
        
        # Gate discovery (optional)
        self._gate_addrs = gate_addrs or []  # TCP
        self._gate_udp_addrs = gate_udp_addrs or []  # UDP for SWIM
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
        
        # Registered workers (indexed by node_id)
        self._workers: dict[str, WorkerRegistration] = {}  # node_id -> registration
        self._worker_addr_to_id: dict[tuple[str, int], str] = {}  # (host, port) -> node_id (reverse mapping)
        self._worker_status: dict[str, WorkerHeartbeat] = {}  # node_id -> last status
        self._worker_last_status: dict[str, float] = {}  # node_id -> timestamp
        
        # Versioned state clock for rejecting stale updates
        # Tracks per-worker and per-job versions using Lamport timestamps
        self._versioned_clock = VersionedStateClock()
        
        # Job and workflow state
        self._jobs: dict[str, JobProgress] = {}  # job_id -> progress
        self._workflow_assignments: dict[str, str] = {}  # workflow_id -> worker_node_id
        self._pending_provisions: dict[str, ProvisionRequest] = {}  # workflow_id -> request
        self._provision_confirmations: dict[str, set[str]] = {}  # workflow_id -> confirming nodes
        
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
        
        # Quorum settings
        self._quorum_timeout = quorum_timeout
        
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
            get_available_cores=lambda: sum(
                status.available_cores for status in self._worker_status.values()
            ),
            on_worker_heartbeat=self._handle_embedded_worker_heartbeat,
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
        required_quorum = self._quorum_size()
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
        required_quorum = self._quorum_size()
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
                response = await self.send_tcp(
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
                response = await self.send_tcp(
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
        return None
    
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
        
        Returns True if the number of active managers (including self)
        is >= the required quorum size.
        """
        active_count = len(self._active_manager_peers) + 1  # Include self
        return active_count >= self._quorum_size()
    
    def _get_healthy_managers(self) -> list[ManagerInfo]:
        """
        Build list of all known healthy managers for worker discovery.
        
        Includes self and all active peer managers. Workers use this
        to maintain redundant communication channels.
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
            udp_addr = tcp_addr  # Default to same
            for udp, tcp in self._manager_udp_to_tcp.items():
                if tcp == tcp_addr:
                    udp_addr = udp
                    break
            
            managers.append(ManagerInfo(
                node_id=f"manager-{tcp_addr[0]}:{tcp_addr[1]}",  # Synthetic ID
                tcp_host=tcp_addr[0],
                tcp_port=tcp_addr[1],
                udp_host=udp_addr[0],
                udp_port=udp_addr[1],
                datacenter=self._node_id.datacenter,  # Assume same DC
                is_leader=False,  # We are the one responding, we know if we're leader
            ))
        
        return managers
    
    async def start(self) -> None:
        """Start the manager server."""
        await super().start()
        
        # Join SWIM cluster with other managers (UDP healthchecks)
        for peer_udp in self._manager_udp_peers:
            await self.join_cluster(peer_udp)
        
        # Start SWIM probe cycle (UDP healthchecks for managers + workers)
        self._task_runner.run(self.start_probe_cycle)
        
        # Start leader election (uses SWIM membership info)
        await self.start_leader_election()
        
        # Start background cleanup for completed jobs
        self._task_runner.run(self._job_cleanup_loop)
        
        # Start TCP heartbeat loop to gates (supplements SWIM embedding)
        # TCP provides reliability for critical status updates
        if self._gate_addrs:
            self._task_runner.run(self._gate_heartbeat_loop)
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Manager started in DC {self._node_id.datacenter}, SWIM healthcheck active",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def stop(self) -> None:
        """Stop the manager server."""
        # Graceful shutdown broadcasts leave via UDP (SWIM)
        await self.graceful_shutdown()
        
        await super().stop()
    
    def _build_manager_heartbeat(self) -> ManagerHeartbeat:
        """Build a ManagerHeartbeat with current state."""
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
            available_cores=sum(
                status.available_cores
                for status in self._worker_status.values()
            ),
        )
    
    async def _gate_heartbeat_loop(self) -> None:
        """
        Periodically send ManagerHeartbeat to gates via TCP.
        
        This supplements the Serf-style SWIM embedding for reliability.
        Gates use this for datacenter health classification.
        """
        heartbeat_interval = 5.0  # Send every 5 seconds
        
        while self._running:
            try:
                await asyncio.sleep(heartbeat_interval)
                
                heartbeat = self._build_manager_heartbeat()
                
                # Send to all configured gates
                for gate_addr in self._gate_addrs:
                    try:
                        await self.send_tcp(
                            gate_addr,
                            "manager_status_update",
                            heartbeat.dump(),
                            timeout=2.0,
                        )
                    except Exception:
                        # Gate might be down - continue to others
                        pass
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.handle_exception(e, "gate_heartbeat_loop")
    
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
        
        return ManagerStateSnapshot(
            node_id=self._node_id.full,
            datacenter=self._node_id.datacenter,
            is_leader=self.is_leader(),
            term=self._leader_election.state.current_term,
            version=self._state_version,
            workers=worker_snapshots,
            jobs=dict(self._jobs),
        )
    
    def _select_worker_for_workflow(self, vus_needed: int) -> str | None:
        """
        Select a worker with sufficient capacity for a workflow.
        
        Uses cryptographically secure random selection among eligible workers.
        Also checks SWIM membership - only select workers that are ALIVE.
        """
        eligible = []
        for node_id, status in self._worker_status.items():
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
    ) -> WorkflowDispatchAck | None:
        """Dispatch a workflow to a specific worker."""
        worker = self._workers.get(worker_node_id)
        if not worker:
            return None
        
        worker_addr = (worker.node.host, worker.node.port)
        
        try:
            response = await self.send_tcp(
                worker_addr,
                "workflow_dispatch",
                dispatch.dump(),
                timeout=5.0,
            )
            
            if isinstance(response, bytes):
                return WorkflowDispatchAck.load(response)
            return None
            
        except Exception as e:
            await self.handle_exception(e, f"dispatch_to_worker_{worker_node_id}")
            return None
    
    async def _request_quorum_confirmation(
        self,
        provision: ProvisionRequest,
    ) -> bool:
        """
        Request quorum confirmation for a provisioning decision.
        
        Returns True if quorum is achieved, False otherwise.
        """
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
            return len(confirmed) >= self._quorum_size
            
        except asyncio.TimeoutError:
            confirmed = self._provision_confirmations.get(provision.workflow_id, set())
            return len(confirmed) >= self._quorum_size
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
            response = await self.send_tcp(
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
    
    @tcp.receive()
    async def receive_worker_register(
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
            return response.dump()
            
        except Exception as e:
            await self.handle_exception(e, "receive_worker_register")
            # Return error response
            response = RegistrationResponse(
                accepted=False,
                manager_id=self._node_id.full,
                healthy_managers=[],
                error=str(e),
            )
            return response.dump()
    
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
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "receive_workflow_progress")
            return b'error'
    
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
            )
            
            # Get worker address
            worker_reg = self._workers.get(new_worker)
            if not worker_reg:
                return False
            
            worker_addr = (worker_reg.node.host, worker_reg.node.port)
            
            # Send dispatch
            response = await self.send_tcp(
                worker_addr,
                "workflow_dispatch",
                new_dispatch.dump(),
                timeout=5.0,
            )
            
            if response:
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
        """
        eligible = []
        for node_id, status in self._worker_status.items():
            if node_id in exclude_workers:
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
    async def receive_job_submission(
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
                    error=f"Not leader. Leader is {leader}" if leader else "No leader elected",
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
            await self.handle_exception(e, "receive_job_submission")
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
            confirmed = await self._request_quorum_confirmation(provision)
            if not confirmed:
                return False
            
            # Send commit to all managers
            await self._send_provision_commit(provision)
        
        # Dispatch to worker
        dispatch = WorkflowDispatch(
            job_id=submission.job_id,
            workflow_id=workflow_id,
            workflow=cloudpickle.dumps(workflow),
            context=b'{}',  # Context would come from job
            vus=cores_needed,
            timeout_seconds=submission.timeout_seconds,
            fence_token=provision.fence_token,
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
    async def receive_provision_request(
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
    async def receive_provision_commit(
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

