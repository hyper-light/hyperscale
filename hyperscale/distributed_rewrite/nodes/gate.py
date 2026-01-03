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
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.distributed_rewrite.server.events import VersionedStateClock
from hyperscale.distributed_rewrite.swim import HealthAwareServer, GateStateEmbedder
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
    DatacenterLease,
    LeaseTransfer,
    DatacenterHealth,
    DatacenterStatus,
    UpdateTier,
)
from hyperscale.distributed_rewrite.swim.core import (
    QuorumError,
    QuorumUnavailableError,
    QuorumTimeoutError,
    QuorumCircuitOpenError,
    ErrorStats,
    CircuitState,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning, ServerError


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
        
        # Per-manager circuit breakers for dispatch failures
        # Key is manager TCP address tuple, value is ErrorStats
        self._manager_circuits: dict[tuple[str, int], ErrorStats] = {}
        
        # Gate peers for clustering
        self._gate_peers = gate_peers or []  # TCP
        self._gate_udp_peers = gate_udp_peers or []  # UDP for SWIM cluster
        
        # Track gate peer addresses for failure detection (same pattern as managers)
        # Maps UDP addr -> TCP addr for peer gates
        self._gate_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}
        for i, tcp_addr in enumerate(self._gate_peers):
            if i < len(self._gate_udp_peers):
                self._gate_udp_to_tcp[self._gate_udp_peers[i]] = tcp_addr
        
        # Track active gate peers (removed when SWIM marks as dead)
        self._active_gate_peers: set[tuple[str, int]] = set(self._gate_peers)
        
        # Track gate peer info from GateHeartbeat (proper node_ids, leadership, etc)
        # Maps UDP addr -> GateHeartbeat for peers we've heard from via SWIM
        self._gate_peer_info: dict[tuple[str, int], GateHeartbeat] = {}
        
        # Known datacenters and their status (from TCP updates)
        # Stored per-datacenter, per-manager for proper aggregation
        self._datacenter_manager_status: dict[str, dict[tuple[str, int], ManagerHeartbeat]] = {}  # dc -> {manager_addr -> heartbeat}
        self._manager_last_status: dict[tuple[str, int], float] = {}  # manager_addr -> timestamp
        
        # Versioned state clock for rejecting stale updates
        # Tracks per-datacenter versions using Lamport timestamps
        self._versioned_clock = VersionedStateClock()
        
        # Global job state
        self._jobs: dict[str, GlobalJobStatus] = {}  # job_id -> status
        
        # Per-DC final results for job completion aggregation
        # job_id -> {datacenter -> JobFinalResult}
        self._job_dc_results: dict[str, dict[str, JobFinalResult]] = {}
        
        # Track which DCs were assigned for each job (to know when complete)
        # job_id -> set of datacenter IDs
        self._job_target_dcs: dict[str, set[str]] = {}
        
        # Client push notification callbacks
        # job_id -> callback address for push notifications
        self._job_callbacks: dict[str, tuple[str, int]] = {}
        
        # Lease management for at-most-once
        self._leases: dict[str, DatacenterLease] = {}  # job_id:dc -> lease
        self._fence_token = 0
        
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
        
        # Configuration
        self._lease_timeout = lease_timeout
        
        # Job cleanup configuration
        self._job_max_age: float = 3600.0  # 1 hour max age for completed jobs
        self._job_cleanup_interval: float = 60.0  # Check every minute
        
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
            on_manager_heartbeat=self._handle_embedded_manager_heartbeat,
            on_gate_heartbeat=self._handle_gate_peer_heartbeat,
        ))
        
        # Register node death and join callbacks for failure/recovery handling
        # (Same pattern as ManagerServer for split-brain prevention)
        self.register_on_node_dead(self._on_node_dead)
        self.register_on_node_join(self._on_node_join)
        
        # Register leadership callbacks for state sync
        self.register_on_become_leader(self._on_gate_become_leader)
        self.register_on_lose_leadership(self._on_gate_lose_leadership)
    
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
        """
        # Remove from active peers
        self._active_gate_peers.discard(tcp_addr)
        
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
        """
        # Add back to active peers
        self._active_gate_peers.add(tcp_addr)
        
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
        """
        # Check if update is stale using versioned clock
        if self._versioned_clock.is_entity_stale(heartbeat.node_id, heartbeat.version):
            return
        
        # Store peer info keyed by UDP address
        self._gate_peer_info[source_addr] = heartbeat
        
        # Update version tracking
        self._task_runner.run(
            self._versioned_clock.update_entity, heartbeat.node_id, heartbeat.version
        )
    
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
            for udp, tcp in self._gate_udp_to_tcp.items():
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
    
    def _get_state_snapshot(self) -> GateStateSnapshot:
        """Get a complete state snapshot for state sync."""
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
                        if sync_response.gate_state:
                            self._apply_gate_state_snapshot(sync_response.gate_state)
                            synced_count += 1
                    break  # Success
                    
                except Exception as e:
                    if attempt == max_retries - 1:
                        await self.handle_exception(e, f"state_sync_from_{peer_addr}")
                    else:
                        await asyncio.sleep(0.5 * (2 ** attempt))  # Backoff
        
        self._task_runner.run(
            self._udp_logger.log,
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
        """Get health classification for all configured datacenters."""
        return {
            dc_id: self._classify_datacenter_health(dc_id)
            for dc_id in self._datacenter_managers.keys()
        }
    
    def _get_available_datacenters(self) -> list[str]:
        """
        Get list of healthy datacenters (for backwards compatibility).
        
        A datacenter is healthy if:
        1. Its manager(s) are alive per SWIM UDP probes
        2. It has workers available (from TCP status updates)
        """
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
            - "unhealthy": All DCs are unhealthy (job should fail)
        """
        # Classify all DCs
        dc_health = self._get_all_datacenter_health()
        
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
            # All DCs are UNHEALTHY - will cause job failure
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
    ) -> tuple[bool, str | None]:
        """
        Try to dispatch job to a single datacenter.
        
        Iterates through managers in the DC, using _try_dispatch_to_manager
        which handles retries and circuit breakers.
        
        Returns:
            (success: bool, error: str | None)
            - True if DC accepted (even if queued)
            - False only if DC is UNHEALTHY (should try fallback)
        """
        managers = self._datacenter_managers.get(dc, [])
        
        for manager_addr in managers:
            success, error = await self._try_dispatch_to_manager(
                manager_addr, submission
            )
            if success:
                return (True, None)
            # Continue to next manager
        
        # All managers failed = DC is UNHEALTHY for this dispatch
        return (False, f"All managers in {dc} failed to accept job")
    
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
        
        for dc in primary_dcs:
            success, error = await self._try_dispatch_to_dc(
                submission.job_id, dc, submission
            )
            
            if success:
                successful.append(dc)
            else:
                # Try fallback
                fallback_success = False
                while fallback_queue:
                    fallback_dc = fallback_queue.pop(0)
                    fb_success, fb_error = await self._try_dispatch_to_dc(
                        submission.job_id, fallback_dc, submission
                    )
                    if fb_success:
                        successful.append(fallback_dc)
                        fallback_success = True
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerInfo(
                                message=f"Job {submission.job_id}: Fallback from {dc} to {fallback_dc}",
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
            
            # Clean up callback if job is final
            if is_final:
                self._job_callbacks.pop(job_id, None)
    
    async def _batch_stats_update(self) -> None:
        """
        Process a batch of Tier 2 (Periodic) updates.
        
        Aggregates pending progress updates and pushes to clients
        that have registered callbacks. This is more efficient than
        sending each update individually.
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
        batch_interval = getattr(self, '_batch_stats_interval', 2.0)  # Default 2s
        
        while True:
            try:
                await asyncio.sleep(batch_interval)
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
                    node_id=self._node_id.full,
                    datacenter=self._node_id.datacenter,
                    current_version=self._state_version,
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
        
        # Add datacenter managers to SWIM for health probing
        for dc, manager_udp_addrs in self._datacenter_manager_udp.items():
            for manager_addr in manager_udp_addrs:
                self._probe_scheduler.add_member(manager_addr)
        
        # Start SWIM probe cycle (UDP healthchecks for gates + DC managers)
        self._task_runner.run(self.start_probe_cycle)
        
        # Start leader election (uses SWIM membership info)
        await self.start_leader_election()
        
        # Wait a short time for leader election to stabilize
        await asyncio.sleep(0.5)
        
        # Sync state and transition to ACTIVE
        await self._complete_startup_sync()
        
        # Start background cleanup tasks via TaskRunner
        self._task_runner.run(self._lease_cleanup_loop)
        self._task_runner.run(self._job_cleanup_loop)
        
        # Start Tier 2 (periodic) batch stats loop
        self._task_runner.run(self._batch_stats_loop)
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate started with {len(self._datacenter_managers)} configured DCs, " +
                        f"state={self._gate_state.value}, SWIM healthcheck active",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def stop(self) -> None:
        """Stop the gate server."""
        # TaskRunner handles cleanup task cancellation
        # Graceful shutdown broadcasts leave via UDP (SWIM)
        await self.graceful_shutdown()
        
        await super().stop()
    
    async def _lease_cleanup_loop(self) -> None:
        """Periodically clean up expired leases."""
        while self._running:
            try:
                await asyncio.sleep(self._lease_timeout / 2)
                
                now = time.monotonic()
                expired = []
                for key, lease in self._leases.items():
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
                
                for job_id, job in self._jobs.items():
                    if job.status in terminal_states:
                        # Check age - use elapsed_seconds as relative timestamp
                        # or timestamp if available
                        age = now - getattr(job, 'timestamp', now)
                        if age > self._job_max_age:
                            jobs_to_remove.append(job_id)
                
                for job_id in jobs_to_remove:
                    self._jobs.pop(job_id, None)
                    # Also clean up any leases for this job
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
        """
        try:
            heartbeat = ManagerHeartbeat.load(data)
            
            # Store per-datacenter, per-manager using manager's self-reported address
            dc = heartbeat.datacenter
            manager_addr = (heartbeat.tcp_host, heartbeat.tcp_port)
            
            if dc not in self._datacenter_manager_status:
                self._datacenter_manager_status[dc] = {}
            self._datacenter_manager_status[dc][manager_addr] = heartbeat
            self._manager_last_status[manager_addr] = time.monotonic()
            
            # Add manager address to datacenter managers (if not already tracked)
            if dc not in self._datacenter_managers:
                self._datacenter_managers[dc] = []
            if manager_addr not in self._datacenter_managers[dc]:
                self._datacenter_managers[dc].append(manager_addr)
            
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"Manager registered: {heartbeat.node_id} from DC {dc} ({heartbeat.worker_count} workers)",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            
            # Return ack with all healthy gates
            response = ManagerRegistrationResponse(
                accepted=True,
                gate_id=self._node_id.full,
                healthy_gates=self._get_healthy_gates(),
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
        
        Only the cluster leader accepts new jobs. Non-leaders redirect
        clients to the current leader for consistent job coordination.
        """
        try:
            submission = JobSubmission.load(data)
            
            # Only leader accepts new jobs
            if not self.is_leader():
                leader = self.get_current_leader()
                ack = JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Not leader" if leader else "No leader elected",
                    leader_addr=leader,
                )
                return ack.dump()
            
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
            
            # Select datacenters
            target_dcs = self._select_datacenters(
                submission.datacenter_count,
                submission.datacenters if submission.datacenters else None,
            )
            
            if not target_dcs:
                ack = JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error="No available datacenters",
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
            
            # Store callback for push notifications (if provided)
            if submission.callback_addr:
                self._job_callbacks[submission.job_id] = submission.callback_addr
            
            self._increment_version()
            
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
        """
        job = self._jobs.get(submission.job_id)
        if not job:
            return
        
        job.status = JobStatus.DISPATCHING.value
        self._increment_version()
        
        # Get primary and fallback DCs based on health classification
        primary_dcs, fallback_dcs, worst_health = self._select_datacenters_with_fallback(
            len(target_dcs),
            target_dcs if target_dcs else None,
        )
        
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
        try:
            job_id = data.decode()
            status = await self._gather_job_status(job_id)
            return status.dump()
            
        except Exception as e:
            await self.handle_exception(e, "receive_job_status_request")
            return b''
    
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
        """
        try:
            progress = JobProgress.load(data)
            
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
        """Handle job cancellation from client."""
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
            
            # Cancel in all DCs
            cancelled_workflows = 0
            for dc in self._get_available_datacenters():
                managers = self._datacenter_managers.get(dc, [])
                for manager_addr in managers:
                    try:
                        response, _ = await self.send_tcp(
                            manager_addr,
                            "cancel_job",
                            cancel.dump(),
                            timeout=2.0,
                        )
                        if isinstance(response, bytes):
                            dc_ack = CancelAck.load(response)
                            cancelled_workflows += dc_ack.workflows_cancelled
                            break
                    except Exception:
                        continue
            
            job.status = JobStatus.CANCELLED.value
            self._increment_version()
            
            ack = CancelAck(
                job_id=cancel.job_id,
                cancelled=True,
                workflows_cancelled=cancelled_workflows,
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
        """
        try:
            request = StateSyncRequest.load(data)
            
            # Build and return state snapshot
            snapshot = self._get_state_snapshot()
            response = StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=self._state_version,
                gate_state=snapshot,
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
        """
        try:
            result = JobFinalResult.load(data)
            
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
        all_workflow_stats: dict[str, list[WorkflowStats]] = defaultdict(list)
        
        for dc_result in all_dc_results:
            for wf_result in dc_result.workflow_results:
                try:
                    # Unpickle WorkflowStats from the workflow result
                    workflow_stats: WorkflowStats = cloudpickle.loads(wf_result.results)
                    all_workflow_stats[wf_result.workflow_name].append(workflow_stats)
                except Exception as e:
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerWarning(
                            message=f"Failed to unpickle WorkflowStats for {wf_result.workflow_name}: {e}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
        
        # 2. Merge WorkflowStats per workflow using Results.merge_results()
        merged_workflow_stats: list[WorkflowStats] = []
        aggregator = Results()
        
        for workflow_name, stats_list in all_workflow_stats.items():
            if len(stats_list) > 1:
                # Multiple DCs ran this workflow - merge their stats
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
        
        # Clean up
        self._job_dc_results.pop(job_id, None)

