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
import time
from typing import Any

from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.distributed_rewrite.server.events import VersionedStateClock
from hyperscale.distributed_rewrite.swim import HealthAwareServer, GateStateEmbedder
from hyperscale.distributed_rewrite.models import (
    NodeInfo,
    NodeRole,
    ManagerHeartbeat,
    JobSubmission,
    JobAck,
    JobStatus,
    JobProgress,
    GlobalJobStatus,
    StateSyncRequest,
    StateSyncResponse,
    CancelJob,
    CancelAck,
    DatacenterLease,
    LeaseTransfer,
    DatacenterHealth,
    DatacenterStatus,
    UpdateTier,
    restricted_loads,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError


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
        
        # Known datacenters and their status (from TCP updates)
        self._datacenter_status: dict[str, ManagerHeartbeat] = {}  # dc -> last status
        self._datacenter_last_status: dict[str, float] = {}  # dc -> timestamp
        
        # Versioned state clock for rejecting stale updates
        # Tracks per-datacenter versions using Lamport timestamps
        self._versioned_clock = VersionedStateClock()
        
        # Global job state
        self._jobs: dict[str, GlobalJobStatus] = {}  # job_id -> status
        
        # Lease management for at-most-once
        self._leases: dict[str, DatacenterLease] = {}  # job_id:dc -> lease
        self._fence_token = 0
        
        # State versioning (local gate state version)
        self._state_version = 0
        
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
            get_active_jobs=lambda: len(self._jobs),
            on_manager_heartbeat=self._handle_embedded_manager_heartbeat,
        ))
        
        # Register node death and join callbacks for failure/recovery handling
        # (Same pattern as ManagerServer for split-brain prevention)
        self.register_on_node_dead(self._on_node_dead)
        self.register_on_node_join(self._on_node_join)
    
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
        
        # Accept update
        self._datacenter_status[heartbeat.datacenter] = heartbeat
        self._datacenter_last_status[heartbeat.datacenter] = time.monotonic()
        
        # Update version tracking via TaskRunner
        self._task_runner.run(
            self._versioned_clock.update_entity, dc_key, heartbeat.version
        )
    
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
    
    def _classify_datacenter_health(self, dc_id: str) -> DatacenterStatus:
        """
        Classify datacenter health based on SWIM probes and status updates.
        
        Health States:
        - HEALTHY: Managers responding, workers available, capacity exists
        - BUSY: Managers responding, workers available, no immediate capacity
        - DEGRADED: Some managers responding, reduced capacity
        - UNHEALTHY: No managers responding OR all workers down
        
        Key insight: BUSY ≠ UNHEALTHY
        - BUSY = transient, will clear → accept job (queued)
        - UNHEALTHY = structural problem → try fallback
        
        See AD-16 in docs/architecture.md.
        """
        # Check manager liveness via SWIM
        manager_udp_addrs = self._datacenter_manager_udp.get(dc_id, [])
        alive_managers = 0
        for addr in manager_udp_addrs:
            node_state = self._incarnation_tracker.get_node_state(addr)
            if node_state and node_state.status == b'OK':
                alive_managers += 1
        
        # Get status from TCP heartbeats
        status = self._datacenter_status.get(dc_id)
        
        # No managers responding = UNHEALTHY
        if alive_managers == 0:
            return DatacenterStatus(
                dc_id=dc_id,
                health=DatacenterHealth.UNHEALTHY.value,
                available_capacity=0,
                queue_depth=0,
                manager_count=0,
                worker_count=0,
                last_update=time.monotonic(),
            )
        
        # No status update or no workers = UNHEALTHY
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
        
        # Determine health based on capacity
        if status.available_cores > 0:
            health = DatacenterHealth.HEALTHY
        elif status.worker_count > 0:
            # Workers exist but no capacity = BUSY (not unhealthy!)
            health = DatacenterHealth.BUSY
        else:
            health = DatacenterHealth.DEGRADED
        
        # Partial manager response = DEGRADED
        if len(manager_udp_addrs) > 0 and alive_managers < len(manager_udp_addrs) // 2 + 1:
            health = DatacenterHealth.DEGRADED
        
        return DatacenterStatus(
            dc_id=dc_id,
            health=health.value,
            available_capacity=status.available_cores,
            queue_depth=getattr(status, 'queue_depth', 0),
            manager_count=alive_managers,
            worker_count=status.worker_count,
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
    ) -> tuple[list[str], list[str]]:
        """
        Select datacenters with fallback list for resilient routing.
        
        Priority order: HEALTHY > BUSY > DEGRADED
        Only UNHEALTHY DCs are excluded entirely.
        
        Args:
            count: Number of primary DCs to select
            preferred: Optional list of preferred DCs
            
        Returns:
            (primary_dcs, fallback_dcs)
        """
        # Classify all DCs
        dc_health = self._get_all_datacenter_health()
        
        # Bucket by health
        healthy = []
        busy = []
        degraded = []
        
        for dc_id, status in dc_health.items():
            if status.health == DatacenterHealth.HEALTHY.value:
                healthy.append((dc_id, status))
            elif status.health == DatacenterHealth.BUSY.value:
                busy.append((dc_id, status))
            elif status.health == DatacenterHealth.DEGRADED.value:
                degraded.append((dc_id, status))
            # UNHEALTHY DCs are excluded
        
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
        
        # Build selection: HEALTHY first, then BUSY, then DEGRADED
        all_usable = healthy_ids + busy_ids + degraded_ids
        
        if len(all_usable) == 0:
            # All DCs are UNHEALTHY - will cause job failure
            return ([], [])
        
        # Primary = first `count` DCs
        primary = all_usable[:count]
        # Fallback = remaining usable DCs
        fallback = all_usable[count:]
        
        return (primary, fallback)
    
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
        primary, _ = self._select_datacenters_with_fallback(count, preferred)
        return primary
    
    async def _try_dispatch_to_dc(
        self,
        job_id: str,
        dc: str,
        submission: JobSubmission,
    ) -> tuple[bool, str | None]:
        """
        Try to dispatch job to a single datacenter.
        
        Returns:
            (success: bool, error: str | None)
            - True if DC accepted (even if queued)
            - False only if DC is UNHEALTHY (should try fallback)
        """
        managers = self._datacenter_managers.get(dc, [])
        
        for manager_addr in managers:
            try:
                response = await self.send_tcp(
                    manager_addr,
                    "job_submission",
                    submission.dump(),
                    timeout=5.0,
                )
                
                if isinstance(response, bytes):
                    ack = JobAck.load(response)
                    if ack.accepted:
                        return (True, None)
                    # Check if it's a capacity issue vs unhealthy
                    if ack.error:
                        error_lower = ack.error.lower()
                        if "no capacity" in error_lower or "busy" in error_lower:
                            # BUSY is still acceptable - job will be queued
                            return (True, None)
                    # Other errors = try next manager
                    
            except Exception as e:
                # Connection error = manager might be down
                continue
        
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
        from hyperscale.distributed_rewrite.models import UpdateTier
        
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
        """
        job = self._jobs.get(job_id)
        if not job:
            return
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Job {job_id}: Immediate update - {event_type}",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
        
        # In a real implementation, this would push to subscribed clients
        # For now, we just track that we would send it
        # Future: WebSocket push, SSE, or callback webhook
    
    async def _batch_stats_update(self) -> None:
        """
        Process a batch of Tier 2 (Periodic) updates.
        
        Aggregates pending progress updates and sends them in a single batch.
        More efficient than sending each update individually.
        """
        # Collect pending updates
        pending_jobs = []
        for job_id, job in self._jobs.items():
            if job.status == JobStatus.RUNNING.value:
                pending_jobs.append((job_id, job))
        
        if not pending_jobs:
            return
        
        # In a real implementation, this would:
        # 1. Aggregate stats from multiple DCs using CRDTs
        # 2. Batch into a single message
        # 3. Send to subscribed clients
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Batch stats update: {len(pending_jobs)} jobs",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
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
        from hyperscale.distributed_rewrite.models import UpdateTier
        
        tier = self._classify_update_tier(job_id, old_status, new_status)
        
        if tier == UpdateTier.IMMEDIATE.value:
            self._task_runner.run(
                self._send_immediate_update,
                job_id,
                f"status:{old_status}->{new_status}",
                progress_data,
            )
        # Tier 2 and 3 are handled by batch loop and on-demand requests
    
    async def start(self) -> None:
        """Start the gate server."""
        await super().start()
        
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
        
        # Start background cleanup tasks via TaskRunner
        self._task_runner.run(self._lease_cleanup_loop)
        self._task_runner.run(self._job_cleanup_loop)
        
        # Start Tier 2 (periodic) batch stats loop
        self._task_runner.run(self._batch_stats_loop)
        
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"Gate started with {len(self._datacenter_managers)} configured DCs, SWIM healthcheck active",
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
                response = await self.send_tcp(
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
                    response = await self.send_tcp(
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
    async def receive_manager_status_update(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle manager status update via TCP.
        
        This is NOT a healthcheck - DC liveness is tracked via SWIM UDP probes.
        This contains job progress and worker capacity information.
        """
        try:
            status = ManagerHeartbeat.load(data)
            
            # Update DC status tracking (from TCP)
            self._datacenter_status[status.datacenter] = status
            self._datacenter_last_status[status.datacenter] = time.monotonic()
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "receive_manager_status_update")
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
    async def receive_job_submission(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job submission from client."""
        try:
            submission = JobSubmission.load(data)
            
            # Only leader accepts jobs
            if not self.is_leader():
                leader = self.get_current_leader()
                ack = JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Not leader. Leader is {leader}" if leader else "No leader",
                )
                return ack.dump()
            
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
            self._increment_version()
            
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
            
        except Exception as e:
            await self.handle_exception(e, "receive_job_submission")
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
        
        Key behavior (per AD-17):
        - Only fails if ALL DCs are UNHEALTHY
        - BUSY DCs are acceptable (job will be queued)
        """
        job = self._jobs.get(submission.job_id)
        if not job:
            return
        
        job.status = JobStatus.DISPATCHING.value
        self._increment_version()
        
        # Get primary and fallback DCs based on health classification
        primary_dcs, fallback_dcs = self._select_datacenters_with_fallback(
            len(target_dcs),
            target_dcs if target_dcs else None,
        )
        
        # If we have no usable DCs at all, fail immediately
        if not primary_dcs and not fallback_dcs:
            job.status = JobStatus.FAILED.value
            job.failed_datacenters = len(target_dcs)
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Job {submission.job_id}: All datacenters are UNHEALTHY",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            self._increment_version()
            return
        
        # Dispatch with fallback support
        successful_dcs, failed_dcs = await self._dispatch_job_with_fallback(
            submission,
            primary_dcs,
            fallback_dcs,
        )
        
        if not successful_dcs:
            # All DCs failed (all UNHEALTHY)
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
            
            return b'ok'
            
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
                        response = await self.send_tcp(
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

