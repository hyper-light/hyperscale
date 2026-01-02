"""
Gate Node Server.

Gates coordinate job execution across datacenters. They:
- Accept jobs from clients
- Dispatch jobs to datacenter managers
- Aggregate global job status
- Handle cross-DC retry with leases
- Provide the global job view to clients
"""

import asyncio
import pickle
import secrets
import time
from typing import Any

from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.distributed_rewrite.swim import UDPServer
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
    restricted_loads,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError


class GateServer(UDPServer):
    """
    Gate node in the distributed Hyperscale system.
    
    Gates:
    - Form a gossip cluster for leader election
    - Accept job submissions from clients
    - Dispatch jobs to managers in target datacenters
    - Aggregate global job status across DCs
    - Manage leases for at-most-once semantics
    - Handle DC failure and retry
    
    The leader gate is responsible for:
    - Accepting new job submissions
    - Managing the global job registry
    - Coordinating lease transfers during scaling
    """
    
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "global",  # Gates typically span DCs
        datacenter_managers: dict[str, list[tuple[str, int]]] | None = None,
        gate_peers: list[tuple[str, int]] | None = None,
        heartbeat_interval: float = 1.0,
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
        self._datacenter_managers = datacenter_managers or {}
        
        # Gate peers for clustering
        self._gate_peers = gate_peers or []
        
        # Known datacenters and their health
        self._datacenter_health: dict[str, ManagerHeartbeat] = {}  # dc -> last heartbeat
        self._datacenter_last_seen: dict[str, float] = {}  # dc -> timestamp
        
        # Global job state
        self._jobs: dict[str, GlobalJobStatus] = {}  # job_id -> status
        
        # Lease management for at-most-once
        self._leases: dict[str, DatacenterLease] = {}  # job_id:dc -> lease
        self._fence_token = 0
        
        # State versioning
        self._state_version = 0
        
        # Configuration
        self._heartbeat_interval = heartbeat_interval
        self._lease_timeout = lease_timeout
        
        # Background tasks
        self._lease_cleanup_task: asyncio.Task | None = None
    
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
    
    def _get_available_datacenters(self) -> list[str]:
        """Get list of healthy datacenters."""
        now = time.monotonic()
        healthy = []
        
        for dc, last_seen in self._datacenter_last_seen.items():
            if now - last_seen < self._lease_timeout:
                hb = self._datacenter_health.get(dc)
                if hb and hb.worker_count > 0:
                    healthy.append(dc)
        
        return healthy
    
    def _select_datacenters(
        self,
        count: int,
        preferred: list[str] | None = None,
    ) -> list[str]:
        """
        Select datacenters for job execution.
        
        Uses cryptographically secure random selection.
        """
        available = self._get_available_datacenters()
        
        if preferred:
            # Use preferred if available
            selected = [dc for dc in preferred if dc in available]
            if len(selected) >= count:
                return selected[:count]
            # Fill with others
            remaining = [dc for dc in available if dc not in selected]
            selected.extend(secrets.SystemRandom().sample(
                remaining,
                min(count - len(selected), len(remaining))
            ))
            return selected
        
        # Random selection
        if len(available) <= count:
            return available
        return secrets.SystemRandom().sample(available, count)
    
    async def start(self) -> None:
        """Start the gate server."""
        await super().start()
        
        # Join gate cluster
        for peer in self._gate_peers:
            await self.join_cluster(peer)
        
        # Start leader election
        await self.start_leader_election()
        
        # Start lease cleanup task
        self._lease_cleanup_task = asyncio.create_task(self._lease_cleanup_loop())
        
        self._udp_logger.log(
            ServerInfo(
                message=f"Gate started with {len(self._datacenter_managers)} configured DCs",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def stop(self) -> None:
        """Stop the gate server."""
        if self._lease_cleanup_task:
            self._lease_cleanup_task.cancel()
            try:
                await self._lease_cleanup_task
            except asyncio.CancelledError:
                pass
        
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
                    ack = JobAck(**pickle.loads(response))
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
                        progress = JobProgress(**pickle.loads(response))
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
    # TCP Handlers - Manager Heartbeats
    # =========================================================================
    
    @tcp.send('manager_heartbeat_ack')
    async def send_manager_heartbeat_ack(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send manager heartbeat ack."""
        return (addr, data, timeout)
    
    @tcp.handle('manager_heartbeat_ack')
    async def handle_manager_heartbeat_ack_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw manager heartbeat ack."""
        return data
    
    @tcp.receive()
    async def receive_manager_heartbeat(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle manager heartbeat."""
        try:
            heartbeat = ManagerHeartbeat(**pickle.loads(data))
            
            # Update DC health tracking
            self._datacenter_health[heartbeat.datacenter] = heartbeat
            self._datacenter_last_seen[heartbeat.datacenter] = time.monotonic()
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "receive_manager_heartbeat")
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
            submission = JobSubmission(**pickle.loads(data))
            
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
            
            # Dispatch to each DC (in background)
            asyncio.create_task(
                self._dispatch_job_to_datacenters(submission, target_dcs)
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
        """Dispatch job to all target datacenters."""
        job = self._jobs.get(submission.job_id)
        if not job:
            return
        
        job.status = JobStatus.DISPATCHING.value
        self._increment_version()
        
        failed_dcs = []
        for dc in target_dcs:
            success = await self._dispatch_job_to_datacenter(
                submission.job_id,
                dc,
                submission,
            )
            if not success:
                failed_dcs.append(dc)
        
        if failed_dcs and len(failed_dcs) == len(target_dcs):
            # All DCs failed
            job.status = JobStatus.FAILED.value
        else:
            job.status = JobStatus.RUNNING.value
        
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
        """Handle job progress update from manager."""
        try:
            progress = JobProgress(**pickle.loads(data))
            
            job = self._jobs.get(progress.job_id)
            if job:
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
            cancel = CancelJob(**pickle.loads(data))
            
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
                            dc_ack = CancelAck(**pickle.loads(response))
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
            transfer = LeaseTransfer(**pickle.loads(data))
            
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

