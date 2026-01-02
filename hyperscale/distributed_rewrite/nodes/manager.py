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
- UDP: SWIM healthchecks (inherited from UDPServer)
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

from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.distributed_rewrite.server.events import VersionedStateClock
from hyperscale.distributed_rewrite.swim import UDPServer, ManagerStateEmbedder
from hyperscale.distributed_rewrite.models import (
    NodeInfo,
    NodeRole,
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


class ManagerServer(UDPServer):
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
        
        # Registered workers (indexed by node_id)
        self._workers: dict[str, WorkerRegistration] = {}  # node_id -> registration
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
        
        # Fencing tokens for at-most-once
        self._fence_token = 0
        
        # State versioning (local manager state version)
        self._state_version = 0
        
        # Quorum settings
        self._quorum_timeout = quorum_timeout
        
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
    
    def _on_manager_become_leader(self) -> None:
        """
        Called when this manager becomes the leader.
        
        Triggers state sync from all known workers to ensure the new leader
        has the most current state.
        """
        # Schedule async state sync via task runner
        self._task_runner.run(self._sync_state_from_workers)
    
    def _on_manager_lose_leadership(self) -> None:
        """Called when this manager loses leadership."""
        # Currently no special cleanup needed
        pass
    
    async def _sync_state_from_workers(self) -> None:
        """
        Request current state from all registered workers.
        
        Called when this manager becomes leader to ensure we have
        the freshest state from all workers.
        """
        if not self._workers:
            return
        
        self._udp_logger.log(
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
            
            self._udp_logger.log(
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
    ) -> WorkerStateSnapshot | None:
        """Request state from a single worker."""
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
                    # Update our tracking with worker's state
                    worker_state = sync_response.worker_state
                    
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
                            cpu_percent=0.0,  # Not in snapshot
                            memory_percent=0.0,  # Not in snapshot
                            version=worker_state.version,
                            active_workflows={
                                wf_id: wf.status
                                for wf_id, wf in worker_state.active_workflows.items()
                            },
                        )
                        self._worker_status[worker_state.node_id] = heartbeat
                        self._worker_last_status[worker_state.node_id] = time.monotonic()
                        await self._versioned_clock.update_entity(
                            worker_state.node_id,
                            worker_state.version,
                        )
                    
                    return worker_state
            
            return None
            
        except Exception as e:
            await self.handle_exception(e, f"request_worker_state:{worker_addr}")
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
        asyncio.create_task(
            self._versioned_clock.update_entity(heartbeat.node_id, heartbeat.version)
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
        """Calculate quorum size (majority of managers)."""
        total_managers = len(self._manager_peers) + 1  # Include self
        return (total_managers // 2) + 1
    
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
        
        # Note: Status updates to gates are now handled via Serf-style heartbeat
        # embedding in SWIM probe responses. Gates learn our state passively.
        
        self._udp_logger.log(
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
    
    # Note: Status updates to gates are now handled via Serf-style heartbeat
    # embedding in SWIM probe responses. See ManagerStateEmbedder in __init__.
    
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
            self._worker_last_status[registration.node.node_id] = time.monotonic()
            self._increment_version()
            
            # Add worker to SWIM cluster for UDP healthchecks
            # The worker's UDP address is derived from registration
            worker_udp_addr = (registration.node.host, registration.node.port)
            self._probe_scheduler.add_member(worker_udp_addr)
            
            self._udp_logger.log(
                ServerInfo(
                    message=f"Worker registered: {registration.node.node_id} with {registration.total_cores} cores (SWIM probe added)",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "receive_worker_register")
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
        """Handle workflow progress update from worker."""
        try:
            progress = WorkflowProgress.load(data)
            
            # Update job progress
            job = self._jobs.get(progress.job_id)
            if job:
                # Find and update workflow in job
                for i, wf in enumerate(job.workflows):
                    if wf.workflow_id == progress.workflow_id:
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
                
                self._increment_version()
            
            return b'ok'
            
        except Exception as e:
            await self.handle_exception(e, "receive_workflow_progress")
            return b'error'
    
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
            
            # Dispatch workflows to workers
            asyncio.create_task(
                self._dispatch_job_workflows(submission, workflows)
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
        """Dispatch all workflows in a job to workers."""
        import cloudpickle
        
        job = self._jobs.get(submission.job_id)
        if not job:
            return
        
        job.status = JobStatus.DISPATCHING.value
        self._increment_version()
        
        for i, workflow in enumerate(workflows):
            workflow_id = f"{submission.job_id}:{i}"
            
            # Select worker
            worker_id = self._select_worker_for_workflow(submission.vus)
            if not worker_id:
                job.status = JobStatus.FAILED.value
                self._increment_version()
                return
            
            # Create provision request for quorum
            provision = ProvisionRequest(
                job_id=submission.job_id,
                workflow_id=workflow_id,
                target_worker=worker_id,
                cores_required=submission.vus,
                fence_token=self._get_fence_token(),
                version=self._state_version,
            )
            
            # Request quorum (skip if only one manager)
            if self._manager_peers:
                confirmed = await self._request_quorum_confirmation(provision)
                if not confirmed:
                    job.status = JobStatus.FAILED.value
                    self._increment_version()
                    return
                
                # Send commit to all managers
                await self._send_provision_commit(provision)
            
            # Dispatch to worker
            dispatch = WorkflowDispatch(
                job_id=submission.job_id,
                workflow_id=workflow_id,
                workflow=cloudpickle.dumps(workflow),
                context=b'{}',  # Context would come from job
                vus=submission.vus,
                timeout_seconds=submission.timeout_seconds,
                fence_token=provision.fence_token,
            )
            
            ack = await self._dispatch_workflow_to_worker(worker_id, dispatch)
            if not ack or not ack.accepted:
                job.status = JobStatus.FAILED.value
                self._increment_version()
                return
            
            self._workflow_assignments[workflow_id] = worker_id
        
        job.status = JobStatus.RUNNING.value
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

