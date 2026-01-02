"""
Worker Node Server.

Workers are the distributed thread/process pool. They:
- Execute workflows assigned by managers
- Report status via TCP to managers
- Participate in UDP healthchecks (SWIM protocol)

Workers are the absolute source of truth for their own state.

Protocols:
- UDP: SWIM healthchecks (inherited from UDPServer)
  - probe/ack for liveness detection
  - indirect probing for network partition handling
  - gossip for membership dissemination
- TCP: Data operations
  - Status updates to managers
  - Workflow dispatch from managers
  - State sync requests
"""

import asyncio
import cloudpickle
import os
import time
from typing import Any

import cloudpickle

from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.distributed_rewrite.swim import UDPServer, WorkerStateEmbedder
from hyperscale.distributed_rewrite.models import (
    NodeInfo,
    NodeRole,
    WorkerRegistration,
    WorkerHeartbeat,
    WorkerState,
    WorkerStateSnapshot,
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkflowProgress,
    WorkflowStatus,
    StepStats,
    StateSyncRequest,
    StateSyncResponse,
    CancelJob,
    CancelAck,
    restricted_loads,
)
from hyperscale.distributed_rewrite.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError


class WorkerServer(UDPServer):
    """
    Worker node in the distributed Hyperscale system.
    
    Workers:
    - Receive workflow dispatches from managers via TCP
    - Execute workflows using available CPU cores
    - Report progress back to managers via TCP
    - Participate in SWIM healthchecks via UDP (inherited from UDPServer)
    
    Workers have no knowledge of other workers - they only communicate
    with their local manager cluster.
    
    Healthchecks (UDP - SWIM protocol):
        Workers join the manager cluster's SWIM protocol. Managers probe
        workers via UDP to detect failures. Workers respond to probes
        automatically via the inherited UDPServer.receive() handler.
    
    Status Updates (TCP):
        Workers send status updates to managers via TCP. These contain
        capacity, queue depth, and workflow progress - NOT healthchecks.
    """
    
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "default",
        total_cores: int | None = None,
        manager_addrs: list[tuple[str, int]] | None = None,
    ):
        super().__init__(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=dc_id,
        )
        
        # Core capacity
        self._total_cores = total_cores or os.cpu_count() or 1
        self._available_cores = self._total_cores
        
        # Manager discovery (UDP addresses for SWIM, TCP for data)
        self._manager_addrs = manager_addrs or []  # TCP addresses
        self._manager_udp_addrs: list[tuple[str, int]] = []  # UDP addresses for SWIM
        self._current_manager: tuple[str, int] | None = None
        
        # Workflow execution state
        self._active_workflows: dict[str, WorkflowProgress] = {}
        self._workflow_tasks: dict[str, asyncio.Task] = {}
        self._workflow_cancel_events: dict[str, asyncio.Event] = {}
        
        # State versioning (Lamport clock extension)
        self._state_version = 0
        
        # Queue depth tracking
        self._pending_workflows: list[WorkflowDispatch] = []
        
        # Inject state embedder for Serf-style heartbeat embedding in SWIM messages
        self.set_state_embedder(WorkerStateEmbedder(
            get_node_id=lambda: self._node_id.full,
            get_worker_state=lambda: self._get_worker_state().value,
            get_available_cores=lambda: self._available_cores,
            get_queue_depth=lambda: len(self._pending_workflows),
            get_cpu_percent=self._get_cpu_percent,
            get_memory_percent=self._get_memory_percent,
            get_state_version=lambda: self._state_version,
            get_active_workflows=lambda: {
                wf_id: wf.status for wf_id, wf in self._active_workflows.items()
            },
        ))
    
    @property
    def node_info(self) -> NodeInfo:
        """Get this worker's node info."""
        return NodeInfo(
            node_id=self._node_id.full,
            role=NodeRole.WORKER.value,
            host=self._host,
            port=self._tcp_port,
            datacenter=self._node_id.datacenter,
            version=self._state_version,
        )
    
    def _increment_version(self) -> int:
        """Increment and return the state version."""
        self._state_version += 1
        return self._state_version
    
    async def start(self) -> None:
        """Start the worker server."""
        await super().start()
        
        # Register with managers (TCP)
        for manager_addr in self._manager_addrs:
            success = await self._register_with_manager(manager_addr)
            if success:
                self._current_manager = manager_addr
                break
        
        # Join SWIM cluster via manager UDP addresses for healthchecks
        # The manager will probe us via UDP to detect failures
        for manager_udp_addr in self._manager_udp_addrs:
            await self.join_cluster(manager_udp_addr)
        
        # Start SWIM probe cycle (UDP healthchecks)
        # This makes us participate in the SWIM protocol
        # Note: Worker state is now embedded in SWIM probe responses (Serf-style)
        # so managers learn our capacity/status passively via the SWIM protocol
        self._task_runner.run(self.start_probe_cycle)
        
        self._udp_logger.log(
            ServerInfo(
                message=f"Worker started with {self._total_cores} cores, SWIM healthcheck active",
                node_host=self._host,
                node_port=self._tcp_port,
                node_id=self._node_id.short,
            )
        )
    
    async def stop(self) -> None:
        """Stop the worker server."""
        # Cancel all active workflows
        for workflow_id in list(self._workflow_tasks.keys()):
            await self._cancel_workflow(workflow_id, "server_shutdown")
        
        # Graceful shutdown broadcasts leave via UDP (SWIM)
        await self.graceful_shutdown()
        
        await super().stop()
    
    async def _register_with_manager(self, manager_addr: tuple[str, int]) -> bool:
        """Register this worker with a manager."""
        try:
            registration = WorkerRegistration(
                node=self.node_info,
                total_cores=self._total_cores,
                available_cores=self._available_cores,
                memory_mb=self._get_memory_mb(),
                available_memory_mb=self._get_available_memory_mb(),
            )
            
            result = await self.send_tcp(
                manager_addr,
                "worker_register",
                registration.dump(),
                timeout=5.0,
            )
            
            return not isinstance(result, Exception)
        except Exception as e:
            self._udp_logger.log(
                ServerError(
                    message=f"Failed to register with manager {manager_addr}: {e}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return False
    
    # Note: Status updates are now handled via Serf-style heartbeat embedding
    # in SWIM probe responses. The WorkerStateEmbedder provides worker capacity
    # and status to managers through the SWIM protocol. See set_state_embedder()
    # in __init__.
    
    def _get_worker_state(self) -> WorkerState:
        """Determine current worker state."""
        if not self._running:
            return WorkerState.OFFLINE
        
        # Check degradation level
        if self._degradation.current_level.value >= 3:  # SEVERE or CRITICAL
            return WorkerState.DRAINING
        elif self._degradation.current_level.value >= 2:  # MODERATE or above
            return WorkerState.DEGRADED
        
        return WorkerState.HEALTHY
    
    def _get_memory_mb(self) -> int:
        """Get total memory in MB."""
        try:
            import psutil
            return psutil.virtual_memory().total // (1024 * 1024)
        except ImportError:
            return 0
    
    def _get_available_memory_mb(self) -> int:
        """Get available memory in MB."""
        try:
            import psutil
            return psutil.virtual_memory().available // (1024 * 1024)
        except ImportError:
            return 0
    
    def _get_cpu_percent(self) -> float:
        """Get CPU utilization percentage."""
        try:
            import psutil
            return psutil.cpu_percent()
        except ImportError:
            return 0.0
    
    def _get_memory_percent(self) -> float:
        """Get memory utilization percentage."""
        try:
            import psutil
            return psutil.virtual_memory().percent
        except ImportError:
            return 0.0
    
    def _get_state_snapshot(self) -> WorkerStateSnapshot:
        """Get a complete state snapshot."""
        return WorkerStateSnapshot(
            node_id=self._node_id.full,
            state=self._get_worker_state().value,
            total_cores=self._total_cores,
            available_cores=self._available_cores,
            version=self._state_version,
            active_workflows=dict(self._active_workflows),
        )
    
    async def _cancel_workflow(self, workflow_id: str, reason: str) -> bool:
        """Cancel a running workflow."""
        if workflow_id not in self._workflow_tasks:
            return False
        
        # Signal cancellation
        cancel_event = self._workflow_cancel_events.get(workflow_id)
        if cancel_event:
            cancel_event.set()
        
        # Cancel the task
        task = self._workflow_tasks.get(workflow_id)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Update state
        if workflow_id in self._active_workflows:
            self._active_workflows[workflow_id].status = WorkflowStatus.CANCELLED.value
        
        # Free cores
        # (Would need to track cores per workflow)
        
        self._increment_version()
        return True
    
    # =========================================================================
    # TCP Handlers - Manager -> Worker
    # =========================================================================
    
    @tcp.send('workflow_dispatch_response')
    async def send_dispatch_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send workflow dispatch response."""
        return (addr, data, timeout)
    
    @tcp.handle('workflow_dispatch_response')
    async def handle_dispatch_response_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw dispatch response data."""
        return data
    
    @tcp.receive()
    async def receive_workflow_dispatch(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Receive a workflow dispatch from a manager.
        
        This is the main entry point for work arriving at the worker.
        """
        try:
            dispatch = WorkflowDispatch.load(data)
            
            # Check if we can accept this workflow
            if self._available_cores < dispatch.vus:
                # Reject - not enough cores
                ack = WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error=f"Insufficient cores: need {dispatch.vus}, have {self._available_cores}",
                )
                return ack.dump()
            
            # Check backpressure
            if self._get_worker_state() == WorkerState.DRAINING:
                ack = WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error="Worker is draining, not accepting new work",
                )
                return ack.dump()
            
            # Accept the workflow
            self._available_cores -= dispatch.vus
            self._increment_version()
            
            # Create progress tracker
            progress = WorkflowProgress(
                job_id=dispatch.job_id,
                workflow_id=dispatch.workflow_id,
                workflow_name="",  # Will be set after unpickling
                status=WorkflowStatus.ASSIGNED.value,
                completed_count=0,
                failed_count=0,
                rate_per_second=0.0,
                elapsed_seconds=0.0,
                timestamp=time.monotonic(),
            )
            self._active_workflows[dispatch.workflow_id] = progress
            
            # Create cancellation event
            cancel_event = asyncio.Event()
            self._workflow_cancel_events[dispatch.workflow_id] = cancel_event
            
            # Start execution task
            task = asyncio.create_task(
                self._execute_workflow(dispatch, progress, cancel_event)
            )
            self._workflow_tasks[dispatch.workflow_id] = task
            
            # Return acknowledgment
            ack = WorkflowDispatchAck(
                workflow_id=dispatch.workflow_id,
                accepted=True,
                cores_assigned=dispatch.vus,
            )
            return ack.dump()
            
        except Exception as e:
            await self.handle_exception(e, "receive_workflow_dispatch")
            ack = WorkflowDispatchAck(
                workflow_id="unknown",
                accepted=False,
                error=str(e),
            )
            return ack.dump()
    
    async def _execute_workflow(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        cancel_event: asyncio.Event,
    ) -> None:
        """Execute a workflow."""
        start_time = time.monotonic()
        
        try:
            # Unpickle workflow and context
            workflow = restricted_loads(dispatch.workflow)
            context = restricted_loads(dispatch.context)
            
            progress.workflow_name = workflow.__class__.__name__
            progress.status = WorkflowStatus.RUNNING.value
            self._increment_version()
            
            # TODO: Actually execute the workflow
            # This would integrate with the existing WorkflowRunner
            # For now, simulate execution
            
            while not cancel_event.is_set():
                await asyncio.sleep(0.1)
                
                # Update progress
                progress.elapsed_seconds = time.monotonic() - start_time
                progress.completed_count += 1
                progress.rate_per_second = (
                    progress.completed_count / progress.elapsed_seconds
                    if progress.elapsed_seconds > 0 else 0
                )
                progress.timestamp = time.monotonic()
                
                # Check timeout
                if progress.elapsed_seconds > dispatch.timeout_seconds:
                    progress.status = WorkflowStatus.FAILED.value
                    break
                
                # Send progress update to manager
                if self._current_manager and int(progress.elapsed_seconds) % 1 == 0:
                    await self._send_progress_update(progress)
            
            if cancel_event.is_set():
                progress.status = WorkflowStatus.CANCELLED.value
            else:
                progress.status = WorkflowStatus.COMPLETED.value
                
        except Exception as e:
            progress.status = WorkflowStatus.FAILED.value
            await self.handle_exception(e, f"execute_workflow_{dispatch.workflow_id}")
        finally:
            # Free cores
            self._available_cores += dispatch.vus
            self._increment_version()
            
            # Cleanup
            self._workflow_tasks.pop(dispatch.workflow_id, None)
            self._workflow_cancel_events.pop(dispatch.workflow_id, None)
    
    async def _send_progress_update(self, progress: WorkflowProgress) -> None:
        """Send a progress update to the manager."""
        if not self._current_manager:
            return
        
        try:
            await self.send_tcp(
                self._current_manager,
                "workflow_progress",
                progress.dump(),
                timeout=1.0,
            )
        except Exception as e:
            # Progress update failure is not critical
            pass
    
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
        """Handle state sync request from a new manager leader."""
        try:
            request = StateSyncRequest.load(data)
            
            # Return our current state snapshot
            response = StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=self._state_version,
                worker_state=self._get_state_snapshot(),
            )
            return response.dump()
            
        except Exception as e:
            await self.handle_exception(e, "receive_state_sync_request")
            return b''
    
    # =========================================================================
    # TCP Handlers - Cancellation
    # =========================================================================
    
    @tcp.send('cancel_ack')
    async def send_cancel_ack(
        self,
        addr: tuple[str, int],
        data: bytes,
        timeout: int | float | None = None,
    ):
        """Send cancellation acknowledgment."""
        return (addr, data, timeout)
    
    @tcp.handle('cancel_ack')
    async def handle_cancel_ack_raw(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle raw cancel ack."""
        return data
    
    @tcp.receive()
    async def receive_cancel_job(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job cancellation request from manager."""
        try:
            cancel_request = CancelJob.load(data)
            
            # Find and cancel all workflows for this job
            cancelled_count = 0
            for workflow_id, progress in list(self._active_workflows.items()):
                if progress.job_id == cancel_request.job_id:
                    if await self._cancel_workflow(workflow_id, cancel_request.reason):
                        cancelled_count += 1
            
            ack = CancelAck(
                job_id=cancel_request.job_id,
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

