"""
Hyperscale Client for Job Submission.

A client that can submit jobs to Gates or Managers and receive
pushed status updates.

Usage:
    client = HyperscaleClient(
        host='127.0.0.1',
        port=8000,
        managers=[('127.0.0.1', 9000), ('127.0.0.1', 9002)],
    )
    await client.start()
    
    # Submit a job
    job_id = await client.submit_job(
        workflows=[MyWorkflow],
        vus=10,
        timeout_seconds=60.0,
    )
    
    # Wait for completion
    result = await client.wait_for_job(job_id)
    
    await client.stop()
"""

import asyncio
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import cloudpickle

from hyperscale.distributed_rewrite.server import tcp
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer
from hyperscale.distributed_rewrite.models import (
    JobSubmission,
    JobAck,
    JobStatus,
    JobStatusPush,
    JobBatchPush,
)
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError


@dataclass
class JobResult:
    """Result of a completed job."""
    job_id: str
    status: str  # JobStatus value
    total_completed: int = 0
    total_failed: int = 0
    overall_rate: float = 0.0
    elapsed_seconds: float = 0.0
    error: str | None = None


class HyperscaleClient(MercurySyncBaseServer):
    """
    Client for submitting jobs and receiving status updates.
    
    The client can connect to either Gates (for multi-datacenter jobs)
    or directly to Managers (for single-datacenter jobs).
    
    Features:
    - Submit jobs with workflow classes
    - Receive push notifications for status updates
    - Wait for job completion
    - Track multiple concurrent jobs
    """
    
    def __init__(
        self,
        host: str = '127.0.0.1',
        port: int = 8500,
        env: Env | None = None,
        managers: list[tuple[str, int]] | None = None,
        gates: list[tuple[str, int]] | None = None,
    ):
        """
        Initialize the client.
        
        Args:
            host: Local host to bind for receiving push notifications
            port: Local TCP port for receiving push notifications
            env: Environment configuration
            managers: List of manager (host, port) addresses
            gates: List of gate (host, port) addresses
        """
        env = env or Env()
        
        super().__init__(
            host=host,
            tcp_port=port,
            udp_port=port + 1,  # UDP not used but required by base
            env=env,
        )
        
        self._managers = managers or []
        self._gates = gates or []
        
        # Job tracking
        self._jobs: dict[str, JobResult] = {}
        self._job_events: dict[str, asyncio.Event] = {}
        self._job_callbacks: dict[str, Callable[[JobStatusPush], None]] = {}
        
        # For selecting targets
        self._current_manager_idx = 0
        self._current_gate_idx = 0
    
    async def start(self) -> None:
        """Start the client and begin listening for push notifications."""
        init_context = {
            'nodes': {},  # Not used for client
        }
        await self.start_server(init_context=init_context)
    
    async def stop(self) -> None:
        """Stop the client."""
        # Cancel any pending job waits
        for event in self._job_events.values():
            event.set()
        await self.shutdown()
    
    def _get_callback_addr(self) -> tuple[str, int]:
        """Get this client's address for push notifications."""
        return (self._host, self._tcp_port)
    
    def _get_next_manager(self) -> tuple[str, int] | None:
        """Get next manager address (round-robin)."""
        if not self._managers:
            return None
        addr = self._managers[self._current_manager_idx]
        self._current_manager_idx = (self._current_manager_idx + 1) % len(self._managers)
        return addr
    
    def _get_next_gate(self) -> tuple[str, int] | None:
        """Get next gate address (round-robin)."""
        if not self._gates:
            return None
        addr = self._gates[self._current_gate_idx]
        self._current_gate_idx = (self._current_gate_idx + 1) % len(self._gates)
        return addr
    
    async def submit_job(
        self,
        workflows: list[type],
        vus: int = 1,
        timeout_seconds: float = 300.0,
        datacenter_count: int = 1,
        datacenters: list[str] | None = None,
        on_status_update: Callable[[JobStatusPush], None] | None = None,
    ) -> str:
        """
        Submit a job for execution.
        
        Args:
            workflows: List of Workflow classes to execute
            vus: Virtual users (cores) per workflow
            timeout_seconds: Maximum execution time
            datacenter_count: Number of datacenters to run in (gates only)
            datacenters: Specific datacenters to target (optional)
            on_status_update: Callback for status updates (optional)
            
        Returns:
            job_id: Unique identifier for the submitted job
            
        Raises:
            RuntimeError: If no managers/gates configured or submission fails
        """
        job_id = f"job-{secrets.token_hex(8)}"
        
        # Serialize workflows
        workflows_bytes = cloudpickle.dumps(workflows)
        
        submission = JobSubmission(
            job_id=job_id,
            workflows=workflows_bytes,
            vus=vus,
            timeout_seconds=timeout_seconds,
            datacenter_count=datacenter_count,
            datacenters=datacenters or [],
            callback_addr=self._get_callback_addr(),
        )
        
        # Try gates first, then managers
        target = self._get_next_gate() or self._get_next_manager()
        if not target:
            raise RuntimeError("No managers or gates configured")
        
        # Initialize job tracking
        self._jobs[job_id] = JobResult(
            job_id=job_id,
            status=JobStatus.SUBMITTED.value,
        )
        self._job_events[job_id] = asyncio.Event()
        if on_status_update:
            self._job_callbacks[job_id] = on_status_update
        
        # Submit to target
        response, _ = await self.send_tcp(
            target,
            "job_submission",
            submission.dump(),
            timeout=10.0,
        )
        
        if isinstance(response, Exception):
            self._jobs[job_id].status = JobStatus.FAILED.value
            self._jobs[job_id].error = str(response)
            self._job_events[job_id].set()
            raise RuntimeError(f"Job submission failed: {response}")
        
        ack = JobAck.load(response)
        if not ack.accepted:
            self._jobs[job_id].status = JobStatus.FAILED.value
            self._jobs[job_id].error = ack.error
            self._job_events[job_id].set()
            raise RuntimeError(f"Job rejected: {ack.error}")
        
        return job_id
    
    async def wait_for_job(
        self,
        job_id: str,
        timeout: float | None = None,
    ) -> JobResult:
        """
        Wait for a job to complete.
        
        Args:
            job_id: Job identifier from submit_job
            timeout: Maximum time to wait (None = wait forever)
            
        Returns:
            JobResult with final status
            
        Raises:
            KeyError: If job_id not found
            asyncio.TimeoutError: If timeout exceeded
        """
        if job_id not in self._jobs:
            raise KeyError(f"Unknown job: {job_id}")
        
        event = self._job_events[job_id]
        
        if timeout:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        else:
            await event.wait()
        
        return self._jobs[job_id]
    
    def get_job_status(self, job_id: str) -> JobResult | None:
        """Get current status of a job."""
        return self._jobs.get(job_id)
    
    # =========================================================================
    # TCP Handlers for Push Notifications
    # =========================================================================
    
    @tcp.receive()
    async def job_status_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle job status push notification from gate/manager."""
        try:
            push = JobStatusPush.load(data)
            
            job = self._jobs.get(push.job_id)
            if job:
                job.status = push.status
                job.total_completed = push.total_completed
                job.total_failed = push.total_failed
                job.overall_rate = push.overall_rate
                job.elapsed_seconds = push.elapsed_seconds
                
                # Call user callback if registered
                callback = self._job_callbacks.get(push.job_id)
                if callback:
                    try:
                        callback(push)
                    except Exception:
                        pass  # Don't let callback errors break us
                
                # If final, signal completion
                if push.is_final:
                    event = self._job_events.get(push.job_id)
                    if event:
                        event.set()
            
            return b'ok'
            
        except Exception as e:
            return b'error'
    
    @tcp.receive()
    async def job_batch_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """Handle batch stats push notification from gate/manager."""
        try:
            push = JobBatchPush.load(data)
            
            # Update all jobs in the batch
            for job_id, stats in push.job_stats.items():
                job = self._jobs.get(job_id)
                if job:
                    job.total_completed = stats.get('completed', 0)
                    job.total_failed = stats.get('failed', 0)
                    job.overall_rate = stats.get('rate', 0.0)
            
            return b'ok'
            
        except Exception as e:
            return b'error'

