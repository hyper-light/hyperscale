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
    JobFinalResult,
    GlobalJobResult,
    PingRequest,
    ManagerPingResponse,
    GatePingResponse,
    WorkflowQueryRequest,
    WorkflowStatusInfo,
    WorkflowQueryResponse,
    DatacenterWorkflowStatus,
    GateWorkflowQueryResponse,
)
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError


@dataclass
class JobResult:
    """
    Result of a completed job.
    
    For single-DC jobs, only basic fields are populated.
    For multi-DC jobs (via gates), per_datacenter_results and aggregated are populated.
    """
    job_id: str
    status: str  # JobStatus value
    total_completed: int = 0
    total_failed: int = 0
    overall_rate: float = 0.0
    elapsed_seconds: float = 0.0
    error: str | None = None
    # Multi-DC fields (populated when result comes from a gate)
    per_datacenter_results: list = field(default_factory=list)  # list[JobFinalResult]
    aggregated: Any = None  # AggregatedJobStats


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
        self._job_targets: dict[str, tuple[str, int]] = {}  # job_id -> manager/gate that accepted
        
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
            
        await super().shutdown()
    
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
    
    # Transient error messages that should trigger retry with backoff
    _TRANSIENT_ERRORS = frozenset([
        "syncing",
        "not ready",
        "initializing",
        "starting up",
        "election in progress",
        "no quorum",
    ])

    def _is_transient_error(self, error: str) -> bool:
        """Check if an error is transient and should be retried."""
        error_lower = error.lower()
        return any(te in error_lower for te in self._TRANSIENT_ERRORS)

    async def submit_job(
        self,
        workflows: list[type],
        vus: int = 1,
        timeout_seconds: float = 300.0,
        datacenter_count: int = 1,
        datacenters: list[str] | None = None,
        on_status_update: Callable[[JobStatusPush], None] | None = None,
        max_redirects: int = 3,
        max_retries: int = 5,
        retry_base_delay: float = 0.5,
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
            max_redirects: Maximum leader redirects to follow
            max_retries: Maximum retries for transient errors (syncing, etc.)
            retry_base_delay: Base delay for exponential backoff (seconds)

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

        # Initialize job tracking
        self._jobs[job_id] = JobResult(
            job_id=job_id,
            status=JobStatus.SUBMITTED.value,
        )
        self._job_events[job_id] = asyncio.Event()
        if on_status_update:
            self._job_callbacks[job_id] = on_status_update

        # Get all available targets for fallback
        all_targets = []
        if self._gates:
            all_targets.extend(self._gates)
        if self._managers:
            all_targets.extend(self._managers)

        if not all_targets:
            raise RuntimeError("No managers or gates configured")

        # Retry loop with exponential backoff for transient errors
        last_error = None
        for retry in range(max_retries + 1):
            # Try each target in order, cycling through on retries
            target_idx = retry % len(all_targets)
            target = all_targets[target_idx]

            # Submit with leader redirect handling
            redirects = 0
            while redirects <= max_redirects:
                response, _ = await self.send_tcp(
                    target,
                    "job_submission",
                    submission.dump(),
                    timeout=10.0,
                )

                if isinstance(response, Exception):
                    last_error = str(response)
                    break  # Try next retry/target

                ack = JobAck.load(response)

                if ack.accepted:
                    # Track which manager accepted this job for future queries
                    self._job_targets[job_id] = target
                    return job_id

                # Check for leader redirect
                if ack.leader_addr and redirects < max_redirects:
                    target = tuple(ack.leader_addr)
                    redirects += 1
                    continue

                # Check if this is a transient error that should be retried
                if ack.error and self._is_transient_error(ack.error):
                    last_error = ack.error
                    break  # Exit redirect loop, continue to retry

                # Permanent rejection - fail immediately
                self._jobs[job_id].status = JobStatus.FAILED.value
                self._jobs[job_id].error = ack.error
                self._job_events[job_id].set()
                raise RuntimeError(f"Job rejected: {ack.error}")

            # If we have retries remaining and the error was transient, wait and retry
            if retry < max_retries and last_error:
                # Exponential backoff: 0.5s, 1s, 2s, 4s, 8s
                delay = retry_base_delay * (2 ** retry)
                await asyncio.sleep(delay)

        # All retries exhausted
        self._jobs[job_id].status = JobStatus.FAILED.value
        self._jobs[job_id].error = last_error
        self._job_events[job_id].set()
        raise RuntimeError(f"Job submission failed after {max_retries} retries: {last_error}")
    
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
    # Ping Methods
    # =========================================================================

    async def ping_manager(
        self,
        addr: tuple[str, int] | None = None,
        timeout: float = 5.0,
    ) -> ManagerPingResponse:
        """
        Ping a manager to get its current status.

        Args:
            addr: Manager (host, port) to ping. If None, uses next manager in rotation.
            timeout: Request timeout in seconds.

        Returns:
            ManagerPingResponse with manager status, worker health, and active jobs.

        Raises:
            RuntimeError: If no managers configured or ping fails.
        """
        target = addr or self._get_next_manager()
        if not target:
            raise RuntimeError("No managers configured")

        request = PingRequest(request_id=secrets.token_hex(8))

        response, _ = await self.send_tcp(
            target,
            "ping",
            request.dump(),
            timeout=timeout,
        )

        if isinstance(response, Exception):
            raise RuntimeError(f"Ping failed: {response}")

        if response == b'error':
            raise RuntimeError("Ping failed: server returned error")

        return ManagerPingResponse.load(response)

    async def ping_gate(
        self,
        addr: tuple[str, int] | None = None,
        timeout: float = 5.0,
    ) -> GatePingResponse:
        """
        Ping a gate to get its current status.

        Args:
            addr: Gate (host, port) to ping. If None, uses next gate in rotation.
            timeout: Request timeout in seconds.

        Returns:
            GatePingResponse with gate status, datacenter health, and active jobs.

        Raises:
            RuntimeError: If no gates configured or ping fails.
        """
        target = addr or self._get_next_gate()
        if not target:
            raise RuntimeError("No gates configured")

        request = PingRequest(request_id=secrets.token_hex(8))

        response, _ = await self.send_tcp(
            target,
            "ping",
            request.dump(),
            timeout=timeout,
        )

        if isinstance(response, Exception):
            raise RuntimeError(f"Ping failed: {response}")

        if response == b'error':
            raise RuntimeError("Ping failed: server returned error")

        return GatePingResponse.load(response)

    async def ping_all_managers(
        self,
        timeout: float = 5.0,
    ) -> dict[tuple[str, int], ManagerPingResponse | Exception]:
        """
        Ping all configured managers concurrently.

        Args:
            timeout: Request timeout in seconds per manager.

        Returns:
            Dict mapping manager address to response or exception.
        """
        if not self._managers:
            return {}

        async def ping_one(addr: tuple[str, int]) -> tuple[tuple[str, int], ManagerPingResponse | Exception]:
            try:
                response = await self.ping_manager(addr, timeout=timeout)
                return (addr, response)
            except Exception as e:
                return (addr, e)

        results = await asyncio.gather(
            *[ping_one(addr) for addr in self._managers],
            return_exceptions=False,
        )

        return dict(results)

    async def ping_all_gates(
        self,
        timeout: float = 5.0,
    ) -> dict[tuple[str, int], GatePingResponse | Exception]:
        """
        Ping all configured gates concurrently.

        Args:
            timeout: Request timeout in seconds per gate.

        Returns:
            Dict mapping gate address to response or exception.
        """
        if not self._gates:
            return {}

        async def ping_one(addr: tuple[str, int]) -> tuple[tuple[str, int], GatePingResponse | Exception]:
            try:
                response = await self.ping_gate(addr, timeout=timeout)
                return (addr, response)
            except Exception as e:
                return (addr, e)

        results = await asyncio.gather(
            *[ping_one(addr) for addr in self._gates],
            return_exceptions=False,
        )

        return dict(results)

    # =========================================================================
    # Workflow Query Methods
    # =========================================================================

    async def query_workflows(
        self,
        workflow_names: list[str],
        job_id: str | None = None,
        timeout: float = 5.0,
    ) -> dict[str, list[WorkflowStatusInfo]]:
        """
        Query workflow status from managers.

        If job_id is specified and we know which manager accepted that job,
        queries that manager first. Otherwise queries all configured managers.

        Args:
            workflow_names: List of workflow class names to query.
            job_id: Optional job ID to filter results.
            timeout: Request timeout in seconds.

        Returns:
            Dict mapping datacenter ID to list of WorkflowStatusInfo.
            If querying managers directly, uses the manager's datacenter.

        Raises:
            RuntimeError: If no managers configured.
        """
        if not self._managers:
            raise RuntimeError("No managers configured")

        request = WorkflowQueryRequest(
            request_id=secrets.token_hex(8),
            workflow_names=workflow_names,
            job_id=job_id,
        )

        results: dict[str, list[WorkflowStatusInfo]] = {}

        async def query_one(addr: tuple[str, int]) -> None:
            try:
                response_data, _ = await self.send_tcp(
                    addr,
                    "workflow_query",
                    request.dump(),
                    timeout=timeout,
                )

                if isinstance(response_data, Exception) or response_data == b'error':
                    return

                response = WorkflowQueryResponse.load(response_data)
                dc_id = response.datacenter

                if dc_id not in results:
                    results[dc_id] = []
                results[dc_id].extend(response.workflows)

            except Exception:
                pass  # Manager query failed - skip

        # If we know which manager accepted this job, query it first
        # This ensures we get results from the job leader
        if job_id and job_id in self._job_targets:
            target = self._job_targets[job_id]
            await query_one(target)
            # If we got results, return them (job leader has authoritative state)
            if results:
                return results

        # Query all managers (either no job_id, or job target query failed)
        await asyncio.gather(
            *[query_one(addr) for addr in self._managers],
            return_exceptions=False,
        )

        return results

    async def query_workflows_via_gate(
        self,
        workflow_names: list[str],
        job_id: str | None = None,
        addr: tuple[str, int] | None = None,
        timeout: float = 10.0,
    ) -> dict[str, list[WorkflowStatusInfo]]:
        """
        Query workflow status via a gate.

        Gates query all datacenter managers and return aggregated results
        grouped by datacenter.

        Args:
            workflow_names: List of workflow class names to query.
            job_id: Optional job ID to filter results.
            addr: Gate (host, port) to query. If None, uses next gate in rotation.
            timeout: Request timeout in seconds (higher for gate aggregation).

        Returns:
            Dict mapping datacenter ID to list of WorkflowStatusInfo.

        Raises:
            RuntimeError: If no gates configured or query fails.
        """
        target = addr or self._get_next_gate()
        if not target:
            raise RuntimeError("No gates configured")

        request = WorkflowQueryRequest(
            request_id=secrets.token_hex(8),
            workflow_names=workflow_names,
            job_id=job_id,
        )

        response_data, _ = await self.send_tcp(
            target,
            "workflow_query",
            request.dump(),
            timeout=timeout,
        )

        if isinstance(response_data, Exception):
            raise RuntimeError(f"Workflow query failed: {response_data}")

        if response_data == b'error':
            raise RuntimeError("Workflow query failed: gate returned error")

        response = GateWorkflowQueryResponse.load(response_data)

        # Convert to dict format
        results: dict[str, list[WorkflowStatusInfo]] = {}
        for dc_status in response.datacenters:
            results[dc_status.dc_id] = dc_status.workflows

        return results

    async def query_all_gates_workflows(
        self,
        workflow_names: list[str],
        job_id: str | None = None,
        timeout: float = 10.0,
    ) -> dict[tuple[str, int], dict[str, list[WorkflowStatusInfo]] | Exception]:
        """
        Query workflow status from all configured gates concurrently.

        Each gate returns results aggregated by datacenter.

        Args:
            workflow_names: List of workflow class names to query.
            job_id: Optional job ID to filter results.
            timeout: Request timeout in seconds per gate.

        Returns:
            Dict mapping gate address to either:
            - Dict of datacenter -> workflow status list
            - Exception if query failed
        """
        if not self._gates:
            return {}

        async def query_one(
            addr: tuple[str, int],
        ) -> tuple[tuple[str, int], dict[str, list[WorkflowStatusInfo]] | Exception]:
            try:
                result = await self.query_workflows_via_gate(
                    workflow_names,
                    job_id=job_id,
                    addr=addr,
                    timeout=timeout,
                )
                return (addr, result)
            except Exception as e:
                return (addr, e)

        results = await asyncio.gather(
            *[query_one(addr) for addr in self._gates],
            return_exceptions=False,
        )

        return dict(results)

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
    
    @tcp.receive()
    async def job_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle final job result from manager (when no gates).
        
        This is a per-datacenter result with all workflow results.
        """
        try:
            result = JobFinalResult.load(data)
            
            job = self._jobs.get(result.job_id)
            if job:
                job.status = result.status
                job.total_completed = result.total_completed
                job.total_failed = result.total_failed
                job.elapsed_seconds = result.elapsed_seconds
                if result.errors:
                    job.error = "; ".join(result.errors)
                
                # Signal completion
                event = self._job_events.get(result.job_id)
                if event:
                    event.set()
            
            return b'ok'
            
        except Exception as e:
            return b'error'
    
    @tcp.receive()
    async def global_job_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle global job result from gate.
        
        This is the aggregated result across all datacenters.
        """
        try:
            result = GlobalJobResult.load(data)
            
            job = self._jobs.get(result.job_id)
            if job:
                job.status = result.status
                job.total_completed = result.total_completed
                job.total_failed = result.total_failed
                job.elapsed_seconds = result.elapsed_seconds
                if result.errors:
                    job.error = "; ".join(result.errors)
                
                # Multi-DC fields
                job.per_datacenter_results = result.per_datacenter_results
                job.aggregated = result.aggregated
                
                # Signal completion
                event = self._job_events.get(result.job_id)
                if event:
                    event.set()
            
            return b'ok'
            
        except Exception as e:
            return b'error'

