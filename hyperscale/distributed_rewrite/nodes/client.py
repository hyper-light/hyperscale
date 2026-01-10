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
from typing import Callable

import cloudpickle

from hyperscale.distributed_rewrite.server import tcp
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer
from hyperscale.core.jobs.protocols.constants import MAX_DECOMPRESSED_SIZE
from hyperscale.distributed_rewrite.errors import MessageTooLargeError
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
    DatacenterListRequest,
    DatacenterListResponse,
    WorkflowQueryRequest,
    WorkflowStatusInfo,
    WorkflowQueryResponse,
    GateWorkflowQueryResponse,
    RegisterCallback,
    RegisterCallbackResponse,
    ReporterResultPush,
    WorkflowResultPush,
    # Cancellation (AD-20)
    JobCancelRequest,
    JobCancelResponse,
    JobCancellationComplete,
    # Section 9: Client leadership tracking
    GateLeaderInfo,
    ManagerLeaderInfo,
    OrphanedJobInfo,
    LeadershipRetryPolicy,
    GateJobLeaderTransfer,
    GateJobLeaderTransferAck,
    ManagerJobLeaderTransfer,
    ManagerJobLeaderTransferAck,
    # Client result models
    ClientReporterResult,
    ClientWorkflowDCResult,
    ClientWorkflowResult,
    ClientJobResult,
)
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.distributed_rewrite.reliability.rate_limiting import (
    AdaptiveRateLimiter,
    AdaptiveRateLimitConfig,
    RequestPriority,
)
from hyperscale.distributed_rewrite.reliability.overload import HybridOverloadDetector
from hyperscale.distributed_rewrite.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    ProtocolVersion,
    NegotiatedCapabilities,
    get_features_for_version,
)
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError
from hyperscale.reporting.reporter import Reporter
from hyperscale.reporting.json import JSONConfig
from hyperscale.reporting.common import ReporterTypes


# Type aliases for backwards compatibility and shorter names in this module
ReporterResult = ClientReporterResult
WorkflowDCResultClient = ClientWorkflowDCResult
WorkflowResult = ClientWorkflowResult
JobResult = ClientJobResult


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

        # Cancellation completion tracking (AD-20 push notifications)
        # job_id -> asyncio.Event (set when cancellation complete notification received)
        self._cancellation_events: dict[str, asyncio.Event] = {}
        # job_id -> list of errors from cancelled workflows
        self._cancellation_errors: dict[str, list[str]] = {}
        # job_id -> bool indicating if cancellation was successful
        self._cancellation_success: dict[str, bool] = {}

        # Reporter result callbacks (called when reporter submission completes)
        self._reporter_callbacks: dict[str, Callable[[ReporterResultPush], None]] = {}

        # Workflow result callbacks (called when each workflow completes)
        self._workflow_callbacks: dict[str, Callable[[WorkflowResultPush], None]] = {}

        # Reporter configs per job for local file-based reporting
        # job_id -> list of ReporterConfig objects
        self._job_reporting_configs: dict[str, list] = {}

        # File-based reporter types that should be handled locally
        self._local_reporter_types = {
            ReporterTypes.JSON,
            ReporterTypes.CSV,
            ReporterTypes.XML,
        }

        # Progress update callbacks (for streaming windowed stats)
        from hyperscale.distributed_rewrite.jobs import WindowedStatsPush
        self._progress_callbacks: dict[str, Callable[[WindowedStatsPush], None]] = {}

        # Rate limiter for progress updates using the same AdaptiveRateLimiter
        # as manager, gate, and worker. This provides health-gated rate limiting
        # with per-operation limits.
        self._rate_limiter = AdaptiveRateLimiter(
            overload_detector=HybridOverloadDetector(),
            config=AdaptiveRateLimitConfig(
                # Progress updates use the default operation limits from
                # AdaptiveRateLimitConfig: (300, 10.0) = 30/s
                # This is more generous than the old token bucket
            ),
        )

        # Protocol version negotiation (AD-25)
        # Tracks negotiated capabilities per server (manager/gate)
        self._server_negotiated_caps: dict[tuple[str, int], NegotiatedCapabilities] = {}
        # Build our capabilities string once
        self._capabilities_str = ','.join(sorted(get_features_for_version(CURRENT_PROTOCOL_VERSION)))

        # For selecting targets
        self._current_manager_idx = 0
        self._current_gate_idx = 0

        # =======================================================================
        # Section 9: Client robust response to leadership takeovers
        # =======================================================================

        # 9.1.1: Gate leadership tracking per job
        self._gate_job_leaders: dict[str, GateLeaderInfo] = {}  # job_id -> gate info

        # 9.2.1: Manager leadership tracking per job (with datacenter)
        # Key is (job_id, datacenter_id) for multi-DC support
        self._manager_job_leaders: dict[tuple[str, str], ManagerLeaderInfo] = {}

        # 9.3.2: Per-job locks for request routing
        self._request_routing_locks: dict[str, asyncio.Lock] = {}  # job_id -> lock

        # 9.3.3: Leadership retry policy (configurable)
        self._leadership_retry_policy = LeadershipRetryPolicy(
            max_retries=3,
            retry_delay=0.5,
            exponential_backoff=True,
            max_delay=5.0,
        )

        # 9.5.1: Orphaned job tracking
        self._orphaned_jobs: dict[str, OrphanedJobInfo] = {}  # job_id -> orphan info
        self._orphan_grace_period: float = env.CLIENT_ORPHAN_GRACE_PERIOD
        self._orphan_check_interval: float = env.CLIENT_ORPHAN_CHECK_INTERVAL
        self._orphan_check_task: asyncio.Task | None = None

        # 9.4.2: Response freshness tracking
        self._response_freshness_timeout: float = env.CLIENT_RESPONSE_FRESHNESS_TIMEOUT

        # 9.6.1: Transfer metrics
        self._gate_transfers_received: int = 0
        self._manager_transfers_received: int = 0
        self._requests_rerouted: int = 0
        self._requests_failed_leadership_change: int = 0

        # 9.1.4: Gate connection state tracking
        self._gate_connection_state: dict[tuple[str, int], str] = {}  # addr -> "connected"/"disconnected"
    
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
        workflows: list[tuple[list[str], object]],
        vus: int = 1,
        timeout_seconds: float = 300.0,
        datacenter_count: int = 1,
        datacenters: list[str] | None = None,
        on_status_update: Callable[[JobStatusPush], None] | None = None,
        on_progress_update: Callable | None = None,  # Callable[[WindowedStatsPush], None]
        on_workflow_result: Callable[[WorkflowResultPush], None] | None = None,
        reporting_configs: list | None = None,
        on_reporter_result: Callable[[ReporterResultPush], None] | None = None,
        max_redirects: int = 3,
        max_retries: int = 5,
        retry_base_delay: float = 0.5,
    ) -> str:
        """
        Submit a job for execution.

        Args:
            workflows: List of (dependencies, workflow_instance) tuples
            vus: Virtual users (cores) per workflow
            timeout_seconds: Maximum execution time
            datacenter_count: Number of datacenters to run in (gates only)
            datacenters: Specific datacenters to target (optional)
            on_status_update: Callback for status updates (optional)
            on_progress_update: Callback for streaming progress updates (optional).
                Called with WindowedStatsPush containing time-correlated aggregated
                stats from workers. Rate-limited to prevent callback spam.
            on_workflow_result: Callback for workflow completion results (optional)
            reporting_configs: List of ReporterConfig objects for result submission (optional)
            on_reporter_result: Callback for reporter submission results (optional)
            max_redirects: Maximum leader redirects to follow
            max_retries: Maximum retries for transient errors (syncing, etc.)
            retry_base_delay: Base delay for exponential backoff (seconds)

        Returns:
            job_id: Unique identifier for the submitted job

        Raises:
            RuntimeError: If no managers/gates configured or submission fails
        """
        job_id = f"job-{secrets.token_hex(8)}"

        # Generate workflow IDs and transform to new format
        # Input: list[tuple[list[str], Workflow]] - (dependencies, workflow)
        # Output: list[tuple[str, list[str], Workflow]] - (workflow_id, dependencies, workflow)
        workflows_with_ids: list[tuple[str, list[str], object]] = []

        # Extract reporter configs from workflow instances for local file handling
        # CSV, XML, and JSON reporters must output locally at the client
        extracted_local_configs: list = []

        for dependencies, workflow_instance in workflows:
            workflow_id = f"wf-{secrets.token_hex(8)}"
            workflows_with_ids.append((workflow_id, dependencies, workflow_instance))

            # Extract reporter config from workflow if present
            workflow_reporting = getattr(workflow_instance, 'reporting', None)
            if workflow_reporting is not None:
                # Handle single config or list of configs
                configs_to_check = (
                    workflow_reporting if isinstance(workflow_reporting, list)
                    else [workflow_reporting]
                )
                for config in configs_to_check:
                    # Check if this is a local file reporter type
                    reporter_type = getattr(config, 'reporter_type', None)
                    if reporter_type in self._local_reporter_types:
                        extracted_local_configs.append(config)

        # Serialize workflows with IDs
        workflows_bytes = cloudpickle.dumps(workflows_with_ids)

        # Pre-submission size validation - fail fast before sending
        if len(workflows_bytes) > MAX_DECOMPRESSED_SIZE:
            raise MessageTooLargeError(
                f"Serialized workflows exceed maximum size: "
                f"{len(workflows_bytes)} > {MAX_DECOMPRESSED_SIZE} bytes (5MB)"
            )

        # Serialize reporter configs if provided
        reporting_configs_bytes = b''
        if reporting_configs:
            reporting_configs_bytes = cloudpickle.dumps(reporting_configs)

        submission = JobSubmission(
            job_id=job_id,
            workflows=workflows_bytes,
            vus=vus,
            timeout_seconds=timeout_seconds,
            datacenter_count=datacenter_count,
            datacenters=datacenters or [],
            callback_addr=self._get_callback_addr(),
            reporting_configs=reporting_configs_bytes,
            # Protocol version fields (AD-25)
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            capabilities=self._capabilities_str,
        )

        # Initialize job tracking
        self._jobs[job_id] = JobResult(
            job_id=job_id,
            status=JobStatus.SUBMITTED.value,
        )
        self._job_events[job_id] = asyncio.Event()
        if on_status_update:
            self._job_callbacks[job_id] = on_status_update
        if on_progress_update:
            self._progress_callbacks[job_id] = on_progress_update
        if on_workflow_result:
            self._workflow_callbacks[job_id] = on_workflow_result
        if on_reporter_result:
            self._reporter_callbacks[job_id] = on_reporter_result

        # Store reporting configs for local file-based reporting
        # Combine extracted local configs from workflows with any explicitly passed configs
        # Filter explicitly passed configs to only include local file types
        explicit_local_configs = [
            config for config in (reporting_configs or [])
            if getattr(config, 'reporter_type', None) in self._local_reporter_types
        ]
        self._job_reporting_configs[job_id] = extracted_local_configs + explicit_local_configs

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

                    # Store negotiated capabilities (AD-25)
                    server_version = ProtocolVersion(
                        major=getattr(ack, 'protocol_version_major', 1),
                        minor=getattr(ack, 'protocol_version_minor', 0),
                    )
                    negotiated_caps_str = getattr(ack, 'capabilities', '')
                    negotiated_features = set(negotiated_caps_str.split(',')) if negotiated_caps_str else set()

                    self._server_negotiated_caps[target] = NegotiatedCapabilities(
                        local_version=CURRENT_PROTOCOL_VERSION,
                        remote_version=server_version,
                        common_features=negotiated_features,
                        compatible=True,
                    )

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
    # Job Cancellation (AD-20)
    # =========================================================================

    async def cancel_job(
        self,
        job_id: str,
        reason: str = "",
        max_redirects: int = 3,
        max_retries: int = 3,
        retry_base_delay: float = 0.5,
        timeout: float = 10.0,
    ) -> JobCancelResponse:
        """
        Cancel a running job.

        Sends a cancellation request to the gate/manager that owns the job.
        The cancellation propagates to all datacenters and workers executing
        workflows for this job.

        Args:
            job_id: Job identifier to cancel.
            reason: Optional reason for cancellation.
            max_redirects: Maximum leader redirects to follow.
            max_retries: Maximum retries for transient errors.
            retry_base_delay: Base delay for exponential backoff (seconds).
            timeout: Request timeout in seconds.

        Returns:
            JobCancelResponse with cancellation result.

        Raises:
            RuntimeError: If no gates/managers configured or cancellation fails.
            KeyError: If job not found (never submitted through this client).
        """
        # Build request
        request = JobCancelRequest(
            job_id=job_id,
            requester_id=f"client-{self._host}:{self._tcp_port}",
            timestamp=time.time(),
            fence_token=0,  # Client doesn't track fence tokens
            reason=reason,
        )

        # Determine targets - prefer the manager/gate that accepted the job
        all_targets: list[tuple[str, int]] = []

        if job_id in self._job_targets:
            # Job was submitted through this client, try its target first
            all_targets.append(self._job_targets[job_id])

        # Add all gates and managers as fallback
        if self._gates:
            for gate in self._gates:
                if gate not in all_targets:
                    all_targets.append(gate)
        if self._managers:
            for manager in self._managers:
                if manager not in all_targets:
                    all_targets.append(manager)

        if not all_targets:
            raise RuntimeError("No managers or gates configured")

        last_error: str | None = None

        # Retry loop with exponential backoff
        for retry in range(max_retries + 1):
            target_idx = retry % len(all_targets)
            target = all_targets[target_idx]

            # Try with leader redirect handling
            redirects = 0
            while redirects <= max_redirects:
                response_data, _ = await self.send_tcp(
                    target,
                    "cancel_job",
                    request.dump(),
                    timeout=timeout,
                )

                if isinstance(response_data, Exception):
                    last_error = str(response_data)
                    break  # Try next retry/target

                if response_data == b'error':
                    last_error = "Server returned error"
                    break

                response = JobCancelResponse.load(response_data)

                if response.success:
                    # Update local job state
                    job = self._jobs.get(job_id)
                    if job:
                        job.status = JobStatus.CANCELLED.value
                        event = self._job_events.get(job_id)
                        if event:
                            event.set()
                    return response

                # Check for already completed/cancelled (not an error)
                if response.already_cancelled or response.already_completed:
                    # Still update local state if we have it
                    job = self._jobs.get(job_id)
                    if job:
                        if response.already_cancelled:
                            job.status = JobStatus.CANCELLED.value
                        elif response.already_completed:
                            job.status = JobStatus.COMPLETED.value
                        event = self._job_events.get(job_id)
                        if event:
                            event.set()
                    return response

                # Check for transient error
                if response.error and self._is_transient_error(response.error):
                    last_error = response.error
                    break  # Exit redirect loop, continue to retry

                # Permanent error
                raise RuntimeError(f"Job cancellation failed: {response.error}")

            # Wait before retry with exponential backoff
            if retry < max_retries:
                delay = retry_base_delay * (2 ** retry)
                await asyncio.sleep(delay)

        # All retries exhausted
        raise RuntimeError(
            f"Job cancellation failed after {max_retries} retries: {last_error}"
        )

    # =========================================================================
    # Client Reconnection
    # =========================================================================

    async def reconnect_to_job(
        self,
        job_id: str,
        on_status_update: Callable[[JobStatusPush], None] | None = None,
        max_retries: int = 3,
        retry_base_delay: float = 0.5,
        timeout: float = 5.0,
    ) -> JobResult:
        """
        Reconnect to an existing job after client disconnect.

        This method re-registers the client's callback address with the
        gate/manager that owns the job, enabling push notification delivery
        to resume. It also returns the current job status for immediate sync.

        Use this when:
        - Client was disconnected and reconnected
        - Client was restarted and needs to resume tracking a job
        - Client wants to start receiving updates for a job submitted elsewhere

        Args:
            job_id: Job identifier to reconnect to
            on_status_update: Optional callback for status updates
            max_retries: Maximum retry attempts for transient errors
            retry_base_delay: Base delay for exponential backoff (seconds)
            timeout: Request timeout in seconds

        Returns:
            JobResult with current job status

        Raises:
            RuntimeError: If no gates/managers configured or reconnection fails
            KeyError: If job not found on any configured gate/manager
        """
        # Build list of all potential targets
        all_targets = []
        if self._gates:
            all_targets.extend(self._gates)
        if self._managers:
            all_targets.extend(self._managers)

        if not all_targets:
            raise RuntimeError("No managers or gates configured")

        request = RegisterCallback(
            job_id=job_id,
            callback_addr=self._get_callback_addr(),
        )

        last_error: str | None = None
        found_target: tuple[str, int] | None = None

        # Try each target with retries
        for retry in range(max_retries + 1):
            for target in all_targets:
                try:
                    response_data, _ = await self.send_tcp(
                        target,
                        "register_callback",
                        request.dump(),
                        timeout=timeout,
                    )

                    if isinstance(response_data, Exception):
                        last_error = str(response_data)
                        continue

                    response = RegisterCallbackResponse.load(response_data)

                    if response.success:
                        found_target = target
                        # Initialize or update job tracking
                        if job_id not in self._jobs:
                            self._jobs[job_id] = JobResult(
                                job_id=job_id,
                                status=response.status,
                                total_completed=response.total_completed,
                                total_failed=response.total_failed,
                                elapsed_seconds=response.elapsed_seconds,
                            )
                            self._job_events[job_id] = asyncio.Event()
                        else:
                            job = self._jobs[job_id]
                            job.status = response.status
                            job.total_completed = response.total_completed
                            job.total_failed = response.total_failed
                            job.elapsed_seconds = response.elapsed_seconds

                        # Track the target for future queries
                        self._job_targets[job_id] = target

                        # Register callback if provided
                        if on_status_update:
                            self._job_callbacks[job_id] = on_status_update

                        # Check if job already completed
                        if response.status in (
                            JobStatus.COMPLETED.value,
                            JobStatus.FAILED.value,
                            JobStatus.CANCELLED.value,
                        ):
                            self._job_events[job_id].set()

                        return self._jobs[job_id]

                    elif response.error:
                        # Check if this is a "job not found" type error
                        if "not found" in response.error.lower():
                            continue  # Try next target
                        elif self._is_transient_error(response.error):
                            last_error = response.error
                            continue  # Try next target
                        else:
                            # Permanent error
                            raise RuntimeError(
                                f"Failed to reconnect to job {job_id}: {response.error}"
                            )

                except Exception as exc:
                    last_error = str(exc)
                    continue

            # If we haven't found the job, wait and retry
            if retry < max_retries and not found_target:
                delay = retry_base_delay * (2 ** retry)
                await asyncio.sleep(delay)

        # Job not found on any target
        raise KeyError(
            f"Job {job_id} not found on any configured gate/manager: {last_error}"
        )

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
    # Datacenter Discovery
    # =========================================================================

    async def get_datacenters(
        self,
        addr: tuple[str, int] | None = None,
        timeout: float = 5.0,
    ) -> DatacenterListResponse:
        """
        Get list of registered datacenters from a gate.

        Returns datacenter information including health status, capacity,
        and leader addresses. Use this to discover available datacenters
        before submitting jobs or to check cluster health.

        Args:
            addr: Gate (host, port) to query. If None, uses next gate in rotation.
            timeout: Request timeout in seconds.

        Returns:
            DatacenterListResponse containing:
            - gate_id: Responding gate's node ID
            - datacenters: List of DatacenterInfo with health/capacity details
            - total_available_cores: Sum of available cores across all DCs
            - healthy_datacenter_count: Count of healthy datacenters

        Raises:
            RuntimeError: If no gates configured or query fails.
        """
        target = addr or self._get_next_gate()
        if not target:
            raise RuntimeError("No gates configured")

        request = DatacenterListRequest(
            request_id=secrets.token_hex(8),
        )

        response_data, _ = await self.send_tcp(
            target,
            "datacenter_list",
            request.dump(),
            timeout=timeout,
        )

        if isinstance(response_data, Exception):
            raise RuntimeError(f"Datacenter list query failed: {response_data}")

        if response_data == b'error':
            raise RuntimeError("Datacenter list query failed: gate returned error")

        return DatacenterListResponse.load(response_data)

    async def get_datacenters_from_all_gates(
        self,
        timeout: float = 5.0,
    ) -> dict[tuple[str, int], DatacenterListResponse | Exception]:
        """
        Query datacenter list from all configured gates concurrently.

        Each gate returns its view of registered datacenters. In a healthy
        cluster, all gates should return the same information.

        Args:
            timeout: Request timeout in seconds per gate.

        Returns:
            Dict mapping gate address to either:
            - DatacenterListResponse on success
            - Exception if query failed
        """
        if not self._gates:
            return {}

        async def query_one(
            gate_addr: tuple[str, int],
        ) -> tuple[tuple[str, int], DatacenterListResponse | Exception]:
            try:
                result = await self.get_datacenters(addr=gate_addr, timeout=timeout)
                return (gate_addr, result)
            except Exception as e:
                return (gate_addr, e)

        results = await asyncio.gather(
            *[query_one(gate_addr) for gate_addr in self._gates],
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

        except Exception:
            return b'error'

    @tcp.receive()
    async def job_batch_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle batch stats push notification from gate/manager.

        JobBatchPush contains detailed progress for a single job including
        step-level stats and per-datacenter breakdown.
        """
        try:
            push = JobBatchPush.load(data)

            job = self._jobs.get(push.job_id)
            if job:
                job.status = push.status
                job.total_completed = push.total_completed
                job.total_failed = push.total_failed
                job.overall_rate = push.overall_rate
                job.elapsed_seconds = push.elapsed_seconds

            return b'ok'

        except Exception:
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

        except Exception:
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

        except Exception:
            return b'error'

    @tcp.receive()
    async def reporter_result_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle reporter result notification from manager or gate.

        Called when a reporter submission completes (success or failure).
        Updates the job's reporter_results and calls any registered callback.
        """
        try:
            push = ReporterResultPush.load(data)

            job = self._jobs.get(push.job_id)
            if job:
                # Store the result
                job.reporter_results[push.reporter_type] = ReporterResult(
                    reporter_type=push.reporter_type,
                    success=push.success,
                    error=push.error,
                    elapsed_seconds=push.elapsed_seconds,
                    source=push.source,
                    datacenter=push.datacenter,
                )

            # Call user callback if registered
            callback = self._reporter_callbacks.get(push.job_id)
            if callback:
                try:
                    callback(push)
                except Exception:
                    pass  # Don't let callback errors break the handler

            return b'ok'

        except Exception:
            return b'error'

    @tcp.receive()
    async def workflow_result_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle workflow result push from manager or gate.

        Called when a workflow completes with aggregated results.
        Updates the job's workflow_results for immediate access.

        For multi-DC jobs (via gates), includes per_dc_results with per-datacenter breakdown.
        For single-DC jobs (direct from manager), per_dc_results will be empty.
        """
        try:
            push = WorkflowResultPush.load(data)

            job = self._jobs.get(push.job_id)
            if job:
                # Extract aggregated stats (should be single item list for client-bound)
                stats = push.results[0] if push.results else None

                # Convert per-DC results from message format to client format
                per_dc_results: list[WorkflowDCResultClient] = []
                for dc_result in push.per_dc_results:
                    per_dc_results.append(WorkflowDCResultClient(
                        datacenter=dc_result.datacenter,
                        status=dc_result.status,
                        stats=dc_result.stats,
                        error=dc_result.error,
                        elapsed_seconds=dc_result.elapsed_seconds,
                    ))

                # Use push.completed_at if provided, otherwise use current time
                completed_at = push.completed_at if push.completed_at > 0 else time.time()

                job.workflow_results[push.workflow_id] = WorkflowResult(
                    workflow_id=push.workflow_id,
                    workflow_name=push.workflow_name,
                    status=push.status,
                    stats=stats,
                    error=push.error,
                    elapsed_seconds=push.elapsed_seconds,
                    completed_at=completed_at,
                    per_dc_results=per_dc_results,
                )

            # Call user callback if registered
            callback = self._workflow_callbacks.get(push.job_id)
            if callback:
                try:
                    callback(push)
                except Exception:
                    pass  # Don't let callback errors break the handler

            # Submit to local file-based reporters (aggregated stats only, not per-DC)
            if stats:
                await self._submit_to_local_reporters(push.job_id, push.workflow_name, stats)

            return b'ok'

        except Exception:
            return b'error'

    async def _submit_to_local_reporters(
        self,
        job_id: str,
        workflow_name: str,
        workflow_stats: dict,
    ) -> None:
        """
        Submit workflow results to local file-based reporters.

        Uses configured reporters if provided, otherwise defaults to per-workflow
        JSON files with naming pattern: <workflow_name>_workflow_results.json
        """
        configs = self._job_reporting_configs.get(job_id, [])

        # Filter to only file-based reporters
        local_configs = [
            config for config in configs
            if hasattr(config, 'reporter_type') and config.reporter_type in self._local_reporter_types
        ]

        # If no file-based configs provided, use default per-workflow JSON
        if not local_configs:
            workflow_name_lower = workflow_name.lower()
            local_configs = [
                JSONConfig(
                    workflow_results_filepath=f"{workflow_name_lower}_workflow_results.json",
                    step_results_filepath=f"{workflow_name_lower}_step_results.json",
                )
            ]

        for config in local_configs:
            await self._submit_single_reporter(config, workflow_stats)

    async def _submit_single_reporter(self, config, workflow_stats: dict) -> None:
        """Submit results to a single local reporter."""
        try:
            reporter = Reporter(config)
            await reporter.connect()

            try:
                await reporter.submit_workflow_results(workflow_stats)
                await reporter.submit_step_results(workflow_stats)
            finally:
                await reporter.close()

        except Exception:
            pass  # Best effort - don't break on reporter failures

    @tcp.receive()
    async def windowed_stats_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ):
        """
        Handle windowed stats push from manager or gate.

        Called periodically with time-correlated aggregated stats.
        Rate-limited using the same AdaptiveRateLimiter as manager/gate/worker.
        """
        try:
            # Use the same AdaptiveRateLimiter infrastructure as manager/gate/worker
            # Client ID is "client-local" since we're the receiver
            # Operation is "progress_update" which has limits of (300, 10.0) = 30/s
            client_id = f"{addr[0]}:{addr[1]}"
            result = self._rate_limiter.check(
                client_id=client_id,
                operation="progress_update",
                priority=RequestPriority.NORMAL,
            )
            if not result.allowed:
                return b'rate_limited'

            import cloudpickle
            import time as time_module
            from hyperscale.distributed_rewrite.jobs import WindowedStatsPush
            push: WindowedStatsPush = cloudpickle.loads(data)

            # Call user callback if registered
            callback = self._progress_callbacks.get(push.job_id)
            if callback:
                try:
                    callback(push)
                except Exception:
                    pass  # Don't let callback errors break the handler

            return b'ok'

        except Exception:
            return b'error'

    @tcp.receive()
    async def receive_job_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle job cancellation completion push from manager or gate (AD-20).

        Called when all workflows in a job have been cancelled. The notification
        includes success status and any errors encountered during cancellation.
        """
        try:
            completion = JobCancellationComplete.load(data)
            job_id = completion.job_id

            # Store results for await_job_cancellation
            self._cancellation_success[job_id] = completion.success
            self._cancellation_errors[job_id] = completion.errors

            # Fire the completion event
            event = self._cancellation_events.get(job_id)
            if event:
                event.set()

            return b"OK"

        except Exception:
            return b"ERROR"

    async def await_job_cancellation(
        self,
        job_id: str,
        timeout: float | None = None,
    ) -> tuple[bool, list[str]]:
        """
        Wait for job cancellation to complete.

        This method blocks until the job cancellation is fully complete and the
        push notification is received from the manager/gate, or until timeout.

        Args:
            job_id: The job ID to wait for cancellation completion
            timeout: Optional timeout in seconds. None means wait indefinitely.

        Returns:
            Tuple of (success, errors):
            - success: True if all workflows were cancelled successfully
            - errors: List of error messages from workflows that failed to cancel
        """
        # Create event if not exists (in case called before cancel_job)
        if job_id not in self._cancellation_events:
            self._cancellation_events[job_id] = asyncio.Event()

        event = self._cancellation_events[job_id]

        try:
            if timeout is not None:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            else:
                await event.wait()
        except asyncio.TimeoutError:
            return (False, [f"Timeout waiting for cancellation completion after {timeout}s"])

        # Get the results
        success = self._cancellation_success.get(job_id, False)
        errors = self._cancellation_errors.get(job_id, [])

        # Cleanup tracking structures
        self._cancellation_events.pop(job_id, None)
        self._cancellation_success.pop(job_id, None)
        self._cancellation_errors.pop(job_id, None)

        return (success, errors)

    # =========================================================================
    # Section 9: Client Leadership Transfer Handling
    # =========================================================================

    def _get_request_routing_lock(self, job_id: str) -> asyncio.Lock:
        """
        Get or create a lock for request routing (Section 9.3.2).

        Per-job locks prevent race conditions between leadership updates
        and request routing.
        """
        if job_id not in self._request_routing_locks:
            self._request_routing_locks[job_id] = asyncio.Lock()
        return self._request_routing_locks[job_id]

    def _validate_gate_fence_token(self, job_id: str, new_fence_token: int) -> tuple[bool, str]:
        """
        Validate a gate transfer's fence token (Section 9.1.2).

        Returns (is_valid, rejection_reason).
        """
        current_leader = self._gate_job_leaders.get(job_id)
        if current_leader and new_fence_token <= current_leader.fence_token:
            return (
                False,
                f"Stale fence token: received {new_fence_token}, current {current_leader.fence_token}"
            )
        return (True, "")

    def _validate_manager_fence_token(
        self,
        job_id: str,
        datacenter_id: str,
        new_fence_token: int,
    ) -> tuple[bool, str]:
        """
        Validate a manager transfer's fence token (Section 9.2.2).

        Returns (is_valid, rejection_reason).
        """
        key = (job_id, datacenter_id)
        current_leader = self._manager_job_leaders.get(key)
        if current_leader and new_fence_token <= current_leader.fence_token:
            return (
                False,
                f"Stale fence token: received {new_fence_token}, current {current_leader.fence_token}"
            )
        return (True, "")

    def _update_gate_leader(
        self,
        job_id: str,
        gate_addr: tuple[str, int],
        fence_token: int,
    ) -> None:
        """Update gate job leader tracking (Section 9.1.1)."""
        self._gate_job_leaders[job_id] = GateLeaderInfo(
            gate_addr=gate_addr,
            fence_token=fence_token,
            last_updated=time.monotonic(),
        )
        # Clear orphan status if present
        if job_id in self._orphaned_jobs:
            del self._orphaned_jobs[job_id]

    def _update_manager_leader(
        self,
        job_id: str,
        datacenter_id: str,
        manager_addr: tuple[str, int],
        fence_token: int,
    ) -> None:
        """Update manager job leader tracking (Section 9.2.1)."""
        key = (job_id, datacenter_id)
        self._manager_job_leaders[key] = ManagerLeaderInfo(
            manager_addr=manager_addr,
            fence_token=fence_token,
            datacenter_id=datacenter_id,
            last_updated=time.monotonic(),
        )

    def _mark_job_orphaned(
        self,
        job_id: str,
        last_known_gate: tuple[str, int] | None,
        last_known_manager: tuple[str, int] | None,
        datacenter_id: str = "",
    ) -> None:
        """Mark a job as orphaned (Section 9.5.1)."""
        if job_id not in self._orphaned_jobs:
            self._orphaned_jobs[job_id] = OrphanedJobInfo(
                job_id=job_id,
                orphan_timestamp=time.monotonic(),
                last_known_gate=last_known_gate,
                last_known_manager=last_known_manager,
                datacenter_id=datacenter_id,
            )

    @tcp.receive()
    async def receive_gate_job_leader_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle gate job leadership transfer notification (Section 9.1.2).

        Received from the new gate job leader when taking over from a failed gate.
        """
        self._gate_transfers_received += 1

        try:
            transfer = GateJobLeaderTransfer.load(data)
            job_id = transfer.job_id

            # Acquire routing lock to prevent race with in-flight requests
            routing_lock = self._get_request_routing_lock(job_id)
            async with routing_lock:

                # Validate fence token
                fence_valid, fence_reason = self._validate_gate_fence_token(
                    job_id, transfer.fence_token
                )
                if not fence_valid:
                    await self._udp_logger.log(
                        ServerInfo(
                            message=f"Rejected gate transfer for job {job_id[:8]}...: {fence_reason}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                    return GateJobLeaderTransferAck(
                        job_id=job_id,
                        client_id=self._node_id.full,
                        accepted=False,
                        rejection_reason=fence_reason,
                    ).dump()

                # Update gate leader
                old_gate_str = f"{transfer.old_gate_addr}" if transfer.old_gate_addr else "unknown"
                self._update_gate_leader(
                    job_id=job_id,
                    gate_addr=transfer.new_gate_addr,
                    fence_token=transfer.fence_token,
                )

                # Update job target for future requests
                if job_id in self._job_targets:
                    self._job_targets[job_id] = transfer.new_gate_addr

                await self._udp_logger.log(
                    ServerInfo(
                        message=f"Gate job leader transfer: job={job_id[:8]}..., "
                                f"old={old_gate_str}, new={transfer.new_gate_addr}, "
                                f"fence_token={transfer.fence_token}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

                return GateJobLeaderTransferAck(
                    job_id=job_id,
                    client_id=self._node_id.full,
                    accepted=True,
                ).dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Error processing gate transfer: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return GateJobLeaderTransferAck(
                job_id="unknown",
                client_id=self._node_id.full,
                accepted=False,
                rejection_reason=str(error),
            ).dump()

    @tcp.receive()
    async def receive_manager_job_leader_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle manager job leadership transfer notification (Section 9.2.2).

        Typically forwarded by gate to client when a manager job leader changes.
        """
        self._manager_transfers_received += 1

        try:
            transfer = ManagerJobLeaderTransfer.load(data)
            job_id = transfer.job_id
            datacenter_id = transfer.datacenter_id

            # Acquire routing lock
            routing_lock = self._get_request_routing_lock(job_id)
            async with routing_lock:

                # Validate fence token
                fence_valid, fence_reason = self._validate_manager_fence_token(
                    job_id, datacenter_id, transfer.fence_token
                )
                if not fence_valid:
                    await self._udp_logger.log(
                        ServerInfo(
                            message=f"Rejected manager transfer for job {job_id[:8]}...: {fence_reason}",
                            node_host=self._host,
                            node_port=self._tcp_port,
                            node_id=self._node_id.short,
                        )
                    )
                    return ManagerJobLeaderTransferAck(
                        job_id=job_id,
                        client_id=self._node_id.full,
                        datacenter_id=datacenter_id,
                        accepted=False,
                        rejection_reason=fence_reason,
                    ).dump()

                # Update manager leader
                old_manager_str = f"{transfer.old_manager_addr}" if transfer.old_manager_addr else "unknown"
                self._update_manager_leader(
                    job_id=job_id,
                    datacenter_id=datacenter_id,
                    manager_addr=transfer.new_manager_addr,
                    fence_token=transfer.fence_token,
                )

                await self._udp_logger.log(
                    ServerInfo(
                        message=f"Manager job leader transfer: job={job_id[:8]}..., dc={datacenter_id}, "
                                f"old={old_manager_str}, new={transfer.new_manager_addr}, "
                                f"fence_token={transfer.fence_token}",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short,
                    )
                )

                return ManagerJobLeaderTransferAck(
                    job_id=job_id,
                    client_id=self._node_id.full,
                    datacenter_id=datacenter_id,
                    accepted=True,
                ).dump()

        except Exception as error:
            await self._udp_logger.log(
                ServerError(
                    message=f"Error processing manager transfer: {error}",
                    node_host=self._host,
                    node_port=self._tcp_port,
                    node_id=self._node_id.short,
                )
            )
            return ManagerJobLeaderTransferAck(
                job_id="unknown",
                client_id=self._node_id.full,
                datacenter_id="",
                accepted=False,
                rejection_reason=str(error),
            ).dump()

    def get_current_gate_leader(self, job_id: str) -> tuple[str, int] | None:
        """Get the current gate leader address for a job (Section 9.1.1)."""
        leader_info = self._gate_job_leaders.get(job_id)
        if leader_info:
            return leader_info.gate_addr
        return None

    def get_current_manager_leader(
        self,
        job_id: str,
        datacenter_id: str,
    ) -> tuple[str, int] | None:
        """Get the current manager leader address for a job in a datacenter (Section 9.2.1)."""
        key = (job_id, datacenter_id)
        leader_info = self._manager_job_leaders.get(key)
        if leader_info:
            return leader_info.manager_addr
        return None

    def is_job_orphaned(self, job_id: str) -> bool:
        """Check if a job is currently in orphan state (Section 9.5.1)."""
        return job_id in self._orphaned_jobs

    def get_leadership_metrics(self) -> dict[str, int]:
        """Get leadership transfer metrics (Section 9.6.1)."""
        return {
            "gate_transfers_received": self._gate_transfers_received,
            "manager_transfers_received": self._manager_transfers_received,
            "requests_rerouted": self._requests_rerouted,
            "requests_failed_leadership_change": self._requests_failed_leadership_change,
            "orphaned_jobs": len(self._orphaned_jobs),
            "tracked_gate_leaders": len(self._gate_job_leaders),
            "tracked_manager_leaders": len(self._manager_job_leaders),
        }

