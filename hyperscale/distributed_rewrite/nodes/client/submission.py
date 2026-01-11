"""
Job submission for HyperscaleClient.

Handles job submission with retry logic, leader redirection, and protocol negotiation.
"""

import asyncio
import random
import secrets
from typing import Callable

import cloudpickle

from hyperscale.core.jobs.protocols.constants import MAX_DECOMPRESSED_SIZE
from hyperscale.distributed_rewrite.errors import MessageTooLargeError
from hyperscale.distributed_rewrite.models import (
    JobSubmission,
    JobAck,
    JobStatusPush,
    WorkflowResultPush,
    ReporterResultPush,
    RateLimitResponse,
)
from hyperscale.distributed_rewrite.protocol.version import CURRENT_PROTOCOL_VERSION
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.distributed_rewrite.nodes.client.config import ClientConfig, TRANSIENT_ERRORS
from hyperscale.logging.hyperscale_logger import Logger


class ClientJobSubmitter:
    """
    Manages job submission with retry logic and leader redirection.

    Submission flow:
    1. Generate job_id and workflow_ids
    2. Extract local reporter configs from workflows
    3. Serialize workflows and reporter configs with cloudpickle
    4. Pre-submission size validation (5MB limit)
    5. Build JobSubmission message with protocol version
    6. Initialize job tracking structures
    7. Retry loop with exponential backoff:
       - Cycle through all targets (gates/managers)
       - Follow leader redirects (up to max_redirects)
       - Detect transient errors and retry
       - Permanent rejection fails immediately
    8. Store negotiated capabilities on success
    9. Return job_id
    """

    def __init__(
        self,
        state: ClientState,
        config: ClientConfig,
        logger: Logger,
        targets,  # ClientTargetSelector
        tracker,  # ClientJobTracker
        protocol,  # ClientProtocol
        send_tcp_func,  # Callable for sending TCP messages
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._targets = targets
        self._tracker = tracker
        self._protocol = protocol
        self._send_tcp = send_tcp_func

    async def submit_job(
        self,
        workflows: list[tuple[list[str], object]],
        vus: int = 1,
        timeout_seconds: float = 300.0,
        datacenter_count: int = 1,
        datacenters: list[str] | None = None,
        on_status_update: Callable[[JobStatusPush], None] | None = None,
        on_progress_update: Callable | None = None,
        on_workflow_result: Callable[[WorkflowResultPush], None] | None = None,
        reporting_configs: list | None = None,
        on_reporter_result: Callable[[ReporterResultPush], None] | None = None,
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
            on_progress_update: Callback for streaming progress updates (optional)
            on_workflow_result: Callback for workflow completion results (optional)
            reporting_configs: List of ReporterConfig objects for result submission (optional)
            on_reporter_result: Callback for reporter submission results (optional)

        Returns:
            job_id: Unique identifier for the submitted job

        Raises:
            RuntimeError: If no managers/gates configured or submission fails
            MessageTooLargeError: If serialized workflows exceed 5MB
        """
        job_id = f"job-{secrets.token_hex(8)}"

        # Extract reporter configs and generate workflow IDs
        workflows_with_ids, extracted_local_configs = self._prepare_workflows(workflows)

        # Serialize workflows
        workflows_bytes = cloudpickle.dumps(workflows_with_ids)

        # Pre-submission size validation - fail fast before sending
        self._validate_submission_size(workflows_bytes)

        # Serialize reporter configs if provided
        reporting_configs_bytes = b''
        if reporting_configs:
            reporting_configs_bytes = cloudpickle.dumps(reporting_configs)

        # Build submission message
        submission = self._build_job_submission(
            job_id=job_id,
            workflows_bytes=workflows_bytes,
            vus=vus,
            timeout_seconds=timeout_seconds,
            datacenter_count=datacenter_count,
            datacenters=datacenters or [],
            reporting_configs_bytes=reporting_configs_bytes,
        )

        # Initialize job tracking
        self._tracker.initialize_job_tracking(
            job_id,
            on_status_update=on_status_update,
            on_progress_update=on_progress_update,
            on_workflow_result=on_workflow_result,
            on_reporter_result=on_reporter_result,
        )

        # Store reporting configs for local file-based reporting
        explicit_local_configs = [
            config
            for config in (reporting_configs or [])
            if getattr(config, 'reporter_type', None) in self._config.local_reporter_types
        ]
        self._state._job_reporting_configs[job_id] = extracted_local_configs + explicit_local_configs

        # Submit with retry logic
        try:
            await self._submit_with_retry(job_id, submission)
            return job_id
        except Exception as error:
            self._tracker.mark_job_failed(job_id, str(error))
            raise

    def _prepare_workflows(
        self,
        workflows: list[tuple[list[str], object]],
    ) -> tuple[list[tuple[str, list[str], object]], list]:
        """
        Generate workflow IDs and extract local reporter configs.

        Args:
            workflows: List of (dependencies, workflow_instance) tuples

        Returns:
            (workflows_with_ids, extracted_local_configs) tuple
        """
        workflows_with_ids: list[tuple[str, list[str], object]] = []
        extracted_local_configs: list = []

        for dependencies, workflow_instance in workflows:
            workflow_id = f"wf-{secrets.token_hex(8)}"
            workflows_with_ids.append((workflow_id, dependencies, workflow_instance))

            # Extract reporter config from workflow if present
            workflow_reporting = getattr(workflow_instance, 'reporting', None)
            if workflow_reporting is not None:
                # Handle single config or list of configs
                configs_to_check = (
                    workflow_reporting
                    if isinstance(workflow_reporting, list)
                    else [workflow_reporting]
                )
                for config in configs_to_check:
                    # Check if this is a local file reporter type
                    reporter_type = getattr(config, 'reporter_type', None)
                    if reporter_type in self._config.local_reporter_types:
                        extracted_local_configs.append(config)

        return (workflows_with_ids, extracted_local_configs)

    def _validate_submission_size(self, workflows_bytes: bytes) -> None:
        """
        Validate serialized workflows don't exceed size limit.

        Args:
            workflows_bytes: Serialized workflows

        Raises:
            MessageTooLargeError: If size exceeds MAX_DECOMPRESSED_SIZE (5MB)
        """
        if len(workflows_bytes) > MAX_DECOMPRESSED_SIZE:
            raise MessageTooLargeError(
                f"Serialized workflows exceed maximum size: "
                f"{len(workflows_bytes)} > {MAX_DECOMPRESSED_SIZE} bytes (5MB)"
            )

    def _build_job_submission(
        self,
        job_id: str,
        workflows_bytes: bytes,
        vus: int,
        timeout_seconds: float,
        datacenter_count: int,
        datacenters: list[str],
        reporting_configs_bytes: bytes,
    ) -> JobSubmission:
        """
        Build JobSubmission message with protocol version.

        Args:
            job_id: Job identifier
            workflows_bytes: Serialized workflows
            vus: Virtual users
            timeout_seconds: Timeout
            datacenter_count: DC count
            datacenters: Specific DCs
            reporting_configs_bytes: Serialized reporter configs

        Returns:
            JobSubmission message
        """
        return JobSubmission(
            job_id=job_id,
            workflows=workflows_bytes,
            vus=vus,
            timeout_seconds=timeout_seconds,
            datacenter_count=datacenter_count,
            datacenters=datacenters,
            callback_addr=self._targets.get_callback_addr(),
            reporting_configs=reporting_configs_bytes,
            # Protocol version fields (AD-25)
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            capabilities=self._protocol.get_client_capabilities_string(),
        )

    async def _submit_with_retry(
        self,
        job_id: str,
        submission: JobSubmission,
    ) -> None:
        """
        Submit job with retry logic and leader redirection.

        Args:
            job_id: Job identifier
            submission: JobSubmission message

        Raises:
            RuntimeError: If submission fails after retries
        """
        # Get all available targets for fallback
        all_targets = self._targets.get_all_targets()
        if not all_targets:
            raise RuntimeError("No managers or gates configured")

        # Retry loop with exponential backoff for transient errors
        last_error = None
        max_retries = self._config.submission_max_retries
        max_redirects = self._config.submission_max_redirects_per_attempt
        retry_base_delay = 0.5

        for retry in range(max_retries + 1):
            # Try each target in order, cycling through on retries
            target_idx = retry % len(all_targets)
            target = all_targets[target_idx]

            # Submit with leader redirect handling
            redirect_result = await self._submit_with_redirects(
                job_id, target, submission, max_redirects
            )

            if redirect_result == "success":
                return  # Success!
            elif redirect_result == "permanent_failure":
                # Permanent rejection - already raised error
                return
            else:
                # Transient error - retry
                last_error = redirect_result

            # Exponential backoff before retry with jitter (AD-21)
            if retry < max_retries and last_error:
                base_delay = retry_base_delay * (2**retry)
                delay = base_delay * (0.5 + random.random())  # Add 0-100% jitter
                await asyncio.sleep(delay)

        # All retries exhausted
        raise RuntimeError(f"Job submission failed after {max_retries} retries: {last_error}")

    async def _submit_with_redirects(
        self,
        job_id: str,
        target: tuple[str, int],
        submission: JobSubmission,
        max_redirects: int,
    ) -> str:
        """
        Submit to target with leader redirect handling.

        Args:
            job_id: Job identifier
            target: Initial target (host, port)
            submission: JobSubmission message
            max_redirects: Maximum redirects to follow

        Returns:
            "success", "permanent_failure", or error message (transient)
        """
        redirects = 0
        while redirects <= max_redirects:
            response, _ = await self._send_tcp(
                target,
                "job_submission",
                submission.dump(),
                timeout=10.0,
            )

            if isinstance(response, Exception):
                return str(response)  # Transient error

            ack = JobAck.load(response)

            if ack.accepted:
                # Track which server accepted this job for future queries
                self._state.mark_job_target(job_id, target)

                # Store negotiated capabilities (AD-25)
                self._protocol.negotiate_capabilities(
                    server_addr=target,
                    server_version_major=getattr(ack, 'protocol_version_major', 1),
                    server_version_minor=getattr(ack, 'protocol_version_minor', 0),
                    server_capabilities_str=getattr(ack, 'capabilities', ''),
                )

                return "success"

            # Check for leader redirect
            if ack.leader_addr and redirects < max_redirects:
                target = tuple(ack.leader_addr)
                redirects += 1
                continue

            # Check if this is a transient error that should be retried
            if ack.error and self._is_transient_error(ack.error):
                return ack.error  # Transient error

            # Permanent rejection - fail immediately
            raise RuntimeError(f"Job rejected: {ack.error}")

        return "max_redirects_exceeded"

    def _is_transient_error(self, error: str) -> bool:
        """
        Check if an error is transient and should be retried.

        Args:
            error: Error message

        Returns:
            True if error matches TRANSIENT_ERRORS patterns
        """
        error_lower = error.lower()
        return any(te in error_lower for te in TRANSIENT_ERRORS)
