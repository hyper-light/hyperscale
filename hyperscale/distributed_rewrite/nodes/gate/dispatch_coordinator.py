"""
Gate job dispatch coordination module.

Coordinates job submission and dispatch to datacenter managers.
"""

import asyncio
import time
from typing import TYPE_CHECKING

import cloudpickle

from hyperscale.distributed_rewrite.models import (
    JobSubmission,
    JobAck,
    JobStatus,
    GlobalJobStatus,
    RateLimitResponse,
)
from hyperscale.distributed_rewrite.protocol.version import (
    ProtocolVersion,
    CURRENT_PROTOCOL_VERSION,
    get_features_for_version,
)
from hyperscale.distributed_rewrite.swim.core import (
    CircuitState,
    QuorumCircuitOpenError,
    QuorumUnavailableError,
)

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.gate.state import GateRuntimeState
    from hyperscale.distributed_rewrite.jobs.gates import GateJobManager
    from hyperscale.distributed_rewrite.routing import GateJobRouter
    from hyperscale.logging.hyperscale_logger import Logger
    from hyperscale.taskex import TaskRunner


class GateDispatchCoordinator:
    """
    Coordinates job dispatch to datacenter managers.

    Responsibilities:
    - Handle job submissions from clients
    - Select target datacenters
    - Dispatch jobs to managers
    - Track job state
    """

    def __init__(
        self,
        state: "GateRuntimeState",
        logger: "Logger",
        task_runner: "TaskRunner",
        job_manager: "GateJobManager",
        job_router: "GateJobRouter | None",
        check_rate_limit: callable,
        should_shed_request: callable,
        has_quorum_available: callable,
        quorum_size: callable,
        quorum_circuit,
        select_datacenters: callable,
        assume_leadership: callable,
        broadcast_leadership: callable,
        dispatch_to_dcs: callable,
    ) -> None:
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._job_manager = job_manager
        self._job_router = job_router
        self._check_rate_limit = check_rate_limit
        self._should_shed_request = should_shed_request
        self._has_quorum_available = has_quorum_available
        self._quorum_size = quorum_size
        self._quorum_circuit = quorum_circuit
        self._select_datacenters = select_datacenters
        self._assume_leadership = assume_leadership
        self._broadcast_leadership = broadcast_leadership
        self._dispatch_to_dcs = dispatch_to_dcs

    async def submit_job(
        self,
        addr: tuple[str, int],
        submission: JobSubmission,
    ) -> JobAck:
        """
        Process job submission from client.

        Args:
            addr: Client address
            submission: Job submission message

        Returns:
            JobAck with acceptance status
        """
        # Check rate limit (AD-24)
        client_id = f"{addr[0]}:{addr[1]}"
        allowed, retry_after = self._check_rate_limit(client_id, "job_submit")
        if not allowed:
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=f"Rate limited, retry after {retry_after}s",
            )

        # Check load shedding (AD-22)
        if self._should_shed_request("JobSubmission"):
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="System under load, please retry later",
            )

        # Protocol version check (AD-25)
        client_version = ProtocolVersion(
            major=getattr(submission, 'protocol_version_major', 1),
            minor=getattr(submission, 'protocol_version_minor', 0),
        )

        if client_version.major != CURRENT_PROTOCOL_VERSION.major:
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=f"Incompatible protocol version: {client_version}",
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            )

        # Negotiate capabilities
        client_caps = getattr(submission, 'capabilities', '')
        client_features = set(client_caps.split(',')) if client_caps else set()
        our_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
        negotiated = ','.join(sorted(client_features & our_features))

        # Check circuit breaker
        if self._quorum_circuit.circuit_state == CircuitState.OPEN:
            retry_after = self._quorum_circuit.half_open_after
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error=f"Circuit open, retry after {retry_after}s",
            )

        # Check quorum (multi-gate deployments)
        if (self._state.get_active_peer_count() > 0 and
            not self._has_quorum_available()):
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="Quorum unavailable",
            )

        # Select datacenters (AD-36 if router available)
        primary_dcs, fallback_dcs, worst_health = self._select_datacenters(
            submission.datacenter_count,
            submission.datacenters if submission.datacenters else None,
            job_id=submission.job_id,
        )

        if worst_health == "initializing":
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="initializing",  # Client will retry
            )

        if not primary_dcs:
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="No available datacenters",
            )

        # Create global job tracking
        job = GlobalJobStatus(
            job_id=submission.job_id,
            status=JobStatus.SUBMITTED.value,
            datacenters=[],
            timestamp=time.monotonic(),
        )
        self._job_manager.set_job(submission.job_id, job)
        self._job_manager.set_target_dcs(submission.job_id, set(primary_dcs))

        # Extract and track workflow IDs
        try:
            workflows = cloudpickle.loads(submission.workflows)
            workflow_ids = {wf_id for wf_id, _, _ in workflows}
            self._state._job_workflow_ids[submission.job_id] = workflow_ids
        except Exception:
            self._state._job_workflow_ids[submission.job_id] = set()

        # Store callback for push notifications
        if submission.callback_addr:
            self._job_manager.set_callback(submission.job_id, submission.callback_addr)
            self._state._progress_callbacks[submission.job_id] = submission.callback_addr

        # Store submission for reporter configs
        if submission.reporting_configs:
            self._state._job_submissions[submission.job_id] = submission

        # Assume leadership for this job
        self._assume_leadership(submission.job_id, len(primary_dcs))

        # Broadcast leadership to peer gates
        await self._broadcast_leadership(submission.job_id, len(primary_dcs))

        # Record success for circuit breaker
        self._quorum_circuit.record_success()

        # Dispatch to DCs in background
        self._task_runner.run(self._dispatch_to_dcs, submission, primary_dcs)

        return JobAck(
            job_id=submission.job_id,
            accepted=True,
            queued_position=self._job_manager.job_count(),
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            capabilities=negotiated,
        )


__all__ = ["GateDispatchCoordinator"]
