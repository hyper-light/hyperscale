"""
Gate job dispatch coordination module.

Coordinates job submission and dispatch to datacenter managers.
"""

import time
from typing import TYPE_CHECKING, Callable

import cloudpickle

from hyperscale.distributed.models import (
    JobSubmission,
    JobAck,
    JobStatus,
    GlobalJobStatus,
)
from hyperscale.distributed.protocol.version import (
    ProtocolVersion,
    CURRENT_PROTOCOL_VERSION,
    get_features_for_version,
)
from hyperscale.distributed.swim.core import CircuitState
from hyperscale.distributed.reliability import (
    RetryExecutor,
    RetryConfig,
    JitterStrategy,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerWarning,
    ServerInfo,
    ServerError,
)

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.distributed.jobs.gates import GateJobManager, GateJobTimeoutTracker
    from hyperscale.distributed.routing import GateJobRouter
    from hyperscale.distributed.health import CircuitBreakerManager
    from hyperscale.logging import Logger
    from hyperscale.distributed.taskex import TaskRunner


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
        job_timeout_tracker: "GateJobTimeoutTracker",
        circuit_breaker_manager: "CircuitBreakerManager",
        datacenter_managers: dict[str, list[tuple[str, int]]],
        check_rate_limit: Callable,
        should_shed_request: Callable,
        has_quorum_available: Callable,
        quorum_size: Callable,
        quorum_circuit,
        select_datacenters: Callable,
        assume_leadership: Callable,
        broadcast_leadership: Callable,
        send_tcp: Callable,
        increment_version: Callable,
        confirm_manager_for_dc: Callable,
        suspect_manager_for_dc: Callable,
        record_forward_throughput_event: Callable,
        get_node_host: Callable[[], str],
        get_node_port: Callable[[], int],
        get_node_id_short: Callable[[], str],
    ) -> None:
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._job_manager = job_manager
        self._job_router = job_router
        self._job_timeout_tracker = job_timeout_tracker
        self._circuit_breaker_manager = circuit_breaker_manager
        self._datacenter_managers = datacenter_managers
        self._check_rate_limit = check_rate_limit
        self._should_shed_request = should_shed_request
        self._has_quorum_available = has_quorum_available
        self._quorum_size = quorum_size
        self._quorum_circuit = quorum_circuit
        self._select_datacenters = select_datacenters
        self._assume_leadership = assume_leadership
        self._broadcast_leadership = broadcast_leadership
        self._send_tcp = send_tcp
        self._increment_version = increment_version
        self._confirm_manager_for_dc = confirm_manager_for_dc
        self._suspect_manager_for_dc = suspect_manager_for_dc
        self._record_forward_throughput_event = record_forward_throughput_event
        self._get_node_host = get_node_host
        self._get_node_port = get_node_port
        self._get_node_id_short = get_node_id_short

    async def _check_rate_and_load(
        self,
        client_id: str,
        job_id: str,
    ) -> JobAck | None:
        """Check rate limit and load shedding. Returns rejection JobAck if rejected."""
        allowed, retry_after = await self._check_rate_limit(client_id, "job_submit")
        if not allowed:
            return JobAck(
                job_id=job_id,
                accepted=False,
                error=f"Rate limited, retry after {retry_after}s",
            )

        if self._should_shed_request("JobSubmission"):
            return JobAck(
                job_id=job_id,
                accepted=False,
                error="System under load, please retry later",
            )
        return None

    def _check_protocol_version(
        self,
        submission: JobSubmission,
    ) -> tuple[JobAck | None, str]:
        """Check protocol compatibility. Returns (rejection_ack, negotiated_caps)."""
        client_version = ProtocolVersion(
            major=getattr(submission, "protocol_version_major", 1),
            minor=getattr(submission, "protocol_version_minor", 0),
        )

        if client_version.major != CURRENT_PROTOCOL_VERSION.major:
            return (
                JobAck(
                    job_id=submission.job_id,
                    accepted=False,
                    error=f"Incompatible protocol version: {client_version}",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ),
                "",
            )

        client_caps = getattr(submission, "capabilities", "")
        client_features = set(client_caps.split(",")) if client_caps else set()
        our_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
        negotiated = ",".join(sorted(client_features & our_features))
        return (None, negotiated)

    def _check_circuit_and_quorum(self, job_id: str) -> JobAck | None:
        """Check circuit breaker and quorum. Returns rejection JobAck if unavailable."""
        if self._quorum_circuit.circuit_state == CircuitState.OPEN:
            retry_after = self._quorum_circuit.half_open_after
            return JobAck(
                job_id=job_id,
                accepted=False,
                error=f"Circuit open, retry after {retry_after}s",
            )

        if self._state.get_active_peer_count() > 0 and not self._has_quorum_available():
            return JobAck(job_id=job_id, accepted=False, error="Quorum unavailable")
        return None

    def _setup_job_tracking(
        self, submission: JobSubmission, primary_dcs: list[str]
    ) -> None:
        """Initialize job tracking state for a new submission."""
        job = GlobalJobStatus(
            job_id=submission.job_id,
            status=JobStatus.SUBMITTED.value,
            datacenters=[],
            timestamp=time.monotonic(),
        )
        self._job_manager.set_job(submission.job_id, job)
        self._job_manager.set_target_dcs(submission.job_id, set(primary_dcs))

        try:
            workflows = cloudpickle.loads(submission.workflows)
            self._state._job_workflow_ids[submission.job_id] = {
                wf_id for wf_id, _, _ in workflows
            }
        except Exception as workflow_parse_error:
            self._state._job_workflow_ids[submission.job_id] = set()
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Failed to parse workflows for job {submission.job_id}: {workflow_parse_error}",
                    node_host="",
                    node_port=0,
                    node_id="",
                ),
            )

        if submission.callback_addr:
            self._job_manager.set_callback(submission.job_id, submission.callback_addr)
            self._state._progress_callbacks[submission.job_id] = (
                submission.callback_addr
            )

        if submission.reporting_configs:
            self._state._job_submissions[submission.job_id] = submission

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
        client_id = f"{addr[0]}:{addr[1]}"

        # Validate rate limit and load (AD-22, AD-24)
        if rejection := await self._check_rate_and_load(client_id, submission.job_id):
            return rejection

        # Validate protocol version (AD-25)
        rejection, negotiated = self._check_protocol_version(submission)
        if rejection:
            return rejection

        # Check circuit breaker and quorum
        if rejection := self._check_circuit_and_quorum(submission.job_id):
            return rejection

        # Select datacenters (AD-36)
        primary_dcs, _, worst_health = self._select_datacenters(
            submission.datacenter_count,
            submission.datacenters if submission.datacenters else None,
            job_id=submission.job_id,
        )

        if worst_health == "initializing":
            return JobAck(
                job_id=submission.job_id, accepted=False, error="initializing"
            )
        if not primary_dcs:
            return JobAck(
                job_id=submission.job_id,
                accepted=False,
                error="No available datacenters",
            )

        # Setup job tracking
        self._setup_job_tracking(submission, primary_dcs)

        # Assume and broadcast leadership
        self._assume_leadership(submission.job_id, len(primary_dcs))
        await self._broadcast_leadership(submission.job_id, len(primary_dcs))
        self._quorum_circuit.record_success()

        # Dispatch in background
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
