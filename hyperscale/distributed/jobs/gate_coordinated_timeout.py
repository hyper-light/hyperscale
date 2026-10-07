"""``GateCoordinatedTimeout`` -- pickled under the namespace
``hyperscale.distributed.jobs.timeout_strategy`` (see that module)."""

from typing import TYPE_CHECKING
import asyncio
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning
from hyperscale.distributed.models.distributed import JobFinalStatus, JobProgressReport, JobStatus, JobTimeoutReport
from hyperscale.distributed.models.jobs import JobInfo, TimeoutTrackingState
from hyperscale.distributed.workflow import WorkflowState
from hyperscale.distributed.models.distributed import JobLeaderTransfer

from .timeout_strategy_shared import _DEFAULT_CLOCK
from .timeout_strategy_base import TimeoutStrategy

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager import ManagerServer


class GateCoordinatedTimeout(TimeoutStrategy):
    """
    Gate has authority (multi-DC deployment) (AD-34 Part 4).

    Manager:
    - Detects DC-local timeouts/stuck state
    - Reports to gate (does not mark job failed locally)
    - Sends periodic progress reports
    - Waits for gate's global decision

    Fault Tolerance:
    - Progress reports are periodic (loss tolerated)
    - Timeout reports are persistent until ACK'd
    - Fallback to local timeout if gate unreachable for 5+ minutes

    Extension Integration (AD-26):
    - Extension info included in progress reports to gate
    - Gate uses extension data for global timeout decisions
    """

    def __init__(self, manager: "ManagerServer"):
        self._manager = manager
        self._pending_reports: dict[str, list[JobTimeoutReport]] = {}
        self._report_lock = asyncio.Lock()

    async def start_tracking(
        self,
        job_id: str,
        timeout_seconds: float,
        gate_addr: tuple[str, int] | None = None,
    ) -> None:
        """Initialize gate-coordinated tracking."""
        if not gate_addr:
            raise ValueError("Gate address required for gate-coordinated timeout")

        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job:
            return

        async with job.lock:
            now = _DEFAULT_CLOCK.monotonic()
            job.timeout_tracking = TimeoutTrackingState(
                strategy_type="gate_coordinated",
                gate_addr=gate_addr,
                started_at=now,
                last_progress_at=now,
                last_report_at=now,
                timeout_seconds=timeout_seconds,
                stuck_threshold=self._manager.env.JOB_STUCK_THRESHOLD,
                timeout_fence_token=0,
            )

    async def resume_tracking(self, job_id: str, fence_token: int) -> None:
        """Resume after leader transfer - advance the fence, notify gate."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        async with job.lock:
            job.timeout_tracking.timeout_fence_token = max(
                job.timeout_tracking.timeout_fence_token + 1, fence_token
            )
            resumed_fence_token = job.timeout_tracking.timeout_fence_token

        # Send leadership transfer notification to gate
        await self._send_leader_transfer_report(job_id, resumed_fence_token)

        await self._manager._udp_logger.log(
            ServerDebug(
                message=f"Resumed gate-coordinated timeout tracking for {job_id} (fence={resumed_fence_token})",
                node_host=self._manager._host,
                node_port=self._manager._tcp_port,
                node_id=self._manager._node_id.short,
            )
        )

    async def report_progress(self, job_id: str, progress_type: str) -> None:
        """Record progress: the stuck clock restarts, and so does the
        extension tolerance accrued since the last progress. Plain stores,
        so it never waits on the job lock (it runs on every advancing
        progress report)."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        job.timeout_tracking.last_progress_at = _DEFAULT_CLOCK.monotonic()
        job.timeout_tracking.extension_seconds_since_progress = 0.0

    async def check_timeout(self, job_id: str) -> tuple[bool, str]:
        """
        Check DC-local timeout and report to gate.

        Does NOT mark job failed locally - waits for gate decision.
        Fallback: if can't reach gate for 5+ minutes, timeout locally.
        """
        if (job := self._tracked_job(job_id)) is None:
            return False, ""

        tracking = job.timeout_tracking

        # Already reported, waiting for gate decision
        if tracking.locally_timed_out:
            return await self._check_gate_unresponsive(job_id, tracking)

        return await self._check_local_timeout(job_id, job, tracking)

    def _tracked_job(self, job_id: str) -> JobInfo | None:
        """The job while it exists and carries timeout tracking, else None."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return None
        return job

    async def _check_gate_unresponsive(
        self,
        job_id: str,
        tracking: TimeoutTrackingState,
    ) -> tuple[bool, str]:
        """After a local timeout report, time out locally once the gate is silent for 5+ minutes."""
        # Fallback: gate unresponsive for 5+ minutes
        if tracking.globally_timed_out:
            return False, ""

        time_since_report = _DEFAULT_CLOCK.monotonic() - tracking.last_report_at
        if time_since_report > 300.0:  # 5 minutes
            await self._manager._udp_logger.log(
                ServerWarning(
                    message=f"Gate unresponsive for {time_since_report:.0f}s, "
                    f"timing out job {job_id} locally",
                    node_host=self._manager._host,
                    node_port=self._manager._tcp_port,
                    node_id=self._manager._node_id.short,
                )
            )
            await self._manager._timeout_job(
                job_id, "Gate unresponsive, local timeout fallback"
            )
            return True, "gate_unresponsive_fallback"

        return False, ""

    async def _check_local_timeout(
        self,
        job_id: str,
        job: JobInfo,
        tracking: TimeoutTrackingState,
    ) -> tuple[bool, str]:
        """Report progress when due, then detect a DC-local timeout (extensions included) or stuck job."""
        # Check terminal state (race protection)
        if job.status in {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }:
            return False, ""

        now = _DEFAULT_CLOCK.monotonic()

        # Send periodic progress reports
        await self._send_periodic_progress(job_id, job, tracking, now)

        # Calculate effective timeout with extensions
        effective_timeout = tracking.timeout_seconds + tracking.total_extensions_granted

        # Check for DC-local timeout
        elapsed = now - tracking.started_at
        if elapsed > effective_timeout:
            reason = (
                f"DC-local timeout ({elapsed:.1f}s > {effective_timeout:.1f}s, "
                f"base={tracking.timeout_seconds:.1f}s + "
                f"extensions={tracking.total_extensions_granted:.1f}s)"
            )
            return await self._report_local_timeout(job_id, job, tracking, reason, now)

        return await self._check_stuck(job_id, job, tracking, now)

    async def _send_periodic_progress(
        self,
        job_id: str,
        job: JobInfo,
        tracking: TimeoutTrackingState,
        now: float,
    ) -> None:
        """Send a progress report to the gate when 10s have passed since the last report."""
        if now - tracking.last_report_at > 10.0:
            await self._send_progress_report(job_id)
            async with job.lock:
                tracking.last_report_at = now

    async def _report_local_timeout(
        self,
        job_id: str,
        job: JobInfo,
        tracking: TimeoutTrackingState,
        reason: str,
        now: float,
    ) -> tuple[bool, str]:
        """Report a DC-local timeout to the gate and mark it reported (gate decides globally)."""
        await self._send_timeout_report(job_id, reason)

        async with job.lock:
            tracking.locally_timed_out = True
            tracking.timeout_reason = reason
            tracking.last_report_at = now

        return True, reason

    async def _check_stuck(
        self,
        job_id: str,
        job: JobInfo,
        tracking: TimeoutTrackingState,
        now: float,
    ) -> tuple[bool, str]:
        """Detect a stuck job: work in flight with no progress past the extended threshold."""
        # Check for stuck (no progress AND no recent extensions)
        time_since_progress = now - tracking.last_progress_at
        time_since_extension = self._time_since_extension(tracking, now)

        # Only work in flight can be stuck: a job whose workflows all wait
        # for capacity or dependencies is waiting, bounded by its timeout.
        if not self._has_work_in_flight(job_id, job):
            return False, ""

        # Stuck: no progress for longer than the threshold plus every AD-26
        # extension granted since -- each grant, log-decaying, buys its own
        # seconds, so work its workers vouch for is not evicted too soon,
        # nor kept forever.
        if (
            time_since_progress
            > tracking.stuck_threshold + tracking.extension_seconds_since_progress
        ):
            reason = (
                f"DC-local stuck (no progress for {time_since_progress:.1f}s, "
                f"no extensions for {time_since_extension:.1f}s)"
            )
            return await self._report_local_timeout(job_id, job, tracking, reason, now)

        return False, ""

    @staticmethod
    def _time_since_extension(tracking: TimeoutTrackingState, now: float) -> float:
        """Seconds since the last AD-26 extension grant (infinite when none was granted)."""
        return (
            now - tracking.last_extension_at
            if tracking.last_extension_at > 0
            else float("inf")
        )

    def _has_work_in_flight(self, job_id: str, job: JobInfo) -> bool:
        """Whether any of the job's workflows is DISPATCHED or RUNNING."""
        lifecycle = self._manager._job_manager.workflow_lifecycle
        return any(
            lifecycle.get_state(job_id, workflow_info.token.workflow_id or "")
            in (WorkflowState.DISPATCHED, WorkflowState.RUNNING)
            for workflow_info in job.workflows.values()
        )

    async def handle_global_timeout(
        self, job_id: str, reason: str, fence_token: int
    ) -> bool:
        """
        Handle global timeout from gate.

        Validates fence token to reject stale decisions.
        """
        if (job := self._tracked_job(job_id)) is None:
            return False

        # Fence token validation (prevent stale decisions)
        if fence_token < job.timeout_tracking.timeout_fence_token:
            await self._manager._udp_logger.log(
                ServerWarning(
                    message=f"Rejected stale global timeout for {job_id} "
                    f"(fence {fence_token} < {job.timeout_tracking.timeout_fence_token})",
                    node_host=self._manager._host,
                    node_port=self._manager._tcp_port,
                    node_id=self._manager._node_id.short,
                )
            )
            return False

        return await self._apply_global_timeout(job_id, job, reason)

    async def _apply_global_timeout(self, job_id: str, job: JobInfo, reason: str) -> bool:
        """Accept the gate's (fence-valid) decision unless the job is already terminal (then correct the gate)."""
        # Check if already terminal
        if job.status in {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }:
            # Send correction to gate
            await self._send_status_correction(job_id, job.status)
            return False

        # Accept gate's decision
        async with job.lock:
            job.timeout_tracking.globally_timed_out = True
            job.timeout_tracking.timeout_reason = reason

        await self._manager._timeout_job(job_id, f"Global timeout: {reason}")
        return True

    async def record_worker_extension(
        self,
        job_id: str,
        worker_id: str,
        extension_seconds: float,
        worker_progress: float,
    ) -> None:
        """Record extension and update tracking (gate learns via progress reports)."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        async with job.lock:
            tracking = job.timeout_tracking
            tracking.total_extensions_granted += extension_seconds
            tracking.max_worker_extension = max(
                tracking.max_worker_extension, extension_seconds
            )
            tracking.last_extension_at = _DEFAULT_CLOCK.monotonic()
            # The grant buys its own seconds of tolerated silence (AD-26's
            # log-decaying grants), on top of the stuck threshold.
            tracking.extension_seconds_since_progress += extension_seconds
            tracking.active_workers_with_extensions.add(worker_id)

        # Gate will learn about extensions via next JobProgressReport

        await self._manager._udp_logger.log(
            ServerDebug(
                message=f"Job {job_id} timeout extended by {extension_seconds:.1f}s "
                f"(worker {worker_id} progress={worker_progress:.2f}, gate will be notified)",
                node_host=self._manager._host,
                node_port=self._manager._tcp_port,
                node_id=self._manager._node_id.short,
            )
        )

    async def stop_tracking(self, job_id: str, reason: str) -> None:
        """
        Stop tracking and notify gate.

        Sends final status update to gate so gate can clean up tracking.
        """
        if (job := self._tracked_job(job_id)) is None:
            return

        async with job.lock:
            job.timeout_tracking.locally_timed_out = True
            job.timeout_tracking.timeout_reason = f"Tracking stopped: {reason}"

        # Send final status to gate
        if job.timeout_tracking.gate_addr:
            await self._send_final_status(job_id, reason)

        await self._manager._udp_logger.log(
            ServerDebug(
                message=f"Stopped timeout tracking for job {job_id}: {reason}",
                node_host=self._manager._host,
                node_port=self._manager._tcp_port,
                node_id=self._manager._node_id.short,
            )
        )

    async def cleanup_worker_extensions(self, job_id: str, worker_id: str) -> None:
        """Remove failed worker (next progress report will reflect updated count)."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        async with job.lock:
            job.timeout_tracking.active_workers_with_extensions.discard(worker_id)

        await self._manager._udp_logger.log(
            ServerDebug(
                message=f"Cleaned up extensions for worker {worker_id} in job {job_id}",
                node_host=self._manager._host,
                node_port=self._manager._tcp_port,
                node_id=self._manager._node_id.short,
            )
        )

    # Helper methods for gate communication

    async def _send_to_gate(
        self,
        job_id: str,
        gate_addr: tuple[str, int],
        handler_name: str,
        payload: bytes,
        failure_log_model: type[ServerDebug] | type[ServerWarning],
    ) -> bool:
        """Send an AD-34 report to the job's gate; True once the gate
        acknowledged it.

        ``send_tcp`` returns a failure rather than raising, and the gate's
        handlers answer b"ok" only when they processed the report, so any
        other answer -- an error, no answer, an unknown handler -- is a
        failed delivery and is logged.
        """
        response, _ = await self._manager.send_tcp(gate_addr, handler_name, payload)
        if response == b"ok":
            return True

        await self._manager._udp_logger.log(
            failure_log_model(
                message=f"Gate {gate_addr} did not acknowledge {handler_name} for {job_id}: {response!r}",
                node_host=self._manager._host,
                node_port=self._manager._tcp_port,
                node_id=self._manager._node_id.short,
            )
        )
        return False

    async def _send_progress_report(self, job_id: str) -> None:
        """Send progress to gate (best-effort, loss tolerated)."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        report = JobProgressReport(
            job_id=job_id,
            datacenter=self._manager._node_id.datacenter,
            manager_id=self._manager._node_id.short,
            manager_host=self._manager._host,
            manager_port=self._manager._tcp_port,
            workflows_total=job.workflows_total,
            workflows_completed=job.workflows_completed,
            workflows_failed=job.workflows_failed,
            has_recent_progress=(
                _DEFAULT_CLOCK.monotonic() - job.timeout_tracking.last_progress_at < 10.0
            ),
            timestamp=_DEFAULT_CLOCK.monotonic(),
            fence_token=job.timeout_tracking.timeout_fence_token,
            # Extension info
            total_extensions_granted=job.timeout_tracking.total_extensions_granted,
            max_worker_extension=job.timeout_tracking.max_worker_extension,
            workers_with_extensions=len(
                job.timeout_tracking.active_workers_with_extensions
            ),
        )

        # Progress report failure is non-critical (the next one supersedes it)
        await self._send_to_gate(
            job_id,
            job.timeout_tracking.gate_addr,
            "receive_job_progress_report",
            report.dump(),
            ServerDebug,
        )

    async def _send_timeout_report(self, job_id: str, reason: str) -> None:
        """Send timeout report to gate (persistent until ACK'd)."""
        if (job := self._tracked_job(job_id)) is None:
            return

        report = JobTimeoutReport(
            job_id=job_id,
            datacenter=self._manager._node_id.datacenter,
            manager_id=self._manager._node_id.short,
            manager_host=self._manager._host,
            manager_port=self._manager._tcp_port,
            reason=reason,
            elapsed_seconds=_DEFAULT_CLOCK.monotonic() - job.timeout_tracking.started_at,
            fence_token=job.timeout_tracking.timeout_fence_token,
        )

        # Store for retry (in production, this would be persisted)
        async with self._report_lock:
            self._pending_reports.setdefault(job_id, []).append(report)

        # Pending until the gate acknowledges it (retried otherwise)
        if await self._send_to_gate(
            job_id,
            job.timeout_tracking.gate_addr,
            "receive_job_timeout_report",
            report.dump(),
            ServerWarning,
        ):
            async with self._report_lock:
                self._pending_reports.pop(job_id, None)

    async def _send_leader_transfer_report(
        self, job_id: str, fence_token: int
    ) -> None:
        """Notify gate of leader change."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return


        report = JobLeaderTransfer(
            job_id=job_id,
            datacenter=self._manager._node_id.datacenter,
            new_leader_id=self._manager._node_id.short,
            new_leader_host=self._manager._host,
            new_leader_port=self._manager._tcp_port,
            fence_token=fence_token,
        )

        await self._send_to_gate(
            job_id,
            job.timeout_tracking.gate_addr,
            "receive_job_leader_transfer",
            report.dump(),
            ServerWarning,
        )

    async def _send_final_status(self, job_id: str, reason: str) -> None:
        """Send final status to gate for cleanup."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        # Map reason to status
        status_map = {
            "completed": JobStatus.COMPLETED.value,
            "failed": JobStatus.FAILED.value,
            "cancelled": JobStatus.CANCELLED.value,
            "timed_out": JobStatus.TIMEOUT.value,
        }
        status = status_map.get(reason, JobStatus.FAILED.value)

        final_report = JobFinalStatus(
            job_id=job_id,
            datacenter=self._manager._node_id.datacenter,
            manager_id=self._manager._node_id.short,
            status=status,
            timestamp=_DEFAULT_CLOCK.monotonic(),
            fence_token=job.timeout_tracking.timeout_fence_token,
        )

        # Best-effort cleanup notification
        await self._send_to_gate(
            job_id,
            job.timeout_tracking.gate_addr,
            "receive_job_final_status",
            final_report.dump(),
            ServerDebug,
        )

    async def _send_status_correction(self, job_id: str, status: str) -> None:
        """Send status correction when gate's timeout conflicts with actual state."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        correction = JobFinalStatus(
            job_id=job_id,
            datacenter=self._manager._node_id.datacenter,
            manager_id=self._manager._node_id.short,
            status=status,
            timestamp=_DEFAULT_CLOCK.monotonic(),
            fence_token=job.timeout_tracking.timeout_fence_token,
        )

        await self._send_to_gate(
            job_id,
            job.timeout_tracking.gate_addr,
            "receive_job_final_status",
            correction.dump(),
            ServerDebug,
        )
