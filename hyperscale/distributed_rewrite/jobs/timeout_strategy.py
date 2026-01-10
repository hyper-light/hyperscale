"""
Job timeout strategies with multi-DC coordination (AD-34).

Provides adaptive timeout detection that auto-detects deployment topology:
- LocalAuthorityTimeout: Single-DC deployments (manager has full authority)
- GateCoordinatedTimeout: Multi-DC deployments (gate coordinates globally)

Integrates with AD-26 healthcheck extensions to respect legitimate long-running work.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerInfo,
    ServerWarning,
)
from hyperscale.distributed_rewrite.models.distributed import (
    JobFinalStatus,
    JobProgressReport,
    JobStatus,
    JobTimeoutReport,
)
from hyperscale.distributed_rewrite.models.jobs import TimeoutTrackingState

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager import ManagerServer


class TimeoutStrategy(ABC):
    """
    Base timeout strategy with lifecycle management (AD-34).

    Subclasses implement either local authority (single-DC) or gate coordination
    (multi-DC) timeout detection and reporting.
    """

    @abstractmethod
    async def start_tracking(
        self,
        job_id: str,
        timeout_seconds: float,
        gate_addr: tuple[str, int] | None = None,
    ) -> None:
        """
        Start tracking timeout for a job.

        Called when job is submitted. Initializes TimeoutTrackingState in JobInfo.

        Args:
            job_id: Job to track
            timeout_seconds: Job timeout in seconds
            gate_addr: Gate address for multi-DC (None for single-DC)
        """
        pass

    @abstractmethod
    async def resume_tracking(self, job_id: str) -> None:
        """
        Resume tracking after leader transfer.

        CRITICAL: New leader calls this to continue timeout tracking.
        Reconstructs strategy state from JobInfo.timeout_tracking.

        Increments fence token to prevent stale timeout decisions.

        Args:
            job_id: Job to resume tracking
        """
        pass

    @abstractmethod
    async def report_progress(self, job_id: str, progress_type: str) -> None:
        """
        Record workflow progress event.

        Updates last_progress_at timestamp. Progress types include:
        - Workflow state transitions (e.g., "workflow_running", "workflow_completed")
        - Worker extension grants (automatically called, updates last_progress_at)

        Args:
            job_id: Job that made progress
            progress_type: Type of progress event
        """
        pass

    @abstractmethod
    async def check_timeout(self, job_id: str) -> tuple[bool, str]:
        """
        Check if job timed out.

        Returns (is_timed_out, reason).
        Idempotent - safe to call multiple times.

        Checks:
        1. Overall timeout: elapsed > effective_timeout (base + extensions)
        2. Stuck detection: no progress for stuck_threshold (120s)

        Args:
            job_id: Job to check

        Returns:
            (is_timed_out, reason) tuple
        """
        pass

    @abstractmethod
    async def handle_global_timeout(
        self, job_id: str, reason: str, fence_token: int
    ) -> bool:
        """
        Handle global timeout decision from gate.

        Validates fence token to reject stale decisions after leader transfers.

        Args:
            job_id: Job that timed out
            reason: Why gate declared timeout
            fence_token: Gate's fence token

        Returns:
            True if accepted, False if rejected (stale)
        """
        pass

    @abstractmethod
    async def record_worker_extension(
        self,
        job_id: str,
        worker_id: str,
        extension_seconds: float,
        worker_progress: float,
    ) -> None:
        """
        Record that a worker was granted an extension (AD-26 integration).

        This adjusts the job's effective timeout to account for legitimate
        long-running work. Extension also counts as progress (updates last_progress_at).

        Args:
            job_id: Job the worker is executing
            worker_id: Worker that received extension
            extension_seconds: Seconds granted
            worker_progress: Progress metric that justified extension
        """
        pass

    @abstractmethod
    async def stop_tracking(self, job_id: str, reason: str) -> None:
        """
        Stop tracking timeout for a job.

        Called when job reaches terminal state (completed, failed, cancelled, timed out).
        Must be idempotent - safe to call multiple times.

        Args:
            job_id: Job to stop tracking
            reason: Why tracking stopped (e.g., "completed", "cancelled", "timed_out")
        """
        pass

    @abstractmethod
    async def cleanup_worker_extensions(self, job_id: str, worker_id: str) -> None:
        """
        Clean up extension tracking for a failed/removed worker.

        Called when worker dies or is removed from job.
        Removes worker from active_workers_with_extensions.

        Args:
            job_id: Job ID
            worker_id: Worker to remove from extension tracking
        """
        pass


class LocalAuthorityTimeout(TimeoutStrategy):
    """
    Manager has full authority (single-DC deployment) (AD-34 Part 3).

    Fault Tolerance:
    - State in JobInfo.timeout_tracking (survives leader transfer)
    - New leader calls resume_tracking() to continue
    - Idempotent timeout marking (won't double-timeout)

    Extension Integration (AD-26):
    - Extension grants update effective_timeout = base + total_extensions
    - Extension grant = progress signal (updates last_progress_at)
    - Not stuck if extension granted within stuck_threshold
    """

    def __init__(self, manager: "ManagerServer"):
        self._manager = manager

    async def start_tracking(
        self,
        job_id: str,
        timeout_seconds: float,
        gate_addr: tuple[str, int] | None = None,
    ) -> None:
        """Initialize timeout tracking state in JobInfo."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job:
            return

        async with job.lock:
            now = time.monotonic()
            job.timeout_tracking = TimeoutTrackingState(
                strategy_type="local_authority",
                gate_addr=None,
                started_at=now,
                last_progress_at=now,
                last_report_at=now,
                timeout_seconds=timeout_seconds,
                timeout_fence_token=0,
            )

    async def resume_tracking(self, job_id: str) -> None:
        """
        Resume after leader transfer.

        State already in JobInfo - just increment fence token.
        """
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            await self._manager._udp_logger.log(
                ServerWarning(
                    message=f"Cannot resume timeout tracking for {job_id} - no state",
                    node_host=self._manager._host,
                    node_port=self._manager._tcp_port,
                    node_id=self._manager._node_id.short,
                )
            )
            return

        # Increment fence token (prevents stale operations)
        async with job.lock:
            job.timeout_tracking.timeout_fence_token += 1

        await self._manager._udp_logger.log(
            ServerDebug(
                message=f"Resumed timeout tracking for {job_id} (fence={job.timeout_tracking.timeout_fence_token})",
                node_host=self._manager._host,
                node_port=self._manager._tcp_port,
                node_id=self._manager._node_id.short,
            )
        )

    async def report_progress(self, job_id: str, progress_type: str) -> None:
        """Update last_progress_at timestamp."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        async with job.lock:
            job.timeout_tracking.last_progress_at = time.monotonic()

    async def check_timeout(self, job_id: str) -> tuple[bool, str]:
        """
        Check for timeout. Idempotent - safe to call repeatedly.

        Only times out once (checked via locally_timed_out flag).
        """
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return False, ""

        # Idempotent: already timed out
        if job.timeout_tracking.locally_timed_out:
            return False, ""

        # Check terminal state (race protection)
        if job.status in {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }:
            return False, ""

        now = time.monotonic()
        tracking = job.timeout_tracking

        # Calculate effective timeout with extensions
        effective_timeout = tracking.timeout_seconds + tracking.total_extensions_granted

        # Check overall timeout (with extensions)
        elapsed = now - tracking.started_at
        if elapsed > effective_timeout:
            async with job.lock:
                tracking.locally_timed_out = True
                tracking.timeout_reason = (
                    f"Job timeout exceeded ({elapsed:.1f}s > {effective_timeout:.1f}s, "
                    f"base={tracking.timeout_seconds:.1f}s + "
                    f"extensions={tracking.total_extensions_granted:.1f}s)"
                )

            await self._manager._timeout_job(job_id, tracking.timeout_reason)
            return True, tracking.timeout_reason

        # Check for stuck (no progress AND no recent extensions)
        time_since_progress = now - tracking.last_progress_at
        time_since_extension = (
            now - tracking.last_extension_at
            if tracking.last_extension_at > 0
            else float("inf")
        )

        # If extensions granted recently, not stuck
        if time_since_extension < tracking.stuck_threshold:
            return False, ""

        # Otherwise check progress-based stuck detection
        if time_since_progress > tracking.stuck_threshold:
            async with job.lock:
                tracking.locally_timed_out = True
                tracking.timeout_reason = (
                    f"Job stuck (no progress for {time_since_progress:.1f}s, "
                    f"no extensions for {time_since_extension:.1f}s)"
                )

            await self._manager._timeout_job(job_id, tracking.timeout_reason)
            return True, tracking.timeout_reason

        return False, ""

    async def handle_global_timeout(
        self, job_id: str, reason: str, fence_token: int
    ) -> bool:
        """Not applicable for local authority."""
        return False

    async def record_worker_extension(
        self,
        job_id: str,
        worker_id: str,
        extension_seconds: float,
        worker_progress: float,
    ) -> None:
        """
        Record that a worker was granted an extension.

        This adjusts the job's effective timeout to account for
        legitimate long-running work.
        """
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        async with job.lock:
            tracking = job.timeout_tracking

            # Update extension tracking
            tracking.total_extensions_granted += extension_seconds
            tracking.max_worker_extension = max(
                tracking.max_worker_extension, extension_seconds
            )
            tracking.last_extension_at = time.monotonic()
            tracking.active_workers_with_extensions.add(worker_id)

            # Extension = progress! Update last_progress_at
            tracking.last_progress_at = time.monotonic()

        await self._manager._udp_logger.log(
            ServerDebug(
                message=f"Job {job_id} timeout extended by {extension_seconds:.1f}s "
                f"(worker {worker_id} progress={worker_progress:.2f})",
                node_host=self._manager._host,
                node_port=self._manager._tcp_port,
                node_id=self._manager._node_id.short,
            )
        )

    async def stop_tracking(self, job_id: str, reason: str) -> None:
        """
        Stop timeout tracking for job.

        Idempotent - safe to call multiple times.
        """
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        async with job.lock:
            # Mark as stopped to prevent further timeout checks
            job.timeout_tracking.locally_timed_out = True
            job.timeout_tracking.timeout_reason = f"Tracking stopped: {reason}"

        await self._manager._udp_logger.log(
            ServerDebug(
                message=f"Stopped timeout tracking for job {job_id}: {reason}",
                node_host=self._manager._host,
                node_port=self._manager._tcp_port,
                node_id=self._manager._node_id.short,
            )
        )

    async def cleanup_worker_extensions(self, job_id: str, worker_id: str) -> None:
        """Remove failed worker from extension tracking."""
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
            now = time.monotonic()
            job.timeout_tracking = TimeoutTrackingState(
                strategy_type="gate_coordinated",
                gate_addr=gate_addr,
                started_at=now,
                last_progress_at=now,
                last_report_at=now,
                timeout_seconds=timeout_seconds,
                timeout_fence_token=0,
            )

    async def resume_tracking(self, job_id: str) -> None:
        """Resume after leader transfer - notify gate."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        async with job.lock:
            job.timeout_tracking.timeout_fence_token += 1
            fence_token = job.timeout_tracking.timeout_fence_token

        # Send leadership transfer notification to gate
        await self._send_leader_transfer_report(job_id, fence_token)

        await self._manager._udp_logger.log(
            ServerDebug(
                message=f"Resumed gate-coordinated timeout tracking for {job_id} (fence={fence_token})",
                node_host=self._manager._host,
                node_port=self._manager._tcp_port,
                node_id=self._manager._node_id.short,
            )
        )

    async def report_progress(self, job_id: str, progress_type: str) -> None:
        """Update progress timestamp."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        async with job.lock:
            job.timeout_tracking.last_progress_at = time.monotonic()

    async def check_timeout(self, job_id: str) -> tuple[bool, str]:
        """
        Check DC-local timeout and report to gate.

        Does NOT mark job failed locally - waits for gate decision.
        Fallback: if can't reach gate for 5+ minutes, timeout locally.
        """
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return False, ""

        tracking = job.timeout_tracking

        # Already reported, waiting for gate decision
        if tracking.locally_timed_out:
            # Fallback: gate unresponsive for 5+ minutes
            if not tracking.globally_timed_out:
                time_since_report = time.monotonic() - tracking.last_report_at
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

        # Check terminal state (race protection)
        if job.status in {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }:
            return False, ""

        now = time.monotonic()

        # Send periodic progress reports
        if now - tracking.last_report_at > 10.0:
            await self._send_progress_report(job_id)
            async with job.lock:
                tracking.last_report_at = now

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
            await self._send_timeout_report(job_id, reason)

            async with job.lock:
                tracking.locally_timed_out = True
                tracking.timeout_reason = reason
                tracking.last_report_at = now

            return True, reason

        # Check for stuck (no progress AND no recent extensions)
        time_since_progress = now - tracking.last_progress_at
        time_since_extension = (
            now - tracking.last_extension_at
            if tracking.last_extension_at > 0
            else float("inf")
        )

        # Not stuck if extensions granted recently
        if time_since_extension < tracking.stuck_threshold:
            return False, ""

        if time_since_progress > tracking.stuck_threshold:
            reason = (
                f"DC-local stuck (no progress for {time_since_progress:.1f}s, "
                f"no extensions for {time_since_extension:.1f}s)"
            )
            await self._send_timeout_report(job_id, reason)

            async with job.lock:
                tracking.locally_timed_out = True
                tracking.timeout_reason = reason
                tracking.last_report_at = now

            return True, reason

        return False, ""

    async def handle_global_timeout(
        self, job_id: str, reason: str, fence_token: int
    ) -> bool:
        """
        Handle global timeout from gate.

        Validates fence token to reject stale decisions.
        """
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
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
            tracking.last_extension_at = time.monotonic()
            tracking.last_progress_at = time.monotonic()
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
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
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

    async def _send_progress_report(self, job_id: str) -> None:
        """Send progress to gate (best-effort, loss tolerated)."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        report = JobProgressReport(
            job_id=job_id,
            datacenter=self._manager._datacenter,
            manager_id=self._manager._node_id.short,
            manager_host=self._manager._host,
            manager_port=self._manager._tcp_port,
            workflows_total=job.workflows_total,
            workflows_completed=job.workflows_completed,
            workflows_failed=job.workflows_failed,
            has_recent_progress=(
                time.monotonic() - job.timeout_tracking.last_progress_at < 10.0
            ),
            timestamp=time.monotonic(),
            fence_token=job.timeout_tracking.timeout_fence_token,
            # Extension info
            total_extensions_granted=job.timeout_tracking.total_extensions_granted,
            max_worker_extension=job.timeout_tracking.max_worker_extension,
            workers_with_extensions=len(
                job.timeout_tracking.active_workers_with_extensions
            ),
        )

        try:
            await self._manager.send_tcp(
                job.timeout_tracking.gate_addr, "job_progress_report", report.dump()
            )
        except Exception as error:
            # Progress report failure is non-critical
            await self._manager._udp_logger.log(
                ServerDebug(
                    message=f"Failed to send progress report for {job_id}: {error}",
                    node_host=self._manager._host,
                    node_port=self._manager._tcp_port,
                    node_id=self._manager._node_id.short,
                )
            )

    async def _send_timeout_report(self, job_id: str, reason: str) -> None:
        """Send timeout report to gate (persistent until ACK'd)."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        report = JobTimeoutReport(
            job_id=job_id,
            datacenter=self._manager._datacenter,
            manager_id=self._manager._node_id.short,
            manager_host=self._manager._host,
            manager_port=self._manager._tcp_port,
            reason=reason,
            elapsed_seconds=time.monotonic() - job.timeout_tracking.started_at,
            fence_token=job.timeout_tracking.timeout_fence_token,
        )

        # Store for retry (in production, this would be persisted)
        async with self._report_lock:
            if job_id not in self._pending_reports:
                self._pending_reports[job_id] = []
            self._pending_reports[job_id].append(report)

        try:
            await self._manager.send_tcp(
                job.timeout_tracking.gate_addr, "job_timeout_report", report.dump()
            )
            # Success - remove from pending
            async with self._report_lock:
                self._pending_reports.pop(job_id, None)
        except Exception as error:
            await self._manager._udp_logger.log(
                ServerWarning(
                    message=f"Failed to send timeout report for {job_id}: {error} (will retry)",
                    node_host=self._manager._host,
                    node_port=self._manager._tcp_port,
                    node_id=self._manager._node_id.short,
                )
            )

    async def _send_leader_transfer_report(
        self, job_id: str, fence_token: int
    ) -> None:
        """Notify gate of leader change."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        from hyperscale.distributed_rewrite.models.distributed import JobLeaderTransfer

        report = JobLeaderTransfer(
            job_id=job_id,
            datacenter=self._manager._datacenter,
            new_leader_id=self._manager._node_id.short,
            new_leader_host=self._manager._host,
            new_leader_port=self._manager._tcp_port,
            fence_token=fence_token,
        )

        try:
            await self._manager.send_tcp(
                job.timeout_tracking.gate_addr, "job_leader_transfer", report.dump()
            )
        except Exception as error:
            await self._manager._udp_logger.log(
                ServerWarning(
                    message=f"Failed to send leader transfer for {job_id}: {error}",
                    node_host=self._manager._host,
                    node_port=self._manager._tcp_port,
                    node_id=self._manager._node_id.short,
                )
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
            datacenter=self._manager._datacenter,
            manager_id=self._manager._node_id.short,
            status=status,
            timestamp=time.monotonic(),
            fence_token=job.timeout_tracking.timeout_fence_token,
        )

        try:
            await self._manager.send_tcp(
                job.timeout_tracking.gate_addr, "job_final_status", final_report.dump()
            )
        except Exception as error:
            # Best-effort cleanup notification
            await self._manager._udp_logger.log(
                ServerDebug(
                    message=f"Failed to send final status for {job_id}: {error}",
                    node_host=self._manager._host,
                    node_port=self._manager._tcp_port,
                    node_id=self._manager._node_id.short,
                )
            )

    async def _send_status_correction(self, job_id: str, status: str) -> None:
        """Send status correction when gate's timeout conflicts with actual state."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return

        correction = JobFinalStatus(
            job_id=job_id,
            datacenter=self._manager._datacenter,
            manager_id=self._manager._node_id.short,
            status=status,
            timestamp=time.monotonic(),
            fence_token=job.timeout_tracking.timeout_fence_token,
        )

        try:
            await self._manager.send_tcp(
                job.timeout_tracking.gate_addr, "job_final_status", correction.dump()
            )
        except Exception as error:
            await self._manager._udp_logger.log(
                ServerDebug(
                    message=f"Failed to send status correction for {job_id}: {error}",
                    node_host=self._manager._host,
                    node_port=self._manager._tcp_port,
                    node_id=self._manager._node_id.short,
                )
            )
