"""``LocalAuthorityTimeout`` -- pickled under the namespace
``hyperscale.distributed.jobs.timeout_strategy`` (see that module)."""

from typing import TYPE_CHECKING
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning
from hyperscale.distributed.models.distributed import JobStatus
from hyperscale.distributed.models.jobs import JobInfo, TimeoutTrackingState
from hyperscale.distributed.workflow import WorkflowState

from .timeout_strategy_shared import _DEFAULT_CLOCK
from .timeout_strategy_base import TimeoutStrategy

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager import ManagerServer


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
            now = _DEFAULT_CLOCK.monotonic()
            job.timeout_tracking = TimeoutTrackingState(
                strategy_type="local_authority",
                gate_addr=None,
                started_at=now,
                last_progress_at=now,
                last_report_at=now,
                timeout_seconds=timeout_seconds,
                stuck_threshold=self._manager.env.JOB_STUCK_THRESHOLD,
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
        Check for timeout. Idempotent - safe to call repeatedly.

        Only times out once (checked via locally_timed_out flag).
        """
        # Idempotent: already timed out
        if (
            job := self._tracked_job(job_id)
        ) is None or job.timeout_tracking.locally_timed_out:
            return False, ""

        return await self._check_active_job(job_id, job)

    def _tracked_job(self, job_id: str) -> JobInfo | None:
        """The job while it exists and carries timeout tracking, else None."""
        job = self._manager._job_manager.get_job_by_id(job_id)
        if not job or not job.timeout_tracking:
            return None
        return job

    async def _check_active_job(self, job_id: str, job: JobInfo) -> tuple[bool, str]:
        """Time out a non-terminal job past its extended timeout, else check whether it is stuck."""
        # Check terminal state (race protection)
        if job.status in {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }:
            return False, ""

        now = _DEFAULT_CLOCK.monotonic()
        tracking = job.timeout_tracking

        # Calculate effective timeout with extensions
        effective_timeout = tracking.timeout_seconds + tracking.total_extensions_granted

        # Check overall timeout (with extensions)
        elapsed = now - tracking.started_at
        if elapsed > effective_timeout:
            return await self._time_out_exceeded(
                job_id, job, tracking, elapsed, effective_timeout
            )

        return await self._check_stuck(job_id, job, tracking, now)

    async def _time_out_exceeded(
        self,
        job_id: str,
        job: JobInfo,
        tracking: TimeoutTrackingState,
        elapsed: float,
        effective_timeout: float,
    ) -> tuple[bool, str]:
        """Mark the job timed out (once) for exceeding its extended timeout, then time it out."""
        async with job.lock:
            tracking.locally_timed_out = True
            tracking.timeout_reason = (
                f"Job timeout exceeded ({elapsed:.1f}s > {effective_timeout:.1f}s, "
                f"base={tracking.timeout_seconds:.1f}s + "
                f"extensions={tracking.total_extensions_granted:.1f}s)"
            )

        await self._manager._timeout_job(job_id, tracking.timeout_reason)
        return True, tracking.timeout_reason

    async def _check_stuck(
        self,
        job_id: str,
        job: JobInfo,
        tracking: TimeoutTrackingState,
        now: float,
    ) -> tuple[bool, str]:
        """Time out a job with work in flight and no progress past the extended stuck threshold."""
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
            return await self._time_out_stuck(
                job_id, job, tracking, time_since_progress, time_since_extension
            )

        return False, ""

    async def _time_out_stuck(
        self,
        job_id: str,
        job: JobInfo,
        tracking: TimeoutTrackingState,
        time_since_progress: float,
        time_since_extension: float,
    ) -> tuple[bool, str]:
        """Mark the job timed out (once) as stuck, then time it out."""
        async with job.lock:
            tracking.locally_timed_out = True
            tracking.timeout_reason = (
                f"Job stuck (no progress for {time_since_progress:.1f}s, "
                f"no extensions for {time_since_extension:.1f}s)"
            )

        await self._manager._timeout_job(job_id, tracking.timeout_reason)
        return True, tracking.timeout_reason

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
            tracking.last_extension_at = _DEFAULT_CLOCK.monotonic()
            tracking.active_workers_with_extensions.add(worker_id)

            # The grant buys its own seconds of tolerated silence (AD-26's
            # log-decaying grants), on top of the stuck threshold.
            tracking.extension_seconds_since_progress += extension_seconds

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
