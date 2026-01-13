"""
Gate Job Manager - Thread-safe job state management for gates.

This class encapsulates all job-related state and operations at the gate level
with proper synchronization using per-job locks. It provides race-condition safe
access to job data structures.

Key responsibilities:
- Job lifecycle management (submission tracking, status aggregation, completion)
- Per-datacenter result aggregation
- Client callback registration
- Per-job locking for concurrent access safety
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from hyperscale.distributed.models import (
    GlobalJobStatus,
    JobFinalResult,
    JobStatus,
)


class GateJobManager:
    """
    Thread-safe job state management for gates.

    Uses per-job locks to ensure race-condition safe access to job state.
    All operations that modify job state acquire the appropriate lock.

    Example usage:
        async with job_manager.lock_job(job_id):
            job = job_manager.get_job(job_id)
            if job:
                job.status = JobStatus.COMPLETED.value
                job_manager.update_job(job_id, job)
    """

    def __init__(self):
        """Initialize GateJobManager."""
        # Main job storage - job_id -> GlobalJobStatus
        self._jobs: dict[str, GlobalJobStatus] = {}

        # Per-DC final results for job completion aggregation
        # job_id -> {datacenter_id -> JobFinalResult}
        self._job_dc_results: dict[str, dict[str, JobFinalResult]] = {}

        # Track which DCs were assigned for each job (to know when complete)
        # job_id -> set of datacenter IDs
        self._job_target_dcs: dict[str, set[str]] = {}

        # Client push notification callbacks
        # job_id -> callback address for push notifications
        self._job_callbacks: dict[str, tuple[str, int]] = {}

        # Per-job fence token tracking for rejecting stale updates
        # job_id -> highest fence_token seen for this job
        self._job_fence_tokens: dict[str, int] = {}

        # Per-job locks for concurrent access safety
        self._job_locks: dict[str, asyncio.Lock] = {}

        # Global lock for job creation/deletion operations
        self._global_lock = asyncio.Lock()

    @asynccontextmanager
    async def lock_job(self, job_id: str) -> AsyncIterator[None]:
        lock = self._job_locks.setdefault(job_id, asyncio.Lock())
        async with lock:
            yield

    async def lock_global(self) -> asyncio.Lock:
        """
        Get the global lock for job creation/deletion.

        Use this when creating or deleting jobs to prevent races.
        """
        return self._global_lock

    # =========================================================================
    # Job CRUD Operations
    # =========================================================================

    def get_job(self, job_id: str) -> GlobalJobStatus | None:
        """
        Get job status. Caller should hold the job lock for modifications.
        """
        return self._jobs.get(job_id)

    def set_job(self, job_id: str, job: GlobalJobStatus) -> None:
        """
        Set job status. Caller should hold the job lock.
        """
        self._jobs[job_id] = job

    def delete_job(self, job_id: str) -> GlobalJobStatus | None:
        """
        Delete a job and all associated data. Caller should hold global lock.

        Returns the deleted job if it existed, None otherwise.
        """
        job = self._jobs.pop(job_id, None)
        self._job_dc_results.pop(job_id, None)
        self._job_target_dcs.pop(job_id, None)
        self._job_callbacks.pop(job_id, None)
        self._job_fence_tokens.pop(job_id, None)
        # Don't delete the lock - it may still be in use
        return job

    def has_job(self, job_id: str) -> bool:
        """Check if a job exists."""
        return job_id in self._jobs

    def get_all_job_ids(self) -> list[str]:
        """Get all job IDs."""
        return list(self._jobs.keys())

    def get_all_jobs(self) -> dict[str, GlobalJobStatus]:
        """Get a copy of all jobs for snapshotting."""
        return dict(self._jobs)

    def job_count(self) -> int:
        """Get the number of tracked jobs."""
        return len(self._jobs)

    def items(self):
        """Iterate over job_id, job pairs."""
        return self._jobs.items()

    def get_running_jobs(self) -> list[tuple[str, GlobalJobStatus]]:
        return [
            (job_id, job)
            for job_id, job in self._jobs.items()
            if job.status == JobStatus.RUNNING.value
        ]

    # =========================================================================
    # Target DC Management
    # =========================================================================

    def set_target_dcs(self, job_id: str, dcs: set[str]) -> None:
        """Set the target datacenters for a job."""
        self._job_target_dcs[job_id] = dcs

    def get_target_dcs(self, job_id: str) -> set[str]:
        """Get the target datacenters for a job."""
        return self._job_target_dcs.get(job_id, set())

    def add_target_dc(self, job_id: str, dc_id: str) -> None:
        """Add a target datacenter to a job."""
        if job_id not in self._job_target_dcs:
            self._job_target_dcs[job_id] = set()
        self._job_target_dcs[job_id].add(dc_id)

    # =========================================================================
    # DC Results Management
    # =========================================================================

    def set_dc_result(self, job_id: str, dc_id: str, result: JobFinalResult) -> None:
        """Set the final result from a datacenter."""
        if job_id not in self._job_dc_results:
            self._job_dc_results[job_id] = {}
        self._job_dc_results[job_id][dc_id] = result

    def get_dc_result(self, job_id: str, dc_id: str) -> JobFinalResult | None:
        """Get the final result from a datacenter."""
        return self._job_dc_results.get(job_id, {}).get(dc_id)

    def get_all_dc_results(self, job_id: str) -> dict[str, JobFinalResult]:
        """Get all datacenter results for a job."""
        return self._job_dc_results.get(job_id, {})

    def get_completed_dc_count(self, job_id: str) -> int:
        """Get the number of datacenters that have reported results."""
        return len(self._job_dc_results.get(job_id, {}))

    def all_dcs_reported(self, job_id: str) -> bool:
        """Check if all target datacenters have reported results."""
        target_dcs = self._job_target_dcs.get(job_id, set())
        reported_dcs = set(self._job_dc_results.get(job_id, {}).keys())
        return target_dcs == reported_dcs and len(target_dcs) > 0

    # =========================================================================
    # Callback Management
    # =========================================================================

    def set_callback(self, job_id: str, addr: tuple[str, int]) -> None:
        """Set the callback address for a job."""
        self._job_callbacks[job_id] = addr

    def get_callback(self, job_id: str) -> tuple[str, int] | None:
        """Get the callback address for a job."""
        return self._job_callbacks.get(job_id)

    def remove_callback(self, job_id: str) -> tuple[str, int] | None:
        """Remove and return the callback address for a job."""
        return self._job_callbacks.pop(job_id, None)

    def has_callback(self, job_id: str) -> bool:
        """Check if a job has a callback registered."""
        return job_id in self._job_callbacks

    # =========================================================================
    # Fence Token Management
    # =========================================================================

    def get_fence_token(self, job_id: str) -> int:
        """Get the current fence token for a job."""
        return self._job_fence_tokens.get(job_id, 0)

    def set_fence_token(self, job_id: str, token: int) -> None:
        """Set the fence token for a job."""
        self._job_fence_tokens[job_id] = token

    async def update_fence_token_if_higher(self, job_id: str, token: int) -> bool:
        """
        Update fence token only if new token is higher.

        Returns True if token was updated, False if rejected as stale.
        Uses per-job lock to ensure atomicity.
        """
        async with self.lock_job(job_id):
            current = self._job_fence_tokens.get(job_id, 0)
            if token > current:
                self._job_fence_tokens[job_id] = token
                return True
            return False

    # =========================================================================
    # Aggregation Helpers
    # =========================================================================

    def aggregate_job_status(self, job_id: str) -> GlobalJobStatus | None:
        """
        Aggregate status across all datacenters for a job.

        Returns updated GlobalJobStatus or None if job doesn't exist.
        Caller should hold the job lock.
        """
        job = self._jobs.get(job_id)
        if not job:
            return None

        dc_results = self._job_dc_results.get(job_id, {})
        target_dcs = self._job_target_dcs.get(job_id, set())

        # Aggregate totals
        total_completed = 0
        total_failed = 0
        completed_dcs = 0
        failed_dcs = 0
        rates: list[float] = []

        for dc_id, result in dc_results.items():
            total_completed += result.total_completed
            total_failed += result.total_failed

            if result.status == JobStatus.COMPLETED.value:
                completed_dcs += 1
            elif result.status == JobStatus.FAILED.value:
                failed_dcs += 1

            if hasattr(result, "rate") and result.rate > 0:
                rates.append(result.rate)

        # Update job with aggregated values
        job.total_completed = total_completed
        job.total_failed = total_failed
        job.completed_datacenters = completed_dcs
        job.failed_datacenters = failed_dcs
        job.overall_rate = sum(rates) if rates else 0.0

        # Calculate elapsed time
        if job.timestamp > 0:
            job.elapsed_seconds = time.monotonic() - job.timestamp

        # Determine overall status
        if len(dc_results) == len(target_dcs) and len(target_dcs) > 0:
            # All DCs have reported
            if failed_dcs == len(target_dcs):
                job.status = JobStatus.FAILED.value
            elif completed_dcs == len(target_dcs):
                job.status = JobStatus.COMPLETED.value
            else:
                # Mixed results - some completed, some failed
                job.status = JobStatus.COMPLETED.value  # Partial success

        return job

    # =========================================================================
    # Cleanup
    # =========================================================================

    def cleanup_old_jobs(self, max_age_seconds: float) -> list[str]:
        """
        Remove jobs older than max_age_seconds that are in terminal state.

        Returns list of cleaned up job IDs.
        Note: Caller should be careful about locking - this iterates all jobs.
        """
        now = time.monotonic()
        terminal_statuses = {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
        }
        to_remove: list[str] = []

        for job_id, job in list(self._jobs.items()):
            if job.status in terminal_statuses:
                age = now - job.timestamp
                if age > max_age_seconds:
                    to_remove.append(job_id)

        for job_id in to_remove:
            self.delete_job(job_id)

        return to_remove

    def cleanup_job_lock(self, job_id: str) -> None:
        """
        Remove the lock for a deleted job to prevent memory leaks.

        Only call this after the job has been deleted and you're sure
        no other coroutines are waiting on the lock.
        """
        self._job_locks.pop(job_id, None)
