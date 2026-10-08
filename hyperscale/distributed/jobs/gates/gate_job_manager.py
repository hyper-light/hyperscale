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
import dataclasses
from contextlib import asynccontextmanager
from types import MappingProxyType
from typing import AsyncIterator

from hyperscale.distributed.models import (
    DatacenterSubstitution,
    GlobalJobStatus,
    JobFinalResult,
    JobStatus,
)

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

_TERMINAL_JOB_STATUSES = frozenset(
    {
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
        JobStatus.TIMEOUT.value,
    }
)

# A datacenter's reported final status, normalized: timeouts and both
# spellings of cancelled fold to one value; anything unrecognized is FAILED.
_NORMALIZED_FINAL_JOB_STATUSES = MappingProxyType(
    {
        "timeout": JobStatus.TIMEOUT.value,
        "timed_out": JobStatus.TIMEOUT.value,
        "cancelled": JobStatus.CANCELLED.value,
        "canceled": JobStatus.CANCELLED.value,
        JobStatus.COMPLETED.value: JobStatus.COMPLETED.value,
        JobStatus.FAILED.value: JobStatus.FAILED.value,
    }
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

        # The datacenters each job runs in -- those its final results are
        # awaited from: job_id -> set of datacenter IDs
        self._job_target_dcs: dict[str, set[str]] = {}

        # Datacenters each job lost while it ran there, each with the one
        # its unfinished workflows re-ran in (AD-36 Part 13):
        # job_id -> lost datacenter -> substitution
        self._job_datacenter_substitutions: dict[
            str, dict[str, DatacenterSubstitution]
        ] = {}

        # Datacenters each job no longer runs in that must stop running
        # it -- lost, or dispatched to without an answer: job_id -> set
        self._job_released_datacenters: dict[str, set[str]] = {}

        # Client push notification callbacks
        # job_id -> callback address for push notifications
        self._job_callbacks: dict[str, tuple[str, int]] = {}

        # Per-job fence token tracking for rejecting stale updates
        # job_id -> highest fence_token seen for this job
        self._job_fence_tokens: dict[str, int] = {}

        # When this gate first found each terminal job terminal (its
        # retention runs from there): job_id -> monotonic instant
        self._job_terminal_since: dict[str, float] = {}

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
        self._job_datacenter_substitutions.pop(job_id, None)
        self._job_released_datacenters.pop(job_id, None)
        self._job_callbacks.pop(job_id, None)
        self._job_fence_tokens.pop(job_id, None)
        self._job_terminal_since.pop(job_id, None)
        # Don't delete the lock - it may still be in use
        return job

    def terminal_since(self, job_id: str, now: float) -> float:
        """When this gate first found the terminal job ``job_id`` terminal
        -- ``now``, the first time it is asked. A job's retention runs from
        its end: its timestamp is its submission, so a job that ran longer
        than the retention was swept the moment it ended."""
        return self._job_terminal_since.setdefault(job_id, now)

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

    def move_target_dc(self, job_id: str, from_dc: str, to_dc: str) -> None:
        """Pass one of the job's datacenter slots to another datacenter --
        before the job is sent there, so a result it delivers is the
        job's, and none from the datacenter it passed from is."""
        target_dcs = self._job_target_dcs.setdefault(job_id, set())
        target_dcs.discard(from_dc)
        target_dcs.add(to_dc)

    def discard_target_dc(self, job_id: str, dc_id: str) -> None:
        """Drop a datacenter slot the job could not be placed in."""
        if (target_dcs := self._job_target_dcs.get(job_id)) is not None:
            target_dcs.discard(dc_id)

    # =========================================================================
    # Placement (AD-36 Part 13: mid-flight failover)
    # =========================================================================

    def get_datacenter_substitutions(self, job_id: str) -> list[DatacenterSubstitution]:
        """The job's lost datacenters and their replacements, in the
        order they were lost."""
        return list(self._job_datacenter_substitutions.get(job_id, {}).values())

    def set_datacenter_substitutions(
        self, job_id: str, substitutions: list[DatacenterSubstitution]
    ) -> None:
        """Replace the job's substitutions (a committed replica's)."""
        if substitutions:
            self._job_datacenter_substitutions[job_id] = {
                substitution.lost_datacenter: substitution
                for substitution in substitutions
            }
        else:
            self._job_datacenter_substitutions.pop(job_id, None)

    def release_datacenter(self, job_id: str, datacenter: str) -> None:
        """A datacenter the job no longer runs in, that must stop running
        it."""
        self._job_released_datacenters.setdefault(job_id, set()).add(datacenter)

    def get_released_datacenters(self, job_id: str) -> set[str]:
        return self._job_released_datacenters.get(job_id, set())

    def set_released_datacenters(self, job_id: str, datacenters: set[str]) -> None:
        """Replace the job's released datacenters (a committed replica's)."""
        if datacenters:
            self._job_released_datacenters[job_id] = set(datacenters)
        else:
            self._job_released_datacenters.pop(job_id, None)

    def expected_workflow_datacenters(self, job_id: str, workflow_id: str) -> set[str]:
        """The datacenters whose results make up a workflow's one
        aggregate: the job's datacenters -- except where a lost datacenter
        delivered the workflow's result before it was lost, which keeps
        that result slot from the replacement it passed the rest to. A
        chain of losses follows the chain: the earliest datacenter along
        it that delivered the workflow holds the slot."""
        target_dcs = self._job_target_dcs.get(job_id, set())
        if not (substitutions := self._job_datacenter_substitutions.get(job_id)):
            return target_dcs

        substitution_by_replacement = self._substitution_by_replacement(substitutions)
        expected_datacenters: set[str] = {
            self._workflow_slot_holder(
                target_dc,
                workflow_id,
                len(substitutions),
                substitution_by_replacement,
            )
            for target_dc in target_dcs
        }
        # A lost datacenter nothing re-ran for delivered every workflow's
        # result: it holds each slot still.
        expected_datacenters.update(self._unreplaced_slot_holders(substitutions, workflow_id))
        return expected_datacenters

    @staticmethod
    def _substitution_by_replacement(
        substitutions: dict[str, DatacenterSubstitution],
    ) -> dict[str, DatacenterSubstitution]:
        """Each substitution keyed by the datacenter that replaced the lost one."""
        return {
            substitution.replacement_datacenter: substitution
            for substitution in substitutions.values()
        }

    def _workflow_slot_holder(
        self,
        target_dc: str,
        workflow_id: str,
        substitution_count: int,
        substitution_by_replacement: dict[str, DatacenterSubstitution],
    ) -> str:
        """Follow the chain of losses back from ``target_dc``: the earliest
        datacenter along it that delivered the workflow holds its slot."""
        slot_holder = chain_datacenter = target_dc
        # A replacement is never a datacenter the job held before, so a
        # chain is no longer than the substitutions.
        for _ in range(substitution_count):
            if (
                substitution := substitution_by_replacement.get(chain_datacenter)
            ) is None:
                break
            slot_holder = self._slot_holder_after(substitution, workflow_id, slot_holder)
            chain_datacenter = substitution.lost_datacenter
        return slot_holder

    @staticmethod
    def _slot_holder_after(
        substitution: DatacenterSubstitution,
        workflow_id: str,
        slot_holder: str,
    ) -> str:
        """The lost datacenter takes the slot when it delivered the workflow before it was lost."""
        return (
            substitution.lost_datacenter
            if workflow_id in substitution.completed_workflow_ids
            else slot_holder
        )

    def _unreplaced_slot_holders(
        self,
        substitutions: dict[str, DatacenterSubstitution],
        workflow_id: str,
    ) -> list[str]:
        """Lost datacenters nothing re-ran for that delivered the workflow."""
        return [
            substitution.lost_datacenter
            for substitution in substitutions.values()
            if self._delivered_without_replacement(substitution, workflow_id)
        ]

    @staticmethod
    def _delivered_without_replacement(
        substitution: DatacenterSubstitution,
        workflow_id: str,
    ) -> bool:
        """Whether a lost datacenter with no replacement delivered the workflow."""
        return (
            not substitution.replacement_datacenter
            and workflow_id in substitution.completed_workflow_ids
        )

    def rerun_origin(self, job_id: str, datacenter: str) -> str | None:
        """The datacenter whose share ``datacenter`` re-runs: the first
        lost datacenter of the chain of losses ending at it (None when it
        replaced none)."""
        if not (substitutions := self._job_datacenter_substitutions.get(job_id)):
            return None
        return self._chain_origin(
            datacenter,
            len(substitutions),
            self._substitution_by_replacement(substitutions),
        )

    @staticmethod
    def _chain_origin(
        datacenter: str,
        substitution_count: int,
        substitution_by_replacement: dict[str, DatacenterSubstitution],
    ) -> str | None:
        """Walk the chain of losses ending at ``datacenter`` back to its first lost datacenter."""
        origin: str | None = None
        for _ in range(substitution_count):
            if (substitution := substitution_by_replacement.get(datacenter)) is None:
                break
            origin = datacenter = substitution.lost_datacenter
        return origin

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

    def _normalize_job_status(self, status: str) -> str:
        normalized = status.strip().lower()
        return _NORMALIZED_FINAL_JOB_STATUSES.get(normalized, JobStatus.FAILED.value)

    def aggregate_job_status(self, job_id: str) -> GlobalJobStatus | None:
        """
        The job as stored, with the datacenters' final results tallied in
        -- how many completed and failed, and their errors -- and its
        elapsed time: a copy, so reading it changes nothing.

        Its status and totals are the stored ones: progress keeps the
        totals and rate live, and only the job's terminal paths decide its
        status. A client's status poll resolved the status here and stored
        it -- marking the job terminal without finalizing it, so the paths
        that finalize then passed over a job already terminal and its
        requestor never got its result; calling datacenters that had not
        reported yet timed out -- and overwrote the live totals and rate
        with sums of final results, zero until datacenters finished.

        Returns None if the job doesn't exist. Caller should hold the job
        lock.
        """
        job = self._jobs.get(job_id)
        if not job:
            return None

        completed_datacenters, failed_datacenters, errors = self._tally_final_results(job_id)

        return dataclasses.replace(
            job,
            completed_datacenters=completed_datacenters,
            failed_datacenters=failed_datacenters,
            errors=errors,
            elapsed_seconds=self._elapsed_seconds_now(job),
        )

    def _tally_final_results(self, job_id: str) -> tuple[int, int, list[str]]:
        """Count the datacenters' completed and failed final results and gather their errors."""
        completed_datacenters = 0
        failed_datacenters = 0
        errors: list[str] = []
        for datacenter_id, result in self._job_dc_results.get(job_id, {}).items():
            if (
                status_value := self._normalize_job_status(result.status)
            ) == JobStatus.COMPLETED.value:
                completed_datacenters += 1
            else:
                failed_datacenters += 1

            errors.extend(self._final_result_errors(datacenter_id, result, status_value))

        return completed_datacenters, failed_datacenters, errors

    def _final_result_errors(
        self,
        datacenter_id: str,
        result: JobFinalResult,
        status_value: str,
    ) -> list[str]:
        """A final result's errors, or a stand-in for a non-completed result reported without any."""
        if result.errors:
            return self._prefixed_errors(datacenter_id, result.errors)
        if status_value != JobStatus.COMPLETED.value:
            return [
                f"{datacenter_id}: reported status {result.status} without error details"
            ]
        return []

    @staticmethod
    def _prefixed_errors(datacenter_id: str, errors: list[str]) -> list[str]:
        """Each error prefixed with the datacenter that reported it."""
        return [f"{datacenter_id}: {error}" for error in errors]

    @staticmethod
    def _elapsed_seconds_now(job: GlobalJobStatus) -> float:
        """Time since the job started, or its stored elapsed time when it has no start."""
        return (
            _DEFAULT_CLOCK.monotonic() - job.timestamp
            if job.timestamp > 0
            else job.elapsed_seconds
        )

    # =========================================================================
    # Cleanup
    # =========================================================================

    def _expired_terminal_job_ids(self, max_age_seconds: float) -> list[str]:
        """Jobs terminal for longer than ``max_age_seconds``, judged at one instant."""
        now = _DEFAULT_CLOCK.monotonic()
        return [
            job_id
            for job_id, job in list(self._jobs.items())
            if self._is_terminal_job_expired(job_id, job, now, max_age_seconds)
        ]

    def _is_terminal_job_expired(
        self,
        job_id: str,
        job: GlobalJobStatus,
        now: float,
        max_age_seconds: float,
    ) -> bool:
        """Whether the job is terminal and has been for longer than ``max_age_seconds``."""
        if job.status not in _TERMINAL_JOB_STATUSES:
            return False
        age = now - self.terminal_since(job_id, now)
        return age > max_age_seconds

    def cleanup_old_jobs(self, max_age_seconds: float) -> list[str]:
        """
        Remove jobs older than max_age_seconds that are in terminal state.

        Returns list of cleaned up job IDs.
        Note: Caller should be careful about locking - this iterates all jobs.
        """
        to_remove = self._expired_terminal_job_ids(max_age_seconds)

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
