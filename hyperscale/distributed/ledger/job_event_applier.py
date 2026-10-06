from __future__ import annotations

from typing import Callable

from .events.event_type import JobEventType
from .datacenter_reassignment import DatacenterReassignment
from .events.job_event import (
    JobAccepted,
    JobCancellationAcked,
    JobCancellationRequested,
    JobCompleted,
    JobCreated,
    JobDatacenterReassigned,
    JobFailed,
    JobProgressReported,
    JobRelinquished,
    JobTimedOut,
)
from .events.job_leadership_acquired import JobLeadershipAcquired
from .job_state import JobState
from .wal.wal_entry import WALEntry


class JobEventApplier:
    """Replays AD-38 job events onto a job-state map.

    The single place that knows how each event type transforms
    ``JobState``, so WAL recovery reproduces exactly the transitions the
    live write paths apply. Events for jobs the map does not hold (e.g.
    already archived) are skipped, matching the live paths' refusal to
    act on unknown jobs -- and so are events for a job already terminal:
    live, a terminal job leaves the active map at once, so nothing after
    its first terminal ever touches it (a later cancellation would
    otherwise turn a completed job back into a cancelling one on replay).
    """

    def __init__(self) -> None:
        self._appliers: dict[
            JobEventType,
            Callable[[bytes, dict[str, JobState]], int],
        ] = {
            JobEventType.JOB_CREATED: self._apply_created,
            JobEventType.JOB_ACCEPTED: self._apply_accepted,
            JobEventType.JOB_PROGRESS_REPORTED: self._apply_progress_reported,
            JobEventType.JOB_CANCELLATION_REQUESTED: self._apply_cancellation_requested,
            JobEventType.JOB_CANCELLATION_ACKED: self._apply_cancellation_acked,
            JobEventType.JOB_COMPLETED: self._apply_completed,
            JobEventType.JOB_FAILED: self._apply_failed,
            JobEventType.JOB_TIMED_OUT: self._apply_timed_out,
            JobEventType.JOB_RELINQUISHED: self._apply_relinquished,
            JobEventType.JOB_DATACENTER_REASSIGNED: self._apply_datacenter_reassigned,
            JobEventType.JOB_LEADERSHIP_ACQUIRED: self._apply_leadership_acquired,
        }

    def apply(self, entry: WALEntry, jobs: dict[str, JobState]) -> int:
        """Apply one WAL entry to ``jobs``; returns the event's fence token.

        Raises:
            KeyError: the entry carries an event type with no applier —
                an unreplayable WAL must fail recovery loudly rather
                than silently drop state.
        """
        return self.apply_event(entry.event_type, entry.payload, jobs)

    def apply_event(
        self,
        event_type: JobEventType,
        payload: bytes,
        jobs: dict[str, JobState],
    ) -> int:
        """Apply one encoded event to ``jobs`` (see ``apply``)."""
        return self._appliers[event_type](payload, jobs)

    @staticmethod
    def _apply_created(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobCreated.from_bytes(payload)
        jobs[event.job_id] = JobState.create(
            job_id=event.job_id,
            fence_token=event.fence_token,
            assigned_datacenters=event.assigned_datacenters,
            created_hlc=event.hlc,
            requestor_id=event.requestor_id,
            timeout_seconds=event.timeout_seconds,
        )
        return event.fence_token

    @staticmethod
    def _apply_accepted(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobAccepted.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_accepted(
                datacenter_id=event.datacenter_id,
                hlc=event.hlc,
            )
        return event.fence_token

    @staticmethod
    def _apply_progress_reported(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobProgressReported.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_progress(
                completed_count=event.completed_count,
                failed_count=event.failed_count,
                hlc=event.hlc,
            )
        return event.fence_token

    @staticmethod
    def _apply_cancellation_requested(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobCancellationRequested.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_cancellation_requested(hlc=event.hlc)
        return event.fence_token

    @staticmethod
    def _apply_cancellation_acked(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobCancellationAcked.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_cancellation_acked(
                datacenter_id=event.datacenter_id,
                hlc=event.hlc,
            )
        return event.fence_token

    @staticmethod
    def _apply_completed(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobCompleted.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_completion(
                final_status=event.final_status,
                total_completed=event.total_completed,
                total_failed=event.total_failed,
                hlc=event.hlc,
            )
        return event.fence_token

    @staticmethod
    def _apply_failed(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobFailed.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_completion(
                final_status=JOB_FAILED_STATUS,
                total_completed=event.total_completed,
                total_failed=event.total_failed,
                hlc=event.hlc,
            )
        return event.fence_token

    @staticmethod
    def _apply_timed_out(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobTimedOut.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_completion(
                final_status=JOB_TIMED_OUT_STATUS,
                total_completed=event.total_completed,
                total_failed=event.total_failed,
                hlc=event.hlc,
            )
        return event.fence_token


    @staticmethod
    def _apply_relinquished(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobRelinquished.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_completion(
                final_status=JOB_RELINQUISHED_STATUS,
                total_completed=job.completed_count,
                total_failed=job.failed_count,
                hlc=event.hlc,
            )
        return event.fence_token

    @staticmethod
    def _apply_datacenter_reassigned(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobDatacenterReassigned.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_datacenter_reassigned(
                reassignment=DatacenterReassignment(
                    lost_datacenter=event.lost_datacenter,
                    replacement_datacenter=event.replacement_datacenter,
                    completed_workflow_ids=event.completed_workflow_ids,
                    total_completed=event.total_completed,
                    total_failed=event.total_failed,
                ),
                hlc=event.hlc,
            )
        return event.fence_token


    @staticmethod
    def _apply_leadership_acquired(payload: bytes, jobs: dict[str, JobState]) -> int:
        event = JobLeadershipAcquired.from_bytes(payload)
        if (job := jobs.get(event.job_id)) is not None and not job.is_terminal:
            jobs[event.job_id] = job.with_leadership_acquired(leader_id=event.leader_id, hlc=event.hlc)
        return event.fence_token


# Terminal statuses the failure/timeout events resolve to — the
# JobStatus vocabulary managers already write (see job_state.py) -- and
# the status ending a record its manager relinquished.
JOB_FAILED_STATUS = "failed"
JOB_TIMED_OUT_STATUS = "timeout"
JOB_RELINQUISHED_STATUS = "relinquished"
