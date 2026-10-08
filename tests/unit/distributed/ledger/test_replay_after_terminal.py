"""
Replay after a job's terminal (AD-38) reproduces live state.

Live, a job leaves the ledger's active map the moment its first terminal
lands, so no later event -- a cancellation, progress, a second terminal --
ever touches it. Replay keeps terminal jobs in its map until the archive
sweep, so the applier must refuse them the same way: otherwise a completed
job replays as cancelling, or as failed, and the recovered ledger disagrees
with the one that wrote the log.
"""

from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.events.job_event import (
    JobCancellationRequested,
    JobCompleted,
    JobCreated,
    JobFailed,
    JobProgressReported,
)
from hyperscale.distributed.ledger.job_event_applier import JobEventApplier
from hyperscale.distributed.ledger.job_state import JobState
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

JOB_ID = "dc-east-1-gate-1-1"


def test_events_after_the_first_terminal_leave_the_job_as_it_ended() -> None:
    clock = new_hybrid_logical_clock()
    applier = JobEventApplier()
    jobs: dict[str, JobState] = {}

    applier.apply_event(
        JobEventType.JOB_CREATED,
        JobCreated(
            job_id=JOB_ID,
            hlc=clock.now(),
            fence_token=1,
            spec_hash=b"spec",
            assigned_datacenters=("dc-east",),
            requestor_id="client-1",
        ).to_bytes(),
        jobs,
    )
    applier.apply_event(
        JobEventType.JOB_COMPLETED,
        JobCompleted(
            job_id=JOB_ID,
            hlc=clock.now(),
            fence_token=1,
            final_status="completed",
            total_completed=10,
            total_failed=0,
            duration_ms=1500,
        ).to_bytes(),
        jobs,
    )
    completed = jobs[JOB_ID]

    later_events = [
        (
            JobEventType.JOB_CANCELLATION_REQUESTED,
            JobCancellationRequested(
                job_id=JOB_ID, hlc=clock.now(), fence_token=1, reason="late", requestor_id="client-1"
            ).to_bytes(),
        ),
        (
            JobEventType.JOB_PROGRESS_REPORTED,
            JobProgressReported(
                job_id=JOB_ID, hlc=clock.now(), fence_token=1, datacenter_id="dc-east", completed_count=3, failed_count=7
            ).to_bytes(),
        ),
        (
            JobEventType.JOB_FAILED,
            JobFailed(
                job_id=JOB_ID, hlc=clock.now(), fence_token=1, error_message="late", failed_datacenter="dc-east"
            ).to_bytes(),
        ),
    ]
    for event_type, payload in later_events:
        applier.apply_event(event_type, payload, jobs)

    assert jobs[JOB_ID] == completed
    assert jobs[JOB_ID].status == "completed" and jobs[JOB_ID].completed_count == 10
