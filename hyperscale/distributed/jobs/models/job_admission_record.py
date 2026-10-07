"""``JobAdmissionRecord`` -- one unfinished job as admission control counts it."""

from dataclasses import dataclass


@dataclass(slots=True)
class JobAdmissionRecord:
    """An unfinished job a DC leader counts against its caps (D-65).

    ``core_seconds`` is the job's work (``job_shape.job_core_seconds``).
    ``admitted_at`` is when, on the leader's monotonic clock, the job was
    admitted (or first counted, for a job a previous leader admitted);
    ``expected_end_at`` is when it is expected to have ended -- its
    admission plus its longest chain of workflow durations -- the soonest
    a cap it fills may free a slot.
    """

    job_class: str
    core_seconds: float
    admitted_at: float
    expected_end_at: float
