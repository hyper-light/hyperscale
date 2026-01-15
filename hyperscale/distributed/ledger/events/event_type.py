from enum import IntEnum


class JobEventType(IntEnum):
    """Event types for job state changes in the ledger."""

    JOB_CREATED = 1
    JOB_ACCEPTED = 2
    JOB_PROGRESS_REPORTED = 3
    JOB_CANCELLATION_REQUESTED = 4
    JOB_CANCELLATION_ACKED = 5
    JOB_COMPLETED = 6
    JOB_FAILED = 7
    JOB_TIMED_OUT = 8
