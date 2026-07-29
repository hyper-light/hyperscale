from enum import Enum
from typing import Dict, Literal

StatusString = Literal[
    "SUBMITTED",
    "CREATED",
    "RUNNING",
    "COMPLETED",
    "PENDING",
    "FAILED",
    "REJECTED",
    "CANCELLED",
    "UNKNOWN",
    "QUEUED",
]


class WorkflowStatus(Enum):
    SUBMITTED = "SUBMITTED"
    QUEUED = "QUEUED"
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    PENDING = "PENDING"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    # Terminal outcome of a hard-cancelled run: the graceful window
    # expired and the executor stopped in-flight work (distinct from
    # FAILED — the workflow did not err, it was stopped by decision).
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def map_value_to_status(cls, status: StatusString):
        status_map: Dict[StatusString, WorkflowStatus] = {
            "SUBMITTED": WorkflowStatus.SUBMITTED,
            "CREATED": WorkflowStatus.CREATED,
            "RUNNING": WorkflowStatus.RUNNING,
            "COMPLETED": WorkflowStatus.COMPLETED,
            "PENDING": WorkflowStatus.PENDING,
            "FAILED": WorkflowStatus.FAILED,
            "REJECTED": WorkflowStatus.REJECTED,
            "CANCELLED": WorkflowStatus.CANCELLED,
            "UNKNOWN": WorkflowStatus.UNKNOWN,
            "QUEUED": WorkflowStatus.QUEUED,
        }

        return status_map[status]
