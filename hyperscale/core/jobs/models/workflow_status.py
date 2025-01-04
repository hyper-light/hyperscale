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
            "UNKNOWN": WorkflowStatus.UNKNOWN,
            "QUEUED": WorkflowStatus.QUEUED,
        }

        return status_map[status]
