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
]


class WorkflowStatus(Enum):
    SUBMITTED = "SUBMITTED"
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
        }

        return status_map[status]
