from enum import Enum
from typing import Dict, Literal

StatusString = Literal[
    "SUBMITTED",
    "CREATED",
    "RUNNING",
    "COMPLETED",
    "PENDING",
    "FAILED",
]


class WorkflowStatus(Enum):
    SUBMITTED = "SUBMITTED"
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    PENDING = "PENDING"
    FAILED = "FAILED"

    def map_value_to_status(self, status: StatusString):
        status_map: Dict[StatusString, WorkflowStatus] = {
            "SUBMITTED": WorkflowStatus.SUBMITTED,
            "CREATED": WorkflowStatus.CREATED,
            "RUNNING": WorkflowStatus.RUNNING,
            "COMPLETED": WorkflowStatus.COMPLETED,
            "PENDING": WorkflowStatus.PENDING,
            "FAILED": WorkflowStatus.FAILED,
        }

        return status_map[status]
