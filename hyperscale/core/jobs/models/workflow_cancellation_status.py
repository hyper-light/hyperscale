from enum import Enum


class WorkflowCancellationStatus(Enum):
    NOT_FOUND="NOT_FOUND"
    REQUESTED="REQUESTED"
    IN_PROGRESS="IN_PROGRESS"
    CANCELLED="CANCELLED"
    FAILED="FAILED"