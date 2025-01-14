from enum import Enum


class ProgressBarStatus(Enum):
    ACTIVE = "ACTIVE"
    FAILED = "FAILED"
    COMPLETE = "COMPLETE"
    READY = "READY"
