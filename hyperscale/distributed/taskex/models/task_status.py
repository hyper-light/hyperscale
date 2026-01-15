from enum import Enum


class TaskStatus(Enum):
    CREATED = "CREATED"
    PENDING = "PENDING"
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
