from enum import Enum
from typing import Literal


class RunStatus(Enum):
    CREATED = "CREATED"
    PENDING = "PENDING"
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


RunStatusName = Literal[
    'CREATED',
    'PENDING',
    'IDLE',
    'RUNNING',
    'COMPLETE',
    'CANCELLED',
    'FAILED',
]