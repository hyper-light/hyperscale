from enum import Enum


class SegmentStatus(Enum):
    READY = "READY"
    ACTIVE = "ACTIVE"
    OK = "OK"
    FAILED = "FAILED"
