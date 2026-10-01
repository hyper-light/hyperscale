from enum import Enum


class ResourceViolationType(Enum):
    """Which budgeted resource a workflow exceeded."""

    CPU_EXCEEDED = "cpu_exceeded"
    MEMORY_EXCEEDED = "memory_exceeded"
