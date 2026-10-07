from enum import Enum


class ResourceViolationType(Enum):
    """Which budgeted resource a workflow exceeded."""

    CPU_EXCEEDED = "cpu_exceeded"
    MEMORY_EXCEEDED = "memory_exceeded"
    # A process of a worker's tree reached its descriptor ceiling
    # (FileDescriptorCeiling): worker-wide, the worker refuses new work.
    FILE_DESCRIPTORS_EXCEEDED = "file_descriptors_exceeded"
