"""
Request priority levels for load shedding (AD-22, AD-37).

Extracted to avoid circular imports between rate_limiting and load_shedding.
"""

from enum import IntEnum


class RequestPriority(IntEnum):
    """Priority levels for request classification.

    Lower values indicate higher priority.
    Maps directly to AD-37 MessageClass via MESSAGE_CLASS_TO_PRIORITY.
    """

    CRITICAL = 0  # CONTROL: SWIM probes/acks, cancellation, leadership - never shed
    HIGH = 1  # DISPATCH: Job submissions, workflow dispatch, state sync
    NORMAL = 2  # DATA: Progress updates, stats queries
    LOW = 3  # TELEMETRY: Debug stats, detailed metrics


__all__ = ["RequestPriority"]
