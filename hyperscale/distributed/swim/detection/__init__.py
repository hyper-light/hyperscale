"""
Failure detection components for SWIM protocol.

This module provides hierarchical failure detection with two layers:
1. Global layer (TimingWheel): Machine-level liveness detection
2. Job layer (JobSuspicionManager): Per-job responsiveness detection

The HierarchicalFailureDetector coordinates both layers for accurate
failure detection in multi-job distributed systems.
"""

from .incarnation_tracker import (
    IncarnationTracker,
    MAX_INCARNATION,
    MAX_INCARNATION_JUMP,
)

from .incarnation_store import (
    IncarnationStore,
    IncarnationRecord,
)

from .suspicion_state import SuspicionState

from .suspicion_manager import SuspicionManager

from .pending_indirect_probe import PendingIndirectProbe

from .indirect_probe_manager import IndirectProbeManager

from .probe_scheduler import ProbeScheduler

from .timing_wheel import (
    TimingWheel,
    TimingWheelConfig,
    TimingWheelBucket,
    WheelEntry,
)

from .job_suspicion_manager import (
    JobSuspicionManager,
    JobSuspicionConfig,
    JobSuspicion,
)

from .hierarchical_failure_detector import (
    HierarchicalFailureDetector,
    HierarchicalConfig,
    NodeStatus,
    FailureSource,
    FailureEvent,
)


__all__ = [
    "IncarnationTracker",
    "MAX_INCARNATION",
    "MAX_INCARNATION_JUMP",
    "IncarnationStore",
    "IncarnationRecord",
    "SuspicionState",
    "SuspicionManager",
    "PendingIndirectProbe",
    "IndirectProbeManager",
    "ProbeScheduler",
    "TimingWheel",
    "TimingWheelConfig",
    "TimingWheelBucket",
    "WheelEntry",
    "JobSuspicionManager",
    "JobSuspicionConfig",
    "JobSuspicion",
    "HierarchicalFailureDetector",
    "HierarchicalConfig",
    "NodeStatus",
    "FailureSource",
    "FailureEvent",
]
