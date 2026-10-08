"""
AD-38: Global Job Ledger with Per-Node Write-Ahead Logging.

This module provides a distributed job ledger with tiered durability guarantees:
- LOCAL: Process crash recovery via fsync'd WAL (<1ms)
- REGIONAL: Node failure within datacenter (2-10ms)
- GLOBAL: Region failure via cross-region replication (50-300ms)

Key components:
- JobLedger: Event-sourced job state with checkpoint/recovery
- NodeWAL: Per-node write-ahead log with CRC verification
- CommitPipeline: Three-stage commit for tiered durability
- JobIdGenerator: Region-encoded globally unique job IDs
"""

from .durability_level import DurabilityLevel
from .job_id import JobIdGenerator
from .job_state import JobState
from .datacenter_reassignment import DatacenterReassignment
from .job_ledger import JobLedger
from .unsatisfiable_durability_error import UnsatisfiableDurabilityError

from .events import (
    JobEventType,
    JobEvent,
    JobCreated,
    JobAccepted,
    JobProgressReported,
    JobCancellationRequested,
    JobCancellationAcked,
    JobCompleted,
    JobFailed,
    JobTimedOut,
    JobRelinquished,
    JobDatacenterReassigned,
    JobEventUnion,
)

from .wal import (
    WALEntryState,
    WALEntry,
    HEADER_SIZE,
    WALStatusSnapshot,
    NodeWAL,
    TransitionResult,
    WALAppendResult,
    WALBackpressureError,
    WALWriterConfig,
)

from .pipeline import (
    CommitPipeline,
    CommitResult,
)

from .checkpoint import (
    Checkpoint,
    CheckpointManager,
)

from .archive import JobArchiveStore

from .cache import BoundedLRUCache

__all__ = [
    "JobLedger",
    "JobState",
    "JobIdGenerator",
    "DurabilityLevel",
    "UnsatisfiableDurabilityError",
    "JobEventType",
    "JobEvent",
    "JobCreated",
    "JobAccepted",
    "JobProgressReported",
    "JobCancellationRequested",
    "JobCancellationAcked",
    "JobCompleted",
    "JobFailed",
    "JobTimedOut",
    "JobRelinquished",
    "JobDatacenterReassigned",
    "DatacenterReassignment",
    "JobEventUnion",
    "WALEntryState",
    "WALEntry",
    "HEADER_SIZE",
    "WALStatusSnapshot",
    "NodeWAL",
    "TransitionResult",
    "WALAppendResult",
    "WALBackpressureError",
    "WALWriterConfig",
    "CommitPipeline",
    "CommitResult",
    "Checkpoint",
    "CheckpointManager",
    "JobArchiveStore",
    "BoundedLRUCache",
]
