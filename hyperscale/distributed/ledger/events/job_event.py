"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import struct
from typing import Any
import msgspec
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .event_type import JobEventType
from .job_leadership_acquired import JobLeadershipAcquired
from .job_accepted import JobAccepted
from .job_cancellation_acked import JobCancellationAcked
from .job_cancellation_requested import JobCancellationRequested
from .job_completed import JobCompleted
from .job_created import JobCreated
from .job_datacenter_reassigned import JobDatacenterReassigned
from .job_failed import JobFailed
from .job_progress_reported import JobProgressReported
from .job_relinquished import JobRelinquished
from .job_timed_out import JobTimedOut


class JobEvent(msgspec.Struct, frozen=True, array_like=True):
    """
    Base event for all job state changes.

    All events are immutable and serialized for WAL storage.
    """

    event_type: JobEventType
    job_id: str
    hlc: HLCTimestamp
    fence_token: int

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobEvent:
        return msgspec.msgpack.decode(data, type=cls)

JobEventUnion = (
    JobCreated
    | JobAccepted
    | JobProgressReported
    | JobCancellationRequested
    | JobCancellationAcked
    | JobCompleted
    | JobFailed
    | JobTimedOut
    | JobRelinquished
    | JobDatacenterReassigned
    | JobLeadershipAcquired
)

_REHOMED = (
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
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
