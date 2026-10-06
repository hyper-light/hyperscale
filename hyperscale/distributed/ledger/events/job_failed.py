"""``JobFailed`` -- pickled under the namespace
``hyperscale.distributed.ledger.events.job_event`` (see that module)."""

from __future__ import annotations

import msgspec
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .event_type import JobEventType


class JobFailed(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: HLCTimestamp
    fence_token: int
    error_message: str
    failed_datacenter: str

    event_type: JobEventType = JobEventType.JOB_FAILED
    # Terminal tallies, so a failed job's record is as complete as a
    # JobCompleted one. Trailing + defaulted: array_like decode of an
    # older record fills them in.
    total_completed: int = 0
    total_failed: int = 0
    duration_ms: int = 0

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobFailed:
        return msgspec.msgpack.decode(data, type=cls)
