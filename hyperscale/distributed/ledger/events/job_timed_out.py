"""``JobTimedOut`` -- pickled under the namespace
``hyperscale.distributed.ledger.events.job_event`` (see that module)."""

from __future__ import annotations

import msgspec
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .event_type import JobEventType


class JobTimedOut(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: HLCTimestamp
    fence_token: int
    timeout_type: str
    last_progress_hlc: HLCTimestamp | None

    event_type: JobEventType = JobEventType.JOB_TIMED_OUT
    # Terminal tallies — same contract as JobFailed's.
    total_completed: int = 0
    total_failed: int = 0
    duration_ms: int = 0

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobTimedOut:
        return msgspec.msgpack.decode(data, type=cls)
