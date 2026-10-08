"""``JobCompleted`` -- pickled under the namespace
``hyperscale.distributed.ledger.events.job_event`` (see that module)."""

from __future__ import annotations

import msgspec
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .event_type import JobEventType


class JobCompleted(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: HLCTimestamp
    fence_token: int
    final_status: str
    total_completed: int
    total_failed: int
    duration_ms: int

    event_type: JobEventType = JobEventType.JOB_COMPLETED
    # D-67: the job's class and how many of its retries were refused for a
    # spent AD-44 budget, so every member of the job's group -- a DC
    # leader's successor among them -- holds the noisy-job breaker's facts.
    # Trailing + defaulted: array_like decode of an older record fills them.
    job_class: str = ""
    refused_retries: int = 0

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobCompleted:
        return msgspec.msgpack.decode(data, type=cls)
