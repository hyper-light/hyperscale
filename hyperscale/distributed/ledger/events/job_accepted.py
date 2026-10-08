"""``JobAccepted`` -- pickled under the namespace
``hyperscale.distributed.ledger.events.job_event`` (see that module)."""

from __future__ import annotations

import msgspec
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .event_type import JobEventType


class JobAccepted(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: HLCTimestamp
    fence_token: int
    datacenter_id: str
    worker_count: int

    event_type: JobEventType = JobEventType.JOB_ACCEPTED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobAccepted:
        return msgspec.msgpack.decode(data, type=cls)
