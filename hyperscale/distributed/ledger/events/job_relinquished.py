"""``JobRelinquished`` -- pickled under the namespace
``hyperscale.distributed.ledger.events.job_event`` (see that module)."""

from __future__ import annotations

import msgspec
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .event_type import JobEventType


class JobRelinquished(msgspec.Struct, frozen=True, array_like=True):
    """This ledger's manager no longer leads the job: another manager of
    its datacenter does, or did and ended it. The job's own record is that
    leader's; this one stops claiming it. Recorded on this manager alone
    (LOCAL) -- never replicated, where it would read as the job's end.
    ``held_by`` is the peer found holding the job."""

    job_id: str
    hlc: HLCTimestamp
    fence_token: int
    held_by: str

    event_type: JobEventType = JobEventType.JOB_RELINQUISHED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobRelinquished:
        return msgspec.msgpack.decode(data, type=cls)
