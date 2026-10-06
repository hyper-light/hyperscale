from __future__ import annotations

import msgspec

from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .event_type import JobEventType


class JobLeadershipAcquired(msgspec.Struct, frozen=True, array_like=True):
    """This ledger's node took the job over from its previous leader
    (AD-31): the record of who leads it from here, under which lease fence.
    Recorded on the new leader (LOCAL) right after it adopts the job's
    replicated history -- the audit trail of the job's leadership changes,
    beside ``JobRelinquished`` on the side that gave a job up.
    ``previous_leader_id`` is empty when no leader was known."""

    job_id: str
    hlc: HLCTimestamp
    fence_token: int
    leader_id: str
    previous_leader_id: str
    lease_fence_token: int

    event_type: JobEventType = JobEventType.JOB_LEADERSHIP_ACQUIRED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobLeadershipAcquired:
        return msgspec.msgpack.decode(data, type=cls)
