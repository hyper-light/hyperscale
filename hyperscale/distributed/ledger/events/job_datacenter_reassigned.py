"""``JobDatacenterReassigned`` -- pickled under the namespace
``hyperscale.distributed.ledger.events.job_event`` (see that module)."""

from __future__ import annotations

import msgspec
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .event_type import JobEventType


class JobDatacenterReassigned(msgspec.Struct, frozen=True, array_like=True):
    """AD-36 mid-flight failover: the job moved off a datacenter it lost
    while it ran there. Its unfinished workflows re-run in
    ``replacement_datacenter`` ("" when it had delivered every workflow's
    result: nothing re-runs, and the job stops waiting on it); the
    workflows it delivered keep their result slots with it; its work
    until it was lost counts in the job's totals."""

    job_id: str
    hlc: HLCTimestamp
    fence_token: int
    lost_datacenter: str
    replacement_datacenter: str
    completed_workflow_ids: tuple[str, ...]
    total_completed: int
    total_failed: int

    event_type: JobEventType = JobEventType.JOB_DATACENTER_REASSIGNED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobDatacenterReassigned:
        return msgspec.msgpack.decode(data, type=cls)
