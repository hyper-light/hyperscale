"""``JobCreated`` -- pickled under the namespace
``hyperscale.distributed.ledger.events.job_event`` (see that module)."""

from __future__ import annotations

import msgspec
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .event_type import JobEventType


class JobCreated(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: HLCTimestamp
    fence_token: int
    spec_hash: bytes
    assigned_datacenters: tuple[str, ...]
    requestor_id: str

    event_type: JobEventType = JobEventType.JOB_CREATED
    # Job-level timeout budget, persisted so a RESTARTED accepting node
    # can resume AD-34 tracking with the remaining budget. Trailing +
    # defaulted: old array_like records decode cleanly (the
    # requestor_id precedent).
    timeout_seconds: float = 0.0

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobCreated:
        return msgspec.msgpack.decode(data, type=cls)
