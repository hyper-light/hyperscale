from __future__ import annotations

import struct
from typing import Any

import msgspec

from hyperscale.logging.lsn import LSN

from .event_type import JobEventType


class JobEvent(msgspec.Struct, frozen=True, array_like=True):
    """
    Base event for all job state changes.

    All events are immutable and serialized for WAL storage.
    """

    event_type: JobEventType
    job_id: str
    hlc: LSN
    fence_token: int

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobEvent:
        return msgspec.msgpack.decode(data, type=cls)


class JobCreated(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: LSN
    fence_token: int
    spec_hash: bytes
    assigned_datacenters: tuple[str, ...]
    requestor_id: str

    event_type: JobEventType = JobEventType.JOB_CREATED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobCreated:
        return msgspec.msgpack.decode(data, type=cls)


class JobAccepted(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: LSN
    fence_token: int
    datacenter_id: str
    worker_count: int

    event_type: JobEventType = JobEventType.JOB_ACCEPTED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobAccepted:
        return msgspec.msgpack.decode(data, type=cls)


class JobProgressReported(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: LSN
    fence_token: int
    datacenter_id: str
    completed_count: int
    failed_count: int

    event_type: JobEventType = JobEventType.JOB_PROGRESS_REPORTED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobProgressReported:
        return msgspec.msgpack.decode(data, type=cls)


class JobCancellationRequested(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: LSN
    fence_token: int
    reason: str
    requestor_id: str

    event_type: JobEventType = JobEventType.JOB_CANCELLATION_REQUESTED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobCancellationRequested:
        return msgspec.msgpack.decode(data, type=cls)


class JobCancellationAcked(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: LSN
    fence_token: int
    datacenter_id: str
    workflows_cancelled: int

    event_type: JobEventType = JobEventType.JOB_CANCELLATION_ACKED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobCancellationAcked:
        return msgspec.msgpack.decode(data, type=cls)


class JobCompleted(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: LSN
    fence_token: int
    final_status: str
    total_completed: int
    total_failed: int
    duration_ms: int

    event_type: JobEventType = JobEventType.JOB_COMPLETED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobCompleted:
        return msgspec.msgpack.decode(data, type=cls)


class JobFailed(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: LSN
    fence_token: int
    error_message: str
    failed_datacenter: str

    event_type: JobEventType = JobEventType.JOB_FAILED

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobFailed:
        return msgspec.msgpack.decode(data, type=cls)


class JobTimedOut(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    hlc: LSN
    fence_token: int
    timeout_type: str
    last_progress_hlc: LSN | None

    event_type: JobEventType = JobEventType.JOB_TIMED_OUT

    def to_bytes(self) -> bytes:
        return msgspec.msgpack.encode(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> JobTimedOut:
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
)
