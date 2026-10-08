"""Wire model ``AsyncSafeJobStatsCRDT`` -- pickled under the wire namespace
``hyperscale.distributed.models.crdt`` (see that module)."""

from __future__ import annotations

import asyncio
from .job_stats_crdt import JobStatsCRDT
from .job_stats_crdt_snapshot import JobStatsCRDTSnapshot


class AsyncSafeJobStatsCRDT:
    """
    Async-safe wrapper around JobStatsCRDT for concurrent coroutine access.

    Provides asyncio.Lock protection around merge operations to prevent
    race conditions when multiple coroutines merge stats concurrently.

    All read operations are lock-free since they access immutable snapshots
    or atomic Python operations. Only merge_in_place requires the lock.
    """

    __slots__ = ("_crdt", "_lock")

    def __init__(self, job_id: str):
        self._crdt = JobStatsCRDT(job_id=job_id)
        self._lock = asyncio.Lock()

    @property
    def job_id(self) -> str:
        return self._crdt.job_id

    @property
    def total_completed(self) -> int:
        return self._crdt.total_completed

    @property
    def total_failed(self) -> int:
        return self._crdt.total_failed

    @property
    def total_rate(self) -> float:
        return self._crdt.total_rate

    def record_completed(self, dc_id: str, count: int) -> None:
        self._crdt.record_completed(dc_id, count)

    def record_failed(self, dc_id: str, count: int) -> None:
        self._crdt.record_failed(dc_id, count)

    def record_rate(self, dc_id: str, rate: float, timestamp: int) -> None:
        self._crdt.record_rate(dc_id, rate, timestamp)

    def record_status(self, dc_id: str, status: str, timestamp: int) -> None:
        self._crdt.record_status(dc_id, status, timestamp)

    def get_dc_completed(self, dc_id: str) -> int:
        return self._crdt.get_dc_completed(dc_id)

    def get_dc_failed(self, dc_id: str) -> int:
        return self._crdt.get_dc_failed(dc_id)

    def get_dc_rate(self, dc_id: str) -> float:
        return self._crdt.get_dc_rate(dc_id)

    def get_dc_status(self, dc_id: str) -> str | None:
        return self._crdt.get_dc_status(dc_id)

    async def merge_in_place(self, other: JobStatsCRDT | AsyncSafeJobStatsCRDT) -> None:
        other_crdt = other._crdt if isinstance(other, AsyncSafeJobStatsCRDT) else other
        async with self._lock:
            self._crdt.merge_in_place(other_crdt)

    def merge(self, other: JobStatsCRDT | AsyncSafeJobStatsCRDT) -> JobStatsCRDT:
        other_crdt = other._crdt if isinstance(other, AsyncSafeJobStatsCRDT) else other
        return self._crdt.merge(other_crdt)

    def to_dict(self) -> JobStatsCRDTSnapshot:
        return self._crdt.to_dict()

    @classmethod
    def from_dict(cls, data: JobStatsCRDTSnapshot) -> AsyncSafeJobStatsCRDT:
        instance = cls(job_id=data["job_id"])
        instance._crdt = JobStatsCRDT.from_dict(data)
        return instance

    def get_inner(self) -> JobStatsCRDT:
        return self._crdt
