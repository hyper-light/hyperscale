"""
Replicated stats store for Raft-backed stats aggregation.

Provides a hot-path local buffer for progress updates with periodic
flush-and-replicate through Raft consensus. This avoids proposing
every individual stat update through Raft while still ensuring
all nodes converge on the same aggregated view.

Pattern:
    1. add_progress() writes to local buffer (no Raft, no await)
    2. flush_and_replicate() serializes + proposes the buffer through Raft
    3. State machine applies aggregated window to all replicas
"""

import time
from typing import TYPE_CHECKING

from .logging_models import RaftDebug, RaftWarning

if TYPE_CHECKING:
    from hyperscale.logging import Logger

    from .raft_consensus import RaftConsensus
    from .raft_job_manager import RaftJobManager


class ReplicatedStatsStore:
    """
    Bounded local stats buffer with Raft-backed flush.

    Accumulates per-job progress samples locally in a bounded
    ring buffer. On flush, serializes the aggregated window and
    proposes it through Raft for consistent replication.

    Backpressure: when the buffer exceeds max_pending_samples,
    oldest samples are evicted (bounded memory).
    """

    __slots__ = (
        "_logger",
        "_node_id",
        "_max_pending_samples",
        "_buffers",
        "_last_flush_times",
        "_flush_interval_seconds",
    )

    def __init__(
        self,
        logger: "Logger",
        node_id: str,
        max_pending_samples: int = 10_000,
        flush_interval_seconds: float = 5.0,
    ) -> None:
        self._logger = logger
        self._node_id = node_id
        self._max_pending_samples = max_pending_samples
        self._flush_interval_seconds = flush_interval_seconds

        # job_id -> list of (timestamp, value) samples
        self._buffers: dict[str, list[tuple[float, float]]] = {}
        # job_id -> last flush monotonic time
        self._last_flush_times: dict[str, float] = {}

    def add_progress(
        self,
        job_id: str,
        value: float,
        timestamp: float | None = None,
    ) -> None:
        """
        Add a progress sample to the local buffer.

        This is the hot path -- synchronous, no Raft, no I/O.
        Evicts oldest samples if buffer exceeds max size.
        """
        sample_time = timestamp if timestamp is not None else time.monotonic()

        buffer = self._buffers.get(job_id)
        if buffer is None:
            buffer = []
            self._buffers[job_id] = buffer

        buffer.append((sample_time, value))

        # Backpressure: evict oldest if over capacity
        if len(buffer) > self._max_pending_samples:
            excess = len(buffer) - self._max_pending_samples
            del buffer[:excess]

    def should_flush(self, job_id: str) -> bool:
        """Check if a job's buffer is due for flushing."""
        last_flush = self._last_flush_times.get(job_id, 0.0)
        return (time.monotonic() - last_flush) >= self._flush_interval_seconds

    def pending_count(self, job_id: str) -> int:
        """Number of buffered samples for a job."""
        buffer = self._buffers.get(job_id)
        return len(buffer) if buffer is not None else 0

    @property
    def total_pending(self) -> int:
        """Total buffered samples across all jobs."""
        return sum(len(buffer) for buffer in self._buffers.values())

    async def flush_and_replicate(
        self,
        job_id: str,
        raft_job_manager: "RaftJobManager",
    ) -> bool:
        """
        Flush buffered samples and replicate through Raft.

        Serializes the accumulated stats window and proposes it
        as a FLUSH_STATS_WINDOW command. Returns True on success.
        """
        buffer = self._buffers.get(job_id)
        if not buffer:
            return True  # Nothing to flush

        import cloudpickle

        stats_data = cloudpickle.dumps(list(buffer))
        success = await raft_job_manager.flush_stats_window(
            job_id=job_id,
            stats_data=stats_data,
        )

        if success:
            buffer.clear()
            self._last_flush_times[job_id] = time.monotonic()
            await self._logger.log(RaftDebug(
                message=f"Flushed {len(buffer)} stats samples for job {job_id}",
                node_id=self._node_id,
                job_id=job_id,
            ))
        else:
            await self._logger.log(RaftWarning(
                message=f"Stats flush rejected for job {job_id} ({len(buffer)} samples pending)",
                node_id=self._node_id,
                job_id=job_id,
            ))

        return success

    async def flush_all(self, raft_job_manager: "RaftJobManager") -> int:
        """
        Flush all jobs that are due. Returns count of successful flushes.
        """
        flushed = 0
        for job_id in list(self._buffers.keys()):
            if not self.should_flush(job_id):
                continue
            if await self.flush_and_replicate(job_id, raft_job_manager):
                flushed += 1
        return flushed

    def cleanup_job(self, job_id: str) -> None:
        """Remove all buffered data for a job. Called on job completion."""
        self._buffers.pop(job_id, None)
        self._last_flush_times.pop(job_id, None)

    def clear(self) -> None:
        """Release all buffered data. Called on node shutdown."""
        self._buffers.clear()
        self._last_flush_times.clear()
