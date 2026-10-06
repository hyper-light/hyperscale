"""
Time-Windowed Stats Collector.

Collects workflow progress updates into time-correlated windows for
aggregation and streaming to clients/gates.

Key features:
- Time bucketing: Stats grouped by collected_at timestamp into windows
- Drift tolerance: Allows for clock skew between workers
- Memory bounded: Windows cleared after flush
- Aggregation modes: Aggregated for clients, unaggregated for gates

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from hyperscale.distributed.models import WorkflowProgress, StepStats, Message
from hyperscale.distributed.runtime import Clock, RealClock

from .window_bucket import WindowBucket
from .windowed_stats_metrics import WindowedStatsMetrics
from .windowed_stats_push import WindowedStatsPush
from .worker_window_stats import WorkerWindowStats

_DEFAULT_CLOCK: Clock = RealClock()


class WindowedStatsCollector:
    """
    Collects workflow progress updates into time-correlated windows.

    Safe for concurrent progress updates from multiple coroutines.

    The collector groups incoming WorkflowProgress updates by their
    collected_at timestamp into discrete time windows. When windows
    are flushed, stats can be aggregated (for direct client push) or
    left unaggregated (for gate forwarding).

    Time correlation ensures that stats from different workers within
    the same time window (accounting for clock drift) are grouped together,
    providing a consistent view of system state at each point in time.
    """

    def __init__(
        self,
        window_size_ms: float = 100.0,
        drift_tolerance_ms: float = 50.0,
        max_window_age_ms: float = 5000.0,
    ):
        self._window_size_ms = window_size_ms
        self._drift_tolerance_ms = drift_tolerance_ms
        self._max_window_age_ms = max_window_age_ms

        self._buckets: dict[tuple[str, str, int], WindowBucket] = {}
        self._lock = asyncio.Lock()
        self._metrics = WindowedStatsMetrics()
        self._seen_updates: dict[tuple[str, str, str, float], float] = {}
        self._dedup_window_seconds = max_window_age_ms / 1000.0

    def _get_bucket_number(self, collected_at: float) -> int:
        """Convert Unix timestamp to window bucket number."""
        return int(collected_at * 1000 / self._window_size_ms)

    def _is_window_closed(self, bucket_num: int, now: float) -> bool:
        """Check if a window can be flushed (all expected stats have arrived)."""
        window_end_ms = (bucket_num + 1) * self._window_size_ms
        current_ms = now * 1000
        # Window is closed when current time exceeds window_end + drift tolerance
        return current_ms > window_end_ms + self._drift_tolerance_ms

    async def add_progress(
        self,
        worker_id: str,
        progress: WorkflowProgress,
    ) -> bool:
        bucket_num = self._get_bucket_number(progress.collected_at)
        key = (progress.job_id, progress.workflow_id, bucket_num)
        dedup_key = (
            worker_id,
            progress.job_id,
            progress.workflow_id,
            progress.collected_at,
        )

        async with self._lock:
            now = _DEFAULT_CLOCK.time()
            self._cleanup_seen_updates(now)

            if dedup_key in self._seen_updates:
                self._metrics.duplicates_detected += 1
                return False

            self._seen_updates[dedup_key] = now

            if key not in self._buckets:
                window_start = bucket_num * self._window_size_ms / 1000
                window_end = (bucket_num + 1) * self._window_size_ms / 1000
                self._buckets[key] = WindowBucket(
                    window_start=window_start,
                    window_end=window_end,
                    job_id=progress.job_id,
                    workflow_id=progress.workflow_id,
                    workflow_name=progress.workflow_name,
                    worker_stats={},
                    created_at=now,
                )

            self._buckets[key].worker_stats[worker_id] = progress
            self._metrics.stats_recorded += 1
            return True

    def _cleanup_seen_updates(self, now: float) -> None:
        cutoff = now - self._dedup_window_seconds
        expired_keys = [k for k, v in self._seen_updates.items() if v < cutoff]
        for k in expired_keys:
            del self._seen_updates[k]

    def _aggregate_bucket(self, bucket: WindowBucket) -> WindowedStatsPush:
        """Aggregate all worker stats in a bucket into single stats."""
        total_completed = 0
        total_failed = 0
        total_rate = 0.0
        total_cpu = 0.0
        total_memory = 0.0
        step_stats_by_name: dict[str, StepStats] = {}

        for progress in bucket.worker_stats.values():
            total_completed += progress.completed_count
            total_failed += progress.failed_count
            total_rate += progress.rate_per_second
            total_cpu += progress.avg_cpu_percent
            total_memory += progress.avg_memory_mb

            for step in progress.step_stats:
                if step.step_name in step_stats_by_name:
                    existing = step_stats_by_name[step.step_name]
                    step_stats_by_name[step.step_name] = StepStats(
                        step_name=step.step_name,
                        completed_count=existing.completed_count + step.completed_count,
                        failed_count=existing.failed_count + step.failed_count,
                        total_count=existing.total_count + step.total_count,
                    )
                else:
                    # Copy to avoid mutating original
                    step_stats_by_name[step.step_name] = StepStats(
                        step_name=step.step_name,
                        completed_count=step.completed_count,
                        failed_count=step.failed_count,
                        total_count=step.total_count,
                    )

        worker_count = len(bucket.worker_stats)
        avg_cpu = total_cpu / worker_count if worker_count > 0 else 0.0
        avg_memory = total_memory / worker_count if worker_count > 0 else 0.0

        return WindowedStatsPush(
            job_id=bucket.job_id,
            workflow_id=bucket.workflow_id,
            workflow_name=bucket.workflow_name,
            window_start=bucket.window_start,
            window_end=bucket.window_end,
            completed_count=total_completed,
            failed_count=total_failed,
            rate_per_second=total_rate,
            step_stats=list(step_stats_by_name.values()),
            worker_count=worker_count,
            avg_cpu_percent=avg_cpu,
            avg_memory_mb=avg_memory,
            is_aggregated=True,
        )

    def _unaggregated_bucket(self, bucket: WindowBucket) -> WindowedStatsPush:
        """Return bucket with per-worker stats (for gate forwarding)."""
        per_worker_stats: list[WorkerWindowStats] = []

        for worker_id, progress in bucket.worker_stats.items():
            per_worker_stats.append(
                WorkerWindowStats(
                    worker_id=worker_id,
                    completed_count=progress.completed_count,
                    failed_count=progress.failed_count,
                    rate_per_second=progress.rate_per_second,
                    step_stats=list(progress.step_stats),
                    avg_cpu_percent=progress.avg_cpu_percent,
                    avg_memory_mb=progress.avg_memory_mb,
                )
            )

        return WindowedStatsPush(
            job_id=bucket.job_id,
            workflow_id=bucket.workflow_id,
            workflow_name=bucket.workflow_name,
            window_start=bucket.window_start,
            window_end=bucket.window_end,
            per_worker_stats=per_worker_stats,
            worker_count=len(per_worker_stats),
            is_aggregated=False,
        )

    async def flush_job_windows(
        self,
        job_id: str,
        aggregate: bool = True,
    ) -> list[WindowedStatsPush]:
        """
        Flush ALL pending windows for a job, ignoring drift tolerance.

        Called when a job completes to get final stats before cleanup.
        Unlike flush_closed_job_windows, this doesn't wait for drift tolerance
        since we know no more updates are coming.

        Args:
            job_id: The job identifier to flush.
            aggregate: If True, aggregate stats within window.

        Returns:
            List of WindowedStatsPush messages for the job.
        """
        results: list[WindowedStatsPush] = []

        async with self._lock:
            keys_to_flush = [key for key in self._buckets.keys() if key[0] == job_id]

            for key in keys_to_flush:
                bucket = self._buckets[key]
                if aggregate:
                    push = self._aggregate_bucket(bucket)
                else:
                    push = self._unaggregated_bucket(bucket)
                results.append(push)
                del self._buckets[key]

        return results

    async def cleanup_job_windows(self, job_id: str) -> int:
        """
        Remove all windows for a completed job.

        Called when a job completes to free memory.
        NOTE: Consider using flush_job_windows first to get final stats.

        Args:
            job_id: The job identifier to clean up.

        Returns:
            Number of windows removed.
        """
        async with self._lock:
            keys_to_remove = [key for key in self._buckets.keys() if key[0] == job_id]
            for key in keys_to_remove:
                del self._buckets[key]
            return len(keys_to_remove)

    async def cleanup_workflow_windows(self, job_id: str, workflow_id: str) -> int:
        """
        Remove all windows for a completed workflow.

        Called when a workflow completes to free memory.

        Args:
            job_id: The job identifier.
            workflow_id: The workflow identifier to clean up.

        Returns:
            Number of windows removed.
        """
        async with self._lock:
            keys_to_remove = [
                key
                for key in self._buckets.keys()
                if key[0] == job_id and key[1] == workflow_id
            ]
            for key in keys_to_remove:
                del self._buckets[key]
            return len(keys_to_remove)

    def get_pending_window_count(self) -> int:
        """Get the number of windows currently being collected."""
        return len(self._buckets)

    def get_pending_windows_for_job(self, job_id: str) -> int:
        """Get the number of pending windows for a specific job."""
        return sum(1 for key in self._buckets.keys() if key[0] == job_id)

    def get_jobs_with_pending_stats(self) -> list[str]:
        """
        Get list of job IDs that have pending stats windows.

        Used by stats coordinators to determine which jobs need
        stats pushed to clients/gates.

        Returns:
            List of unique job IDs with pending windows.
        """
        job_ids: set[str] = set()
        for job_id, _, _ in self._buckets.keys():
            job_ids.add(job_id)
        return list(job_ids)

    async def get_aggregated_stats(self, job_id: str) -> list[WindowedStatsPush]:
        """
        Get aggregated stats for a job's closed windows.

        Flushes closed windows for the specified job and returns them
        as aggregated WindowedStatsPush messages. Windows that are not
        yet closed (within drift tolerance) are left in place.

        This is the primary method used by GateStatsCoordinator to
        push periodic stats to clients.

        Args:
            job_id: The job identifier.

        Returns:
            List of WindowedStatsPush for closed windows belonging to this job.
        """
        return await self.flush_closed_job_windows(job_id, aggregate=True)

    async def flush_closed_job_windows(
        self,
        job_id: str,
        aggregate: bool,
    ) -> list[WindowedStatsPush]:
        """
        Flush one job's closed windows, leaving its open windows and every
        other job's windows in place.

        A window is closed once the current time passes its end plus the
        drift tolerance, so late-arriving stats are still counted. A window
        older than the maximum window age that has still not closed (stats
        stamped by a clock running ahead) is dropped.

        Args:
            job_id: The job identifier.
            aggregate: If True, aggregate stats within window.
                      If False, return per-worker stats (for Gate forwarding).

        Returns:
            List of WindowedStatsPush for closed windows belonging to this job.
        """
        now = _DEFAULT_CLOCK.time()
        results: list[WindowedStatsPush] = []
        keys_to_remove: list[tuple[str, str, int]] = []

        async with self._lock:
            for key, bucket in self._buckets.items():
                if key[0] != job_id:
                    continue

                _, _, bucket_num = key
                if self._is_window_closed(bucket_num, now):
                    push = (
                        self._aggregate_bucket(bucket)
                        if aggregate
                        else self._unaggregated_bucket(bucket)
                    )
                    results.append(push)
                    keys_to_remove.append(key)
                    self._metrics.windows_flushed += 1

                elif (now - bucket.created_at) * 1000 > self._max_window_age_ms:
                    keys_to_remove.append(key)
                    self._metrics.windows_dropped_late += 1
                    self._metrics.stats_dropped_late += len(bucket.worker_stats)

            for key in keys_to_remove:
                del self._buckets[key]

        return results

    async def record(self, worker_id: str, progress: WorkflowProgress) -> bool:
        return await self.add_progress(worker_id, progress)

    def get_metrics(self) -> WindowedStatsMetrics:
        return self._metrics

    def reset_metrics(self) -> None:
        self._metrics = WindowedStatsMetrics()

_REHOMED = (
    WorkerWindowStats,
    WindowedStatsPush,
    WindowBucket,
    WindowedStatsMetrics,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
