"""
Dispatch time tracking for gate-side job latency measurement (AD-45).
"""

from __future__ import annotations

import asyncio
import time


class DispatchTimeTracker:
    """
    Tracks dispatch and completion times for jobs routed to datacenters.
    """

    def __init__(self, stale_threshold_seconds: float = 600.0) -> None:
        self._dispatch_times: dict[tuple[str, str], float] = {}
        self._lock = asyncio.Lock()
        self._stale_threshold_seconds = stale_threshold_seconds

    async def record_dispatch(self, job_id: str, datacenter_id: str) -> float:
        dispatch_time = time.monotonic()
        async with self._lock:
            self._dispatch_times[(job_id, datacenter_id)] = dispatch_time
        return dispatch_time

    async def record_completion(
        self,
        job_id: str,
        datacenter_id: str,
        success: bool,
    ) -> float | None:
        async with self._lock:
            dispatch_time = self._dispatch_times.pop((job_id, datacenter_id), None)
        if dispatch_time is None:
            return None

        latency_ms = (time.monotonic() - dispatch_time) * 1000.0
        if not success:
            return None
        return latency_ms

    async def cleanup_stale_entries(self) -> int:
        now = time.monotonic()
        stale_cutoff = now - self._stale_threshold_seconds
        async with self._lock:
            stale_keys = [
                key
                for key, dispatch_time in self._dispatch_times.items()
                if dispatch_time < stale_cutoff
            ]
            for key in stale_keys:
                self._dispatch_times.pop(key, None)
        return len(stale_keys)

    async def remove_job(self, job_id: str) -> None:
        async with self._lock:
            keys_to_remove = [key for key in self._dispatch_times if key[0] == job_id]
            for key in keys_to_remove:
                self._dispatch_times.pop(key, None)
