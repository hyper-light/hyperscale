"""
Dispatch time tracking for gate-side job latency measurement (AD-45).
"""

from __future__ import annotations

import time


class DispatchTimeTracker:
    """
    Tracks dispatch and completion times for jobs routed to datacenters.
    """

    def __init__(self) -> None:
        self._dispatch_times: dict[tuple[str, str], float] = {}

    def record_dispatch(self, job_id: str, datacenter_id: str) -> float:
        """
        Record a dispatch time for a job and datacenter.
        """
        dispatch_time = time.monotonic()
        self._dispatch_times[(job_id, datacenter_id)] = dispatch_time
        return dispatch_time

    def record_completion(
        self,
        job_id: str,
        datacenter_id: str,
        success: bool,
    ) -> float | None:
        """
        Record completion time and return latency in milliseconds.
        """
        dispatch_time = self._dispatch_times.pop((job_id, datacenter_id), None)
        if dispatch_time is None:
            return None

        latency_ms = (time.monotonic() - dispatch_time) * 1000.0
        if not success:
            return None
        return latency_ms
