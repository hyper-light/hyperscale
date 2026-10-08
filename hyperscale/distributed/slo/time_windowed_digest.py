from __future__ import annotations


from .latency_observation import LatencyObservation
from .slo_config import SLOConfig
from .tdigest import TDigest


class TimeWindowedTDigest:
    """Maintains multiple T-Digest buckets by time window.

    Times are the owning node's monotonic clock readings, passed in by
    the caller.
    """

    def __init__(self, config: SLOConfig) -> None:
        self._config = config
        self._window_duration_seconds = self._config.window_duration_seconds
        self._max_windows = self._config.max_windows
        self._windows: dict[float, TDigest] = {}
        self._window_order: list[float] = []

    def _window_end(self, window_start: float) -> float:
        return window_start + self._window_duration_seconds

    def _register_window(self, window_start: float) -> None:
        if window_start not in self._windows:
            self._windows[window_start] = TDigest(_config=self._config)
            self._window_order.append(window_start)
            self._window_order.sort()

    def _prune_windows(self, reference_time: float) -> None:
        cutoff_time = reference_time - self._window_duration_seconds * self._max_windows
        retained_windows: list[float] = []
        for window_start in self._window_order:
            if self._window_end(window_start) >= cutoff_time:
                retained_windows.append(window_start)
            else:
                self._windows.pop(window_start, None)
        self._window_order = retained_windows

        while len(self._window_order) > self._max_windows:
            oldest_start = self._window_order.pop(0)
            self._windows.pop(oldest_start, None)

    def add(self, value: float, timestamp: float, weight: float = 1.0) -> None:
        """Add a value to the time window ``timestamp`` falls in.

        Windows are pruned when a new one opens, not on every add: the
        window cap keeps at most ``max_windows`` digests, and
        ``get_recent_observation`` prunes by age before it reads, so a
        window that ages out between openings is never observed. A sample
        older than every retained window is dropped, as pruning after the
        add dropped it.
        """
        window_start = int(timestamp / self._window_duration_seconds) * self._window_duration_seconds
        if (window_digest := self._windows.get(window_start)) is None:
            self._register_window(window_start)
            self._prune_windows(timestamp)
            window_digest = self._windows.get(window_start)
        if window_digest is not None:
            window_digest.add(value, weight)

    def add_batch(self, values: list[float], timestamp: float) -> None:
        """Add multiple values into the same time window."""
        for value in values:
            self.add(value, timestamp)

    def get_recent_observation(
        self,
        target_id: str,
        now: float,
    ) -> LatencyObservation | None:
        """Aggregate the windows recent at ``now`` into a latency observation."""
        self._prune_windows(now)
        if not self._window_order:
            return None

        aggregated_digest = TDigest(_config=self._config)
        for window_start in self._window_order:
            aggregated_digest.merge(self._windows[window_start])

        if aggregated_digest.count() <= 0:
            return None

        window_start = min(self._window_order)
        window_end = max(self._window_order) + self._window_duration_seconds

        return LatencyObservation(
            target_id=target_id,
            p50_ms=aggregated_digest.p50(),
            p95_ms=aggregated_digest.p95(),
            p99_ms=aggregated_digest.p99(),
            sample_count=int(aggregated_digest.count()),
            window_start=window_start,
            window_end=window_end,
        )
