"""
Backpressure for Stats Updates (AD-23).

Provides tiered retention for stats with automatic aggregation and
backpressure signaling based on buffer fill levels.

Retention Tiers:
- HOT: 0-60s, full resolution, ring buffer (max 1000 entries)
- WARM: 1-60min, 10s aggregates (max 360 entries)
- COLD: 1-24h, 1min aggregates (max 1440 entries)
- ARCHIVE: final summary only

Backpressure Levels:
- NONE: <70% fill, accept all
- THROTTLE: 70-85% fill, reduce frequency
- BATCH: 85-95% fill, batched updates only
- REJECT: >95% fill, reject non-critical
"""

import time
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Generic, TypeVar, Callable


class BackpressureLevel(IntEnum):
    """Backpressure levels for stats updates."""

    NONE = 0  # Accept all updates
    THROTTLE = 1  # Reduce update frequency
    BATCH = 2  # Accept batched updates only
    REJECT = 3  # Reject non-critical updates


@dataclass(slots=True)
class StatsEntry:
    """A single stats entry with timestamp."""

    timestamp: float
    value: float
    count: int = 1  # Number of entries aggregated (1 for raw, >1 for aggregated)
    min_value: float | None = None
    max_value: float | None = None
    sum_value: float | None = None

    def __post_init__(self) -> None:
        if self.min_value is None:
            self.min_value = self.value
        if self.max_value is None:
            self.max_value = self.value
        if self.sum_value is None:
            self.sum_value = self.value

    @classmethod
    def aggregate(cls, entries: list["StatsEntry"]) -> "StatsEntry":
        """Aggregate multiple entries into a single entry."""
        if not entries:
            raise ValueError("Cannot aggregate empty list")

        total_count = sum(e.count for e in entries)
        total_sum = sum(e.sum_value or e.value for e in entries)
        min_val = min(e.min_value or e.value for e in entries)
        max_val = max(e.max_value or e.value for e in entries)

        return cls(
            timestamp=entries[-1].timestamp,  # Use latest timestamp
            value=total_sum / total_count,  # Average value
            count=total_count,
            min_value=min_val,
            max_value=max_val,
            sum_value=total_sum,
        )


@dataclass(slots=True)
class StatsBufferConfig:
    """Configuration for StatsBuffer."""

    # HOT tier settings
    hot_max_entries: int = 1000
    hot_max_age_seconds: float = 60.0

    # WARM tier settings (10s aggregates)
    warm_max_entries: int = 360
    warm_aggregate_seconds: float = 10.0
    warm_max_age_seconds: float = 3600.0  # 1 hour

    # COLD tier settings (1min aggregates)
    cold_max_entries: int = 1440
    cold_aggregate_seconds: float = 60.0
    cold_max_age_seconds: float = 86400.0  # 24 hours

    # Backpressure thresholds (as fraction of hot tier capacity)
    throttle_threshold: float = 0.70
    batch_threshold: float = 0.85
    reject_threshold: float = 0.95


class StatsBuffer:
    """
    Tiered stats buffer with automatic aggregation and backpressure signaling.

    Stores stats in three tiers with automatic promotion:
    - HOT: Full resolution recent data
    - WARM: 10-second aggregates
    - COLD: 1-minute aggregates

    Example usage:
        buffer = StatsBuffer()

        # Record stats
        buffer.record(100.5)
        buffer.record(102.3)

        # Check backpressure level
        level = buffer.get_backpressure_level()
        if level >= BackpressureLevel.REJECT:
            return "backpressure"

        # Get recent stats
        recent = buffer.get_hot_stats()

        # Get aggregated stats for longer periods
        hourly = buffer.get_warm_stats()
    """

    def __init__(self, config: StatsBufferConfig | None = None):
        self._config = config or StatsBufferConfig()

        # HOT tier: ring buffer for recent full-resolution data
        self._hot: deque[StatsEntry] = deque(maxlen=self._config.hot_max_entries)

        # WARM tier: 10-second aggregates
        self._warm: deque[StatsEntry] = deque(maxlen=self._config.warm_max_entries)
        self._warm_pending: list[StatsEntry] = []  # Entries being aggregated

        # COLD tier: 1-minute aggregates
        self._cold: deque[StatsEntry] = deque(maxlen=self._config.cold_max_entries)
        self._cold_pending: list[StatsEntry] = []  # Entries being aggregated

        # Archive: final summary (computed lazily)
        self._archive_summary: StatsEntry | None = None
        self._archive_dirty: bool = True

        # Timestamps for tier promotion
        self._last_warm_promotion: float = time.monotonic()
        self._last_cold_promotion: float = time.monotonic()

        # Metrics
        self._total_recorded: int = 0
        self._total_dropped: int = 0

    def record(self, value: float, timestamp: float | None = None) -> bool:
        """
        Record a stats value.

        Args:
            value: The stats value to record
            timestamp: Optional timestamp (defaults to current time)

        Returns:
            True if recorded, False if dropped due to backpressure
        """
        if timestamp is None:
            timestamp = time.monotonic()

        # Check if we should drop due to backpressure
        level = self.get_backpressure_level()
        if level >= BackpressureLevel.REJECT:
            self._total_dropped += 1
            return False

        entry = StatsEntry(timestamp=timestamp, value=value)
        self._hot.append(entry)
        self._total_recorded += 1
        self._archive_dirty = True

        # Check for tier promotions
        self._maybe_promote_tiers()

        return True

    def record_batch(self, values: list[tuple[float, float | None]]) -> int:
        """
        Record a batch of stats values.

        Args:
            values: List of (value, timestamp) tuples

        Returns:
            Number of values actually recorded
        """
        recorded = 0
        for value, timestamp in values:
            if self.record(value, timestamp):
                recorded += 1
        return recorded

    def get_backpressure_level(self) -> BackpressureLevel:
        """
        Get current backpressure level based on buffer fill.

        Returns:
            BackpressureLevel indicating how full the buffer is
        """
        fill_ratio = len(self._hot) / self._config.hot_max_entries

        if fill_ratio >= self._config.reject_threshold:
            return BackpressureLevel.REJECT
        elif fill_ratio >= self._config.batch_threshold:
            return BackpressureLevel.BATCH
        elif fill_ratio >= self._config.throttle_threshold:
            return BackpressureLevel.THROTTLE
        else:
            return BackpressureLevel.NONE

    def get_hot_stats(self) -> list[StatsEntry]:
        """Get all entries from HOT tier."""
        return list(self._hot)

    def get_warm_stats(self) -> list[StatsEntry]:
        """Get all entries from WARM tier."""
        return list(self._warm)

    def get_cold_stats(self) -> list[StatsEntry]:
        """Get all entries from COLD tier."""
        return list(self._cold)

    def get_summary(self) -> StatsEntry | None:
        """
        Get archive summary of all data.

        Lazily computed and cached until new data is added.
        """
        if self._archive_dirty:
            self._compute_archive_summary()
        return self._archive_summary

    def get_recent_average(self, window_seconds: float = 60.0) -> float | None:
        """
        Get average value over recent window.

        Args:
            window_seconds: How far back to look

        Returns:
            Average value, or None if no data in window
        """
        cutoff = time.monotonic() - window_seconds
        recent = [e for e in self._hot if e.timestamp >= cutoff]

        if not recent:
            return None

        total_sum = sum(e.sum_value or e.value for e in recent)
        total_count = sum(e.count for e in recent)
        return total_sum / total_count

    def get_metrics(self) -> dict:
        """Get buffer metrics."""
        return {
            "hot_count": len(self._hot),
            "hot_capacity": self._config.hot_max_entries,
            "hot_fill_ratio": len(self._hot) / self._config.hot_max_entries,
            "warm_count": len(self._warm),
            "warm_capacity": self._config.warm_max_entries,
            "cold_count": len(self._cold),
            "cold_capacity": self._config.cold_max_entries,
            "backpressure_level": self.get_backpressure_level().name,
            "total_recorded": self._total_recorded,
            "total_dropped": self._total_dropped,
        }

    def get_backpressure_signal(self) -> "BackpressureSignal":
        """
        Get current backpressure signal for embedding in responses.

        This is a convenience wrapper that converts the backpressure level
        to a full BackpressureSignal with suggested delays and behaviors.

        Returns:
            BackpressureSignal with level, suggested delay, and behavior hints
        """
        level = self.get_backpressure_level()
        return BackpressureSignal.from_level(level)

    def clear(self) -> None:
        """Clear all data from all tiers."""
        self._hot.clear()
        self._warm.clear()
        self._cold.clear()
        self._warm_pending.clear()
        self._cold_pending.clear()
        self._archive_summary = None
        self._archive_dirty = True
        self._total_recorded = 0
        self._total_dropped = 0

    def _maybe_promote_tiers(self) -> None:
        """Check and perform tier promotions if needed."""
        now = time.monotonic()

        # HOT -> WARM promotion (every 10 seconds)
        if now - self._last_warm_promotion >= self._config.warm_aggregate_seconds:
            self._promote_hot_to_warm()
            self._last_warm_promotion = now

        # WARM -> COLD promotion (every 1 minute)
        if now - self._last_cold_promotion >= self._config.cold_aggregate_seconds:
            self._promote_warm_to_cold()
            self._last_cold_promotion = now

    def _promote_hot_to_warm(self) -> None:
        """Aggregate old HOT entries and promote to WARM."""
        now = time.monotonic()
        cutoff = now - self._config.hot_max_age_seconds

        # Find entries to promote (older than hot max age)
        to_promote: list[StatsEntry] = []
        while self._hot and self._hot[0].timestamp < cutoff:
            to_promote.append(self._hot.popleft())

        if to_promote:
            # Aggregate into single entry
            aggregated = StatsEntry.aggregate(to_promote)
            self._warm.append(aggregated)

    def _promote_warm_to_cold(self) -> None:
        """Aggregate old WARM entries and promote to COLD."""
        now = time.monotonic()
        cutoff = now - self._config.warm_max_age_seconds

        # Find entries to promote (older than warm max age)
        to_promote: list[StatsEntry] = []
        while self._warm and self._warm[0].timestamp < cutoff:
            to_promote.append(self._warm.popleft())

        if to_promote:
            # Aggregate into single entry
            aggregated = StatsEntry.aggregate(to_promote)
            self._cold.append(aggregated)

    def _compute_archive_summary(self) -> None:
        """Compute archive summary from all tiers."""
        all_entries: list[StatsEntry] = []
        all_entries.extend(self._hot)
        all_entries.extend(self._warm)
        all_entries.extend(self._cold)

        if all_entries:
            self._archive_summary = StatsEntry.aggregate(all_entries)
        else:
            self._archive_summary = None

        self._archive_dirty = False

    def export_checkpoint(self) -> list[tuple[float, float]]:
        """
        Export pending stats as a checkpoint for recovery (Task 33).

        Returns a list of (timestamp, value) tuples from the HOT tier.
        WARM and COLD tiers are aggregated and less critical for recovery.
        """
        return [(entry.timestamp, entry.value) for entry in self._hot]

    def import_checkpoint(self, checkpoint: list[tuple[float, float]]) -> int:
        """
        Import stats from a checkpoint during recovery (Task 33).

        Only imports entries that are newer than our current oldest entry
        to avoid duplicating data.

        Args:
            checkpoint: List of (timestamp, value) tuples

        Returns:
            Number of entries imported
        """
        if not checkpoint:
            return 0

        oldest_timestamp = float("inf")
        if self._hot:
            oldest_timestamp = self._hot[0].timestamp

        imported = 0
        for timestamp, value in checkpoint:
            if timestamp >= oldest_timestamp:
                continue
            entry = StatsEntry(timestamp=timestamp, value=value)
            self._hot.appendleft(entry)
            imported += 1

        if imported > 0:
            self._archive_dirty = True

        return imported


@dataclass(slots=True)
class BackpressureSignal:
    """
    Backpressure signal to include in responses.

    This signal tells the sender how to adjust their behavior.
    """

    level: BackpressureLevel
    suggested_delay_ms: int = 0
    batch_only: bool = False
    drop_non_critical: bool = False

    @property
    def delay_ms(self) -> int:
        return self.suggested_delay_ms

    @classmethod
    def from_level(
        cls,
        level: BackpressureLevel,
        throttle_delay_ms: int = 100,
        batch_delay_ms: int = 500,
        reject_delay_ms: int = 1000,
    ) -> "BackpressureSignal":
        """
        Create signal from backpressure level.

        Args:
            level: The backpressure level to signal.
            throttle_delay_ms: Suggested delay for THROTTLE level (default: 100ms).
            batch_delay_ms: Suggested delay for BATCH level (default: 500ms).
            reject_delay_ms: Suggested delay for REJECT level (default: 1000ms).
        """
        if level == BackpressureLevel.NONE:
            return cls(level=level)
        elif level == BackpressureLevel.THROTTLE:
            return cls(level=level, suggested_delay_ms=throttle_delay_ms)
        elif level == BackpressureLevel.BATCH:
            return cls(level=level, suggested_delay_ms=batch_delay_ms, batch_only=True)
        else:  # REJECT
            return cls(
                level=level,
                suggested_delay_ms=reject_delay_ms,
                batch_only=True,
                drop_non_critical=True,
            )

    def to_dict(self) -> dict:
        """Serialize to dictionary for embedding in messages."""
        return {
            "level": self.level.value,
            "suggested_delay_ms": self.suggested_delay_ms,
            "batch_only": self.batch_only,
            "drop_non_critical": self.drop_non_critical,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BackpressureSignal":
        """Deserialize from dictionary."""
        return cls(
            level=BackpressureLevel(data.get("level", 0)),
            suggested_delay_ms=data.get("suggested_delay_ms", 0),
            batch_only=data.get("batch_only", False),
            drop_non_critical=data.get("drop_non_critical", False),
        )
