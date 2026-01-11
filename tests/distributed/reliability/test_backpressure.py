"""
Integration tests for Backpressure (AD-23).

Tests:
- StatsBuffer tiered storage and aggregation
- BackpressureLevel thresholds
- Tier promotion (HOT -> WARM -> COLD)
- BackpressureSignal generation
"""

import time
from unittest.mock import patch

import pytest

from hyperscale.distributed.reliability import (
    BackpressureLevel,
    BackpressureSignal,
    StatsBuffer,
    StatsBufferConfig,
    StatsEntry,
)


class TestStatsEntry:
    """Test StatsEntry basic operations."""

    def test_create_entry(self) -> None:
        """Test creating a stats entry."""
        entry = StatsEntry(timestamp=100.0, value=50.0)

        assert entry.timestamp == 100.0
        assert entry.value == 50.0
        assert entry.count == 1
        assert entry.min_value == 50.0
        assert entry.max_value == 50.0
        assert entry.sum_value == 50.0

    def test_aggregate_entries(self) -> None:
        """Test aggregating multiple entries."""
        entries = [
            StatsEntry(timestamp=100.0, value=10.0),
            StatsEntry(timestamp=101.0, value=20.0),
            StatsEntry(timestamp=102.0, value=30.0),
        ]

        aggregated = StatsEntry.aggregate(entries)

        assert aggregated.timestamp == 102.0  # Latest timestamp
        assert aggregated.value == 20.0  # Average
        assert aggregated.count == 3
        assert aggregated.min_value == 10.0
        assert aggregated.max_value == 30.0
        assert aggregated.sum_value == 60.0

    def test_aggregate_already_aggregated(self) -> None:
        """Test aggregating entries that were already aggregated."""
        entry1 = StatsEntry(
            timestamp=100.0,
            value=15.0,
            count=2,
            min_value=10.0,
            max_value=20.0,
            sum_value=30.0,
        )
        entry2 = StatsEntry(
            timestamp=200.0,
            value=25.0,
            count=2,
            min_value=20.0,
            max_value=30.0,
            sum_value=50.0,
        )

        aggregated = StatsEntry.aggregate([entry1, entry2])

        assert aggregated.count == 4
        assert aggregated.min_value == 10.0
        assert aggregated.max_value == 30.0
        assert aggregated.sum_value == 80.0
        assert aggregated.value == 20.0  # 80/4

    def test_aggregate_empty_raises(self) -> None:
        """Test that aggregating empty list raises."""
        with pytest.raises(ValueError, match="Cannot aggregate empty"):
            StatsEntry.aggregate([])


class TestStatsBuffer:
    """Test StatsBuffer operations."""

    def test_record_value(self) -> None:
        """Test recording a single value."""
        buffer = StatsBuffer()

        result = buffer.record(100.0)

        assert result is True
        assert len(buffer.get_hot_stats()) == 1
        assert buffer.get_hot_stats()[0].value == 100.0

    def test_record_multiple_values(self) -> None:
        """Test recording multiple values."""
        buffer = StatsBuffer()

        buffer.record(10.0)
        buffer.record(20.0)
        buffer.record(30.0)

        stats = buffer.get_hot_stats()
        assert len(stats) == 3
        assert [s.value for s in stats] == [10.0, 20.0, 30.0]

    def test_record_with_timestamp(self) -> None:
        """Test recording with explicit timestamp."""
        buffer = StatsBuffer()

        buffer.record(100.0, timestamp=12345.0)

        stats = buffer.get_hot_stats()
        assert stats[0].timestamp == 12345.0

    def test_record_batch(self) -> None:
        """Test recording a batch of values."""
        buffer = StatsBuffer()

        values = [(10.0, None), (20.0, None), (30.0, None)]
        recorded = buffer.record_batch(values)

        assert recorded == 3
        assert len(buffer.get_hot_stats()) == 3

    def test_get_recent_average(self) -> None:
        """Test getting recent average."""
        buffer = StatsBuffer()

        # Record some values
        now = time.monotonic()
        buffer.record(10.0, now - 10)
        buffer.record(20.0, now - 5)
        buffer.record(30.0, now)

        avg = buffer.get_recent_average(window_seconds=60.0)

        assert avg == 20.0

    def test_get_recent_average_with_window(self) -> None:
        """Test recent average respects window."""
        buffer = StatsBuffer()

        now = time.monotonic()
        buffer.record(100.0, now - 120)  # 2 minutes ago - outside window
        buffer.record(10.0, now - 30)  # 30 seconds ago - inside window
        buffer.record(20.0, now)  # Now - inside window

        avg = buffer.get_recent_average(window_seconds=60.0)

        assert avg == 15.0  # Only includes 10 and 20

    def test_get_recent_average_empty(self) -> None:
        """Test recent average with no data in window."""
        buffer = StatsBuffer()

        avg = buffer.get_recent_average()

        assert avg is None

    def test_clear(self) -> None:
        """Test clearing the buffer."""
        buffer = StatsBuffer()

        buffer.record(10.0)
        buffer.record(20.0)
        buffer.clear()

        assert len(buffer.get_hot_stats()) == 0
        assert len(buffer.get_warm_stats()) == 0
        assert len(buffer.get_cold_stats()) == 0

    def test_metrics(self) -> None:
        """Test getting buffer metrics."""
        buffer = StatsBuffer()

        buffer.record(10.0)
        buffer.record(20.0)

        metrics = buffer.get_metrics()

        assert metrics["hot_count"] == 2
        assert metrics["total_recorded"] == 2
        assert metrics["total_dropped"] == 0
        assert metrics["backpressure_level"] == "NONE"


class TestBackpressureLevels:
    """Test backpressure level thresholds."""

    def test_none_when_empty(self) -> None:
        """Test NONE level when buffer is empty."""
        buffer = StatsBuffer()

        level = buffer.get_backpressure_level()

        assert level == BackpressureLevel.NONE

    def test_none_below_throttle_threshold(self) -> None:
        """Test NONE level below throttle threshold."""
        config = StatsBufferConfig(hot_max_entries=100)
        buffer = StatsBuffer(config=config)

        # Fill to 50% - below 70% throttle threshold
        for i in range(50):
            buffer.record(float(i))

        level = buffer.get_backpressure_level()

        assert level == BackpressureLevel.NONE

    def test_throttle_at_threshold(self) -> None:
        """Test THROTTLE level at throttle threshold."""
        config = StatsBufferConfig(hot_max_entries=100, throttle_threshold=0.70)
        buffer = StatsBuffer(config=config)

        # Fill to 75% - above 70% throttle threshold
        for i in range(75):
            buffer.record(float(i))

        level = buffer.get_backpressure_level()

        assert level == BackpressureLevel.THROTTLE

    def test_batch_at_threshold(self) -> None:
        """Test BATCH level at batch threshold."""
        config = StatsBufferConfig(hot_max_entries=100, batch_threshold=0.85)
        buffer = StatsBuffer(config=config)

        # Fill to 90% - above 85% batch threshold
        for i in range(90):
            buffer.record(float(i))

        level = buffer.get_backpressure_level()

        assert level == BackpressureLevel.BATCH

    def test_reject_at_threshold(self) -> None:
        """Test REJECT level at reject threshold."""
        config = StatsBufferConfig(hot_max_entries=100, reject_threshold=0.95)
        buffer = StatsBuffer(config=config)

        # Fill to 98% - above 95% reject threshold
        for i in range(98):
            buffer.record(float(i))

        level = buffer.get_backpressure_level()

        assert level == BackpressureLevel.REJECT

    def test_record_drops_at_reject(self) -> None:
        """Test that recording drops values at REJECT level."""
        config = StatsBufferConfig(hot_max_entries=100, reject_threshold=0.95)
        buffer = StatsBuffer(config=config)

        # Fill to reject level
        for i in range(98):
            buffer.record(float(i))

        # Try to record more
        result = buffer.record(999.0)

        assert result is False
        metrics = buffer.get_metrics()
        assert metrics["total_dropped"] >= 1


class TestTierPromotion:
    """Test tier promotion from HOT to WARM to COLD."""

    def test_hot_to_warm_promotion(self) -> None:
        """Test promotion from HOT to WARM."""
        config = StatsBufferConfig(
            hot_max_entries=100,
            hot_max_age_seconds=1.0,  # Short age for testing
            warm_aggregate_seconds=0.5,  # Promote every 0.5s
        )
        buffer = StatsBuffer(config=config)

        # Record some entries with old timestamps
        old_time = time.monotonic() - 2.0  # 2 seconds ago
        buffer.record(10.0, old_time)
        buffer.record(20.0, old_time + 0.1)

        # Record new entry to trigger promotion check
        buffer.record(100.0)

        # Force promotion by calling internal method
        buffer._last_warm_promotion = time.monotonic() - 1.0
        buffer._maybe_promote_tiers()

        # Old entries should be in WARM tier
        warm_stats = buffer.get_warm_stats()
        assert len(warm_stats) >= 1
        assert warm_stats[0].count == 2  # Two entries aggregated

    def test_summary_computation(self) -> None:
        """Test archive summary computation."""
        buffer = StatsBuffer()

        buffer.record(10.0)
        buffer.record(20.0)
        buffer.record(30.0)

        summary = buffer.get_summary()

        assert summary is not None
        assert summary.value == 20.0  # Average
        assert summary.count == 3
        assert summary.min_value == 10.0
        assert summary.max_value == 30.0

    def test_summary_cached(self) -> None:
        """Test that summary is cached until new data."""
        buffer = StatsBuffer()

        buffer.record(10.0)
        summary1 = buffer.get_summary()

        # Same summary without new data
        summary2 = buffer.get_summary()
        assert summary1 is summary2

        # New data invalidates cache
        buffer.record(20.0)
        summary3 = buffer.get_summary()
        assert summary3 is not summary1


class TestBackpressureSignal:
    """Test BackpressureSignal generation."""

    def test_from_level_none(self) -> None:
        """Test signal for NONE level."""
        signal = BackpressureSignal.from_level(BackpressureLevel.NONE)

        assert signal.level == BackpressureLevel.NONE
        assert signal.suggested_delay_ms == 0
        assert signal.batch_only is False
        assert signal.drop_non_critical is False

    def test_from_level_throttle(self) -> None:
        """Test signal for THROTTLE level."""
        signal = BackpressureSignal.from_level(BackpressureLevel.THROTTLE)

        assert signal.level == BackpressureLevel.THROTTLE
        assert signal.suggested_delay_ms == 100
        assert signal.batch_only is False
        assert signal.drop_non_critical is False

    def test_from_level_batch(self) -> None:
        """Test signal for BATCH level."""
        signal = BackpressureSignal.from_level(BackpressureLevel.BATCH)

        assert signal.level == BackpressureLevel.BATCH
        assert signal.suggested_delay_ms == 500
        assert signal.batch_only is True
        assert signal.drop_non_critical is False

    def test_from_level_reject(self) -> None:
        """Test signal for REJECT level."""
        signal = BackpressureSignal.from_level(BackpressureLevel.REJECT)

        assert signal.level == BackpressureLevel.REJECT
        assert signal.suggested_delay_ms == 1000
        assert signal.batch_only is True
        assert signal.drop_non_critical is True

    def test_to_dict_roundtrip(self) -> None:
        """Test serialization roundtrip."""
        original = BackpressureSignal(
            level=BackpressureLevel.BATCH,
            suggested_delay_ms=250,
            batch_only=True,
            drop_non_critical=False,
        )

        data = original.to_dict()
        restored = BackpressureSignal.from_dict(data)

        assert restored.level == original.level
        assert restored.suggested_delay_ms == original.suggested_delay_ms
        assert restored.batch_only == original.batch_only
        assert restored.drop_non_critical == original.drop_non_critical


class TestBackpressureLevelEnum:
    """Test BackpressureLevel enum ordering."""

    def test_level_ordering(self) -> None:
        """Test that levels are correctly ordered."""
        assert BackpressureLevel.NONE < BackpressureLevel.THROTTLE
        assert BackpressureLevel.THROTTLE < BackpressureLevel.BATCH
        assert BackpressureLevel.BATCH < BackpressureLevel.REJECT

    def test_level_values(self) -> None:
        """Test level numeric values."""
        assert BackpressureLevel.NONE == 0
        assert BackpressureLevel.THROTTLE == 1
        assert BackpressureLevel.BATCH == 2
        assert BackpressureLevel.REJECT == 3


class TestRingBufferBehavior:
    """Test that HOT tier behaves as a ring buffer."""

    def test_ring_buffer_overflow(self) -> None:
        """Test that old entries are evicted when buffer is full."""
        # Set reject_threshold to 2.0 (200%) to disable backpressure rejection
        # so we can test the pure ring buffer eviction behavior
        config = StatsBufferConfig(hot_max_entries=5, reject_threshold=2.0)
        buffer = StatsBuffer(config=config)

        # Record more than capacity
        for i in range(10):
            buffer.record(float(i))

        stats = buffer.get_hot_stats()

        # Should only have last 5 entries
        assert len(stats) == 5
        assert [s.value for s in stats] == [5.0, 6.0, 7.0, 8.0, 9.0]
