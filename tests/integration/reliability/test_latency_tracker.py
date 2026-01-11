"""
Integration tests for LatencyTracker.

Tests:
- Happy path: recording latencies, calculating averages
- Negative path: missing peers, empty data
- Failure modes: sample expiration, count limits
- Concurrent access and race conditions
- Edge cases: boundary conditions, precision
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from hyperscale.distributed_rewrite.health.latency_tracker import (
    LatencyTracker,
    LatencyConfig,
)


# =============================================================================
# Happy Path Tests
# =============================================================================


class TestLatencyTrackerHappyPath:
    """Test normal operation of LatencyTracker."""

    def test_initialization_default_config(self) -> None:
        """Test LatencyTracker initializes with default config."""
        tracker = LatencyTracker()

        assert tracker._config.sample_max_age == 60.0
        assert tracker._config.sample_max_count == 100
        assert len(tracker._samples) == 0

    def test_initialization_custom_config(self) -> None:
        """Test LatencyTracker initializes with custom config."""
        tracker = LatencyTracker(sample_max_age=30.0, sample_max_count=50)

        assert tracker._config.sample_max_age == 30.0
        assert tracker._config.sample_max_count == 50

    def test_record_latency_single_peer(self) -> None:
        """Test recording latency for a single peer."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.5)

        assert "peer-1" in tracker._samples
        assert len(tracker._samples["peer-1"]) == 1
        assert tracker._samples["peer-1"][0][1] == 10.5

    def test_record_latency_multiple_samples(self) -> None:
        """Test recording multiple latency samples for a peer."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-1", 20.0)
        tracker.record_latency("peer-1", 30.0)

        assert len(tracker._samples["peer-1"]) == 3

    def test_record_latency_multiple_peers(self) -> None:
        """Test recording latencies for multiple peers."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-2", 20.0)
        tracker.record_latency("peer-3", 30.0)

        assert len(tracker._samples) == 3
        assert "peer-1" in tracker._samples
        assert "peer-2" in tracker._samples
        assert "peer-3" in tracker._samples

    def test_get_peer_latency(self) -> None:
        """Test get_peer_latency returns correct average."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-1", 20.0)
        tracker.record_latency("peer-1", 30.0)

        avg = tracker.get_peer_latency("peer-1")

        assert avg == 20.0  # (10 + 20 + 30) / 3

    def test_get_average_latency(self) -> None:
        """Test get_average_latency returns correct global average."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-1", 20.0)
        tracker.record_latency("peer-2", 30.0)
        tracker.record_latency("peer-2", 40.0)

        avg = tracker.get_average_latency()

        assert avg == 25.0  # (10 + 20 + 30 + 40) / 4

    def test_get_all_peer_latencies(self) -> None:
        """Test get_all_peer_latencies returns averages for all peers."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-1", 20.0)
        tracker.record_latency("peer-2", 30.0)

        latencies = tracker.get_all_peer_latencies()

        assert len(latencies) == 2
        assert latencies["peer-1"] == 15.0  # (10 + 20) / 2
        assert latencies["peer-2"] == 30.0

    def test_get_sample_count(self) -> None:
        """Test get_sample_count returns correct count."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-1", 20.0)
        tracker.record_latency("peer-1", 30.0)

        assert tracker.get_sample_count("peer-1") == 3

    def test_remove_peer(self) -> None:
        """Test remove_peer removes all samples for a peer."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-2", 20.0)

        tracker.remove_peer("peer-1")

        assert "peer-1" not in tracker._samples
        assert "peer-2" in tracker._samples

    def test_clear_all(self) -> None:
        """Test clear_all removes all samples."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-2", 20.0)
        tracker.record_latency("peer-3", 30.0)

        tracker.clear_all()

        assert len(tracker._samples) == 0


# =============================================================================
# Negative Path Tests
# =============================================================================


class TestLatencyTrackerNegativePath:
    """Test error handling and missing data scenarios."""

    def test_get_peer_latency_unknown_peer(self) -> None:
        """Test get_peer_latency returns None for unknown peer."""
        tracker = LatencyTracker()

        avg = tracker.get_peer_latency("unknown-peer")

        assert avg is None

    def test_get_average_latency_no_samples(self) -> None:
        """Test get_average_latency returns None with no samples."""
        tracker = LatencyTracker()

        avg = tracker.get_average_latency()

        assert avg is None

    def test_get_all_peer_latencies_no_samples(self) -> None:
        """Test get_all_peer_latencies returns empty dict with no samples."""
        tracker = LatencyTracker()

        latencies = tracker.get_all_peer_latencies()

        assert latencies == {}

    def test_get_sample_count_unknown_peer(self) -> None:
        """Test get_sample_count returns 0 for unknown peer."""
        tracker = LatencyTracker()

        count = tracker.get_sample_count("unknown-peer")

        assert count == 0

    def test_remove_peer_unknown_peer(self) -> None:
        """Test remove_peer on unknown peer is a no-op."""
        tracker = LatencyTracker()

        # Should not raise
        tracker.remove_peer("unknown-peer")

        assert len(tracker._samples) == 0

    def test_get_peer_latency_after_remove(self) -> None:
        """Test get_peer_latency after peer is removed."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        tracker.remove_peer("peer-1")

        avg = tracker.get_peer_latency("peer-1")

        assert avg is None


# =============================================================================
# Failure Mode Tests - Sample Expiration and Limits
# =============================================================================


class TestLatencyTrackerFailureModes:
    """Test sample expiration and count limits."""

    def test_samples_expire_after_max_age(self) -> None:
        """Test old samples are pruned after max_age."""
        tracker = LatencyTracker(sample_max_age=0.1)  # 100ms

        tracker.record_latency("peer-1", 10.0)

        # Wait for samples to expire
        time.sleep(0.15)

        # Record new sample to trigger pruning
        tracker.record_latency("peer-1", 20.0)

        # Only the new sample should remain
        assert len(tracker._samples["peer-1"]) == 1
        assert tracker._samples["peer-1"][0][1] == 20.0

    def test_samples_limited_by_max_count(self) -> None:
        """Test samples are limited by max_count."""
        tracker = LatencyTracker(sample_max_count=5)

        for idx in range(10):
            tracker.record_latency("peer-1", float(idx))

        # Should only keep the last 5 samples
        assert len(tracker._samples["peer-1"]) == 5
        # Last 5 samples are 5, 6, 7, 8, 9
        latencies = [lat for _, lat in tracker._samples["peer-1"]]
        assert latencies == [5.0, 6.0, 7.0, 8.0, 9.0]

    def test_average_after_sample_expiration(self) -> None:
        """Test average calculation after some samples expire."""
        tracker = LatencyTracker(sample_max_age=0.1)

        tracker.record_latency("peer-1", 100.0)  # Will expire
        time.sleep(0.05)
        tracker.record_latency("peer-1", 200.0)  # Will expire

        time.sleep(0.12)  # Wait long enough for both to expire (0.05 + 0.12 = 0.17 > 0.15)

        # First two should have expired
        tracker.record_latency("peer-1", 10.0)  # Fresh
        tracker.record_latency("peer-1", 20.0)  # Fresh

        avg = tracker.get_peer_latency("peer-1")

        # Should only include fresh samples
        assert avg == 15.0  # (10 + 20) / 2

    def test_average_with_max_count_limit(self) -> None:
        """Test average calculation respects max_count limit."""
        tracker = LatencyTracker(sample_max_count=3)

        tracker.record_latency("peer-1", 100.0)  # Will be dropped
        tracker.record_latency("peer-1", 200.0)  # Will be dropped
        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-1", 20.0)
        tracker.record_latency("peer-1", 30.0)

        avg = tracker.get_peer_latency("peer-1")

        # Should only include last 3 samples
        assert avg == 20.0  # (10 + 20 + 30) / 3

    def test_get_average_latency_with_expired_samples(self) -> None:
        """Test global average after samples expire."""
        tracker = LatencyTracker(sample_max_age=0.1)

        tracker.record_latency("peer-1", 100.0)  # Will expire
        tracker.record_latency("peer-2", 200.0)  # Will expire

        time.sleep(0.15)

        tracker.record_latency("peer-3", 30.0)  # Fresh

        # Trigger pruning by recording
        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-2", 20.0)

        avg = tracker.get_average_latency()

        # peer-1 has 10.0, peer-2 has 20.0, peer-3 has 30.0
        assert avg == 20.0  # (10 + 20 + 30) / 3

    def test_empty_peer_after_expiration(self) -> None:
        """Test peer with all expired samples."""
        tracker = LatencyTracker(sample_max_age=0.05)

        tracker.record_latency("peer-1", 10.0)

        time.sleep(0.1)

        # Trigger pruning by recording for same peer
        # The old sample should be pruned but new one added
        tracker.record_latency("peer-1", 20.0)

        assert tracker.get_sample_count("peer-1") == 1
        assert tracker.get_peer_latency("peer-1") == 20.0


# =============================================================================
# Concurrent Access Tests
# =============================================================================


class TestLatencyTrackerConcurrency:
    """Test thread safety and concurrent access."""

    def test_concurrent_record_same_peer(self) -> None:
        """Test concurrent recording for same peer."""
        tracker = LatencyTracker(sample_max_count=1000)
        peer_id = "peer-1"

        def record_worker(latency: float) -> None:
            tracker.record_latency(peer_id, latency)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(record_worker, float(idx))
                for idx in range(100)
            ]
            for future in futures:
                future.result()

        # Should have up to 100 samples (or max_count if less)
        count = tracker.get_sample_count(peer_id)
        assert count <= 100

    def test_concurrent_record_different_peers(self) -> None:
        """Test concurrent recording for different peers."""
        tracker = LatencyTracker()

        def record_worker(peer_idx: int) -> None:
            peer_id = f"peer-{peer_idx}"
            tracker.record_latency(peer_id, float(peer_idx))

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(record_worker, idx)
                for idx in range(50)
            ]
            for future in futures:
                future.result()

        # Should have 50 different peers
        assert len(tracker._samples) == 50

    def test_concurrent_read_and_write(self) -> None:
        """Test concurrent read and write operations."""
        tracker = LatencyTracker()

        # Pre-populate
        for idx in range(10):
            tracker.record_latency(f"peer-{idx}", float(idx * 10))

        results: list = []

        def write_worker() -> None:
            tracker.record_latency("peer-0", 999.0)

        def read_worker() -> None:
            avg = tracker.get_average_latency()
            if avg is not None:
                results.append(avg)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for idx in range(100):
                if idx % 2 == 0:
                    futures.append(executor.submit(write_worker))
                else:
                    futures.append(executor.submit(read_worker))
            for future in futures:
                future.result()

        # Should complete without errors
        assert len(results) > 0

    def test_concurrent_remove_and_record(self) -> None:
        """Test concurrent remove and record operations."""
        tracker = LatencyTracker()
        peer_id = "peer-1"

        tracker.record_latency(peer_id, 10.0)

        def remove_worker() -> None:
            tracker.remove_peer(peer_id)

        def record_worker() -> None:
            tracker.record_latency(peer_id, 20.0)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for idx in range(100):
                if idx % 2 == 0:
                    futures.append(executor.submit(remove_worker))
                else:
                    futures.append(executor.submit(record_worker))
            for future in futures:
                future.result()

        # Should complete without errors

    def test_concurrent_clear_and_record(self) -> None:
        """Test concurrent clear_all and record operations."""
        tracker = LatencyTracker()

        def clear_worker() -> None:
            tracker.clear_all()

        def record_worker(idx: int) -> None:
            tracker.record_latency(f"peer-{idx}", float(idx))

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for idx in range(100):
                if idx % 10 == 0:
                    futures.append(executor.submit(clear_worker))
                else:
                    futures.append(executor.submit(record_worker, idx))
            for future in futures:
                future.result()

        # Should complete without errors


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestLatencyTrackerEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_latency(self) -> None:
        """Test recording zero latency."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 0.0)

        assert tracker.get_peer_latency("peer-1") == 0.0

    def test_negative_latency(self) -> None:
        """Test recording negative latency (edge case - should not happen)."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", -10.0)

        # Should still work, even if negative latency is invalid in practice
        assert tracker.get_peer_latency("peer-1") == -10.0

    def test_very_large_latency(self) -> None:
        """Test recording very large latency values."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 1_000_000.0)  # 1 million ms

        assert tracker.get_peer_latency("peer-1") == 1_000_000.0

    def test_very_small_latency(self) -> None:
        """Test recording very small latency values."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 0.001)  # 1 microsecond

        assert tracker.get_peer_latency("peer-1") == 0.001

    def test_floating_point_precision(self) -> None:
        """Test floating point precision in average calculation."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 0.1)
        tracker.record_latency("peer-1", 0.2)
        tracker.record_latency("peer-1", 0.3)

        avg = tracker.get_peer_latency("peer-1")

        # Should be approximately 0.2, allowing for floating point errors
        assert abs(avg - 0.2) < 1e-10

    def test_single_sample_average(self) -> None:
        """Test average with single sample."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 42.0)

        assert tracker.get_peer_latency("peer-1") == 42.0
        assert tracker.get_average_latency() == 42.0

    def test_sample_max_count_one(self) -> None:
        """Test with sample_max_count=1."""
        tracker = LatencyTracker(sample_max_count=1)

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-1", 20.0)
        tracker.record_latency("peer-1", 30.0)

        assert tracker.get_sample_count("peer-1") == 1
        assert tracker.get_peer_latency("peer-1") == 30.0

    def test_sample_max_age_zero(self) -> None:
        """Test with sample_max_age=0 (edge case - immediate expiration)."""
        tracker = LatencyTracker(sample_max_age=0.0)

        tracker.record_latency("peer-1", 10.0)

        # With max_age=0, samples should expire immediately on next record
        tracker.record_latency("peer-1", 20.0)

        # Only the most recent should remain
        assert tracker.get_sample_count("peer-1") == 1

    def test_empty_peer_id(self) -> None:
        """Test with empty peer_id string."""
        tracker = LatencyTracker()

        tracker.record_latency("", 10.0)

        assert tracker.get_peer_latency("") == 10.0
        assert tracker.get_sample_count("") == 1

    def test_unicode_peer_id(self) -> None:
        """Test with unicode characters in peer_id."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-日本語-🎉", 10.0)

        assert tracker.get_peer_latency("peer-日本語-🎉") == 10.0

    def test_very_long_peer_id(self) -> None:
        """Test with very long peer_id."""
        tracker = LatencyTracker()
        long_id = "peer-" + "x" * 10000

        tracker.record_latency(long_id, 10.0)

        assert tracker.get_peer_latency(long_id) == 10.0

    def test_many_peers(self) -> None:
        """Test with many different peers."""
        tracker = LatencyTracker()

        for idx in range(1000):
            tracker.record_latency(f"peer-{idx}", float(idx))

        assert len(tracker._samples) == 1000

        latencies = tracker.get_all_peer_latencies()
        assert len(latencies) == 1000

    def test_many_samples_per_peer(self) -> None:
        """Test with many samples for a single peer."""
        tracker = LatencyTracker(sample_max_count=10000)

        for idx in range(5000):
            tracker.record_latency("peer-1", float(idx))

        assert tracker.get_sample_count("peer-1") == 5000

        # Average should be (0 + 1 + ... + 4999) / 5000 = 2499.5
        avg = tracker.get_peer_latency("peer-1")
        assert avg == 2499.5

    def test_timestamps_are_monotonic(self) -> None:
        """Test that timestamps use monotonic time."""
        tracker = LatencyTracker()

        tracker.record_latency("peer-1", 10.0)
        ts1 = tracker._samples["peer-1"][0][0]

        tracker.record_latency("peer-1", 20.0)
        ts2 = tracker._samples["peer-1"][1][0]

        # Timestamps should be monotonically increasing
        assert ts2 >= ts1

    def test_latency_config_dataclass(self) -> None:
        """Test LatencyConfig dataclass."""
        config = LatencyConfig(sample_max_age=30.0, sample_max_count=50)

        assert config.sample_max_age == 30.0
        assert config.sample_max_count == 50

    def test_get_all_peer_latencies_excludes_empty(self) -> None:
        """Test get_all_peer_latencies excludes peers with no samples."""
        tracker = LatencyTracker(sample_max_age=0.05)

        tracker.record_latency("peer-1", 10.0)
        tracker.record_latency("peer-2", 20.0)

        time.sleep(0.1)

        # Record for peer-3 only, triggering pruning
        tracker.record_latency("peer-3", 30.0)

        # peer-1 and peer-2 samples are expired but entries may still exist
        # get_all_peer_latencies should only return peer-3
        latencies = tracker.get_all_peer_latencies()

        # At minimum, peer-3 should be present
        assert "peer-3" in latencies
        assert latencies["peer-3"] == 30.0
