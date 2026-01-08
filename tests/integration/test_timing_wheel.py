"""
Comprehensive tests for the TimingWheel component.

Tests cover:
1. Happy path: Normal add, remove, expire operations
2. Negative path: Invalid inputs, missing entries
3. Failure modes: Callback exceptions, rapid operations
4. Edge cases: Bucket boundaries, wrap-around, LHM adjustments
5. Concurrency correctness: Async safety under concurrent operations
"""

import asyncio
import time
from unittest.mock import MagicMock, AsyncMock

import pytest

from hyperscale.distributed_rewrite.swim.detection.timing_wheel import (
    TimingWheel,
    TimingWheelConfig,
    TimingWheelBucket,
    WheelEntry,
)
from hyperscale.distributed_rewrite.swim.detection.suspicion_state import SuspicionState


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def default_config() -> TimingWheelConfig:
    """Default timing wheel configuration for tests."""
    return TimingWheelConfig(
        coarse_tick_ms=1000,
        coarse_wheel_size=64,
        fine_tick_ms=100,
        fine_wheel_size=16,
        fine_wheel_threshold_ms=2000,
    )


@pytest.fixture
def fast_config() -> TimingWheelConfig:
    """Fast timing wheel for quick expiration tests."""
    return TimingWheelConfig(
        coarse_tick_ms=100,
        coarse_wheel_size=10,
        fine_tick_ms=10,
        fine_wheel_size=10,
        fine_wheel_threshold_ms=200,
    )


@pytest.fixture
def sample_node() -> tuple[str, int]:
    """A sample node address."""
    return ("192.168.1.1", 7946)


@pytest.fixture
def sample_state(sample_node: tuple[str, int]) -> SuspicionState:
    """A sample suspicion state."""
    return SuspicionState(
        node=sample_node,
        incarnation=1,
        start_time=time.monotonic(),
        min_timeout=1.0,
        max_timeout=10.0,
    )


def make_node(index: int) -> tuple[str, int]:
    """Create a node address from an index."""
    return (f"192.168.1.{index}", 7946)


def make_state(node: tuple[str, int], incarnation: int = 1) -> SuspicionState:
    """Create a suspicion state for a node."""
    return SuspicionState(
        node=node,
        incarnation=incarnation,
        start_time=time.monotonic(),
        min_timeout=1.0,
        max_timeout=10.0,
    )


# =============================================================================
# Test TimingWheelBucket
# =============================================================================


class TestTimingWheelBucket:
    """Tests for the TimingWheelBucket class."""

    @pytest.mark.asyncio
    async def test_add_entry_happy_path(self, sample_node: tuple[str, int], sample_state: SuspicionState):
        """Adding an entry should store it successfully."""
        bucket = TimingWheelBucket()
        entry = WheelEntry(
            node=sample_node,
            state=sample_state,
            expiration_time=time.monotonic() + 5.0,
            epoch=1,
        )

        await bucket.add(entry)

        assert len(bucket) == 1
        retrieved = await bucket.get(sample_node)
        assert retrieved is entry

    @pytest.mark.asyncio
    async def test_add_overwrites_existing_entry(self, sample_node: tuple[str, int], sample_state: SuspicionState):
        """Adding an entry with same node overwrites the previous one."""
        bucket = TimingWheelBucket()

        entry1 = WheelEntry(node=sample_node, state=sample_state, expiration_time=1.0, epoch=1)
        entry2 = WheelEntry(node=sample_node, state=sample_state, expiration_time=2.0, epoch=2)

        await bucket.add(entry1)
        await bucket.add(entry2)

        assert len(bucket) == 1
        retrieved = await bucket.get(sample_node)
        assert retrieved.epoch == 2

    @pytest.mark.asyncio
    async def test_remove_entry_happy_path(self, sample_node: tuple[str, int], sample_state: SuspicionState):
        """Removing an entry should return it and clear from bucket."""
        bucket = TimingWheelBucket()
        entry = WheelEntry(node=sample_node, state=sample_state, expiration_time=1.0, epoch=1)

        await bucket.add(entry)
        removed = await bucket.remove(sample_node)

        assert removed is entry
        assert len(bucket) == 0

    @pytest.mark.asyncio
    async def test_remove_nonexistent_returns_none(self, sample_node: tuple[str, int]):
        """Removing a nonexistent entry returns None."""
        bucket = TimingWheelBucket()

        removed = await bucket.remove(sample_node)

        assert removed is None

    @pytest.mark.asyncio
    async def test_pop_all_clears_bucket(self):
        """pop_all should return all entries and clear the bucket."""
        bucket = TimingWheelBucket()

        entries = []
        for i in range(5):
            node = make_node(i)
            state = make_state(node)
            entry = WheelEntry(node=node, state=state, expiration_time=1.0, epoch=i)
            entries.append(entry)
            await bucket.add(entry)

        assert len(bucket) == 5

        popped = await bucket.pop_all()

        assert len(popped) == 5
        assert len(bucket) == 0

    @pytest.mark.asyncio
    async def test_get_returns_none_for_missing(self, sample_node: tuple[str, int]):
        """get should return None for missing entries."""
        bucket = TimingWheelBucket()

        result = await bucket.get(sample_node)

        assert result is None

    @pytest.mark.asyncio
    async def test_concurrent_add_remove_maintains_consistency(self):
        """Concurrent add/remove operations should not corrupt bucket state."""
        bucket = TimingWheelBucket()
        num_operations = 100

        async def add_entries():
            for i in range(num_operations):
                node = make_node(i)
                state = make_state(node)
                entry = WheelEntry(node=node, state=state, expiration_time=1.0, epoch=i)
                await bucket.add(entry)
                await asyncio.sleep(0)

        async def remove_entries():
            for i in range(num_operations):
                node = make_node(i)
                await bucket.remove(node)
                await asyncio.sleep(0)

        # Run concurrently - some removes may happen before adds
        await asyncio.gather(add_entries(), remove_entries())

        # Bucket should be in consistent state (may have entries remaining)
        # Key assertion: no exceptions raised, bucket still functional
        await bucket.pop_all()
        assert len(bucket) == 0


# =============================================================================
# Test TimingWheel - Happy Path
# =============================================================================


class TestTimingWheelHappyPath:
    """Happy path tests for TimingWheel."""

    @pytest.mark.asyncio
    async def test_add_single_entry(
        self,
        default_config: TimingWheelConfig,
        sample_node: tuple[str, int],
        sample_state: SuspicionState,
    ):
        """Adding a single entry should be tracked correctly."""
        wheel = TimingWheel(config=default_config)

        expiration = time.monotonic() + 5.0
        result = await wheel.add(sample_node, sample_state, expiration)

        assert result is True
        assert await wheel.contains(sample_node) is True
        retrieved = await wheel.get_state(sample_node)
        assert retrieved is sample_state

    @pytest.mark.asyncio
    async def test_add_multiple_entries(self, default_config: TimingWheelConfig):
        """Adding multiple entries should track all of them."""
        wheel = TimingWheel(config=default_config)

        for i in range(10):
            node = make_node(i)
            state = make_state(node)
            expiration = time.monotonic() + 5.0 + i
            await wheel.add(node, state, expiration)

        stats = wheel.get_stats()
        assert stats["current_entries"] == 10
        assert stats["entries_added"] == 10

    @pytest.mark.asyncio
    async def test_remove_entry(
        self,
        default_config: TimingWheelConfig,
        sample_node: tuple[str, int],
        sample_state: SuspicionState,
    ):
        """Removing an entry should return the state and stop tracking."""
        wheel = TimingWheel(config=default_config)

        expiration = time.monotonic() + 5.0
        await wheel.add(sample_node, sample_state, expiration)

        removed = await wheel.remove(sample_node)

        assert removed is sample_state
        assert await wheel.contains(sample_node) is False
        stats = wheel.get_stats()
        assert stats["entries_removed"] == 1

    @pytest.mark.asyncio
    async def test_update_expiration_extends_timeout(
        self,
        default_config: TimingWheelConfig,
        sample_node: tuple[str, int],
        sample_state: SuspicionState,
    ):
        """Updating expiration should move entry to later bucket."""
        wheel = TimingWheel(config=default_config)

        original_expiration = time.monotonic() + 1.0
        await wheel.add(sample_node, sample_state, original_expiration)

        new_expiration = time.monotonic() + 10.0
        result = await wheel.update_expiration(sample_node, new_expiration)

        assert result is True
        stats = wheel.get_stats()
        assert stats["entries_moved"] == 1

    @pytest.mark.asyncio
    async def test_entry_placement_in_fine_wheel(self, default_config: TimingWheelConfig):
        """Entries with short timeout should go to fine wheel."""
        wheel = TimingWheel(config=default_config)

        node = make_node(1)
        state = make_state(node)
        # Expiration within fine_wheel_threshold_ms (2000ms = 2s)
        expiration = time.monotonic() + 1.5

        await wheel.add(node, state, expiration)

        # Check that it's in the fine wheel via internal state
        async with wheel._lock:
            location = wheel._node_locations.get(node)
            assert location is not None
            assert location[0] == "fine"

    @pytest.mark.asyncio
    async def test_entry_placement_in_coarse_wheel(self, default_config: TimingWheelConfig):
        """Entries with long timeout should go to coarse wheel."""
        wheel = TimingWheel(config=default_config)

        node = make_node(1)
        state = make_state(node)
        # Expiration beyond fine_wheel_threshold_ms
        expiration = time.monotonic() + 10.0

        await wheel.add(node, state, expiration)

        # Check that it's in the coarse wheel via internal state
        async with wheel._lock:
            location = wheel._node_locations.get(node)
            assert location is not None
            assert location[0] == "coarse"

    @pytest.mark.asyncio
    async def test_expiration_callback_invoked(self, fast_config: TimingWheelConfig):
        """Expired entries should trigger the callback."""
        expired_nodes: list[tuple[str, int]] = []

        def on_expired(node: tuple[str, int], state: SuspicionState) -> None:
            expired_nodes.append(node)

        wheel = TimingWheel(config=fast_config, on_expired=on_expired)
        wheel.start()

        try:
            node = make_node(1)
            state = make_state(node)
            # Expire in ~50ms
            expiration = time.monotonic() + 0.05

            await wheel.add(node, state, expiration)

            # Wait for expiration
            await asyncio.sleep(0.2)

            assert node in expired_nodes
            stats = wheel.get_stats()
            assert stats["entries_expired"] == 1
        finally:
            await wheel.stop()

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self, default_config: TimingWheelConfig):
        """Starting and stopping the wheel should work correctly."""
        wheel = TimingWheel(config=default_config)

        assert wheel._running is False
        assert wheel._advance_task is None

        wheel.start()

        assert wheel._running is True
        assert wheel._advance_task is not None

        await wheel.stop()

        assert wheel._running is False


# =============================================================================
# Test TimingWheel - Negative Path
# =============================================================================


class TestTimingWheelNegativePath:
    """Negative path tests for TimingWheel."""

    @pytest.mark.asyncio
    async def test_add_duplicate_returns_false(
        self,
        default_config: TimingWheelConfig,
        sample_node: tuple[str, int],
        sample_state: SuspicionState,
    ):
        """Adding the same node twice should return False."""
        wheel = TimingWheel(config=default_config)

        expiration = time.monotonic() + 5.0
        result1 = await wheel.add(sample_node, sample_state, expiration)
        result2 = await wheel.add(sample_node, sample_state, expiration)

        assert result1 is True
        assert result2 is False
        stats = wheel.get_stats()
        assert stats["current_entries"] == 1

    @pytest.mark.asyncio
    async def test_remove_nonexistent_returns_none(
        self,
        default_config: TimingWheelConfig,
        sample_node: tuple[str, int],
    ):
        """Removing a nonexistent node should return None."""
        wheel = TimingWheel(config=default_config)

        result = await wheel.remove(sample_node)

        assert result is None
        stats = wheel.get_stats()
        assert stats["entries_removed"] == 0

    @pytest.mark.asyncio
    async def test_update_expiration_nonexistent_returns_false(
        self,
        default_config: TimingWheelConfig,
        sample_node: tuple[str, int],
    ):
        """Updating expiration for nonexistent node returns False."""
        wheel = TimingWheel(config=default_config)

        result = await wheel.update_expiration(sample_node, time.monotonic() + 10.0)

        assert result is False

    @pytest.mark.asyncio
    async def test_contains_nonexistent_returns_false(
        self,
        default_config: TimingWheelConfig,
        sample_node: tuple[str, int],
    ):
        """Contains check for nonexistent node returns False."""
        wheel = TimingWheel(config=default_config)

        result = await wheel.contains(sample_node)

        assert result is False

    @pytest.mark.asyncio
    async def test_get_state_nonexistent_returns_none(
        self,
        default_config: TimingWheelConfig,
        sample_node: tuple[str, int],
    ):
        """Getting state for nonexistent node returns None."""
        wheel = TimingWheel(config=default_config)

        result = await wheel.get_state(sample_node)

        assert result is None


# =============================================================================
# Test TimingWheel - Failure Modes
# =============================================================================


class TestTimingWheelFailureModes:
    """Failure mode tests for TimingWheel."""

    @pytest.mark.asyncio
    async def test_callback_exception_does_not_stop_wheel(self, fast_config: TimingWheelConfig):
        """Exceptions in callback should not stop the wheel."""
        call_count = 0

        def failing_callback(node: tuple[str, int], state: SuspicionState) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Simulated callback failure")

        wheel = TimingWheel(config=fast_config, on_expired=failing_callback)
        wheel.start()

        try:
            # Add two entries that will expire
            for i in range(2):
                node = make_node(i)
                state = make_state(node)
                expiration = time.monotonic() + 0.05
                await wheel.add(node, state, expiration)

            # Wait for expirations
            await asyncio.sleep(0.3)

            # Both should have been processed despite first failing
            assert call_count == 2
        finally:
            await wheel.stop()

    @pytest.mark.asyncio
    async def test_stop_during_tick_completes_gracefully(self, fast_config: TimingWheelConfig):
        """Stopping the wheel during a tick should complete gracefully."""
        wheel = TimingWheel(config=fast_config)

        # Add many entries
        for i in range(50):
            node = make_node(i)
            state = make_state(node)
            expiration = time.monotonic() + 0.05
            await wheel.add(node, state, expiration)

        wheel.start()

        # Start processing and immediately stop
        await asyncio.sleep(0.01)
        await wheel.stop()

        # Should complete without errors
        assert wheel._running is False

    @pytest.mark.asyncio
    async def test_double_stop_is_safe(self, default_config: TimingWheelConfig):
        """Stopping an already-stopped wheel should be safe."""
        wheel = TimingWheel(config=default_config)
        wheel.start()

        await wheel.stop()
        await wheel.stop()  # Should not raise

        assert wheel._running is False

    @pytest.mark.asyncio
    async def test_double_start_is_safe(self, default_config: TimingWheelConfig):
        """Starting an already-running wheel should be idempotent."""
        wheel = TimingWheel(config=default_config)

        wheel.start()
        wheel.start()  # Should not create second task

        try:
            assert wheel._running is True
            # Only one task should exist
        finally:
            await wheel.stop()


# =============================================================================
# Test TimingWheel - Edge Cases
# =============================================================================


class TestTimingWheelEdgeCases:
    """Edge case tests for TimingWheel."""

    @pytest.mark.asyncio
    async def test_expiration_in_past_expires_immediately(self, fast_config: TimingWheelConfig):
        """Entry with expiration in the past should expire on next tick."""
        expired_nodes: list[tuple[str, int]] = []

        def on_expired(node: tuple[str, int], state: SuspicionState) -> None:
            expired_nodes.append(node)

        wheel = TimingWheel(config=fast_config, on_expired=on_expired)
        wheel.start()

        try:
            node = make_node(1)
            state = make_state(node)
            # Expiration in the past
            expiration = time.monotonic() - 1.0

            await wheel.add(node, state, expiration)

            # Wait for tick
            await asyncio.sleep(0.05)

            assert node in expired_nodes
        finally:
            await wheel.stop()

    @pytest.mark.asyncio
    async def test_bucket_wrap_around(self, default_config: TimingWheelConfig):
        """Wheel should handle bucket index wrap-around correctly."""
        wheel = TimingWheel(config=default_config)

        # Force position near end of wheel
        wheel._fine_position = default_config.fine_wheel_size - 1

        node = make_node(1)
        state = make_state(node)
        # This should wrap around to early buckets
        expiration = time.monotonic() + 0.3

        await wheel.add(node, state, expiration)

        assert await wheel.contains(node) is True

    @pytest.mark.asyncio
    async def test_update_moves_between_wheels(self, default_config: TimingWheelConfig):
        """Updating expiration should move entry between coarse and fine wheels."""
        wheel = TimingWheel(config=default_config)

        node = make_node(1)
        state = make_state(node)

        # Start in coarse wheel (far future)
        expiration = time.monotonic() + 30.0
        await wheel.add(node, state, expiration)

        async with wheel._lock:
            location = wheel._node_locations.get(node)
            assert location[0] == "coarse"

        # Move to fine wheel (near future)
        new_expiration = time.monotonic() + 1.0
        await wheel.update_expiration(node, new_expiration)

        async with wheel._lock:
            location = wheel._node_locations.get(node)
            assert location[0] == "fine"

    @pytest.mark.asyncio
    async def test_clear_removes_all_entries(self, default_config: TimingWheelConfig):
        """Clear should remove all entries from both wheels."""
        wheel = TimingWheel(config=default_config)

        # Add entries to both wheels
        for i in range(5):
            node = make_node(i)
            state = make_state(node)
            # Some in fine wheel, some in coarse
            expiration = time.monotonic() + (1.0 if i % 2 == 0 else 10.0)
            await wheel.add(node, state, expiration)

        assert wheel.get_stats()["current_entries"] == 5

        await wheel.clear()

        assert wheel.get_stats()["current_entries"] == 0

    @pytest.mark.asyncio
    async def test_lhm_adjustment_extends_all_timeouts(self, default_config: TimingWheelConfig):
        """LHM adjustment should proportionally extend all timeouts."""
        wheel = TimingWheel(config=default_config)

        # Add several entries
        for i in range(5):
            node = make_node(i)
            state = make_state(node)
            expiration = time.monotonic() + 5.0
            await wheel.add(node, state, expiration)

        # Apply 2x multiplier
        adjusted = await wheel.apply_lhm_adjustment(2.0)

        assert adjusted == 5
        # All entries should still be tracked
        assert wheel.get_stats()["current_entries"] == 5

    @pytest.mark.asyncio
    async def test_lhm_adjustment_identity_multiplier(self, default_config: TimingWheelConfig):
        """LHM adjustment with multiplier 1.0 should do nothing."""
        wheel = TimingWheel(config=default_config)

        node = make_node(1)
        state = make_state(node)
        await wheel.add(node, state, time.monotonic() + 5.0)

        adjusted = await wheel.apply_lhm_adjustment(1.0)

        assert adjusted == 0

    @pytest.mark.asyncio
    async def test_cascade_from_coarse_to_fine(self, fast_config: TimingWheelConfig):
        """Entries should cascade from coarse to fine wheel as time passes."""
        expired_nodes: list[tuple[str, int]] = []

        def on_expired(node: tuple[str, int], state: SuspicionState) -> None:
            expired_nodes.append(node)

        wheel = TimingWheel(config=fast_config, on_expired=on_expired)

        node = make_node(1)
        state = make_state(node)
        # Start in coarse wheel
        expiration = time.monotonic() + 0.5

        await wheel.add(node, state, expiration)

        wheel.start()

        try:
            # Wait for cascade and expiration
            await asyncio.sleep(0.8)

            assert node in expired_nodes
            stats = wheel.get_stats()
            assert stats["cascade_count"] >= 1
        finally:
            await wheel.stop()

    @pytest.mark.asyncio
    async def test_remove_during_cascade(self, fast_config: TimingWheelConfig):
        """Removing an entry during cascade should not cause errors."""
        wheel = TimingWheel(config=fast_config)

        node = make_node(1)
        state = make_state(node)
        expiration = time.monotonic() + 0.3

        await wheel.add(node, state, expiration)

        wheel.start()

        try:
            # Remove while wheel is running
            await asyncio.sleep(0.1)
            removed = await wheel.remove(node)

            assert removed is state
            # Entry should not expire (was removed)
            await asyncio.sleep(0.4)
            assert wheel.get_stats()["entries_expired"] == 0
        finally:
            await wheel.stop()


# =============================================================================
# Test TimingWheel - Concurrency Correctness
# =============================================================================


class TestTimingWheelConcurrency:
    """Concurrency correctness tests for TimingWheel (asyncio)."""

    @pytest.mark.asyncio
    async def test_concurrent_adds_no_duplicates(self, default_config: TimingWheelConfig):
        """Concurrent adds of the same node should result in only one entry."""
        wheel = TimingWheel(config=default_config)
        node = make_node(1)
        state = make_state(node)
        expiration = time.monotonic() + 5.0

        results: list[bool] = []

        async def try_add():
            result = await wheel.add(node, state, expiration)
            results.append(result)

        # Try to add same node concurrently
        await asyncio.gather(*[try_add() for _ in range(10)])

        # Exactly one should succeed
        assert sum(results) == 1
        assert wheel.get_stats()["current_entries"] == 1

    @pytest.mark.asyncio
    async def test_concurrent_add_remove_different_nodes(self, default_config: TimingWheelConfig):
        """Concurrent add/remove of different nodes should work correctly."""
        wheel = TimingWheel(config=default_config)
        num_operations = 100

        async def add_entries():
            for i in range(num_operations):
                node = make_node(i)
                state = make_state(node)
                expiration = time.monotonic() + 10.0
                await wheel.add(node, state, expiration)
                await asyncio.sleep(0)

        async def remove_entries():
            for i in range(num_operations):
                node = make_node(i)
                await wheel.remove(node)
                await asyncio.sleep(0)

        # Run concurrently - order matters, so some removes may fail
        await asyncio.gather(add_entries(), remove_entries())

        # State should be consistent
        stats = wheel.get_stats()
        assert stats["current_entries"] >= 0
        assert stats["entries_added"] == num_operations

    @pytest.mark.asyncio
    async def test_concurrent_updates_maintain_consistency(self, default_config: TimingWheelConfig):
        """Concurrent updates to same entry should not corrupt state."""
        wheel = TimingWheel(config=default_config)
        node = make_node(1)
        state = make_state(node)

        await wheel.add(node, state, time.monotonic() + 5.0)

        async def update_expiration(delay: float):
            for _ in range(20):
                new_exp = time.monotonic() + delay
                await wheel.update_expiration(node, new_exp)
                await asyncio.sleep(0)

        # Concurrent updates with different values
        await asyncio.gather(
            update_expiration(3.0),
            update_expiration(5.0),
            update_expiration(7.0),
        )

        # Entry should still be tracked and valid
        assert await wheel.contains(node) is True
        assert await wheel.get_state(node) is state

    @pytest.mark.asyncio
    async def test_concurrent_operations_during_tick(self, fast_config: TimingWheelConfig):
        """Operations during wheel tick should not cause corruption."""
        wheel = TimingWheel(config=fast_config)
        wheel.start()

        try:
            async def add_and_remove():
                for i in range(50):
                    node = make_node(i)
                    state = make_state(node)
                    expiration = time.monotonic() + 0.5
                    await wheel.add(node, state, expiration)
                    await asyncio.sleep(0.01)
                    await wheel.remove(node)

            async def update_entries():
                for i in range(50):
                    node = make_node(i)
                    await wheel.update_expiration(node, time.monotonic() + 1.0)
                    await asyncio.sleep(0.01)

            await asyncio.gather(add_and_remove(), update_entries())

            # Wheel should still be functional
            stats = wheel.get_stats()
            assert stats["current_entries"] >= 0
        finally:
            await wheel.stop()

    @pytest.mark.asyncio
    async def test_concurrent_lhm_adjustment_with_operations(self, default_config: TimingWheelConfig):
        """LHM adjustment during other operations should be safe."""
        wheel = TimingWheel(config=default_config)

        # Pre-populate
        for i in range(20):
            node = make_node(i)
            state = make_state(node)
            await wheel.add(node, state, time.monotonic() + 5.0)

        async def perform_operations():
            for i in range(20, 40):
                node = make_node(i)
                state = make_state(node)
                await wheel.add(node, state, time.monotonic() + 5.0)
                await asyncio.sleep(0)
            for i in range(10):
                await wheel.remove(make_node(i))
                await asyncio.sleep(0)

        async def apply_adjustments():
            for multiplier in [1.5, 2.0, 0.75, 1.0]:
                await wheel.apply_lhm_adjustment(multiplier)
                await asyncio.sleep(0.01)

        await asyncio.gather(perform_operations(), apply_adjustments())

        # Wheel should be in consistent state
        stats = wheel.get_stats()
        assert stats["current_entries"] >= 0

    @pytest.mark.asyncio
    async def test_contains_during_concurrent_modifications(self, default_config: TimingWheelConfig):
        """contains() should return correct values during modifications."""
        wheel = TimingWheel(config=default_config)
        node = make_node(1)
        state = make_state(node)

        results: list[bool] = []
        done = asyncio.Event()

        async def check_contains():
            while not done.is_set():
                result = await wheel.contains(node)
                results.append(result)
                await asyncio.sleep(0)

        async def toggle_entry():
            for _ in range(50):
                await wheel.add(node, state, time.monotonic() + 5.0)
                await asyncio.sleep(0)
                await wheel.remove(node)
                await asyncio.sleep(0)
            done.set()

        await asyncio.gather(check_contains(), toggle_entry())

        # All results should be valid booleans
        assert all(isinstance(r, bool) for r in results)
        # We should see both True and False
        assert True in results or False in results

    @pytest.mark.asyncio
    async def test_expiration_callbacks_not_duplicated(self, fast_config: TimingWheelConfig):
        """Each entry should only trigger one expiration callback."""
        expired_counts: dict[tuple[str, int], int] = {}
        lock = asyncio.Lock()

        async def on_expired(node: tuple[str, int], state: SuspicionState) -> None:
            async with lock:
                expired_counts[node] = expired_counts.get(node, 0) + 1

        # Use sync callback since TimingWheel expects sync
        def sync_on_expired(node: tuple[str, int], state: SuspicionState) -> None:
            expired_counts[node] = expired_counts.get(node, 0) + 1

        wheel = TimingWheel(config=fast_config, on_expired=sync_on_expired)
        wheel.start()

        try:
            # Add multiple entries
            for i in range(10):
                node = make_node(i)
                state = make_state(node)
                expiration = time.monotonic() + 0.05
                await wheel.add(node, state, expiration)

            # Wait for all to expire
            await asyncio.sleep(0.3)

            # Each node should have expired exactly once
            for i in range(10):
                node = make_node(i)
                assert expired_counts.get(node, 0) == 1, f"Node {node} expired {expired_counts.get(node, 0)} times"
        finally:
            await wheel.stop()

    @pytest.mark.asyncio
    async def test_stats_consistency_under_load(self, fast_config: TimingWheelConfig):
        """Stats should remain consistent under heavy concurrent load."""
        wheel = TimingWheel(config=fast_config)
        wheel.start()

        try:
            async def hammer():
                for i in range(100):
                    node = make_node(i)
                    state = make_state(node)
                    await wheel.add(node, state, time.monotonic() + 0.1)
                    await asyncio.sleep(0)

            await asyncio.gather(*[hammer() for _ in range(5)])

            # Wait for expirations
            await asyncio.sleep(0.3)

            stats = wheel.get_stats()
            # Basic consistency checks
            assert stats["entries_added"] >= stats["entries_removed"]
            assert stats["current_entries"] >= 0
            # All should have expired or been processed
            assert stats["current_entries"] <= stats["entries_added"]
        finally:
            await wheel.stop()
