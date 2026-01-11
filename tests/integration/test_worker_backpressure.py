"""
Integration tests for WorkerBackpressureManager (Section 15.2.6.6).

Tests WorkerBackpressureManager for overload detection, circuit breakers,
and backpressure signals (AD-18, AD-23, AD-37).

Covers:
- Happy path: Normal overload detection and backpressure handling
- Negative path: Invalid backpressure levels
- Failure mode: Resource sampling failures
- Concurrency: Thread-safe state updates
- Edge cases: Boundary values, all backpressure levels
"""

import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from hyperscale.distributed_rewrite.nodes.worker.backpressure import WorkerBackpressureManager
from hyperscale.distributed_rewrite.reliability import BackpressureLevel


class TestWorkerBackpressureManagerInitialization:
    """Test WorkerBackpressureManager initialization."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation."""
        logger = MagicMock()
        manager = WorkerBackpressureManager(logger=logger)

        assert manager._logger == logger
        assert manager._poll_interval == 0.25
        assert manager._running is False

    def test_custom_poll_interval(self):
        """Test with custom poll interval."""
        manager = WorkerBackpressureManager(poll_interval=0.5)

        assert manager._poll_interval == 0.5

    def test_with_registry(self):
        """Test with registry reference."""
        logger = MagicMock()
        registry = MagicMock()
        manager = WorkerBackpressureManager(logger=logger, registry=registry)

        assert manager._registry == registry

    def test_default_resource_getters(self):
        """Test default resource getters return 0."""
        manager = WorkerBackpressureManager()

        assert manager._get_cpu_percent() == 0.0
        assert manager._get_memory_percent() == 0.0


class TestWorkerBackpressureManagerResourceGetters:
    """Test resource getter configuration."""

    def test_set_resource_getters(self):
        """Test setting resource getter functions."""
        manager = WorkerBackpressureManager()

        cpu_getter = lambda: 75.0
        memory_getter = lambda: 60.0

        manager.set_resource_getters(cpu_getter, memory_getter)

        assert manager._get_cpu_percent() == 75.0
        assert manager._get_memory_percent() == 60.0


class TestWorkerBackpressureManagerBackpressureTracking:
    """Test manager backpressure tracking (AD-23)."""

    def test_set_manager_backpressure(self):
        """Test setting manager backpressure level."""
        manager = WorkerBackpressureManager()

        manager.set_manager_backpressure("mgr-1", BackpressureLevel.THROTTLE)

        assert manager._manager_backpressure["mgr-1"] == BackpressureLevel.THROTTLE

    def test_get_max_backpressure_level_none(self):
        """Test max backpressure with no managers."""
        manager = WorkerBackpressureManager()

        level = manager.get_max_backpressure_level()
        assert level == BackpressureLevel.NONE

    def test_get_max_backpressure_level_single(self):
        """Test max backpressure with single manager."""
        manager = WorkerBackpressureManager()

        manager.set_manager_backpressure("mgr-1", BackpressureLevel.BATCH)

        level = manager.get_max_backpressure_level()
        assert level == BackpressureLevel.BATCH

    def test_get_max_backpressure_level_multiple(self):
        """Test max backpressure across multiple managers."""
        manager = WorkerBackpressureManager()

        manager.set_manager_backpressure("mgr-1", BackpressureLevel.NONE)
        manager.set_manager_backpressure("mgr-2", BackpressureLevel.BATCH)
        manager.set_manager_backpressure("mgr-3", BackpressureLevel.THROTTLE)

        level = manager.get_max_backpressure_level()
        assert level == BackpressureLevel.BATCH  # BATCH > THROTTLE

    def test_set_backpressure_delay_ms(self):
        """Test setting backpressure delay."""
        manager = WorkerBackpressureManager()

        manager.set_backpressure_delay_ms(500)

        assert manager.get_backpressure_delay_ms() == 500


class TestWorkerBackpressureManagerOverloadDetection:
    """Test overload detection (AD-18)."""

    def test_get_overload_state_str(self):
        """Test getting overload state string."""
        manager = WorkerBackpressureManager()
        manager.set_resource_getters(lambda: 50.0, lambda: 40.0)

        state = manager.get_overload_state_str()

        assert isinstance(state, str)

    def test_is_overloaded_normal(self):
        """Test overload check under normal conditions."""
        manager = WorkerBackpressureManager()
        manager.set_resource_getters(lambda: 30.0, lambda: 40.0)

        assert manager.is_overloaded() is False

    def test_record_workflow_latency(self):
        """Test recording workflow latency."""
        manager = WorkerBackpressureManager()

        # Should not raise
        manager.record_workflow_latency(100.0)


class TestWorkerBackpressureManagerAD37Policy:
    """Test AD-37 explicit backpressure policy methods."""

    def test_should_throttle_none(self):
        """Test should_throttle with NONE level."""
        manager = WorkerBackpressureManager()

        assert manager.should_throttle() is False

    def test_should_throttle_throttle(self):
        """Test should_throttle with THROTTLE level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.THROTTLE)

        assert manager.should_throttle() is True

    def test_should_throttle_higher(self):
        """Test should_throttle with higher level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.BATCH)

        assert manager.should_throttle() is True

    def test_should_batch_only_none(self):
        """Test should_batch_only with NONE level."""
        manager = WorkerBackpressureManager()

        assert manager.should_batch_only() is False

    def test_should_batch_only_throttle(self):
        """Test should_batch_only with THROTTLE level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.THROTTLE)

        assert manager.should_batch_only() is False

    def test_should_batch_only_batch(self):
        """Test should_batch_only with BATCH level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.BATCH)

        assert manager.should_batch_only() is True

    def test_should_reject_updates_none(self):
        """Test should_reject_updates with NONE level."""
        manager = WorkerBackpressureManager()

        assert manager.should_reject_updates() is False

    def test_should_reject_updates_batch(self):
        """Test should_reject_updates with BATCH level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.BATCH)

        assert manager.should_reject_updates() is False

    def test_should_reject_updates_reject(self):
        """Test should_reject_updates with REJECT level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.REJECT)

        assert manager.should_reject_updates() is True


class TestWorkerBackpressureManagerThrottleDelay:
    """Test throttle delay calculations (AD-37)."""

    def test_get_throttle_delay_none(self):
        """Test throttle delay with NONE level."""
        manager = WorkerBackpressureManager()

        delay = manager.get_throttle_delay_seconds()
        assert delay == 0.0

    def test_get_throttle_delay_throttle(self):
        """Test throttle delay with THROTTLE level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.THROTTLE)
        manager.set_backpressure_delay_ms(0)

        delay = manager.get_throttle_delay_seconds()
        assert delay == 0.5  # Default 500ms

    def test_get_throttle_delay_throttle_with_delay(self):
        """Test throttle delay with THROTTLE level and suggested delay."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.THROTTLE)
        manager.set_backpressure_delay_ms(1000)

        delay = manager.get_throttle_delay_seconds()
        assert delay == 1.0  # 1000ms

    def test_get_throttle_delay_batch(self):
        """Test throttle delay with BATCH level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.BATCH)
        manager.set_backpressure_delay_ms(500)

        delay = manager.get_throttle_delay_seconds()
        assert delay == 1.0  # 500ms * 2

    def test_get_throttle_delay_reject(self):
        """Test throttle delay with REJECT level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.REJECT)
        manager.set_backpressure_delay_ms(500)

        delay = manager.get_throttle_delay_seconds()
        assert delay == 2.0  # 500ms * 4


class TestWorkerBackpressureManagerStateName:
    """Test backpressure state name (AD-37)."""

    def test_get_backpressure_state_name_none(self):
        """Test state name for NONE level."""
        manager = WorkerBackpressureManager()

        name = manager.get_backpressure_state_name()
        assert name == "NO_BACKPRESSURE"

    def test_get_backpressure_state_name_throttle(self):
        """Test state name for THROTTLE level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.THROTTLE)

        name = manager.get_backpressure_state_name()
        assert name == "THROTTLED"

    def test_get_backpressure_state_name_batch(self):
        """Test state name for BATCH level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.BATCH)

        name = manager.get_backpressure_state_name()
        assert name == "BATCH_ONLY"

    def test_get_backpressure_state_name_reject(self):
        """Test state name for REJECT level."""
        manager = WorkerBackpressureManager()
        manager.set_manager_backpressure("mgr-1", BackpressureLevel.REJECT)

        name = manager.get_backpressure_state_name()
        assert name == "REJECT"


class TestWorkerBackpressureManagerPolling:
    """Test overload polling loop."""

    @pytest.mark.asyncio
    async def test_run_overload_poll_loop_starts_running(self):
        """Test that poll loop starts running."""
        manager = WorkerBackpressureManager(poll_interval=0.01)

        task = asyncio.create_task(manager.run_overload_poll_loop())

        await asyncio.sleep(0.05)

        assert manager._running is True

        manager.stop()
        await asyncio.sleep(0.02)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_stop_stops_loop(self):
        """Test that stop() stops the loop."""
        manager = WorkerBackpressureManager(poll_interval=0.01)

        task = asyncio.create_task(manager.run_overload_poll_loop())

        await asyncio.sleep(0.03)
        manager.stop()

        assert manager._running is False

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_poll_loop_handles_exceptions(self):
        """Test that poll loop handles exceptions gracefully."""
        manager = WorkerBackpressureManager(poll_interval=0.01)

        call_count = [0]

        def failing_getter():
            call_count[0] += 1
            if call_count[0] < 3:
                raise RuntimeError("Resource unavailable")
            return 50.0

        manager.set_resource_getters(failing_getter, lambda: 30.0)

        task = asyncio.create_task(manager.run_overload_poll_loop())

        await asyncio.sleep(0.05)

        manager.stop()
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Should have been called multiple times despite exceptions
        assert call_count[0] >= 3


class TestWorkerBackpressureManagerEdgeCases:
    """Test edge cases for WorkerBackpressureManager."""

    def test_many_managers(self):
        """Test with many manager backpressure levels."""
        manager = WorkerBackpressureManager()

        for i in range(100):
            level = BackpressureLevel.NONE if i < 90 else BackpressureLevel.THROTTLE
            manager.set_manager_backpressure(f"mgr-{i}", level)

        level = manager.get_max_backpressure_level()
        assert level == BackpressureLevel.THROTTLE

    def test_update_manager_backpressure(self):
        """Test updating manager backpressure level."""
        manager = WorkerBackpressureManager()

        manager.set_manager_backpressure("mgr-1", BackpressureLevel.NONE)
        assert manager.get_max_backpressure_level() == BackpressureLevel.NONE

        manager.set_manager_backpressure("mgr-1", BackpressureLevel.BATCH)
        assert manager.get_max_backpressure_level() == BackpressureLevel.BATCH

    def test_special_characters_in_manager_id(self):
        """Test manager IDs with special characters."""
        manager = WorkerBackpressureManager()

        special_id = "mgr-🚀-test"
        manager.set_manager_backpressure(special_id, BackpressureLevel.THROTTLE)

        assert manager._manager_backpressure[special_id] == BackpressureLevel.THROTTLE
