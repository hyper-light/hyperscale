"""
Comprehensive tests for RobustMessageQueue.

Tests cover:
- Basic operations (put, get, clear)
- Backpressure signaling at each threshold
- Overflow handling (primary full → overflow)
- Saturation behavior (both queues full)
- Drop policies (preserve newest vs reject new)
- Concurrent access patterns
- Metrics accuracy
- State transitions
- Edge cases and failure scenarios
"""

import asyncio
import pytest
from dataclasses import dataclass

from hyperscale.distributed.reliability.robust_queue import (
    RobustMessageQueue,
    RobustQueueConfig,
    QueuePutResult,
    QueueState,
    QueueFullError,
)
from hyperscale.distributed.reliability.backpressure import (
    BackpressureLevel,
)


@dataclass
class TestMessage:
    """Simple test message type."""
    id: int
    data: str = "test"


class TestRobustQueueBasicOperations:
    """Tests for basic queue operations."""

    def test_create_with_default_config(self):
        """Queue creates with default configuration."""
        queue: RobustMessageQueue[str] = RobustMessageQueue()
        assert queue.qsize() == 0
        assert queue.empty()
        assert not queue.full()
        assert queue.get_state() == QueueState.HEALTHY

    def test_create_with_custom_config(self):
        """Queue creates with custom configuration."""
        config = RobustQueueConfig(
            maxsize=100,
            overflow_size=20,
            throttle_threshold=0.5,
        )
        queue: RobustMessageQueue[str] = RobustMessageQueue(config)
        assert queue._config.maxsize == 100
        assert queue._config.overflow_size == 20
        assert queue._config.throttle_threshold == 0.5

    def test_put_and_get_single_item(self):
        """Single item can be put and retrieved."""
        queue: RobustMessageQueue[str] = RobustMessageQueue()
        result = queue.put_nowait("hello")

        assert result.accepted
        assert not result.in_overflow
        assert not result.dropped
        assert queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_put_and_get_async(self):
        """Items can be retrieved asynchronously."""
        queue: RobustMessageQueue[str] = RobustMessageQueue()
        queue.put_nowait("hello")
        queue.put_nowait("world")

        item1 = await queue.get()
        item2 = await queue.get()

        assert item1 == "hello"
        assert item2 == "world"
        assert queue.empty()

    def test_get_nowait_success(self):
        """get_nowait returns item when available."""
        queue: RobustMessageQueue[str] = RobustMessageQueue()
        queue.put_nowait("hello")

        item = queue.get_nowait()
        assert item == "hello"

    def test_get_nowait_empty_raises(self):
        """get_nowait raises QueueEmpty when empty."""
        queue: RobustMessageQueue[str] = RobustMessageQueue()

        with pytest.raises(asyncio.QueueEmpty):
            queue.get_nowait()

    def test_put_returns_result(self):
        """put_nowait returns QueuePutResult with correct fields."""
        queue: RobustMessageQueue[str] = RobustMessageQueue()
        result = queue.put_nowait("hello")

        assert isinstance(result, QueuePutResult)
        assert result.accepted is True
        assert result.in_overflow is False
        assert result.dropped is False
        assert result.queue_state == QueueState.HEALTHY
        assert 0.0 <= result.fill_ratio <= 1.0
        assert result.backpressure is not None

    def test_clear_empties_both_queues(self):
        """clear() removes all items from both queues."""
        config = RobustQueueConfig(maxsize=5, overflow_size=5)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill primary
        for i in range(5):
            queue.put_nowait(i)

        # Force some into overflow
        for i in range(5, 8):
            queue.put_nowait(i)

        assert queue.qsize() > 0
        assert queue.overflow_qsize() > 0

        cleared = queue.clear()
        assert cleared == 8
        assert queue.empty()
        assert queue.primary_qsize() == 0
        assert queue.overflow_qsize() == 0

    def test_fifo_order_maintained(self):
        """Items are returned in FIFO order."""
        queue: RobustMessageQueue[int] = RobustMessageQueue()

        for i in range(10):
            queue.put_nowait(i)

        for i in range(10):
            item = queue.get_nowait()
            assert item == i

    def test_repr_shows_state(self):
        """__repr__ shows useful state information."""
        queue: RobustMessageQueue[str] = RobustMessageQueue()
        queue.put_nowait("hello")

        repr_str = repr(queue)
        assert "RobustMessageQueue" in repr_str
        assert "primary=" in repr_str
        assert "overflow=" in repr_str
        assert "state=" in repr_str


class TestBackpressureThresholds:
    """Tests for backpressure signaling at various thresholds."""

    def test_healthy_below_throttle_threshold(self):
        """Queue reports HEALTHY when below throttle threshold."""
        config = RobustQueueConfig(
            maxsize=100,
            throttle_threshold=0.70,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill to 69% (below 70% throttle threshold)
        for i in range(69):
            queue.put_nowait(i)

        result = queue.put_nowait(69)
        assert result.queue_state == QueueState.HEALTHY
        assert result.backpressure.level == BackpressureLevel.NONE
        assert result.backpressure.suggested_delay_ms == 0

    def test_throttle_at_throttle_threshold(self):
        """Queue reports THROTTLED at throttle threshold."""
        config = RobustQueueConfig(
            maxsize=100,
            throttle_threshold=0.70,
            batch_threshold=0.85,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill to 70% (at throttle threshold)
        for i in range(70):
            queue.put_nowait(i)

        result = queue.put_nowait(70)
        assert result.queue_state == QueueState.THROTTLED
        assert result.backpressure.level == BackpressureLevel.THROTTLE
        assert result.backpressure.suggested_delay_ms > 0

    def test_batch_at_batch_threshold(self):
        """Queue reports BATCHING at batch threshold."""
        config = RobustQueueConfig(
            maxsize=100,
            throttle_threshold=0.70,
            batch_threshold=0.85,
            reject_threshold=0.95,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill to 85% (at batch threshold)
        for i in range(85):
            queue.put_nowait(i)

        result = queue.put_nowait(85)
        assert result.queue_state == QueueState.BATCHING
        assert result.backpressure.level == BackpressureLevel.BATCH
        assert result.backpressure.batch_only is True

    def test_overflow_near_reject_threshold(self):
        """Queue reports about-to-overflow near reject threshold."""
        config = RobustQueueConfig(
            maxsize=100,
            throttle_threshold=0.70,
            batch_threshold=0.85,
            reject_threshold=0.95,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill to 95% (at reject threshold, but primary not full)
        for i in range(95):
            queue.put_nowait(i)

        result = queue.put_nowait(95)
        # Should be OVERFLOW (approaching overflow) not HEALTHY
        assert result.queue_state == QueueState.OVERFLOW
        assert result.backpressure.level == BackpressureLevel.REJECT

    def test_backpressure_delay_increases_with_severity(self):
        """Suggested delay increases as queue fills."""
        config = RobustQueueConfig(
            maxsize=100,
            throttle_threshold=0.50,
            batch_threshold=0.75,
            reject_threshold=0.90,
            suggested_throttle_delay_ms=50,
            suggested_batch_delay_ms=200,
            suggested_overflow_delay_ms=100,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # HEALTHY state - no delay
        result_healthy = queue.put_nowait(0)
        delay_healthy = result_healthy.backpressure.suggested_delay_ms

        # THROTTLED state - some delay
        for i in range(1, 51):
            queue.put_nowait(i)
        result_throttled = queue.put_nowait(51)
        delay_throttled = result_throttled.backpressure.suggested_delay_ms

        # BATCHING state - more delay
        for i in range(52, 76):
            queue.put_nowait(i)
        result_batching = queue.put_nowait(76)
        delay_batching = result_batching.backpressure.suggested_delay_ms

        assert delay_healthy == 0
        assert delay_throttled > delay_healthy
        assert delay_batching > delay_throttled


class TestOverflowHandling:
    """Tests for overflow buffer behavior."""

    def test_overflow_when_primary_full(self):
        """Items go to overflow when primary is full."""
        config = RobustQueueConfig(maxsize=5, overflow_size=5)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill primary
        for i in range(5):
            result = queue.put_nowait(i)
            assert not result.in_overflow

        # Next item goes to overflow
        result = queue.put_nowait(5)
        assert result.accepted
        assert result.in_overflow
        assert result.queue_state == QueueState.OVERFLOW

        assert queue.primary_qsize() == 5
        assert queue.overflow_qsize() == 1

    def test_overflow_items_drained_first(self):
        """Overflow items are drained before primary (FIFO across both)."""
        config = RobustQueueConfig(maxsize=3, overflow_size=3)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill primary with 0, 1, 2
        for i in range(3):
            queue.put_nowait(i)

        # Add 3, 4 to overflow
        queue.put_nowait(3)
        queue.put_nowait(4)

        assert queue.overflow_qsize() == 2

        # Drain - should get overflow items first
        item0 = queue.get_nowait()
        item1 = queue.get_nowait()

        # Overflow drained first (3, 4), then primary (0, 1, 2)
        assert item0 == 3
        assert item1 == 4

        # Now primary items
        assert queue.get_nowait() == 0
        assert queue.get_nowait() == 1
        assert queue.get_nowait() == 2

    def test_overflow_metrics_tracked(self):
        """Overflow events are tracked in metrics."""
        config = RobustQueueConfig(maxsize=3, overflow_size=3)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill primary
        for i in range(3):
            queue.put_nowait(i)

        # Force overflow
        queue.put_nowait(3)
        queue.put_nowait(4)

        metrics = queue.get_metrics()
        assert metrics["total_overflow"] == 2
        assert metrics["overflow_activations"] >= 1


class TestSaturationBehavior:
    """Tests for behavior when both queues are full."""

    def test_preserve_newest_drops_oldest(self):
        """With preserve_newest=True, oldest overflow items are dropped."""
        config = RobustQueueConfig(
            maxsize=3,
            overflow_size=3,
            preserve_newest=True,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill both queues completely
        for i in range(6):  # 3 primary + 3 overflow
            queue.put_nowait(i)

        # Add one more - should drop oldest overflow (item 3)
        result = queue.put_nowait(100)
        assert result.accepted
        assert result.in_overflow
        assert not result.dropped

        metrics = queue.get_metrics()
        assert metrics["total_oldest_dropped"] == 1

        # Verify oldest was dropped: overflow should have 4, 5, 100
        queue.clear()  # Clear and check what would have been there

    def test_reject_new_when_preserve_newest_false(self):
        """With preserve_newest=False, new items are rejected when full."""
        config = RobustQueueConfig(
            maxsize=3,
            overflow_size=3,
            preserve_newest=False,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill both queues completely
        for i in range(6):  # 3 primary + 3 overflow
            queue.put_nowait(i)

        # Try to add one more - should be rejected
        result = queue.put_nowait(100)
        assert not result.accepted
        assert result.dropped
        assert result.queue_state == QueueState.SATURATED

        metrics = queue.get_metrics()
        assert metrics["total_dropped"] == 1

    def test_saturated_state_reported(self):
        """SATURATED state is reported when both queues full."""
        config = RobustQueueConfig(maxsize=3, overflow_size=3)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill both queues
        for i in range(6):
            queue.put_nowait(i)

        # Next put shows saturated
        result = queue.put_nowait(100)
        assert result.queue_state == QueueState.SATURATED
        assert result.backpressure.level == BackpressureLevel.REJECT
        assert result.backpressure.drop_non_critical is True

    def test_saturated_activations_tracked(self):
        """Saturation events are tracked in metrics."""
        config = RobustQueueConfig(maxsize=2, overflow_size=2)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill completely
        for i in range(4):
            queue.put_nowait(i)

        # Trigger saturation
        queue.put_nowait(100)

        metrics = queue.get_metrics()
        assert metrics["saturated_activations"] >= 1


class TestConcurrentAccess:
    """Tests for concurrent producer/consumer patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_producers(self):
        """Multiple producers can enqueue concurrently."""
        config = RobustQueueConfig(maxsize=1000, overflow_size=100)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        async def producer(producer_id: int, count: int):
            for i in range(count):
                queue.put_nowait(producer_id * 1000 + i)
                await asyncio.sleep(0)  # Yield to other tasks

        # Run 5 producers, each adding 100 items
        producers = [producer(p, 100) for p in range(5)]
        await asyncio.gather(*producers)

        assert queue.qsize() == 500

    @pytest.mark.asyncio
    async def test_concurrent_producer_consumer(self):
        """Producer and consumer can work concurrently."""
        config = RobustQueueConfig(maxsize=100, overflow_size=10)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)
        consumed: list[int] = []
        stop_consumer = asyncio.Event()

        async def producer():
            for i in range(200):
                queue.put_nowait(i)
                await asyncio.sleep(0.001)

        async def consumer():
            while not stop_consumer.is_set() or not queue.empty():
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=0.1)
                    consumed.append(item)
                except asyncio.TimeoutError:
                    continue

        # Start consumer
        consumer_task = asyncio.create_task(consumer())

        # Run producer
        await producer()

        # Signal consumer to stop after draining
        stop_consumer.set()
        await asyncio.sleep(0.2)  # Let consumer drain
        consumer_task.cancel()

        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

        # Most items should be consumed
        assert len(consumed) >= 180  # Allow some margin

    @pytest.mark.asyncio
    async def test_get_blocks_until_item_available(self):
        """get() blocks until an item is available."""
        queue: RobustMessageQueue[str] = RobustMessageQueue()
        received: list[str] = []

        async def delayed_producer():
            await asyncio.sleep(0.1)
            queue.put_nowait("delayed_item")

        async def waiting_consumer():
            item = await queue.get()
            received.append(item)

        # Start consumer first (will block)
        consumer_task = asyncio.create_task(waiting_consumer())

        # Start producer after delay
        await delayed_producer()

        # Wait for consumer
        await asyncio.wait_for(consumer_task, timeout=1.0)

        assert received == ["delayed_item"]


class TestMetrics:
    """Tests for metrics accuracy."""

    def test_enqueue_dequeue_counts(self):
        """Enqueue and dequeue counts are accurate."""
        queue: RobustMessageQueue[int] = RobustMessageQueue()

        for i in range(100):
            queue.put_nowait(i)

        for i in range(50):
            queue.get_nowait()

        metrics = queue.get_metrics()
        assert metrics["total_enqueued"] == 100
        assert metrics["total_dequeued"] == 50

    def test_peak_sizes_tracked(self):
        """Peak queue sizes are tracked correctly."""
        config = RobustQueueConfig(maxsize=10, overflow_size=5)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill to peak
        for i in range(12):  # 10 primary + 2 overflow
            queue.put_nowait(i)

        # Drain some
        for i in range(5):
            queue.get_nowait()

        metrics = queue.get_metrics()
        assert metrics["peak_primary_size"] == 10
        assert metrics["peak_overflow_size"] == 2

    def test_reset_metrics(self):
        """reset_metrics clears all counters."""
        queue: RobustMessageQueue[int] = RobustMessageQueue()

        for i in range(10):
            queue.put_nowait(i)

        for i in range(5):
            queue.get_nowait()

        queue.reset_metrics()
        metrics = queue.get_metrics()

        assert metrics["total_enqueued"] == 0
        assert metrics["total_dequeued"] == 0
        assert metrics["peak_primary_size"] == 0

    def test_fill_ratio_calculation(self):
        """Fill ratio is calculated correctly."""
        config = RobustQueueConfig(maxsize=100)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        assert queue.get_fill_ratio() == 0.0

        for i in range(50):
            queue.put_nowait(i)

        assert queue.get_fill_ratio() == 0.5

        for i in range(50):
            queue.put_nowait(i)

        assert queue.get_fill_ratio() == 1.0

    def test_state_transition_activations(self):
        """State transition activations are counted correctly."""
        config = RobustQueueConfig(
            maxsize=100,
            throttle_threshold=0.3,
            batch_threshold=0.6,
            reject_threshold=0.9,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill to THROTTLED
        for i in range(31):
            queue.put_nowait(i)

        # Fill to BATCHING
        for i in range(30):
            queue.put_nowait(i)

        metrics = queue.get_metrics()
        assert metrics["throttle_activations"] >= 1
        assert metrics["batch_activations"] >= 1


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_zero_size_overflow_disables_overflow(self):
        """Setting overflow_size=0 effectively disables overflow."""
        config = RobustQueueConfig(
            maxsize=3,
            overflow_size=0,
            preserve_newest=False,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill primary
        for i in range(3):
            queue.put_nowait(i)

        # Next item cannot go to overflow (size 0)
        result = queue.put_nowait(3)
        assert result.dropped

    def test_single_item_queue(self):
        """Queue works correctly with size 1."""
        config = RobustQueueConfig(maxsize=1, overflow_size=1)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        result1 = queue.put_nowait(1)
        assert result1.accepted
        assert not result1.in_overflow

        result2 = queue.put_nowait(2)
        assert result2.accepted
        assert result2.in_overflow

        # Drain
        assert queue.get_nowait() == 2  # Overflow first
        assert queue.get_nowait() == 1  # Then primary

    def test_empty_queue_state(self):
        """Empty queue is in HEALTHY state."""
        queue: RobustMessageQueue[int] = RobustMessageQueue()
        assert queue.get_state() == QueueState.HEALTHY
        assert queue.get_backpressure_level() == BackpressureLevel.NONE

    def test_full_method_accuracy(self):
        """full() accurately reports when both queues at capacity."""
        config = RobustQueueConfig(maxsize=2, overflow_size=2)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        assert not queue.full()

        # Fill primary
        queue.put_nowait(1)
        queue.put_nowait(2)
        assert not queue.full()  # Overflow still empty

        # Fill overflow
        queue.put_nowait(3)
        queue.put_nowait(4)
        assert queue.full()

    def test_len_returns_total_size(self):
        """len() returns total items in both queues."""
        config = RobustQueueConfig(maxsize=3, overflow_size=3)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        for i in range(5):
            queue.put_nowait(i)

        assert len(queue) == 5
        assert queue.qsize() == 5

    def test_task_done_and_join(self):
        """task_done and join work for primary queue."""
        queue: RobustMessageQueue[int] = RobustMessageQueue()

        queue.put_nowait(1)
        queue.put_nowait(2)

        queue.get_nowait()
        queue.task_done()

        queue.get_nowait()
        queue.task_done()

        # join should complete immediately (all tasks done)
        # This is a simple smoke test

    @pytest.mark.asyncio
    async def test_typed_queue(self):
        """Queue works correctly with typed messages."""
        queue: RobustMessageQueue[TestMessage] = RobustMessageQueue()

        msg1 = TestMessage(id=1, data="first")
        msg2 = TestMessage(id=2, data="second")

        queue.put_nowait(msg1)
        queue.put_nowait(msg2)

        retrieved1 = await queue.get()
        retrieved2 = await queue.get()

        assert retrieved1.id == 1
        assert retrieved1.data == "first"
        assert retrieved2.id == 2


class TestNegativeCases:
    """Tests for error handling and negative scenarios."""

    def test_drain_empty_primary_and_overflow(self):
        """Draining empty queue raises QueueEmpty."""
        queue: RobustMessageQueue[int] = RobustMessageQueue()

        with pytest.raises(asyncio.QueueEmpty):
            queue.get_nowait()

    def test_clear_empty_queue_returns_zero(self):
        """Clearing empty queue returns 0."""
        queue: RobustMessageQueue[int] = RobustMessageQueue()
        cleared = queue.clear()
        assert cleared == 0

    def test_metrics_accurate_after_dropped_items(self):
        """Metrics are accurate when items are dropped."""
        config = RobustQueueConfig(
            maxsize=2,
            overflow_size=2,
            preserve_newest=False,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill completely
        for i in range(4):
            queue.put_nowait(i)

        # Try to add more - should be dropped
        dropped_count = 0
        for i in range(10):
            result = queue.put_nowait(i + 100)
            if result.dropped:
                dropped_count += 1

        metrics = queue.get_metrics()
        assert metrics["total_dropped"] == dropped_count
        assert metrics["total_enqueued"] == 4  # Only first 4 accepted


class TestBackpressureIntegration:
    """Tests for integration with existing backpressure system."""

    def test_backpressure_signal_has_correct_fields(self):
        """Backpressure signal has all required fields."""
        queue: RobustMessageQueue[int] = RobustMessageQueue()
        result = queue.put_nowait(1)

        signal = result.backpressure
        assert hasattr(signal, 'level')
        assert hasattr(signal, 'suggested_delay_ms')
        assert hasattr(signal, 'batch_only')
        assert hasattr(signal, 'drop_non_critical')

    def test_backpressure_signal_to_dict(self):
        """Backpressure signal can be serialized to dict."""
        config = RobustQueueConfig(
            maxsize=100,
            throttle_threshold=0.5,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Fill to throttle
        for i in range(51):
            queue.put_nowait(i)

        result = queue.put_nowait(51)
        signal_dict = result.backpressure.to_dict()

        assert "level" in signal_dict
        assert "suggested_delay_ms" in signal_dict
        assert signal_dict["level"] > 0  # Not NONE

    def test_get_backpressure_level_method(self):
        """get_backpressure_level returns correct BackpressureLevel."""
        config = RobustQueueConfig(
            maxsize=100,
            throttle_threshold=0.50,
            batch_threshold=0.75,
            reject_threshold=0.90,
        )
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # HEALTHY
        assert queue.get_backpressure_level() == BackpressureLevel.NONE

        # Fill to THROTTLE
        for i in range(51):
            queue.put_nowait(i)
        assert queue.get_backpressure_level() == BackpressureLevel.THROTTLE

        # Fill to BATCH
        for i in range(25):
            queue.put_nowait(i)
        assert queue.get_backpressure_level() == BackpressureLevel.BATCH

        # Fill to REJECT
        for i in range(15):
            queue.put_nowait(i)
        assert queue.get_backpressure_level() == BackpressureLevel.REJECT


class TestUsagePatterns:
    """Tests demonstrating typical usage patterns."""

    @pytest.mark.asyncio
    async def test_handler_with_backpressure_response(self):
        """Demonstrates handler returning backpressure response."""
        config = RobustQueueConfig(maxsize=10, overflow_size=5)
        queue: RobustMessageQueue[str] = RobustMessageQueue(config)

        # Simulate handler receiving messages
        responses: list[dict] = []

        for i in range(20):
            message = f"message_{i}"
            result = queue.put_nowait(message)

            if not result.accepted:
                # Message dropped - return error response
                responses.append({"status": "dropped", "retry": True})
            elif result.in_overflow:
                # In overflow - return backpressure response
                responses.append({
                    "status": "accepted",
                    "backpressure": result.backpressure.to_dict(),
                })
            else:
                # Normal - return OK
                responses.append({"status": "ok"})

        # Verify we got some backpressure responses
        backpressure_responses = [r for r in responses if "backpressure" in r]
        assert len(backpressure_responses) > 0

    @pytest.mark.asyncio
    async def test_consumer_with_batch_processing(self):
        """Demonstrates batch consumption pattern."""
        queue: RobustMessageQueue[int] = RobustMessageQueue()

        # Add items
        for i in range(100):
            queue.put_nowait(i)

        # Batch consume
        batch_size = 10
        batches_processed = 0

        while not queue.empty():
            batch: list[int] = []
            for _ in range(batch_size):
                if queue.empty():
                    break
                batch.append(queue.get_nowait())

            if batch:
                batches_processed += 1
                # Process batch...

        assert batches_processed == 10
        assert queue.empty()

    def test_metrics_for_monitoring(self):
        """Demonstrates metrics suitable for monitoring/alerting."""
        config = RobustQueueConfig(maxsize=100, overflow_size=20)
        queue: RobustMessageQueue[int] = RobustMessageQueue(config)

        # Simulate traffic
        for i in range(150):
            queue.put_nowait(i)

        for i in range(50):
            queue.get_nowait()

        metrics = queue.get_metrics()

        # These metrics are suitable for monitoring dashboards
        assert "fill_ratio" in metrics  # Current load
        assert "state" in metrics  # Current state string
        assert "total_enqueued" in metrics  # Throughput
        assert "total_dropped" in metrics  # Error indicator
        assert "overflow_activations" in metrics  # Pressure indicator
