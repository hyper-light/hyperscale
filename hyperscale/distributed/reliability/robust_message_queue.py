"""``RobustMessageQueue`` -- pickled under the namespace
``hyperscale.distributed.reliability.robust_queue`` (see that module)."""

import asyncio
from collections import deque
from typing import TypeVar, Generic
from hyperscale.distributed.reliability.backpressure import BackpressureLevel, BackpressureSignal

from .queue_metrics import QueueMetrics
from .queue_put_result import QueuePutResult
from .queue_state import QueueState
from .robust_queue_config import RobustQueueConfig

T = TypeVar("T")


class RobustMessageQueue(Generic[T]):
    """
    A robust async message queue with overflow handling and backpressure.

    This queue provides graceful degradation under load:
    1. Primary queue handles normal traffic
    2. Overflow buffer catches bursts when primary is full
    3. Backpressure signals tell senders to slow down
    4. Only drops messages as last resort (with metrics)

    Thread-safety:
    - Safe for multiple concurrent asyncio tasks
    - put_nowait is synchronous and non-blocking
    - get() is async and blocks until message available

    Example:
        queue = RobustMessageQueue[MyMessage](config)

        # Producer
        result = queue.put_nowait(message)
        if not result.accepted:
            log.warning(f"Message dropped, queue saturated")
        elif result.in_overflow:
            # Return backpressure signal to sender
            return result.backpressure.to_dict()

        # Consumer
        while True:
            message = await queue.get()
            await process(message)
    """

    def __init__(self, config: RobustQueueConfig | None = None):
        self._config = config or RobustQueueConfig()

        # Primary bounded queue
        self._primary: asyncio.Queue[T] = asyncio.Queue(maxsize=self._config.maxsize)

        # Overflow ring buffer (deque with maxlen auto-drops oldest)
        self._overflow: deque[T] = deque(maxlen=self._config.overflow_size)

        # State tracking
        self._last_state = QueueState.HEALTHY
        self._metrics = QueueMetrics()

        # Event for notifying consumers when overflow has items
        self._overflow_not_empty = asyncio.Event()

        # Lock for atomic state transitions
        self._state_lock = asyncio.Lock()

    def put_nowait(self, item: T) -> QueuePutResult:
        """
        Add an item to the queue without blocking.

        Args:
            item: The item to enqueue

        Returns:
            QueuePutResult with acceptance status and backpressure info

        Note:
            This method never raises QueueFull. Instead, it returns
            a result indicating whether the message was accepted,
            went to overflow, or was dropped.
        """
        current_state = self._compute_state()
        fill_ratio = self._primary.qsize() / self._config.maxsize

        # Track state transitions
        self._track_state_transition(current_state)

        # While overflow holds items, every newer one queues behind them:
        # taking a freed primary slot would overtake them.
        if self._overflow:
            return self._handle_overflow(item, fill_ratio)

        # Try primary queue first
        try:
            self._primary.put_nowait(item)
            self._metrics.total_enqueued += 1
            self._metrics.peak_primary_size = max(
                self._metrics.peak_primary_size,
                self._primary.qsize()
            )

            backpressure = self._compute_backpressure(current_state, in_overflow=False)

            return QueuePutResult(
                accepted=True,
                in_overflow=False,
                dropped=False,
                queue_state=current_state,
                fill_ratio=fill_ratio,
                backpressure=backpressure,
            )

        except asyncio.QueueFull:
            # Primary full - try overflow
            return self._handle_overflow(item, fill_ratio)

    def _handle_overflow(self, item: T, fill_ratio: float) -> QueuePutResult:
        """Handle item when primary queue is full."""
        overflow_was_full = len(self._overflow) == self._overflow.maxlen

        if overflow_was_full:
            if self._config.preserve_newest:
                # Drop oldest, accept newest
                self._metrics.total_oldest_dropped += 1
            else:
                # Reject new item
                self._metrics.total_dropped += 1
                backpressure = self._compute_backpressure(
                    QueueState.SATURATED,
                    in_overflow=True
                )
                return QueuePutResult(
                    accepted=False,
                    in_overflow=False,
                    dropped=True,
                    queue_state=QueueState.SATURATED,
                    fill_ratio=1.0,
                    backpressure=backpressure,
                )

        # Add to overflow (deque auto-drops oldest if at maxlen)
        self._overflow.append(item)
        self._overflow_not_empty.set()

        self._metrics.total_enqueued += 1
        self._metrics.total_overflow += 1
        self._metrics.peak_overflow_size = max(
            self._metrics.peak_overflow_size,
            len(self._overflow)
        )

        # Determine if we're saturated or just in overflow
        current_state = QueueState.SATURATED if overflow_was_full else QueueState.OVERFLOW
        backpressure = self._compute_backpressure(current_state, in_overflow=True)

        return QueuePutResult(
            accepted=True,
            in_overflow=True,
            dropped=False,
            queue_state=current_state,
            fill_ratio=fill_ratio,
            backpressure=backpressure,
        )

    async def get(self) -> T:
        """
        Remove and return the oldest item in the queue.

        Items overflow only while primary is full, so every overflow item
        is newer than every primary item: the oldest is always primary's
        head, and each slot a get frees is refilled from overflow's head.
        Overflow is therefore non-empty only while primary is full.

        Returns:
            The next item in the queue

        Note:
            Blocks until an item is available.
        """
        item = await self._primary.get()
        # The slot just freed is refilled before anything else runs.
        if self._overflow:
            self._primary.put_nowait(self._overflow.popleft())
            if not self._overflow:
                self._overflow_not_empty.clear()
        self._metrics.total_dequeued += 1
        return item

    def get_nowait(self) -> T:
        """
        Remove and return an item without blocking.

        Raises:
            asyncio.QueueEmpty: If no items available
        """
        # The oldest item is primary's head (see ``get``); may raise QueueEmpty.
        item = self._primary.get_nowait()
        if self._overflow:
            self._primary.put_nowait(self._overflow.popleft())
            if not self._overflow:
                self._overflow_not_empty.clear()
        self._metrics.total_dequeued += 1
        return item

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete."""
        self._primary.task_done()

    async def join(self) -> None:
        """Block until all items in the primary queue have been processed."""
        await self._primary.join()

    def qsize(self) -> int:
        """Return total number of items in both queues."""
        return self._primary.qsize() + len(self._overflow)

    def primary_qsize(self) -> int:
        """Return number of items in primary queue."""
        return self._primary.qsize()

    def overflow_qsize(self) -> int:
        """Return number of items in overflow buffer."""
        return len(self._overflow)

    def empty(self) -> bool:
        """Return True if both queues are empty."""
        return self._primary.empty() and not self._overflow

    def full(self) -> bool:
        """Return True if both primary and overflow are at capacity."""
        return (
            self._primary.full() and
            len(self._overflow) >= self._config.overflow_size
        )

    def get_state(self) -> QueueState:
        """Get current queue state."""
        return self._compute_state()

    def get_fill_ratio(self) -> float:
        """Get primary queue fill ratio (0.0 - 1.0)."""
        return self._primary.qsize() / self._config.maxsize

    def get_backpressure_level(self) -> BackpressureLevel:
        """Get current backpressure level based on queue state."""
        state = self._compute_state()

        if state == QueueState.HEALTHY:
            return BackpressureLevel.NONE
        elif state == QueueState.THROTTLED:
            return BackpressureLevel.THROTTLE
        elif state == QueueState.BATCHING:
            return BackpressureLevel.BATCH
        else:  # OVERFLOW or SATURATED
            return BackpressureLevel.REJECT

    def get_metrics(self) -> dict:
        """Get queue metrics as dictionary."""
        return {
            "primary_size": self._primary.qsize(),
            "primary_capacity": self._config.maxsize,
            "overflow_size": len(self._overflow),
            "overflow_capacity": self._config.overflow_size,
            "fill_ratio": self.get_fill_ratio(),
            "state": self.get_state().name,
            "backpressure_level": self.get_backpressure_level().name,
            "total_enqueued": self._metrics.total_enqueued,
            "total_dequeued": self._metrics.total_dequeued,
            "total_overflow": self._metrics.total_overflow,
            "total_dropped": self._metrics.total_dropped,
            "total_oldest_dropped": self._metrics.total_oldest_dropped,
            "peak_primary_size": self._metrics.peak_primary_size,
            "peak_overflow_size": self._metrics.peak_overflow_size,
            "throttle_activations": self._metrics.throttle_activations,
            "batch_activations": self._metrics.batch_activations,
            "overflow_activations": self._metrics.overflow_activations,
            "saturated_activations": self._metrics.saturated_activations,
        }

    def clear(self) -> int:
        """
        Clear all items from both queues.

        Returns:
            Number of items cleared
        """
        cleared = 0

        # Clear overflow
        cleared += len(self._overflow)
        self._overflow.clear()
        self._overflow_not_empty.clear()

        # Clear primary (no direct clear, so drain it)
        while not self._primary.empty():
            try:
                self._primary.get_nowait()
                cleared += 1
            except asyncio.QueueEmpty:
                break

        return cleared

    def reset_metrics(self) -> None:
        """Reset all metrics counters."""
        self._metrics = QueueMetrics()
        self._last_state = QueueState.HEALTHY

    def _compute_state(self) -> QueueState:
        """Compute current queue state based on fill levels."""
        fill_ratio = self._primary.qsize() / self._config.maxsize

        # Check if using overflow
        if self._primary.full():
            if len(self._overflow) >= self._config.overflow_size:
                return QueueState.SATURATED
            return QueueState.OVERFLOW

        # Check backpressure thresholds
        if fill_ratio >= self._config.reject_threshold:
            return QueueState.OVERFLOW  # About to overflow
        elif fill_ratio >= self._config.batch_threshold:
            return QueueState.BATCHING
        elif fill_ratio >= self._config.throttle_threshold:
            return QueueState.THROTTLED
        else:
            return QueueState.HEALTHY

    def _track_state_transition(self, new_state: QueueState) -> None:
        """Track state transitions for metrics."""
        if new_state != self._last_state:
            if new_state == QueueState.THROTTLED:
                self._metrics.throttle_activations += 1
            elif new_state == QueueState.BATCHING:
                self._metrics.batch_activations += 1
            elif new_state == QueueState.OVERFLOW:
                self._metrics.overflow_activations += 1
            elif new_state == QueueState.SATURATED:
                self._metrics.saturated_activations += 1

            self._last_state = new_state

    def _compute_backpressure(
        self,
        state: QueueState,
        in_overflow: bool
    ) -> BackpressureSignal:
        """Compute backpressure signal based on state."""
        if state == QueueState.HEALTHY:
            return BackpressureSignal(level=BackpressureLevel.NONE)

        elif state == QueueState.THROTTLED:
            return BackpressureSignal(
                level=BackpressureLevel.THROTTLE,
                suggested_delay_ms=self._config.suggested_throttle_delay_ms,
            )

        elif state == QueueState.BATCHING:
            return BackpressureSignal(
                level=BackpressureLevel.BATCH,
                suggested_delay_ms=self._config.suggested_batch_delay_ms,
                batch_only=True,
            )

        elif state == QueueState.OVERFLOW:
            return BackpressureSignal(
                level=BackpressureLevel.REJECT,
                suggested_delay_ms=self._config.suggested_overflow_delay_ms,
                batch_only=True,
                drop_non_critical=True,
            )

        else:  # SATURATED
            return BackpressureSignal(
                level=BackpressureLevel.REJECT,
                suggested_delay_ms=self._config.suggested_reject_delay_ms,
                batch_only=True,
                drop_non_critical=True,
            )

    def __len__(self) -> int:
        """Return total items in both queues."""
        return self.qsize()

    def __repr__(self) -> str:
        return (
            f"RobustMessageQueue("
            f"primary={self._primary.qsize()}/{self._config.maxsize}, "
            f"overflow={len(self._overflow)}/{self._config.overflow_size}, "
            f"state={self.get_state().name})"
        )
