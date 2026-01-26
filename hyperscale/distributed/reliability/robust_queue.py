"""
Robust Message Queue with Backpressure Support.

Provides a bounded async queue with overflow handling, backpressure signaling,
and comprehensive metrics. Designed for distributed systems where message loss
must be minimized while preventing OOM under load.

Features:
- Primary bounded queue with configurable size
- Overflow ring buffer (newest messages preserved)
- Backpressure signals aligned with AD-23
- Per-message priority support
- Comprehensive metrics for observability
- Thread-safe for asyncio concurrent access

Usage:
    queue = RobustMessageQueue(maxsize=1000, overflow_size=100)

    # Producer side
    result = queue.put_nowait(message)
    if result.in_overflow:
        # Signal backpressure to sender
        return BackpressureResponse(retry_after_ms=result.suggested_delay_ms)

    # Consumer side
    message = await queue.get()
"""

import asyncio
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TypeVar, Generic

from hyperscale.distributed.reliability.backpressure import (
    BackpressureLevel,
    BackpressureSignal,
)


T = TypeVar("T")


class QueueState(IntEnum):
    """State of the queue for monitoring."""
    HEALTHY = 0      # Below throttle threshold
    THROTTLED = 1    # Above throttle, below batch
    BATCHING = 2     # Above batch, below reject
    OVERFLOW = 3     # Primary full, using overflow
    SATURATED = 4    # Both primary and overflow full


class QueueFullError(Exception):
    """Raised when both primary and overflow queues are exhausted."""
    pass


@dataclass(slots=True)
class QueuePutResult:
    """Result of a put operation with backpressure information."""
    accepted: bool           # True if message was queued
    in_overflow: bool        # True if message went to overflow buffer
    dropped: bool            # True if message was dropped
    queue_state: QueueState  # Current queue state
    fill_ratio: float        # Primary queue fill ratio (0.0 - 1.0)
    backpressure: BackpressureSignal  # Backpressure signal for sender

    @property
    def suggested_delay_ms(self) -> int:
        """Convenience accessor for backpressure delay."""
        return self.backpressure.suggested_delay_ms


@dataclass(slots=True)
class RobustQueueConfig:
    """Configuration for RobustMessageQueue."""

    # Primary queue settings
    maxsize: int = 1000              # Primary queue capacity

    # Overflow buffer settings
    overflow_size: int = 100         # Overflow ring buffer size
    preserve_newest: bool = True     # If True, drop oldest on overflow full

    # Backpressure thresholds (as fraction of primary capacity)
    throttle_threshold: float = 0.70   # Start suggesting delays
    batch_threshold: float = 0.85      # Suggest batching
    reject_threshold: float = 0.95     # Reject non-critical

    # Timing
    suggested_throttle_delay_ms: int = 50    # Delay at throttle level
    suggested_batch_delay_ms: int = 200      # Delay at batch level
    suggested_reject_delay_ms: int = 500     # Delay at reject level
    suggested_overflow_delay_ms: int = 100   # Delay when in overflow


@dataclass(slots=True)
class QueueMetrics:
    """Metrics for queue observability."""

    total_enqueued: int = 0           # Total messages accepted
    total_dequeued: int = 0           # Total messages consumed
    total_overflow: int = 0           # Messages that went to overflow
    total_dropped: int = 0            # Messages dropped (overflow full)
    total_oldest_dropped: int = 0     # Oldest messages evicted from overflow

    peak_primary_size: int = 0        # High water mark for primary
    peak_overflow_size: int = 0       # High water mark for overflow

    throttle_activations: int = 0     # Times we entered throttle state
    batch_activations: int = 0        # Times we entered batch state
    overflow_activations: int = 0     # Times we entered overflow state
    saturated_activations: int = 0    # Times both queues were full


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
        Remove and return an item from the queue.

        Drains overflow first to maintain FIFO ordering,
        then pulls from primary queue.

        Returns:
            The next item in the queue

        Note:
            Blocks until an item is available.
        """
        # Check overflow first (drain it before primary)
        if self._overflow:
            item = self._overflow.popleft()
            if not self._overflow:
                self._overflow_not_empty.clear()
            self._metrics.total_dequeued += 1
            return item

        # No overflow items - get from primary (may block)
        item = await self._primary.get()
        self._metrics.total_dequeued += 1
        return item

    def get_nowait(self) -> T:
        """
        Remove and return an item without blocking.

        Raises:
            asyncio.QueueEmpty: If no items available
        """
        # Check overflow first
        if self._overflow:
            item = self._overflow.popleft()
            if not self._overflow:
                self._overflow_not_empty.clear()
            self._metrics.total_dequeued += 1
            return item

        # Try primary (may raise QueueEmpty)
        item = self._primary.get_nowait()
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
