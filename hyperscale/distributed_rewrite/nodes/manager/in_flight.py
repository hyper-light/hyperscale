"""
Manager in-flight tracking module.

Implements AD-32 bounded execution with priority-aware in-flight tracking
to prevent unbounded task accumulation and memory exhaustion.

Uses the centralized AD-37 message classification from the reliability module
for consistent priority handling across all node types.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.reliability import (
    RequestPriority,
    classify_handler_to_priority,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.logging import Logger


class InFlightTracker:
    """
    Tracks in-flight requests with per-priority bounds (AD-32).

    Prevents unbounded task accumulation while ensuring critical
    operations are never blocked.

    Priority limits:
    - CRITICAL: Unlimited (always allowed)
    - HIGH: 500 concurrent
    - NORMAL: 300 concurrent
    - LOW: 200 concurrent
    - Global limit: 1000 total
    """

    def __init__(
        self,
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
        global_limit: int = 1000,
        high_limit: int = 500,
        normal_limit: int = 300,
        low_limit: int = 200,
    ) -> None:
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner

        # Per-priority limits (CRITICAL has no limit)
        # Uses RequestPriority enum for AD-37 compliant indexing
        self._limits: dict[RequestPriority, float] = {
            RequestPriority.CRITICAL: float("inf"),
            RequestPriority.HIGH: high_limit,
            RequestPriority.NORMAL: normal_limit,
            RequestPriority.LOW: low_limit,
        }

        # Current counts per priority
        self._counts: dict[RequestPriority, int] = {
            RequestPriority.CRITICAL: 0,
            RequestPriority.HIGH: 0,
            RequestPriority.NORMAL: 0,
            RequestPriority.LOW: 0,
        }

        # Global limit
        self._global_limit = global_limit
        self._global_count = 0

        # Task tracking for cleanup
        self._pending_tasks: set[asyncio.Task] = set()

        # Metrics
        self._acquired_total: int = 0
        self._rejected_total: int = 0
        self._rejected_by_priority: dict[RequestPriority, int] = {
            RequestPriority.CRITICAL: 0,
            RequestPriority.HIGH: 0,
            RequestPriority.NORMAL: 0,
            RequestPriority.LOW: 0,
        }

        # Lock for thread-safe operations
        self._lock = asyncio.Lock()

    async def try_acquire(self, priority: RequestPriority) -> bool:
        """
        Try to acquire a slot for the given priority.

        Args:
            priority: Request priority

        Returns:
            True if slot acquired, False if at limit
        """
        async with self._lock:
            # CRITICAL always allowed (AD-37: CONTROL messages never shed)
            if priority == RequestPriority.CRITICAL:
                self._counts[priority] += 1
                self._global_count += 1
                self._acquired_total += 1
                return True

            # Check priority-specific limit
            if self._counts[priority] >= self._limits[priority]:
                self._rejected_total += 1
                self._rejected_by_priority[priority] += 1
                return False

            # Check global limit (excluding CRITICAL)
            non_critical_count = sum(
                self._counts[p] for p in [
                    RequestPriority.HIGH,
                    RequestPriority.NORMAL,
                    RequestPriority.LOW,
                ]
            )
            if non_critical_count >= self._global_limit:
                self._rejected_total += 1
                self._rejected_by_priority[priority] += 1
                return False

            # Acquire slot
            self._counts[priority] += 1
            self._global_count += 1
            self._acquired_total += 1
            return True

    async def try_acquire_for_handler(self, handler_name: str) -> bool:
        """
        Try to acquire a slot using AD-37 MessageClass classification.

        This is the preferred method for AD-37 compliant bounded execution.

        Args:
            handler_name: Name of the handler (e.g., "receive_workflow_progress")

        Returns:
            True if slot acquired, False if at limit
        """
        priority = classify_handler_to_priority(handler_name)
        return await self.try_acquire(priority)

    async def release(self, priority: RequestPriority) -> None:
        """
        Release a slot for the given priority.

        Args:
            priority: Request priority
        """
        async with self._lock:
            self._counts[priority] = max(0, self._counts[priority] - 1)
            self._global_count = max(0, self._global_count - 1)

    async def release_for_handler(self, handler_name: str) -> None:
        """
        Release a slot using AD-37 MessageClass classification.

        Args:
            handler_name: Name of the handler
        """
        priority = classify_handler_to_priority(handler_name)
        await self.release(priority)

    def try_acquire_sync(self, priority: RequestPriority) -> bool:
        """
        Synchronous version of try_acquire for use in sync callbacks.

        Args:
            priority: Request priority

        Returns:
            True if slot acquired, False if at limit
        """
        # CRITICAL always allowed (AD-37: CONTROL messages never shed)
        if priority == RequestPriority.CRITICAL:
            self._counts[priority] += 1
            self._global_count += 1
            self._acquired_total += 1
            return True

        # Check priority-specific limit
        if self._counts[priority] >= self._limits[priority]:
            self._rejected_total += 1
            self._rejected_by_priority[priority] += 1
            return False

        # Check global limit
        non_critical_count = sum(
            self._counts[p] for p in [
                RequestPriority.HIGH,
                RequestPriority.NORMAL,
                RequestPriority.LOW,
            ]
        )
        if non_critical_count >= self._global_limit:
            self._rejected_total += 1
            self._rejected_by_priority[priority] += 1
            return False

        # Acquire slot
        self._counts[priority] += 1
        self._global_count += 1
        self._acquired_total += 1
        return True

    def try_acquire_sync_for_handler(self, handler_name: str) -> bool:
        """
        Synchronous try_acquire using AD-37 MessageClass classification.

        Args:
            handler_name: Name of the handler

        Returns:
            True if slot acquired, False if at limit
        """
        priority = classify_handler_to_priority(handler_name)
        return self.try_acquire_sync(priority)

    def release_sync(self, priority: RequestPriority) -> None:
        """
        Synchronous version of release.

        Args:
            priority: Request priority
        """
        self._counts[priority] = max(0, self._counts[priority] - 1)
        self._global_count = max(0, self._global_count - 1)

    def release_sync_for_handler(self, handler_name: str) -> None:
        """
        Synchronous release using AD-37 MessageClass classification.

        Args:
            handler_name: Name of the handler
        """
        priority = classify_handler_to_priority(handler_name)
        self.release_sync(priority)

    def track_task(self, task: asyncio.Task, priority: "RequestPriority") -> None:
        """
        Track an asyncio task and auto-release on completion.

        Args:
            task: Task to track
            priority: Priority for auto-release
        """
        self._pending_tasks.add(task)

        def on_done(t: asyncio.Task) -> None:
            self._pending_tasks.discard(t)
            self.release_sync(priority)

        task.add_done_callback(on_done)

    def get_available(self, priority: "RequestPriority") -> int:
        """
        Get number of available slots for priority.

        Args:
            priority: Priority to check

        Returns:
            Number of available slots
        """
        priority_val = priority.value
        if priority_val == 0:
            return 999999  # Unlimited

        limit = self._limits[priority_val]
        current = self._counts[priority_val]
        return int(max(0, limit - current))

    def get_fill_ratio(self) -> float:
        """
        Get global fill ratio (excluding CRITICAL).

        Returns:
            Fill ratio 0.0-1.0
        """
        non_critical = sum(self._counts[p] for p in range(1, 4))
        return non_critical / self._global_limit if self._global_limit > 0 else 0.0

    def get_metrics(self) -> dict:
        """Get in-flight tracking metrics."""
        return {
            "global_count": self._global_count,
            "global_limit": self._global_limit,
            "fill_ratio": self.get_fill_ratio(),
            "critical_count": self._counts[0],
            "high_count": self._counts[1],
            "normal_count": self._counts[2],
            "low_count": self._counts[3],
            "acquired_total": self._acquired_total,
            "rejected_total": self._rejected_total,
            "rejected_critical": self._rejected_by_priority[0],
            "rejected_high": self._rejected_by_priority[1],
            "rejected_normal": self._rejected_by_priority[2],
            "rejected_low": self._rejected_by_priority[3],
            "pending_tasks": len(self._pending_tasks),
        }

    async def cleanup_completed_tasks(self) -> int:
        """
        Cleanup completed tasks from tracking.

        Returns:
            Number of tasks cleaned up
        """
        async with self._lock:
            completed = {t for t in self._pending_tasks if t.done()}
            self._pending_tasks -= completed
            return len(completed)


class BoundedRequestExecutor:
    """
    Executes requests with bounded concurrency and priority awareness (AD-32).

    Combines InFlightTracker with LoadShedder for complete protection.
    """

    def __init__(
        self,
        in_flight: InFlightTracker,
        load_shedder,  # ManagerLoadShedder
        logger: "Logger",
        node_id: str,
        task_runner,
    ) -> None:
        self._in_flight = in_flight
        self._load_shedder = load_shedder
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner

    async def execute_if_allowed(
        self,
        priority: "RequestPriority",
        coro,
        message_type: str = "unknown",
    ):
        """
        Execute coroutine if load shedding and in-flight limits allow.

        Args:
            priority: Request priority
            coro: Coroutine to execute
            message_type: Message type for logging

        Returns:
            Result of coroutine or None if shed/rejected
        """
        # Check load shedding first
        if self._load_shedder.should_shed(priority):
            return None

        # Try to acquire in-flight slot
        if not await self._in_flight.try_acquire(priority):
            return None

        try:
            self._load_shedder.on_request_start()
            return await coro
        finally:
            await self._in_flight.release(priority)
            self._load_shedder.on_request_end()

    def execute_if_allowed_sync(
        self,
        priority: "RequestPriority",
        handler,
        *args,
        message_type: str = "unknown",
        **kwargs,
    ):
        """
        Execute sync handler with tracking and create task if async.

        For use in protocol callbacks where sync execution is required.

        Args:
            priority: Request priority
            handler: Handler function
            *args: Handler args
            message_type: Message type for logging
            **kwargs: Handler kwargs

        Returns:
            Task if async handler, or result if sync, or None if rejected
        """
        # Check load shedding
        if self._load_shedder.should_shed(priority):
            return None

        # Try to acquire slot
        if not self._in_flight.try_acquire_sync(priority):
            return None

        self._load_shedder.on_request_start()

        try:
            result = handler(*args, **kwargs)

            # If handler returns a coroutine, wrap it
            if asyncio.iscoroutine(result):
                async def wrapped():
                    try:
                        return await result
                    finally:
                        self._in_flight.release_sync(priority)
                        self._load_shedder.on_request_end()

                task = asyncio.create_task(wrapped())
                self._in_flight.track_task(task, priority)
                return task
            else:
                # Sync handler, release immediately
                self._in_flight.release_sync(priority)
                self._load_shedder.on_request_end()
                return result

        except Exception:
            self._in_flight.release_sync(priority)
            self._load_shedder.on_request_end()
            raise
