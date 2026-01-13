"""
Worker backpressure manager (AD-18, AD-23, AD-37).

Handles overload detection, circuit breakers, and load shedding
signals for worker health reporting. Implements explicit backpressure
policy for progress updates per AD-37.

Note: Backpressure state is delegated to WorkerState to maintain
single source of truth (no duplicate state).
"""

import asyncio
from typing import Callable, TYPE_CHECKING

from hyperscale.distributed.reliability import (
    BackpressureLevel,
    HybridOverloadDetector,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from .registry import WorkerRegistry
    from .state import WorkerState


class WorkerBackpressureManager:
    """
    Manages backpressure and overload detection for worker.

    Combines CPU, memory, and latency signals to determine worker
    health state for gossip reporting (AD-18). Also tracks manager
    backpressure signals (AD-23) to adjust update frequency.

    Delegates backpressure state to WorkerState (single source of truth).
    """

    def __init__(
        self,
        state: "WorkerState",
        logger: "Logger | None" = None,
        registry: "WorkerRegistry | None" = None,
        poll_interval: float = 0.25,
        throttle_delay_ms: int = 500,
        batch_delay_ms: int = 1000,
        reject_delay_ms: int = 2000,
    ) -> None:
        self._state = state
        self._logger = logger
        self._registry = registry
        self._overload_detector = HybridOverloadDetector()
        self._poll_interval = poll_interval
        self._running = False

        # Configurable backpressure delay defaults (AD-37)
        self._throttle_delay_ms = throttle_delay_ms
        self._batch_delay_ms = batch_delay_ms
        self._reject_delay_ms = reject_delay_ms

        # Resource getters (set by server)
        self._get_cpu_percent: callable = lambda: 0.0
        self._get_memory_percent: callable = lambda: 0.0

    def set_resource_getters(
        self,
        cpu_getter: callable,
        memory_getter: callable,
    ) -> None:
        """
        Set resource getter functions.

        Args:
            cpu_getter: Function returning CPU utilization percentage
            memory_getter: Function returning memory utilization percentage
        """
        self._get_cpu_percent = cpu_getter
        self._get_memory_percent = memory_getter

    async def run_overload_poll_loop(self) -> None:
        """
        Fast polling loop for overload detection (AD-18).

        Samples CPU and memory at a fast interval (default 250ms) to ensure
        immediate detection when resources are exhausted.
        """
        self._running = True
        while self._running:
            try:
                await asyncio.sleep(self._poll_interval)

                # Sample current resource usage
                cpu_percent = self._get_cpu_percent()
                memory_percent = self._get_memory_percent()

                # Update detector state - escalation is immediate
                self._overload_detector.get_state(cpu_percent, memory_percent)

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def stop(self) -> None:
        """Stop the polling loop."""
        self._running = False

    def get_overload_state_str(self) -> str:
        """
        Get current overload state as string for health gossip.

        Returns:
            Overload state value string
        """
        cpu = self._get_cpu_percent()
        memory = self._get_memory_percent()
        state = self._overload_detector.get_state(cpu, memory)
        return state.value

    def record_workflow_latency(self, latency_ms: float) -> None:
        """
        Record workflow execution latency for overload detection.

        Args:
            latency_ms: Workflow execution latency in milliseconds
        """
        self._overload_detector.record_latency(latency_ms)

    def set_manager_backpressure(
        self,
        manager_id: str,
        level: BackpressureLevel,
    ) -> None:
        """
        Update backpressure level for a manager (AD-23).

        Delegates to WorkerState (single source of truth).

        Args:
            manager_id: Manager node identifier
            level: Backpressure level from manager
        """
        self._state.set_manager_backpressure(manager_id, level)

    def get_max_backpressure_level(self) -> BackpressureLevel:
        """
        Get maximum backpressure level across all managers.

        Delegates to WorkerState (single source of truth).
        """
        return self._state.get_max_backpressure_level()

    def set_backpressure_delay_ms(self, delay_ms: int) -> None:
        """
        Set backpressure delay from manager.

        Delegates to WorkerState (single source of truth).
        """
        self._state.set_backpressure_delay_ms(delay_ms)

    def get_backpressure_delay_ms(self) -> int:
        """
        Get current backpressure delay.

        Delegates to WorkerState (single source of truth).
        """
        return self._state.get_backpressure_delay_ms()

    def is_overloaded(self) -> bool:
        """Check if worker is currently overloaded."""
        state_str = self.get_overload_state_str()
        return state_str in ("overloaded", "critical")

    # =========================================================================
    # AD-37: Explicit Backpressure Policy Methods
    # =========================================================================

    def should_throttle(self) -> bool:
        """
        Check if progress updates should be throttled (AD-37).

        Returns True when backpressure level is THROTTLE or higher.
        """
        level = self.get_max_backpressure_level()
        return level.value >= BackpressureLevel.THROTTLE.value

    def should_batch_only(self) -> bool:
        """
        Check if only batched progress updates should be sent (AD-37).

        Returns True when backpressure level is BATCH or higher.
        """
        level = self.get_max_backpressure_level()
        return level.value >= BackpressureLevel.BATCH.value

    def should_reject_updates(self) -> bool:
        """
        Check if non-critical progress updates should be dropped (AD-37).

        Returns True when backpressure level is REJECT.
        """
        level = self.get_max_backpressure_level()
        return level.value >= BackpressureLevel.REJECT.value

    def get_throttle_delay_seconds(self) -> float:
        """
        Get additional delay for throttled updates (AD-37).

        Returns delay in seconds based on backpressure state.
        """
        level = self.get_max_backpressure_level()
        delay_ms = self.get_backpressure_delay_ms()

        if level == BackpressureLevel.NONE:
            return 0.0
        elif level == BackpressureLevel.THROTTLE:
            return max(delay_ms, self._throttle_delay_ms) / 1000.0
        elif level == BackpressureLevel.BATCH:
            return max(delay_ms * 2, self._batch_delay_ms) / 1000.0
        else:
            return max(delay_ms * 4, self._reject_delay_ms) / 1000.0

    def get_backpressure_state_name(self) -> str:
        """
        Get human-readable backpressure state name (AD-37).

        Returns state name for logging/metrics.
        """
        level = self.get_max_backpressure_level()
        return {
            BackpressureLevel.NONE: "NO_BACKPRESSURE",
            BackpressureLevel.THROTTLE: "THROTTLED",
            BackpressureLevel.BATCH: "BATCH_ONLY",
            BackpressureLevel.REJECT: "REJECT",
        }.get(level, "UNKNOWN")
