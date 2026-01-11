"""
Worker backpressure manager (AD-18, AD-23).

Handles overload detection, circuit breakers, and load shedding
signals for worker health reporting.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.reliability import (
    BackpressureLevel,
    HybridOverloadDetector,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from .registry import WorkerRegistry


class WorkerBackpressureManager:
    """
    Manages backpressure and overload detection for worker.

    Combines CPU, memory, and latency signals to determine worker
    health state for gossip reporting (AD-18). Also tracks manager
    backpressure signals (AD-23) to adjust update frequency.
    """

    def __init__(
        self,
        logger: "Logger | None" = None,
        registry: "WorkerRegistry | None" = None,
        poll_interval: float = 0.25,
    ) -> None:
        """
        Initialize backpressure manager.

        Args:
            logger: Logger instance for logging
            registry: WorkerRegistry for manager tracking
            poll_interval: Polling interval for resource sampling (default 250ms)
        """
        self._logger = logger
        self._registry = registry
        self._overload_detector = HybridOverloadDetector()
        self._poll_interval = poll_interval
        self._running = False

        # Manager backpressure tracking (AD-23)
        self._manager_backpressure: dict[str, BackpressureLevel] = {}
        self._backpressure_delay_ms: int = 0

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

        Args:
            manager_id: Manager node identifier
            level: Backpressure level from manager
        """
        self._manager_backpressure[manager_id] = level

    def get_max_backpressure_level(self) -> BackpressureLevel:
        """Get maximum backpressure level across all managers."""
        if not self._manager_backpressure:
            return BackpressureLevel.NONE
        return max(self._manager_backpressure.values(), key=lambda x: x.value)

    def set_backpressure_delay_ms(self, delay_ms: int) -> None:
        """Set backpressure delay from manager."""
        self._backpressure_delay_ms = delay_ms

    def get_backpressure_delay_ms(self) -> int:
        """Get current backpressure delay."""
        return self._backpressure_delay_ms

    def is_overloaded(self) -> bool:
        """Check if worker is currently overloaded."""
        state_str = self.get_overload_state_str()
        return state_str in ("overloaded", "critical")
