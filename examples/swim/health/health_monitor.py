"""
Event Loop Health Monitor for proactive CPU saturation detection.

Detects event loop lag and system pressure before failures cascade.
Integrates with LHM to proactively adjust timeouts when the node
is under stress.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Any, Protocol
from collections import deque

from hyperscale.logging.hyperscale_logging_models import ServerDebug


class TaskRunnerProtocol(Protocol):
    """Protocol for TaskRunner to avoid circular imports."""
    def run(self, call: Callable, *args, **kwargs) -> Any: ...


class LoggerProtocol(Protocol):
    """Protocol for logger to avoid circular imports."""
    async def log(self, entry: Any) -> None: ...


@dataclass(slots=True)
class HealthSample:
    """
    A single health measurement.
    
    Uses __slots__ for memory efficiency since many instances are created.
    """
    timestamp: float
    expected_sleep: float
    actual_sleep: float
    lag_ratio: float  # (actual - expected) / expected
    
    @property
    def is_lagging(self) -> bool:
        """True if lag is significant (> 50% of expected)."""
        return self.lag_ratio > 0.5


@dataclass
class EventLoopHealthMonitor:
    """
    Monitors event loop health by measuring sleep lag.
    
    When the event loop is overloaded (CPU saturation, GC pauses, etc.),
    scheduled sleeps take longer than expected. This monitor detects
    that lag proactively, before it causes probe timeouts.
    
    Integration with SWIM:
    - When lag is detected, increment LHM proactively
    - This extends timeouts before failures occur
    - Reduces false positive failure detection
    
    Example:
        monitor = EventLoopHealthMonitor(
            on_lag_detected=lambda ratio: lhm.increment(),
            on_recovered=lambda: lhm.decrement(),
        )
        await monitor.start()
    """
    
    # Measurement configuration
    sample_interval: float = 1.0
    """How often to take measurements (seconds)."""
    
    expected_sleep: float = 0.01
    """Expected sleep duration for measurements (10ms default)."""
    
    lag_threshold: float = 0.5
    """Lag ratio threshold to consider "lagging" (50% = 15ms actual for 10ms expected)."""
    
    critical_lag_threshold: float = 2.0
    """Lag ratio for critical overload (200% = 30ms actual for 10ms expected)."""
    
    # Sample history
    history_size: int = 60
    """Number of samples to keep for trend analysis."""
    
    _samples: deque[HealthSample] = field(default_factory=lambda: deque(maxlen=60))
    
    # State
    _running: bool = False
    _monitor_task: asyncio.Task | None = None
    _consecutive_lag_count: int = 0
    _consecutive_ok_count: int = 0
    _is_degraded: bool = False
    
    # Maximum consecutive counter value (prevents unbounded growth)
    MAX_CONSECUTIVE_COUNT: int = 1000
    
    # Thresholds for state transitions
    lag_count_to_degrade: int = 3
    """Consecutive lag samples to enter degraded state."""
    
    ok_count_to_recover: int = 5
    """Consecutive OK samples to exit degraded state."""
    
    # Callbacks
    _on_lag_detected: Callable[[float], Awaitable[None] | None] | None = None
    _on_critical_lag: Callable[[float], Awaitable[None] | None] | None = None
    _on_recovered: Callable[[], Awaitable[None] | None] | None = None
    _on_sample: Callable[[HealthSample], None] | None = None
    
    # TaskRunner for managed async callbacks (optional)
    _task_runner: TaskRunnerProtocol | None = None
    
    # Stats
    _total_samples: int = 0
    _total_lag_samples: int = 0
    _total_critical_samples: int = 0
    _degraded_transitions: int = 0
    _unmanaged_tasks_created: int = 0  # Track fallback task creation
    
    # Track fallback tasks so they can be cleaned up
    _pending_callback_tasks: set[asyncio.Task] = field(default_factory=set)
    
    # Logger for structured logging (optional)
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0
    
    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
        node_id: int,
    ) -> None:
        """Set logger for structured logging."""
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id
    
    async def _log_debug(self, message: str) -> None:
        """Log a debug message."""
        if self._logger:
            try:
                await self._logger.log(ServerDebug(
                    message=f"[HealthMonitor] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                ))
            except Exception:
                pass  # Don't let logging errors propagate
    
    def __post_init__(self):
        self._samples = deque(maxlen=self.history_size)
        self._pending_callback_tasks = set()
    
    def set_callbacks(
        self,
        on_lag_detected: Callable[[float], Awaitable[None] | None] | None = None,
        on_critical_lag: Callable[[float], Awaitable[None] | None] | None = None,
        on_recovered: Callable[[], Awaitable[None] | None] | None = None,
        on_sample: Callable[[HealthSample], None] | None = None,
        task_runner: TaskRunnerProtocol | None = None,
    ) -> None:
        """Set callback functions for health events."""
        self._on_lag_detected = on_lag_detected
        self._on_critical_lag = on_critical_lag
        self._on_recovered = on_recovered
        self._on_sample = on_sample
        self._task_runner = task_runner
    
    async def start(self) -> None:
        """Start the health monitor."""
        if self._running:
            return
        
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
    
    async def stop(self) -> None:
        """Stop the health monitor."""
        self._running = False
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        self._monitor_task = None
        
        # Cancel any pending callback tasks
        for task in list(self._pending_callback_tasks):
            if not task.done():
                task.cancel()
        self._pending_callback_tasks.clear()
    
    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._running:
            try:
                sample = await self._take_sample()
                await self._process_sample(sample)
                await asyncio.sleep(self.sample_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                # Don't let monitoring errors crash the node
                await asyncio.sleep(self.sample_interval)
    
    async def _take_sample(self) -> HealthSample:
        """Take a single health measurement."""
        start = time.monotonic()
        await asyncio.sleep(self.expected_sleep)
        end = time.monotonic()
        
        actual = end - start
        lag_ratio = (actual - self.expected_sleep) / self.expected_sleep
        
        sample = HealthSample(
            timestamp=start,
            expected_sleep=self.expected_sleep,
            actual_sleep=actual,
            lag_ratio=lag_ratio,
        )
        
        return sample
    
    async def _process_sample(self, sample: HealthSample) -> None:
        """Process a sample and trigger callbacks as needed."""
        self._samples.append(sample)
        self._total_samples += 1
        
        # Notify of sample
        if self._on_sample:
            self._on_sample(sample)
        
        # Check for lag
        is_lagging = sample.lag_ratio > self.lag_threshold
        is_critical = sample.lag_ratio > self.critical_lag_threshold
        
        if is_critical:
            self._total_critical_samples += 1
            self._consecutive_lag_count = min(self._consecutive_lag_count + 1, self.MAX_CONSECUTIVE_COUNT)
            self._consecutive_ok_count = 0
            await self._trigger_callback(self._on_critical_lag, sample.lag_ratio)
        elif is_lagging:
            self._total_lag_samples += 1
            self._consecutive_lag_count = min(self._consecutive_lag_count + 1, self.MAX_CONSECUTIVE_COUNT)
            self._consecutive_ok_count = 0
            await self._trigger_callback(self._on_lag_detected, sample.lag_ratio)
        else:
            self._consecutive_ok_count = min(self._consecutive_ok_count + 1, self.MAX_CONSECUTIVE_COUNT)
            self._consecutive_lag_count = 0
        
        # State transitions
        if not self._is_degraded and self._consecutive_lag_count >= self.lag_count_to_degrade:
            self._is_degraded = True
            self._degraded_transitions += 1
        elif self._is_degraded and self._consecutive_ok_count >= self.ok_count_to_recover:
            self._is_degraded = False
            await self._trigger_callback(self._on_recovered)
    
    async def _trigger_callback(
        self,
        callback: Callable[..., Awaitable[None] | None] | None,
        *args: Any,
    ) -> None:
        """Trigger a callback, handling both sync and async."""
        if callback is None:
            return
        
        try:
            result = callback(*args)
            if asyncio.iscoroutine(result):
                # Use TaskRunner if available, otherwise fall back
                if self._task_runner:
                    # Wrap the awaitable in a lambda for TaskRunner
                    async def _run_callback():
                        await result
                    self._task_runner.run(_run_callback)
                else:
                    # Fallback: create task but track it for cleanup
                    self._unmanaged_tasks_created += 1
                    task = asyncio.create_task(result)
                    self._pending_callback_tasks.add(task)
                    # Clean up task from set when done
                    task.add_done_callback(self._pending_callback_tasks.discard)
        except Exception as e:
            await self._log_debug(f"Callback error: {type(e).__name__}: {e}")
    
    @property
    def is_degraded(self) -> bool:
        """True if the event loop is in a degraded state."""
        return self._is_degraded
    
    @property
    def current_lag_ratio(self) -> float:
        """Get the most recent lag ratio."""
        if self._samples:
            return self._samples[-1].lag_ratio
        return 0.0
    
    @property
    def average_lag_ratio(self) -> float:
        """Get the average lag ratio over recent samples."""
        if not self._samples:
            return 0.0
        return sum(s.lag_ratio for s in self._samples) / len(self._samples)
    
    @property
    def max_lag_ratio(self) -> float:
        """Get the maximum lag ratio over recent samples."""
        if not self._samples:
            return 0.0
        return max(s.lag_ratio for s in self._samples)
    
    def get_lag_percentile(self, percentile: float) -> float:
        """Get a percentile of lag ratios (e.g., p99)."""
        if not self._samples:
            return 0.0
        
        sorted_ratios = sorted(s.lag_ratio for s in self._samples)
        idx = int(len(sorted_ratios) * percentile / 100)
        idx = min(idx, len(sorted_ratios) - 1)
        return sorted_ratios[idx]
    
    def get_health_score(self) -> float:
        """
        Get a health score from 0.0 (critical) to 1.0 (healthy).
        
        Based on average lag ratio:
        - 0% lag = 1.0 (healthy)
        - 100% lag = 0.5 (degraded)
        - 200% lag = 0.0 (critical)
        """
        avg_lag = self.average_lag_ratio
        if avg_lag <= 0:
            return 1.0
        elif avg_lag >= self.critical_lag_threshold:
            return 0.0
        else:
            return 1.0 - (avg_lag / self.critical_lag_threshold)
    
    def get_stats(self) -> dict[str, Any]:
        """Get monitoring statistics."""
        return {
            'is_degraded': self._is_degraded,
            'current_lag_ratio': self.current_lag_ratio,
            'average_lag_ratio': self.average_lag_ratio,
            'max_lag_ratio': self.max_lag_ratio,
            'p99_lag_ratio': self.get_lag_percentile(99),
            'health_score': self.get_health_score(),
            'total_samples': self._total_samples,
            'lag_samples': self._total_lag_samples,
            'critical_samples': self._total_critical_samples,
            'degraded_transitions': self._degraded_transitions,
            'consecutive_lag': self._consecutive_lag_count,
            'consecutive_ok': self._consecutive_ok_count,
            'unmanaged_tasks_created': self._unmanaged_tasks_created,
            'pending_callback_tasks': len(self._pending_callback_tasks),
        }
    
    def reset_stats(self) -> None:
        """Reset statistics (but keep monitoring)."""
        self._samples.clear()
        self._total_samples = 0
        self._total_lag_samples = 0
        self._total_critical_samples = 0
        self._degraded_transitions = 0
        self._consecutive_lag_count = 0
        self._consecutive_ok_count = 0
        self._is_degraded = False


async def measure_event_loop_lag() -> float:
    """
    One-shot measurement of event loop lag.
    
    Returns:
        Lag ratio (0.0 = no lag, 1.0 = 100% lag)
    """
    expected = 0.01
    start = time.monotonic()
    await asyncio.sleep(expected)
    actual = time.monotonic() - start
    return (actual - expected) / expected

