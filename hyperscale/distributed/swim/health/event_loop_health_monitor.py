"""``EventLoopHealthMonitor`` -- pickled under the namespace
``hyperscale.distributed.swim.health.health_monitor`` (see that module)."""

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Awaitable
from collections import deque
from itertools import filterfalse
from operator import methodcaller
from hyperscale.logging.hyperscale_logging_models import ServerDebug
from hyperscale.distributed.swim.core.protocols import LoggerProtocol, TaskRunnerProtocol

from .event_loop_health_stats import EventLoopHealthStats
from .health_monitor_shared import _DEFAULT_CLOCK
from .health_sample import HealthSample


@dataclass(slots=True)
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
    
    # Callbacks (all support both sync and async)
    _on_lag_detected: Callable[[float], Awaitable[None] | None] | None = None
    _on_critical_lag: Callable[[float], Awaitable[None] | None] | None = None
    _on_recovered: Callable[[], Awaitable[None] | None] | None = None
    _on_sample: Callable[[HealthSample], Awaitable[None] | None] | None = None
    
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
    # Log records lost because the logger's write itself failed.
    _log_write_failures: int = 0
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
                # The logger itself failed: nowhere left to report it but
                # these stats.
                self._log_write_failures += 1
    
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
        # Phase 6b: explicit ``loop.create_task`` so the task binds to
        # the loop ``start`` was called from rather than implicitly going
        # through ``get_running_loop`` at task-creation time.
        self._monitor_task = asyncio.get_running_loop().create_task(
            self._monitor_loop()
        )
    
    async def stop(self) -> None:
        """Stop the health monitor."""
        self._running = False
        if self._monitor_task_is_live():
            await self._cancel_and_await_monitor_task()
        self._monitor_task = None

        # Cancel-and-await any pending callback tasks; merely cancelling
        # leaves them alive when callers inspect asyncio.all_tasks().
        await self._cancel_pending_callback_tasks()

    def _monitor_task_is_live(self) -> bool:
        """Whether a monitor task exists and has not finished."""
        return self._monitor_task is not None and not self._monitor_task.done()

    async def _cancel_and_await_monitor_task(self) -> None:
        """Cancel the monitor task and wait for it, re-raising a cancel aimed at the caller meanwhile."""
        self._monitor_task.cancel()
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        try:
            await self._monitor_task
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise

    async def _cancel_pending_callback_tasks(self) -> None:
        """Cancel every unfinished callback task, await them all, and forget the set."""
        pending = list(filterfalse(methodcaller("done"), self._pending_callback_tasks))
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._pending_callback_tasks.clear()
    
    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._running:
            if not await self._run_monitor_pass():
                break

    async def _run_monitor_pass(self) -> bool:
        """Take, process and pace one sample; False when cancelled (the loop ends)."""
        try:
            sample = await self._take_sample()
            await self._process_sample(sample)
            await _DEFAULT_CLOCK.sleep(self.sample_interval)
        except asyncio.CancelledError:
            return False
        except Exception:
            # Don't let monitoring errors crash the node
            await _DEFAULT_CLOCK.sleep(self.sample_interval)
        return True
    
    async def _take_sample(self) -> HealthSample:
        """Take a single health measurement."""
        start = _DEFAULT_CLOCK.monotonic()
        await _DEFAULT_CLOCK.sleep(self.expected_sleep)
        end = _DEFAULT_CLOCK.monotonic()
        
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
        
        # Notify of sample (await if callback is async)
        await self._notify_sample(sample)
        
        # Check for lag
        is_lagging = sample.lag_ratio > self.lag_threshold
        is_critical = sample.lag_ratio > self.critical_lag_threshold

        # Track sample stats (counters + consecutive run-length) on
        # every observation so degradation thresholds and recovery
        # debouncing work the same as before. The callback fires below
        # are gated on *state transitions*, not raw samples.
        self._record_sample_counters(is_critical, is_lagging)

        # State transitions — only fire LHM callbacks on the OK→degraded
        # and degraded→OK edges. The previous implementation fired
        # ``on_lag_detected`` on every lagging sample, which (with a
        # default 100 ms sample interval) pumped LHM at up to 10/s
        # under any sustained lag. Per the Lifeguard paper LHM
        # responds to *events*, not raw measurements; the consecutive
        # debouncing already in place gives us the correct edges.
        await self._apply_state_transition(sample, is_critical)

    async def _notify_sample(self, sample: HealthSample) -> None:
        """Hand ``sample`` to on_sample, awaiting the result when the callback is async."""
        if self._on_sample:
            result = self._on_sample(sample)
            if result is not None:
                await result

    def _record_sample_counters(self, is_critical: bool, is_lagging: bool) -> None:
        """Count the sample by severity and advance the consecutive lag/OK run-lengths."""
        if is_critical:
            self._total_critical_samples += 1
            self._consecutive_lag_count = min(
                self._consecutive_lag_count + 1, self.MAX_CONSECUTIVE_COUNT
            )
            self._consecutive_ok_count = 0
        elif is_lagging:
            self._total_lag_samples += 1
            self._consecutive_lag_count = min(
                self._consecutive_lag_count + 1, self.MAX_CONSECUTIVE_COUNT
            )
            self._consecutive_ok_count = 0
        else:
            self._consecutive_ok_count = min(
                self._consecutive_ok_count + 1, self.MAX_CONSECUTIVE_COUNT
            )
            self._consecutive_lag_count = 0

    async def _apply_state_transition(self, sample: HealthSample, is_critical: bool) -> None:
        """Fire the LHM callbacks on the OK->degraded and degraded->OK edges only (Lifeguard)."""
        was_degraded = self._is_degraded
        if self._should_degrade(was_degraded):
            await self._enter_degraded(sample, is_critical)
        elif self._should_recover(was_degraded):
            self._is_degraded = False
            await self._trigger_callback(self._on_recovered)

    def _should_degrade(self, was_degraded: bool) -> bool:
        """Whether enough consecutive lagging samples move an OK loop to degraded."""
        return not was_degraded and self._consecutive_lag_count >= self.lag_count_to_degrade

    def _should_recover(self, was_degraded: bool) -> bool:
        """Whether enough consecutive OK samples move a degraded loop back to OK."""
        return was_degraded and self._consecutive_ok_count >= self.ok_count_to_recover

    async def _enter_degraded(self, sample: HealthSample, is_critical: bool) -> None:
        """Mark the loop degraded and fire the critical- or plain-lag callback."""
        self._is_degraded = True
        self._degraded_transitions += 1
        if is_critical:
            await self._trigger_callback(
                self._on_critical_lag, sample.lag_ratio
            )
        else:
            await self._trigger_callback(
                self._on_lag_detected, sample.lag_ratio
            )
    
    async def _trigger_callback(
        self,
        callback: Callable[..., Awaitable[None] | None] | None,
        *args: float,
    ) -> None:
        """Trigger a callback, handling both sync and async.

        Async callbacks are awaited directly. ``_trigger_callback`` is
        itself ``async`` and runs on the health-monitor loop, so there
        is no benefit to deferring through ``TaskRunner`` — and a real
        cost: ``TaskRunner.run`` keys tasks by ``call.__name__``, and
        the previous implementation wrapped the awaitable in a closure
        named ``_run_callback`` that collided on every invocation. The
        second invocation onward reused the first task instance and
        silently dropped the freshly-captured ``result`` coroutine —
        the source of the ``coroutine 'HealthAwareServer._on_event_loop_recovered'
        was never awaited`` runtime warnings, and the reason
        ``on_recovered`` only ever ran once (so LHM grew monotonically
        once the loop registered any lag).
        """
        if callback is None:
            return

        try:
            result = callback(*args)
            await self._await_if_coroutine(result)
        except Exception as e:
            await self._log_debug(f"Callback error: {type(e).__name__}: {e}")
    
    @staticmethod
    async def _await_if_coroutine(result: Awaitable[None] | None) -> None:
        """Await a coroutine callback result; a sync callback's result needs nothing."""
        if asyncio.iscoroutine(result):
            await result

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
    
    def get_stats(self) -> EventLoopHealthStats:
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
            'log_write_failures': self._log_write_failures,
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
