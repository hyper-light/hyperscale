"""
Hybrid Overload Detection (AD-18).

Combines delta-based detection with absolute safety bounds for robust
overload detection that is self-calibrating yet protected against drift.

Three-tier detection:
1. Primary: Delta-based (% above EMA baseline + trend slope)
2. Secondary: Absolute safety bounds (hard limits)
3. Tertiary: Resource signals (CPU, memory, queue depth)

Final state = max(delta_state, absolute_state, resource_state)
"""

from collections import deque
from dataclasses import dataclass, field
from enum import Enum


class OverloadState(Enum):
    """
    Overload state levels.

    Each level has associated actions:
    - HEALTHY: Normal operation
    - BUSY: Reduce new work intake
    - STRESSED: Shed low-priority requests
    - OVERLOADED: Emergency shedding, only critical operations
    """

    HEALTHY = "healthy"
    BUSY = "busy"
    STRESSED = "stressed"
    OVERLOADED = "overloaded"


# State ordering for max() comparison
_STATE_ORDER = {
    OverloadState.HEALTHY: 0,
    OverloadState.BUSY: 1,
    OverloadState.STRESSED: 2,
    OverloadState.OVERLOADED: 3,
}


@dataclass
class OverloadConfig:
    """Configuration for hybrid overload detection."""

    # Delta detection parameters
    ema_alpha: float = 0.1  # Smoothing factor for baseline (lower = more stable)
    current_window: int = 10  # Samples for current average
    trend_window: int = 20  # Samples for trend calculation

    # Delta thresholds (% above baseline)
    # busy / stressed / overloaded
    delta_thresholds: tuple[float, float, float] = (0.2, 0.5, 1.0)

    # Absolute bounds (milliseconds) - safety rails
    # busy / stressed / overloaded
    absolute_bounds: tuple[float, float, float] = (200.0, 500.0, 2000.0)

    # Resource thresholds (0.0 to 1.0)
    # busy / stressed / overloaded
    cpu_thresholds: tuple[float, float, float] = (0.7, 0.85, 0.95)
    memory_thresholds: tuple[float, float, float] = (0.7, 0.85, 0.95)

    # Trend threshold - positive slope indicates worsening
    # Trend must be combined with elevated delta to trigger (not standalone)
    trend_threshold: float = 0.1  # Rising trend amplifies delta state

    # Minimum samples before delta detection is active
    min_samples: int = 3

    # Warmup samples before baseline is considered stable
    # During warmup, only absolute bounds are used for state detection
    warmup_samples: int = 10

    # Hysteresis: number of consecutive samples at a state before transitioning
    # Prevents flapping between states on single-sample variations
    hysteresis_samples: int = 2


class HybridOverloadDetector:
    """
    Combines delta-based and absolute detection for robust overload detection.

    Delta-based detection is self-calibrating but can miss absolute limits.
    Absolute bounds prevent baseline drift from masking real problems.
    Resource signals provide capacity awareness.

    Example usage:
        detector = HybridOverloadDetector()

        # Record latency samples
        detector.record_latency(50.0)  # 50ms
        detector.record_latency(55.0)
        detector.record_latency(120.0)  # spike

        # Get current state
        state = detector.get_state(cpu_percent=75.0, memory_percent=60.0)
        if state == OverloadState.STRESSED:
            # Shed low-priority requests
            pass
    """

    def __init__(self, config: OverloadConfig | None = None):
        self._config = config or OverloadConfig()

        # Baseline tracking using Exponential Moving Average
        self._baseline_ema: float = 0.0
        self._initialized: bool = False

        # Recent samples for current average
        self._recent: deque[float] = deque(maxlen=self._config.current_window)

        # Delta history for trend calculation
        self._delta_history: deque[float] = deque(maxlen=self._config.trend_window)

        # Sample count
        self._sample_count: int = 0

        # Hysteresis state tracking
        self._current_state: OverloadState = OverloadState.HEALTHY
        self._pending_state: OverloadState = OverloadState.HEALTHY
        self._pending_state_count: int = 0

    def record_latency(self, latency_ms: float) -> None:
        """
        Record a latency sample and update internal state.

        Args:
            latency_ms: Latency in milliseconds. Negative values are clamped to 0.
        """
        # Validate input - negative latencies are invalid
        if latency_ms < 0:
            latency_ms = 0.0

        self._sample_count += 1

        # Track recent samples first (used for current average)
        self._recent.append(latency_ms)

        # Update baseline EMA using configured alpha
        # (warmup only affects delta detection, not EMA calculation)
        if not self._initialized:
            self._baseline_ema = latency_ms
            self._initialized = True
        else:
            alpha = self._config.ema_alpha
            self._baseline_ema = alpha * latency_ms + (1 - alpha) * self._baseline_ema

        # Calculate and track delta (% above baseline)
        # Only track delta after we have enough samples for a meaningful average
        if self._baseline_ema > 0 and len(self._recent) >= self._config.min_samples:
            current_avg = sum(self._recent) / len(self._recent)
            delta = (current_avg - self._baseline_ema) / self._baseline_ema
            self._delta_history.append(delta)

    def _calculate_trend(self) -> float:
        """
        Calculate trend slope using linear regression on delta history.

        Returns positive slope if things are getting worse,
        negative if improving, near-zero if stable.
        """
        if len(self._delta_history) < 3:
            return 0.0

        # Simple linear regression
        n = len(self._delta_history)
        x_sum = sum(range(n))
        y_sum = sum(self._delta_history)
        xy_sum = sum(i * y for i, y in enumerate(self._delta_history))
        x2_sum = sum(i * i for i in range(n))

        denominator = n * x2_sum - x_sum * x_sum
        if denominator == 0:
            return 0.0

        slope = (n * xy_sum - x_sum * y_sum) / denominator
        return slope

    def _get_delta_state(self) -> OverloadState:
        """Get state based on delta detection.

        Delta detection is only active after the warmup period to ensure
        baseline stability. During warmup, returns HEALTHY to let absolute
        bounds handle detection.
        """
        # During warmup, delta detection is not reliable - defer to absolute bounds
        if self._sample_count < self._config.warmup_samples:
            return OverloadState.HEALTHY

        if len(self._recent) < self._config.min_samples:
            return OverloadState.HEALTHY

        current_avg = sum(self._recent) / len(self._recent)
        if self._baseline_ema <= 0:
            return OverloadState.HEALTHY

        delta = (current_avg - self._baseline_ema) / self._baseline_ema
        trend = self._calculate_trend()

        thresholds = self._config.delta_thresholds

        # Determine base state from delta
        if delta > thresholds[2]:
            base_state = OverloadState.OVERLOADED
        elif delta > thresholds[1]:
            base_state = OverloadState.STRESSED
        elif delta > thresholds[0]:
            base_state = OverloadState.BUSY
        else:
            base_state = OverloadState.HEALTHY

        # Rising trend can escalate state by one level (but not trigger from HEALTHY)
        # This prevents trend-only overload triggering without actual elevated latency
        if trend > self._config.trend_threshold and base_state != OverloadState.HEALTHY:
            if base_state == OverloadState.BUSY:
                return OverloadState.STRESSED
            elif base_state == OverloadState.STRESSED:
                return OverloadState.OVERLOADED
            # Already OVERLOADED, can't escalate further
            return OverloadState.OVERLOADED

        return base_state

    def _get_absolute_state(self) -> OverloadState:
        """Get state based on absolute latency bounds."""
        if not self._recent:
            return OverloadState.HEALTHY

        current_avg = sum(self._recent) / len(self._recent)
        bounds = self._config.absolute_bounds

        if current_avg > bounds[2]:
            return OverloadState.OVERLOADED
        elif current_avg > bounds[1]:
            return OverloadState.STRESSED
        elif current_avg > bounds[0]:
            return OverloadState.BUSY
        else:
            return OverloadState.HEALTHY

    def _get_resource_state(
        self,
        cpu_percent: float = 0.0,
        memory_percent: float = 0.0,
    ) -> OverloadState:
        """Get state based on resource utilization."""
        states = [OverloadState.HEALTHY]

        # Normalize to 0-1 range
        cpu = cpu_percent / 100.0
        memory = memory_percent / 100.0

        cpu_thresholds = self._config.cpu_thresholds
        memory_thresholds = self._config.memory_thresholds

        # CPU state
        if cpu > cpu_thresholds[2]:
            states.append(OverloadState.OVERLOADED)
        elif cpu > cpu_thresholds[1]:
            states.append(OverloadState.STRESSED)
        elif cpu > cpu_thresholds[0]:
            states.append(OverloadState.BUSY)

        # Memory state
        if memory > memory_thresholds[2]:
            states.append(OverloadState.OVERLOADED)
        elif memory > memory_thresholds[1]:
            states.append(OverloadState.STRESSED)
        elif memory > memory_thresholds[0]:
            states.append(OverloadState.BUSY)

        return max(states, key=lambda s: _STATE_ORDER[s])

    def _get_raw_state(
        self,
        cpu_percent: float = 0.0,
        memory_percent: float = 0.0,
    ) -> OverloadState:
        """Get raw state without hysteresis (for internal use)."""
        states = [
            self._get_delta_state(),
            self._get_absolute_state(),
            self._get_resource_state(cpu_percent, memory_percent),
        ]
        return max(states, key=lambda s: _STATE_ORDER[s])

    def get_state(
        self,
        cpu_percent: float = 0.0,
        memory_percent: float = 0.0,
    ) -> OverloadState:
        """
        Get current overload state using hybrid detection with hysteresis.

        Combines delta-based, absolute bounds, and resource signals,
        returning the worst (most severe) state. Uses hysteresis to
        prevent flapping between states on single-sample variations.

        State transitions require `hysteresis_samples` consecutive readings
        at the new state before transitioning. Exception: transitions to
        more severe states (escalation) happen immediately to ensure quick
        response to deteriorating conditions.

        Args:
            cpu_percent: Current CPU utilization (0-100)
            memory_percent: Current memory utilization (0-100)

        Returns:
            Current OverloadState
        """
        raw_state = self._get_raw_state(cpu_percent, memory_percent)

        # Fast path: if hysteresis is disabled, return raw state
        if self._config.hysteresis_samples <= 1:
            self._current_state = raw_state
            return raw_state

        # Escalation (getting worse) happens immediately for responsiveness
        if _STATE_ORDER[raw_state] > _STATE_ORDER[self._current_state]:
            self._current_state = raw_state
            self._pending_state = raw_state
            self._pending_state_count = 0
            return raw_state

        # De-escalation (getting better) requires hysteresis
        if raw_state == self._pending_state:
            self._pending_state_count += 1
        else:
            # New pending state
            self._pending_state = raw_state
            self._pending_state_count = 1

        # Transition if we've seen enough consecutive samples at the new state
        if self._pending_state_count >= self._config.hysteresis_samples:
            self._current_state = self._pending_state
            self._pending_state_count = 0

        return self._current_state

    def get_state_str(
        self,
        cpu_percent: float = 0.0,
        memory_percent: float = 0.0,
    ) -> str:
        """Get state as string for compatibility."""
        return self.get_state(cpu_percent, memory_percent).value

    @property
    def baseline(self) -> float:
        """Get current baseline EMA value."""
        return self._baseline_ema

    @property
    def current_average(self) -> float:
        """Get current average from recent samples."""
        if not self._recent:
            return 0.0
        return sum(self._recent) / len(self._recent)

    @property
    def trend(self) -> float:
        """Get current trend slope."""
        return self._calculate_trend()

    @property
    def sample_count(self) -> int:
        """Get total samples recorded."""
        return self._sample_count

    @property
    def in_warmup(self) -> bool:
        """Check if detector is still in warmup period."""
        return self._sample_count < self._config.warmup_samples

    def reset(self) -> None:
        """Reset all state."""
        self._baseline_ema = 0.0
        self._initialized = False
        self._recent.clear()
        self._delta_history.clear()
        self._sample_count = 0
        self._current_state = OverloadState.HEALTHY
        self._pending_state = OverloadState.HEALTHY
        self._pending_state_count = 0

    def get_diagnostics(self) -> dict:
        """
        Get diagnostic information for debugging/monitoring.

        Returns dict with:
        - baseline: Current EMA baseline
        - current_avg: Current window average
        - delta: Current % above baseline
        - trend: Trend slope
        - sample_count: Total samples
        - in_warmup: Whether still in warmup period
        - states: Individual state components
        - hysteresis: Current hysteresis state
        """
        current_avg = self.current_average
        delta = 0.0
        if self._baseline_ema > 0:
            delta = (current_avg - self._baseline_ema) / self._baseline_ema

        return {
            "baseline": self._baseline_ema,
            "current_avg": current_avg,
            "delta": delta,
            "trend": self._calculate_trend(),
            "sample_count": self._sample_count,
            "in_warmup": self._sample_count < self._config.warmup_samples,
            "delta_state": self._get_delta_state().value,
            "absolute_state": self._get_absolute_state().value,
            "current_state": self._current_state.value,
            "pending_state": self._pending_state.value,
            "pending_state_count": self._pending_state_count,
        }
