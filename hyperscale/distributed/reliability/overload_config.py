"""``OverloadConfig`` -- pickled under the namespace
``hyperscale.distributed.reliability.overload`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class OverloadConfig:
    """Configuration for hybrid overload detection."""

    # Delta detection parameters
    ema_alpha: float = 0.1  # Smoothing factor for fast baseline (lower = more stable)
    slow_ema_alpha: float = (
        0.02  # Smoothing factor for stable baseline (for drift detection)
    )
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

    # Baseline drift threshold - detects when fast baseline drifts above slow baseline
    # This catches gradual degradation that delta alone misses because baseline adapts
    # Drift = (fast_ema - slow_ema) / slow_ema
    drift_threshold: float = 0.15  # 15% drift triggers escalation

    # High drift threshold - if drift exceeds this, escalate even from HEALTHY to BUSY
    # This catches the "boiled frog" scenario where latency rises so gradually that
    # delta stays near zero (because fast baseline tracks the rise), but the system
    # has significantly degraded from its original operating point.
    # Set to 2x drift_threshold by default. Set to a very high value to disable.
    high_drift_threshold: float = 0.30  # 30% drift triggers HEALTHY -> BUSY

    # Minimum samples before delta detection is active
    min_samples: int = 3

    # Warmup samples before baseline is considered stable
    # During warmup, only absolute bounds are used for state detection
    warmup_samples: int = 10

    # Hysteresis: number of consecutive samples at a state before transitioning
    # Prevents flapping between states on single-sample variations
    hysteresis_samples: int = 2

    def __post_init__(self) -> None:
        self._validate_ascending("delta_thresholds", self.delta_thresholds)
        self._validate_ascending("absolute_bounds", self.absolute_bounds)
        self._validate_ascending("cpu_thresholds", self.cpu_thresholds)
        self._validate_ascending("memory_thresholds", self.memory_thresholds)

    def _validate_ascending(
        self, name: str, values: tuple[float, float, float]
    ) -> None:
        if not (values[0] <= values[1] <= values[2]):
            raise ValueError(
                f"{name} must be in ascending order: "
                f"got ({values[0]}, {values[1]}, {values[2]})"
            )
