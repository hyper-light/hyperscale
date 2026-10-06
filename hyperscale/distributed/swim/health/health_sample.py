"""``HealthSample`` -- pickled under the namespace
``hyperscale.distributed.swim.health.health_monitor`` (see that module)."""

from dataclasses import dataclass


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
