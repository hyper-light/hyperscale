from dataclasses import dataclass, field

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(slots=True)
class ResourceMetrics:
    """Point-in-time resource usage with uncertainty."""

    cpu_percent: float
    cpu_uncertainty: float
    memory_bytes: int
    memory_uncertainty: float
    memory_percent: float
    file_descriptor_count: int
    timestamp_monotonic: float = field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())
    sample_count: int = 1
    process_count: int = 1

    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """Return True if metrics are older than max_age_seconds."""
        return (_DEFAULT_CLOCK.monotonic() - self.timestamp_monotonic) > max_age_seconds
