from dataclasses import dataclass, field
from time import monotonic


@dataclass(slots=True)
class ResourceMetrics:
    """Point-in-time resource usage with uncertainty."""

    cpu_percent: float
    cpu_uncertainty: float
    memory_bytes: int
    memory_uncertainty: float
    memory_percent: float
    file_descriptor_count: int
    timestamp_monotonic: float = field(default_factory=monotonic)
    sample_count: int = 1
    process_count: int = 1

    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """Return True if metrics are older than max_age_seconds."""
        return (monotonic() - self.timestamp_monotonic) > max_age_seconds
