"""``LatencyConfig`` -- pickled under the namespace
``hyperscale.distributed.health.latency_tracker`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class LatencyConfig:
    """Configuration for latency tracking."""
    sample_max_age: float = 60.0  # Max age of samples in seconds
    sample_max_count: int = 100   # Max samples to keep per peer
