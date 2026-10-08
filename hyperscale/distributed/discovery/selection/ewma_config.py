"""``EWMAConfig`` -- pickled under the namespace
``hyperscale.distributed.discovery.selection.ewma_tracker`` (see that module)."""

from dataclasses import dataclass


@dataclass
class EWMAConfig:
    """Configuration for EWMA tracking."""

    alpha: float = 0.3
    """
    Smoothing factor for EWMA (0 < alpha <= 1).

    Higher alpha gives more weight to recent samples:
    - 0.1: Very smooth, slow to react to changes
    - 0.3: Balanced (default)
    - 0.5: Responsive, moderate smoothing
    - 0.9: Very responsive, minimal smoothing
    """

    initial_estimate_ms: float = 50.0
    """Initial latency estimate for new peers (ms)."""

    failure_penalty_ms: float = 1000.0
    """Latency penalty per consecutive failure (ms)."""

    max_failure_penalty_ms: float = 10000.0
    """Maximum total failure penalty (ms)."""

    decay_interval_seconds: float = 60.0
    """Interval for decaying failure counts."""
