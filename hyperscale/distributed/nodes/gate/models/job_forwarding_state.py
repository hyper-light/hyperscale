"""
Job forwarding state tracking.

Tracks cross-gate job forwarding and throughput metrics.
"""

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class ForwardingMetrics:
    """Metrics for job forwarding throughput."""

    count: int = 0
    interval_start: float = field(default_factory=time.monotonic)
    last_throughput: float = 0.0
    interval_seconds: float = 10.0

    def record_forward(self) -> None:
        """Record a forwarded job."""
        self.count += 1

    def calculate_throughput(self) -> float:
        """Calculate and reset throughput for the current interval."""
        now = time.monotonic()
        elapsed = now - self.interval_start
        if elapsed >= self.interval_seconds:
            self.last_throughput = self.count / elapsed if elapsed > 0 else 0.0
            self.count = 0
            self.interval_start = now
        return self.last_throughput


@dataclass(slots=True)
class JobForwardingState:
    """
    State container for cross-gate job forwarding.

    Tracks:
    - Throughput metrics for health signal calculation
    - Forwarding configuration

    Note: The actual JobForwardingTracker instance is a separate class
    that manages the cross-gate forwarding logic. This state container
    holds the metrics used for monitoring and health signals.
    """

    # Forwarding throughput metrics (for AD-19 health signals)
    throughput_metrics: ForwardingMetrics = field(default_factory=ForwardingMetrics)

    # Configuration
    forward_timeout: float = 3.0
    max_forward_attempts: int = 3

    def record_forward(self) -> None:
        """Record a successful job forward."""
        self.throughput_metrics.record_forward()

    def get_throughput(self) -> float:
        """Get the current forwarding throughput."""
        return self.throughput_metrics.calculate_throughput()
