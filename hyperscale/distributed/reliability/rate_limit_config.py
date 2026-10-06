"""``RateLimitConfig`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class RateLimitConfig:
    """
    Configuration for rate limits per operation type.

    Each operation type has its own bucket configuration.
    """

    # Default limits for unknown operations
    default_bucket_size: int = 100
    default_refill_rate: float = 10.0  # per second

    # Per-operation limits: operation_name -> (bucket_size, refill_rate)
    operation_limits: dict[str, tuple[int, float]] = field(
        default_factory=lambda: {
            # High-frequency operations get larger buckets
            "stats_update": (500, 50.0),
            "heartbeat": (200, 20.0),
            "progress_update": (300, 30.0),
            # Standard operations
            "job_submit": (50, 5.0),
            "job_status": (100, 10.0),
            "workflow_dispatch": (100, 10.0),
            # Infrequent operations
            "cancel": (20, 2.0),
            "reconnect": (10, 1.0),
        }
    )

    # Minimum window size when converting bucket configs to sliding windows
    # Lower values allow faster recovery but may increase CPU usage
    min_window_size_seconds: float = 0.05

    def get_limits(self, operation: str) -> tuple[int, float]:
        """Get bucket size and refill rate for an operation."""
        return self.operation_limits.get(
            operation,
            (self.default_bucket_size, self.default_refill_rate),
        )
