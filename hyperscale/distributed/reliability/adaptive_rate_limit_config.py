"""``AdaptiveRateLimitConfig`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.distributed.reliability.priority import RequestPriority


@dataclass(slots=True)
class AdaptiveRateLimitConfig:
    """
    Configuration for adaptive rate limiting.

    The adaptive rate limiter integrates with HybridOverloadDetector to
    provide health-gated limiting:
    - When HEALTHY: Per-operation limits apply (bursts within limits are fine)
    - When BUSY: Low-priority requests may be limited + per-operation limits
    - When STRESSED: Fair-share limiting per client/operation
    - When OVERLOADED: Only critical requests allowed

    Note: RequestPriority uses IntEnum where lower values = higher priority.
    CRITICAL=0, HIGH=1, NORMAL=2, LOW=3
    """

    # Window configuration for SlidingWindowCounter
    window_size_seconds: float = 60.0

    # Default per-operation limits when system is HEALTHY
    # Operations not in operation_limits use these defaults
    default_max_requests: int = 100
    default_window_size: float = 10.0  # seconds

    # Per-operation limits: operation_name -> (max_requests, window_size_seconds)
    # These apply when system is HEALTHY or BUSY
    operation_limits: dict[str, tuple[int, float]] = field(
        default_factory=lambda: {
            # High-frequency operations get larger limits
            "stats_update": (500, 10.0),
            "heartbeat": (200, 10.0),
            "progress_update": (300, 10.0),
            # Standard operations
            "job_submit": (50, 10.0),
            "job_status": (100, 10.0),
            "workflow_dispatch": (100, 10.0),
            # Infrequent operations
            "cancel": (20, 10.0),
            "reconnect": (10, 10.0),
            # Default for simple check() API
            "default": (100, 10.0),
        }
    )

    # Per-client limits when system is stressed (applied on top of operation limits)
    # These are applied per-client across all operations
    stressed_requests_per_window: int = 100
    overloaded_requests_per_window: int = 10

    # Fair share calculation
    # When stressed, each client gets: global_limit / active_clients
    # This is the minimum guaranteed share even with many clients
    min_fair_share: int = 10

    # Maximum clients to track before cleanup
    max_tracked_clients: int = 10000

    # Inactive client cleanup interval
    inactive_cleanup_seconds: float = 300.0  # 5 minutes
    # A request refused for an overloaded node may come back once the node's
    # load is next sampled (``OVERLOAD_SAMPLE_INTERVAL_SECONDS``): no sooner
    # can its state change.
    overload_retry_after_seconds: float = 1.0

    # Priority thresholds for each overload state
    # Requests with priority <= threshold are allowed (lower = higher priority)
    # BUSY allows HIGH (1) and CRITICAL (0)
    # STRESSED allows only CRITICAL (0) - HIGH goes through counter
    # OVERLOADED allows only CRITICAL (0)
    busy_min_priority: RequestPriority = field(default=RequestPriority.HIGH)
    stressed_min_priority: RequestPriority = field(default=RequestPriority.CRITICAL)
    overloaded_min_priority: RequestPriority = field(default=RequestPriority.CRITICAL)

    # Async retry configuration for handling concurrency
    # When multiple coroutines are waiting for slots, they retry in small increments
    # to handle race conditions where only one can acquire after the calculated wait
    async_retry_increment_factor: float = (
        0.1  # Fraction of window size per retry iteration
    )

    def get_operation_limits(self, operation: str) -> tuple[int, float]:
        """Get max_requests and window_size for an operation."""
        return self.operation_limits.get(
            operation,
            (self.default_max_requests, self.default_window_size),
        )
