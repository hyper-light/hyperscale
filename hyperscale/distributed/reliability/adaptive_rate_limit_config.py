"""``AdaptiveRateLimitConfig`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""

from dataclasses import dataclass, field

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.reliability.priority import RequestPriority

from .rate_limit_derivation import (
    configured_or_unbounded,
    derive_max_tracked_clients,
    derive_operation_limits,
    derive_rate_limit_window_seconds,
    derive_stressed_max_requests,
)


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

    # Every limit below is derived from the protocol rate it bounds
    # (``rate_limit_derivation``, docs/architecture/AD_24.md). Nodes build
    # their config from their Env (``from_env``); a config built bare takes
    # the derivations of the Env defaults.

    # Window of the per-client STRESSED counters
    window_size_seconds: float = field(default_factory=lambda: derive_rate_limit_window_seconds(Env()))

    # Limit and window of an operation not in operation_limits
    default_max_requests: int = field(
        default_factory=lambda: configured_or_unbounded(Env().RATE_LIMIT_DEFAULT_MAX_REQUESTS)
    )
    default_window_size: float = field(default_factory=lambda: derive_rate_limit_window_seconds(Env()))

    # Per-operation limits: operation_name -> (max_requests, window_size_seconds)
    # These apply when system is HEALTHY or BUSY
    operation_limits: dict[str, tuple[int, float]] = field(default_factory=lambda: derive_operation_limits(Env()))

    # Per-client budget across all operations while STRESSED
    stressed_requests_per_window: int = field(default_factory=lambda: derive_stressed_max_requests(Env()))

    # Maximum clients to track before evicting the least recently active
    max_tracked_clients: int = field(default_factory=lambda: derive_max_tracked_clients(Env(), None))

    # Seconds a client may stay inactive before cleanup removes its counters
    inactive_cleanup_seconds: float = field(default_factory=lambda: Env().RATE_LIMIT_CLIENT_IDLE_TIMEOUT)
    # A request refused for an overloaded node may come back once the node's
    # load is next sampled (``OVERLOAD_SAMPLE_INTERVAL_SECONDS``): no sooner
    # can its state change.
    overload_retry_after_seconds: float = field(default_factory=lambda: Env().OVERLOAD_SAMPLE_INTERVAL_SECONDS)

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

    @classmethod
    def from_env(cls, env: Env, accepted_connection_cap: int | None) -> "AdaptiveRateLimitConfig":
        """The limits ``env`` derives, for a node whose TCP server holds at
        most ``accepted_connection_cap`` connections at once (None: no cap)."""
        default_max_requests, default_window_size = (operation_limits := derive_operation_limits(env))["default"]
        return cls(
            window_size_seconds=derive_rate_limit_window_seconds(env),
            default_max_requests=default_max_requests,
            default_window_size=default_window_size,
            operation_limits=operation_limits,
            stressed_requests_per_window=derive_stressed_max_requests(env),
            max_tracked_clients=derive_max_tracked_clients(env, accepted_connection_cap),
            inactive_cleanup_seconds=env.RATE_LIMIT_CLIENT_IDLE_TIMEOUT,
            overload_retry_after_seconds=env.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
        )

    def get_operation_limits(self, operation: str) -> tuple[int, float]:
        """Get max_requests and window_size for an operation."""
        return self.operation_limits.get(
            operation,
            (self.default_max_requests, self.default_window_size),
        )
