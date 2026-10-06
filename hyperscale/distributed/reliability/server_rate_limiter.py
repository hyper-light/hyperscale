"""``ServerRateLimiter`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""

from hyperscale.distributed.reliability.overload import HybridOverloadDetector
from hyperscale.distributed.reliability.priority import RequestPriority

from .adaptive_rate_limit_config import AdaptiveRateLimitConfig
from .adaptive_rate_limiter import AdaptiveRateLimiter
from .rate_limit_config import RateLimitConfig
from .rate_limit_result import RateLimitResult

# The AD-24 operation whose per-client budget a transport request draws
# from, by TCP handler. A handler not listed draws from a budget named for
# itself (the default limits), so no handler's volume consumes another's.
HANDLER_RATE_LIMIT_OPERATIONS: dict[str, str] = {
    "workflow_progress": "progress_update",
    "receive_job_progress": "progress_update",
    "receive_job_progress_report": "progress_update",
    "worker_heartbeat": "heartbeat",
    "manager_status_update": "heartbeat",
    "manager_resource_gossip": "heartbeat",
    "windowed_stats_push": "stats_update",
}


class ServerRateLimiter:
    """
    Server-side rate limiter with health-gated adaptive behavior.

    Thin wrapper around AdaptiveRateLimiter that provides:
    - Per-operation rate limiting
    - Health-gated behavior (only limits under stress for system health)
    - Priority-based request shedding during overload
    - Backward-compatible check() API for TCP/UDP protocols

    Key behaviors:
    - HEALTHY state: Per-operation limits apply
    - BUSY state: Low priority shed + per-operation limits
    - STRESSED state: Fair-share limiting per client
    - OVERLOADED state: Only critical requests pass

    Example usage:
        limiter = ServerRateLimiter()

        # Check rate limit for operation
        result = limiter.check_rate_limit("client-123", "job_submit")
        if not result.allowed:
            return Response(429, headers={"Retry-After": str(result.retry_after_seconds)})

        # For priority-aware limiting
        result = limiter.check_rate_limit_with_priority(
            "client-123",
            "job_submit",
            RequestPriority.HIGH
        )
    """

    def __init__(
        self,
        config: RateLimitConfig | None = None,
        inactive_cleanup_seconds: float = 300.0,  # 5 minutes
        overload_detector: HybridOverloadDetector | None = None,
        adaptive_config: AdaptiveRateLimitConfig | None = None,
        detector_sampled_externally: bool = False,
        overload_retry_after_seconds: float = 1.0,
    ):
        self._inactive_cleanup_seconds = inactive_cleanup_seconds

        # Create adaptive config, merging with RateLimitConfig if provided
        if adaptive_config is None:
            adaptive_config = AdaptiveRateLimitConfig(
                inactive_cleanup_seconds=inactive_cleanup_seconds,
                overload_retry_after_seconds=overload_retry_after_seconds,
            )
            # Merge operation limits from RateLimitConfig if provided
            if config is not None:
                # Convert (bucket_size, refill_rate) to (max_requests, window_size)
                min_window = config.min_window_size_seconds
                operation_limits = {}
                for operation, (
                    bucket_size,
                    refill_rate,
                ) in config.operation_limits.items():
                    window_size = bucket_size / refill_rate if refill_rate > 0 else 10.0
                    operation_limits[operation] = (
                        bucket_size,
                        max(min_window, window_size),
                    )
                # Add default
                default_window = (
                    config.default_bucket_size / config.default_refill_rate
                    if config.default_refill_rate > 0
                    else 10.0
                )
                operation_limits["default"] = (
                    config.default_bucket_size,
                    max(min_window, default_window),
                )
                adaptive_config.operation_limits = operation_limits
                adaptive_config.default_max_requests = config.default_bucket_size
                adaptive_config.default_window_size = max(min_window, default_window)

        # Internal adaptive rate limiter
        self._adaptive = AdaptiveRateLimiter(
            overload_detector=overload_detector,
            config=adaptive_config,
            detector_sampled_externally=detector_sampled_externally,
        )

        # Track for backward compatibility metrics
        self._clients_cleaned: int = 0

    async def check(
        self,
        addr: tuple[str, int],
        raise_on_limit: bool = False,
    ) -> bool:
        """
        Compatibility method matching the simple RateLimiter.check() API.

        This allows ServerRateLimiter to be used as a drop-in replacement
        for the simple RateLimiter in base server code.

        Args:
            addr: Source address tuple (host, port)
            raise_on_limit: If True, raise RateLimitExceeded instead of returning False

        Returns:
            True if request is allowed, False if rate limited

        Raises:
            RateLimitExceeded: If raise_on_limit is True and rate is exceeded
        """
        client_id = f"{addr[0]}:{addr[1]}"
        result = await self._adaptive.check(
            client_id, "default", RequestPriority.NORMAL
        )

        if not result.allowed and raise_on_limit:
            from hyperscale.core.jobs.protocols.rate_limiter import RateLimitExceeded

            raise RateLimitExceeded(f"Rate limit exceeded for {addr[0]}:{addr[1]}")

        return result.allowed

    async def check_handler(
        self,
        addr: tuple[str, int],
        handler_name: str,
        priority: RequestPriority,
    ) -> RateLimitResult:
        """Admit one transport request from ``addr`` for ``handler_name``.

        The request draws from the peer's budget for the handler's AD-24
        operation at ``priority``, the handler's AD-37 class: CONTROL
        traffic (SWIM, cancellation, leadership, consensus) is never
        limited, and a burst on one handler cannot exhaust another's
        budget.
        """
        return await self._adaptive.check(
            f"{addr[0]}:{addr[1]}",
            HANDLER_RATE_LIMIT_OPERATIONS.get(handler_name, handler_name),
            priority,
        )

    def check_sync(self, addr: tuple[str, int]) -> bool:
        """Synchronous rate-limit fast-path for transport-layer callers.

        UDP transport callbacks (``MercurySyncBaseServer.read_udp``) run
        in a sync ``DatagramProtocol.datagram_received`` context where
        ``await`` is unavailable. The previous code called the async
        ``check(...)`` from there, which created a coroutine and
        immediately discarded it (``RuntimeWarning: coroutine
        'ServerRateLimiter.check' was never awaited``); the rate-limit
        path silently never ran and ``if not <coroutine>:`` always
        evaluated False.

        This sync variant uses the *existing* per-client counters'
        non-blocking ``try_acquire`` to enforce the rate limit
        without requiring an event loop. The first contact from a
        new client is permitted (no counter exists yet); a follow-up
        async pass through ``check`` will lazily allocate the counter
        on demand. After that, all subsequent sync calls hit the
        per-client counter directly.
        """
        client_id = f"{addr[0]}:{addr[1]}"

        # CRITICAL priority and HEALTHY-state HIGH/NORMAL traffic are
        # rate-limited via per-client operation counters. Read the
        # cached counter; if absent, allow (the next async path will
        # populate it).
        counters = self._adaptive._operation_counters.get(client_id)
        if counters is None:
            return True

        operation_counter = counters.get("default")
        if operation_counter is None:
            return True

        acquired, _wait_time = operation_counter.try_acquire(1)
        return acquired

    async def check_rate_limit(
        self,
        client_id: str,
        operation: str,
        tokens: int = 1,
    ) -> RateLimitResult:
        """
        Check if a request is within rate limits.

        Args:
            client_id: Identifier for the client
            operation: Type of operation being performed
            tokens: Number of tokens to consume

        Returns:
            RateLimitResult indicating if allowed and retry info
        """
        return await self._adaptive.check(
            client_id, operation, RequestPriority.NORMAL, tokens
        )

    async def check_rate_limit_with_priority(
        self,
        client_id: str,
        operation: str,
        priority: RequestPriority,
        tokens: int = 1,
    ) -> RateLimitResult:
        """
        Check rate limit with priority awareness.

        Use this method when you want priority-based shedding during
        overload conditions.

        Args:
            client_id: Identifier for the client
            operation: Type of operation being performed
            priority: Priority level of the request
            tokens: Number of tokens to consume

        Returns:
            RateLimitResult indicating if allowed
        """
        return await self._adaptive.check(client_id, operation, priority, tokens)

    async def check_rate_limit_async(
        self,
        client_id: str,
        operation: str,
        tokens: int = 1,
        max_wait: float = 0.0,
    ) -> RateLimitResult:
        """
        Check rate limit with optional wait.

        Args:
            client_id: Identifier for the client
            operation: Type of operation being performed
            tokens: Number of tokens to consume
            max_wait: Maximum time to wait if rate limited (0 = no wait)

        Returns:
            RateLimitResult indicating if allowed
        """
        return await self._adaptive.check_async(
            client_id, operation, RequestPriority.NORMAL, tokens, max_wait
        )

    async def check_rate_limit_with_priority_async(
        self,
        client_id: str,
        operation: str,
        priority: RequestPriority,
        tokens: int = 1,
        max_wait: float = 0.0,
    ) -> RateLimitResult:
        """
        Async check rate limit with priority awareness.

        Args:
            client_id: Identifier for the client
            operation: Type of operation being performed
            priority: Priority level of the request
            tokens: Number of tokens to consume
            max_wait: Maximum time to wait if rate limited (0 = no wait)

        Returns:
            RateLimitResult indicating if allowed
        """
        return await self._adaptive.check_async(
            client_id, operation, priority, tokens, max_wait
        )

    async def cleanup_inactive_clients(self) -> int:
        """
        Remove counters for clients that have been inactive.

        Returns:
            Number of clients cleaned up
        """
        cleaned = await self._adaptive.cleanup_inactive_clients()
        self._clients_cleaned += cleaned
        return cleaned

    def reset_client(self, client_id: str) -> None:
        """Reset all counters for a client."""
        self._adaptive.reset_client(client_id)

    def get_client_stats(self, client_id: str) -> dict[str, float]:
        """Get available slots for all operations for a client."""
        return self._adaptive.get_client_stats(client_id)

    def get_metrics(self) -> dict:
        """Get rate limiting metrics."""
        adaptive_metrics = self._adaptive.get_metrics()

        return {
            "total_requests": adaptive_metrics["total_requests"],
            "rate_limited_requests": adaptive_metrics["shed_requests"],
            "rate_limited_rate": adaptive_metrics["shed_rate"],
            "active_clients": adaptive_metrics["active_clients"],
            "clients_cleaned": self._clients_cleaned,
            "current_state": adaptive_metrics["current_state"],
            "shed_by_state": adaptive_metrics["shed_by_state"],
        }

    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self._clients_cleaned = 0
        self._adaptive.reset_metrics()

    @property
    def overload_detector(self) -> HybridOverloadDetector:
        """Get the underlying overload detector for recording latency samples."""
        return self._adaptive.overload_detector

    @property
    def adaptive_limiter(self) -> AdaptiveRateLimiter:
        """Get the underlying adaptive rate limiter."""
        return self._adaptive
