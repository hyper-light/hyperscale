"""``CooperativeRateLimiter`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""

from .rate_limiting_shared import _DEFAULT_CLOCK


class CooperativeRateLimiter:
    """
    Client-side cooperative rate limiter.

    Respects rate limit signals from the server and adjusts
    request rate accordingly.

    Example usage:
        limiter = CooperativeRateLimiter()

        # Before sending request
        await limiter.wait_if_needed("job_submit")

        # After receiving response
        if response.status == 429:
            retry_after = float(response.headers.get("Retry-After", 1.0))
            limiter.handle_rate_limit("job_submit", retry_after)
    """

    def __init__(self, default_backoff: float = 1.0):
        self._default_backoff = default_backoff

        # Per-operation state
        self._blocked_until: dict[str, float] = {}  # operation -> monotonic time

        # Metrics
        self._total_waits: int = 0
        self._total_wait_time: float = 0.0

    async def wait_if_needed(self, operation: str) -> float:
        """
        Wait if operation is currently rate limited.

        Args:
            operation: Type of operation

        Returns:
            Time waited in seconds
        """
        blocked_until = self._blocked_until.get(operation, 0.0)
        now = _DEFAULT_CLOCK.monotonic()

        if blocked_until <= now:
            return 0.0

        wait_time = blocked_until - now
        self._total_waits += 1
        self._total_wait_time += wait_time

        await _DEFAULT_CLOCK.sleep(wait_time)
        return wait_time

    def handle_rate_limit(
        self,
        operation: str,
        retry_after: float | None = None,
    ) -> None:
        """
        Handle rate limit response from server.

        Args:
            operation: Type of operation that was rate limited
            retry_after: Suggested retry time from server
        """
        delay = retry_after if retry_after is not None else self._default_backoff
        self._blocked_until[operation] = _DEFAULT_CLOCK.monotonic() + delay

    def is_blocked(self, operation: str) -> bool:
        """Check if operation is currently blocked."""
        blocked_until = self._blocked_until.get(operation, 0.0)
        return _DEFAULT_CLOCK.monotonic() < blocked_until

    def get_retry_after(self, operation: str) -> float:
        """Get remaining time until operation is unblocked."""
        blocked_until = self._blocked_until.get(operation, 0.0)
        remaining = blocked_until - _DEFAULT_CLOCK.monotonic()
        return max(0.0, remaining)

    def clear(self, operation: str | None = None) -> None:
        """Clear rate limit state for operation (or all if None)."""
        if operation is None:
            self._blocked_until.clear()
        else:
            self._blocked_until.pop(operation, None)

    def get_metrics(self) -> dict:
        """Get cooperative rate limiting metrics."""
        return {
            "total_waits": self._total_waits,
            "total_wait_time": self._total_wait_time,
            "active_blocks": len(self._blocked_until),
        }
