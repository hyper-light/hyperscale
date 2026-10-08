"""``AdaptiveRateLimiter`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""

import asyncio
from hyperscale.distributed.reliability.overload import HybridOverloadDetector, OverloadState
from hyperscale.distributed.reliability.priority import RequestPriority

from .rate_limiting_shared import _DEFAULT_CLOCK
from .adaptive_rate_limit_config import AdaptiveRateLimitConfig
from .rate_limit_result import RateLimitResult
from .sliding_window_counter import SlidingWindowCounter


class AdaptiveRateLimiter:
    """
    Health-gated adaptive rate limiter with per-operation limits.

    Integrates with HybridOverloadDetector to provide intelligent rate
    limiting that applies per-operation limits while adjusting behavior
    based on system health:

    - When system is HEALTHY: Per-operation limits apply (controlled bursts)
    - When BUSY: Low-priority requests may be shed + per-operation limits
    - When STRESSED: Fair-share limiting per client kicks in
    - When OVERLOADED: Only critical requests pass

    The key insight is that per-operation limits prevent any single operation
    type from overwhelming the system, while health-gating ensures we shed
    load appropriately under stress.

    Example:
        detector = HybridOverloadDetector()
        limiter = AdaptiveRateLimiter(detector)

        # During normal operation - per-operation limits apply
        result = limiter.check("client-1", "job_submit", RequestPriority.NORMAL)
        assert result.allowed  # True if within operation limits

        # When system stressed - fair share limiting per client
        detector.record_latency(500.0)  # High latency triggers STRESSED
        result = limiter.check("client-1", "job_submit", RequestPriority.NORMAL)
        # Now subject to per-client limits on top of operation limits
    """

    def __init__(
        self,
        overload_detector: HybridOverloadDetector | None = None,
        config: AdaptiveRateLimitConfig | None = None,
        detector_sampled_externally: bool = False,
    ):
        self._detector = overload_detector or HybridOverloadDetector()
        self._config = config or AdaptiveRateLimitConfig()
        # True when the node's resource sampler owns the detector's
        # samples: checks read the state it settled on. Sampling it here
        # with no readings (zero CPU and memory) per request counted toward
        # its de-escalation hysteresis and talked an overloaded node out
        # of its stressed limits.
        self._detector_sampled_externally = detector_sampled_externally

        # Per-client, per-operation sliding window counters
        # Structure: {client_id: {operation: SlidingWindowCounter}}
        self._operation_counters: dict[str, dict[str, SlidingWindowCounter]] = {}

        # Per-client stress counters (used when STRESSED/OVERLOADED)
        self._client_stress_counters: dict[str, SlidingWindowCounter] = {}

        # Track last activity per client for cleanup
        self._client_last_activity: dict[str, float] = {}

        # Global counter for total request tracking (metrics only)
        self._global_counter = SlidingWindowCounter(
            window_size_seconds=self._config.window_size_seconds,
            max_requests=1_000_000,
        )

        # Metrics
        self._total_requests: int = 0
        self._allowed_requests: int = 0
        self._shed_requests: int = 0
        self._shed_by_state: dict[str, int] = {
            "healthy": 0,  # Rate limited by operation limits when healthy
            "busy": 0,
            "stressed": 0,
            "overloaded": 0,
        }

        # Lock for async operations and counter creation
        self._async_lock = asyncio.Lock()
        self._counter_creation_lock = asyncio.Lock()

    def _overload_state(self) -> OverloadState:
        if self._detector_sampled_externally:
            return self._detector.current_state
        return self._detector.get_state()

    async def check(
        self,
        client_id: str,
        operation: str = "default",
        priority: RequestPriority = RequestPriority.NORMAL,
        tokens: int = 1,
    ) -> "RateLimitResult":
        """
        Check if a request should be allowed.

        The decision is based on current system health and per-operation limits:
        - HEALTHY: Per-operation limits apply
        - BUSY: Allow HIGH/CRITICAL priority, apply per-operation limits
        - STRESSED: Apply per-client fair-share limits
        - OVERLOADED: Only CRITICAL allowed

        Args:
            client_id: Identifier for the client
            operation: Type of operation being performed
            priority: Priority level of the request
            tokens: Number of tokens/slots to consume

        Returns:
            RateLimitResult indicating if request is allowed
        """
        self._total_requests += 1
        self._client_last_activity[client_id] = _DEFAULT_CLOCK.monotonic()

        state = self._overload_state()

        if priority == RequestPriority.CRITICAL:
            self._allowed_requests += 1
            self._global_counter.try_acquire(tokens)
            return RateLimitResult(allowed=True, retry_after_seconds=0.0)

        if state == OverloadState.OVERLOADED:
            return self._reject_request(state, self._config.overload_retry_after_seconds)

        if state == OverloadState.STRESSED:
            return await self._check_stress_counter(client_id, state, tokens)

        if state == OverloadState.BUSY:
            if priority == RequestPriority.LOW:
                return self._reject_request(state, self._config.overload_retry_after_seconds)

        return await self._check_operation_counter(client_id, operation, state, tokens)

    async def check_simple(
        self,
        client_id: str,
        priority: RequestPriority = RequestPriority.NORMAL,
    ) -> "RateLimitResult":
        """
        Simplified check without operation tracking.

        Use this for simple per-client rate limiting without operation
        granularity. Uses "default" operation internally.

        Args:
            client_id: Identifier for the client
            priority: Priority level of the request

        Returns:
            RateLimitResult indicating if request is allowed
        """
        return await self.check(client_id, "default", priority)

    async def check_async(
        self,
        client_id: str,
        operation: str = "default",
        priority: RequestPriority = RequestPriority.NORMAL,
        tokens: int = 1,
        max_wait: float = 0.0,
    ) -> "RateLimitResult":
        """
        Async version of check with optional wait.

        Uses a retry loop to handle concurrency: when multiple coroutines are
        waiting for rate limit slots, only one may succeed after the calculated
        wait time. The retry loop ensures others keep trying in small increments
        rather than failing immediately.

        Args:
            client_id: Identifier for the client
            operation: Type of operation being performed
            priority: Priority level of the request
            tokens: Number of tokens/slots to consume
            max_wait: Maximum time to wait if rate limited (0 = no wait)

        Returns:
            RateLimitResult indicating if request is allowed
        """
        async with self._async_lock:
            result = await self.check(client_id, operation, priority, tokens)

            if result.allowed or max_wait <= 0:
                return result

            _, window_size = self._config.get_operation_limits(operation)
            wait_increment = window_size * self._config.async_retry_increment_factor

            total_waited = 0.0
            while total_waited < max_wait:
                wait_time = min(
                    result.retry_after_seconds,
                    wait_increment,
                    max_wait - total_waited,
                )

                if wait_time <= 0 or result.retry_after_seconds == float("inf"):
                    return result

                await _DEFAULT_CLOCK.sleep(wait_time)
                total_waited += wait_time

                result = await self.check(client_id, operation, priority, tokens)
                if result.allowed:
                    return result

            return await self.check(client_id, operation, priority, tokens)

    def _priority_allows_bypass(
        self,
        priority: RequestPriority,
        state: OverloadState,
    ) -> bool:
        """Check if priority allows bypassing rate limiting in current state.

        Note: RequestPriority uses IntEnum where lower values = higher priority.
        CRITICAL=0, HIGH=1, NORMAL=2, LOW=3
        """
        if state == OverloadState.BUSY:
            min_priority = self._config.busy_min_priority
        elif state == OverloadState.STRESSED:
            min_priority = self._config.stressed_min_priority
        else:  # OVERLOADED
            min_priority = self._config.overloaded_min_priority

        # Lower value = higher priority, so priority <= min_priority means allowed
        return priority <= min_priority

    async def _check_operation_counter(
        self,
        client_id: str,
        operation: str,
        state: OverloadState,
        tokens: int,
    ) -> "RateLimitResult":
        """Check and update per-operation counter for client."""
        counter = await self._get_or_create_operation_counter(client_id, operation)
        acquired, wait_time = counter.try_acquire(tokens)

        if acquired:
            self._allowed_requests += 1
            self._global_counter.try_acquire(tokens)
            return RateLimitResult(
                allowed=True,
                retry_after_seconds=0.0,
                tokens_remaining=counter.available_slots,
            )

        return self._reject_request(state, wait_time, counter.available_slots)

    async def _check_stress_counter(
        self,
        client_id: str,
        state: OverloadState,
        tokens: int,
    ) -> "RateLimitResult":
        """Check and update per-client stress counter."""
        counter = await self._get_or_create_stress_counter(client_id)
        acquired, wait_time = counter.try_acquire(tokens)

        if acquired:
            self._allowed_requests += 1
            self._global_counter.try_acquire(tokens)
            return RateLimitResult(
                allowed=True,
                retry_after_seconds=0.0,
                tokens_remaining=counter.available_slots,
            )

        return self._reject_request(state, wait_time, counter.available_slots)

    async def _get_or_create_operation_counter(
        self,
        client_id: str,
        operation: str,
    ) -> SlidingWindowCounter:
        async with self._counter_creation_lock:
            if client_id not in self._operation_counters:
                if len(self._operation_counters) >= self._config.max_tracked_clients:
                    await self._evict_oldest_client()
                self._operation_counters[client_id] = {}

            counters = self._operation_counters[client_id]
            if operation not in counters:
                max_requests, window_size = self._config.get_operation_limits(operation)
                counters[operation] = SlidingWindowCounter(
                    window_size_seconds=window_size,
                    max_requests=max_requests,
                )

            return counters[operation]

    async def _evict_oldest_client(self) -> None:
        if not self._client_last_activity:
            return
        oldest_client = min(
            self._client_last_activity.keys(),
            key=lambda client_id: self._client_last_activity.get(
                client_id, float("inf")
            ),
        )
        self._operation_counters.pop(oldest_client, None)
        self._client_stress_counters.pop(oldest_client, None)
        self._client_last_activity.pop(oldest_client, None)

    async def _get_or_create_stress_counter(
        self,
        client_id: str,
    ) -> SlidingWindowCounter:
        """Get or create the client's STRESSED budget counter."""
        async with self._counter_creation_lock:
            if client_id not in self._client_stress_counters:
                # Bounded as the operation counters are: an OVERLOADED node
                # refuses before counting, so only STRESSED clients get here.
                if len(self._client_stress_counters) >= self._config.max_tracked_clients:
                    await self._evict_oldest_client()
                self._client_stress_counters[client_id] = SlidingWindowCounter(
                    window_size_seconds=self._config.window_size_seconds,
                    max_requests=self._config.stressed_requests_per_window,
                )

            return self._client_stress_counters[client_id]

    def _reject_request(
        self,
        state: OverloadState,
        retry_after: float,
        tokens_remaining: float = 0.0,
    ) -> "RateLimitResult":
        """Record rejection and return result."""
        self._shed_requests += 1
        self._shed_by_state[state.value] += 1

        return RateLimitResult(
            allowed=False,
            retry_after_seconds=retry_after,
            tokens_remaining=tokens_remaining,
        )

    async def cleanup_inactive_clients(self) -> int:
        now = _DEFAULT_CLOCK.monotonic()
        cutoff = now - self._config.inactive_cleanup_seconds

        async with self._async_lock:
            inactive_clients = [
                client_id
                for client_id, last_activity in self._client_last_activity.items()
                if last_activity < cutoff
            ]

            for client_id in inactive_clients:
                self._operation_counters.pop(client_id, None)
                self._client_stress_counters.pop(client_id, None)
                self._client_last_activity.pop(client_id, None)

        return len(inactive_clients)

    def reset_client(self, client_id: str) -> None:
        """Reset all counters for a client."""
        if client_id in self._operation_counters:
            for counter in self._operation_counters[client_id].values():
                counter.reset()
        if client_id in self._client_stress_counters:
            self._client_stress_counters[client_id].reset()

    def get_client_stats(self, client_id: str) -> dict[str, float]:
        """Get available slots for all operations for a client."""
        if client_id not in self._operation_counters:
            return {}

        return {
            operation: counter.available_slots
            for operation, counter in self._operation_counters[client_id].items()
        }

    def get_metrics(self) -> dict:
        """Get rate limiting metrics."""
        total = self._total_requests or 1  # Avoid division by zero

        # Count active clients (those with any counter)
        active_clients = len(self._operation_counters) + len(
            set(self._client_stress_counters.keys())
            - set(self._operation_counters.keys())
        )

        return {
            "total_requests": self._total_requests,
            "allowed_requests": self._allowed_requests,
            "shed_requests": self._shed_requests,
            "shed_rate": self._shed_requests / total,
            "shed_by_state": dict(self._shed_by_state),
            "active_clients": active_clients,
            "current_state": self._overload_state().value,
        }

    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self._total_requests = 0
        self._allowed_requests = 0
        self._shed_requests = 0
        self._shed_by_state = {
            "healthy": 0,
            "busy": 0,
            "stressed": 0,
            "overloaded": 0,
        }

    @property
    def overload_detector(self) -> HybridOverloadDetector:
        """Get the underlying overload detector."""
        return self._detector
