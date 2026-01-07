"""
Rate Limiting (AD-24).

Provides adaptive rate limiting that integrates with the HybridOverloadDetector
to avoid false positives during legitimate traffic bursts.

Components:
- SlidingWindowCounter: Deterministic counting without time-division edge cases
- AdaptiveRateLimiter: Health-gated limiting that only activates under stress
- ServerRateLimiter: Per-client rate limiting using adaptive approach
- TokenBucket: Legacy token bucket implementation (kept for compatibility)
- CooperativeRateLimiter: Client-side rate limit tracking
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed_rewrite.reliability.overload import (
    HybridOverloadDetector,
    OverloadConfig,
    OverloadState,
)
from hyperscale.distributed_rewrite.reliability.load_shedding import (
    RequestPriority,
)


@dataclass
class SlidingWindowCounter:
    """
    Sliding window counter for deterministic rate limiting.

    Uses a hybrid approach that combines the current window count with
    a weighted portion of the previous window to provide smooth limiting
    without time-based division edge cases (like TokenBucket's divide-by-zero).

    The count is calculated as:
        effective_count = current_count + previous_count * (1 - window_progress)

    Where window_progress is how far into the current window we are (0.0 to 1.0).

    Example:
        - Window size: 60 seconds
        - Previous window: 100 requests
        - Current window: 30 requests
        - 15 seconds into current window (25% progress)
        - Effective count = 30 + 100 * 0.75 = 105

    Thread-safety note: All operations run atomically within a single event
    loop iteration. The async method uses an asyncio.Lock to prevent race
    conditions across await points.
    """

    window_size_seconds: float
    max_requests: int

    # Internal state
    _current_count: int = field(init=False, default=0)
    _previous_count: int = field(init=False, default=0)
    _window_start: float = field(init=False)
    _async_lock: asyncio.Lock = field(init=False)

    def __post_init__(self) -> None:
        self._window_start = time.monotonic()
        self._async_lock = asyncio.Lock()

    def _maybe_rotate_window(self) -> float:
        """
        Check if window needs rotation and return window progress.

        Returns:
            Window progress as float from 0.0 to 1.0
        """
        now = time.monotonic()
        elapsed = now - self._window_start

        # Check if we've passed the window boundary
        if elapsed >= self.window_size_seconds:
            # How many complete windows have passed?
            windows_passed = int(elapsed / self.window_size_seconds)

            if windows_passed >= 2:
                # Multiple windows passed - both previous and current are stale
                self._previous_count = 0
                self._current_count = 0
            else:
                # Exactly one window passed - rotate
                self._previous_count = self._current_count
                self._current_count = 0

            # Move window start forward by complete windows
            self._window_start += windows_passed * self.window_size_seconds
            elapsed = now - self._window_start

        return elapsed / self.window_size_seconds

    def get_effective_count(self) -> float:
        """
        Get the effective request count using sliding window calculation.

        Returns:
            Weighted count of requests in the sliding window
        """
        window_progress = self._maybe_rotate_window()
        return self._current_count + self._previous_count * (1.0 - window_progress)

    def try_acquire(self, count: int = 1) -> tuple[bool, float]:
        """
        Try to acquire request slots from the window.

        Args:
            count: Number of request slots to acquire

        Returns:
            Tuple of (acquired, wait_seconds). If not acquired,
            wait_seconds indicates estimated time until slots available.
        """
        effective = self.get_effective_count()

        if effective + count <= self.max_requests:
            self._current_count += count
            return True, 0.0

        # Calculate accurate wait time for sliding window decay
        # We need: current_count + previous_count * (1 - progress) + count <= max_requests
        # After window rotation, current becomes previous, so we need:
        #   0 + total_count * (1 - progress) + count <= max_requests
        # Solving for progress:
        #   progress >= 1 - (max_requests - count) / total_count
        #
        # The wait time is: progress * window_size - elapsed_in_current_window

        now = time.monotonic()
        elapsed_in_window = now - self._window_start

        # Total count that will become "previous" after rotation
        total_count = self._current_count + self._previous_count

        if total_count <= 0:
            # Edge case: no history, just wait for window to rotate
            return False, max(0.0, self.window_size_seconds - elapsed_in_window)

        # Calculate the progress needed for effective count to allow our request
        available_slots = self.max_requests - count
        if available_slots < 0:
            # Request exceeds max even with empty counter
            return False, float('inf')

        # After rotation: effective = 0 + total_count * (1 - progress)
        # We need: total_count * (1 - progress) <= available_slots
        # So: (1 - progress) <= available_slots / total_count
        # progress >= 1 - available_slots / total_count
        required_progress = 1.0 - (available_slots / total_count)

        if required_progress <= 0:
            # Should already be allowed (edge case)
            return False, 0.01  # Small wait to recheck

        # Time from window start to reach required progress
        time_to_progress = required_progress * self.window_size_seconds

        # Account for current window progress and potential rotation
        current_progress = elapsed_in_window / self.window_size_seconds

        if current_progress >= 1.0:
            # Window has already rotated, calculate from new window start
            # After rotation, we're at progress 0 in new window
            wait_time = time_to_progress
        else:
            # We need to wait for window to rotate first, then decay
            time_until_rotation = self.window_size_seconds - elapsed_in_window
            wait_time = time_until_rotation + time_to_progress

        return False, max(0.01, wait_time)

    async def acquire_async(
        self,
        count: int = 1,
        max_wait: float = 10.0,
        retry_increment_factor: float = 0.1,
    ) -> bool:
        """
        Async version that waits for slots if necessary.

        Uses asyncio.Lock to prevent race conditions where multiple coroutines
        wait for slots and all try to acquire after the wait completes.

        The method uses a retry loop with small increments to handle concurrency:
        when multiple coroutines are waiting for slots, only one may succeed after
        the calculated wait time. The retry loop ensures others keep trying in
        small increments rather than failing immediately.

        Args:
            count: Number of request slots to acquire
            max_wait: Maximum time to wait for slots
            retry_increment_factor: Fraction of window size to wait per retry iteration

        Returns:
            True if slots were acquired, False if timed out
        """
        async with self._async_lock:
            total_waited = 0.0
            wait_increment = self.window_size_seconds * retry_increment_factor

            while total_waited < max_wait:
                acquired, wait_time = self.try_acquire(count)
                if acquired:
                    return True

                if wait_time == float('inf'):
                    return False

                # Wait in small increments to handle concurrency
                # Use the smaller of: calculated wait time, increment, or remaining time
                actual_wait = min(wait_time, wait_increment, max_wait - total_waited)
                if actual_wait <= 0:
                    return False

                await asyncio.sleep(actual_wait)
                total_waited += actual_wait

            # Final attempt after exhausting max_wait
            acquired, _ = self.try_acquire(count)
            return acquired

    @property
    def available_slots(self) -> float:
        """Get estimated available request slots."""
        effective = self.get_effective_count()
        return max(0.0, self.max_requests - effective)

    def reset(self) -> None:
        """Reset the counter to empty state."""
        self._current_count = 0
        self._previous_count = 0
        self._window_start = time.monotonic()


@dataclass
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
    async_retry_increment_factor: float = 0.1  # Fraction of window size per retry iteration

    def get_operation_limits(self, operation: str) -> tuple[int, float]:
        """Get max_requests and window_size for an operation."""
        return self.operation_limits.get(
            operation,
            (self.default_max_requests, self.default_window_size),
        )


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
    ):
        self._detector = overload_detector or HybridOverloadDetector()
        self._config = config or AdaptiveRateLimitConfig()

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

        # Lock for async operations
        self._async_lock = asyncio.Lock()

    def check(
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
        self._client_last_activity[client_id] = time.monotonic()

        # Get current system state
        state = self._detector.get_state()

        # Check priority-based bypass first (CRITICAL always passes)
        if priority == RequestPriority.CRITICAL:
            self._allowed_requests += 1
            self._global_counter.try_acquire(tokens)
            return RateLimitResult(allowed=True, retry_after_seconds=0.0)

        # OVERLOADED: Only CRITICAL passes (handled above)
        if state == OverloadState.OVERLOADED:
            return self._reject_request(state)

        # STRESSED: Apply per-client fair-share limiting
        if state == OverloadState.STRESSED:
            return self._check_stress_counter(client_id, state, tokens)

        # BUSY: Check priority then per-operation limits
        if state == OverloadState.BUSY:
            # LOW priority is shed unconditionally during BUSY
            if priority == RequestPriority.LOW:
                return self._reject_request(state)
            # HIGH and NORMAL go through operation limits

        # HEALTHY or BUSY (non-LOW): Apply per-operation limits
        return self._check_operation_counter(client_id, operation, state, tokens)

    def check_simple(
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
        return self.check(client_id, "default", priority)

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
            result = self.check(client_id, operation, priority, tokens)

            if result.allowed or max_wait <= 0:
                return result

            # Get operation window size for calculating wait increment
            _, window_size = self._config.get_operation_limits(operation)
            wait_increment = window_size * self._config.async_retry_increment_factor

            total_waited = 0.0
            while total_waited < max_wait:
                # Use the smaller of: calculated wait time, increment, or remaining time
                wait_time = min(
                    result.retry_after_seconds,
                    wait_increment,
                    max_wait - total_waited,
                )

                if wait_time <= 0 or result.retry_after_seconds == float('inf'):
                    return result

                await asyncio.sleep(wait_time)
                total_waited += wait_time

                # Re-check after wait (state may have changed)
                result = self.check(client_id, operation, priority, tokens)
                if result.allowed:
                    return result

            # Final check after exhausting max_wait
            return self.check(client_id, operation, priority, tokens)

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

    def _check_operation_counter(
        self,
        client_id: str,
        operation: str,
        state: OverloadState,
        tokens: int,
    ) -> "RateLimitResult":
        """Check and update per-operation counter for client."""
        counter = self._get_or_create_operation_counter(client_id, operation)
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

    def _check_stress_counter(
        self,
        client_id: str,
        state: OverloadState,
        tokens: int,
    ) -> "RateLimitResult":
        """Check and update per-client stress counter."""
        counter = self._get_or_create_stress_counter(client_id, state)
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

    def _get_or_create_operation_counter(
        self,
        client_id: str,
        operation: str,
    ) -> SlidingWindowCounter:
        """Get or create a counter for the client/operation combination."""
        if client_id not in self._operation_counters:
            self._operation_counters[client_id] = {}

        counters = self._operation_counters[client_id]
        if operation not in counters:
            max_requests, window_size = self._config.get_operation_limits(operation)
            counters[operation] = SlidingWindowCounter(
                window_size_seconds=window_size,
                max_requests=max_requests,
            )

        return counters[operation]

    def _get_or_create_stress_counter(
        self,
        client_id: str,
        state: OverloadState,
    ) -> SlidingWindowCounter:
        """Get or create a stress counter for the client based on current state."""
        if client_id not in self._client_stress_counters:
            # Determine limit based on state
            if state == OverloadState.STRESSED:
                max_requests = self._config.stressed_requests_per_window
            else:  # OVERLOADED
                max_requests = self._config.overloaded_requests_per_window

            self._client_stress_counters[client_id] = SlidingWindowCounter(
                window_size_seconds=self._config.window_size_seconds,
                max_requests=max_requests,
            )

        return self._client_stress_counters[client_id]

    def _reject_request(
        self,
        state: OverloadState,
        retry_after: float = 1.0,
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

    def cleanup_inactive_clients(self) -> int:
        """
        Remove counters for clients that have been inactive.

        Returns:
            Number of clients cleaned up
        """
        now = time.monotonic()
        cutoff = now - self._config.inactive_cleanup_seconds

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
            set(self._client_stress_counters.keys()) - set(self._operation_counters.keys())
        )

        return {
            "total_requests": self._total_requests,
            "allowed_requests": self._allowed_requests,
            "shed_requests": self._shed_requests,
            "shed_rate": self._shed_requests / total,
            "shed_by_state": dict(self._shed_by_state),
            "active_clients": active_clients,
            "current_state": self._detector.get_state().value,
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


@dataclass
class TokenBucket:
    """
    Classic token bucket algorithm for rate limiting.

    Tokens are added at a constant rate up to a maximum bucket size.
    Each operation consumes tokens, and operations are rejected when
    the bucket is empty.

    Thread-safety note: Synchronous methods (acquire, try_acquire) are safe
    for use in asyncio as they run atomically within a single event loop
    iteration. The async method (acquire_async) uses an asyncio.Lock to
    prevent race conditions across await points.

    Example usage:
        bucket = TokenBucket(bucket_size=100, refill_rate=10.0)

        # Check if operation is allowed
        if bucket.acquire():
            # Process request
            pass
        else:
            # Rate limited
            return 429
    """

    bucket_size: int
    refill_rate: float  # Tokens per second

    # Internal state
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)
    _async_lock: asyncio.Lock = field(init=False)

    def __post_init__(self) -> None:
        self._tokens = float(self.bucket_size)
        self._last_refill = time.monotonic()
        self._async_lock = asyncio.Lock()

    def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            True if tokens were acquired, False if rate limited
        """
        self._refill()

        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def try_acquire(self, tokens: int = 1) -> tuple[bool, float]:
        """
        Try to acquire tokens and return wait time if not available.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            Tuple of (acquired, wait_seconds). If not acquired,
            wait_seconds indicates how long to wait for tokens.
        """
        self._refill()

        if self._tokens >= tokens:
            self._tokens -= tokens
            return True, 0.0

        # Calculate wait time for tokens to be available
        tokens_needed = tokens - self._tokens

        # If no refill rate, tokens will never become available
        if self.refill_rate <= 0:
            return False, float('inf')

        wait_seconds = tokens_needed / self.refill_rate
        return False, wait_seconds

    async def acquire_async(self, tokens: int = 1, max_wait: float = 10.0) -> bool:
        """
        Async version that waits for tokens if necessary.

        Uses asyncio.Lock to prevent race conditions where multiple coroutines
        wait for tokens and all try to acquire after the wait completes.

        Args:
            tokens: Number of tokens to acquire
            max_wait: Maximum time to wait for tokens

        Returns:
            True if tokens were acquired, False if timed out
        """
        async with self._async_lock:
            acquired, wait_time = self.try_acquire(tokens)
            if acquired:
                return True

            if wait_time > max_wait:
                return False

            # Wait while holding lock - prevents race where multiple waiters
            # all succeed after the wait
            await asyncio.sleep(wait_time)
            return self.acquire(tokens)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill

        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.refill_rate
        self._tokens = min(self.bucket_size, self._tokens + tokens_to_add)
        self._last_refill = now

    @property
    def available_tokens(self) -> float:
        """Get current number of available tokens."""
        self._refill()
        return self._tokens

    def reset(self) -> None:
        """Reset bucket to full capacity."""
        self._tokens = float(self.bucket_size)
        self._last_refill = time.monotonic()


@dataclass
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


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool
    retry_after_seconds: float = 0.0
    tokens_remaining: float = 0.0


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
    ):
        self._inactive_cleanup_seconds = inactive_cleanup_seconds

        # Create adaptive config, merging with RateLimitConfig if provided
        if adaptive_config is None:
            adaptive_config = AdaptiveRateLimitConfig(
                inactive_cleanup_seconds=inactive_cleanup_seconds,
            )
            # Merge operation limits from RateLimitConfig if provided
            if config is not None:
                # Convert (bucket_size, refill_rate) to (max_requests, window_size)
                min_window = config.min_window_size_seconds
                operation_limits = {}
                for operation, (bucket_size, refill_rate) in config.operation_limits.items():
                    window_size = bucket_size / refill_rate if refill_rate > 0 else 10.0
                    operation_limits[operation] = (bucket_size, max(min_window, window_size))
                # Add default
                default_window = config.default_bucket_size / config.default_refill_rate if config.default_refill_rate > 0 else 10.0
                operation_limits["default"] = (config.default_bucket_size, max(min_window, default_window))
                adaptive_config.operation_limits = operation_limits
                adaptive_config.default_max_requests = config.default_bucket_size
                adaptive_config.default_window_size = max(min_window, default_window)

        # Internal adaptive rate limiter
        self._adaptive = AdaptiveRateLimiter(
            overload_detector=overload_detector,
            config=adaptive_config,
        )

        # Track for backward compatibility metrics
        self._clients_cleaned: int = 0

    def check(
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
        result = self._adaptive.check(client_id, "default", RequestPriority.NORMAL)

        if not result.allowed and raise_on_limit:
            from hyperscale.core.jobs.protocols.rate_limiter import RateLimitExceeded
            raise RateLimitExceeded(f"Rate limit exceeded for {addr[0]}:{addr[1]}")

        return result.allowed

    def check_rate_limit(
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
        return self._adaptive.check(client_id, operation, RequestPriority.NORMAL, tokens)

    def check_rate_limit_with_priority(
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
        return self._adaptive.check(client_id, operation, priority, tokens)

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

    def cleanup_inactive_clients(self) -> int:
        """
        Remove counters for clients that have been inactive.

        Returns:
            Number of clients cleaned up
        """
        cleaned = self._adaptive.cleanup_inactive_clients()
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
        now = time.monotonic()

        if blocked_until <= now:
            return 0.0

        wait_time = blocked_until - now
        self._total_waits += 1
        self._total_wait_time += wait_time

        await asyncio.sleep(wait_time)
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
        self._blocked_until[operation] = time.monotonic() + delay

    def is_blocked(self, operation: str) -> bool:
        """Check if operation is currently blocked."""
        blocked_until = self._blocked_until.get(operation, 0.0)
        return time.monotonic() < blocked_until

    def get_retry_after(self, operation: str) -> float:
        """Get remaining time until operation is unblocked."""
        blocked_until = self._blocked_until.get(operation, 0.0)
        remaining = blocked_until - time.monotonic()
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


def is_rate_limit_response(data: bytes) -> bool:
    """
    Check if response data is a RateLimitResponse.

    This is a lightweight check before attempting full deserialization.
    Uses the msgspec message type marker to identify RateLimitResponse.

    Args:
        data: Raw response bytes from TCP handler

    Returns:
        True if this appears to be a RateLimitResponse
    """
    # RateLimitResponse has 'operation' and 'retry_after_seconds' fields
    # Check for common patterns in msgspec serialization
    # This is a heuristic - the full check requires deserialization
    if len(data) < 10:
        return False

    # RateLimitResponse will contain 'operation' field name in the struct
    # For msgspec Struct serialization, look for the field marker
    return b"operation" in data and b"retry_after_seconds" in data


async def handle_rate_limit_response(
    limiter: CooperativeRateLimiter,
    operation: str,
    retry_after_seconds: float,
    wait: bool = True,
) -> float:
    """
    Handle a rate limit response from the server.

    Registers the rate limit with the cooperative limiter and optionally
    waits before returning.

    Args:
        limiter: The CooperativeRateLimiter instance
        operation: The operation that was rate limited
        retry_after_seconds: How long to wait before retrying
        wait: If True, wait for the retry_after period before returning

    Returns:
        Time waited in seconds (0 if wait=False)

    Example:
        # In client code after receiving response
        response_data = await send_tcp(addr, "job_submit", request.dump())
        if is_rate_limit_response(response_data):
            rate_limit = RateLimitResponse.load(response_data)
            await handle_rate_limit_response(
                my_limiter,
                rate_limit.operation,
                rate_limit.retry_after_seconds,
            )
            # Retry the request
            response_data = await send_tcp(addr, "job_submit", request.dump())
    """
    limiter.handle_rate_limit(operation, retry_after_seconds)

    if wait:
        return await limiter.wait_if_needed(operation)

    return 0.0


class RateLimitRetryConfig:
    """Configuration for rate limit retry behavior."""

    def __init__(
        self,
        max_retries: int = 3,
        max_total_wait: float = 60.0,
        backoff_multiplier: float = 1.5,
    ):
        """
        Initialize retry configuration.

        Args:
            max_retries: Maximum number of retry attempts after rate limiting
            max_total_wait: Maximum total time to spend waiting/retrying (seconds)
            backoff_multiplier: Multiplier applied to retry_after on each retry
        """
        self.max_retries = max_retries
        self.max_total_wait = max_total_wait
        self.backoff_multiplier = backoff_multiplier


class RateLimitRetryResult:
    """Result of a rate-limit-aware operation."""

    def __init__(
        self,
        success: bool,
        response: bytes | None,
        retries: int,
        total_wait_time: float,
        final_error: str | None = None,
    ):
        self.success = success
        self.response = response
        self.retries = retries
        self.total_wait_time = total_wait_time
        self.final_error = final_error


async def execute_with_rate_limit_retry(
    operation_func,
    operation_name: str,
    limiter: CooperativeRateLimiter,
    config: RateLimitRetryConfig | None = None,
    response_parser=None,
) -> RateLimitRetryResult:
    """
    Execute an operation with automatic retry on rate limiting.

    This function wraps any async operation and automatically handles
    rate limit responses by waiting the specified retry_after time
    and retrying up to max_retries times.

    Args:
        operation_func: Async function that performs the operation and returns bytes
        operation_name: Name of the operation for rate limiting (e.g., "job_submit")
        limiter: CooperativeRateLimiter to track rate limit state
        config: Retry configuration (defaults to RateLimitRetryConfig())
        response_parser: Optional function to parse response and check if it's
                         a RateLimitResponse. If None, uses is_rate_limit_response.

    Returns:
        RateLimitRetryResult with success status, response, retry count, and wait time

    Example:
        async def submit_job():
            return await send_tcp(gate_addr, "job_submit", submission.dump())

        result = await execute_with_rate_limit_retry(
            submit_job,
            "job_submit",
            my_limiter,
        )

        if result.success:
            job_ack = JobAck.load(result.response)
        else:
            print(f"Failed after {result.retries} retries: {result.final_error}")
    """
    if config is None:
        config = RateLimitRetryConfig()

    if response_parser is None:
        response_parser = is_rate_limit_response

    total_wait_time = 0.0
    retries = 0
    start_time = time.monotonic()

    # Check if we're already blocked for this operation
    if limiter.is_blocked(operation_name):
        initial_wait = await limiter.wait_if_needed(operation_name)
        total_wait_time += initial_wait

    while retries <= config.max_retries:
        # Check if we've exceeded max total wait time
        elapsed = time.monotonic() - start_time
        if elapsed >= config.max_total_wait:
            return RateLimitRetryResult(
                success=False,
                response=None,
                retries=retries,
                total_wait_time=total_wait_time,
                final_error=f"Exceeded max total wait time ({config.max_total_wait}s)",
            )

        try:
            # Execute the operation
            response = await operation_func()

            # Check if response is a rate limit response
            if response and response_parser(response):
                # Parse the rate limit response to get retry_after
                # Import here to avoid circular dependency
                from hyperscale.distributed_rewrite.models import RateLimitResponse

                try:
                    rate_limit = RateLimitResponse.load(response)
                    retry_after = rate_limit.retry_after_seconds

                    # Apply backoff multiplier for subsequent retries
                    if retries > 0:
                        retry_after *= config.backoff_multiplier ** retries

                    # Check if waiting would exceed our limits
                    if total_wait_time + retry_after > config.max_total_wait:
                        return RateLimitRetryResult(
                            success=False,
                            response=response,
                            retries=retries,
                            total_wait_time=total_wait_time,
                            final_error=f"Rate limited, retry_after ({retry_after}s) would exceed max wait",
                        )

                    # Wait and retry
                    limiter.handle_rate_limit(operation_name, retry_after)
                    await asyncio.sleep(retry_after)
                    total_wait_time += retry_after
                    retries += 1
                    continue

                except Exception:
                    # Couldn't parse rate limit response, treat as failure
                    return RateLimitRetryResult(
                        success=False,
                        response=response,
                        retries=retries,
                        total_wait_time=total_wait_time,
                        final_error="Failed to parse rate limit response",
                    )

            # Success - not a rate limit response
            return RateLimitRetryResult(
                success=True,
                response=response,
                retries=retries,
                total_wait_time=total_wait_time,
            )

        except Exception as e:
            # Operation failed with exception
            return RateLimitRetryResult(
                success=False,
                response=None,
                retries=retries,
                total_wait_time=total_wait_time,
                final_error=str(e),
            )

    # Exhausted retries
    return RateLimitRetryResult(
        success=False,
        response=None,
        retries=retries,
        total_wait_time=total_wait_time,
        final_error=f"Exhausted max retries ({config.max_retries})",
    )
