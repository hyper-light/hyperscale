"""``SlidingWindowCounter`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""

import asyncio
from dataclasses import dataclass, field

from .rate_limiting_shared import _DEFAULT_CLOCK


@dataclass(slots=True)
class SlidingWindowCounter:
    """
    Sliding window counter for deterministic rate limiting.

    Uses a hybrid approach that combines the current window count with
    a weighted portion of the previous window to provide smooth limiting
    without time-based division edge cases (a divide-by-zero on a zero refill interval).

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
        self._window_start = _DEFAULT_CLOCK.monotonic()
        self._async_lock = asyncio.Lock()

    def _maybe_rotate_window(self) -> float:
        """
        Check if window needs rotation and return window progress.

        Returns:
            Window progress as float from 0.0 to 1.0
        """
        now = _DEFAULT_CLOCK.monotonic()
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

        now = _DEFAULT_CLOCK.monotonic()
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
            return False, float("inf")

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

                if wait_time == float("inf"):
                    return False

                # Wait in small increments to handle concurrency
                # Use the smaller of: calculated wait time, increment, or remaining time
                actual_wait = min(wait_time, wait_increment, max_wait - total_waited)
                if actual_wait <= 0:
                    return False

                await _DEFAULT_CLOCK.sleep(actual_wait)
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
        self._window_start = _DEFAULT_CLOCK.monotonic()
