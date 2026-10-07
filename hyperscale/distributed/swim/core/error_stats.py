"""``ErrorStats`` -- pickled under the namespace
``hyperscale.distributed.swim.core.error_handler`` (see that module)."""

from dataclasses import dataclass, field
from collections import deque
from hyperscale.distributed.runtime import Clock, RealClock

from .circuit_state import CircuitState

_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(slots=True)
class ErrorStats:
    """
    Track error rates for circuit breaker decisions.

    Uses a sliding window to calculate recent error rate and
    determine if the circuit should open.

    Memory safety:
    - Timestamps deque is bounded to prevent unbounded growth
    - Prunes old entries on each operation
    """

    window_seconds: float = 60.0
    """Time window for error rate calculation."""

    max_errors: int = 10
    """Circuit opens after this many errors in window."""

    half_open_after: float = 30.0
    """Seconds to wait before attempting recovery."""

    max_timestamps: int = 1000
    """Maximum timestamps to store (prevents memory growth under sustained errors)."""

    # Alias parameters for compatibility
    error_threshold: int | None = None
    """Alias for max_errors (for backwards compatibility)."""

    error_rate_threshold: float = 0.5
    """Error rate threshold (errors per second) for circuit opening."""

    _timestamps: deque[float] = field(default_factory=deque)
    _circuit_state: CircuitState = CircuitState.CLOSED
    _circuit_opened_at: float | None = None

    def __post_init__(self):
        """Initialize bounded deque and handle parameter aliases."""
        # Handle error_threshold alias for max_errors
        if self.error_threshold is not None:
            object.__setattr__(self, "max_errors", self.error_threshold)

        self._bound_timestamps()

    def _bound_timestamps(self) -> None:
        """Rebuild the timestamps deque bounded to ``max_timestamps`` (prevents memory growth)."""
        # Create bounded deque if not already bounded
        if (
            not hasattr(self._timestamps, "maxlen")
            or self._timestamps.maxlen != self.max_timestamps
        ):
            self._timestamps = deque(self._timestamps, maxlen=self.max_timestamps)

    def _should_open_circuit(self, error_count: int) -> bool:
        if error_count >= self.max_errors:
            return True
        if self.error_rate_threshold <= 0 or self.window_seconds <= 0:
            return False
        return (error_count / self.window_seconds) >= self.error_rate_threshold

    def record_error(self) -> None:
        """Record an error occurrence."""
        now = _DEFAULT_CLOCK.monotonic()
        self._timestamps.append(now)  # Deque maxlen handles overflow automatically
        self._prune_old_entries(now)
        error_count = len(self._timestamps)
        should_open = self._should_open_circuit(error_count)
        current_state = self.circuit_state

        # Check if we should open the circuit
        if current_state == CircuitState.CLOSED:
            if should_open:
                self._circuit_state = CircuitState.OPEN
                self._circuit_opened_at = now
        elif current_state == CircuitState.HALF_OPEN:
            # Error during half-open state means recovery failed - reopen circuit
            self._circuit_state = CircuitState.OPEN
            self._circuit_opened_at = now
        elif current_state == CircuitState.OPEN:
            self._circuit_opened_at = now

    def record_failure(self) -> None:
        """Record a failure occurrence (alias for record_error)."""
        self.record_error()

    def is_open(self) -> bool:
        """Check if circuit is open (rejecting requests). Method form for compatibility."""
        return self.circuit_state == CircuitState.OPEN

    def record_success(self) -> None:
        """
        Record a successful operation.

        In HALF_OPEN state: Closes the circuit and clears error history.
        In OPEN state: No effect (must wait for half_open_after timeout first).
        In CLOSED state: Prunes old timestamps, helping prevent false opens.

        IMPORTANT: When closing from HALF_OPEN, we clear the timestamps deque.
        Without this, the circuit would immediately re-open on the next error
        because old errors would still be counted in the window.
        """
        current_state = self.circuit_state
        if current_state == CircuitState.HALF_OPEN:
            self._circuit_state = CircuitState.CLOSED
            self._circuit_opened_at = None
            # CRITICAL: Clear error history to allow real recovery
            # Without this, circuit immediately re-opens on next error
            self._timestamps.clear()
        elif current_state == CircuitState.CLOSED:
            # Prune old entries to keep window current
            self._prune_old_entries(_DEFAULT_CLOCK.monotonic())

    def _prune_old_entries(self, now: float) -> None:
        """Remove entries outside the window."""
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    @property
    def error_count(self) -> int:
        """Number of errors in current window."""
        self._prune_old_entries(_DEFAULT_CLOCK.monotonic())
        return len(self._timestamps)

    @property
    def error_rate(self) -> float:
        """Errors per second in the window."""
        count = self.error_count
        if count == 0:
            return 0.0
        return count / self.window_seconds

    @property
    def circuit_state(self) -> CircuitState:
        """Get current circuit state, transitioning to half-open if appropriate."""
        now = _DEFAULT_CLOCK.monotonic()
        if self._circuit_state == CircuitState.OPEN:
            if self._circuit_opened_at is None:
                self._circuit_opened_at = now
            else:
                elapsed = now - self._circuit_opened_at
                if elapsed >= self.half_open_after:
                    self._prune_old_entries(now)
                    self._circuit_state = CircuitState.HALF_OPEN
                    self._circuit_opened_at = None
        return self._circuit_state

    @property
    def is_circuit_open(self) -> bool:
        """Check if circuit is open (rejecting requests)."""
        return self.circuit_state == CircuitState.OPEN

    def reset(self) -> None:
        """Reset error stats and close circuit."""
        self._timestamps.clear()
        self._circuit_state = CircuitState.CLOSED
        self._circuit_opened_at = None
