"""
Per-worker workflow dispatch routing state.

This state is intentionally separate from SWIM membership and worker lifecycle
health. A failed workflow_dispatch TCP attempt is routing evidence for the
allocator, not a declaration that the worker is dead.
"""

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class WorkerDispatchRoutingState:
    """Tracks temporary dispatch routing cooldown for one worker."""

    worker_id: str
    base_cooldown_seconds: float
    max_cooldown_seconds: float
    consecutive_failures: int = 0
    suspended_until: float = 0.0
    last_failure_at: float = 0.0
    last_success_at: float = field(default_factory=time.monotonic)
    last_error: str = ""

    def is_routable(self, now: float | None = None) -> bool:
        """Return whether workflow dispatch should currently route to this worker."""
        current_time = time.monotonic() if now is None else now
        return current_time >= self.suspended_until

    def record_success(self, now: float | None = None) -> None:
        """Clear routing cooldown after a confirmed successful dispatch."""
        current_time = time.monotonic() if now is None else now
        self.consecutive_failures = 0
        self.suspended_until = 0.0
        self.last_success_at = current_time
        self.last_error = ""

    def record_failure(
        self,
        *,
        error: str = "",
        cooldown_seconds: float | None = None,
        now: float | None = None,
    ) -> None:
        """Apply bounded exponential routing cooldown after dispatch failure."""
        current_time = time.monotonic() if now is None else now
        self.consecutive_failures += 1
        self.last_failure_at = current_time
        self.last_error = error

        if cooldown_seconds is None:
            exponent = min(self.consecutive_failures - 1, 8)
            cooldown = self.base_cooldown_seconds * (2**exponent)
        else:
            cooldown = cooldown_seconds

        bounded_cooldown = min(
            self.max_cooldown_seconds,
            max(0.0, cooldown),
        )
        self.suspended_until = max(
            self.suspended_until,
            current_time + bounded_cooldown,
        )

    def remaining_cooldown_seconds(self, now: float | None = None) -> float:
        """Return remaining routing cooldown seconds."""
        current_time = time.monotonic() if now is None else now
        return max(0.0, self.suspended_until - current_time)
