"""``JobLease`` -- pickled under the namespace
``hyperscale.distributed.leases.job_lease`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass, field

from .job_lease_shared import _DEFAULT_CLOCK
from .lease_state import LeaseState


@dataclass(slots=True)
class JobLease:
    job_id: str
    owner_node: str
    fence_token: int
    created_at: float
    expires_at: float
    lease_duration: float = 30.0
    state: LeaseState = field(default=LeaseState.ACTIVE)

    def is_expired(self) -> bool:
        if self.state == LeaseState.RELEASED:
            return True
        return _DEFAULT_CLOCK.monotonic() >= self.expires_at

    def is_active(self) -> bool:
        return not self.is_expired() and self.state == LeaseState.ACTIVE

    def remaining_seconds(self) -> float:
        if self.is_expired():
            return 0.0
        return max(0.0, self.expires_at - _DEFAULT_CLOCK.monotonic())

    def extend(self, duration: float | None = None) -> None:
        if duration is None:
            duration = self.lease_duration
        now = _DEFAULT_CLOCK.monotonic()
        self.expires_at = now + duration

    def mark_released(self) -> None:
        """End the lease now: released, it expires at its release."""
        self.state = LeaseState.RELEASED
        self.expires_at = _DEFAULT_CLOCK.monotonic()
