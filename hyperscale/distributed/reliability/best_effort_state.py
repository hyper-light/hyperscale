"""
Best-effort completion state tracking (AD-44).
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class BestEffortState:
    """
    Tracks best-effort completion state for a job.

    Enforced at gate level since gates handle DC routing.
    """

    job_id: str
    enabled: bool
    min_dcs: int
    deadline: float
    target_dcs: set[str]
    dcs_completed: set[str] = field(default_factory=set)
    dcs_failed: set[str] = field(default_factory=set)
    # Under the ``update`` late-result policy: the job's result went out
    # provisionally on reaching ``min_dcs``; it now completes only when
    # every datacenter reported or the deadline passed.
    released: bool = False
    # The reason it was released for -- the reason its standing result
    # gives until it completes for good.
    release_reason: str = ""

    def record_dc_result(self, dc_id: str, success: bool) -> None:
        """Record result from a datacenter."""
        if success:
            self.dcs_completed.add(dc_id)
            self.dcs_failed.discard(dc_id)
            return

        self.dcs_failed.add(dc_id)
        self.dcs_completed.discard(dc_id)

    def check_completion(self, now: float) -> tuple[bool, str, bool]:
        """
        Check if job should complete.

        Returns:
            (should_complete, reason, is_success)
        """
        if self.all_reported():
            return True, "all_dcs_reported", len(self.dcs_completed) > 0

        if not self.enabled:
            return False, "waiting_for_all_dcs", False

        return self._check_partial_completion(now)

    def all_reported(self) -> bool:
        """Every target datacenter reported."""
        return (self.dcs_completed | self.dcs_failed) == self.target_dcs

    def awaits_stragglers(self, now: float) -> bool:
        """True while a result handed out now could still be updated: not
        yet released, some datacenter unreported, the deadline ahead."""
        return not self.released and now < self.deadline and not self.all_reported()

    def _check_partial_completion(self, now: float) -> tuple[bool, str, bool]:
        """Completion before every datacenter reported: ``min_dcs``
        completed (judged once -- not again after a provisional release),
        or the deadline passed."""
        if self._reaches_min_dcs():
            return (
                True,
                f"min_dcs_reached ({len(self.dcs_completed)}/{self.min_dcs})",
                True,
            )

        if now >= self.deadline:
            return (
                True,
                f"deadline_expired (completed: {len(self.dcs_completed)})",
                len(self.dcs_completed) > 0,
            )

        return self._waiting()

    def _waiting(self) -> tuple[bool, str, bool]:
        """No completion yet; a released job's standing result keeps the
        reason and success it was released with."""
        return (False, self.release_reason, True) if self.released else (False, "waiting", False)

    def mark_released(self, provisional: bool, reason: str) -> None:
        """Record a provisional release (``update`` policy) for ``reason``."""
        if provisional:
            self.released = True
            self.release_reason = reason

    def _reaches_min_dcs(self) -> bool:
        """``min_dcs`` completed and the job not already released on it."""
        return not self.released and len(self.dcs_completed) >= self.min_dcs

    def get_completion_ratio(self) -> float:
        """Get ratio of completed DCs."""
        if not self.target_dcs:
            return 0.0
        return len(self.dcs_completed) / len(self.target_dcs)
