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
        all_reported = (self.dcs_completed | self.dcs_failed) == self.target_dcs
        if all_reported:
            success = len(self.dcs_completed) > 0
            return True, "all_dcs_reported", success

        if not self.enabled:
            return False, "waiting_for_all_dcs", False

        if len(self.dcs_completed) >= self.min_dcs:
            return (
                True,
                f"min_dcs_reached ({len(self.dcs_completed)}/{self.min_dcs})",
                True,
            )

        if now >= self.deadline:
            success = len(self.dcs_completed) > 0
            return (
                True,
                f"deadline_expired (completed: {len(self.dcs_completed)})",
                success,
            )

        return False, "waiting", False

    def get_completion_ratio(self) -> float:
        """Get ratio of completed DCs."""
        if not self.target_dcs:
            return 0.0
        return len(self.dcs_completed) / len(self.target_dcs)
