"""
Active dispatch tracking for capacity estimation (AD-43).
"""

from dataclasses import dataclass


@dataclass(slots=True)
class ActiveDispatch:
    """
    Tracks a workflow currently executing on a worker.
    """

    workflow_id: str
    job_id: str
    worker_id: str
    cores_allocated: int
    dispatched_at: float
    duration_seconds: float
    timeout_seconds: float

    def remaining_seconds(self, now: float) -> float:
        """
        Estimate remaining execution time.

        Args:
            now: Current monotonic time

        Returns:
            Remaining execution time in seconds
        """
        elapsed = now - self.dispatched_at
        remaining = self.duration_seconds - elapsed
        return max(0.0, remaining)

    def expected_completion(self) -> float:
        """
        Return expected completion timestamp (monotonic).

        Returns:
            Monotonic timestamp when dispatch should complete
        """
        return self.dispatched_at + self.duration_seconds
