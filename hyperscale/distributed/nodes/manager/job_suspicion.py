"""``JobSuspicion`` -- pickled under the namespace
``hyperscale.distributed.nodes.manager.health`` (see that module)."""

from .health_shared import _DEFAULT_CLOCK


class JobSuspicion:
    """
    Tracks job-specific suspicion state for AD-30.

    Per (job_id, worker_id) suspicion with confirmation tracking.
    """

    __slots__ = (
        "job_id",
        "worker_id",
        "started_at",
        "confirmation_count",
        "last_confirmation_at",
        "timeout_seconds",
    )

    def __init__(
        self,
        job_id: str,
        worker_id: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.job_id = job_id
        self.worker_id = worker_id
        self.started_at = _DEFAULT_CLOCK.monotonic()
        self.confirmation_count = 0
        self.last_confirmation_at = self.started_at
        self.timeout_seconds = timeout_seconds

    def add_confirmation(self) -> None:
        """Add a confirmation (does NOT reschedule timer per AD-30)."""
        self.confirmation_count += 1
        self.last_confirmation_at = _DEFAULT_CLOCK.monotonic()

    def time_remaining(self, cluster_size: int) -> float:
        """
        Calculate time remaining before expiration.

        Per Lifeguard, timeout shrinks with confirmations.

        Args:
            cluster_size: Number of nodes in cluster

        Returns:
            Seconds until expiration
        """
        # Timeout shrinks with confirmations (Lifeguard formula)
        # More confirmations = shorter timeout = faster failure declaration
        shrink_factor = max(1, 1 + self.confirmation_count)
        effective_timeout = self.timeout_seconds / shrink_factor

        elapsed = _DEFAULT_CLOCK.monotonic() - self.started_at
        return max(0, effective_timeout - elapsed)

    def is_expired(self, cluster_size: int) -> bool:
        """Check if suspicion has expired."""
        return self.time_remaining(cluster_size) <= 0
