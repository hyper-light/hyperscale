"""
TCP handler for job submission from clients.

Handles JobSubmission messages, performs validation, and dispatches jobs
to datacenter managers.

Dependencies:
- Rate limiter (AD-24)
- Load shedder (AD-22)
- Protocol version negotiation (AD-25)
- Quorum circuit breaker
- GateJobRouter (AD-36)
- Job manager, lease manager, leadership tracker

TODO: Extract from gate.py job_submission() method (lines 5012-5230)
"""

from typing import Protocol


class JobSubmissionDependencies(Protocol):
    """Protocol defining dependencies for job submission handler."""

    def check_rate_limit_for_operation(self, client_id: str, operation: str) -> tuple[bool, float]:
        """Check rate limit and return (allowed, retry_after)."""
        ...

    def should_shed_request(self, request_type: str) -> bool:
        """Check if request should be shed due to load."""
        ...

    def has_quorum_available(self) -> bool:
        """Check if quorum is available for multi-gate deployments."""
        ...

    def select_datacenters_with_fallback(
        self, count: int, explicit_dcs: list[str] | None, job_id: str
    ) -> tuple[list[str], list[str], str]:
        """Select primary and fallback datacenters. Returns (primary_dcs, fallback_dcs, worst_health)."""
        ...


# Placeholder for full handler implementation
# The handler will be extracted when the composition root is refactored

__all__ = ["JobSubmissionDependencies"]
