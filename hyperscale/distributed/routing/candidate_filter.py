"""
Candidate filtering for datacenter selection (AD-36 Part 2).
"""

from collections.abc import Callable

from .datacenter_candidate import DatacenterCandidate
from .exclusion_reason import ExclusionReason


class CandidateFilter:
    """
    Applies AD-36's hard excludes to datacenter candidates.

    A datacenter is excluded when it is UNHEALTHY, has not reported yet
    (INITIALIZING), has no managers to dispatch to, or every one of its
    managers' circuits is open. Everything else -- HEALTHY, BUSY and
    DEGRADED -- stays eligible; a missing or immature coordinate never
    excludes (the latency estimate turns conservative instead).
    """

    # The hard excludes in the order they are checked: the first that
    # holds is the reason a datacenter is left out.
    _EXCLUSION_CHECKS: tuple[tuple[Callable[[DatacenterCandidate], bool], ExclusionReason], ...] = (
        (lambda candidate: candidate.health_bucket == "UNHEALTHY", ExclusionReason.UNHEALTHY_STATUS),
        (lambda candidate: candidate.health_bucket == "INITIALIZING", ExclusionReason.INITIALIZING),
        (lambda candidate: candidate.total_managers == 0, ExclusionReason.NO_REGISTERED_MANAGERS),
        (lambda candidate: candidate.healthy_managers == 0, ExclusionReason.ALL_MANAGERS_CIRCUIT_OPEN),
    )

    def partition(
        self,
        candidates: list[DatacenterCandidate],
    ) -> tuple[list[DatacenterCandidate], dict[str, ExclusionReason]]:
        """Split ``candidates`` into the eligible ones and the reason each
        excluded datacenter was left out."""
        eligible: list[DatacenterCandidate] = []
        exclusions: dict[str, ExclusionReason] = {}
        for candidate in candidates:
            if (exclusion_reason := self._exclusion_reason(candidate)) is None:
                eligible.append(candidate)
            else:
                exclusions[candidate.datacenter_id] = exclusion_reason

        return eligible, exclusions

    @staticmethod
    def _exclusion_reason(candidate: DatacenterCandidate) -> ExclusionReason | None:
        """The first AD-36 hard exclude ``candidate`` hits, checked in
        order, or None when it stays eligible."""
        return next(
            (
                exclusion_reason
                for is_excluded, exclusion_reason in CandidateFilter._EXCLUSION_CHECKS
                if is_excluded(candidate)
            ),
            None,
        )
