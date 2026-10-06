"""
Candidate filtering for datacenter selection (AD-36 Part 2).
"""

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

    def partition(
        self,
        candidates: list[DatacenterCandidate],
    ) -> tuple[list[DatacenterCandidate], dict[str, ExclusionReason]]:
        """Split ``candidates`` into the eligible ones and the reason each
        excluded datacenter was left out."""
        eligible: list[DatacenterCandidate] = []
        exclusions: dict[str, ExclusionReason] = {}
        for candidate in candidates:
            if candidate.health_bucket == "UNHEALTHY":
                exclusions[candidate.datacenter_id] = ExclusionReason.UNHEALTHY_STATUS
            elif candidate.health_bucket == "INITIALIZING":
                exclusions[candidate.datacenter_id] = ExclusionReason.INITIALIZING
            elif candidate.total_managers == 0:
                exclusions[candidate.datacenter_id] = ExclusionReason.NO_REGISTERED_MANAGERS
            elif candidate.healthy_managers == 0:
                exclusions[candidate.datacenter_id] = ExclusionReason.ALL_MANAGERS_CIRCUIT_OPEN
            else:
                eligible.append(candidate)

        return eligible, exclusions
