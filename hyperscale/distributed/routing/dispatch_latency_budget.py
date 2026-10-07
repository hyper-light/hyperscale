"""
D-62 latency budget: a job's bound on its datacenters' dispatch latency.
"""

from .datacenter_candidate import DatacenterCandidate
from .exclusion_reason import ExclusionReason
from .models import PlacementRequest


class DispatchLatencyBudget:
    """
    A job's ``dispatch_latency_budget_ms`` against each datacenter's p95
    workflow dispatch round trip (the D-5 per-datacenter digest).

    A datacenter is over budget by its p95's excess over the budget. One
    whose digest has too few samples to grade (``DatacenterCandidate.
    dispatch_latency_p95_ms`` None) is over by nothing: no evidence holds
    against it, as AD-42 does not grade it either.

    The budget narrows placement to the datacenters within it while they
    are as many as the job asks for; the rest are excluded. When fewer
    meet it, the policy falls back: every eligible datacenter stays, in
    order of excess -- those within the budget first, then the nearest
    to it -- and the placement is marked relaxed. A job is never refused
    for its budget: the datacenters' latency moves with their load, and a
    refusal would leave the job nowhere to run while the nearest
    datacenter is still the best place for it.
    """

    __slots__ = ()

    @staticmethod
    def excess_ms(candidate: DatacenterCandidate, budget_ms: float) -> float:
        """How far ``candidate``'s p95 dispatch latency exceeds ``budget_ms``
        (0 within it, without evidence, or with no budget set)."""
        if budget_ms <= 0.0 or candidate.dispatch_latency_p95_ms is None:
            return 0.0
        return max(0.0, candidate.dispatch_latency_p95_ms - budget_ms)

    def narrow(
        self,
        eligible: list[DatacenterCandidate],
        request: PlacementRequest,
    ) -> tuple[list[DatacenterCandidate], dict[str, ExclusionReason], bool]:
        """The datacenters ``request`` may be placed in under its budget,
        those excluded for it, and whether the budget was relaxed."""
        budget_ms = request.dispatch_latency_budget_ms
        within = self._within_budget(eligible, budget_ms)
        if len(within) >= request.datacenter_count:
            return within, self._over_budget_exclusions(eligible, budget_ms), False
        return eligible, {}, len(within) < len(eligible)

    def _within_budget(self, eligible: list[DatacenterCandidate], budget_ms: float) -> list[DatacenterCandidate]:
        """The eligible datacenters over ``budget_ms`` by nothing."""
        return [candidate for candidate in eligible if self.excess_ms(candidate, budget_ms) == 0.0]

    def _over_budget_exclusions(
        self,
        eligible: list[DatacenterCandidate],
        budget_ms: float,
    ) -> dict[str, ExclusionReason]:
        """Each eligible datacenter over ``budget_ms``, excluded for it."""
        return {
            candidate.datacenter_id: ExclusionReason.OVER_DISPATCH_LATENCY_BUDGET
            for candidate in eligible
            if self.excess_ms(candidate, budget_ms) > 0.0
        }

    def within_reach(
        self,
        request: PlacementRequest,
        primary_datacenter: str,
        fallback_datacenters: list[str],
        candidates: list[DatacenterCandidate],
    ) -> list[str]:
        """The fallbacks no further over ``request``'s budget than
        ``primary_datacenter``: a spillover never trades the budget for
        capacity. A datacenter the candidates do not show is over by
        nothing."""
        excess_by_datacenter = self._excess_by_datacenter(candidates, request.dispatch_latency_budget_ms)
        primary_excess_ms = excess_by_datacenter.get(primary_datacenter, 0.0)
        return [
            datacenter_id
            for datacenter_id in fallback_datacenters
            if excess_by_datacenter.get(datacenter_id, 0.0) <= primary_excess_ms
        ]

    def _excess_by_datacenter(self, candidates: list[DatacenterCandidate], budget_ms: float) -> dict[str, float]:
        """Each candidate's excess over ``budget_ms``, by datacenter id."""
        return {candidate.datacenter_id: self.excess_ms(candidate, budget_ms) for candidate in candidates}
