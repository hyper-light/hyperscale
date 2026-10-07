"""
The gate's placement policy: AD-36 routing under a job's constraints (D-62).
"""

from functools import partial
from operator import attrgetter

from hyperscale.distributed.discovery.selection.rendezvous_hash import (
    WeightedRendezvousHash,
)

from .candidate_filter import CandidateFilter
from .datacenter_candidate import DatacenterCandidate
from .datacenter_latency_estimator import DatacenterLatencyEstimator
from .datacenter_routing_score import DatacenterRoutingScore
from .dispatch_latency_budget import DispatchLatencyBudget
from .models import PlacementPlan
from .models import PlacementRequest
from .routing_scorer import RoutingScorer


HEALTH_BUCKET_RANK: dict[str, int] = {"HEALTHY": 0, "BUSY": 1, "DEGRADED": 2}


class ConstrainedPlacementPolicy:
    """
    AD-36 routing under a job's explicit constraints.

    1. The hard excludes drop UNHEALTHY (a datacenter whose storage cannot
       write is UNHEALTHY), initializing, managerless and all-circuits-open
       datacenters (``CandidateFilter``).
    2. Region affinity: the job's ``datacenters`` list narrows the eligible
       set, and the datacenters it already occupies leave it; an affinity
       nothing satisfies places nowhere.
    3. Latency budget: the job's dispatch latency budget narrows the set to
       the datacenters within it, or -- when fewer than the job asks for
       are -- is relaxed to order by excess (``DispatchLatencyBudget``).
    4. Each eligible datacenter is scored: estimated latency times load,
       health severity and SLO factors (``RoutingScorer``).
    5. The order: datacenters cooling down from a failed dispatch of this
       job last; then by excess over the budget; by health bucket
       (HEALTHY, BUSY, DEGRADED -- AD-17's order, never traded for
       latency); by score; ties broken by rendezvous hash on the job id,
       so equal datacenters share jobs evenly and every routing of one job
       agrees.

    Minimum capacity is not a constraint here: a datacenter's room for a
    job is decided where its work is counted, at its leader's admission
    (D-65), and a refusal for want of room sends the job to the next
    datacenter. AD-43 spillover moves a primary's share only to a fallback
    no further over the budget than the primary.
    """

    __slots__ = ("_candidate_filter", "_latency_budget", "_latency_estimator", "_scorer")

    def __init__(self, latency_estimator: DatacenterLatencyEstimator, scorer: RoutingScorer) -> None:
        self._latency_estimator = latency_estimator
        self._scorer = scorer
        self._candidate_filter = CandidateFilter()
        self._latency_budget = DispatchLatencyBudget()

    def place(self, request: PlacementRequest, candidates: list[DatacenterCandidate]) -> PlacementPlan:
        """Every datacenter ``request`` may be placed in, best first."""
        latencies_ms = self._latency_estimator.estimate(map(attrgetter("datacenter_id"), candidates))
        eligible, exclusions = self._candidate_filter.partition(candidates)
        eligible, budget_exclusions, budget_relaxed = self._latency_budget.narrow(
            self._within_affinity(eligible, request),
            request,
        )
        exclusions.update(budget_exclusions)
        scores = self._score_candidates(eligible, latencies_ms)
        return PlacementPlan(
            ordered=sorted(eligible, key=self._preference_key(request, scores)),
            scores=scores,
            exclusions=exclusions,
            latency_budget_relaxed=budget_relaxed,
        )

    def spillover_candidates(
        self,
        request: PlacementRequest,
        primary_datacenter: str,
        fallback_datacenters: list[str],
        candidates: list[DatacenterCandidate],
    ) -> list[str]:
        """The fallbacks no further over the job's budget than its primary."""
        return self._latency_budget.within_reach(request, primary_datacenter, fallback_datacenters, candidates)

    def _preference_key(
        self,
        request: PlacementRequest,
        scores: dict[str, DatacenterRoutingScore],
    ):
        """The sort key of step 5 for ``request``'s eligible datacenters."""
        rendezvous_rank = self._rendezvous_rank(request.job_id, scores)
        budget_ms = request.dispatch_latency_budget_ms
        excess_ms = self._latency_budget.excess_ms
        return lambda candidate: (
            candidate.datacenter_id in request.cooling_datacenters,
            excess_ms(candidate, budget_ms),
            HEALTH_BUCKET_RANK[candidate.health_bucket],
            scores[candidate.datacenter_id].final_score,
            rendezvous_rank[candidate.datacenter_id],
        )

    @staticmethod
    def _within_affinity(
        eligible: list[DatacenterCandidate],
        request: PlacementRequest,
    ) -> list[DatacenterCandidate]:
        """Narrow ``eligible`` to the job's affinity and away from the
        datacenters it occupies, when either is given (step 2)."""
        if request.affinity or request.occupied_datacenters:
            return list(filter(partial(ConstrainedPlacementPolicy._is_placeable, request=request), eligible))
        return eligible

    @staticmethod
    def _is_placeable(candidate: DatacenterCandidate, request: PlacementRequest) -> bool:
        """Whether ``candidate`` satisfies the job's affinity (if any) and is
        not already occupied by the job."""
        return (
            not request.affinity or candidate.datacenter_id in request.affinity
        ) and candidate.datacenter_id not in request.occupied_datacenters

    def _score_candidates(
        self,
        eligible: list[DatacenterCandidate],
        latencies_ms: dict[str, float],
    ) -> dict[str, DatacenterRoutingScore]:
        """Each eligible datacenter's step 4 score, keyed by id."""
        return {
            candidate.datacenter_id: self._scorer.score_datacenter(
                candidate,
                latencies_ms[candidate.datacenter_id],
            )
            for candidate in eligible
        }

    @staticmethod
    def _rendezvous_rank(job_id: str, scores: dict[str, DatacenterRoutingScore]) -> dict[str, int]:
        """Each scored datacenter's rendezvous-hash rank for ``job_id``: the
        tie-breaker that spreads equal datacenters' jobs evenly (step 5)."""
        tie_breaker = WeightedRendezvousHash()
        for datacenter_id in scores:
            tie_breaker.add_peer(datacenter_id)
        return {
            datacenter_id: rank
            for rank, datacenter_id in enumerate(
                tie_breaker.select_n(job_id, len(scores))
            )
        }
