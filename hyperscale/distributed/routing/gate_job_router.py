"""
Gate job router with Vivaldi-based multi-factor routing (AD-36).
"""

from collections.abc import Callable

from hyperscale.distributed.discovery.selection.rendezvous_hash import (
    WeightedRendezvousHash,
)

from .candidate_filter import CandidateFilter
from .datacenter_candidate import DatacenterCandidate
from .datacenter_latency_estimator import DatacenterLatencyEstimator
from .job_dispatch_cooldowns import JobDispatchCooldowns
from .routing_decision import RoutingDecision
from .routing_scorer import RoutingScorer


HEALTH_BUCKET_RANK: dict[str, int] = {"HEALTHY": 0, "BUSY": 1, "DEGRADED": 2}


class GateJobRouter:
    """
    Routes a job to the datacenters it runs in (AD-36).

    1. Every known datacenter becomes a candidate; the hard excludes drop
       UNHEALTHY, initializing, managerless and all-circuits-open ones.
    2. A placement constraint (the submission's ``datacenters`` list)
       narrows the eligible set; one nothing satisfies routes nowhere.
    3. Each eligible datacenter is scored: estimated latency times load,
       health severity and SLO factors (``RoutingScorer``).
    4. The eligible datacenters are ordered by health bucket (HEALTHY,
       BUSY, DEGRADED -- AD-17's order, never traded for latency), then by
       score, with ties broken by rendezvous hash on the job id so equal
       datacenters share jobs evenly and every routing of one job agrees.
       Datacenters cooling down from a failed dispatch of this job go
       last.
    5. The first ``datacenter_count`` are the primaries -- a job asking for
       more datacenters than the best bucket holds fills from the next --
       and the rest are its fallbacks, in order.
    """

    def __init__(
        self,
        get_datacenter_candidates: Callable[[], list[DatacenterCandidate]],
        latency_estimator: DatacenterLatencyEstimator,
        scorer: RoutingScorer,
        dispatch_cooldowns: JobDispatchCooldowns,
    ) -> None:
        self._get_datacenter_candidates = get_datacenter_candidates
        self._latency_estimator = latency_estimator
        self._scorer = scorer
        self._dispatch_cooldowns = dispatch_cooldowns
        self._candidate_filter = CandidateFilter()
        # AD-36 Part 11 counters, since start: decisions by the worst
        # health bucket among their primaries ("none" when nothing was
        # eligible), datacenters excluded by reason, fallbacks a dispatch
        # used (from, to), and dispatch failures that cooled a datacenter
        # for a job.
        self._decisions_by_bucket: dict[str, int] = {}
        self._exclusions_by_reason: dict[str, int] = {}
        self._fallbacks_used: dict[tuple[str, str], int] = {}
        self._cooldowns_recorded = 0

    def route_job(
        self,
        job_id: str,
        datacenter_count: int,
        placement_constraint: set[str] | None,
        occupied_datacenters: frozenset[str] = frozenset(),
    ) -> RoutingDecision:
        """
        Route ``job_id`` to ``datacenter_count`` datacenters, within
        ``placement_constraint`` when one is given and outside
        ``occupied_datacenters`` -- those already running, or already lost
        by, the job when a lost datacenter's work is placed anew.
        """
        candidates = self._get_datacenter_candidates()
        latencies_ms = self._latency_estimator.estimate(
            candidate.datacenter_id for candidate in candidates
        )
        eligible, exclusions = self._candidate_filter.partition(candidates)
        if placement_constraint or occupied_datacenters:
            eligible = [
                candidate
                for candidate in eligible
                if (
                    not placement_constraint
                    or candidate.datacenter_id in placement_constraint
                )
                and candidate.datacenter_id not in occupied_datacenters
            ]

        scores = {
            candidate.datacenter_id: self._scorer.score_datacenter(
                candidate,
                latencies_ms[candidate.datacenter_id],
            )
            for candidate in eligible
        }
        cooling_datacenters = self._dispatch_cooldowns.cooling_datacenters(job_id)
        tie_breaker = WeightedRendezvousHash()
        for datacenter_id in scores:
            tie_breaker.add_peer(datacenter_id)
        rendezvous_rank = {
            datacenter_id: rank
            for rank, datacenter_id in enumerate(
                tie_breaker.select_n(job_id, len(scores))
            )
        }
        ordered = sorted(
            eligible,
            key=lambda candidate: (
                candidate.datacenter_id in cooling_datacenters,
                HEALTH_BUCKET_RANK[candidate.health_bucket],
                scores[candidate.datacenter_id].final_score,
                rendezvous_rank[candidate.datacenter_id],
            ),
        )
        primaries = ordered[:datacenter_count]
        worst_primary_health_bucket = (
            max(
                (candidate.health_bucket for candidate in primaries),
                key=HEALTH_BUCKET_RANK.__getitem__,
            )
            if primaries
            else None
        )
        decision_bucket = worst_primary_health_bucket or "none"
        self._decisions_by_bucket[decision_bucket] = self._decisions_by_bucket.get(decision_bucket, 0) + 1
        for exclusion_reason in exclusions.values():
            self._exclusions_by_reason[exclusion_reason.value] = (
                self._exclusions_by_reason.get(exclusion_reason.value, 0) + 1
            )
        return RoutingDecision(
            job_id=job_id,
            primary_datacenters=[candidate.datacenter_id for candidate in primaries],
            fallback_datacenters=[
                candidate.datacenter_id for candidate in ordered[datacenter_count:]
            ],
            worst_primary_health_bucket=worst_primary_health_bucket,
            scores=scores,
            exclusions=exclusions,
            cooling_datacenters=cooling_datacenters,
        )

    def record_dispatch_failure(self, job_id: str, datacenter_id: str) -> None:
        """Demote ``datacenter_id`` for ``job_id`` after a failed dispatch."""
        self._dispatch_cooldowns.record_failure(job_id, datacenter_id)
        self._cooldowns_recorded += 1

    def record_fallback_used(self, from_datacenter: str, to_datacenter: str) -> None:
        """A dispatch that failed at ``from_datacenter`` landed on its
        fallback ``to_datacenter``."""
        route = (from_datacenter, to_datacenter)
        self._fallbacks_used[route] = self._fallbacks_used.get(route, 0) + 1

    def get_metrics(self) -> dict[str, int]:
        """The AD-36 Part 11 counters, as ``kind:label`` keys: ``decision:
        <bucket>``, ``exclusion:<reason>``, ``fallback:<from>><to>`` and
        ``cooldowns``."""
        return {
            **{f"decision:{bucket}": count for bucket, count in self._decisions_by_bucket.items()},
            **{f"exclusion:{reason}": count for reason, count in self._exclusions_by_reason.items()},
            **{
                f"fallback:{from_datacenter}>{to_datacenter}": count
                for (from_datacenter, to_datacenter), count in self._fallbacks_used.items()
            },
            "cooldowns": self._cooldowns_recorded,
        }

    def cleanup_job_state(self, job_id: str) -> None:
        """Forget a finished job's routing state."""
        self._dispatch_cooldowns.clear_job(job_id)
