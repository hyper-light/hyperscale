"""
Gate job router: AD-36 routing through a pluggable placement policy (D-62).
"""

from collections.abc import Callable
from operator import attrgetter

from .constrained_placement_policy import HEALTH_BUCKET_RANK
from .datacenter_candidate import DatacenterCandidate
from .exclusion_reason import ExclusionReason
from .job_dispatch_cooldowns import JobDispatchCooldowns
from .placement_policy import PlacementPolicy
from .models import PlacementRequest
from .routing_decision import RoutingDecision


class GateJobRouter:
    """
    Routes a job to the datacenters it runs in (AD-36).

    Every known datacenter becomes a candidate; the placement policy
    (``ConstrainedPlacementPolicy`` on a gate) answers which of them the
    job may be placed in, best first. The first ``datacenter_count`` are
    the primaries -- a job asking for more datacenters than the best
    bucket holds fills from the next -- and the rest are its fallbacks, in
    order. The router keeps what outlives one decision: the datacenters
    cooling down from a failed dispatch of each job, and the AD-36 Part 11
    counters.
    """

    def __init__(
        self,
        get_datacenter_candidates: Callable[[], list[DatacenterCandidate]],
        placement_policy: PlacementPolicy,
        dispatch_cooldowns: JobDispatchCooldowns,
    ) -> None:
        self._get_datacenter_candidates = get_datacenter_candidates
        self._placement_policy = placement_policy
        self._dispatch_cooldowns = dispatch_cooldowns
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
        dispatch_latency_budget_ms: float = 0.0,
    ) -> RoutingDecision:
        """
        Route ``job_id`` to ``datacenter_count`` datacenters, within
        ``placement_constraint`` when one is given, outside
        ``occupied_datacenters`` -- those already running, or already lost
        by, the job when a lost datacenter's work is placed anew -- and
        under ``dispatch_latency_budget_ms`` when one is set (D-62).
        """
        request = self._placement_request(
            job_id, datacenter_count, placement_constraint, occupied_datacenters, dispatch_latency_budget_ms
        )
        plan = self._placement_policy.place(request, self._get_datacenter_candidates())
        primaries = plan.ordered[:datacenter_count]
        worst_primary_health_bucket = self._worst_health_bucket(primaries)
        self._count_decision(worst_primary_health_bucket, plan.exclusions)
        return RoutingDecision(
            job_id=job_id,
            primary_datacenters=list(map(attrgetter("datacenter_id"), primaries)),
            fallback_datacenters=list(map(attrgetter("datacenter_id"), plan.ordered[datacenter_count:])),
            worst_primary_health_bucket=worst_primary_health_bucket,
            scores=plan.scores,
            exclusions=plan.exclusions,
            cooling_datacenters=request.cooling_datacenters,
            latency_budget_relaxed=plan.latency_budget_relaxed,
        )

    def spillover_candidates(
        self,
        job_id: str,
        primary_datacenter: str,
        fallback_datacenters: list[str],
        dispatch_latency_budget_ms: float,
    ) -> list[str]:
        """The ``fallback_datacenters`` AD-43 spillover may move ``job_id``'s
        share in ``primary_datacenter`` to, as its placement policy allows."""
        request = self._placement_request(job_id, 1, None, frozenset(), dispatch_latency_budget_ms)
        return self._placement_policy.spillover_candidates(
            request,
            primary_datacenter,
            fallback_datacenters,
            self._get_datacenter_candidates(),
        )

    def _placement_request(
        self,
        job_id: str,
        datacenter_count: int,
        placement_constraint: set[str] | None,
        occupied_datacenters: frozenset[str],
        dispatch_latency_budget_ms: float,
    ) -> PlacementRequest:
        """What one routing of ``job_id`` asks of its placement, with the
        datacenters cooling down for it now."""
        return PlacementRequest(
            job_id=job_id,
            datacenter_count=datacenter_count,
            affinity=frozenset(placement_constraint) if placement_constraint else None,
            occupied_datacenters=occupied_datacenters,
            cooling_datacenters=self._dispatch_cooldowns.cooling_datacenters(job_id),
            dispatch_latency_budget_ms=dispatch_latency_budget_ms,
        )

    @staticmethod
    def _worst_health_bucket(primaries: list[DatacenterCandidate]) -> str | None:
        """The worst health bucket among the primaries, None without any."""
        return (
            max(
                map(attrgetter("health_bucket"), primaries),
                key=HEALTH_BUCKET_RANK.__getitem__,
            )
            if primaries
            else None
        )

    def _count_decision(
        self,
        worst_primary_health_bucket: str | None,
        exclusions: dict[str, ExclusionReason],
    ) -> None:
        """Count a decision by its worst primary bucket and its exclusions
        by reason (AD-36 Part 11)."""
        decision_bucket = worst_primary_health_bucket or "none"
        self._decisions_by_bucket[decision_bucket] = self._decisions_by_bucket.get(decision_bucket, 0) + 1
        for exclusion_reason in exclusions.values():
            self._exclusions_by_reason[exclusion_reason.value] = (
                self._exclusions_by_reason.get(exclusion_reason.value, 0) + 1
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
            **self._fallback_metrics(),
            "cooldowns": self._cooldowns_recorded,
        }

    def _fallback_metrics(self) -> dict[str, int]:
        """The ``fallback:<from>><to>`` counters of AD-36 Part 11."""
        return {
            f"fallback:{from_datacenter}>{to_datacenter}": count
            for (from_datacenter, to_datacenter), count in self._fallbacks_used.items()
        }

    def cleanup_job_state(self, job_id: str) -> None:
        """Forget a finished job's routing state."""
        self._dispatch_cooldowns.clear_job(job_id)
