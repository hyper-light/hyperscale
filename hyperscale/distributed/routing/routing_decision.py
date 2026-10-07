"""
The outcome of routing one job (AD-36 Part 9).
"""

from dataclasses import dataclass

from .datacenter_routing_score import DatacenterRoutingScore
from .exclusion_reason import ExclusionReason


@dataclass(slots=True, frozen=True)
class RoutingDecision:
    """
    Where a job goes: ``primary_datacenters`` are the datacenters it runs
    in (as many as it asked for, when that many are eligible), and
    ``fallback_datacenters`` the rest of the eligible ones in the order
    they would replace a primary. ``worst_primary_health_bucket`` is the
    lowest health bucket among the primaries (None without primaries).
    ``scores`` holds every eligible datacenter's score, ``exclusions`` the
    reason each ineligible one was left out, and ``cooling_datacenters``
    those demoted for having recently failed this job's dispatch.
    ``latency_budget_relaxed`` is True when fewer datacenters than the job
    asked for met its dispatch latency budget, so it was placed nearest to
    it (D-62).
    """

    job_id: str
    primary_datacenters: list[str]
    fallback_datacenters: list[str]
    worst_primary_health_bucket: str | None
    scores: dict[str, DatacenterRoutingScore]
    exclusions: dict[str, ExclusionReason]
    cooling_datacenters: frozenset[str]
    latency_budget_relaxed: bool = False
