"""
A placement policy's answer for one job (D-62).
"""

from dataclasses import dataclass

from ..datacenter_candidate import DatacenterCandidate
from ..datacenter_routing_score import DatacenterRoutingScore
from ..exclusion_reason import ExclusionReason


@dataclass(slots=True, frozen=True)
class PlacementPlan:
    """
    ``ordered`` holds every datacenter the job may be placed in, best
    first: the router takes the first ``datacenter_count`` as primaries
    and the rest as fallbacks. ``scores`` holds the AD-36 score of each
    one scored, ``exclusions`` why each left out was left out, and
    ``latency_budget_relaxed`` whether fewer datacenters than the job asked
    for met its dispatch latency budget, so it was placed nearest to it.
    """

    ordered: list[DatacenterCandidate]
    scores: dict[str, DatacenterRoutingScore]
    exclusions: dict[str, ExclusionReason]
    latency_budget_relaxed: bool
