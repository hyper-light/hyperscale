"""
Routing module for distributed job assignment (AD-36).

Provides:
- Vivaldi-based multi-factor datacenter routing (AD-36)
- Confidence-weighted latency estimation from Vivaldi and observed
  latency (AD-35, AD-45)
- Health bucket ordering preserving AD-17 semantics
- Per-job dispatch-failure cooldowns
- A pluggable placement policy with region affinity and a dispatch
  latency budget (D-62)
"""

from .blended_latency_scorer import BlendedLatencyScorer
from .blended_scoring_config import BlendedScoringConfig
from .candidate_filter import CandidateFilter
from .constrained_placement_policy import ConstrainedPlacementPolicy
from .datacenter_candidate import DatacenterCandidate
from .datacenter_latency_estimator import DatacenterLatencyEstimator
from .datacenter_routing_score import DatacenterRoutingScore
from .dispatch_latency_budget import DispatchLatencyBudget
from .exclusion_reason import ExclusionReason
from .gate_job_router import GateJobRouter
from .job_dispatch_cooldowns import JobDispatchCooldowns
from .observed_latency_state import ObservedLatencyState
from .observed_latency_tracker import ObservedLatencyTracker
from .models import PlacementPlan
from .placement_policy import PlacementPolicy
from .models import PlacementRequest
from .routing_decision import RoutingDecision
from .routing_scorer import RoutingScorer
from .scoring_config import ScoringConfig

__all__ = [
    "BlendedLatencyScorer",
    "BlendedScoringConfig",
    "CandidateFilter",
    "ConstrainedPlacementPolicy",
    "DatacenterCandidate",
    "DatacenterLatencyEstimator",
    "DatacenterRoutingScore",
    "DispatchLatencyBudget",
    "ExclusionReason",
    "GateJobRouter",
    "JobDispatchCooldowns",
    "ObservedLatencyState",
    "ObservedLatencyTracker",
    "PlacementPlan",
    "PlacementPolicy",
    "PlacementRequest",
    "RoutingDecision",
    "RoutingScorer",
    "ScoringConfig",
]
