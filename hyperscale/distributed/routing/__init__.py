"""
Routing module for distributed job assignment (AD-36).

Provides:
- Vivaldi-based multi-factor routing (AD-36)
- Consistent hashing for deterministic job-to-node mapping
- Health bucket selection preserving AD-17 semantics
- Hysteresis and stickiness for routing stability
"""

from .bootstrap import BootstrapConfig, BootstrapModeManager
from .bucket_selector import BucketSelectionResult, BucketSelector
from .candidate_filter import (
    CandidateFilter,
    DatacenterCandidate,
    DemotionReason,
    ExclusionReason,
    ManagerCandidate,
)
from .consistent_hash import ConsistentHashRing
from .fallback_chain import FallbackChain, FallbackChainBuilder
from .gate_job_router import GateJobRouter, GateJobRouterConfig, RoutingDecision
from .hysteresis import HysteresisConfig, HysteresisManager, HysteresisResult
from .routing_state import (
    DatacenterRoutingScore,
    JobRoutingState,
    RoutingDecisionReason,
    RoutingStateManager,
)
from .scoring import RoutingScorer, ScoringConfig
from .observed_latency_state import ObservedLatencyState
from .observed_latency_tracker import ObservedLatencyTracker
from .blended_scoring_config import BlendedScoringConfig
from .datacenter_routing_score_extended import DatacenterRoutingScoreExtended
from .dispatch_time_tracker import DispatchTimeTracker
from .blended_latency_scorer import BlendedLatencyScorer

__all__ = [
    # Main router
    "GateJobRouter",
    "GateJobRouterConfig",
    "RoutingDecision",
    # Candidate models
    "DatacenterCandidate",
    "ManagerCandidate",
    # Filtering
    "CandidateFilter",
    "ExclusionReason",
    "DemotionReason",
    # Bucket selection
    "BucketSelector",
    "BucketSelectionResult",
    # Scoring
    "RoutingScorer",
    "ScoringConfig",
    "DatacenterRoutingScore",
    "BlendedScoringConfig",
    "DatacenterRoutingScoreExtended",
    "BlendedLatencyScorer",
    "ObservedLatencyState",
    "ObservedLatencyTracker",
    "DispatchTimeTracker",
    # Hysteresis
    "HysteresisManager",
    "HysteresisConfig",
    "HysteresisResult",
    # Bootstrap mode
    "BootstrapModeManager",
    "BootstrapConfig",
    # Fallback chain
    "FallbackChainBuilder",
    "FallbackChain",
    # State management
    "RoutingStateManager",
    "JobRoutingState",
    "RoutingDecisionReason",
    # Legacy consistent hashing
    "ConsistentHashRing",
]
