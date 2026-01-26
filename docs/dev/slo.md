SLO-Aware Health Routing Architecture
Current State Analysis
What exists:

Vivaldi coordinates (AD-35): RTT estimation with UCB uncertainty
Multi-factor scoring (AD-36): score = rtt_ucb × load × quality × preference
Health buckets: HEALTHY > BUSY > DEGRADED > UNHEALTHY (capacity-based)
Percentile fields: AggregatedJobStats has p50/p95/p99, but for job results only
Latency tracking: Averages only, no percentiles for routing decisions
What's missing:

Streaming percentile computation for dispatch/response latencies
SLO definitions with latency targets (p95 < 200ms, p99 < 500ms)
SLO compliance tracking per datacenter/manager
SLO-aware routing factor in scoring function
End-to-end latency attribution (dispatch → response)
Most Robust Architecture Options
Option 1: T-Digest for Streaming Percentiles
Approach: Use the T-Digest algorithm for streaming, mergeable quantile estimation.


┌─────────────────────────────────────────────────────────────────────────┐
│                    T-DIGEST FOR STREAMING PERCENTILES                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  PROPERTIES:                                                             │
│  - Constant memory: O(δ) where δ controls accuracy (~100 centroids)    │
│  - Accuracy: ~0.1% at tails (p99, p99.9), ~1% at median                │
│  - Mergeable: Can combine digests from multiple nodes                   │
│  - Streaming: Update in O(1) amortized                                  │
│                                                                          │
│  WHY T-DIGEST:                                                           │
│  ┌──────────────────┬─────────────────┬─────────────────────────────┐   │
│  │ Alternative      │ Weakness        │ T-Digest Advantage          │   │
│  ├──────────────────┼─────────────────┼─────────────────────────────┤   │
│  │ HDR Histogram    │ Fixed range     │ Dynamic range, no binning   │   │
│  │ P² Algorithm     │ Single quantile │ All quantiles, mergeable    │   │
│  │ Sorted buffer    │ O(n) memory     │ O(δ) memory, bounded        │   │
│  │ Random sampling  │ Tail inaccuracy │ Tail-optimized compression  │   │
│  └──────────────────┴─────────────────┴─────────────────────────────┘   │
│                                                                          │
│  IMPLEMENTATION:                                                         │
│  - Pure Python with numpy for performance                               │
│  - Periodic merging from workers → managers → gates                     │
│  - TTL-based expiry for recency (last 5 minutes)                        │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
Option 2: Exponentially Decaying Histogram (DDSketch)
Approach: Use DDSketch for guaranteed relative-error quantiles.


┌─────────────────────────────────────────────────────────────────────────┐
│                    DDSketch FOR QUANTILE ESTIMATION                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  PROPERTIES:                                                             │
│  - Relative error guarantee: ε = 1% means p99 ± 1% of true value       │
│  - Memory: O(log(max/min) / log(1+ε)) buckets                          │
│  - Mergeable: Combine sketches by summing bucket counts                 │
│  - Collapse-resistant: Buckets never overflow                           │
│                                                                          │
│  ADVANTAGE OVER T-DIGEST:                                                │
│  - Simpler implementation                                                │
│  - Deterministic error bounds (vs empirical for T-Digest)               │
│  - Faster updates (bucket increment vs centroid search)                 │
│                                                                          │
│  DISADVANTAGE:                                                           │
│  - Slightly higher memory for same accuracy                             │
│  - Less accurate at exact median                                         │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
Option 3: Time-Decaying Circular Buffer with Approximate Percentiles
Approach: Simpler implementation using rotating time buckets with approximate percentiles.


┌─────────────────────────────────────────────────────────────────────────┐
│                    TIME-BUCKETED PERCENTILE TRACKER                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  DESIGN:                                                                 │
│  - N time buckets (e.g., 12 × 5-second = 1 minute window)              │
│  - Each bucket stores sorted samples (bounded, reservoir sampling)      │
│  - Query merges recent buckets and computes percentiles                 │
│  - Old buckets rotate out (implicit time decay)                         │
│                                                                          │
│  BUCKET STRUCTURE:                                                       │
│  ┌────────┬────────┬────────┬────────┬────────┬────────┐              │
│  │ t-55s  │ t-50s  │ t-45s  │ t-40s  │  ...   │ t-0s   │              │
│  │ 100    │ 100    │ 100    │ 100    │        │ 100    │ samples      │
│  └────────┴────────┴────────┴────────┴────────┴────────┘              │
│                                                                          │
│  PERCENTILE QUERY:                                                       │
│  1. Collect all samples from buckets in query window                    │
│  2. Sort merged samples (small N, fast)                                 │
│  3. Return interpolated percentiles                                      │
│                                                                          │
│  ADVANTAGES:                                                             │
│  - Very simple implementation                                            │
│  - Exact percentiles within sample set                                  │
│  - Natural time decay (old buckets expire)                              │
│  - Pure Python, no dependencies beyond stdlib                           │
│                                                                          │
│  DISADVANTAGES:                                                          │
│  - Higher memory than sketches                                           │
│  - Accuracy depends on sample count (reservoir bias at tail)            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
Recommended Architecture: Hybrid Approach
Given the constraints (pure Python, pip-installable, asyncio-compatible, robust), I recommend a hybrid approach:

T-Digest for accurate streaming percentiles (simple Python implementation using numpy)
Time-windowed aggregation for recency (only consider last N minutes)
Hierarchical merging (workers → managers → gates)
SLO scoring factor integrated into existing routing score

┌─────────────────────────────────────────────────────────────────────────┐
│                    SLO-AWARE ROUTING ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  LAYER 1: LATENCY COLLECTION (per worker/manager/gate)                  │
│  ─────────────────────────────────────────────────────                  │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ LatencyDigestTracker                                             │   │
│  │   - T-Digest per (datacenter, operation_type)                   │   │
│  │   - Operations: dispatch, response, e2e, network                │   │
│  │   - Windowed: reset digest every 5 minutes (or merge & decay)   │   │
│  │   - Query: p50, p95, p99 in O(log δ)                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  LAYER 2: SLO DEFINITION (per job or global)                            │
│  ─────────────────────────────────────────                              │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ LatencySLO                                                       │   │
│  │   p50_target_ms: 50.0    # Median target                        │   │
│  │   p95_target_ms: 200.0   # Tail target (most important)         │   │
│  │   p99_target_ms: 500.0   # Extreme tail target                  │   │
│  │   evaluation_window_seconds: 300.0  # 5-minute window           │   │
│  │   min_sample_count: 100  # Minimum for confidence               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  LAYER 3: SLO COMPLIANCE SCORING                                        │
│  ────────────────────────────────                                       │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ SLOComplianceScore (per datacenter)                              │   │
│  │                                                                  │   │
│  │   Inputs:                                                        │   │
│  │     observed_p50, observed_p95, observed_p99                    │   │
│  │     target_p50, target_p95, target_p99                          │   │
│  │     sample_count (for confidence)                               │   │
│  │                                                                  │   │
│  │   Score calculation:                                             │   │
│  │     ratio_p50 = observed_p50 / target_p50                       │   │
│  │     ratio_p95 = observed_p95 / target_p95                       │   │
│  │     ratio_p99 = observed_p99 / target_p99                       │   │
│  │                                                                  │   │
│  │     # Weighted by importance (p95 most critical for SLO)        │   │
│  │     slo_score = 0.2 * ratio_p50 + 0.5 * ratio_p95 + 0.3 * ratio_p99  │
│  │                                                                  │   │
│  │     # Confidence adjustment (fewer samples = higher score/penalty) │
│  │     confidence = min(1.0, sample_count / min_sample_count)      │   │
│  │     slo_score = slo_score * (2.0 - confidence)                  │   │
│  │                                                                  │   │
│  │   Interpretation:                                                │   │
│  │     < 1.0: Meeting SLO (bonus)                                  │   │
│  │     = 1.0: At SLO boundary                                      │   │
│  │     > 1.0: Violating SLO (penalty)                              │   │
│  │     > 2.0: Severely violating (major penalty)                   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  LAYER 4: ROUTING INTEGRATION (extend AD-36 scoring)                    │
│  ────────────────────────────────────────────────────                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Extended Scoring Formula:                                        │   │
│  │                                                                  │   │
│  │   OLD (AD-36):                                                   │   │
│  │     score = rtt_ucb × load_factor × quality_penalty × pref_mult │   │
│  │                                                                  │   │
│  │   NEW (with SLO):                                                │   │
│  │     score = rtt_ucb × load_factor × quality_penalty              │   │
│  │             × slo_factor × pref_mult                            │   │
│  │                                                                  │   │
│  │   Where:                                                         │   │
│  │     slo_factor = 1.0 + A_SLO × (slo_score - 1.0)               │   │
│  │                  capped to [0.5, 3.0]                            │   │
│  │     A_SLO = 0.4 (weight, configurable)                          │   │
│  │                                                                  │   │
│  │   Effect:                                                        │   │
│  │     SLO met (slo_score=0.8):  slo_factor = 0.92 (8% bonus)     │   │
│  │     SLO boundary (1.0):       slo_factor = 1.0 (neutral)        │   │
│  │     SLO violated (1.5):       slo_factor = 1.2 (20% penalty)    │   │
│  │     SLO severe (2.5):         slo_factor = 1.6 (60% penalty)    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
Complete Implementation
Part 1: T-Digest Implementation (Pure Python + NumPy)

"""
T-Digest implementation for streaming percentile estimation.

Based on the algorithm by Ted Dunning:
https://github.com/tdunning/t-digest

Key properties:
- Streaming: Update in O(log δ) amortized
- Accurate: ~0.1% error at tails (p99, p99.9)
- Mergeable: Combine digests from distributed nodes
- Bounded: O(δ) memory where δ ≈ 100 centroids
"""

from dataclasses import dataclass, field
from typing import Optional

import numpy as np


@dataclass(slots=True)
class Centroid:
    """A weighted centroid in the T-Digest."""
    mean: float
    weight: float


@dataclass
class TDigest:
    """
    T-Digest for streaming quantile estimation.
    
    Uses the scaling function k1 (which provides better accuracy at tails):
    k(q) = δ/2 * (arcsin(2q - 1) / π + 0.5)
    
    Attributes:
        delta: Compression parameter (higher = more accurate, more memory)
        max_unmerged: Maximum unmerged points before compression
    """
    
    delta: float = 100.0
    max_unmerged: int = 2048
    
    # Internal state
    _centroids: list[Centroid] = field(default_factory=list, init=False)
    _unmerged: list[float] = field(default_factory=list, init=False)
    _total_weight: float = field(default=0.0, init=False)
    _min: float = field(default=float('inf'), init=False)
    _max: float = field(default=float('-inf'), init=False)
    
    def add(self, value: float, weight: float = 1.0) -> None:
        """Add a value to the digest."""
        self._unmerged.append(value)
        self._total_weight += weight
        self._min = min(self._min, value)
        self._max = max(self._max, value)
        
        if len(self._unmerged) >= self.max_unmerged:
            self._compress()
    
    def add_batch(self, values: list[float]) -> None:
        """Add multiple values efficiently."""
        for v in values:
            self.add(v)
    
    def _compress(self) -> None:
        """Compress unmerged points into centroids."""
        if not self._unmerged:
            return
        
        # Combine existing centroids with unmerged points
        all_points: list[tuple[float, float]] = []
        for c in self._centroids:
            all_points.append((c.mean, c.weight))
        for v in self._unmerged:
            all_points.append((v, 1.0))
        
        # Sort by value
        all_points.sort(key=lambda x: x[0])
        
        # Rebuild centroids using clustering
        new_centroids: list[Centroid] = []
        
        if not all_points:
            self._centroids = new_centroids
            self._unmerged.clear()
            return
        
        # Start with first point
        current_mean = all_points[0][0]
        current_weight = all_points[0][1]
        cumulative_weight = current_weight
        
        for mean, weight in all_points[1:]:
            # Calculate the size limit for the current centroid
            q = cumulative_weight / self._total_weight if self._total_weight > 0 else 0.5
            limit = self._k_inverse(self._k(q) + 1.0) - q
            max_weight = self._total_weight * limit
            
            if current_weight + weight <= max_weight:
                # Merge into current centroid
                new_weight = current_weight + weight
                current_mean = (current_mean * current_weight + mean * weight) / new_weight
                current_weight = new_weight
            else:
                # Save current centroid and start new one
                new_centroids.append(Centroid(current_mean, current_weight))
                current_mean = mean
                current_weight = weight
            
            cumulative_weight += weight
        
        # Don't forget the last centroid
        new_centroids.append(Centroid(current_mean, current_weight))
        
        self._centroids = new_centroids
        self._unmerged.clear()
    
    def _k(self, q: float) -> float:
        """Scaling function k(q) = δ/2 * (arcsin(2q-1)/π + 0.5)"""
        return (self.delta / 2.0) * (np.arcsin(2.0 * q - 1.0) / np.pi + 0.5)
    
    def _k_inverse(self, k: float) -> float:
        """Inverse scaling function."""
        return 0.5 * (np.sin((k / (self.delta / 2.0) - 0.5) * np.pi) + 1.0)
    
    def quantile(self, q: float) -> float:
        """
        Get the value at quantile q (0 <= q <= 1).
        
        Returns interpolated value at the given quantile.
        """
        if q < 0.0 or q > 1.0:
            raise ValueError(f"Quantile must be in [0, 1], got {q}")
        
        self._compress()  # Ensure all points merged
        
        if not self._centroids:
            return 0.0
        
        if q == 0.0:
            return self._min
        if q == 1.0:
            return self._max
        
        target_weight = q * self._total_weight
        cumulative = 0.0
        
        for i, centroid in enumerate(self._centroids):
            if cumulative + centroid.weight >= target_weight:
                # Interpolate within or between centroids
                if i == 0:
                    # Interpolate between min and first centroid
                    weight_before = cumulative
                    weight_after = cumulative + centroid.weight / 2
                    if target_weight <= weight_after:
                        ratio = target_weight / max(weight_after, 1e-10)
                        return self._min + ratio * (centroid.mean - self._min)
                
                prev = self._centroids[i - 1] if i > 0 else None
                if prev is not None:
                    # Interpolate between previous and current centroid
                    mid_prev = cumulative - prev.weight / 2
                    mid_curr = cumulative + centroid.weight / 2
                    ratio = (target_weight - mid_prev) / max(mid_curr - mid_prev, 1e-10)
                    return prev.mean + ratio * (centroid.mean - prev.mean)
                
                return centroid.mean
            
            cumulative += centroid.weight
        
        return self._max
    
    def percentile(self, p: float) -> float:
        """Get value at percentile p (0 <= p <= 100)."""
        return self.quantile(p / 100.0)
    
    def p50(self) -> float:
        """Median."""
        return self.quantile(0.50)
    
    def p95(self) -> float:
        """95th percentile."""
        return self.quantile(0.95)
    
    def p99(self) -> float:
        """99th percentile."""
        return self.quantile(0.99)
    
    def mean(self) -> float:
        """Mean of all values."""
        self._compress()
        if self._total_weight == 0:
            return 0.0
        return sum(c.mean * c.weight for c in self._centroids) / self._total_weight
    
    def count(self) -> float:
        """Total weight (count if weights are 1)."""
        return self._total_weight
    
    def merge(self, other: "TDigest") -> "TDigest":
        """
        Merge another digest into this one.
        
        Used for aggregating digests from multiple nodes.
        """
        other._compress()
        for c in other._centroids:
            self._unmerged.extend([c.mean] * int(c.weight))
        
        self._total_weight += other._total_weight
        self._min = min(self._min, other._min)
        self._max = max(self._max, other._max)
        
        self._compress()
        return self
    
    def reset(self) -> None:
        """Clear the digest."""
        self._centroids.clear()
        self._unmerged.clear()
        self._total_weight = 0.0
        self._min = float('inf')
        self._max = float('-inf')
    
    def to_dict(self) -> dict:
        """Serialize for network transfer."""
        self._compress()
        return {
            "delta": self.delta,
            "centroids": [(c.mean, c.weight) for c in self._centroids],
            "total_weight": self._total_weight,
            "min": self._min if self._min != float('inf') else None,
            "max": self._max if self._max != float('-inf') else None,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "TDigest":
        """Deserialize from network transfer."""
        digest = cls(delta=data.get("delta", 100.0))
        digest._centroids = [
            Centroid(mean=m, weight=w)
            for m, w in data.get("centroids", [])
        ]
        digest._total_weight = data.get("total_weight", 0.0)
        digest._min = data.get("min") if data.get("min") is not None else float('inf')
        digest._max = data.get("max") if data.get("max") is not None else float('-inf')
        return digest
Part 2: Latency SLO Models

"""
SLO definitions and compliance scoring for latency-aware routing.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from time import monotonic
from typing import Optional


class SLOComplianceLevel(Enum):
    """SLO compliance classification."""
    EXCEEDING = auto()  # Well below targets (bonus)
    MEETING = auto()  # At or below targets
    WARNING = auto()  # Approaching targets (80-100%)
    VIOLATING = auto()  # Above targets (100-150%)
    CRITICAL = auto()  # Severely above targets (>150%)


@dataclass(frozen=True, slots=True)
class LatencySLO:
    """
    Latency SLO definition.
    
    Defines targets for p50, p95, p99 latencies.
    Can be defined globally, per-datacenter, or per-job.
    """
    
    p50_target_ms: float = 50.0  # Median target
    p95_target_ms: float = 200.0  # 95th percentile target (primary SLO)
    p99_target_ms: float = 500.0  # 99th percentile target (extreme tail)
    
    # Weights for composite score (must sum to 1.0)
    p50_weight: float = 0.2
    p95_weight: float = 0.5  # p95 is typically the SLO target
    p99_weight: float = 0.3
    
    # Minimum samples for confident scoring
    min_sample_count: int = 100
    
    # Evaluation window
    evaluation_window_seconds: float = 300.0  # 5 minutes
    
    def __post_init__(self) -> None:
        total_weight = self.p50_weight + self.p95_weight + self.p99_weight
        if abs(total_weight - 1.0) > 0.001:
            raise ValueError(f"Weights must sum to 1.0, got {total_weight}")


@dataclass(slots=True)
class LatencyObservation:
    """Observed latency percentiles for a target."""
    
    target_id: str  # datacenter_id, manager_id, etc.
    p50_ms: float
    p95_ms: float
    p99_ms: float
    mean_ms: float
    sample_count: int
    window_start: float  # Monotonic timestamp
    window_end: float
    
    def is_stale(self, max_age_seconds: float = 300.0) -> bool:
        return (monotonic() - self.window_end) > max_age_seconds


@dataclass(slots=True)
class SLOComplianceScore:
    """
    Computed SLO compliance for a target.
    
    Score interpretation:
    - < 0.8: Exceeding SLO (bonus in routing)
    - 0.8 - 1.0: Meeting SLO
    - 1.0 - 1.2: Warning (approaching violation)
    - 1.2 - 1.5: Violating (penalty in routing)
    - > 1.5: Critical (major penalty, consider exclusion)
    """
    
    target_id: str
    
    # Individual ratios (observed / target)
    p50_ratio: float
    p95_ratio: float
    p99_ratio: float
    
    # Composite score (weighted average of ratios)
    composite_score: float
    
    # Confidence (based on sample count)
    confidence: float  # 0.0 to 1.0
    
    # Classification
    compliance_level: SLOComplianceLevel
    
    # For routing: factor to apply to score
    # < 1.0 = bonus, > 1.0 = penalty
    routing_factor: float
    
    @classmethod
    def calculate(
        cls,
        target_id: str,
        observation: LatencyObservation,
        slo: LatencySLO,
    ) -> "SLOComplianceScore":
        """Calculate compliance score from observation."""
        
        # Calculate ratios
        p50_ratio = observation.p50_ms / slo.p50_target_ms
        p95_ratio = observation.p95_ms / slo.p95_target_ms
        p99_ratio = observation.p99_ms / slo.p99_target_ms
        
        # Weighted composite
        composite = (
            slo.p50_weight * p50_ratio +
            slo.p95_weight * p95_ratio +
            slo.p99_weight * p99_ratio
        )
        
        # Confidence based on sample count
        confidence = min(1.0, observation.sample_count / slo.min_sample_count)
        
        # Adjust composite for low confidence (assume worst case)
        if confidence < 1.0:
            # With low confidence, inflate score towards 1.0 (neutral)
            # If we're doing well (composite < 1.0), reduce the bonus
            # If we're doing poorly (composite > 1.0), don't hide it
            composite = composite * confidence + 1.0 * (1.0 - confidence)
        
        # Classification
        if composite < 0.8:
            level = SLOComplianceLevel.EXCEEDING
        elif composite < 1.0:
            level = SLOComplianceLevel.MEETING
        elif composite < 1.2:
            level = SLOComplianceLevel.WARNING
        elif composite < 1.5:
            level = SLOComplianceLevel.VIOLATING
        else:
            level = SLOComplianceLevel.CRITICAL
        
        # Routing factor: adjust score based on compliance
        # Meeting SLO (composite ≈ 1.0) → factor = 1.0 (neutral)
        # Below SLO (composite < 1.0) → factor < 1.0 (bonus)
        # Above SLO (composite > 1.0) → factor > 1.0 (penalty)
        # Capped to [0.5, 3.0] to prevent extreme swings
        a_slo = 0.4  # Weight for SLO factor
        routing_factor = 1.0 + a_slo * (composite - 1.0)
        routing_factor = max(0.5, min(3.0, routing_factor))
        
        return cls(
            target_id=target_id,
            p50_ratio=p50_ratio,
            p95_ratio=p95_ratio,
            p99_ratio=p99_ratio,
            composite_score=composite,
            confidence=confidence,
            compliance_level=level,
            routing_factor=routing_factor,
        )
Part 3: Latency Digest Tracker

"""
Time-windowed latency tracking with T-Digest for percentile estimation.
"""

import asyncio
from dataclasses import dataclass, field
from time import monotonic
from typing import Optional

from hyperscale.distributed.resources.slo.tdigest import TDigest
from hyperscale.distributed.resources.slo.slo_models import (
    LatencyObservation,
    LatencySLO,
    SLOComplianceScore,
)


class LatencyType:
    """Types of latency we track."""
    DISPATCH = "dispatch"  # Time to dispatch job to manager
    RESPONSE = "response"  # Time for manager to respond
    E2E = "e2e"  # End-to-end job latency
    NETWORK = "network"  # Pure network RTT (from Vivaldi probes)


@dataclass(slots=True)
class LatencyWindow:
    """A time window with its T-Digest."""
    window_start: float  # Monotonic timestamp
    window_end: float
    digest: TDigest
    sample_count: int = 0


@dataclass
class LatencyDigestTracker:
    """
    Tracks latency percentiles per target using T-Digest.
    
    Maintains rolling windows of latency data with automatic expiry.
    Provides SLO compliance scoring for routing decisions.
    """
    
    # Configuration
    window_duration_seconds: float = 60.0  # Each window covers 1 minute
    max_windows: int = 5  # Keep 5 windows (5 minutes of history)
    tdigest_delta: float = 100.0  # T-Digest compression parameter
    
    # Per-target, per-latency-type windows
    _windows: dict[tuple[str, str], list[LatencyWindow]] = field(
        default_factory=dict, init=False
    )
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    
    async def record_latency(
        self,
        target_id: str,
        latency_type: str,
        latency_ms: float,
    ) -> None:
        """Record a latency observation."""
        now = monotonic()
        key = (target_id, latency_type)
        
        async with self._lock:
            if key not in self._windows:
                self._windows[key] = []
            
            windows = self._windows[key]
            
            # Get or create current window
            current_window = self._get_current_window(windows, now)
            if current_window is None:
                current_window = LatencyWindow(
                    window_start=now,
                    window_end=now + self.window_duration_seconds,
                    digest=TDigest(delta=self.tdigest_delta),
                )
                windows.append(current_window)
            
            # Add sample
            current_window.digest.add(latency_ms)
            current_window.sample_count += 1
            
            # Cleanup old windows
            self._cleanup_windows(windows, now)
    
    def _get_current_window(
        self,
        windows: list[LatencyWindow],
        now: float,
    ) -> Optional[LatencyWindow]:
        """Get the current active window."""
        for window in reversed(windows):
            if window.window_start <= now < window.window_end:
                return window
        return None
    
    def _cleanup_windows(
        self,
        windows: list[LatencyWindow],
        now: float,
    ) -> None:
        """Remove expired windows."""
        max_age = self.window_duration_seconds * self.max_windows
        cutoff = now - max_age
        
        while windows and windows[0].window_end < cutoff:
            windows.pop(0)
    
    async def get_observation(
        self,
        target_id: str,
        latency_type: str,
        window_seconds: float = 300.0,
    ) -> Optional[LatencyObservation]:
        """
        Get aggregated latency observation for a target.
        
        Merges all windows within the specified time range.
        """
        now = monotonic()
        key = (target_id, latency_type)
        
        async with self._lock:
            if key not in self._windows:
                return None
            
            windows = self._windows[key]
            cutoff = now - window_seconds
            
            # Merge digests from relevant windows
            merged = TDigest(delta=self.tdigest_delta)
            total_samples = 0
            earliest_start = now
            latest_end = 0.0
            
            for window in windows:
                if window.window_end >= cutoff:
                    merged.merge(window.digest)
                    total_samples += window.sample_count
                    earliest_start = min(earliest_start, window.window_start)
                    latest_end = max(latest_end, window.window_end)
            
            if total_samples == 0:
                return None
            
            return LatencyObservation(
                target_id=target_id,
                p50_ms=merged.p50(),
                p95_ms=merged.p95(),
                p99_ms=merged.p99(),
                mean_ms=merged.mean(),
                sample_count=total_samples,
                window_start=earliest_start,
                window_end=latest_end,
            )
    
    async def get_compliance_score(
        self,
        target_id: str,
        latency_type: str,
        slo: LatencySLO,
    ) -> Optional[SLOComplianceScore]:
        """Get SLO compliance score for a target."""
        observation = await self.get_observation(
            target_id,
            latency_type,
            slo.evaluation_window_seconds,
        )
        
        if observation is None:
            return None
        
        return SLOComplianceScore.calculate(
            target_id=target_id,
            observation=observation,
            slo=slo,
        )
    
    async def get_all_observations(
        self,
        latency_type: str,
        window_seconds: float = 300.0,
    ) -> dict[str, LatencyObservation]:
        """Get observations for all targets of a given type."""
        results: dict[str, LatencyObservation] = {}
        
        async with self._lock:
            for (target_id, ltype), windows in self._windows.items():
                if ltype != latency_type:
                    continue
                
                # Get observation for this target
                obs = await self.get_observation(target_id, latency_type, window_seconds)
                if obs is not None:
                    results[target_id] = obs
        
        return results
    
    async def cleanup_target(self, target_id: str) -> None:
        """Remove all data for a target (e.g., on DC removal)."""
        async with self._lock:
            keys_to_remove = [
                key for key in self._windows.keys()
                if key[0] == target_id
            ]
            for key in keys_to_remove:
                del self._windows[key]
Part 4: Extended Routing Scorer (SLO-Aware)

"""
SLO-aware routing scorer (extends AD-36 Part 4).
"""

from dataclasses import dataclass

from hyperscale.distributed.routing.candidate_filter import DatacenterCandidate
from hyperscale.distributed.routing.routing_state import DatacenterRoutingScore
from hyperscale.distributed.resources.slo.slo_models import (
    LatencySLO,
    SLOComplianceScore,
)


@dataclass(slots=True)
class SLOAwareScoringConfig:
    """Configuration for SLO-aware scoring."""
    
    # Load factor weights (from AD-36)
    a_util: float = 0.5
    a_queue: float = 0.3
    a_cb: float = 0.2
    queue_smoothing: float = 10.0
    load_factor_max: float = 5.0
    
    # Quality penalty weights (from AD-36)
    a_quality: float = 0.5
    quality_penalty_max: float = 2.0
    
    # Preference multiplier (from AD-36)
    preference_multiplier: float = 0.9
    
    # NEW: SLO factor configuration
    enable_slo_scoring: bool = True
    slo_factor_min: float = 0.5  # Maximum bonus
    slo_factor_max: float = 3.0  # Maximum penalty
    a_slo: float = 0.4  # Weight for SLO deviation
    
    # Default SLO (can be overridden per-job)
    default_slo: LatencySLO = None
    
    def __post_init__(self):
        if self.default_slo is None:
            self.default_slo = LatencySLO()


@dataclass(slots=True)
class SLOAwareRoutingScore:
    """Extended routing score with SLO factor."""
    
    datacenter_id: str
    
    # Base components (from AD-36)
    rtt_ucb_ms: float
    load_factor: float
    quality_penalty: float
    preference_multiplier: float
    
    # NEW: SLO component
    slo_factor: float
    slo_compliance: SLOComplianceScore | None
    
    # Final score (lower is better)
    final_score: float
    
    @classmethod
    def calculate(
        cls,
        candidate: DatacenterCandidate,
        slo_compliance: SLOComplianceScore | None,
        config: SLOAwareScoringConfig,
        is_preferred: bool = False,
    ) -> "SLOAwareRoutingScore":
        """Calculate SLO-aware routing score."""
        
        # Calculate utilization
        if candidate.total_cores > 0:
            utilization = 1.0 - (candidate.available_cores / candidate.total_cores)
        else:
            utilization = 1.0
        
        # Queue factor
        queue_normalized = candidate.queue_depth / (
            candidate.queue_depth + config.queue_smoothing
        )
        
        # Load factor (from AD-36)
        load_factor = (
            1.0
            + config.a_util * utilization
            + config.a_queue * queue_normalized
            + config.a_cb * candidate.circuit_breaker_pressure
        )
        load_factor = min(load_factor, config.load_factor_max)
        
        # Quality penalty (from AD-36)
        quality_penalty = 1.0 + config.a_quality * (1.0 - candidate.coordinate_quality)
        quality_penalty = min(quality_penalty, config.quality_penalty_max)
        
        # Preference multiplier
        pref_mult = config.preference_multiplier if is_preferred else 1.0
        
        # NEW: SLO factor
        if config.enable_slo_scoring and slo_compliance is not None:
            slo_factor = slo_compliance.routing_factor
        else:
            slo_factor = 1.0
        
        slo_factor = max(config.slo_factor_min, min(config.slo_factor_max, slo_factor))
        
        # Final score (lower is better)
        final_score = (
            candidate.rtt_ucb_ms *
            load_factor *
            quality_penalty *
            slo_factor *
            pref_mult
        )
        
        return cls(
            datacenter_id=candidate.datacenter_id,
            rtt_ucb_ms=candidate.rtt_ucb_ms,
            load_factor=load_factor,
            quality_penalty=quality_penalty,
            preference_multiplier=pref_mult,
            slo_factor=slo_factor,
            slo_compliance=slo_compliance,
            final_score=final_score,
        )


class SLOAwareRoutingScorer:
    """
    SLO-aware routing scorer (extends AD-36 RoutingScorer).
    
    Extended score formula:
        score = rtt_ucb × load_factor × quality_penalty × slo_factor × pref_mult
    
    The slo_factor is derived from SLO compliance:
        - Meeting SLO (ratio < 1.0): factor < 1.0 (bonus)
        - At SLO boundary (ratio = 1.0): factor = 1.0 (neutral)
        - Violating SLO (ratio > 1.0): factor > 1.0 (penalty)
    """
    
    def __init__(
        self,
        config: SLOAwareScoringConfig | None = None,
    ) -> None:
        self._config = config or SLOAwareScoringConfig()
    
    def score_datacenter(
        self,
        candidate: DatacenterCandidate,
        slo_compliance: SLOComplianceScore | None = None,
        is_preferred: bool = False,
    ) -> SLOAwareRoutingScore:
        """Score a datacenter with SLO awareness."""
        return SLOAwareRoutingScore.calculate(
            candidate=candidate,
            slo_compliance=slo_compliance,
            config=self._config,
            is_preferred=is_preferred,
        )
    
    def score_datacenters(
        self,
        candidates: list[DatacenterCandidate],
        slo_scores: dict[str, SLOComplianceScore],
        preferred_datacenters: set[str] | None = None,
    ) -> list[SLOAwareRoutingScore]:
        """Score and rank datacenters with SLO awareness."""
        preferred = preferred_datacenters or set()
        
        scores = [
            self.score_datacenter(
                candidate=c,
                slo_compliance=slo_scores.get(c.datacenter_id),
                is_preferred=c.datacenter_id in preferred,
            )
            for c in candidates
        ]
        
        return sorted(scores, key=lambda s: s.final_score)
Data Flow Diagram

┌─────────────────────────────────────────────────────────────────────────┐
│                    SLO-AWARE ROUTING DATA FLOW                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. LATENCY COLLECTION                                                   │
│  ─────────────────────                                                   │
│                                                                          │
│  Gate dispatches job → Manager                                          │
│       │                    │                                             │
│       │ t_start            │                                             │
│       └────────────────────┘                                             │
│                            │                                             │
│  Manager responds ←────────┘                                             │
│       │                                                                  │
│       │ t_end                                                            │
│       │                                                                  │
│       ▼                                                                  │
│  latency_ms = t_end - t_start                                           │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ LatencyDigestTracker.record_latency(                             │    │
│  │     target_id="dc-east",                                        │    │
│  │     latency_type="dispatch",                                    │    │
│  │     latency_ms=145.3,                                           │    │
│  │ )                                                                │    │
│  │                                                                  │    │
│  │ Internal: Updates T-Digest for (dc-east, dispatch) window       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  2. SLO COMPLIANCE COMPUTATION (on routing decision)                    │
│  ────────────────────────────────────────────────────                   │
│                                                                          │
│  New job arrives → need to select datacenter                            │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ For each datacenter candidate:                                   │    │
│  │                                                                  │    │
│  │   observation = tracker.get_observation("dc-east", "dispatch")  │    │
│  │   → {p50: 45ms, p95: 180ms, p99: 420ms, samples: 1523}         │    │
│  │                                                                  │    │
│  │   slo = LatencySLO(p50=50, p95=200, p99=500)                   │    │
│  │                                                                  │    │
│  │   compliance = SLOComplianceScore.calculate(observation, slo)   │    │
│  │   → {                                                           │    │
│  │       p50_ratio: 0.90,  # 45/50 = under target                 │    │
│  │       p95_ratio: 0.90,  # 180/200 = under target               │    │
│  │       p99_ratio: 0.84,  # 420/500 = under target               │    │
│  │       composite: 0.88,  # Weighted average                     │    │
│  │       level: MEETING,                                           │    │
│  │       routing_factor: 0.95,  # 5% bonus                        │    │
│  │     }                                                           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  3. ROUTING SCORE INTEGRATION                                           │
│  ─────────────────────────────                                          │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ SLOAwareRoutingScorer.score_datacenter(candidate, compliance)   │    │
│  │                                                                  │    │
│  │ score = rtt_ucb × load_factor × quality × slo_factor × pref    │    │
│  │       = 145     × 1.2        × 1.05    × 0.95      × 1.0       │    │
│  │       = 172.4                                                   │    │
│  │                                                                  │    │
│  │ Compare to DC without SLO bonus:                                │    │
│  │       = 145     × 1.2        × 1.05    × 1.0       × 1.0       │    │
│  │       = 181.5                                                   │    │
│  │                                                                  │    │
│  │ DC meeting SLO gets 5% lower score (better routing priority)    │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  4. COMPARISON: DC VIOLATING SLO                                        │
│  ─────────────────────────────────                                       │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ dc-west observation: {p50: 80ms, p95: 350ms, p99: 800ms}       │    │
│  │                                                                  │    │
│  │ compliance:                                                      │    │
│  │   p50_ratio: 1.60  # 80/50 = over target                        │    │
│  │   p95_ratio: 1.75  # 350/200 = over target                      │    │
│  │   p99_ratio: 1.60  # 800/500 = over target                      │    │
│  │   composite: 1.68                                                │    │
│  │   level: CRITICAL                                                │    │
│  │   routing_factor: 1.27  # 27% penalty                           │    │
│  │                                                                  │    │
│  │ score = 120 × 1.1 × 1.0 × 1.27 × 1.0 = 167.6                   │    │
│  │                                                                  │    │
│  │ Even though dc-west has lower RTT (120 vs 145), its SLO        │    │
│  │ violation penalty makes it score similarly to dc-east.          │    │
│  │ If violation were worse, dc-east would be preferred.           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
Summary: Architecture Comparison
Approach	Accuracy	Memory	Merge	Complexity	Recommendation
T-Digest	~0.1% at tails	O(δ) ≈ 100 centroids	✅ Yes	Medium	Primary choice
DDSketch	ε-guaranteed	O(log range)	✅ Yes	Low	Alternative
Circular buffer	Exact (samples)	O(n × windows)	❌ No	Very low	Fallback
HDR Histogram	Fixed precision	O(buckets)	✅ Yes	Low	If range known
Recommended: T-Digest because:

Tail-optimized (p95, p99 most important for SLO)
Mergeable (aggregate across nodes)
Pure Python + numpy (existing dependency)
Battle-tested algorithm
Shall I add this as AD-42 to the architecture document?