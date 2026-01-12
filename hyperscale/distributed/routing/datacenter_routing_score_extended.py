"""
Extended datacenter routing score for adaptive latency blending (AD-45).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class DatacenterRoutingScoreExtended:
    """
    Routing score with blended latency fields.
    """

    datacenter_id: str
    health_bucket: str
    rtt_ucb_ms: float
    blended_latency_ms: float = 0.0
    observed_latency_ms: float = 0.0
    observed_confidence: float = 0.0
    load_factor: float = 1.0
    quality_penalty: float = 1.0
    final_score: float = 0.0
    is_preferred: bool = False
