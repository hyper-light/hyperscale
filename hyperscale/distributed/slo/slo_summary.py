from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class SLOSummary:
    """Compact SLO summary for SWIM gossip."""

    p50_ms: float
    p95_ms: float
    p99_ms: float
    sample_count: int
    compliance_score: float
    routing_factor: float
    updated_at: float
