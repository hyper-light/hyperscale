"""
Spillover decision model for capacity-aware routing (AD-43).
"""

from dataclasses import dataclass


@dataclass(slots=True)
class SpilloverDecision:
    """
    Result of spillover evaluation.
    """

    should_spillover: bool
    reason: str
    primary_dc: str
    spillover_dc: str | None
    primary_wait_seconds: float
    spillover_wait_seconds: float
    latency_penalty_ms: float
