"""
Blended latency scorer for routing decisions (AD-45).
"""

from __future__ import annotations

from .observed_latency_tracker import ObservedLatencyTracker


class BlendedLatencyScorer:
    """
    Applies adaptive latency blending for routing scores.
    """

    def __init__(self, observed_latency_tracker: ObservedLatencyTracker) -> None:
        self._observed_latency_tracker = observed_latency_tracker

    def get_latency_for_scoring(
        self,
        datacenter_id: str,
        predicted_rtt_ms: float,
        use_blending: bool,
    ) -> float:
        """
        Get latency for routing score calculation.
        """
        if use_blending:
            return self._observed_latency_tracker.get_blended_latency(
                datacenter_id=datacenter_id,
                predicted_rtt_ms=predicted_rtt_ms,
            )
        return predicted_rtt_ms
