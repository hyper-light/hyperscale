"""
Observed-latency evidence for routing decisions (AD-45).
"""

from __future__ import annotations

from .observed_latency_tracker import ObservedLatencyTracker


class BlendedLatencyScorer:
    """
    The gate's AD-45 latency evidence: each datacenter's observed latency
    and the confidence in it, or none at all while adaptive routing is
    disabled. The datacenter latency estimator blends it with Vivaldi.
    """

    def __init__(
        self,
        observed_latency_tracker: ObservedLatencyTracker,
        adaptive_routing_enabled: bool,
    ) -> None:
        self._observed_latency_tracker = observed_latency_tracker
        self._adaptive_routing_enabled = adaptive_routing_enabled

    def get_observed_latency(self, datacenter_id: str) -> tuple[float, float]:
        """``(observed_latency_ms, confidence)``; zero confidence while
        adaptive routing is disabled."""
        if self._adaptive_routing_enabled:
            return self._observed_latency_tracker.get_observed_latency(datacenter_id)
        return 0.0, 0.0
