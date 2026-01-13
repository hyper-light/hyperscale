"""
Observed latency tracker for adaptive route learning (AD-45).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from time import monotonic

from .observed_latency_state import ObservedLatencyState


@dataclass
class ObservedLatencyTracker:
    """
    Gate-level tracker for observed latencies across datacenters.
    """

    alpha: float = 0.1
    min_samples_for_confidence: int = 10
    max_staleness_seconds: float = 300.0
    latency_cap_ms: float | None = None

    _latencies: dict[str, ObservedLatencyState] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def record_job_latency(
        self,
        datacenter_id: str,
        latency_ms: float,
        now: float | None = None,
    ) -> None:
        capped_latency = self._cap_latency(latency_ms)
        async with self._lock:
            state = self._latencies.get(datacenter_id)
            if state is None:
                state = ObservedLatencyState(datacenter_id=datacenter_id)
                self._latencies[datacenter_id] = state

            state.record_latency(
                latency_ms=capped_latency,
                alpha=self.alpha,
                now=now,
            )

    def get_observed_latency(self, datacenter_id: str) -> tuple[float, float]:
        """
        Get observed latency and confidence for a datacenter.
        """
        state = self._latencies.get(datacenter_id)
        if state is None:
            return 0.0, 0.0

        current_time = monotonic()
        confidence = self._get_effective_confidence(state, current_time)
        return state.ewma_ms, confidence

    def get_blended_latency(
        self,
        datacenter_id: str,
        predicted_rtt_ms: float,
    ) -> float:
        """
        Blend observed latency with predicted RTT UCB.
        """
        observed_ms, confidence = self.get_observed_latency(datacenter_id)
        if confidence == 0.0:
            return predicted_rtt_ms
        return (confidence * observed_ms) + ((1 - confidence) * predicted_rtt_ms)

    def get_metrics(self) -> dict[str, dict[str, float | int | bool]]:
        """
        Return tracker metrics for observability.
        """
        current_time = monotonic()
        per_datacenter: dict[str, dict[str, float | int | bool]] = {}
        for datacenter_id, state in self._latencies.items():
            confidence = self._get_effective_confidence(state, current_time)
            per_datacenter[datacenter_id] = {
                "ewma_ms": state.ewma_ms,
                "sample_count": state.sample_count,
                "confidence": confidence,
                "stddev_ms": state.get_stddev_ms(),
                "last_update": state.last_update,
                "stale": state.is_stale(self.max_staleness_seconds, current_time),
            }

        return {
            "tracked_dcs": len(self._latencies),
            "per_dc": per_datacenter,
        }

    def _cap_latency(self, latency_ms: float) -> float:
        if self.latency_cap_ms is None:
            return latency_ms
        return min(latency_ms, self.latency_cap_ms)

    def _get_effective_confidence(
        self,
        state: ObservedLatencyState,
        current_time: float,
    ) -> float:
        base_confidence = state.get_confidence(self.min_samples_for_confidence)
        if base_confidence == 0.0:
            return 0.0
        if state.is_stale(self.max_staleness_seconds, current_time):
            staleness_seconds = current_time - state.last_update
            return base_confidence * self._get_staleness_factor(staleness_seconds)
        return base_confidence

    def _get_staleness_factor(self, staleness_seconds: float) -> float:
        if self.max_staleness_seconds <= 0.0:
            return 0.0
        return max(0.0, 1.0 - (staleness_seconds / self.max_staleness_seconds))
