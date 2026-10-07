"""
Observed latency tracker for adaptive route learning (AD-45).
"""

from __future__ import annotations

import asyncio

from hyperscale.distributed.runtime import Clock

from .blended_scoring_config import BlendedScoringConfig
from .observed_latency_state import ObservedLatencyState


class ObservedLatencyTracker:
    """
    Gate-level tracker for observed latencies across datacenters.

    Each datacenter's latency is an EWMA of its samples, read on the
    owning node's clock. Confidence ramps with the sample count and
    decays linearly with the age of the newest sample, reaching zero at
    the configured staleness bound; a datacenter whose observations have
    no confidence left is forgotten by ``cleanup_stale_entries``.
    """

    def __init__(self, config: BlendedScoringConfig, clock: Clock) -> None:
        self._config = config
        self._clock = clock
        self._latencies: dict[str, ObservedLatencyState] = {}
        self._lock = asyncio.Lock()

    async def record_job_latency(
        self,
        datacenter_id: str,
        latency_ms: float,
    ) -> tuple[float, int]:
        """Fold a sample into ``datacenter_id``'s observed latency; returns
        the observed latency and sample count after it."""
        capped_latency = min(latency_ms, self._config.latency_cap_ms)
        async with self._lock:
            state = self._latencies.get(datacenter_id)
            if state is None:
                state = ObservedLatencyState(datacenter_id=datacenter_id)
                self._latencies[datacenter_id] = state

            state.record_latency(
                latency_ms=capped_latency,
                alpha=self._config.ewma_alpha,
                now=self._clock.monotonic(),
            )
            return state.ewma_ms, state.sample_count

    def get_observed_latency(self, datacenter_id: str) -> tuple[float, float]:
        """
        Get observed latency and confidence for a datacenter.
        """
        state = self._latencies.get(datacenter_id)
        if state is None:
            return 0.0, 0.0

        confidence = self._get_effective_confidence(state, self._clock.monotonic())
        return state.ewma_ms, confidence

    def get_metrics(self) -> dict[str, int | dict[str, dict[str, float | int | bool]]]:
        """
        Return tracker metrics for observability.
        """
        current_time = self._clock.monotonic()
        per_datacenter: dict[str, dict[str, float | int | bool]] = {}
        for datacenter_id, state in self._latencies.items():
            confidence = self._get_effective_confidence(state, current_time)
            per_datacenter[datacenter_id] = {
                "ewma_ms": state.ewma_ms,
                "sample_count": state.sample_count,
                "confidence": confidence,
                "stddev_ms": state.get_stddev_ms(),
                "last_update": state.last_update,
                "stale": state.is_stale(self._config.max_staleness_seconds, current_time),
            }

        return {
            "tracked_dcs": len(self._latencies),
            "per_dc": per_datacenter,
        }

    def _get_effective_confidence(
        self,
        state: ObservedLatencyState,
        current_time: float,
    ) -> float:
        base_confidence = state.get_confidence(self._config.min_samples_for_confidence)
        if base_confidence == 0.0:
            return 0.0
        return base_confidence * self._get_staleness_factor(current_time - state.last_update)

    def _get_staleness_factor(self, staleness_seconds: float) -> float:
        if self._config.max_staleness_seconds <= 0.0:
            return 0.0
        return max(0.0, 1.0 - (staleness_seconds / self._config.max_staleness_seconds))

    async def cleanup_stale_entries(self) -> list[str]:
        """Forget datacenters whose observations have no confidence left;
        returns them."""
        current_time = self._clock.monotonic()
        async with self._lock:
            stale_datacenter_ids = self._stale_datacenter_ids(current_time)
            for datacenter_id in stale_datacenter_ids:
                self._latencies.pop(datacenter_id, None)
        return stale_datacenter_ids

    def _stale_datacenter_ids(self, current_time: float) -> list[str]:
        """The datacenters whose observations are stale at ``current_time``
        (AD-45); called under ``_lock``."""
        return [
            datacenter_id
            for datacenter_id, state in self._latencies.items()
            if state.is_stale(self._config.max_staleness_seconds, current_time)
        ]

    async def remove_datacenter(self, datacenter_id: str) -> None:
        async with self._lock:
            self._latencies.pop(datacenter_id, None)
