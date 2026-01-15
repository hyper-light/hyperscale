from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic

from hyperscale.distributed.env import Env

from .latency_observation import LatencyObservation
from .latency_slo import LatencySLO
from .slo_config import SLOConfig


@dataclass(slots=True)
class SLOHealthClassifier:
    """Converts SLO compliance to AD-16 health signal."""

    _config: SLOConfig = field(default_factory=SLOConfig.from_env)
    _violation_start: dict[str, float] = field(default_factory=dict, init=False)

    @classmethod
    def from_env(cls, env: Env | None = None) -> "SLOHealthClassifier":
        return cls(_config=SLOConfig.from_env(env))

    def _violation_duration(
        self, datacenter_id: str, is_violating: bool, now: float
    ) -> float:
        if not is_violating:
            self._violation_start.pop(datacenter_id, None)
            return 0.0
        start_time = self._violation_start.get(datacenter_id)
        if start_time is None:
            self._violation_start[datacenter_id] = now
            return 0.0
        return now - start_time

    def compute_health_signal(
        self,
        datacenter_id: str,
        slo: LatencySLO,
        observation: LatencyObservation,
    ) -> str:
        """Return HEALTHY, BUSY, DEGRADED, or UNHEALTHY."""
        now = monotonic()
        p50_ratio = observation.p50_ms / slo.p50_target_ms
        p95_ratio = observation.p95_ms / slo.p95_target_ms
        p99_ratio = observation.p99_ms / slo.p99_target_ms

        is_violating = (
            p50_ratio > self._config.busy_p50_ratio
            or p95_ratio > 1.0
            or p99_ratio > 1.0
        )
        violation_duration = self._violation_duration(datacenter_id, is_violating, now)
        if violation_duration == 0.0:
            return "HEALTHY"

        if (
            p99_ratio >= self._config.unhealthy_p99_ratio
            and violation_duration >= self._config.unhealthy_window_seconds
        ):
            return "UNHEALTHY"

        if violation_duration >= self._config.degraded_window_seconds and (
            p95_ratio >= self._config.degraded_p95_ratio
            or p99_ratio >= self._config.degraded_p99_ratio
        ):
            return "DEGRADED"

        if (
            violation_duration >= self._config.busy_window_seconds
            and p50_ratio >= self._config.busy_p50_ratio
        ):
            return "BUSY"

        return "HEALTHY"
