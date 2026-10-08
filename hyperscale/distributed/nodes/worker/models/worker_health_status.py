"""``WorkerHealthIntegration.get_health_status`` -- the worker's overload, backpressure and manager health."""

from __future__ import annotations

from typing import TypedDict


class WorkerHealthStatus(TypedDict):
    """Whether the worker is healthy, its overload state, the highest backpressure level its
    managers signal (``BackpressureLevel`` value) and the delay it imposes, and its managers."""

    healthy: bool
    overload_state: str
    backpressure_level: int
    backpressure_delay_ms: int
    healthy_managers: int
    known_managers: int
    primary_manager: str | None
