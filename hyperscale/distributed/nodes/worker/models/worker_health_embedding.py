"""``WorkerHealthIntegration.get_health_embedding`` -- the worker health a SWIM state embedding carries."""

from __future__ import annotations

from typing import TypedDict


class WorkerHealthEmbedding(TypedDict):
    """The worker's overload state and the monotonic time it was read."""

    overload_state: str
    timestamp: float
