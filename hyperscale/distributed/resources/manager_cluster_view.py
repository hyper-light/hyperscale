from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic


@dataclass(slots=True)
class ManagerClusterResourceView:
    """Aggregated cluster view computed from manager local views."""

    datacenter: str
    computing_manager_id: str
    manager_count: int = 0
    manager_aggregate_cpu_percent: float = 0.0
    manager_aggregate_memory_bytes: int = 0
    manager_views: dict[str, "ManagerLocalView"] = field(default_factory=dict)
    worker_count: int = 0
    worker_aggregate_cpu_percent: float = 0.0
    worker_aggregate_memory_bytes: int = 0
    total_cores_available: int = 0
    total_cores_allocated: int = 0
    cpu_pressure: float = 0.0
    memory_pressure: float = 0.0
    vector_clock: dict[str, int] = field(default_factory=dict)
    timestamp_monotonic: float = field(default_factory=monotonic)


from hyperscale.distributed.resources.manager_local_view import ManagerLocalView
