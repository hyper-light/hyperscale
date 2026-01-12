from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic
from typing import TYPE_CHECKING

from hyperscale.distributed.resources.resource_metrics import ResourceMetrics

if TYPE_CHECKING:
    from hyperscale.distributed.resources.worker_resource_report import (
        WorkerResourceReport,
    )


@dataclass(slots=True)
class ManagerLocalView:
    """Local resource view for a single manager."""

    manager_node_id: str
    datacenter: str
    self_metrics: ResourceMetrics
    worker_count: int = 0
    worker_aggregate_cpu_percent: float = 0.0
    worker_aggregate_memory_bytes: int = 0
    worker_reports: dict[str, WorkerResourceReport] = field(default_factory=dict)
    version: int = 0
    timestamp_monotonic: float = field(default_factory=monotonic)

    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """Return True if this view is older than max_age_seconds."""
        return (monotonic() - self.timestamp_monotonic) > max_age_seconds
