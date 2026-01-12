from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic

from hyperscale.distributed.resources.resource_metrics import ResourceMetrics


@dataclass(slots=True)
class WorkerResourceReport:
    """Aggregate resource metrics for a worker node."""

    node_id: str
    aggregate_metrics: ResourceMetrics
    workflow_metrics: dict[str, ResourceMetrics] = field(default_factory=dict)
    total_system_memory_bytes: int = 0
    total_system_cpu_count: int = 0
    version: int = 0
    timestamp_monotonic: float = field(default_factory=monotonic)

    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """Return True if this report is older than max_age_seconds."""
        return (monotonic() - self.timestamp_monotonic) > max_age_seconds
