from __future__ import annotations

from dataclasses import dataclass, field

from hyperscale.distributed.resources.resource_metrics import ResourceMetrics
from hyperscale.distributed.resources.workload_resource_totals import WorkloadResourceTotals


@dataclass(slots=True, frozen=True)
class ManagerResourceReport:
    """AD-41: what one manager tells gates about its datacenter's resources.

    ``workload`` covers only the workflows this manager leads (see
    ``LedWorkflowResources``), so a gate sums it across the datacenter's
    managers. Capacity covers every healthy registered worker -- the same
    pool on every manager -- so a gate takes it from one report:
    ``cpu_capacity_percent`` is 100 per allotted core,
    ``memory_capacity_bytes`` counts each worker host's memory once.
    """

    manager_metrics: ResourceMetrics | None
    workload: WorkloadResourceTotals = field(default_factory=WorkloadResourceTotals)
    cpu_capacity_percent: float = 0.0
    memory_capacity_bytes: int = 0
