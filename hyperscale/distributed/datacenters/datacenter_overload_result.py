"""``DatacenterOverloadResult`` -- pickled under the namespace
``hyperscale.distributed.datacenters.datacenter_overload_classifier`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.datacenters.datacenter_overload_config import DatacenterOverloadState


@dataclass(slots=True)
class DatacenterOverloadResult:
    state: DatacenterOverloadState
    worker_overload_ratio: float
    manager_unhealthy_ratio: float
    manager_overload_ratio: float
    capacity_utilization: float
    health_severity_weight: float
    leader_overloaded: bool = False
