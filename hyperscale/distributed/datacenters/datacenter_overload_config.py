from dataclasses import dataclass
from enum import Enum


class DatacenterOverloadState(Enum):
    HEALTHY = "healthy"
    BUSY = "busy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


OVERLOAD_STATE_ORDER = {
    DatacenterOverloadState.HEALTHY: 0,
    DatacenterOverloadState.BUSY: 1,
    DatacenterOverloadState.DEGRADED: 2,
    DatacenterOverloadState.UNHEALTHY: 3,
}


@dataclass(slots=True)
class DatacenterOverloadConfig:
    worker_overload_busy_threshold: float = 0.30
    worker_overload_degraded_threshold: float = 0.50
    worker_overload_unhealthy_threshold: float = 0.80

    manager_unhealthy_busy_threshold: float = 0.30
    manager_unhealthy_degraded_threshold: float = 0.50
    manager_unhealthy_unhealthy_threshold: float = 0.80

    capacity_utilization_busy_threshold: float = 0.70
    capacity_utilization_degraded_threshold: float = 0.85
    capacity_utilization_unhealthy_threshold: float = 0.95

    health_severity_weight_healthy: float = 1.0
    health_severity_weight_busy: float = 1.5
    health_severity_weight_degraded: float = 3.0
