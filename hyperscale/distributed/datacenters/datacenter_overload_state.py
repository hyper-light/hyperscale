"""``DatacenterOverloadState`` -- pickled under the namespace
``hyperscale.distributed.datacenters.datacenter_overload_config`` (see that module)."""

from enum import Enum


class DatacenterOverloadState(Enum):
    HEALTHY = "healthy"
    BUSY = "busy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
