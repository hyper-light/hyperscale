"""``DatacenterOverloadSignals`` -- pickled under the namespace
``hyperscale.distributed.datacenters.datacenter_overload_classifier`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class DatacenterOverloadSignals:
    total_workers: int
    healthy_workers: int
    overloaded_workers: int
    stressed_workers: int
    busy_workers: int
    total_managers: int
    alive_managers: int
    total_cores: int
    available_cores: int
    overloaded_managers: int = 0
    stressed_managers: int = 0
    busy_managers: int = 0
    leader_health_state: str = "healthy"
