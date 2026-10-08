from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class DatacenterResourceView:
    """AD-41: a gate's view of one datacenter's resource pressure.

    Pressures are the running workload's share of the datacenter's worker
    capacity, capped at 1.0; uncertainties are one standard deviation of
    the workload estimate, in its own units (CPU percent, memory bytes).
    """

    datacenter: str
    reporting_manager_count: int
    workload_cpu_percent: float
    workload_cpu_uncertainty: float
    workload_memory_bytes: float
    workload_memory_uncertainty: float
    cpu_capacity_percent: float
    memory_capacity_bytes: int
    cpu_pressure: float
    memory_pressure: float
    manager_cpu_percent: float
    manager_memory_bytes: int
