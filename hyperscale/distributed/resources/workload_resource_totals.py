from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class WorkloadResourceTotals:
    """AD-41: summed resource estimates of a set of running workflows.

    CPU is in percent of one core (100 per busy core), memory in bytes.
    Variances add across independent workflow estimates, so totals from
    several managers combine by summing every field.
    """

    cpu_percent: float = 0.0
    cpu_variance: float = 0.0
    memory_bytes: float = 0.0
    memory_variance: float = 0.0
    workflow_count: int = 0
