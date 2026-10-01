from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class LedWorkflowSample:
    """The latest resource estimate of one workflow its job leader holds."""

    job_id: str
    cpu_percent: float
    cpu_variance: float
    memory_bytes: float
    memory_variance: float
    observed_at: float
