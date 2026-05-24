"""
Subset of `hyperscale.distributed.env.Env` fields the harness can override
per-cluster, per-DC, or per-node.

Kept narrow on purpose: only the fields the harness actually exercises
today. Extend as scenarios demand it.
"""

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class EnvOverrides:
    """Optional overrides applied on top of the default `Env`.

    None on a field means "leave the Env default in place." Concrete
    values are applied during server construction by the harness.
    """

    request_timeout: str | None = None
    log_level: str | None = None
    connect_timeout_seconds: float | None = None
    worker_max_cores: int | None = None
    max_workers_per_manager: int | None = None
    recovery_jitter_min: float | None = None
    recovery_jitter_max: float | None = None
    gate_swim_global_min_timeout: float | None = None
    gate_swim_global_max_timeout: float | None = None
    gate_swim_job_min_timeout: float | None = None
    gate_swim_job_max_timeout: float | None = None
