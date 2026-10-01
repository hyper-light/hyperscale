from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env


@dataclass(frozen=True, slots=True)
class ResourceBudget:
    """AD-41 resource limits one workflow of a job is enforced against.

    Thresholds are fractions of the limit: past ``warning_threshold`` a
    sustained violation is warned about once; past ``kill_threshold``
    with 2-sigma confidence it is killed.
    """

    max_cpu_percent: float
    max_memory_bytes: int
    warning_threshold: float
    kill_threshold: float
    warning_grace_seconds: float
    kill_grace_seconds: float

    @classmethod
    def from_env(cls, env: "Env") -> ResourceBudget:
        """The default budget, for jobs that assign none."""
        return cls(
            max_cpu_percent=env.RESOURCE_GUARD_MAX_CPU_PERCENT,
            max_memory_bytes=env.RESOURCE_GUARD_MAX_MEMORY_BYTES,
            warning_threshold=env.RESOURCE_GUARD_WARNING_THRESHOLD,
            kill_threshold=env.RESOURCE_GUARD_KILL_THRESHOLD,
            warning_grace_seconds=env.RESOURCE_GUARD_WARNING_GRACE_SECONDS,
            kill_grace_seconds=env.RESOURCE_GUARD_KILL_GRACE_SECONDS,
        )
