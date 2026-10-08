from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env


@dataclass(frozen=True, slots=True)
class ResourceBudget:
    """AD-41 resource limits one workflow of a job is enforced against.

    Thresholds are fractions of the limit: past ``warning_threshold`` a
    sustained violation is warned about once; past ``throttle_threshold``
    the workflow's concurrency is cut back toward it; past
    ``kill_threshold`` with 2-sigma confidence it is killed.
    """

    max_cpu_percent: float
    max_memory_bytes: int
    warning_threshold: float
    throttle_threshold: float
    kill_threshold: float
    warning_grace_seconds: float
    kill_grace_seconds: float

    def __post_init__(self) -> None:
        if errors := self.validation_errors():
            raise ValueError(f"invalid resource budget: {'; '.join(errors)}")

    def validation_errors(self) -> list[str]:
        """What makes this budget unenforceable (empty when it is valid).

        A budget can arrive deserialized from a submission, which skips
        ``__init__``, so a receiver checks it explicitly.
        """
        return [
            message
            for violated, message in (
                (not self.max_cpu_percent > 0.0, "max_cpu_percent must be positive"),
                (not self.max_memory_bytes > 0, "max_memory_bytes must be positive"),
                (
                    not 0.0 < self.warning_threshold <= self.throttle_threshold <= self.kill_threshold,
                    "thresholds must satisfy 0 < warning_threshold <= throttle_threshold <= kill_threshold",
                ),
                (
                    # A chained comparison short-circuits as `and` does: kill grace
                    # is compared only once warning grace is non-negative.
                    not self.warning_grace_seconds >= 0.0 <= self.kill_grace_seconds,
                    "grace periods must not be negative",
                ),
            )
            if violated
        ]

    @classmethod
    def from_env(cls, env: "Env") -> ResourceBudget:
        """The default budget, for jobs that assign none."""
        return cls(
            max_cpu_percent=env.RESOURCE_GUARD_MAX_CPU_PERCENT,
            max_memory_bytes=env.RESOURCE_GUARD_MAX_MEMORY_BYTES,
            warning_threshold=env.RESOURCE_GUARD_WARNING_THRESHOLD,
            throttle_threshold=env.RESOURCE_GUARD_THROTTLE_THRESHOLD,
            kill_threshold=env.RESOURCE_GUARD_KILL_THRESHOLD,
            warning_grace_seconds=env.RESOURCE_GUARD_WARNING_GRACE_SECONDS,
            kill_grace_seconds=env.RESOURCE_GUARD_KILL_GRACE_SECONDS,
        )
