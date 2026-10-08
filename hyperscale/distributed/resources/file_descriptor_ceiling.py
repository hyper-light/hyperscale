"""
AD-41 file-descriptor ceiling for a worker's process tree.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

try:
    import resource
except ImportError:
    # Windows reports no per-process descriptor limit (and psutil reports
    # no descriptor counts there): no ceiling.
    resource = None

from hyperscale.distributed.swim.health.degradation_level import DegradationLevel

from .resource_violation_type import ResourceViolationType

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env


class FileDescriptorCeiling:
    """Refuses new work while a process of the worker's tree nears its descriptor limit.

    The limit is the process's RLIMIT_NOFILE soft limit, read at runtime:
    a per-process kernel ceiling past which ``open``/``socket``/``accept``
    fail with EMFILE. Workflow executors inherit it from the worker, so
    the measure is the largest single process's descriptor count, not
    the tree's sum. Executor processes are not attributable to a workflow
    from the worker, so the response is worker-wide and never kills: at
    ``kill_threshold`` of the limit the worker stops taking new workflows
    (a new workflow opens more sockets on an exhausted process), and it
    takes them again once every process is back under
    ``warning_threshold`` -- the AD-41 budget fractions, with the gap
    between them as hysteresis. Descriptor counts are exact, so no
    uncertainty grace applies.
    """

    __slots__ = (
        "_descriptor_limit",
        "_comparison_lines",
        "_refusing_new_work",
    )

    def __init__(
        self,
        descriptor_limit: int | None,
        warning_threshold: float,
        kill_threshold: float,
    ) -> None:
        self._descriptor_limit = descriptor_limit
        # The line a sample is compared against, indexed by whether the
        # worker is refusing: the kill line to start, the warning line to
        # keep refusing. An unreported limit is an infinite one.
        effective_limit = descriptor_limit if descriptor_limit is not None else math.inf
        self._comparison_lines: tuple[float, float] = (
            effective_limit * kill_threshold,
            effective_limit * warning_threshold,
        )
        self._refusing_new_work = False

    @classmethod
    def from_env(cls, env: "Env") -> FileDescriptorCeiling:
        """The ceiling at this process's descriptor soft limit, with the AD-41 budget fractions."""
        return cls(
            descriptor_limit=cls.detect_descriptor_limit(),
            warning_threshold=env.RESOURCE_GUARD_WARNING_THRESHOLD,
            kill_threshold=env.RESOURCE_GUARD_KILL_THRESHOLD,
        )

    @staticmethod
    def detect_descriptor_limit() -> int | None:
        """This process's RLIMIT_NOFILE soft limit; None where the platform reports none or it is unlimited."""
        if resource is None:
            return None
        soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        return soft_limit if soft_limit != resource.RLIM_INFINITY else None

    @property
    def descriptor_limit(self) -> int | None:
        return self._descriptor_limit

    @property
    def refusing_new_work(self) -> bool:
        return self._refusing_new_work

    @property
    def degradation_floor(self) -> DegradationLevel:
        """HEAVY (the worker drains) while refusing new work, else NORMAL."""
        return DegradationLevel.HEAVY if self._refusing_new_work else DegradationLevel.NORMAL

    def observe(self, largest_process_descriptors: int) -> ResourceViolationType | None:
        """Fold in the latest sample; the violation when refusal just began, else None.

        Refusal begins at the kill line and ends only under the warning
        line; a limit the platform does not report never refuses.
        """
        was_refusing = self._refusing_new_work
        self._refusing_new_work = largest_process_descriptors >= self._comparison_lines[was_refusing]
        return ResourceViolationType.FILE_DESCRIPTORS_EXCEEDED if self._refusing_new_work and not was_refusing else None
