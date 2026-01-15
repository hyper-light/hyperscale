"""
Execution time estimation for capacity planning (AD-43).
"""

from __future__ import annotations

import time

from hyperscale.distributed.models.jobs import PendingWorkflow
from hyperscale.distributed.taskex.util.time_parser import TimeParser

from .active_dispatch import ActiveDispatch


class ExecutionTimeEstimator:
    """
    Estimates when cores will become available based on workflow durations.
    """

    def __init__(
        self,
        active_dispatches: dict[str, ActiveDispatch],
        pending_workflows: dict[str, PendingWorkflow],
        total_cores: int,
    ) -> None:
        self._active = active_dispatches
        self._pending = pending_workflows
        self._total_cores = total_cores

    def estimate_wait_for_cores(self, cores_needed: int) -> float:
        """
        Estimate seconds until the requested cores are available.
        """
        if cores_needed <= 0:
            return 0.0
        if self._total_cores <= 0:
            return float("inf")

        now = time.monotonic()
        completions = self._get_completions(now)
        available_cores = self._get_available_cores()

        if available_cores >= cores_needed:
            return 0.0

        for completion_time, cores_freeing in completions:
            available_cores += cores_freeing
            if available_cores >= cores_needed:
                return completion_time - now

        return float("inf")

    def get_pending_duration_sum(self) -> float:
        """
        Sum duration for all pending workflows that are not dispatched.
        """
        return sum(
            TimeParser(pending.workflow.duration).time
            for pending in self._pending.values()
            if not pending.dispatched
        )

    def get_active_remaining_sum(self) -> float:
        """
        Sum remaining duration for all active dispatches.
        """
        now = time.monotonic()
        return sum(
            dispatch.remaining_seconds(now) for dispatch in self._active.values()
        )

    def _get_completions(self, now: float) -> list[tuple[float, int]]:
        completions: list[tuple[float, int]] = []
        for dispatch in self._active.values():
            completion = dispatch.expected_completion()
            if completion > now:
                completions.append((completion, dispatch.cores_allocated))
        completions.sort(key=lambda entry: entry[0])
        return completions

    def _get_available_cores(self) -> int:
        active_cores = sum(
            dispatch.cores_allocated for dispatch in self._active.values()
        )
        return self._total_cores - active_cores
