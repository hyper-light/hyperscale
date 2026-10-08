"""
Execution time estimation for capacity planning (AD-43).
"""

from __future__ import annotations


from hyperscale.distributed.models.jobs import PendingWorkflow
from hyperscale.distributed.taskex.util.time_parser import TimeParser

from .active_dispatch import ActiveDispatch

from hyperscale.distributed.runtime import Clock


class ExecutionTimeEstimator:
    """
    Estimates when cores will become available based on workflow durations.
    """

    def __init__(
        self,
        active_dispatches: dict[str, ActiveDispatch],
        pending_workflows: dict[str, PendingWorkflow],
        total_cores: int,
        clock: Clock,
    ) -> None:
        """``clock`` is the one the dispatches' ``dispatched_at`` was read
        from: remaining time is measured on it."""
        self._active = active_dispatches
        self._pending = pending_workflows
        self._total_cores = total_cores
        self._clock = clock

    def get_release_schedule(self) -> list[tuple[float, int]]:
        """
        When the cores executing workflows hold come free: ``(seconds from
        now, cores)``, soonest first. A dispatch frees its cores when its
        workflow's duration ends; one running past that frees them by its
        timeout at the latest; one past both is freeing them now.
        """
        now = self._clock.monotonic()
        schedule: list[tuple[float, int]] = []
        for dispatch in self._active.values():
            expected_release = dispatch.expected_completion()
            if expected_release <= now:
                expected_release = max(
                    dispatch.dispatched_at + dispatch.timeout_seconds, now
                )
            schedule.append((expected_release - now, dispatch.cores_allocated))
        schedule.sort()
        return schedule

    def get_pending_duration_sum(self) -> float:
        """
        Sum duration for all pending workflows -- the queued work that will
        still run.
        """
        return sum(
            TimeParser(pending.workflow.duration).time
            for pending in self._pending.values()
        )

    def get_active_remaining_sum(self) -> float:
        """
        Sum remaining duration for all active dispatches.
        """
        now = self._clock.monotonic()
        return sum(
            dispatch.remaining_seconds(now) for dispatch in self._active.values()
        )
