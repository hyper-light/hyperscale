"""
Best-effort completion manager (AD-44).
"""

import asyncio
from typing import Awaitable, Callable

from hyperscale.distributed.runtime import Clock, Runner

from .best_effort_state import BestEffortState
from .reliability_config import ReliabilityConfig

CompletionHandler = Callable[[str, str, bool], Awaitable[None]]
BestEffortDecision = tuple[bool, str, bool]


class BestEffortManager:
    """
    Manages best-effort completion state per job.

    A job in best-effort mode completes as soon as ``min_dcs`` of its
    datacenters completed, when its deadline passes, or when every
    datacenter reported -- whichever comes first. Results are judged as
    they arrive (``record_result``); a periodic deadline check, driven by
    the injected clock, completes jobs whose deadline passed with no
    further result. State is held only until the job completes or is
    cleaned up.
    """

    __slots__ = (
        "_states",
        "_lock",
        "_config",
        "_task_runner",
        "_clock",
        "_deadline_task_token",
        "_completion_handler",
    )

    def __init__(
        self,
        task_runner: Runner,
        config: ReliabilityConfig,
        clock: Clock,
        completion_handler: CompletionHandler,
    ) -> None:
        self._config = config
        self._task_runner = task_runner
        self._clock = clock
        self._states: dict[str, BestEffortState] = {}
        self._lock = asyncio.Lock()
        self._deadline_task_token: str | None = None
        self._completion_handler = completion_handler

    async def create_state(
        self,
        job_id: str,
        min_dcs: int,
        deadline_seconds: float,
        target_dcs: set[str],
    ) -> BestEffortState:
        """Track a best-effort job; ``deadline_seconds`` counts from now
        (0 = the configured default, clamped to the configured maximum)."""
        state = BestEffortState(
            job_id=job_id,
            enabled=True,
            min_dcs=self._resolve_min_dcs(min_dcs, target_dcs),
            deadline=self._clock.monotonic() + self._resolve_deadline_seconds(deadline_seconds),
            target_dcs=set(target_dcs),
        )
        async with self._lock:
            self._states[job_id] = state
        return state

    def has_state(self, job_id: str) -> bool:
        return job_id in self._states

    def get_target_dcs(self, job_id: str) -> set[str]:
        """The datacenters a tracked job waits on (empty when untracked)."""
        if (state := self._states.get(job_id)) is None:
            return set()
        return set(state.target_dcs)

    async def record_result(
        self,
        job_id: str,
        dc_id: str,
        success: bool,
    ) -> BestEffortDecision | None:
        """Record a datacenter's result; the job's completion decision, or
        None when the job is not (or no longer) tracked as best-effort."""
        async with self._lock:
            if (state := self._states.get(job_id)) is None:
                return None
            state.record_dc_result(dc_id, success)
            return state.check_completion(self._clock.monotonic())

    async def check_all_completions(self) -> list[tuple[str, str, bool]]:
        """Every tracked job whose completion conditions now hold."""
        now = self._clock.monotonic()
        async with self._lock:
            return [
                (job_id, reason, success)
                for job_id, state in self._states.items()
                for should_complete, reason, success in (state.check_completion(now),)
                if should_complete
            ]

    def start_deadline_loop(self) -> None:
        """Start the periodic deadline check (a TaskRunner task on the
        injected clock)."""
        if self._deadline_task_token:
            return

        run = self._task_runner.run(
            self._deadline_check_loop,
            alias="best_effort_deadline_check",
        )
        if run is not None:
            self._deadline_task_token = f"{run.task_name}:{run.run_id}"

    async def stop_deadline_loop(self) -> None:
        """Stop the periodic deadline check."""
        if not self._deadline_task_token:
            return

        token, self._deadline_task_token = self._deadline_task_token, None
        await self._task_runner.cancel(token)

    async def cleanup(self, job_id: str) -> None:
        """Remove best-effort state for a completed job."""
        async with self._lock:
            self._states.pop(job_id, None)

    async def shutdown(self) -> None:
        """Stop deadline checks and clear state."""
        await self.stop_deadline_loop()
        async with self._lock:
            self._states.clear()

    @property
    def tracked_job_count(self) -> int:
        return len(self._states)

    async def _deadline_check_loop(self) -> None:
        while True:
            await self._clock.sleep(self._config.best_effort_deadline_check_interval)
            for job_id, reason, success in await self.check_all_completions():
                await self._completion_handler(job_id, reason, success)

    def _resolve_deadline_seconds(self, deadline_seconds: float) -> float:
        if deadline_seconds <= 0:
            return self._config.best_effort_deadline_default
        return min(deadline_seconds, self._config.best_effort_deadline_max)

    def _resolve_min_dcs(self, min_dcs: int, target_dcs: set[str]) -> int:
        requested = min_dcs if min_dcs > 0 else self._config.best_effort_min_dcs_default
        if not target_dcs:
            return 0
        return min(max(1, requested), len(target_dcs))
