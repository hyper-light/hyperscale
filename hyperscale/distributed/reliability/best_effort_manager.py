"""
Best-effort completion manager (AD-44).
"""

import asyncio
import time
from typing import Awaitable, Callable

from hyperscale.distributed.env import Env
from hyperscale.distributed.taskex import TaskRunner

from .best_effort_state import BestEffortState
from .reliability_config import ReliabilityConfig, create_reliability_config_from_env

CompletionHandler = Callable[[str, str, bool], Awaitable[None]]


class BestEffortManager:
    """
    Manages best-effort completion state per job.

    Runs deadline checks via TaskRunner and protects state with an asyncio lock.
    """

    __slots__ = (
        "_states",
        "_lock",
        "_config",
        "_task_runner",
        "_deadline_task_token",
        "_completion_handler",
    )

    def __init__(
        self,
        task_runner: TaskRunner,
        config: ReliabilityConfig | None = None,
        completion_handler: CompletionHandler | None = None,
    ) -> None:
        env_config = config or create_reliability_config_from_env(Env())
        self._config = env_config
        self._task_runner = task_runner
        self._states: dict[str, BestEffortState] = {}
        self._lock = asyncio.Lock()
        self._deadline_task_token: str | None = None
        self._completion_handler = completion_handler

    async def create_state(
        self,
        job_id: str,
        min_dcs: int,
        deadline: float,
        target_dcs: set[str],
    ):
        """Create and store best-effort state for a job."""
        now = time.monotonic()
        effective_min_dcs = self._resolve_min_dcs(min_dcs, target_dcs)
        effective_deadline = self._resolve_deadline(deadline, now)
        state = BestEffortState(
            job_id=job_id,
            enabled=True,
            min_dcs=effective_min_dcs,
            deadline=effective_deadline,
            target_dcs=set(target_dcs),
        )
        async with self._lock:
            self._states[job_id] = state
        return state

    async def record_result(self, job_id: str, dc_id: str, success: bool):
        """Record a datacenter result for a job."""
        async with self._lock:
            state = self._states.get(job_id)
            if state is None:
                raise KeyError(f"Best-effort state missing for job {job_id}")
            state.record_dc_result(dc_id, success)

    async def check_all_completions(self):
        """Check all best-effort states for completion conditions."""
        now = time.monotonic()
        completions: list[tuple[str, str, bool]] = []
        async with self._lock:
            for job_id, state in self._states.items():
                should_complete, reason, success = state.check_completion(now)
                if should_complete:
                    completions.append((job_id, reason, success))

        return completions

    def start_deadline_loop(self):
        """Start periodic deadline checks using TaskRunner."""
        if self._deadline_task_token:
            return

        interval = self._config.best_effort_deadline_check_interval
        run = self._task_runner.run(
            self._deadline_check_loop,
            alias="best_effort_deadline_check",
            schedule=f"{interval}s",
            trigger="ON_START",
            repeat="ALWAYS",
        )
        if run is not None:
            self._deadline_task_token = run.token

    async def stop_deadline_loop(self):
        """Stop periodic deadline checks."""
        if not self._deadline_task_token:
            return

        await self._task_runner.cancel_schedule(self._deadline_task_token)
        self._deadline_task_token = None

    async def cleanup(self, job_id: str):
        """Remove best-effort state for a completed job."""
        async with self._lock:
            self._states.pop(job_id, None)

    async def shutdown(self):
        """Stop deadline checks and clear state."""
        await self.stop_deadline_loop()
        async with self._lock:
            self._states.clear()

    async def _deadline_check_loop(self):
        completions = await self.check_all_completions()
        if not completions or self._completion_handler is None:
            return

        for job_id, reason, success in completions:
            await self._completion_handler(job_id, reason, success)

    def _resolve_deadline(self, deadline: float, now: float):
        if deadline <= 0:
            return now + self._config.best_effort_deadline_default
        return min(deadline, now + self._config.best_effort_deadline_max)

    def _resolve_min_dcs(self, min_dcs: int, target_dcs: set[str]):
        requested = min_dcs if min_dcs > 0 else self._config.best_effort_min_dcs_default
        if not target_dcs:
            return 0
        return min(max(1, requested), len(target_dcs))
