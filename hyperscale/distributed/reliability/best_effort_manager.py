"""
Best-effort completion manager (AD-44).
"""

import asyncio
from typing import Awaitable, Callable

from hyperscale.distributed.runtime import Clock, Runner

from .models.best_effort_decision import BestEffortDecision
from .best_effort_state import BestEffortState
from .late_result_policy import LateResultPolicy
from .reliability_config import ReliabilityConfig

CompletionHandler = Callable[[str, BestEffortDecision], Awaitable[None]]


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

    Under the ``update`` late-result policy a job reaching ``min_dcs``
    before its deadline, with datacenters still unreported, is released
    provisionally (its decision says so, once): its state stays, and it
    completes for good when every datacenter reported or the deadline
    passed.
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
        now = self._clock.monotonic()
        async with self._lock:
            if (state := self._states.get(job_id)) is None:
                return None
            state.record_dc_result(dc_id, success)
            return self._decide(state, now)

    async def check_all_completions(self) -> list[tuple[str, BestEffortDecision]]:
        """Every tracked job whose completion conditions now hold."""
        now = self._clock.monotonic()
        async with self._lock:
            return [
                (job_id, decision)
                for job_id, state in self._states.items()
                if (decision := self._decide(state, now)).should_complete
            ]

    def is_released(self, job_id: str) -> bool:
        """True while the job's result is out provisionally (``update``
        policy) and it has not completed for good."""
        return (state := self._states.get(job_id)) is not None and state.released

    def remaining_seconds(self, job_id: str) -> float:
        """Seconds until the job's deadline (0.0 when passed or untracked)."""
        if (state := self._states.get(job_id)) is None:
            return 0.0
        return max(state.deadline - self._clock.monotonic(), 0.0)

    async def restore_release(
        self,
        job_id: str,
        target_dcs: set[str],
        datacenter_outcomes: dict[str, bool],
        release_reason: str,
        remaining_seconds: float,
    ) -> None:
        """Track a job whose result went out provisionally before this
        gate led it (a restart or a takeover): released for
        ``release_reason``, the datacenters that reported then with their
        outcomes (True: completed), and its deadline ``remaining_seconds``
        from now (0.0: passed -- the next deadline check completes it)."""
        state = BestEffortState(
            job_id=job_id,
            enabled=True,
            min_dcs=len(datacenter_outcomes),
            deadline=self._clock.monotonic() + max(remaining_seconds, 0.0),
            target_dcs=set(target_dcs),
            released=True,
            release_reason=release_reason,
        )
        for dc_id, success in datacenter_outcomes.items():
            state.record_dc_result(dc_id, success)
        async with self._lock:
            self._states[job_id] = state

    def completion_ratio(self, job_id: str) -> float:
        """Share of the job's target datacenters that completed (0.0 when untracked)."""
        if (state := self._states.get(job_id)) is None:
            return 0.0
        return state.get_completion_ratio()

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
            for job_id, decision in await self.check_all_completions():
                await self._completion_handler(job_id, decision)

    def _decide(self, state: BestEffortState, now: float) -> BestEffortDecision:
        """The job's decision now; a provisional one marks the state
        released (caller holds the lock), so it is handed out once."""
        holds_stragglers = state.awaits_stragglers(now) and self._holds_stragglers
        should_complete, reason, success = state.check_completion(now)
        provisional = should_complete and holds_stragglers
        state.mark_released(provisional, reason)
        return BestEffortDecision(should_complete, reason, success, provisional)

    @property
    def _holds_stragglers(self) -> bool:
        return self._config.best_effort_late_result_policy is LateResultPolicy.UPDATE

    def _resolve_deadline_seconds(self, deadline_seconds: float) -> float:
        if deadline_seconds <= 0:
            return self._config.best_effort_deadline_default
        return min(deadline_seconds, self._config.best_effort_deadline_max)

    def _resolve_min_dcs(self, min_dcs: int, target_dcs: set[str]) -> int:
        requested = min_dcs if min_dcs > 0 else self._config.best_effort_min_dcs_default
        if not target_dcs:
            return 0
        return min(max(1, requested), len(target_dcs))
