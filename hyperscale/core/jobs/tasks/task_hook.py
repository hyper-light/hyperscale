import asyncio
from collections import defaultdict
import time
from typing import (
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    TypeVar,
)

from hyperscale.core.snowflake.snowflake_generator import SnowflakeGenerator

from .cancel import cancel

# SIM seam — MUST share Run.start's monotonic axis (see run.py): age =
# now - run.start is only meaningful when both readings come from the
# same clock.
_DEFAULT_MONOTONIC_SOURCE = time.monotonic
from .models import RunStatus
from .run import Run

T = TypeVar("T")


class Task(Generic[T]):
    def __init__(
        self,
        task: Callable[[], T],
        id_generator: SnowflakeGenerator,
    ) -> None:
        # Shared, monotone id source owned and injected by the
        # constructing TaskRunner — required, so every Task in a runner
        # draws from ONE ordered stream (separate generators with the
        # same instance would collide at the same millisecond, and a
        # module-level fallback would be hidden mutable global state).
        # Set before any id is drawn.
        self._id_generator = id_generator
        self.task_id = self.create_id()
        self.name: str = task.name
        self.schedule: Optional[int | float] = task.schedule
        self.trigger: Literal["MANUAL", "ON_START"] = task.trigger
        self.repeat: Literal["NEVER", "ALWAYS"] | int = task.repeat
        self.timeout: Optional[int | float] = task.timeout
        self.keep: Optional[int] = task.keep
        self.max_age: Optional[float] = task.max_age
        self.keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = task.keep_policy

        self.call = task
        self._runs: Dict[int, Run] = {}
        self._schedules: Dict[int, asyncio.Task] = {}
        self._schedule_running_statuses: Dict[int, bool] = defaultdict(lambda: False)

        keep = self.keep
        if keep is None:
            keep = 10

        self._sem = asyncio.Semaphore(keep)

    @property
    def status(self):
        if run := self.latest():
            return run.status

        return RunStatus.IDLE
    
    def create_id(self) -> int:
        # Snowflake id: monotone + totally ordered, so ``latest()`` /
        # ``max(self._runs)`` and the count-eviction ``sorted(...)`` are
        # correct (a random ``uuid4`` id broke that ordering — latest()
        # could return a stale run and eviction could drop the newest).
        return self._id_generator.generate()


    def get_run_status(self, run_id: str):
        if run := self._runs.get(run_id):
            return run.status

    def latest(self):
        if len(self._runs) > 0:
            latest_run_id = max(self._runs)
            return self._runs[latest_run_id]

    async def update(self, run_id: str, status: RunStatus):
        if run := self._runs.get(run_id):
            run.update_status(status)
            self._runs[run_id] = run

    async def complete(self, run_id: str):
        if run := self._runs.get(run_id):
            return await run.complete()

    async def cancel(self, run_id: str, timeout: float = 5.0):
        if run := self._runs.get(run_id):
            await run.cancel(timeout=timeout)

    async def cancel_schedule(self):
        # Snapshot to avoid dict mutation during iteration
        for run_id in list(self._schedule_running_statuses.keys()):
            self._schedule_running_statuses[run_id] = False
        await asyncio.gather(*[
            cancel(scheduled) for scheduled in list(self._schedules.values())
        ])

    async def shutdown(self):
        # Snapshot to avoid dict mutation during iteration
        for run in list(self._runs.values()):
            await run.cancel()

        await self.cancel_schedule()

    def abort(self):
        # Snapshot to avoid dict mutation during iteration
        for run in list(self._runs.values()):
            run.abort()

            self._schedule_running_statuses[run.run_id] = False

            schedule = self._schedules.get(run.run_id)
            if schedule and not schedule.done():
                try:
                    schedule.cancel()
                except Exception:
                    pass

    async def cleanup(self):
        match self.keep_policy:
            case "COUNT":
                await self._execute_count_policy()

            case "AGE":
                await self._execute_age_policy()

            case "COUNT_AND_AGE":
                await self._execute_age_policy()
                await self._execute_count_policy()

            case _:
                pass

    async def _execute_count_policy(self):
        removed_runs: List[Run] = []
        if len(self._runs) > self.keep:
            run_ids = list(sorted(self._runs))
            for run_id in run_ids[: self.keep]:
                removed_runs.append(self._runs[run_id])
                del self._runs[run_id]

        if len(removed_runs) > 0:
            await asyncio.gather(
                *[run.cancel() for run in removed_runs], return_exceptions=True
            )

    async def _execute_age_policy(self):
        removed_runs: List[Run] = []
        current_time = _DEFAULT_MONOTONIC_SOURCE()
        for run_id, run in list(self._runs.items()):
            if current_time - run.start > self.max_age:
                removed_runs.append(run)
                del self._runs[run_id]

        if len(removed_runs) > 0:
            await asyncio.gather(
                *[run.cancel() for run in removed_runs], return_exceptions=True
            )

    def run(
        self,
        *args,
        run_id: Optional[str] = None,
        timeout: Optional[int | float] = None,
        **kwargs,
    ):
        if timeout is None:
            timeout = self.timeout

        if run_id is None:
            run_id = self.create_id()
            
        run = Run(run_id, self.call, timeout=timeout)

        run.execute(*args, **kwargs)

        self._runs[run.run_id] = run

        return run

    def stop(self, run_id: Optional[str] = None):
        """Stop scheduled repetition — every schedule of this task, or
        just the one started under ``run_id`` when given (concurrent
        runs of one task must not stop each other's schedules: one
        workflow completing used to kill every workflow's status
        pusher on the node)."""
        if run_id is not None:
            self._schedule_running_statuses[run_id] = False
            return
        # Snapshot to avoid dict mutation during iteration
        for run_id in list(self._schedule_running_statuses.keys()):
            self._schedule_running_statuses[run_id] = False

    def run_schedule(
        self,
        *args,
        run_id: Optional[str] = None,
        timeout: Optional[int | float] = None,
        **kwargs,
    ):
        if run_id is None:
            run_id = self.create_id()

        if timeout is None:
            timeout = self.timeout

        if self._schedules.get(run_id) is None and self._schedule_running_statuses[run_id] is False:
            self._schedule_running_statuses[run_id] = True
            run = Run(run_id, self.call, timeout=timeout)

            self._schedules[run_id] = asyncio.ensure_future(
                self._run_schedule(run, *args, **kwargs)
            )

            return run

        return self.latest()

    async def _run_schedule(self, run: Run, *args, **kwargs):
        self._runs[run.run_id] = run

        # The loop's control key is the ORIGINAL schedule id — the one
        # ``run_schedule`` registered and the only one ``stop`` can
        # know about. The loop rebinds ``run`` to a fresh id every
        # iteration for per-execution bookkeeping; checking THAT id
        # (the old code) made every ALWAYS schedule unstoppable: stop
        # flipped the ids existing at stop time, then the next
        # iteration minted a fresh id set True and the loop checked
        # the fresh one. Every completed workflow leaked its 10-20Hz
        # status pollers forever — the measured per-worker cumulative
        # drag (wall cost per virtual decade growing linearly with
        # jobs served, resetting only on worker death) plus unbounded
        # growth of ``_runs`` / ``_schedule_running_statuses``.
        schedule_control_id = run.run_id

        if self.repeat == "ALWAYS":
            while self._schedule_running_statuses[schedule_control_id]:
                run.execute(*args, **kwargs)

                await asyncio.sleep(self.schedule)
                previous_run_id = run.run_id
                run = Run(
                    self.create_id(),
                    self.call,
                    timeout=self.timeout,
                )

                self._runs[run.run_id] = run
                # Per-iteration status keys are never the control key;
                # drop the previous iteration's entry so the dict holds
                # O(active schedules), not O(iterations ever).
                if previous_run_id != schedule_control_id:
                    self._schedule_running_statuses.pop(previous_run_id, None)
            self._schedule_running_statuses.pop(schedule_control_id, None)

        elif isinstance(self.repeat, int):
            for _ in range(self.repeat):
                if self._schedule_running_statuses[schedule_control_id] is False:
                    await run.cancel()
                    break

                run.execute(*args, **kwargs)

                await asyncio.sleep(self.schedule)
                previous_run_id = run.run_id
                run = Run(
                    self.create_id(),
                    self.call,
                    timeout=self.timeout,
                )

                self._runs[run.run_id] = run
                if previous_run_id != schedule_control_id:
                    self._schedule_running_statuses.pop(previous_run_id, None)
            self._schedule_running_statuses.pop(schedule_control_id, None)

    async def _run(self, run: Run, *args, **kwargs):
        run.update_status(RunStatus.PENDING)

        async with self._sem:
            run.update_status(RunStatus.RUNNING)

            return await run.execute(*args, **kwargs)
