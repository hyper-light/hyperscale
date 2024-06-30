import asyncio
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

from hyperscale.core_rewrite.snowflake.snowflake_generator import SnowflakeGenerator

from .cancel import cancel
from .models import RunStatus
from .run import Run

T = TypeVar("T")


class Task(Generic[T]):
    def __init__(
        self, task: Callable[[], T], snowflake_generator: SnowflakeGenerator
    ) -> None:
        self.task_id = snowflake_generator.generate()
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
        self._schedule: Optional[asyncio.Task] = None
        self._schedule_running: bool = False

        self._snowflake_generator = snowflake_generator

        keep = self.keep
        if keep is None:
            keep = 10

        self._sem = asyncio.Semaphore(keep)

    @property
    def status(self):
        if run := self.latest():
            return run.status

        return RunStatus.IDLE

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

    async def cancel(self, run_id: str):
        if run := self._runs.get(run_id):
            await run.cancel()

    async def cancel_schedule(self):
        self._schedule_running = False
        await cancel(self._schedule)

    async def shutdown(self):
        for run in self._runs.values():
            await run.cancel()

        await self.cancel_schedule()

    def abort(self):
        for run in self._runs.values():
            run.abort()

        self._schedule_running = False

        try:

            self._schedule.cancel()

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
            await asyncio.gather(*[run.cancel() for run in removed_runs])

    async def _execute_age_policy(self):
        removed_runs: List[Run] = []
        current_time = time.monotonic()
        for run_id, run in list(self._runs.items()):
            if current_time - run.start > self.max_age:
                removed_runs.append(run)
                del self._runs[run_id]

        if len(removed_runs) > 0:
            await asyncio.gather(*[run.cancel() for run in removed_runs])

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
            run_id = self._snowflake_generator.generate()

        run = Run(run_id, self.call, timeout=timeout)

        run.execute(*args, **kwargs)

        self._runs[run.run_id] = run

        return run

    def stop(self):
        self._schedule_running = False

    def run_schedule(
        self,
        *args,
        run_id: Optional[str] = None,
        timeout: Optional[int | float] = None,
        **kwargs,
    ):
        if run_id is None:
            run_id = self._snowflake_generator.generate()

        if timeout is None:
            timeout = self.timeout

        if self._schedule is None and self._schedule_running is False:
            self._schedule_running = True

            run = Run(run_id, self.call, timeout=timeout)

            self._schedule = asyncio.create_task(
                self._run_schedule(run, *args, **kwargs)
            )

            return run

        return self.latest()

    async def _run_schedule(self, run: Run, *args, **kwargs):
        self._runs[run.run_id] = run

        if self.repeat == "ALWAYS":
            while self._schedule_running:
                run.execute(*args, **kwargs)

                await asyncio.sleep(self.schedule)
                run = Run(
                    self._snowflake_generator.generate(),
                    self.call,
                    timeout=self.timeout,
                )

                self._runs[run.run_id] = run

        elif isinstance(self.repeat, int):
            for _ in range(self.repeat):
                if self._schedule_running is False:
                    await run.cancel()
                    break

                run.execute(*args, **kwargs)

                await asyncio.sleep(self.schedule)
                run = Run(
                    self._snowflake_generator.generate(),
                    self.call,
                    timeout=self.timeout,
                )

                self._runs[run.run_id] = run

    async def _run(self, run: Run, *args, **kwargs):
        run.update_status(RunStatus.PENDING)

        async with self._sem:
            run.update_status(RunStatus.RUNNING)

            return await run.execute(*args, **kwargs)
