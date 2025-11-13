import asyncio
import pathlib
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    TypeVar,
)

from .models import RunStatus, TaskType
from .run import Run
from .snowflake import SnowflakeGenerator
from .util import TimeParser

T = TypeVar("T")


class Task(Generic[T]):
    def __init__(
        self,
        snowflake_generator: SnowflakeGenerator,
        name: str,
        task: Callable[[], T] | str,
        executor: ProcessPoolExecutor | ThreadPoolExecutor | None,
        semaphore: asyncio.Semaphore,
        *args: tuple[Any, ...],
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
        repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
        timeout: int | float | str | None = None,
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
        task_type: TaskType = TaskType.CALLABLE,
    ) -> None:
        self._snowflake_generator = snowflake_generator
        self.task_id = snowflake_generator.generate()
        self.name: str = name
        self.args = args
        self.trigger: Literal["MANUAL", "ON_START"] = trigger
        self.repeat: Literal["NEVER", "ALWAYS"] | int = repeat

        self.schedule: int | float | None = None
        if schedule:
            self.schedule = TimeParser(schedule).time

        self.timeout: int | float | None = None
        if isinstance(timeout, str):
            self.timeout = TimeParser(timeout).time

        self.keep = keep

        self.max_age: float | None = None
        if max_age:
            self.max_age = TimeParser(max_age).time

        self.keep_policy = keep_policy

        self.call = task
        self.task_type = task_type

        self._runs: Dict[int, Run] = {}
        self._schedules: Dict[int, asyncio.Task] = {}
        self._schedule_running_statuses: Dict[int, bool] = defaultdict(lambda: False)

        keep = self.keep
        if keep is None:
            keep = 10

        self._sem = asyncio.Semaphore(keep)
        self._executor = executor
        self._executor_semaphore = semaphore

    @property
    def status(self):
        if run := self.latest():
            return run.status

        return RunStatus.IDLE

    async def get_run_update(self, run_id: int):
        return await self._runs[run_id].get_run_update()

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

    async def cancel_schedule(self, run_id: str):
        if run := self._runs.get(run_id) and self._schedules.get(run_id):
            self._schedule_running_statuses[run_id] = False

            try:
                self._schedules[run_id].set_result(None)

            except Exception:
                pass

            await run.cancel()

    async def shutdown(self):
        for run in self._runs.values():
            await run.cancel()

            if self._schedules.get(run.run_id):
                self._schedule_running_statuses[run.run_id] = False

                try:
                    self._schedules[run.run_id].set_result(None)

                except Exception:
                    pass

    def abort(self):
        for run in self._runs.values():
            run.abort()

            if self._schedules.get(run.run_id):
                self._schedule_running_statuses[run.run_id] = False

                try:
                    self._schedules[run.run_id].set_result(None)

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
        current_time = time.monotonic()
        for run_id, run in list(self._runs.items()):
            if current_time - run.start > self.max_age:
                removed_runs.append(run)
                del self._runs[run_id]

        if len(removed_runs) > 0:
            await asyncio.gather(
                *[run.cancel() for run in removed_runs], return_exceptions=True
            )

    def run_shell(
        self,
        *args: tuple[Any, ...],
        env: Dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
        shell: bool = False,
        run_id: Optional[str] = None,
        timeout: Optional[int | float] = None,
        poll_interval: int | float = 0.5,
    ):
        if timeout is None:
            timeout = self.timeout

        if run_id is None:
            run_id = self._snowflake_generator.generate()

        run = Run(
            run_id,
            self.name,
            self.call,
            TaskType.SHELL,
            self._executor,
            self._executor_semaphore,
            timeout=timeout,
        )

        run.execute_shell(
            *args,
            env=env,
            cwd=cwd,
            shell=shell,
            poll_interval=poll_interval,
        )

        self._runs[run.run_id] = run

        return run

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

        run = Run(
            run_id,
            self.name,
            self.call,
            TaskType.CALLABLE,
            self._executor,
            self._executor_semaphore,
            timeout=timeout,
        )

        run.execute(*args, **kwargs)

        self._runs[run.run_id] = run

        return run

    def stop_schedules(self):
        for run_id in self._schedule_running_statuses:
            self._schedule_running_statuses[run_id] = False

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

        if (
            self._schedules.get(run_id) is None
            and self._schedule_running_statuses[run_id] is False
        ):
            self._schedule_running_statuses[run_id] = True
            run = Run(
                run_id,
                self.name,
                self.call,
                TaskType.CALLABLE,
                self._executor,
                self._executor_semaphore,
                timeout=timeout,
            )

            self._schedules[run_id] = asyncio.ensure_future(
                self._run_schedule(run, *args, **kwargs)
            )

            return run

        return self.latest()

    def run_shell_schedule(
        self,
        *args: tuple[Any, ...],
        env: Dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
        shell: bool = False,
        run_id: Optional[str] = None,
        timeout: Optional[int | float] = None,
        poll_interval: int | float = 0.5,
    ):
        if run_id is None:
            run_id = self._snowflake_generator.generate()

        if timeout is None:
            timeout = self.timeout

        if (
            self._schedules.get(run_id) is None
            and self._schedule_running_statuses[run_id] is False
        ):
            self._schedule_running_statuses[run_id] = True
            run = Run(
                run_id,
                self.name,
                self.call,
                TaskType.SHELL,
                self._executor,
                self._executor_semaphore,
                timeout=timeout,
            )

            self._schedules[run_id] = asyncio.ensure_future(
                self._run_shell_schedule(
                    run,
                    *args,
                    env=env,
                    cwd=cwd,
                    shell=shell,
                    poll_interval=poll_interval,
                )
            )

            return run

        return self.latest()

    async def _run_schedule(self, run: Run, *args, **kwargs):
        self._runs[run.run_id] = run

        if self.repeat == "ALWAYS":
            while self._schedule_running_statuses[run.run_id]:
                run.execute(*args, **kwargs)

                await asyncio.sleep(self.schedule)
                run = Run(
                    self._snowflake_generator.generate(),
                    self.name,
                    self.call,
                    self._executor,
                    self._executor_semaphore,
                    timeout=self.timeout,
                )

                self._runs[run.run_id] = run
                self._schedule_running_statuses[run.run_id] = True

        elif isinstance(self.repeat, int):
            for _ in range(self.repeat):
                if self._schedule_running_statuses[run.run_id] is False:
                    await run.cancel()
                    break

                run.execute(*args, **kwargs)

                await asyncio.sleep(self.schedule)
                run = Run(
                    self._snowflake_generator.generate(),
                    self.name,
                    self.call,
                    self._executor,
                    self._executor_semaphore,
                    timeout=self.timeout,
                )

                self._runs[run.run_id] = run
                self._schedule_running_statuses[run.run_id] = True

    async def _run_shell_schedule(
        self,
        run: Run,
        *args: tuple[str, ...],
        env: Dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
        shell: bool = False,
        poll_interval: int | float = 0.5,
    ):
        self._runs[run.run_id] = run

        if self.repeat == "ALWAYS":
            while self._schedule_running_statuses[run.run_id]:
                run.execute_shell(
                    *args,
                    env=env,
                    cwd=cwd,
                    shell=shell,
                    poll_interval=poll_interval,
                )

                await asyncio.sleep(self.schedule)
                run = Run(
                    self._snowflake_generator.generate(),
                    self.name,
                    self.call,
                    self._executor,
                    self._executor_semaphore,
                    timeout=self.timeout,
                )

                self._runs[run.run_id] = run
                self._schedule_running_statuses[run.run_id] = True

        elif isinstance(self.repeat, int):
            for _ in range(self.repeat):
                if self._schedule_running_statuses[run.run_id] is False:
                    await run.cancel()
                    break

                run.execute_shell(
                    *args,
                    env=env,
                    cwd=cwd,
                    shell=shell,
                )

                await asyncio.sleep(self.schedule)
                run = Run(
                    self._snowflake_generator.generate(),
                    self.name,
                    self.call,
                    self._executor,
                    self._executor_semaphore,
                    timeout=self.timeout,
                )

                self._runs[run.run_id] = run
                self._schedule_running_statuses[run.run_id] = True

    async def _run(self, run: Run, *args, **kwargs):
        run.update_status(RunStatus.PENDING)

        async with self._sem:
            run.update_status(RunStatus.RUNNING)

            return await run.execute(*args, **kwargs)
