import asyncio
import pathlib
from types import MappingProxyType
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import (
    Awaitable,
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

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

FINISHED_RUN_STATUSES = frozenset((RunStatus.COMPLETE, RunStatus.CANCELLED, RunStatus.FAILED))

T = TypeVar("T")


class Task(Generic[T]):
    def __init__(
        self,
        name: str,
        task: Callable[..., Awaitable[T]] | str,
        executor: ProcessPoolExecutor | ThreadPoolExecutor | None,
        semaphore: asyncio.Semaphore,
        *args: object,
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
        repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
        timeout: int | float | str | None = None,
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
        task_type: TaskType = TaskType.CALLABLE,
        id_generator: SnowflakeGenerator,
    ) -> None:
        # Shared, monotone, deterministic id source owned and injected
        # by the constructing TaskRunner — required, so every Task in a
        # runner draws from ONE ordered stream (separate generators with
        # the same instance would collide at the same virtual
        # millisecond, and a module-level fallback would be hidden
        # mutable global state). Set before any id is drawn.
        self._id_generator = id_generator
        self.task_id = self.generate_id()
        self.name: str = name
        self.args = args
        self.trigger: Literal["MANUAL", "ON_START"] = trigger
        self.repeat: Literal["NEVER", "ALWAYS"] | int = repeat

        self.schedule: int | float | None = self._parse_duration(schedule)

        self.timeout: int | float | None = self._parse_timeout(timeout)

        # The finished runs retained (and the runs allowed at once); unset,
        # the task's default. Left None, every retention sweep raised on
        # ``len(...) > None`` and no run was ever released.
        self.keep: int = keep if keep is not None else 10

        self.max_age: float | None = self._parse_duration(max_age)

        self.keep_policy = keep_policy

        self.call = task
        self.task_type = task_type

        self._runs: Dict[int, Run[T]] = {}
        # A schedule is keyed by the id of its first run (the token its
        # caller holds); its later runs take fresh ids. Both maps hold a
        # schedule only while it runs -- it removes itself when it ends.
        self._schedules: Dict[int, asyncio.Task] = {}
        self._schedule_running_statuses: Dict[int, bool] = {}

        self._sem = asyncio.Semaphore(self.keep)
        self._executor = executor
        self._executor_semaphore = semaphore

    @staticmethod
    def _parse_duration(duration: str | None) -> int | float | None:
        """A duration string in seconds; None when none is given."""
        if duration:
            return TimeParser(duration).time
        return None

    @staticmethod
    def _parse_timeout(timeout: int | float | str | None) -> int | float | None:
        """A timeout string in seconds; any timeout not given as a string
        leaves the task without one."""
        if isinstance(timeout, str):
            return TimeParser(timeout).time
        return None

    @property
    def status(self):
        if run := self.latest():
            return run.status

        return RunStatus.IDLE
    
    def generate_id(self) -> int:
        # Snowflake id: monotone + totally ordered, so ``latest()`` /
        # ``max(self._runs)`` and the count-eviction ``sorted(...)`` are
        # correct (a random ``uuid4`` id broke that ordering). Clock-
        # seamed and drawing NO randomness, so it is deterministic under
        # SIM replay without perturbing the shared protocol RNG stream.
        return self._id_generator.generate_sync()

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

    async def cancel_schedule(self, run_id: int):
        """Stop the schedule ``run_id`` started: no further runs, and the
        run in flight cancelled (the schedule cancels it on its way out)."""
        if (schedule := self._schedules.get(run_id)) is None:
            return

        self._schedule_running_statuses[run_id] = False
        if not schedule.done():
            schedule.cancel()

    async def shutdown(self):
        self._cancel_all_schedules()

        for run in list(self._runs.values()):
            await run.cancel()

    def abort(self):
        self._cancel_all_schedules()

        for run in list(self._runs.values()):
            run.abort()

    def _cancel_all_schedules(self) -> None:
        """Stop every schedule's flag and cancel each one still running."""
        # Snapshots: schedules remove themselves as they end.
        for schedule_id, schedule in list(self._schedules.items()):
            self._schedule_running_statuses[schedule_id] = False
            if not schedule.done():
                schedule.cancel()

    async def cleanup(self):
        for retention_step in self._RETENTION_STEPS.get(self.keep_policy, ()):
            await retention_step(self)

    async def _execute_count_policy(self):
        # Release finished runs beyond the newest ``keep``. Retention never
        # cancels a run still working: the old policy cancelled the OLDEST
        # ``keep`` runs, live ones included.
        finished_run_ids = self._finished_run_ids()
        for run_id in finished_run_ids[: max(0, len(finished_run_ids) - self.keep)]:
            del self._runs[run_id]

    def _finished_run_ids(self) -> list[int]:
        """The finished runs' ids, oldest first."""
        return sorted(
            run_id
            for run_id, run in self._runs.items()
            if run.status in FINISHED_RUN_STATUSES
        )

    async def _execute_age_policy(self):
        # Release finished runs older than ``max_age``; a run's own timeout,
        # not retention, bounds how long it works.
        if self.max_age is None:
            return
        current_time = _DEFAULT_CLOCK.monotonic()
        for run_id in self._expired_run_ids(current_time):
            del self._runs[run_id]

    def _expired_run_ids(self, current_time: float) -> list[int]:
        """The finished runs older than ``max_age`` at ``current_time``."""
        return [
            run_id
            for run_id, run in self._runs.items()
            if self._finished_before(run, current_time)
        ]

    def _finished_before(self, run: Run[T], current_time: float) -> bool:
        """Whether ``run`` finished and started more than ``max_age`` ago."""
        return run.status in FINISHED_RUN_STATUSES and current_time - run.start > self.max_age

    # The retention steps each ``keep_policy`` runs, in order; an unknown
    # policy runs none.
    _RETENTION_STEPS = MappingProxyType(
        {
            "COUNT": (_execute_count_policy,),
            "AGE": (_execute_age_policy,),
            "COUNT_AND_AGE": (_execute_age_policy, _execute_count_policy),
        }
    )

    def run_shell(
        self,
        *args: str,
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
            run_id = self.generate_id()

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
            run_id = self.generate_id()

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
        """Let every schedule finish its current interval and stop."""
        for schedule_id in list(self._schedule_running_statuses):
            self._schedule_running_statuses[schedule_id] = False

    def _resolve_run_id(self, run_id: int | None) -> int:
        """The given run id, or a fresh one."""
        if run_id is None:
            run_id = self.generate_id()
        return run_id

    def _resolve_timeout(self, timeout: int | float | None) -> int | float | None:
        """The given timeout, or the task's own."""
        if timeout is None:
            timeout = self.timeout
        return timeout

    def _initial_remaining_runs(self) -> int | None:
        """How many runs a schedule makes: None when it repeats always."""
        return None if self.repeat == "ALWAYS" else self.repeat

    def _schedule_continues(self, schedule_id: int, remaining_runs: int | None):
        """Whether schedule ``schedule_id`` is still flagged to run and has
        runs left."""
        return self._schedule_running_statuses.get(schedule_id, False) and (
            remaining_runs is None or remaining_runs > 0
        )

    @staticmethod
    def _consume_run(remaining_runs: int | None) -> int | None:
        """The runs left after one more, for a bounded schedule."""
        if remaining_runs is not None:
            remaining_runs -= 1
        return remaining_runs

    def run_schedule(
        self,
        *args,
        run_id: Optional[str] = None,
        timeout: Optional[int | float] = None,
        **kwargs,
    ):
        run_id = self._resolve_run_id(run_id)
        timeout = self._resolve_timeout(timeout)

        if self._schedules.get(run_id) is None:
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
                self._run_schedule(run_id, run, *args, **kwargs)
            )

            return run

        return self.latest()

    def run_shell_schedule(
        self,
        *args: str,
        env: Dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
        shell: bool = False,
        run_id: Optional[str] = None,
        timeout: Optional[int | float] = None,
        poll_interval: int | float = 0.5,
    ):
        run_id = self._resolve_run_id(run_id)
        timeout = self._resolve_timeout(timeout)

        if self._schedules.get(run_id) is None:
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
                    run_id,
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

    async def _run_schedule(
        self,
        schedule_id: int,
        run: Run[T],
        *args,
        **kwargs,
    ):
        """Execute ``run``, then a fresh run every ``schedule`` seconds --
        always, or ``repeat`` times -- until the schedule is stopped.

        The schedule's own flag (``schedule_id``) decides each interval;
        keying it by the current run's id lost a stop the moment the first
        interval passed. Cancelled, it cancels the run in flight; ending
        any way, it releases its future and flag.
        """
        remaining_runs = self._initial_remaining_runs()
        try:
            while self._schedule_continues(schedule_id, remaining_runs):
                self._runs[run.run_id] = run
                run.execute(*args, **kwargs)
                remaining_runs = self._consume_run(remaining_runs)

                await _DEFAULT_CLOCK.sleep(self.schedule)
                run = Run(
                    self.generate_id(),
                    self.name,
                    self.call,
                    TaskType.CALLABLE,
                    self._executor,
                    self._executor_semaphore,
                    timeout=self.timeout,
                )

        except asyncio.CancelledError:
            await run.cancel()
            raise

        finally:
            self._schedules.pop(schedule_id, None)
            self._schedule_running_statuses.pop(schedule_id, None)

    async def _run_shell_schedule(
        self,
        schedule_id: int,
        run: Run[T],
        *args: tuple[str, ...],
        env: Dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
        shell: bool = False,
        poll_interval: int | float = 0.5,
    ):
        """``_run_schedule`` for a shell command."""
        remaining_runs = self._initial_remaining_runs()
        try:
            while self._schedule_continues(schedule_id, remaining_runs):
                self._runs[run.run_id] = run
                run.execute_shell(
                    *args,
                    env=env,
                    cwd=cwd,
                    shell=shell,
                    poll_interval=poll_interval,
                )
                remaining_runs = self._consume_run(remaining_runs)

                await _DEFAULT_CLOCK.sleep(self.schedule)
                run = Run(
                    self.generate_id(),
                    self.name,
                    self.call,
                    TaskType.SHELL,
                    self._executor,
                    self._executor_semaphore,
                    timeout=self.timeout,
                )

        except asyncio.CancelledError:
            await run.cancel()
            raise

        finally:
            self._schedules.pop(schedule_id, None)
            self._schedule_running_statuses.pop(schedule_id, None)

    async def _run(self, run: Run[T], *args, **kwargs):
        run.update_status(RunStatus.PENDING)

        async with self._sem:
            run.update_status(RunStatus.RUNNING)

            return await run.execute(*args, **kwargs)
