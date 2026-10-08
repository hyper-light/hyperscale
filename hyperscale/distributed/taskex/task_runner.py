import asyncio
import functools
import shlex
import signal
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from types import FrameType
from typing import (
    Awaitable,
    Callable,
    Dict,
    Literal,
    Optional,
    TypeVar,
)


from hyperscale.distributed.env import Env
from .models import RunStatus, ShellProcess, TaskRun, TaskType
from .snowflake import SnowflakeGenerator
from .snowflake.constants import MAX_INSTANCE
from .task import Task
from .util.time_parser import TimeParser

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

T = TypeVar("T")


def shutdown_executor(
    sig: int,
    executor: ThreadPoolExecutor | ProcessPoolExecutor | None,
    default_handler: Callable[[int, FrameType | None], object] | int | signal.Handlers | None,
):
    if executor:
        executor.shutdown(cancel_futures=True)

    signal.signal(sig, default_handler)


class TaskRunner:
    def __init__(
        self,
        instance_id: int | None = None,
        config: Env | None = None,
        executor_type: Literal['thread', 'process', 'disabled'] = 'disabled',
    ) -> None:
        if instance_id is None:
            instance_id = 0

        if config is None:
            config = Env()

        self.instance_id = instance_id
        # One monotone, clock-seamed snowflake generator for this runner,
        # shared by every Task it builds so all task/run ids come from a
        # single ordered stream (distinct generators with the same
        # instance would collide at the same virtual millisecond). The
        # instance field is masked to the snowflake's bit width; callers
        # pass distinct ``instance_id``s for cross-process uniqueness.
        self._id_generator = SnowflakeGenerator(
            instance=self.instance_id & MAX_INSTANCE
        )
        self.tasks: Dict[str, Task[object]] = {}
        self.results: Dict[str, object] = {}
        self._cleanup_interval = TimeParser(config.MERCURY_SYNC_CLEANUP_INTERVAL).time
        self._cleanup_task: Optional[asyncio.Task] = None
        self._run_cleanup: bool = False

        self._executor: ThreadPoolExecutor | ProcessPoolExecutor | None = self._create_executor(
            executor_type, config
        )

        self._executor_semaphore = asyncio.Semaphore(
            value=config.MERCURY_SYNC_TASK_RUNNER_MAX_THREADS
        )
        self._loop = asyncio.get_event_loop()

        # Skip list - tasks in this set will not be run or scheduled
        self._skipped_tasks: set[str] = set()

    @staticmethod
    def _create_executor(
        executor_type: Literal['thread', 'process', 'disabled'],
        config: Env,
    ) -> ThreadPoolExecutor | ProcessPoolExecutor | None:
        """The pool ``executor_type`` names, or None when disabled."""
        if executor_type == "thread":
            return ThreadPoolExecutor(
                max_workers=config.MERCURY_SYNC_TASK_RUNNER_MAX_THREADS
            )

        if executor_type == 'process':
            return ProcessPoolExecutor(
                max_workers=config.MERCURY_SYNC_TASK_RUNNER_MAX_THREADS
            )

        return None

    def all_tasks(self):
        # Snapshot to avoid dict mutation during iteration
        for task in list(self.tasks.values()):
            yield task

    def start_cleanup(self):
        self._run_cleanup = True
        self._cleanup_task = asyncio.ensure_future(self._cleanup())

    def create_task_id(self):
        # Deterministic, monotone snowflake id from this runner's shared
        # generator (was ``uuid.uuid4().int >> 64`` — random, so
        # non-deterministic under SIM and non-ordered).
        return self._id_generator.generate_sync()

    def skip_tasks(self, task_names: list[str]) -> None:
        """
        Add tasks to the skip list. Skipped tasks will not be run or scheduled.

        Also stops any running schedules for these tasks.

        Args:
            task_names: List of task names (or aliases) to skip
        """
        for name in task_names:
            self._skipped_tasks.add(name)
            # Stop any running schedules for this task
            task = self.tasks.get(name)
            if task:
                task.stop_schedules()

    def unskip_tasks(self, task_names: list[str]) -> None:
        """
        Remove tasks from the skip list, allowing them to run again.

        Args:
            task_names: List of task names (or aliases) to unskip
        """
        for name in task_names:
            self._skipped_tasks.discard(name)

    def is_skipped(self, task_name: str) -> bool:
        """Check if a task is in the skip list."""
        return task_name in self._skipped_tasks

    def bundle(
        self,
        call: Callable[..., Awaitable[T]],
        *args: object,
        **kwargs: object,
    ):
        return functools.partial(
            call,
            *args,
            **kwargs,
        )

    def run(
        self,
        call: Callable[..., Awaitable[object]],
        *args: object,
        alias: str | None = None,
        run_id: int | None = None,
        timeout: str | int | float | None = None,
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
        repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
        **kwargs: object,
    ):
        if isinstance(timeout, str):
            timeout = TimeParser(timeout).time

        if self._cleanup_task is None:
            self.start_cleanup()

        command_name = alias
        if command_name is None and isinstance(call, functools.partial):
            command_name = call.func.__name__

        elif command_name is None:
            command_name = call.__name__

        # Check if task is skipped - return None without running
        if command_name in self._skipped_tasks:
            return None

        task = self.tasks.get(command_name)
        if task is None and call:
            task = Task(
                command_name,
                call,
                self._executor,
                self._executor_semaphore,
                schedule=schedule,
                trigger=trigger,
                repeat=repeat,
                keep=keep,
                max_age=max_age,
                keep_policy=keep_policy,
                id_generator=self._id_generator,
            )

            self.tasks[command_name] = task

        if task and task.repeat == "NEVER":
            return task.run(
                *args,
                **kwargs,
                run_id=run_id,
                timeout=timeout,
            )

        elif task and task.schedule:
            return task.run_schedule(
                *args,
                **kwargs,
                run_id=run_id,
                timeout=timeout,
            )

    def command(
        self,
        command: str,
        *args: str,
        alias: str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        shell: bool = False,
        run_id: int | None = None,
        timeout: str | int | float | None = None,
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
        repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
    ):
        self._ensure_cleanup_started()

        command_name = command if alias is None else alias

        # Check if task is skipped - return None without running
        if command_name in self._skipped_tasks:
            return None

        timeout = self._parse_timeout(timeout)
        args = self._quote_shell_args(args, shell)

        task = self._get_or_create_shell_task(
            command_name,
            command,
            schedule=schedule,
            trigger=trigger,
            repeat=repeat,
            keep=keep,
            max_age=max_age,
            keep_policy=keep_policy,
        )

        return self._start_shell_task(
            task,
            args,
            env=env,
            cwd=cwd,
            shell=shell,
            run_id=run_id,
            timeout=timeout,
        )

    def _ensure_cleanup_started(self) -> None:
        """Start the retention sweep on first use."""
        if self._cleanup_task is None:
            self.start_cleanup()

    @staticmethod
    def _parse_timeout(timeout: str | int | float | None) -> int | float | None:
        """A timeout given as a duration string, in seconds."""
        if isinstance(timeout, str):
            timeout = TimeParser(timeout).time
        return timeout

    @staticmethod
    def _quote_shell_args(args: tuple[str, ...], shell: bool) -> tuple[str, ...] | list[str]:
        """Shell-quote each argument when the command runs through a shell."""
        if shell:
            args = [shlex.quote(arg) for arg in args]
        return args

    def _get_or_create_shell_task(
        self,
        command_name: str,
        command: str,
        schedule: str | None,
        trigger: Literal["MANUAL", "ON_START"],
        repeat: Literal["NEVER", "ALWAYS"] | int,
        keep: int | None,
        max_age: str | None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"],
    ) -> Task[object]:
        """The shell task registered as ``command_name``, made on first use."""
        task = self.tasks.get(command_name)
        if task is None:
            task = Task(
                command_name,
                command,
                self._executor,
                self._executor_semaphore,
                schedule=schedule,
                trigger=trigger,
                repeat=repeat,
                keep=keep,
                max_age=max_age,
                keep_policy=keep_policy,
                task_type=TaskType.SHELL,
                id_generator=self._id_generator,
            )

            self.tasks[command_name] = task

        return task

    def _start_shell_task(
        self,
        task: Task[object],
        args: tuple[str, ...] | list[str],
        env: dict[str, str] | None,
        cwd: str | None,
        shell: bool,
        run_id: int | None,
        timeout: int | float | None,
    ):
        """Run ``task`` once, or start its schedule; a repeating task
        without a schedule starts nothing. (A Task is always truthy, so
        the former ``task and`` guards never decided anything.)"""
        if task.repeat == "NEVER":
            return task.run_shell(
                *args,
                env=env,
                cwd=cwd,
                shell=shell,
                run_id=run_id,
                timeout=timeout,
                poll_interval=self._cleanup_interval,
            )

        if task.schedule:
            return task.run_shell_schedule(
                *args,
                env=env,
                cwd=cwd,
                shell=shell,
                run_id=run_id,
                timeout=timeout,
            )

    async def wait_all(
        self,
        tokens: list[str],
        timeout: float | None = None,
    ):
        """
        Wait for multiple task runs to complete.

        Args:
            tokens: List of task run tokens.
            timeout: Maximum time to wait for ALL tasks in seconds. None means wait forever.

        Returns:
            List of completed task run results.

        Raises:
            asyncio.TimeoutError: If timeout is exceeded before all tasks complete.
        """
        return await asyncio.gather(
            *[self.wait(token, timeout=timeout) for token in tokens],
        )

    async def wait(
        self,
        token: str,
        timeout: float | None = None,
    ) -> ShellProcess | TaskRun:
        """
        Wait for a task run to complete.

        Args:
            token: The task run token (format: "task_name:run_id")
            timeout: Maximum time to wait in seconds. None means wait forever.

        Returns:
            The completed task run result.

        Raises:
            asyncio.TimeoutError: If timeout is exceeded before task completes.
            KeyError: If task or run doesn't exist.
        """
        task_name, run_id_str = token.rsplit(":", maxsplit=1)
        run_id = int(run_id_str)

        start_time = asyncio.get_event_loop().time()

        update = await self.tasks[task_name].get_run_update(run_id)
        while update.status not in [
            RunStatus.COMPLETE,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
        ]:
            self._raise_if_wait_timed_out(token, timeout, start_time)

            await _DEFAULT_CLOCK.sleep(self._cleanup_interval)
            update = await self.tasks[task_name].get_run_update(run_id)

        return await self.tasks[task_name].complete(run_id)

    @staticmethod
    def _raise_if_wait_timed_out(token: str, timeout: float | None, start_time: float) -> None:
        """Raise ``asyncio.TimeoutError`` once a bounded wait has run
        ``timeout`` seconds since ``start_time``."""
        if timeout is not None:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                raise asyncio.TimeoutError(
                    f"Timeout waiting for task {token} after {timeout}s"
                )

    async def get_task_update(self, token: str):
        task_name, run_id = token.rsplit(":", maxsplit=1)
        return await self.tasks[task_name].get_run_update(
            int(run_id),
        )

    def stop_schedules(
        self,
        task_name: str,
    ):
        task = self.tasks.get(task_name)
        if task:
            task.stop_schedules()

    def get_task_status(self, task_name: str):
        if task := self.tasks.get(task_name):
            return task.status

    def get_run_status(self, token: str):
        task_name, run_id = token.rsplit(":", maxsplit=1)

        if task := self.tasks.get(task_name):
            return task.get_run_status(int(run_id))

    async def complete(self, token: str):
        task_name, run_id = token.rsplit(":", maxsplit=1)

        if task := self.tasks.get(task_name):
            return await task.complete(int(run_id))

    async def cancel(self, token: str):
        task_name, run_id = token.rsplit(":", maxsplit=1)

        task = self.tasks.get(task_name)
        if task:
            await task.cancel(int(run_id))

    async def cancel_schedule(
        self,
        token: str,
    ):
        task_name, run_id = token.rsplit(":", maxsplit=1)

        task = self.tasks.get(task_name)
        if task:
            await task.cancel_schedule(int(run_id))

    async def stop(self):
        # Snapshot to avoid dict mutation during iteration
        for task in list(self.tasks.values()):
            await task.shutdown()

    async def shutdown(self):
        await self._shutdown_tasks()

        self._run_cleanup = False

        # Cancel and AWAIT the cleanup task. The previous version only yielded
        # control once via `_DEFAULT_CLOCK.sleep(0)` after cancel, which is not
        # enough — the cancelled task may still be alive when shutdown
        # returns and surfaces as a leaked asyncio task.
        if self._cleanup_task_started_and_unfinished():
            self._cleanup_task.cancel()
            cancels_requested_before_wait = asyncio.current_task().cancelling()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                # The task we cancelled ended; a cancel aimed at this task
                # while it waited goes on.
                self._reraise_if_cancel_aimed_at_caller(cancels_requested_before_wait)
            except Exception:
                pass

        if self._executor:
            try:
                self._executor.shutdown(cancel_futures=True)

            except Exception:
                pass

    async def _shutdown_tasks(self) -> None:
        """Shut every task down."""
        # Snapshot to avoid dict mutation during iteration
        for task in list(self.tasks.values()):
            await task.shutdown()

    def _cleanup_task_started_and_unfinished(self) -> bool:
        """Whether the retention sweep was started and has not ended."""
        return self._cleanup_task is not None and not self._cleanup_task.done()

    @staticmethod
    def _reraise_if_cancel_aimed_at_caller(cancels_requested_before_wait: int) -> None:
        """Re-raise the CancelledError being handled when the current task
        was itself cancelled since ``cancels_requested_before_wait``."""
        if asyncio.current_task().cancelling() > cancels_requested_before_wait:
            raise

    def abort(self):
        self._abort_tasks()

        self._run_cleanup = False

        if self._cleanup_task_unfinished():
            try:
                self._cleanup_task.cancel()
            except Exception:
                pass

        if self._executor:
            try:
                self._executor.shutdown(cancel_futures=True)

            except Exception:
                pass

    def _abort_tasks(self) -> None:
        """Abort every task."""
        # Snapshot to avoid dict mutation during iteration
        for task in list(self.tasks.values()):
            task.abort()

    def _cleanup_task_unfinished(self) -> bool:
        """Whether a retention sweep was started and has not ended."""
        return bool(self._cleanup_task and not self._cleanup_task.done())

    async def _cleanup(self):
        while self._run_cleanup:
            await self._cleanup_scheduled_tasks()
            await _DEFAULT_CLOCK.sleep(self._cleanup_interval)

    async def _cleanup_scheduled_tasks(self):
        # Every task is swept even when one fails; the failures then raise
        # together (they once aborted the sweep, silently, at the first).
        cleanup_errors: list[Exception] = []
        # Snapshot to avoid dict mutation during iteration
        for task in list(self.tasks.values()):
            await self._sweep_task(task, cleanup_errors)
        if cleanup_errors:
            raise ExceptionGroup("task retention sweep failed", cleanup_errors)

    @staticmethod
    async def _sweep_task(task: Task[object], cleanup_errors: list[Exception]) -> None:
        """Run one task's retention sweep, collecting its failure so the
        rest of the sweep still runs."""
        try:
            await task.cleanup()
        except Exception as cleanup_error:
            cleanup_errors.append(cleanup_error)
