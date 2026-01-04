import asyncio
import functools
import shlex
import signal
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Literal,
    Optional,
    TypeVar,
)


from hyperscale.distributed_rewrite.env import Env
from .models import RunStatus, ShellProcess, TaskRun, TaskType
from .snowflake import SnowflakeGenerator
from .task import Task
from .util.time_parser import TimeParser

T = TypeVar("T")


def shutdown_executor(
    sig: int,
    executor: ThreadPoolExecutor | ProcessPoolExecutor | None,
    default_handler: Callable[..., Any],
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

        self.tasks: Dict[str, Task[Any]] = {}
        self.results: Dict[str, Any] = {}
        self._cleanup_interval = TimeParser(config.MERCURY_SYNC_CLEANUP_INTERVAL).time
        self._cleanup_task: Optional[asyncio.Task] = None
        self._run_cleanup: bool = False
        self._snowflake_generator = SnowflakeGenerator(instance_id)

        self._executor: ThreadPoolExecutor | ProcessPoolExecutor | None = None
        if executor_type == "thread":
            self._executor = ThreadPoolExecutor(
                max_workers=config.MERCURY_SYNC_TASK_RUNNER_MAX_THREADS
            )

        elif executor_type == 'process':
            self._executor = ProcessPoolExecutor(
                max_workers=config.MERCURY_SYNC_TASK_RUNNER_MAX_THREADS
            )

        self._executor_semaphore = asyncio.Semaphore(
            value=config.MERCURY_SYNC_TASK_RUNNER_MAX_THREADS
        )
        self._loop = asyncio.get_event_loop()


    def all_tasks(self):
        # Snapshot to avoid dict mutation during iteration
        for task in list(self.tasks.values()):
            yield task

    def start_cleanup(self):
        self._run_cleanup = True
        self._cleanup_task = asyncio.ensure_future(self._cleanup())

    def create_task_id(self):
        return self._snowflake_generator.generate()
    
    def bundle(
        self,
        call: Callable[..., Awaitable[T]],
        *args: tuple[Any, ...],
        **kwargs: dict[str, Any],
    ):
        return functools.partial(
            call,
            *args,
            **kwargs,
        )

    def run(
        self,
        call: Callable[..., Awaitable[T]],
        *args,
        alias: str | None = None,
        run_id: int | None = None,
        timeout: str | int | float | None = None,
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
        repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
        **kwargs,
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

        task = self.tasks.get(command_name)
        if task is None and call:
            task = Task(
                self._snowflake_generator,
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
            )

            self.tasks[command_name] = task

        if isinstance(timeout, str):
            timeout = TimeParser(timeout).time

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
        *args: tuple[str, ...],
        alias: str | None = None,
        env: dict[str, Any] | None = None,
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
        if self._cleanup_task is None:
            self.start_cleanup()

        command_name = alias
        if command_name is None:
            command_name = command

        if isinstance(timeout, str):
            timeout = TimeParser(timeout).time

        if shell:
            args = [shlex.quote(arg) for arg in args]

        task = self.tasks.get(command_name)
        if task is None:
            task = Task(
                self._snowflake_generator,
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
            )

            self.tasks[command_name] = task

        if task and task.repeat == "NEVER":
            return task.run_shell(
                *args,
                env=env,
                cwd=cwd,
                shell=shell,
                run_id=run_id,
                timeout=timeout,
                poll_interval=self._cleanup_interval,
            )

        elif task and task.schedule:
            return task.run_shell_schedule(
                *args,
                env=env,
                cwd=cwd,
                shell=shell,
                run_id=run_id,
                timeout=timeout,
            )

    async def wait_all(self, tokens: list[str]):
        return await asyncio.gather(
            *[self.wait(token) for token in tokens],
        )

    async def wait(self, token: str) -> ShellProcess | TaskRun:
        task_name, run_id_str = token.split(":", maxsplit=1)
        run_id = int(run_id_str)

        update = await self.tasks[task_name].get_run_update(run_id)
        while update.status not in [
            RunStatus.COMPLETE,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
        ]:
            await asyncio.sleep(self._cleanup_interval)
            update = await self.tasks[task_name].get_run_update(run_id)

        return await self.tasks[task_name].complete(run_id)

    async def get_task_update(self, token: str):
        task_name, run_id = token.split(":", maxsplit=1)
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
        task_name, run_id = token.split(":", maxsplit=1)

        if task := self.tasks.get(task_name):
            return task.get_run_status(int(run_id))

    async def complete(self, token: str):
        task_name, run_id = token.split(":", maxsplit=1)

        if task := self.tasks.get(task_name):
            return await task.complete(int(run_id))

    async def cancel(self, token: str):
        task_name, run_id = token.split(":", maxsplit=1)

        task = self.tasks.get(task_name)
        if task:
            await task.cancel(int(run_id))

    async def cancel_schedule(
        self,
        token: str,
    ):
        task_name, run_id = token.split(":", maxsplit=1)

        task = self.tasks.get(task_name)
        if task:
            await task.cancel_schedule(int(run_id))

    async def stop(self):
        # Snapshot to avoid dict mutation during iteration
        for task in list(self.tasks.values()):
            await task.shutdown()

    async def shutdown(self):
        # Snapshot to avoid dict mutation during iteration
        for task in list(self.tasks.values()):
            await task.shutdown()

        self._run_cleanup = False

        try:
            self._cleanup_task.cancel()
            await asyncio.sleep(0)

        except Exception:
            pass

        if self._executor:
            try:
                self._executor.shutdown(cancel_futures=True)

            except Exception:
                pass

    def abort(self):
        # Snapshot to avoid dict mutation during iteration
        for task in list(self.tasks.values()):
            task.abort()

        self._run_cleanup = False

        try:
            self._cleanup_task.set_result(None)

        except Exception:
            pass
        
        if self._executor:
            try:
                self._executor.shutdown(cancel_futures=True)

            except Exception:
                pass

    async def _cleanup(self):
        while self._run_cleanup:
            await self._cleanup_scheduled_tasks()
            await asyncio.sleep(self._cleanup_interval)

    async def _cleanup_scheduled_tasks(self):
        try:
            # Snapshot to avoid dict mutation during iteration
            for task in list(self.tasks.values()):
                await task.cleanup()

        except Exception:
            pass
