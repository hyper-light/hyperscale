import asyncio
import functools
import inspect
import json
import pathlib
import traceback
from asyncio.subprocess import Process
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Awaitable, Callable, Dict, Generic, Optional, TypeVar

from .models import (
    CommandType,
    RunStatus,
    ShellProcess,
    TaskRun,
    TaskType,
)

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

T = TypeVar("T")


class Run(Generic[T]):
    __slots__ = (
        "run_id",
        "status",
        "error",
        "trace",
        "start",
        "end",
        "elapsed",
        "timeout",
        "call",
        "result",
        "task_type",
        "_task",
        "_process",
        "_args",
        "_env",
        "_working_directory",
        "_command_type",
        "_buffer_size",
        "_read_lock",
        "_read_timeout",
        "_loop",
        "_executor",
        "_semaphore",
        "_return_code",
        "_return_code_read_lock",
        "task_name",
    )

    def __init__(
        self,
        run_id: int,
        task_name: str,
        call: Callable[..., Awaitable[T]] | str,
        task_type: TaskType,
        executor: ProcessPoolExecutor | ThreadPoolExecutor | None,
        semaphore: asyncio.Semaphore,
        timeout: Optional[int] = None,
    ) -> None:
        self.run_id = run_id
        self.task_name = task_name
        self.status = RunStatus.CREATED

        self._args: tuple[str, ...] | None = None
        self._env: dict[str, str] | None = None
        self._working_directory: str | None = None

        self.error: Optional[str] = None
        self.trace: Optional[str] = None
        self.start = _DEFAULT_CLOCK.monotonic()
        self.end = 0
        self.elapsed = 0
        self.timeout = timeout

        self.call = call
        self.task_type = task_type
        self.result: T | None = None
        self._args: tuple[str, ...] | None = None
        self._env: dict[str, str] | None = None
        self._working_directory: str | None = None
        self._read_lock = asyncio.Lock()

        # ``call`` runs as given: a bound method is already bound to its
        # instance and a partial already carries its bound method. Re-binding
        # them (and caching the result on the instance) did nothing for a
        # method, while a partial broke -- ``partial`` has no ``__get__``
        # before Python 3.14 (AttributeError), and from 3.14 binding one
        # prepends the instance as an extra argument and the cache replaced
        # the instance's method with that partial.

        self._task: Optional[asyncio.Task] = None
        self._process: Process | None = None
        self._command_type: CommandType = "subprocess"
        self._buffer_size = 8192
        self._read_timeout: int | float = 1
        self._loop = asyncio.get_event_loop()
        self._executor = executor
        self._semaphore = semaphore
        self._return_code: int | None = None
        self._return_code_read_lock = asyncio.Lock()

    def to_dict(self):
        return {"run_id": self.run_id, "task_name": self.task_name}

    def to_serialized_dict(self):
        return json.dumps(self.to_dict())

    @property
    def token(self):
        return f"{self.task_name}:{self.run_id}"

    @property
    def running(self):
        return self.status == RunStatus.RUNNING

    @property
    def cancelled(self):
        return self.status == RunStatus.CANCELLED

    @property
    def failed(self):
        return self.status == RunStatus.FAILED

    @property
    def pending(self):
        return self.status == RunStatus.PENDING

    @property
    def completed(self):
        return self.status == RunStatus.COMPLETE

    @property
    def created(self):
        return self.status == RunStatus.CREATED

    @property
    def pid(self):
        if self._process:
            return self._process.pid

    @property
    def return_code(self):
        return self._return_code

    async def get_stdout(self):
        buffer = bytearray()

        if self._process:
            chunk = await self._read_stdout_with_timeout()
            buffer.extend(chunk)

            while chunk:
                chunk = await self._read_stdout_with_timeout()
                buffer.extend(chunk)

        return bytes(buffer).decode()

    async def get_stderr(self):
        buffer = bytearray()
        if self._process:
            chunk = await self._read_stderr_with_timeout()
            buffer.extend(chunk)

            while chunk:
                chunk = await self._read_stderr_with_timeout()
                buffer.extend(chunk)

        return bytes(buffer).decode()

    async def _read_stderr_with_timeout(self):
        await self._read_lock.acquire()

        try:
            chunk = await _DEFAULT_CLOCK.wait_for(
                self._process.stderr.read(self._buffer_size),
                timeout=self._read_timeout,
            )

        except asyncio.TimeoutError:
            chunk = b""

        if self._read_lock.locked():
            self._read_lock.release()

        return chunk

    async def _read_stdout_with_timeout(self):
        await self._read_lock.acquire()

        try:
            chunk = await _DEFAULT_CLOCK.wait_for(
                self._process.stdout.read(self._buffer_size),
                timeout=self._read_timeout,
            )

        except asyncio.TimeoutError:
            chunk = b""

        if self._read_lock.locked():
            self._read_lock.release()

        return chunk

    @property
    def task_running(self):
        if self._process:
            return self._return_code is None

        return self._task_unfinished()

    def _task_unfinished(self):
        """The callable run's task when it is neither done nor cancelled
        (the value ``task_running`` has always returned)."""
        return self._task and not self._task.done() and not self._task.cancelled()

    async def get_run_update(self):
        if self._process:
            stderr = await self.get_stderr()
            stdout = await self.get_stdout()

            return ShellProcess(
                run_id=self.run_id,
                task_name=self.task_name,
                process_id=self._process.pid,
                command=self.call,
                args=self._args,
                status=self.status,
                env=self._env,
                working_directory=self._working_directory,
                command_type=self._command_type,
                error=stderr,
                result=stdout,
                trace=self.trace,
                elapsed=_DEFAULT_CLOCK.monotonic() - self.start,
            )

        return TaskRun(
            run_id=self.run_id,
            task_name=self.task_name,
            status=self.status,
            error=self.error,
            trace=self.trace,
            start=self.start,
            end=self.end,
            elapsed=_DEFAULT_CLOCK.monotonic() - self.start,
            result=self.result,
        )

    def update_status(self, status: RunStatus):
        self.status = status
        self.elapsed = _DEFAULT_CLOCK.monotonic() - self.start

    async def complete(self):
        completed = self.status in [RunStatus.COMPLETE, RunStatus.FAILED]

        if completed:
            try:
                return await self._task

            except (asyncio.InvalidStateError, asyncio.CancelledError):
                pass

    async def cancel(self):
        if self._process:
            try:
                self._process.terminate()

            except Exception:
                pass

        # Actually cancel the asyncio task if it's running
        if self._task_pending():
            try:
                self._task.cancel()
                await self._await_cancelled_task()
            except Exception:
                pass
        else:
            # Task already done, try to set result
            try:
                self._task.set_result(None)
            except Exception:
                pass

        self.status = RunStatus.CANCELLED

    def _task_pending(self):
        """The task when it was started and has not finished (truthiness
        is what ``cancel`` tests)."""
        return self._task and not self._task.done()

    async def _await_cancelled_task(self) -> None:
        """Give the cancelled task a chance to handle its cancellation."""
        # Give the task a chance to handle cancellation
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        try:
            await self._task
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise

    def abort(self):
        if self._process:
            self._process.kill()

        # ``self._task`` is an ``asyncio.Task`` (set by ``execute``),
        # not a bare ``Future``. ``Task.set_result`` does not exist;
        # the previous call quietly raised ``AttributeError`` and got
        # swallowed by the broad ``except``, so the underlying task
        # kept running long after ``abort()`` returned. The
        # simulation harness relies on ``abort()`` actually stopping
        # in-flight work — workflow executors, registration retries,
        # heartbeat loops — to mirror real process death. ``cancel()``
        # delivers the ``CancelledError`` to whatever the task is
        # awaiting, which is the correct stop signal.
        if self._task_unfinished_since_started():
            try:
                self._task.cancel()
            except Exception:
                pass

        self.status = RunStatus.CANCELLED

    def _task_unfinished_since_started(self) -> bool:
        """Whether the task was started and has not finished."""
        return self._task is not None and not self._task.done()

    def execute(self, *args, **kwargs):
        self._task = asyncio.ensure_future(self._execute(*args, **kwargs))

    def execute_shell(
        self,
        *args: str,
        poll_interval: int | float = 0.5,
        env: Dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
        shell: bool = False,
        timeout: int | float | None = None,
    ):
        self._args = args
        self._env = env

        if cwd:
            self._working_directory = str(cwd)

        self._task = asyncio.ensure_future(
            self._execute_shell(
                *args,
                env=env,
                cwd=cwd,
                shell=shell,
                timeout=timeout,
                poll_interval=poll_interval,
            )
        )

    async def _execute_shell(
        self,
        *args: str,
        poll_interval: int | float = 0.5,
        env: Dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
        shell: bool = False,
        timeout: int | float | None = None,
    ):
        if shell:
            self._command_type = "shell"

        if (spawn_failure := await self._spawn_process(args, env, cwd, shell)) is not None:
            return spawn_failure

        self.status = RunStatus.RUNNING

        return await self._await_process(timeout)

    async def _spawn_process(
        self,
        args: tuple[str, ...],
        env: Dict[str, str] | None,
        cwd: str | pathlib.Path | None,
        shell: bool,
    ) -> ShellProcess | None:
        """Start the run's process; the failed run's ShellProcess when it
        could not start, else None."""
        working_directory: pathlib.Path | None = None
        if cwd:
            working_directory = pathlib.Path(cwd)

        try:
            self._process = await self._create_process(args, env, shell, working_directory)

        except Exception as spawn_error:
            return self._spawn_failed(spawn_error)

        return None

    async def _create_process(
        self,
        args: tuple[str, ...],
        env: Dict[str, str] | None,
        shell: bool,
        working_directory: pathlib.Path | None,
    ) -> Process:
        """Spawn the command through a shell or directly. (The working
        directory is None exactly when no ``cwd`` was given.)"""
        if shell:
            command = [self.call]
            command.extend(args)

            return await asyncio.create_subprocess_shell(
                " ".join(command),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=working_directory,
            )

        return await asyncio.create_subprocess_exec(
            self.call,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=working_directory,
        )

    def _spawn_failed(self, spawn_error: Exception) -> ShellProcess:
        """Fail the run with the spawn's own error (called while handling it)."""
        # No process exists: the run fails with the spawn's own error.
        self.error = f"Err. - Task Run - {self.run_id} - could not start: {spawn_error!r}."
        self.trace = traceback.format_exc()
        self.status = RunStatus.FAILED

        return ShellProcess(
            run_id=self.run_id,
            task_name=self.task_name,
            process_id=None,
            command=self.call,
            args=self._args,
            status=self.status,
            env=self._env,
            working_directory=self._working_directory,
            command_type=self._command_type,
            error=self.error,
            trace=self.trace,
            elapsed=_DEFAULT_CLOCK.monotonic() - self.start,
        )

    async def _await_process(self, timeout: int | float | None) -> ShellProcess:
        """Wait for the started process and settle the run from its exit."""
        try:
            stderr, stdout = await self._wait_and_read_output(timeout)

        except asyncio.TimeoutError:
            return await self._timed_out_process()

        except Exception as err:
            return self._failed_process(err)

        return self._finished_process(stdout, stderr)

    async def _wait_and_read_output(self, timeout: int | float | None) -> tuple[str, str]:
        """The process's exit code, within ``timeout`` when one is set,
        then its stderr and stdout."""
        if timeout:
            self._return_code = await _DEFAULT_CLOCK.wait_for(
                self._process.wait(),
                timeout=timeout,
            )

        else:
            self._return_code = await self._process.wait()

        stderr = await self.get_stderr()
        stdout = await self.get_stdout()
        return stderr, stdout

    async def _timed_out_process(self) -> ShellProcess:
        """Fail a run whose process overran its deadline."""
        error = f"Err. - Task Run - {self.run_id} - timed out. Exceeded deadline of - {self.timeout} - seconds."
        self.status = RunStatus.FAILED

        await self.get_stderr()
        await self.get_stdout()

        return ShellProcess(
            run_id=self.run_id,
            task_name=self.task_name,
            process_id=self._process.pid,
            command=self.call,
            args=self._args,
            status=self.status,
            env=self._env,
            working_directory=self._working_directory,
            command_type=self._command_type,
            error=error,
            trace=self.trace,
            elapsed=_DEFAULT_CLOCK.monotonic() - self.start,
        )

    def _failed_process(self, err: Exception) -> ShellProcess:
        """Fail a run whose wait raised (called while handling ``err``)."""
        error = f"Err. - Task Run - {self.run_id} - encountered error {str(err)}."
        self.trace = traceback.format_exc()
        self.status = RunStatus.FAILED

        return ShellProcess(
            run_id=self.run_id,
            task_name=self.task_name,
            process_id=self._process.pid,
            command=self.call,
            args=self._args,
            status=self.status,
            env=self._env,
            working_directory=self._working_directory,
            command_type=self._command_type,
            error=error,
            trace=self.trace,
            elapsed=_DEFAULT_CLOCK.monotonic() - self.start,
        )

    def _finished_process(self, stdout: str, stderr: str) -> ShellProcess:
        """Settle a run whose process exited: complete on exit code 0."""
        self.result = stdout
        if stderr:
            self.error = stderr

        if self.return_code != 0:
            self.error = f"Err. - Task Run - {self.run_id} - failed. Encountered exception - {stderr}."
            self.status = RunStatus.FAILED

        else:
            self.status = RunStatus.COMPLETE

        return ShellProcess(
            run_id=self.run_id,
            task_name=self.task_name,
            process_id=self._process.pid,
            command=self.call,
            args=self._args,
            status=self.status,
            return_code=self._return_code,
            env=self._env,
            working_directory=self._working_directory,
            command_type=self._command_type,
            error=self.error,
            result=self.result,
            trace=self.trace,
            elapsed=_DEFAULT_CLOCK.monotonic() - self.start,
        )

    async def _execute(self, *args, **kwargs):
        try:
            self.status = RunStatus.RUNNING

            is_coroutine = (
                inspect.iscoroutine(self.call)
                or inspect.isawaitable(self.call)
                or inspect.iscoroutinefunction(self.call)
            )

            if self.timeout and is_coroutine:
                self.result = await _DEFAULT_CLOCK.wait_for(
                    self.call(*args, **kwargs), timeout=self.timeout
                )

            elif is_coroutine:
                self.result = await self.call(*args, **kwargs)

            elif self.timeout:
                await self._semaphore.acquire()
                self.result = await _DEFAULT_CLOCK.wait_for(
                    self._loop.run_in_executor(
                        self._executor, 
                        functools.partial(
                            self.call, 
                            *args, 
                            **kwargs,
                        ),
                    )
                )

                self._semaphore.release()

            else:
                await self._semaphore.acquire()
                self.result = await self._loop.run_in_executor(
                    self._executor, 
                    functools.partial(
                        self.call, 
                        *args, 
                        **kwargs,
                    ),
                )

                self._semaphore.release()

            self.status = RunStatus.COMPLETE

        except asyncio.TimeoutError:
            self.error = f"Err. - Task Run - {self.run_id} - timed out. Exceeded deadline of - {self.timeout} - seconds."
            self.status = RunStatus.FAILED

        except Exception as e:
            self.error = f"Err. - Task Run - {self.run_id} - failed. Encountered exception - {str(e)}."
            self.trace = traceback.format_exc()
            self.status = RunStatus.FAILED

        self.end = _DEFAULT_CLOCK.monotonic()
        self.elapsed = self.end - self.start

        return TaskRun(
            run_id=self.run_id,
            task_name=self.task_name,
            status=self.status,
            error=self.error,
            trace=self.trace,
            start=self.start,
            end=self.end,
            elapsed=_DEFAULT_CLOCK.monotonic() - self.start,
            result=self.result,
        )
