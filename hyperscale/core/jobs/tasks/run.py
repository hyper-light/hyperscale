import asyncio
import time
import traceback
from typing import Any, Awaitable, Callable, Optional

from .models import RunStatus, TaskRun


class Run:
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
        "_task",
    )

    def __init__(
        self,
        run_id: int,
        call: Callable[..., Awaitable[Any]],
        timeout: Optional[int] = None,
    ) -> None:
        self.run_id = run_id
        self.status = RunStatus.CREATED

        self.error: Optional[str] = None
        self.trace: Optional[str] = None
        self.start = time.monotonic()
        self.end = 0
        self.elapsed = 0
        self.timeout = timeout

        bound_instance = call.__self__

        self.call = call
        self.result: Any | None = None

        self.call = self.call.__get__(bound_instance, self.call.__class__)
        setattr(bound_instance, self.call.__name__, self.call)

        self._task: Optional[asyncio.Task] = None

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
    def task_running(self):
        return self._task and not self._task.done() and not self._task.cancelled()

    def to_task_run(self):
        return TaskRun(
            run_id=self.run_id,
            status=self.status,
            error=self.error,
            trace=self.trace,
            start=self.start,
            end=self.end,
            elapsed=self.elapsed,
            result=self.result,
        )

    def update_status(self, status: RunStatus):
        self.status = status
        self.elapsed = time.monotonic() - self.start

    async def complete(self):
        if self.running and self.task_running:
            try:
                return await self._task

            except (asyncio.InvalidStateError, asyncio.CancelledError):
                pass

    async def cancel(self):
        try:
            self._task.set_result(None)

        except Exception:
            pass

        self.status = RunStatus.CANCELLED

    def abort(self):
        try:
            self._task.set_result(None)

        except Exception:
            pass

        self.status = RunStatus.CANCELLED

    def execute(self, *args, **kwargs):
        self._task = asyncio.ensure_future(self._execute(*args, **kwargs))

    async def _execute(self, *args, **kwargs):
        try:
            if self.timeout:
                self.result = await asyncio.wait_for(
                    self.call(*args, **kwargs), timeout=self.timeout
                )

            else:
                self.result = await self.call(*args, **kwargs)

            self.status = RunStatus.COMPLETE

        except asyncio.TimeoutError:
            self.error = f"Err. - Task Run - {self.run_id} - timed out. Exceeded deadline of - {self.timeout} - seconds."
            self.status = RunStatus.FAILED

        except Exception as e:
            self.error = f"Err. - Task Run - {self.run_id} - failed. Encountered exception - {str(e)}."
            self.trace = traceback.format_exc()
            self.status = RunStatus.FAILED

        self.end = time.monotonic()
        self.elapsed = self.end - self.start

        return self.result
