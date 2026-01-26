from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Awaitable

from hyperscale.distributed.reliability.robust_queue import (
    RobustMessageQueue,
    RobustQueueConfig,
    QueuePutResult,
    QueueState,
)
from hyperscale.distributed.reliability.backpressure import (
    BackpressureLevel,
    BackpressureSignal,
)
from hyperscale.logging.hyperscale_logging_models import WALError

if TYPE_CHECKING:
    from hyperscale.logging import Logger


class WALBackpressureError(Exception):
    """Raised when WAL rejects a write due to backpressure."""

    def __init__(
        self,
        message: str,
        queue_state: QueueState,
        backpressure: BackpressureSignal,
    ) -> None:
        super().__init__(message)
        self.queue_state = queue_state
        self.backpressure = backpressure


@dataclass(slots=True)
class WriteRequest:
    data: bytes
    future: asyncio.Future[None]


@dataclass(slots=True)
class WriteBatch:
    requests: list[WriteRequest] = field(default_factory=list)
    total_bytes: int = 0

    def add(self, request: WriteRequest) -> None:
        self.requests.append(request)
        self.total_bytes += len(request.data)

    def clear(self) -> None:
        self.requests.clear()
        self.total_bytes = 0

    def __len__(self) -> int:
        return len(self.requests)


@dataclass(slots=True)
class WALWriterConfig:
    batch_timeout_microseconds: int = 500
    batch_max_entries: int = 1000
    batch_max_bytes: int = 1024 * 1024
    queue_max_size: int = 10000
    overflow_size: int = 1000
    preserve_newest: bool = True
    throttle_threshold: float = 0.70
    batch_threshold: float = 0.85
    reject_threshold: float = 0.95


@dataclass(slots=True)
class WALWriterMetrics:
    total_submitted: int = 0
    total_written: int = 0
    total_batches: int = 0
    total_bytes_written: int = 0
    total_fsyncs: int = 0
    total_rejected: int = 0
    total_overflow: int = 0
    total_errors: int = 0
    peak_queue_size: int = 0
    peak_batch_size: int = 0


class WALWriter:
    """
    Asyncio-native WAL writer with group commit and backpressure.

    Uses RobustMessageQueue for graduated backpressure (NONE -> THROTTLE -> BATCH -> REJECT).
    File I/O is delegated to executor. Batches writes with configurable timeout and size limits.
    """

    __slots__ = (
        "_path",
        "_config",
        "_queue",
        "_loop",
        "_running",
        "_writer_task",
        "_current_batch",
        "_metrics",
        "_error",
        "_last_queue_state",
        "_state_change_callback",
        "_pending_state_change",
        "_state_change_task",
        "_logger",
    )

    def __init__(
        self,
        path: Path,
        config: WALWriterConfig | None = None,
        state_change_callback: Callable[
            [QueueState, BackpressureSignal], Awaitable[None]
        ]
        | None = None,
        logger: Logger | None = None,
    ) -> None:
        self._path = path
        self._config = config or WALWriterConfig()
        self._logger = logger

        queue_config = RobustQueueConfig(
            maxsize=self._config.queue_max_size,
            overflow_size=self._config.overflow_size,
            preserve_newest=self._config.preserve_newest,
            throttle_threshold=self._config.throttle_threshold,
            batch_threshold=self._config.batch_threshold,
            reject_threshold=self._config.reject_threshold,
        )
        self._queue: RobustMessageQueue[WriteRequest] = RobustMessageQueue(queue_config)

        self._loop: asyncio.AbstractEventLoop | None = None
        self._running = False
        self._writer_task: asyncio.Task[None] | None = None
        self._current_batch = WriteBatch()
        self._metrics = WALWriterMetrics()
        self._error: BaseException | None = None
        self._last_queue_state = QueueState.HEALTHY
        self._state_change_callback = state_change_callback
        self._pending_state_change: tuple[QueueState, BackpressureSignal] | None = None
        self._state_change_task: asyncio.Task[None] | None = None

    def _create_background_task(self, coro, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        task.add_done_callback(lambda t: self._handle_background_task_error(t, name))
        return task

    def _handle_background_task_error(self, task: asyncio.Task, name: str) -> None:
        if task.cancelled():
            return

        exception = task.exception()
        if exception is None:
            return

        self._metrics.total_errors += 1
        if self._error is None:
            self._error = exception

        if self._logger is not None and self._loop is not None:
            self._loop.call_soon(
                lambda: asyncio.create_task(
                    self._logger.log(
                        WALError(
                            message=f"Background task '{name}' failed: {exception}",
                            path=str(self._path),
                            error_type=type(exception).__name__,
                        )
                    )
                )
            )

    async def start(self) -> None:
        if self._running:
            return

        self._loop = asyncio.get_running_loop()
        self._running = True
        self._path.parent.mkdir(parents=True, exist_ok=True)

        self._writer_task = self._create_background_task(
            self._writer_loop(),
            f"wal-writer-{self._path.name}",
        )

    async def stop(self) -> None:
        if not self._running:
            return

        self._running = False

        try:
            self._queue._primary.put_nowait(None)  # type: ignore
        except asyncio.QueueFull:
            pass

        if self._writer_task is not None:
            try:
                await asyncio.wait_for(self._writer_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._writer_task.cancel()
                try:
                    await self._writer_task
                except asyncio.CancelledError:
                    pass
            finally:
                self._writer_task = None

        if self._state_change_task is not None and not self._state_change_task.done():
            self._state_change_task.cancel()
            try:
                await self._state_change_task
            except asyncio.CancelledError:
                pass

        await self._fail_pending_requests(RuntimeError("WAL writer stopped"))

    def submit(self, request: WriteRequest) -> QueuePutResult:
        if not self._running:
            error = RuntimeError("WAL writer is not running")
            if not request.future.done():
                request.future.set_exception(error)
            return QueuePutResult(
                accepted=False,
                in_overflow=False,
                dropped=True,
                queue_state=QueueState.SATURATED,
                fill_ratio=1.0,
                backpressure=BackpressureSignal.from_level(BackpressureLevel.REJECT),
            )

        if self._error is not None:
            if not request.future.done():
                request.future.set_exception(self._error)
            return QueuePutResult(
                accepted=False,
                in_overflow=False,
                dropped=True,
                queue_state=QueueState.SATURATED,
                fill_ratio=1.0,
                backpressure=BackpressureSignal.from_level(BackpressureLevel.REJECT),
            )

        result = self._queue.put_nowait(request)

        if result.accepted:
            self._metrics.total_submitted += 1
            if result.in_overflow:
                self._metrics.total_overflow += 1
            self._metrics.peak_queue_size = max(
                self._metrics.peak_queue_size,
                self._queue.qsize(),
            )
        else:
            self._metrics.total_rejected += 1
            error = WALBackpressureError(
                f"WAL queue saturated: {result.queue_state.name}",
                queue_state=result.queue_state,
                backpressure=result.backpressure,
            )
            if not request.future.done():
                request.future.set_exception(error)

        if result.queue_state != self._last_queue_state:
            self._last_queue_state = result.queue_state
            self._schedule_state_change_callback(
                result.queue_state, result.backpressure
            )

        return result

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def has_error(self) -> bool:
        return self._error is not None

    @property
    def error(self) -> BaseException | None:
        return self._error

    @property
    def metrics(self) -> WALWriterMetrics:
        return self._metrics

    @property
    def queue_state(self) -> QueueState:
        return self._queue.get_state()

    @property
    def backpressure_level(self) -> BackpressureLevel:
        return self._queue.get_backpressure_level()

    def get_queue_metrics(self) -> dict:
        queue_metrics = self._queue.get_metrics()
        return {
            **queue_metrics,
            "total_submitted": self._metrics.total_submitted,
            "total_written": self._metrics.total_written,
            "total_batches": self._metrics.total_batches,
            "total_bytes_written": self._metrics.total_bytes_written,
            "total_fsyncs": self._metrics.total_fsyncs,
            "total_rejected": self._metrics.total_rejected,
            "total_overflow": self._metrics.total_overflow,
            "total_errors": self._metrics.total_errors,
            "peak_queue_size": self._metrics.peak_queue_size,
            "peak_batch_size": self._metrics.peak_batch_size,
        }

    def _schedule_state_change_callback(
        self,
        queue_state: QueueState,
        backpressure: BackpressureSignal,
    ) -> None:
        if self._state_change_callback is None or self._loop is None:
            return

        self._pending_state_change = (queue_state, backpressure)

        if self._state_change_task is None or self._state_change_task.done():
            self._state_change_task = self._create_background_task(
                self._flush_state_change_callback(),
                f"wal-state-change-{self._path.name}",
            )

    async def _flush_state_change_callback(self) -> None:
        while self._pending_state_change is not None and self._running:
            callback = self._state_change_callback
            if callback is None:
                return

            queue_state, backpressure = self._pending_state_change
            self._pending_state_change = None

            try:
                await callback(queue_state, backpressure)
            except Exception as exc:
                self._metrics.total_errors += 1
                if self._error is None:
                    self._error = exc
                if self._logger is not None:
                    await self._logger.log(
                        WALError(
                            message=f"State change callback failed: {exc}",
                            path=str(self._path),
                            error_type=type(exc).__name__,
                        )
                    )

    async def _writer_loop(self) -> None:
        try:
            while self._running:
                await self._collect_batch()

                if len(self._current_batch) > 0:
                    await self._commit_batch()

            await self._drain_remaining()

        except asyncio.CancelledError:
            await self._drain_remaining()
            raise

        except BaseException as exception:
            self._error = exception
            self._metrics.total_errors += 1
            await self._fail_pending_requests(exception)

    async def _collect_batch(self) -> None:
        batch_timeout = self._config.batch_timeout_microseconds / 1_000_000

        try:
            request = await asyncio.wait_for(
                self._queue.get(),
                timeout=batch_timeout,
            )

            if request is None:
                self._running = False
                return

            self._current_batch.add(request)

        except asyncio.TimeoutError:
            return

        while (
            len(self._current_batch) < self._config.batch_max_entries
            and self._current_batch.total_bytes < self._config.batch_max_bytes
        ):
            try:
                request = self._queue.get_nowait()

                if request is None:
                    self._running = False
                    return

                self._current_batch.add(request)

            except asyncio.QueueEmpty:
                break

    async def _commit_batch(self) -> None:
        if len(self._current_batch) == 0:
            return

        loop = self._loop
        assert loop is not None

        requests = self._current_batch.requests.copy()
        combined_data = b"".join(request.data for request in requests)

        try:
            await loop.run_in_executor(
                None,
                self._sync_write_and_fsync,
                combined_data,
            )

            self._metrics.total_written += len(requests)
            self._metrics.total_batches += 1
            self._metrics.total_bytes_written += len(combined_data)
            self._metrics.total_fsyncs += 1
            self._metrics.peak_batch_size = max(
                self._metrics.peak_batch_size,
                len(requests),
            )

            for request in requests:
                if not request.future.done():
                    request.future.set_result(None)

        except BaseException as exception:
            self._error = exception
            self._metrics.total_errors += 1

            for request in requests:
                if not request.future.done():
                    request.future.set_exception(exception)

            raise

        finally:
            self._current_batch.clear()

    def _sync_write_and_fsync(self, data: bytes) -> None:
        with open(self._path, "ab", buffering=0) as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())

    async def _drain_remaining(self) -> None:
        while not self._queue.empty():
            try:
                request = self._queue.get_nowait()
                if request is not None:
                    self._current_batch.add(request)
            except asyncio.QueueEmpty:
                break

        if len(self._current_batch) > 0:
            try:
                await self._commit_batch()
            except BaseException as exc:
                self._metrics.total_errors += 1
                if self._error is None:
                    self._error = exc
                if self._logger is not None:
                    await self._logger.log(
                        WALError(
                            message=f"Failed to drain WAL during shutdown: {exc}",
                            path=str(self._path),
                            error_type=type(exc).__name__,
                        )
                    )
                for request in self._current_batch.requests:
                    if not request.future.done():
                        request.future.set_exception(exc)
                self._current_batch.clear()

    async def _fail_pending_requests(self, exception: BaseException) -> None:
        for request in self._current_batch.requests:
            if not request.future.done():
                request.future.set_exception(exception)
        self._current_batch.clear()

        while not self._queue.empty():
            try:
                request = self._queue.get_nowait()
                if request is not None and not request.future.done():
                    request.future.set_exception(exception)
            except asyncio.QueueEmpty:
                break
