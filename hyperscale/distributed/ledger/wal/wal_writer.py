"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Awaitable
from hyperscale.distributed.reliability.robust_queue import (
    RobustMessageQueue,
    RobustQueueConfig,
    QueuePutResult,
    QueueState,
)
from hyperscale.distributed.reliability.backpressure import BackpressureLevel, BackpressureSignal
from hyperscale.logging.hyperscale_logging_models import WALError
from hyperscale.distributed.runtime import Clock, Filesystem, RealClock, RealFilesystem
from hyperscale.distributed.ledger.storage_health import StorageHealth

from .wal_backpressure_error import WALBackpressureError
from .wal_writer_config import WALWriterConfig
from .wal_writer_metrics import WALWriterMetrics
from .write_batch import WriteBatch
from .write_request import WriteRequest

if TYPE_CHECKING:
    from hyperscale.logging import Logger

_DEFAULT_CLOCK: Clock = RealClock()

# Module-level storage seam (Phase 7). The writer BORROWS this
# (or an injected instance) — it never shuts the filesystem down.
# ``swap_defaults`` rebinds it to the SIM filesystem so WAL commits
# become deterministic and storage-faultable under replay.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()


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
        "_filesystem",
        "_committed_length",
        "_storage_failure",
        "_storage_health",
        "_file_lock",
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
        filesystem: Filesystem | None = None,
        storage_health: StorageHealth | None = None,
    ) -> None:
        self._path = path
        # Shared node storage health this writer records its commit
        # outcomes into (None: the owner does not track storage health).
        self._storage_health = storage_health
        self._config = config or WALWriterConfig()
        self._logger = logger
        # Borrowed, never shut down here — see _DEFAULT_FILESYSTEM.
        self._filesystem = (
            filesystem if filesystem is not None else _DEFAULT_FILESYSTEM
        )

        queue_config = RobustQueueConfig(
            maxsize=self._config.queue_max_size,
            overflow_size=self._config.overflow_size,
            # A write the queue cannot hold is refused, never dropped:
            # each queued write holds an LSN and an appender awaiting it.
            preserve_newest=False,
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
        # Bytes durably committed to the log file. A failed append can
        # leave a torn partial record past this point; it is cut back
        # here before the writer continues.
        self._committed_length = 0
        # Held by each group commit and by a rewrite: a batch appended
        # between a rewrite's read and its rename would be lost.
        self._file_lock = asyncio.Lock()
        # The most recent storage failure a group commit hit (cleared by
        # the next successful commit).
        self._storage_failure: OSError | None = None

    def _create_background_task(self, coro, name: str) -> asyncio.Task:
        # Phase 6b: explicit ``self._loop.create_task`` instead of
        # ``asyncio.create_task`` so the task lands on the loop this
        # writer was started on rather than ``get_running_loop()`` of
        # whoever called us. ``_loop`` is set in ``start()`` before
        # the first background task is spawned.
        task = self._loop.create_task(coro, name=name)
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
            loop = self._loop
            self._loop.call_soon(
                lambda: loop.create_task(
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
        await self._filesystem.mkdir(
            self._path.parent, parents=True, exist_ok=True
        )
        self._committed_length = (
            await self._filesystem.file_size(self._path)
            if await self._filesystem.exists(self._path)
            else 0
        )

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
                await _DEFAULT_CLOCK.wait_for(self._writer_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._writer_task.cancel()
                cancels_requested_before_wait = asyncio.current_task().cancelling()
                try:
                    await self._writer_task
                except asyncio.CancelledError:
                    # The task we cancelled ended; a cancel aimed at this task
                    # while it waited goes on.
                    if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                        raise
            finally:
                self._writer_task = None

        if self._state_change_task is not None and not self._state_change_task.done():
            self._state_change_task.cancel()
            cancels_requested_before_wait = asyncio.current_task().cancelling()
            try:
                await self._state_change_task
            except asyncio.CancelledError:
                # The task we cancelled ended; a cancel aimed at this task
                # while it waited goes on.
                if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                    raise

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
    def storage_failure(self) -> OSError | None:
        """The storage error the latest group commit hit, or None once a
        later commit succeeded."""
        return self._storage_failure


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
        # An idle writer sleeps until a write (or stop's sentinel) arrives;
        # a batch is whatever queued meanwhile -- during the last commit.
        # Waiting with a timeout here woke an idle writer ~1,500 times a
        # second for nothing (4.5% of a core, measured).
        request = await self._queue.get()

        if request is None:
            self._running = False
            return

        self._current_batch.add(request)

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
            # The append and, if it fails, the cut back to the committed
            # length are one unit against a concurrent rewrite.
            async with self._file_lock:
                try:
                    # One durable unit per group commit through the storage
                    # seam -- the same append+flush+fsync sequence as before,
                    # as a single job on the filesystem's own executor.
                    await self._filesystem.append_fsync(self._path, combined_data)
                except OSError as storage_error:
                    # The device refused the append (full, read-only, I/O
                    # error). The batch fails; the log is cut back to its
                    # last committed record so the torn tail cannot cost
                    # later records at recovery; the writer keeps serving --
                    # the condition may clear. Only if the cut itself fails
                    # is the log unusable.
                    self._metrics.total_errors += 1
                    for request in requests:
                        if not request.future.done():
                            request.future.set_exception(storage_error)
                    await self._discard_failed_append(storage_error, len(combined_data))
                    return
                self._committed_length += len(combined_data)

            self._storage_failure = None
            if self._storage_health is not None:
                self._storage_health.record_success(len(combined_data))

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

    async def _discard_failed_append(
        self, storage_error: OSError, attempted_bytes: int
    ) -> None:
        """Cut the log back to its committed length after a failed append
        and record the storage failure; a failed cut latches the writer."""
        try:
            # A device fault can strike before the append created the
            # file; then there is nothing to cut.
            if await self._filesystem.exists(self._path):
                await self._filesystem.truncate(self._path, self._committed_length)
        except BaseException as truncate_error:
            self._error = truncate_error
            raise truncate_error from storage_error
        self._storage_failure = storage_error
        if self._storage_health is not None:
            self._storage_health.record_failure(storage_error, attempted_bytes)
        if self._logger is not None:
            await self._logger.log(
                WALError(
                    message=(
                        f"WAL append failed and was rolled back to "
                        f"{self._committed_length} bytes: {storage_error}"
                    ),
                    path=str(self._path),
                    error_type=type(storage_error).__name__,
                )
            )

    async def rewrite(self, transform: Callable[[bytes], bytes]) -> int:
        """Replace the log with ``transform`` of its committed bytes,
        atomically and with no group commit in flight; returns how many
        bytes it shrank by. A crash leaves the old log or the new, never
        a mix (the filesystem's atomic write: temp file, fsync, rename,
        directory fsync)."""
        async with self._file_lock:
            if not await self._filesystem.exists(self._path):
                return 0
            committed = await self._filesystem.read_bytes(self._path)
            rewritten = transform(committed)
            if rewritten is committed:
                return 0
            await self._filesystem.atomic_write(self._path, rewritten)
            self._committed_length = len(rewritten)
            return len(committed) - len(rewritten)

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

_REHOMED = (
    WALBackpressureError,
    WriteRequest,
    WriteBatch,
    WALWriterConfig,
    WALWriterMetrics,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
