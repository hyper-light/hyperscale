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
        if task.cancelled() or (exception := task.exception()) is None:
            return

        self._record_error(exception)
        self._schedule_background_error_log(name, exception)

    def _record_error(self, exception: BaseException) -> None:
        """Count a failure and latch it as the writer's error unless an
        earlier one is already latched."""
        self._metrics.total_errors += 1
        if self._error is None:
            self._error = exception

    def _schedule_background_error_log(self, name: str, exception: BaseException) -> None:
        """Log a background task's failure on the writer's loop (a done
        callback cannot await the logger itself)."""
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

        await self._stop_writer_task()
        await self._cancel_state_change_task()

        await self._fail_pending_requests(RuntimeError("WAL writer stopped"))

    async def _stop_writer_task(self) -> None:
        """Let the writer task finish its drain, cancelling it after five
        seconds; the task reference is dropped either way."""
        if self._writer_task is None:
            return
        try:
            await _DEFAULT_CLOCK.wait_for(self._writer_task, timeout=5.0)
        except asyncio.TimeoutError:
            self._writer_task.cancel()
            await self._await_cancelled_task(self._writer_task)
        finally:
            self._writer_task = None

    async def _cancel_state_change_task(self) -> None:
        """Cancel an unfinished state-change flush and wait for it to end."""
        if self._state_change_task is not None and not self._state_change_task.done():
            self._state_change_task.cancel()
            await self._await_cancelled_task(self._state_change_task)

    async def _await_cancelled_task(self, task: asyncio.Task[None]) -> None:
        """Wait for a task this writer just cancelled, re-raising only a
        cancel aimed at the caller while it waited."""
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        try:
            await task
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise

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
        self._ensure_state_change_flush()

    def _ensure_state_change_flush(self) -> None:
        """Start the state-change flush unless one is still running (it
        picks up the newest pending change)."""
        if self._state_change_task is None or self._state_change_task.done():
            self._state_change_task = self._create_background_task(
                self._flush_state_change_callback(),
                f"wal-state-change-{self._path.name}",
            )

    def _has_pending_state_change(self) -> bool:
        """Whether a state change awaits delivery while the writer runs."""
        return self._pending_state_change is not None and self._running

    async def _flush_state_change_callback(self) -> None:
        while self._has_pending_state_change():
            callback = self._state_change_callback
            if callback is None:
                return

            queue_state, backpressure = self._pending_state_change
            self._pending_state_change = None

            await self._deliver_state_change(callback, queue_state, backpressure)

    async def _deliver_state_change(
        self,
        callback: Callable[[QueueState, BackpressureSignal], Awaitable[None]],
        queue_state: QueueState,
        backpressure: BackpressureSignal,
    ) -> None:
        """Run the state-change callback; its failure is counted, latched
        and logged."""
        try:
            await callback(queue_state, backpressure)
        except Exception as exc:
            self._record_error(exc)
            await self._log_error("State change callback failed: ", exc)

    async def _log_error(self, message_prefix: str, exception: BaseException) -> None:
        """Log ``exception`` after ``message_prefix`` when a logger is set."""
        if self._logger is not None:
            await self._logger.log(
                WALError(
                    message=f"{message_prefix}{exception}",
                    path=str(self._path),
                    error_type=type(exception).__name__,
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
        await self._truncate_to_committed_length(storage_error)
        self._storage_failure = storage_error
        if self._storage_health is not None:
            self._storage_health.record_failure(storage_error, attempted_bytes)
        await self._log_rolled_back_append(storage_error)

    async def _truncate_to_committed_length(self, storage_error: OSError) -> None:
        """Cut a torn tail back to the committed length; a failed cut
        latches the writer and is raised from ``storage_error``."""
        try:
            # A device fault can strike before the append created the
            # file; then there is nothing to cut.
            if await self._filesystem.exists(self._path):
                await self._filesystem.truncate(self._path, self._committed_length)
        except BaseException as truncate_error:
            self._error = truncate_error
            raise truncate_error from storage_error

    async def _log_rolled_back_append(self, storage_error: OSError) -> None:
        """Log a failed append that was rolled back, when a logger is set."""
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
        self._drain_queue_into_batch()

        if len(self._current_batch) > 0:
            try:
                await self._commit_batch()
            except BaseException as exc:
                await self._fail_drained_batch(exc)

    def _drain_queue_into_batch(self) -> None:
        """Move every queued write into the current batch at shutdown."""
        while not self._queue.empty():
            try:
                self._take_queued_into_batch()
            except asyncio.QueueEmpty:
                break

    def _take_queued_into_batch(self) -> None:
        """Add the next queued write (not stop's sentinel) to the batch;
        raises ``asyncio.QueueEmpty`` when none is queued."""
        request = self._queue.get_nowait()
        if request is not None:
            self._current_batch.add(request)

    async def _fail_drained_batch(self, exc: BaseException) -> None:
        """Record, log and fail the shutdown drain's batch after its
        commit raised."""
        self._record_error(exc)
        await self._log_error("Failed to drain WAL during shutdown: ", exc)
        self._fail_requests(self._current_batch.requests, exc)
        self._current_batch.clear()

    @staticmethod
    def _fail_requests(requests: list[WriteRequest], exception: BaseException) -> None:
        """Fail every unresolved write in ``requests`` with ``exception``."""
        for request in requests:
            if not request.future.done():
                request.future.set_exception(exception)

    async def _fail_pending_requests(self, exception: BaseException) -> None:
        self._fail_requests(self._current_batch.requests, exception)
        self._current_batch.clear()

        self._fail_queued_requests(exception)

    def _fail_queued_requests(self, exception: BaseException) -> None:
        """Fail every write still queued with ``exception``."""
        while not self._queue.empty():
            try:
                self._fail_next_queued_request(exception)
            except asyncio.QueueEmpty:
                break

    def _fail_next_queued_request(self, exception: BaseException) -> None:
        """Fail the next queued write unless it is stop's sentinel or
        already resolved; raises ``asyncio.QueueEmpty`` when none is queued."""
        request = self._queue.get_nowait()
        if request is not None and not request.future.done():
            request.future.set_exception(exception)

_REHOMED = (
    WALBackpressureError,
    WriteRequest,
    WriteBatch,
    WALWriterConfig,
    WALWriterMetrics,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
