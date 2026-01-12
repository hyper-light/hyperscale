from __future__ import annotations

import asyncio
import io
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
    pass


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
    """Configuration for WALWriter."""

    # Batching settings
    batch_timeout_microseconds: int = 500
    batch_max_entries: int = 1000
    batch_max_bytes: int = 1024 * 1024  # 1MB

    # Queue settings (primary + overflow)
    queue_max_size: int = 10000
    overflow_size: int = 1000

    # Backpressure thresholds
    throttle_threshold: float = 0.70
    batch_threshold: float = 0.85
    reject_threshold: float = 0.95


@dataclass(slots=True)
class WALWriterMetrics:
    """Metrics for WAL writer observability."""

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

    Design principles:
    - Fully asyncio-native with RobustMessageQueue for backpressure
    - Batches writes: collect for N microseconds OR until batch full
    - Single write() + single fsync() commits entire batch via executor
    - File I/O delegated to thread pool (only sync operation)
    - Comprehensive metrics and backpressure signaling

    Throughput model:
    - fsync at 500μs = 2,000 batches/sec
    - 100 entries/batch = 200,000 entries/sec
    - 1000 entries/batch = 2,000,000 entries/sec

    Backpressure:
    - Uses RobustMessageQueue with overflow buffer
    - Graduated levels: NONE -> THROTTLE -> BATCH -> REJECT
    - Never silently drops - returns QueuePutResult with status
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
    )

    def __init__(
        self,
        path: Path,
        config: WALWriterConfig | None = None,
        state_change_callback: asyncio.coroutine | None = None,
    ) -> None:
        self._path = path
        self._config = config or WALWriterConfig()

        queue_config = RobustQueueConfig(
            maxsize=self._config.queue_max_size,
            overflow_size=self._config.overflow_size,
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

    async def start(self) -> None:
        """Start the WAL writer background task."""
        if self._running:
            return

        self._loop = asyncio.get_running_loop()
        self._running = True

        # Ensure directory exists
        self._path.parent.mkdir(parents=True, exist_ok=True)

        # Start the writer task
        self._writer_task = asyncio.create_task(
            self._writer_loop(),
            name=f"wal-writer-{self._path.name}",
        )

    async def stop(self) -> None:
        """Stop the WAL writer and wait for pending writes."""
        if not self._running:
            return

        self._running = False

        # Signal shutdown by putting None
        try:
            self._queue._primary.put_nowait(None)  # type: ignore
        except asyncio.QueueFull:
            pass

        # Wait for writer task to complete
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

        # Fail any remaining requests
        await self._fail_pending_requests(RuntimeError("WAL writer stopped"))

    def submit(self, request: WriteRequest) -> QueuePutResult:
        """
        Submit a write request to the queue.

        This is synchronous and non-blocking. Returns immediately with
        the result indicating acceptance status and backpressure level.

        Args:
            request: The write request containing data and future

        Returns:
            QueuePutResult with acceptance status and backpressure info
        """
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

        # Update metrics
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

        # Track state transitions
        if result.queue_state != self._last_queue_state:
            self._last_queue_state = result.queue_state
            if self._state_change_callback is not None and self._loop is not None:
                self._loop.call_soon(
                    lambda: asyncio.create_task(
                        self._state_change_callback(
                            result.queue_state, result.backpressure
                        )
                    )
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
        """Get combined metrics from writer and queue."""
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

    async def _writer_loop(self) -> None:
        """Main writer loop - collects batches and writes to disk."""
        try:
            while self._running:
                await self._collect_batch()

                if len(self._current_batch) > 0:
                    await self._commit_batch()

            # Final drain on shutdown
            await self._drain_remaining()

        except asyncio.CancelledError:
            # Graceful cancellation
            await self._drain_remaining()
            raise

        except BaseException as exception:
            self._error = exception
            self._metrics.total_errors += 1
            await self._fail_pending_requests(exception)

    async def _collect_batch(self) -> None:
        """Collect requests into a batch with timeout."""
        batch_timeout = self._config.batch_timeout_microseconds / 1_000_000

        try:
            # Wait for first request with timeout
            request = await asyncio.wait_for(
                self._queue.get(),
                timeout=batch_timeout,
            )

            # Check for shutdown signal
            if request is None:
                self._running = False
                return

            self._current_batch.add(request)

        except asyncio.TimeoutError:
            # No requests within timeout - that's fine
            return

        # Collect more requests without waiting (non-blocking)
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
        """Commit the current batch to disk with fsync."""
        if len(self._current_batch) == 0:
            return

        loop = self._loop
        assert loop is not None

        requests = self._current_batch.requests.copy()
        combined_data = b"".join(request.data for request in requests)

        try:
            # Delegate file I/O to executor
            await loop.run_in_executor(
                None,
                self._sync_write_and_fsync,
                combined_data,
            )

            # Update metrics
            self._metrics.total_written += len(requests)
            self._metrics.total_batches += 1
            self._metrics.total_bytes_written += len(combined_data)
            self._metrics.total_fsyncs += 1
            self._metrics.peak_batch_size = max(
                self._metrics.peak_batch_size,
                len(requests),
            )

            # Resolve all futures successfully
            for request in requests:
                if not request.future.done():
                    request.future.set_result(None)

        except BaseException as exception:
            self._error = exception
            self._metrics.total_errors += 1

            # Fail all futures in batch
            for request in requests:
                if not request.future.done():
                    request.future.set_exception(exception)

            raise

        finally:
            self._current_batch.clear()

    def _sync_write_and_fsync(self, data: bytes) -> None:
        """Synchronous write and fsync - runs in executor."""
        with open(self._path, "ab", buffering=0) as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())

    async def _drain_remaining(self) -> None:
        """Drain and commit any remaining requests on shutdown."""
        # Collect any remaining requests
        while not self._queue.empty():
            try:
                request = self._queue.get_nowait()
                if request is not None:
                    self._current_batch.add(request)
            except asyncio.QueueEmpty:
                break

        # Commit final batch
        if len(self._current_batch) > 0:
            try:
                await self._commit_batch()
            except BaseException:
                # Already handled in _commit_batch
                pass

    async def _fail_pending_requests(self, exception: BaseException) -> None:
        """Fail all pending requests with the given exception."""
        # Fail current batch
        for request in self._current_batch.requests:
            if not request.future.done():
                request.future.set_exception(exception)
        self._current_batch.clear()

        # Fail queued requests
        while not self._queue.empty():
            try:
                request = self._queue.get_nowait()
                if request is not None and not request.future.done():
                    request.future.set_exception(exception)
            except asyncio.QueueEmpty:
                break
