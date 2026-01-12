from __future__ import annotations

import asyncio
import io
import os
import queue
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path


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


class WALWriter:
    """
    Dedicated writer thread for WAL with group commit.

    Design principles:
    - Single-worker ThreadPoolExecutor owns the file handle exclusively
    - Batches writes: collect for N microseconds OR until batch full
    - Single write() + single fsync() commits entire batch
    - Single call_soon_threadsafe resolves all futures in batch
    - File handle cleanup guaranteed by executor thread ownership

    Throughput model:
    - fsync at 500μs = 2,000 batches/sec
    - 100 entries/batch = 200,000 entries/sec
    - 1000 entries/batch = 2,000,000 entries/sec
    """

    __slots__ = (
        "_path",
        "_file",
        "_queue",
        "_executor",
        "_writer_future",
        "_loop",
        "_ready_event",
        "_running",
        "_batch_timeout_seconds",
        "_batch_max_entries",
        "_batch_max_bytes",
        "_current_batch",
        "_error",
    )

    def __init__(
        self,
        path: Path,
        batch_timeout_microseconds: int = 500,
        batch_max_entries: int = 1000,
        batch_max_bytes: int = 1024 * 1024,
    ) -> None:
        self._path = path
        self._file: io.FileIO | None = None
        self._queue: queue.Queue[WriteRequest | None] = queue.Queue()
        self._executor: ThreadPoolExecutor | None = None
        self._writer_future: Future[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready_event: asyncio.Event | None = None
        self._running = False
        self._batch_timeout_seconds = batch_timeout_microseconds / 1_000_000
        self._batch_max_entries = batch_max_entries
        self._batch_max_bytes = batch_max_bytes
        self._current_batch = WriteBatch()
        self._error: BaseException | None = None

    async def start(self) -> None:
        if self._running:
            return

        self._loop = asyncio.get_running_loop()
        self._ready_event = asyncio.Event()
        self._running = True

        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f"wal-writer-{self._path.name}",
        )

        self._writer_future = self._executor.submit(self._run)

        await self._ready_event.wait()

    async def stop(self) -> None:
        if not self._running:
            return

        self._running = False
        self._queue.put(None)

        if self._writer_future is not None:
            loop = self._loop
            assert loop is not None

            await loop.run_in_executor(None, self._writer_future.result)
            self._writer_future = None

        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None

    def submit(self, request: WriteRequest) -> None:
        if not self._running:
            loop = self._loop
            if loop is not None:
                loop.call_soon_threadsafe(
                    self._resolve_future,
                    request.future,
                    RuntimeError("WAL writer is not running"),
                )
            return

        if self._error is not None:
            loop = self._loop
            if loop is not None:
                loop.call_soon_threadsafe(
                    self._resolve_future,
                    request.future,
                    self._error,
                )
            return

        self._queue.put(request)

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def has_error(self) -> bool:
        return self._error is not None

    @property
    def error(self) -> BaseException | None:
        return self._error

    def _run(self) -> None:
        try:
            self._open_file()
            self._signal_ready()
            self._process_loop()
        except BaseException as exception:
            self._error = exception
            self._fail_pending_requests(exception)
        finally:
            self._close_file()

    def _signal_ready(self) -> None:
        loop = self._loop
        ready_event = self._ready_event

        if loop is not None and ready_event is not None:
            loop.call_soon_threadsafe(ready_event.set)

    def _open_file(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, "ab", buffering=0)

    def _close_file(self) -> None:
        if self._file is not None:
            try:
                self._file.flush()
                os.fsync(self._file.fileno())
                self._file.close()
            except Exception:
                pass
            finally:
                self._file = None

    def _process_loop(self) -> None:
        while self._running:
            self._collect_batch()

            if len(self._current_batch) > 0:
                self._commit_batch()

    def _collect_batch(self) -> None:
        try:
            request = self._queue.get(timeout=self._batch_timeout_seconds)

            if request is None:
                return

            self._current_batch.add(request)
        except queue.Empty:
            return

        while (
            len(self._current_batch) < self._batch_max_entries
            and self._current_batch.total_bytes < self._batch_max_bytes
        ):
            try:
                request = self._queue.get_nowait()

                if request is None:
                    return

                self._current_batch.add(request)
            except queue.Empty:
                break

    def _commit_batch(self) -> None:
        if self._file is None:
            exception = RuntimeError("WAL file is not open")
            self._fail_batch(exception)
            return

        try:
            combined_data = b"".join(
                request.data for request in self._current_batch.requests
            )

            self._file.write(combined_data)
            self._file.flush()
            os.fsync(self._file.fileno())

            futures = [request.future for request in self._current_batch.requests]

            loop = self._loop
            if loop is not None:
                loop.call_soon_threadsafe(self._resolve_batch, futures, None)

        except BaseException as exception:
            self._fail_batch(exception)
            raise

        finally:
            self._current_batch.clear()

    def _resolve_batch(
        self,
        futures: list[asyncio.Future[None]],
        error: BaseException | None,
    ) -> None:
        for future in futures:
            if future.cancelled():
                continue

            self._resolve_future(future, error)

    def _resolve_future(
        self,
        future: asyncio.Future[None],
        error: BaseException | None,
    ) -> None:
        if future.done():
            return

        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(None)

    def _fail_batch(self, exception: BaseException) -> None:
        futures = [request.future for request in self._current_batch.requests]

        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(self._resolve_batch, futures, exception)

        self._current_batch.clear()

    def _fail_pending_requests(self, exception: BaseException) -> None:
        self._fail_batch(exception)

        pending_futures: list[asyncio.Future[None]] = []

        while True:
            try:
                request = self._queue.get_nowait()
                if request is not None:
                    pending_futures.append(request.future)
            except queue.Empty:
                break

        if pending_futures:
            loop = self._loop
            if loop is not None:
                loop.call_soon_threadsafe(
                    self._resolve_batch,
                    pending_futures,
                    exception,
                )
