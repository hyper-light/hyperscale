from __future__ import annotations

import io
import os
import queue
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


@dataclass(slots=True)
class WriteRequest:
    data: bytes
    on_complete: Callable[[BaseException | None], None]


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
    - Single thread owns the file handle exclusively (no races, no leaks)
    - Batches writes: collect for N microseconds OR until batch full
    - Single write() + single fsync() commits entire batch
    - Resolves all futures in batch after fsync completes
    - File handle cleanup guaranteed by thread ownership

    Throughput model:
    - fsync at 500μs = 2,000 batches/sec
    - 100 entries/batch = 200,000 entries/sec
    - 1000 entries/batch = 2,000,000 entries/sec
    """

    __slots__ = (
        "_path",
        "_file",
        "_queue",
        "_thread",
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
        self._thread: threading.Thread | None = None
        self._running = False
        self._batch_timeout_seconds = batch_timeout_microseconds / 1_000_000
        self._batch_max_entries = batch_max_entries
        self._batch_max_bytes = batch_max_bytes
        self._current_batch = WriteBatch()
        self._error: BaseException | None = None

    def start(self) -> None:
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._run,
            name=f"wal-writer-{self._path.name}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if not self._running:
            return

        self._running = False
        self._queue.put(None)

        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def submit(self, request: WriteRequest) -> None:
        if not self._running:
            request.on_complete(RuntimeError("WAL writer is not running"))
            return

        if self._error is not None:
            request.on_complete(self._error)
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
            self._process_loop()
        except BaseException as exception:
            self._error = exception
            self._fail_pending_requests(exception)
        finally:
            self._close_file()

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

            for request in self._current_batch.requests:
                request.on_complete(None)

        except BaseException as exception:
            self._fail_batch(exception)
            raise

        finally:
            self._current_batch.clear()

    def _fail_batch(self, exception: BaseException) -> None:
        for request in self._current_batch.requests:
            try:
                request.on_complete(exception)
            except Exception:
                pass

        self._current_batch.clear()

    def _fail_pending_requests(self, exception: BaseException) -> None:
        self._fail_batch(exception)

        while True:
            try:
                request = self._queue.get_nowait()
                if request is not None:
                    try:
                        request.on_complete(exception)
                    except Exception:
                        pass
            except queue.Empty:
                break
