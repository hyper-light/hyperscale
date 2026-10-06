"""
Default ``Filesystem`` implementation binding to the stdlib THROUGH the
event loop's executor — every operation is async and runs off-loop by
construction, so no caller can stall the loop on a disk touch (fsync
blocks for milliseconds to seconds; the seamed components previously
each carried their own ``run_in_executor`` / ``to_thread`` discipline
at the call site, which the seam now owns in one place).

Production behavior is exactly what the seamed components did inline
before Phase 7 — with one deliberate strengthening: ``atomic_write``
always performs the FULL crash-consistency sequence (temp file in the
destination's directory → write → flush → fsync → atomic rename →
parent-directory fsync) as ONE executor job, the pattern
``CheckpointManager`` already implemented by hand. Components that
previously skipped the fsyncs (``IncarnationStore``) become crash-safe
simply by expressing their intent through this operation. Pattern
operations (``append_fsync``, ``atomic_write``) batch their whole
sync sequence into a single executor job, matching the one-hop-per-
commit shape ``WALWriter`` and ``CheckpointManager`` already had.
"""

import asyncio
import errno
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# On macOS fsync(2) only moves data to the drive, which may keep it in its
# volatile cache and write it later, out of order; F_FULLFSYNC asks the
# drive to flush it to permanent storage (fsync(2), fcntl(2) man pages).
# Every sync below is meant to survive power loss, so on macOS it is a
# full sync. Other platforms' fsync already flushes the drive.
if sys.platform == "darwin":
    import fcntl

    _FULL_SYNC_COMMAND: int | None = fcntl.F_FULLFSYNC
else:
    _FULL_SYNC_COMMAND = None

# A filesystem that cannot honor F_FULLFSYNC (some network and FUSE
# filesystems) answers with one of these, and gets the strongest sync it
# offers: fsync. Any other error is a failed sync and raises.
_FULL_SYNC_UNSUPPORTED_ERRNOS = frozenset(
    {errno.ENOTSUP, errno.EOPNOTSUPP, errno.ENOTTY, errno.EINVAL}
)


class RealFileHandle:
    """A stdlib file object with its IO dispatched off-loop.

    Only ``RealFilesystem`` constructs these; ``RealFilesystem.fsync``
    relies on the extra ``fileno()`` accessor beyond the ``FileHandle``
    Protocol surface, and each handle dispatches through its owning
    filesystem's dedicated executor.
    """

    __slots__ = ("_file", "_run")

    def __init__(self, file, run) -> None:
        self._file = file
        self._run = run

    @property
    def closed(self) -> bool:
        return self._file.closed

    def fileno(self) -> int:
        return self._file.fileno()

    def tell(self) -> int:
        return self._file.tell()

    def close_sync(self) -> None:
        self._file.close()

    async def write(self, data: bytes) -> int:
        return await self._run(self._file.write, data)

    async def read(self, size: int = -1) -> bytes:
        return await self._run(self._file.read, size)

    async def readline(self) -> bytes:
        return await self._run(self._file.readline)

    async def seek(self, offset: int, whence: int = 0) -> int:
        return await self._run(self._file.seek, offset, whence)

    async def flush(self) -> None:
        await self._run(self._file.flush)

    async def close(self) -> None:
        await self._run(self._file.close)


class RealFilesystem:
    """Stdlib-backed ``Filesystem`` running every operation on its own
    bounded ``ThreadPoolExecutor`` — NOT the event loop's ambient
    default executor.

    A dedicated pool keeps disk IO from competing with other
    default-executor users, bounds the thread count, and — critically —
    has an owner responsible for shutting it down. Lifecycle contract:

    * Threads spawn lazily on first IO, so constructing an instance
      (including module-default singletons at import time) costs no
      threads.
    * An INJECTED filesystem is borrowed: the borrower must never call
      ``shutdown`` — the constructor of the instance owns it.
    * An OWNED instance must have ``shutdown()`` wired into its owner's
      close/abort path, or threads leak until interpreter exit (the
      executor's atexit join is the last-resort backstop for
      process-lifetime singletons, not a substitute for ownership).
    * ``shutdown(wait=False)`` (the default) is loop-safe: it signals
      the pool and returns immediately — in-flight operations complete
      on their threads, which then exit. ``wait=True`` JOINS the
      threads and must only be used off-loop (process teardown).

    After shutdown, further operations raise ``RuntimeError`` from the
    executor — loud, never a silent no-op.
    """

    __slots__ = ("_executor", "_shutdown")

    def __init__(self, max_workers: int = 4) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="hyperscale-filesystem",
        )
        self._shutdown = False

    def shutdown(self, wait: bool = False) -> None:
        """Shut the IO pool down. Idempotent.

        ``wait=False`` (default) is safe on the event loop; ``wait=True``
        blocks until every worker thread has exited — teardown only.
        """
        if self._shutdown:
            return
        self._shutdown = True
        self._executor.shutdown(wait=wait, cancel_futures=False)

    def _run(self, call, *call_args):
        return asyncio.get_running_loop().run_in_executor(
            self._executor, lambda: call(*call_args)
        )

    async def open(self, path: str | Path, mode: str) -> RealFileHandle:
        opened = await self._run(open, path, mode)
        return RealFileHandle(opened, self._run)

    async def write_flush(
        self,
        handle: RealFileHandle,
        data: bytes,
        *,
        flush: bool = True,
        fsync: bool = False,
    ) -> int:
        return await self._run(
            self._write_flush_sync, handle._file, data, flush, fsync
        )

    async def fsync(self, handle: RealFileHandle) -> None:
        await self._run(self._sync_durably, handle.fileno())

    async def file_size(self, path: str | Path) -> int:
        return await self._run(os.path.getsize, path)

    async def fsync_directory(self, path: str | Path) -> None:
        await self._run(self._fsync_directory_sync, path)

    async def append_fsync(self, path: str | Path, data: bytes) -> None:
        await self._run(self._append_fsync_sync, path, data)

    async def truncate(self, path: str | Path, length: int) -> None:
        await self._run(self._truncate_sync, path, length)

    async def atomic_write(self, path: str | Path, data: bytes) -> None:
        await self._run(self._atomic_write_sync, path, data)

    async def read_bytes(self, path: str | Path) -> bytes:
        return await self._run(Path(path).read_bytes)

    async def read_text(
        self,
        path: str | Path,
        encoding: str = "utf-8",
    ) -> str:
        return await self._run(Path(path).read_text, encoding)

    async def exists(self, path: str | Path) -> bool:
        return await self._run(Path(path).exists)

    async def mkdir(
        self,
        path: str | Path,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        await self._run(self._mkdir_sync, path, parents, exist_ok)

    async def list_directory(
        self,
        path: str | Path,
        pattern: str = "*",
    ) -> list[Path]:
        return await self._run(self._list_directory_sync, path, pattern)

    async def list_subdirectories(self, path: str | Path) -> list[Path]:
        return await self._run(self._list_subdirectories_sync, path)

    async def remove(self, path: str | Path) -> None:
        await self._run(os.unlink, path)

    async def remove_directory(self, path: str | Path) -> None:
        await self._run(os.rmdir, path)

    # -- single-executor-job sync sequences ------------------------------

    @staticmethod
    def _sync_durably(descriptor: int) -> None:
        """Flush the descriptor's data and metadata to permanent storage:
        F_FULLFSYNC on macOS (fsync only reaches the drive's cache there),
        falling back to fsync on a filesystem that cannot honor it; fsync
        elsewhere."""
        if _FULL_SYNC_COMMAND is not None:
            try:
                fcntl.fcntl(descriptor, _FULL_SYNC_COMMAND)
                return
            except OSError as full_sync_error:
                if full_sync_error.errno not in _FULL_SYNC_UNSUPPORTED_ERRNOS:
                    raise
        os.fsync(descriptor)

    @classmethod
    def _write_flush_sync(
        cls, file, data: bytes, flush: bool, fsync: bool
    ) -> int:
        written = file.write(data)
        if flush or fsync:
            file.flush()
        if fsync:
            cls._sync_durably(file.fileno())
        return written

    @staticmethod
    def _mkdir_sync(path: str | Path, parents: bool, exist_ok: bool) -> None:
        Path(path).mkdir(parents=parents, exist_ok=exist_ok)

    @staticmethod
    def _list_directory_sync(path: str | Path, pattern: str) -> list[Path]:
        return sorted(Path(path).glob(pattern))

    @staticmethod
    def _list_subdirectories_sync(path: str | Path) -> list[Path]:
        return sorted(
            entry for entry in Path(path).iterdir() if entry.is_dir()
        )

    @classmethod
    def _fsync_directory_sync(cls, path: str | Path) -> None:
        directory_descriptor = os.open(path, os.O_RDONLY)
        try:
            cls._sync_durably(directory_descriptor)
        finally:
            os.close(directory_descriptor)

    @classmethod
    def _append_fsync_sync(cls, path: str | Path, data: bytes) -> None:
        # A raw write may be SHORT (near ENOSPC the kernel writes what
        # fits and returns the count; only the next write raises), so
        # every byte is written or the error surfaces -- a single
        # unchecked write reported success for a torn record.
        descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o666)
        try:
            remaining = memoryview(data)
            while remaining:
                written = os.write(descriptor, remaining)
                if written == 0:
                    raise OSError(errno.EIO, "append made no progress", str(path))
                remaining = remaining[written:]
            cls._sync_durably(descriptor)
        finally:
            os.close(descriptor)

    @classmethod
    def _truncate_sync(cls, path: str | Path, length: int) -> None:
        descriptor = os.open(path, os.O_WRONLY)
        try:
            os.ftruncate(descriptor, length)
            cls._sync_durably(descriptor)
        finally:
            os.close(descriptor)

    @classmethod
    def _atomic_write_sync(cls, path: str | Path, data: bytes) -> None:
        destination = Path(path)
        temp_descriptor, temp_name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(temp_descriptor, "wb") as temp_file:
                temp_file.write(data)
                temp_file.flush()
                cls._sync_durably(temp_file.fileno())
            os.rename(temp_name, destination)
        except BaseException:
            # The temp file must never linger on a failed write — but
            # after the rename succeeded there is nothing to remove.
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass
            raise
        cls._fsync_directory_sync(destination.parent)
