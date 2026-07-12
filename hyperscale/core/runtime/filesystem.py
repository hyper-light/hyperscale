"""
Filesystem interface — the dependency-injection seam for every
production disk touch (Phase 7 storage faults).

Why this exists
---------------

Phase 5 seamed the wall clock, the RNG, and the transport; disk IO is
the remaining environmental input. The production storage surface is
eight components (survey order = durability rank): RaftWAL and NodeWAL
(both committing through the shared ``WALWriter`` group-commit fsync
engine), the ManagerIdempotencyLedger's own WAL, the IncarnationStore,
the CheckpointManager, the JobArchiveStore, the LoggerStream, and the
reporting result writers. All previously called ``open`` / ``os.fsync``
/ ``pathlib`` directly, so SIM mode could neither make their IO
deterministic nor inject the Phase 7 fault classes (slow disk, disk
full, fsync reordering, torn writes revealed by crash).

Why every operation is ASYNC
----------------------------

Disk operations block — ``fsync`` for milliseconds to seconds — and
everything in this codebase must be asyncio-compatible. The components
being seamed already knew this: every serious write ran under
``run_in_executor`` / ``to_thread`` at its call site, a discipline the
one component that forgot it (``IncarnationStore``, which fsync-lessly
blocked the event loop) proves cannot be left to convention. The seam
therefore owns the off-loop dispatch: ``RealFilesystem`` runs each
operation in the executor INSIDE the method, so no caller can stall
the loop by construction. And the SIM implementation REQUIRES async —
``slow_disk`` costs virtual time, which is only expressible as
``await clock.sleep(...)`` inside the operation; a synchronous seam
cannot model it at all.

The Protocol exposes two altitudes deliberately:

* **Durability patterns** — ``append_fsync`` (the WAL commit: append,
  flush, fsync, one durable unit) and ``atomic_write`` (the full
  temp-file → flush → fsync → rename → parent-directory-fsync sequence
  that ``CheckpointManager`` pioneered). Components that express
  intent at this level get crash-consistency BY CONSTRUCTION — one
  awaited call, one executor job in REAL mode — and the SIM
  implementation can model fault semantics per pattern (a torn
  ``append_fsync`` tail, an ``atomic_write`` whose rename survived but
  whose directory entry did not).
* **Handle/path primitives** — ``open`` / handle IO / ``fsync`` /
  ``fsync_directory`` and the small path ops, for the LoggerStream's
  handle-per-file write path and rotation machinery that genuinely
  needs them.

This module lives in ``hyperscale/core/runtime`` (not
``hyperscale/distributed/runtime``) because ``hyperscale/logging`` is
deliberately self-contained and must not import from
``hyperscale/distributed``; both packages import the seam from here
without a cycle.
"""

from pathlib import Path
from typing import Protocol


class FileHandle(Protocol):
    """The subset of an open binary file the write paths consume.

    All IO methods are async: a buffered ``write`` can trigger a
    flush-to-OS, ``flush``/``close`` always can, and reads always
    touch the device — none of them may run on the event loop.
    ``closed`` is pure in-memory state and stays synchronous.
    """

    @property
    def closed(self) -> bool: ...

    async def write(self, data: bytes) -> int: ...

    async def read(self, size: int = -1) -> bytes: ...

    async def seek(self, offset: int, whence: int = 0) -> int: ...

    async def flush(self) -> None: ...

    async def close(self) -> None: ...


class Filesystem(Protocol):
    """Perform every production disk operation, off-loop by construction.

    Durability semantics implementations must honor:

    * A handle ``write`` is BUFFERED — not crash-durable until
      ``fsync`` (``flush`` only moves it to the OS).
    * ``fsync(handle)`` makes all flushed bytes of that handle durable.
    * ``append_fsync`` appends one durable unit: the bytes are
      crash-durable when it returns; a crash mid-call may leave a torn
      tail that readers must tolerate.
    * ``atomic_write`` is all-or-nothing across crashes: readers see
      the complete old content or the complete new content, never a
      mix and never a disappearance — it owns the temp-file fsync, the
      rename, AND the parent-directory fsync.
    """

    async def open(self, path: str | Path, mode: str) -> FileHandle: ...

    async def fsync(self, handle: FileHandle) -> None: ...

    async def fsync_directory(self, path: str | Path) -> None: ...

    async def append_fsync(self, path: str | Path, data: bytes) -> None: ...

    async def atomic_write(self, path: str | Path, data: bytes) -> None: ...

    async def read_bytes(self, path: str | Path) -> bytes: ...

    async def read_text(
        self,
        path: str | Path,
        encoding: str = "utf-8",
    ) -> str: ...

    async def exists(self, path: str | Path) -> bool: ...

    async def mkdir(
        self,
        path: str | Path,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None: ...

    async def list_directory(
        self,
        path: str | Path,
        pattern: str = "*",
    ) -> list[Path]: ...

    async def list_subdirectories(self, path: str | Path) -> list[Path]: ...

    async def remove(self, path: str | Path) -> None: ...

    async def remove_directory(self, path: str | Path) -> None: ...
