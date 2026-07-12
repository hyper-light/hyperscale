"""
SimFilesystem — the SIM-mode ``Filesystem``: pure in-memory, zero
threads, deterministic.

The ``SimulationLoop`` bans ``run_in_executor`` (thread pools are the
largest single source of asyncio non-determinism), so the REAL
filesystem cannot run under SIM at all — this implementation routes
every operation through plain awaits on the loop.

Durability model — the part storage faults are built on:

Every file's content is ``durable_content`` plus an ordered list of
``volatile_segments``. Writes append segments (visible to every
reader, like the OS page cache); the DURABILITY barriers —
``fsync`` / ``append_fsync`` / ``atomic_write`` — promote segments to
durable. ``crash()`` models power loss: every file collapses to its
durable content, dropping volatile tails. Keeping the volatile side as
discrete SEGMENTS (not one buffer) is deliberate: the Phase 7
``fsync_reorder`` fault drops a seeded SUBSET of un-promoted segments
on crash rather than a clean suffix, which is exactly how reordering
devices tear.

Directory bookkeeping is loose on write (parents auto-created) and
exact on read (``exists`` / ``list_directory``): the production
components all ``mkdir`` before writing, and strictness here would
test the model rather than the system. ``list_directory`` returns
sorted paths — deterministic iteration everywhere.
"""

from pathlib import Path


class _SimFileState:
    """One file's content: durable prefix + volatile (un-fsynced) tail."""

    __slots__ = ("durable_content", "volatile_segments")

    def __init__(self) -> None:
        self.durable_content = b""
        self.volatile_segments: list[bytes] = []

    @property
    def visible_content(self) -> bytes:
        return self.durable_content + b"".join(self.volatile_segments)

    def promote_volatile(self) -> None:
        """A durability barrier: everything written becomes durable."""
        self.durable_content = self.visible_content
        self.volatile_segments.clear()

    def collapse_to_durable(self) -> None:
        """Power loss: volatile tails vanish."""
        self.volatile_segments.clear()


class SimFileHandle:
    """An open handle over a ``_SimFileState`` with mode semantics for
    the modes production consumers use: ``r`` (read), ``w`` (truncate +
    sequential write), ``a`` (append)."""

    __slots__ = ("_state", "_mode", "_position", "_closed")

    def __init__(self, state: _SimFileState, mode: str) -> None:
        self._state = state
        self._mode = mode
        self._position = 0
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def write(self, data: bytes) -> int:
        self._require_open()
        if "r" in self._mode and "+" not in self._mode:
            raise OSError("file not open for writing")
        self._state.volatile_segments.append(bytes(data))
        return len(data)

    async def read(self, size: int = -1) -> bytes:
        self._require_open()
        content = self._state.visible_content
        if size < 0:
            data = content[self._position :]
            self._position = len(content)
            return data
        data = content[self._position : self._position + size]
        self._position += len(data)
        return data

    async def seek(self, offset: int, whence: int = 0) -> int:
        self._require_open()
        content_length = len(self._state.visible_content)
        if whence == 0:
            self._position = offset
        elif whence == 1:
            self._position += offset
        else:
            self._position = content_length + offset
        return self._position

    async def flush(self) -> None:
        # Volatile segments are already reader-visible (the page-cache
        # model); only the durability barriers change state.
        self._require_open()

    async def close(self) -> None:
        self._closed = True

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")


class SimFilesystem:
    """In-memory ``Filesystem`` for SIM mode.

    Per-process state (each simulation child owns its own instance —
    a node's disk is local to it). ``crash()`` is the scenario-facing
    power-loss primitive; the Phase 7 fault knobs (slow disk, disk
    full, fsync reordering) layer onto this durability model.
    """

    __slots__ = ("_files", "_directories")

    def __init__(self) -> None:
        self._files: dict[str, _SimFileState] = {}
        self._directories: set[str] = set()

    # -- scenario-facing fault primitive ---------------------------------

    def crash(self) -> None:
        """Model power loss: every file loses its un-fsynced tail.

        Files created but never fsynced collapse to empty durable
        content (they existed as directory entries whose data never
        reached the platter).
        """
        for state in self._files.values():
            state.collapse_to_durable()

    # -- Filesystem protocol ---------------------------------------------

    async def open(self, path: str | Path, mode: str) -> SimFileHandle:
        key = str(path)
        state = self._files.get(key)

        if "r" in mode and "+" not in mode:
            if state is None:
                raise FileNotFoundError(2, "No such file or directory", key)
            return SimFileHandle(state, mode)

        if state is None:
            state = _SimFileState()
            self._files[key] = state
            self._register_parents(Path(path))
        if "w" in mode:
            state.durable_content = b""
            state.volatile_segments.clear()

        return SimFileHandle(state, mode)

    async def fsync(self, handle: SimFileHandle) -> None:
        handle._state.promote_volatile()

    async def fsync_directory(self, path: str | Path) -> None:
        # Directory entries (renames, creations) are modeled as
        # immediately durable in v1; the fsync_reorder fault will hook
        # here to make un-synced renames crash-revertible.
        return None

    async def append_fsync(self, path: str | Path, data: bytes) -> None:
        key = str(path)
        state = self._files.get(key)
        if state is None:
            state = _SimFileState()
            self._files[key] = state
            self._register_parents(Path(path))
        state.volatile_segments.append(bytes(data))
        state.promote_volatile()

    async def atomic_write(self, path: str | Path, data: bytes) -> None:
        key = str(path)
        state = self._files.get(key)
        if state is None:
            state = _SimFileState()
            self._files[key] = state
            self._register_parents(Path(path))
        # All-or-nothing: the complete new content, durable at once.
        state.durable_content = bytes(data)
        state.volatile_segments.clear()

    async def read_bytes(self, path: str | Path) -> bytes:
        state = self._files.get(str(path))
        if state is None:
            raise FileNotFoundError(2, "No such file or directory", str(path))
        return state.visible_content

    async def read_text(
        self,
        path: str | Path,
        encoding: str = "utf-8",
    ) -> str:
        return (await self.read_bytes(path)).decode(encoding)

    async def exists(self, path: str | Path) -> bool:
        key = str(path)
        return key in self._files or key in self._directories

    async def mkdir(
        self,
        path: str | Path,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        # Loose on parents by design (v1): missing intermediate
        # directories are created implicitly rather than raising —
        # strict-parent errors would test the model, not the system.
        # ``exist_ok=False`` still raises, mirroring pathlib.
        key = str(path)
        if key in self._directories:
            if not exist_ok:
                raise FileExistsError(17, "File exists", key)
            return
        self._directories.add(key)
        self._register_parents(Path(path) / "placeholder")

    async def list_directory(
        self,
        path: str | Path,
        pattern: str = "*",
    ) -> list[Path]:
        directory = Path(path)
        return sorted(
            Path(file_path)
            for file_path in self._files
            if Path(file_path).parent == directory
            and Path(file_path).match(pattern)
        )

    async def remove(self, path: str | Path) -> None:
        key = str(path)
        if key not in self._files:
            raise FileNotFoundError(2, "No such file or directory", key)
        del self._files[key]

    def _register_parents(self, path: Path) -> None:
        for parent in path.parents:
            self._directories.add(str(parent))
