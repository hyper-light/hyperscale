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

Fault knobs (Phase 7 — all deterministic):

* ``set_slow_disk(delay_seconds)`` — every operation costs that much
  VIRTUAL time (awaits the injected clock inside the op; the reason
  the whole seam is async). Scenarios toggle it at chosen virtual
  instants rather than passing time windows — they own the timeline.
* ``set_disk_full(remaining_bytes)`` — a byte budget; writes that
  exceed it raise ``OSError(ENOSPC)``, exactly the error shape the
  production error paths handle.
* ``set_fsync_reorder(seed)`` — arms reordering-crash semantics: a
  subsequent ``crash()`` keeps a SEEDED SUBSET of each file's
  un-fsynced volatile segments (out-of-order persistence — how
  reordering devices tear, not a clean suffix) and truncates the last
  surviving segment at a seeded offset (the torn tail). Durable
  (fsynced) content is never touched — ``atomic_write`` /
  ``append_fsync`` guarantees hold by construction.
* ``set_read_corruption(seed, probability, path_glob)`` — runtime
  bitrot surfacing at READ time: seeded byte flips in the bytes a
  read RETURNS, stored state never mutated (the rot is in the read
  path, and disarming restores clean reads).
* ``set_misdirect(seed, probability)`` — kernel/FS misdirected IO: a
  path-level write lands on a seeded SIBLING file, or a path-level
  read is satisfied from one.
* ``set_io_error(seed, probability, at_time, until_time)`` —
  transient device errors: seeded draws inside the virtual-time
  window raise ``OSError(EIO)`` from the operation.
"""

import random
from pathlib import Path

from hyperscale.distributed.runtime import Clock


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
    sequential write), ``a`` (append). Carries its path so the
    read-corruption knob can glob-match handle reads."""

    __slots__ = (
        "_state",
        "_mode",
        "_position",
        "_closed",
        "_filesystem",
        "_path",
    )

    def __init__(
        self, state: _SimFileState, mode: str, filesystem, path: str
    ) -> None:
        self._state = state
        self._mode = mode
        self._position = 0
        self._closed = False
        self._filesystem = filesystem
        self._path = path

    @property
    def closed(self) -> bool:
        return self._closed

    def tell(self) -> int:
        return self._position

    def close_sync(self) -> None:
        self._closed = True

    async def write(self, data: bytes) -> int:
        self._require_open()
        if "r" in self._mode and "+" not in self._mode:
            raise OSError("file not open for writing")
        await self._filesystem._charge_operation(write_bytes=len(data))
        self._state.volatile_segments.append(bytes(data))
        return len(data)

    async def read(self, size: int = -1) -> bytes:
        self._require_open()
        await self._filesystem._charge_operation()
        content = self._state.visible_content
        if size < 0:
            data = content[self._position :]
            self._position = len(content)
            return self._filesystem._corrupt_read(self._path, data)
        data = content[self._position : self._position + size]
        self._position += len(data)
        return self._filesystem._corrupt_read(self._path, data)

    async def readline(self) -> bytes:
        self._require_open()
        await self._filesystem._charge_operation()
        content = self._state.visible_content
        newline_index = content.find(b"\n", self._position)
        if newline_index < 0:
            # Delegates to ``read`` — read corruption applies there.
            return await self.read()
        line = content[self._position : newline_index + 1]
        self._position = newline_index + 1
        return self._filesystem._corrupt_read(self._path, line)

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
    power-loss primitive; the fault knobs layer onto the durability
    model (see the module docstring).
    """

    __slots__ = (
        "_files",
        "_directories",
        "_clock",
        "_slow_disk_delay",
        "_disk_full_remaining",
        "_fsync_reorder_random",
        "_read_corruption_random",
        "_read_corruption_probability",
        "_read_corruption_glob",
        "_misdirect_random",
        "_misdirect_probability",
        "_io_error_random",
        "_io_error_probability",
        "_io_error_at_time",
        "_io_error_until_time",
    )

    def __init__(self, clock: Clock | None = None) -> None:
        self._files: dict[str, _SimFileState] = {}
        self._directories: set[str] = set()
        self._clock = clock
        self._slow_disk_delay = 0.0
        self._disk_full_remaining: int | None = None
        self._fsync_reorder_random: random.Random | None = None
        self._read_corruption_random: random.Random | None = None
        self._read_corruption_probability = 0.0
        self._read_corruption_glob: str | None = None
        self._misdirect_random: random.Random | None = None
        self._misdirect_probability = 0.0
        self._io_error_random: random.Random | None = None
        self._io_error_probability = 0.0
        self._io_error_at_time: float | None = None
        self._io_error_until_time: float | None = None

    # -- scenario-facing fault primitives --------------------------------

    def set_slow_disk(self, delay_seconds: float) -> None:
        """Every subsequent operation costs ``delay_seconds`` of
        VIRTUAL time. Requires the clock injected at construction."""
        if delay_seconds > 0.0 and self._clock is None:
            raise ValueError(
                "slow_disk needs a Clock: construct "
                "SimFilesystem(clock=...) so operations can await "
                "virtual time"
            )
        self._slow_disk_delay = delay_seconds

    def clear_slow_disk(self) -> None:
        self._slow_disk_delay = 0.0

    def set_disk_full(self, remaining_bytes: int) -> None:
        """Writes beyond ``remaining_bytes`` raise ``OSError(ENOSPC)``."""
        self._disk_full_remaining = remaining_bytes

    def clear_disk_full(self) -> None:
        self._disk_full_remaining = None

    def set_fsync_reorder(self, seed: int) -> None:
        """Arm reordering-crash semantics for the next ``crash()``."""
        self._fsync_reorder_random = random.Random(seed)

    def set_read_corruption(
        self,
        seed: int,
        probability: float,
        path_glob: str | None = None,
    ) -> None:
        """Arm runtime read corruption: bitrot surfacing at READ time.

        Every subsequent read operation — ``read_bytes`` /
        ``read_text`` / handle ``read`` / ``readline`` — whose path
        matches ``path_glob`` (``None`` matches every path;
        ``Path.match`` semantics, same as ``list_directory``) flips one
        seeded byte of the RETURNED bytes with ``probability`` per
        operation. Stored state is NEVER mutated: disarming via
        ``clear_read_corruption`` restores clean reads, exactly like a
        flaky read path (latent sector, bad cable, DRAM bit) over an
        intact platter. Production analog: bitrot detected at read
        time — the invariant this knob exists for is that a
        CRC-failing read is a LOUD failure or a clean
        recovery-truncation, never silently-applied wrong state.
        Non-matching paths consume no RNG draws (scoping means the
        knob does not touch them at all); every matching read consumes
        exactly one probability draw, plus index/mask draws on a hit.
        """
        if not 0.0 <= probability <= 1.0:
            raise ValueError(
                "read corruption probability must be within [0.0, 1.0]"
            )
        self._read_corruption_random = random.Random(seed)
        self._read_corruption_probability = probability
        self._read_corruption_glob = path_glob

    def clear_read_corruption(self) -> None:
        self._read_corruption_random = None
        self._read_corruption_probability = 0.0
        self._read_corruption_glob = None

    def set_misdirect(self, seed: int, probability: float) -> None:
        """Arm misdirected IO: the kernel/FS wrote-or-read-the-wrong-
        place fault class.

        With ``probability`` per PATH-level operation, a write
        (``append_fsync`` / ``atomic_write``) applies its content to a
        seeded SIBLING file (same parent directory) instead of its
        target — the target is untouched — and a ``read_bytes`` /
        ``read_text`` is satisfied from a seeded sibling. Production
        analog: firmware/kernel misdirected IO — a block written to or
        fetched from the wrong location. The class is narrowed to
        same-directory files because this layout is file-per-purpose
        (WAL segments, submission files, incarnation store): sibling
        confusion — one WAL segment's bytes landing in another — is the
        production-possible shape; cross-directory confusion has no
        single mechanism above the raw-sector layer. The invariant it
        protects: foreign bytes are caught by record framing + CRC +
        file-format headers, never interpreted as valid state.

        Deliberate scoping, each production-justified:

        * An operation whose target has no sibling proceeds correctly —
          a lone file has no neighbor to hit (the probability draw is
          still consumed, so the RNG stream is uniform per armed op).
        * Handle-based sequential IO (``open``/``read``/``write`` on a
          ``SimFileHandle``, ``write_flush``) is exempt: an open fd is
          bound to its inode; misdirection strikes at the path/block
          layer, which the path-level entry points model.
        * A read of a MISSING target still raises ``FileNotFoundError``
          (no draw consumed): path resolution fails before any device
          IO — the namei stage is not misdirectable.
        """
        if not 0.0 <= probability <= 1.0:
            raise ValueError(
                "misdirect probability must be within [0.0, 1.0]"
            )
        self._misdirect_random = random.Random(seed)
        self._misdirect_probability = probability

    def clear_misdirect(self) -> None:
        self._misdirect_random = None
        self._misdirect_probability = 0.0

    def set_io_error(
        self,
        seed: int,
        probability: float,
        at_time: float | None = None,
        until_time: float | None = None,
    ) -> None:
        """Arm transient device IO errors (EIO).

        Each subsequent operation inside the VIRTUAL-time window
        ``[at_time, until_time)`` draws with ``probability`` and raises
        ``OSError(5, "Input/output error")`` on a hit. ``None`` bounds
        are unbounded on that side; both ``None`` means always armed. A
        windowed schedule requires the clock injected at construction
        (the same contract as ``set_slow_disk``). Production analog:
        transient device/controller errors — the invariant this knob
        exists for is that an EIO is retried or escalates LOUDLY, never
        a silent skip. A failing operation has no effect: it lands no
        bytes and consumes no disk-full budget (the write never reached
        the platter). Draws are consumed only inside the window, so the
        raise pattern is a deterministic function of (seed, operation
        order, virtual time).
        """
        if not 0.0 <= probability <= 1.0:
            raise ValueError(
                "io_error probability must be within [0.0, 1.0]"
            )
        if (at_time is not None or until_time is not None) and (
            self._clock is None
        ):
            raise ValueError(
                "a windowed io_error needs a Clock: construct "
                "SimFilesystem(clock=...) so the window can key on "
                "virtual time"
            )
        if (
            at_time is not None
            and until_time is not None
            and until_time <= at_time
        ):
            raise ValueError(
                "io_error until_time must be strictly after at_time "
                f"(got at_time={at_time}, until_time={until_time})"
            )
        self._io_error_random = random.Random(seed)
        self._io_error_probability = probability
        self._io_error_at_time = at_time
        self._io_error_until_time = until_time

    def clear_io_error(self) -> None:
        self._io_error_random = None
        self._io_error_probability = 0.0
        self._io_error_at_time = None
        self._io_error_until_time = None

    def crash(self) -> None:
        """Model power loss.

        Default: every file loses its un-fsynced volatile tail (files
        never fsynced collapse to empty — directory entries whose data
        never reached the platter). With ``set_fsync_reorder`` armed: a
        seeded SUBSET of each file's volatile segments survives
        instead, and the last survivor is torn at a seeded byte offset
        — the surviving junk becomes on-disk content the recovery
        paths must tolerate. Durable content is never touched.
        """
        reorder_random = self._fsync_reorder_random
        for state in self._files.values():
            if reorder_random is None or not state.volatile_segments:
                state.collapse_to_durable()
                continue

            surviving_segments = [
                segment
                for segment in state.volatile_segments
                if reorder_random.random() < 0.5
            ]
            if surviving_segments:
                last_segment = surviving_segments[-1]
                torn_length = reorder_random.randrange(0, len(last_segment) + 1)
                surviving_segments[-1] = last_segment[:torn_length]

            state.durable_content += b"".join(surviving_segments)
            state.volatile_segments.clear()

    # -- restart support: durable state across process generations -------

    def dump_durable(self) -> dict:
        """Serialize durable state (post-``crash()`` survivors) for a
        coordinator-driven restart. Sorted for determinism."""
        return {
            "files": {
                path: state.durable_content
                for path, state in sorted(self._files.items())
            },
            "directories": sorted(self._directories),
        }

    def restore_durable(self, durable_state: dict) -> None:
        """Seed this (fresh) filesystem with a prior generation's
        durable state — the disk that survived the reboot."""
        for path, content in durable_state["files"].items():
            state = _SimFileState()
            state.durable_content = content
            self._files[path] = state
        self._directories.update(durable_state["directories"])

    # -- fault application (internal) ------------------------------------

    async def _charge_operation(self, write_bytes: int = 0) -> None:
        # Stage order is deliberate: the latency cost is paid first (a
        # failing device still made the caller wait), then the device
        # can fail with EIO (before any budget accounting — a failed
        # write never reached the platter), then the byte budget.
        await self._charge_latency_and_device_faults()
        if write_bytes > 0 and self._disk_full_remaining is not None:
            if write_bytes > self._disk_full_remaining:
                raise OSError(28, "No space left on device")
            self._disk_full_remaining -= write_bytes

    async def _charge_latency_and_device_faults(self) -> None:
        if self._slow_disk_delay > 0.0:
            await self._clock.sleep(self._slow_disk_delay)
        if self._io_error_random is not None and self._io_error_window_active():
            if self._io_error_random.random() < self._io_error_probability:
                raise OSError(5, "Input/output error")

    def _io_error_window_active(self) -> bool:
        """Whether virtual time is inside the armed EIO window.

        Window-less arming (both bounds ``None``) is always active and
        needs no clock; a windowed schedule reads the injected clock
        (guaranteed present by ``set_io_error``'s validation).
        """
        if self._io_error_at_time is None and self._io_error_until_time is None:
            return True
        current_virtual_time = self._clock.monotonic()
        if (
            self._io_error_at_time is not None
            and current_virtual_time < self._io_error_at_time
        ):
            return False
        return (
            self._io_error_until_time is None
            or current_virtual_time < self._io_error_until_time
        )

    def _corrupt_read(self, path: str, data: bytes) -> bytes:
        """Apply armed read corruption to bytes leaving a read op.

        Returns ``data`` untouched when disarmed or when the path falls
        outside the glob scope (no RNG draws consumed — scoping means
        the knob does not see the operation). A matching read consumes
        one probability draw; on a hit, one seeded byte of the RETURNED
        copy is XOR-flipped with a seeded non-zero mask (the byte
        always changes). Zero-length reads pass unchanged after the
        draw — nothing to rot. Stored state is never mutated.
        """
        if self._read_corruption_random is None:
            return data
        if self._read_corruption_glob is not None and not Path(path).match(
            self._read_corruption_glob
        ):
            return data
        if (
            self._read_corruption_random.random()
            >= self._read_corruption_probability
        ):
            return data
        if not data:
            return data
        corrupt_index = self._read_corruption_random.randrange(len(data))
        flip_mask = self._read_corruption_random.randrange(1, 256)
        return (
            data[:corrupt_index]
            + bytes([data[corrupt_index] ^ flip_mask])
            + data[corrupt_index + 1 :]
        )

    def _draw_misdirect_sibling(self, path: str) -> str | None:
        """Draw the misdirection target for one path-level operation.

        Returns ``None`` when disarmed, when the per-op probability
        draw misses, or when the target has no sibling file in its
        parent directory (a lone file has no neighbor to hit — the
        probability draw is still consumed so the stream stays uniform
        per armed op). Sibling choice is seeded over the SORTED sibling
        list — deterministic under replay.
        """
        if self._misdirect_random is None:
            return None
        if self._misdirect_random.random() >= self._misdirect_probability:
            return None
        parent_directory = Path(path).parent
        sibling_paths = sorted(
            candidate_path
            for candidate_path in self._files
            if candidate_path != path
            and Path(candidate_path).parent == parent_directory
        )
        if not sibling_paths:
            return None
        return self._misdirect_random.choice(sibling_paths)

    # -- Filesystem protocol ---------------------------------------------

    async def open(self, path: str | Path, mode: str) -> SimFileHandle:
        await self._charge_operation()
        key = str(path)
        state = self._files.get(key)

        if "r" in mode and "+" not in mode:
            if state is None:
                raise FileNotFoundError(2, "No such file or directory", key)
            return SimFileHandle(state, mode, self, key)

        if state is None:
            state = _SimFileState()
            self._files[key] = state
            self._register_parents(Path(path))
        if "w" in mode:
            state.durable_content = b""
            state.volatile_segments.clear()

        return SimFileHandle(state, mode, self, key)

    async def write_flush(
        self,
        handle: SimFileHandle,
        data: bytes,
        *,
        flush: bool = True,
        fsync: bool = False,
    ) -> int:
        # One durability unit = ONE charge (the handle path would
        # double-charge), appended directly.
        await self._charge_operation(write_bytes=len(data))
        handle._state.volatile_segments.append(bytes(data))
        if fsync:
            handle._state.promote_volatile()
        return len(data)

    async def fsync(self, handle: SimFileHandle) -> None:
        await self._charge_operation()
        handle._state.promote_volatile()

    async def file_size(self, path: str | Path) -> int:
        await self._charge_operation()
        state = self._files.get(str(path))
        if state is None:
            raise FileNotFoundError(2, "No such file or directory", str(path))
        return len(state.visible_content)

    async def fsync_directory(self, path: str | Path) -> None:
        # Directory entries (renames, creations) are modeled as
        # immediately durable in v1; the fsync_reorder fault will hook
        # here to make un-synced renames crash-revertible.
        return None

    async def append_fsync(self, path: str | Path, data: bytes) -> None:
        await self._charge_latency_and_device_faults()
        # A full device takes what fits and THEN fails, like a real
        # short write followed by ENOSPC: the written prefix sits in the
        # page cache (visible, never fsynced -- power loss drops it).
        fitting_bytes = len(data)
        if self._disk_full_remaining is not None:
            fitting_bytes = min(fitting_bytes, self._disk_full_remaining)
            self._disk_full_remaining -= fitting_bytes
        key = str(path)
        # Misdirected IO (armed via ``set_misdirect``): the append —
        # bytes AND durability barrier — lands on a seeded sibling; the
        # intended path is untouched (and not created: its bytes went
        # elsewhere).
        misdirected_path = self._draw_misdirect_sibling(key)
        if misdirected_path is not None:
            key = misdirected_path
        state = self._files.get(key)
        if state is None:
            state = _SimFileState()
            self._files[key] = state
            self._register_parents(Path(key))
        if fitting_bytes < len(data):
            state.volatile_segments.append(bytes(data[:fitting_bytes]))
            raise OSError(28, "No space left on device")
        state.volatile_segments.append(bytes(data))
        state.promote_volatile()

    async def truncate(self, path: str | Path, length: int) -> None:
        await self._charge_latency_and_device_faults()
        key = str(path)
        state = self._files.get(key)
        if state is None:
            raise FileNotFoundError(2, "No such file or directory", key)
        state.durable_content = state.visible_content[:length]
        state.volatile_segments.clear()

    async def atomic_write(self, path: str | Path, data: bytes) -> None:
        await self._charge_operation(write_bytes=len(data))
        key = str(path)
        # Misdirected IO: the whole-file replacement clobbers a seeded
        # sibling instead of its target (a rename landing on the wrong
        # directory entry); the intended path is untouched.
        misdirected_path = self._draw_misdirect_sibling(key)
        if misdirected_path is not None:
            key = misdirected_path
        state = self._files.get(key)
        if state is None:
            state = _SimFileState()
            self._files[key] = state
            self._register_parents(Path(key))
        # All-or-nothing: the complete new content, durable at once.
        state.durable_content = bytes(data)
        state.volatile_segments.clear()

    async def read_bytes(self, path: str | Path) -> bytes:
        await self._charge_operation()
        key = str(path)
        state = self._files.get(key)
        if state is None:
            # Path resolution fails before any device IO — the namei
            # stage is not misdirectable, so a missing target is a
            # plain FileNotFoundError even with misdirect armed.
            raise FileNotFoundError(2, "No such file or directory", key)
        # Misdirected IO: the fetch is satisfied from a seeded sibling.
        misdirected_path = self._draw_misdirect_sibling(key)
        if misdirected_path is not None:
            key = misdirected_path
            state = self._files[key]
        # Read corruption composes after misdirection, matched against
        # the path actually fetched — the rot lives on the physical
        # blocks the device returned.
        return self._corrupt_read(key, state.visible_content)

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

    async def list_subdirectories(self, path: str | Path) -> list[Path]:
        directory = Path(path)
        return sorted(
            {
                Path(known_directory)
                for known_directory in self._directories
                if Path(known_directory).parent == directory
            }
        )

    async def remove(self, path: str | Path) -> None:
        key = str(path)
        if key not in self._files:
            raise FileNotFoundError(2, "No such file or directory", key)
        del self._files[key]

    async def remove_directory(self, path: str | Path) -> None:
        directory = Path(path)
        key = str(directory)
        if key not in self._directories:
            raise FileNotFoundError(2, "No such file or directory", key)
        has_children = any(
            Path(file_path).parent == directory for file_path in self._files
        ) or any(
            Path(known_directory).parent == directory
            for known_directory in self._directories
            if known_directory != key
        )
        if has_children:
            raise OSError(39, "Directory not empty", key)
        self._directories.discard(key)

    def _register_parents(self, path: Path) -> None:
        for parent in path.parents:
            self._directories.add(str(parent))
