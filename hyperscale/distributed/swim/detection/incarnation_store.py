"""
Persistent incarnation storage for SWIM protocol.

Provides file-based persistence for incarnation numbers to ensure nodes
can safely rejoin the cluster with an incarnation higher than any they
previously used. This prevents the "zombie node" problem where a stale
node could claim operations with old incarnation numbers.

Key features:
- Atomic writes using rename for crash safety
- Async-compatible synchronous I/O (file writes are fast)
- Automatic directory creation
- Graceful fallback if storage unavailable

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable
from hyperscale.distributed.runtime import Clock, Filesystem, RealClock, RealFilesystem
from hyperscale.distributed.swim.core.protocols import LoggerProtocol
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerError, ServerInfo, ServerWarning

from .incarnation_record import IncarnationRecord

_DEFAULT_CLOCK: Clock = RealClock()

# Module-level storage seam (Phase 7). The store BORROWS this (or an
# injected instance) — it never shuts the filesystem down.
# ``swap_defaults`` rebinds it to the SIM filesystem so incarnation
# persistence becomes deterministic and storage-faultable under replay.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()


@dataclass
class IncarnationStore:
    """
    Persistent storage for incarnation numbers.

    Stores incarnation numbers to disk so that nodes can safely rejoin
    with an incarnation number higher than any previously used. This
    prevents split-brain scenarios where a crashed-and-restarted node
    could use stale incarnation numbers.

    Storage format:
    - Single JSON file per node
    - Atomic writes via rename
    - Contains incarnation, timestamp, and node address

    Thread/Async Safety:
    - Uses asyncio lock for concurrent access
    - File I/O is synchronous but fast (single small JSON)
    """

    storage_directory: Path
    node_address: str

    # Minimum incarnation bump on restart to ensure freshness
    restart_incarnation_bump: int = 10

    # Storage seam (Phase 7): borrowed, never shut down here. All disk
    # touches route through it — crash-safe atomic writes, off-loop by
    # construction (the previous inline pathlib IO ran synchronously ON
    # the event loop and skipped every fsync).
    filesystem: Filesystem | None = None

    # Logger for debugging
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0

    # Internal state
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _current_record: IncarnationRecord | None = field(default=None, init=False)
    _initialized: bool = field(default=False, init=False)

    # Persistence-degradation truth (the anti-swallow contract): the
    # LIVE incarnation must advance regardless of disk health — protocol
    # monotonicity cannot wait on storage — but a failed save means the
    # PERSISTED value is stale, eroding the restart zombie-guard margin
    # this store exists for. That state is tracked here, logged loudly
    # on every transition, and exposed via ``persistence_degraded`` /
    # ``get_stats`` instead of being silently absorbed.
    _persist_failure_count: int = field(default=0, init=False)
    _persistence_degraded: bool = field(default=False, init=False)

    def __post_init__(self):
        self._lock = asyncio.Lock()
        if self.filesystem is None:
            self.filesystem = _DEFAULT_FILESYSTEM

    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
    ) -> None:
        """Set logger for structured logging."""
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port

    @property
    def _storage_path(self) -> Path:
        """Get the path to this node's incarnation file."""
        safe_address = self.node_address.replace(":", "_").replace("/", "_")
        return self.storage_directory / f"incarnation_{safe_address}.json"

    async def initialize(self) -> int:
        """
        Initialize the store and return the starting incarnation.

        If a previous incarnation is found on disk, returns that value
        plus restart_incarnation_bump to ensure freshness. Otherwise
        returns restart_incarnation_bump (not 0, to be safe).

        Returns:
            The initial incarnation number to use.
        """
        async with self._lock:
            if self._initialized:
                return (
                    self._current_record.incarnation
                    if self._current_record
                    else self.restart_incarnation_bump
                )

            try:
                await self.filesystem.mkdir(
                    self.storage_directory, parents=True, exist_ok=True
                )
            except OSError as error:
                await self._log_warning(
                    f"Failed to create incarnation storage directory: {error}"
                )
                self._initialized = True
                return self.restart_incarnation_bump

            loaded_record = await self._load_from_disk()

            if loaded_record:
                # Bump incarnation on restart to ensure we're always fresh
                new_incarnation = (
                    loaded_record.incarnation + self.restart_incarnation_bump
                )
                self._current_record = IncarnationRecord(
                    incarnation=new_incarnation,
                    last_updated_at=_DEFAULT_CLOCK.time(),
                    node_address=self.node_address,
                )
                await self._save_to_disk(self._current_record)
                await self._log_debug(
                    f"Loaded persisted incarnation {loaded_record.incarnation}, "
                    f"starting at {new_incarnation}"
                )
            else:
                # First time - start with restart_incarnation_bump
                self._current_record = IncarnationRecord(
                    incarnation=self.restart_incarnation_bump,
                    last_updated_at=_DEFAULT_CLOCK.time(),
                    node_address=self.node_address,
                )
                await self._save_to_disk(self._current_record)
                await self._log_debug(
                    f"No persisted incarnation found, starting at {self.restart_incarnation_bump}"
                )

            self._initialized = True
            return self._current_record.incarnation

    async def get_incarnation(self) -> int:
        """Get the current persisted incarnation."""
        async with self._lock:
            if self._current_record:
                return self._current_record.incarnation
            return 0

    @property
    def persistence_degraded(self) -> bool:
        """True while the most recent save attempt failed: the LIVE
        incarnation is ahead of the PERSISTED one, so a reboot in this
        state starts from a stale value and the restart bump's
        zombie-guard margin is eroded. Heals on the next successful
        save (every accepted ``update_incarnation`` attempts one)."""
        return self._persistence_degraded

    async def update_incarnation(self, new_incarnation: int) -> bool:
        """
        Update the persisted incarnation number.

        Only updates if the new value is higher than the current one.
        This ensures monotonicity of incarnation numbers.

        The returned bool is the MONOTONICITY verdict only: the live
        record always advances on acceptance, because protocol
        correctness cannot wait on storage. Whether the accepted value
        actually reached disk is tracked separately — a failed save
        flips ``persistence_degraded`` and logs loudly rather than
        silently reporting success.

        Args:
            new_incarnation: The new incarnation number.

        Returns:
            True if accepted (higher), False if rejected (not higher).
        """
        async with self._lock:
            current = self._current_record.incarnation if self._current_record else 0

            if new_incarnation <= current:
                return False

            self._current_record = IncarnationRecord(
                incarnation=new_incarnation,
                last_updated_at=_DEFAULT_CLOCK.time(),
                node_address=self.node_address,
            )

            await self._save_to_disk(self._current_record)
            return True

    async def get_last_death_timestamp(self) -> float | None:
        """
        Get the timestamp of the last incarnation update.

        This can be used to detect zombie nodes - if a node died recently
        and is trying to rejoin with a low incarnation, it may be stale.

        Returns:
            Timestamp of last update, or None if unknown.
        """
        async with self._lock:
            if self._current_record:
                return self._current_record.last_updated_at
            return None

    async def _load_from_disk(self) -> IncarnationRecord | None:
        """Load incarnation record from disk."""
        try:
            if not await self.filesystem.exists(self._storage_path):
                return None

            content = await self.filesystem.read_text(
                self._storage_path, encoding="utf-8"
            )
            data = json.loads(content)

            return IncarnationRecord(
                incarnation=data["incarnation"],
                last_updated_at=data["last_updated_at"],
                node_address=data["node_address"],
            )
        except (OSError, json.JSONDecodeError, KeyError) as error:
            await self._log_warning(f"Failed to load incarnation from disk: {error}")
            return None

    async def _save_to_disk(self, record: IncarnationRecord) -> bool:
        """
        Save incarnation record to disk atomically.

        ``Filesystem.atomic_write`` performs the FULL crash-consistency
        sequence — temp file, flush, fsync, atomic rename, parent-
        directory fsync — off the event loop. The previous inline
        temp-then-rename skipped both fsyncs (a lost/torn-write window
        on power failure that undermined the zombie-prevention
        guarantee this store exists for) and blocked the loop.

        Failure is CONTAINED but never silent: the degraded transition
        logs at ERROR with the eroded-zombie-guard consequence spelled
        out, every repeat is counted, and recovery logs the heal — the
        pre-fix behavior logged a WARNING per failure and reported
        nothing to any caller or diagnostic surface while the persisted
        value went stale under the advancing live incarnation.
        """
        try:
            data = {
                "incarnation": record.incarnation,
                "last_updated_at": record.last_updated_at,
                "node_address": record.node_address,
            }

            await self.filesystem.atomic_write(
                self._storage_path,
                json.dumps(data).encode("utf-8"),
            )
        except OSError as error:
            self._persist_failure_count += 1
            if not self._persistence_degraded:
                self._persistence_degraded = True
                await self._log_error(
                    f"Incarnation persistence DEGRADED: save of "
                    f"incarnation {record.incarnation} failed "
                    f"({type(error).__name__}: {error}); the live "
                    "incarnation is now ahead of disk — a reboot in "
                    "this state starts from a stale value and erodes "
                    "the restart zombie-guard margin. Every accepted "
                    "update retries; recovery will be logged."
                )
            else:
                await self._log_warning(
                    f"Incarnation save still failing "
                    f"({type(error).__name__}); "
                    f"{self._persist_failure_count} failures since "
                    "degradation"
                )
            return False

        if self._persistence_degraded:
            self._persistence_degraded = False
            await self._log_info(
                f"Incarnation persistence RECOVERED at incarnation "
                f"{record.incarnation} after "
                f"{self._persist_failure_count} failed save(s)"
            )
        return True

    async def _log_debug(self, message: str) -> None:
        """Log a debug message."""
        if self._logger:
            await self._logger.log(
                ServerDebug(
                    message=f"[IncarnationStore] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self.node_address,
                )
            )

    async def _log_info(self, message: str) -> None:
        """Log an info message."""
        if self._logger:
            await self._logger.log(
                ServerInfo(
                    message=f"[IncarnationStore] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self.node_address,
                )
            )

    async def _log_warning(self, message: str) -> None:
        """Log a warning message."""
        if self._logger:
            await self._logger.log(
                ServerWarning(
                    message=f"[IncarnationStore] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self.node_address,
                )
            )

    async def _log_error(self, message: str) -> None:
        """Log an error message."""
        if self._logger:
            await self._logger.log(
                ServerError(
                    message=f"[IncarnationStore] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self.node_address,
                )
            )

    def get_stats(self) -> dict:
        """Get storage statistics."""
        return {
            "initialized": self._initialized,
            "current_incarnation": self._current_record.incarnation
            if self._current_record
            else 0,
            "last_updated_at": self._current_record.last_updated_at
            if self._current_record
            else 0,
            "storage_path": str(self._storage_path),
            "restart_bump": self.restart_incarnation_bump,
            "persistence_degraded": self._persistence_degraded,
            "persist_failure_count": self._persist_failure_count,
        }

_REHOMED = (
    IncarnationRecord,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
