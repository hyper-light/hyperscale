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
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from hyperscale.distributed.swim.core.protocols import LoggerProtocol
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning


@dataclass(slots=True)
class IncarnationRecord:
    """
    Record of a node's incarnation history.

    Stores both the last known incarnation and the timestamp when it was
    last updated. The timestamp enables time-based zombie detection.
    """

    incarnation: int
    last_updated_at: float
    node_address: str


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

    # Logger for debugging
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0

    # Internal state
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _current_record: IncarnationRecord | None = field(default=None, init=False)
    _initialized: bool = field(default=False, init=False)

    def __post_init__(self):
        self._lock = asyncio.Lock()

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
                self.storage_directory.mkdir(parents=True, exist_ok=True)
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
                    last_updated_at=time.time(),
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
                    last_updated_at=time.time(),
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

    async def update_incarnation(self, new_incarnation: int) -> bool:
        """
        Update the persisted incarnation number.

        Only updates if the new value is higher than the current one.
        This ensures monotonicity of incarnation numbers.

        Args:
            new_incarnation: The new incarnation number.

        Returns:
            True if updated, False if rejected (not higher).
        """
        async with self._lock:
            current = self._current_record.incarnation if self._current_record else 0

            if new_incarnation <= current:
                return False

            self._current_record = IncarnationRecord(
                incarnation=new_incarnation,
                last_updated_at=time.time(),
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
            if not self._storage_path.exists():
                return None

            content = self._storage_path.read_text(encoding="utf-8")
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

        Uses write-to-temp-then-rename for crash safety.
        """
        try:
            data = {
                "incarnation": record.incarnation,
                "last_updated_at": record.last_updated_at,
                "node_address": record.node_address,
            }

            temp_path = self._storage_path.with_suffix(".tmp")
            temp_path.write_text(json.dumps(data), encoding="utf-8")
            temp_path.rename(self._storage_path)
            return True
        except OSError as error:
            await self._log_warning(f"Failed to save incarnation to disk: {error}")
            return False

    async def _log_debug(self, message: str) -> None:
        """Log a debug message."""
        if self._logger:
            try:
                await self._logger.log(
                    ServerDebug(
                        message=f"[IncarnationStore] {message}",
                        node_host=self._node_host,
                        node_port=self._node_port,
                        node_id=0,
                    )
                )
            except Exception:
                pass

    async def _log_warning(self, message: str) -> None:
        """Log a warning message."""
        if self._logger:
            try:
                await self._logger.log(
                    ServerWarning(
                        message=f"[IncarnationStore] {message}",
                        node_host=self._node_host,
                        node_port=self._node_port,
                        node_id=0,
                    )
                )
            except Exception:
                pass

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
        }
