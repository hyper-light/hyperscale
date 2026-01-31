"""
Raft snapshot support for log compaction and state transfer.

Provides:
- RaftSnapshot: holds compacted state at a specific log position
- SnapshotManager: creates snapshots, applies them, compacts logs
- InstallSnapshot message for leader-to-follower state transfer

When the Raft log grows beyond a threshold, the leader creates a
snapshot of the applied state and compacts the log. Followers that
fall behind the snapshot point receive the full snapshot via
InstallSnapshot RPC instead of individual AppendEntries.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import cloudpickle

from .logging_models import RaftDebug, RaftInfo, RaftWarning
from .models.messages import Message

if TYPE_CHECKING:
    from hyperscale.logging import Logger

    from .raft_log import RaftLog


@dataclass(slots=True)
class InstallSnapshot(Message):
    """
    Raft InstallSnapshot RPC message.

    Sent by leader to followers that are too far behind the log
    to replicate via AppendEntries. Contains the full serialized
    state at the snapshot point.
    """

    job_id: str
    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    data: bytes  # Serialized snapshot state

    def dump(self) -> bytes:
        """Serialize for wire transport."""
        return cloudpickle.dumps(self)

    @classmethod
    def load(cls, data: bytes) -> "InstallSnapshot":
        """Deserialize from wire transport."""
        return cloudpickle.loads(data)


@dataclass(slots=True)
class InstallSnapshotResponse(Message):
    """Response to InstallSnapshot RPC."""

    job_id: str
    term: int
    success: bool

    def dump(self) -> bytes:
        """Serialize for wire transport."""
        return cloudpickle.dumps(self)

    @classmethod
    def load(cls, data: bytes) -> "InstallSnapshotResponse":
        """Deserialize from wire transport."""
        return cloudpickle.loads(data)


@dataclass(slots=True)
class RaftSnapshot:
    """
    Immutable snapshot of applied Raft state at a log position.

    Attributes:
        last_included_index: Log index of the last entry in the snapshot.
        last_included_term: Term of the last entry in the snapshot.
        state_data: Serialized application state at this point.
    """

    last_included_index: int
    last_included_term: int
    state_data: bytes


class SnapshotManager:
    """
    Manages Raft snapshot lifecycle: creation, application, and log compaction.

    When the log exceeds `compaction_threshold` entries, a snapshot is
    created from the current applied state and entries up to the snapshot
    point are removed from the in-memory log.

    Usage:
        manager = SnapshotManager(logger=logger, node_id="node-1")

        # Check if compaction needed after applying entries
        if manager.should_compact(raft_log):
            snapshot = manager.create_snapshot(raft_log, state_bytes)
            manager.compact_log(raft_log)

        # Apply received snapshot on follower
        manager.apply_snapshot(raft_log, snapshot)
    """

    __slots__ = (
        "_logger",
        "_node_id",
        "_compaction_threshold",
        "_current_snapshot",
    )

    def __init__(
        self,
        logger: "Logger",
        node_id: str,
        compaction_threshold: int = 10_000,
    ) -> None:
        self._logger = logger
        self._node_id = node_id
        self._compaction_threshold = compaction_threshold
        self._current_snapshot: RaftSnapshot | None = None

    @property
    def current_snapshot(self) -> RaftSnapshot | None:
        """The most recent snapshot, or None if no snapshot exists."""
        return self._current_snapshot

    def should_compact(self, raft_log: "RaftLog") -> bool:
        """
        Check if the log has grown large enough to warrant compaction.

        Returns True when the number of entries exceeds the threshold.
        """
        return len(raft_log) > self._compaction_threshold

    async def create_snapshot(
        self,
        raft_log: "RaftLog",
        state_data: bytes,
        last_applied_index: int,
    ) -> RaftSnapshot | None:
        """
        Create a snapshot at the given applied index.

        Captures the serialized application state and records the
        log position. Returns None if the index is invalid.
        """
        last_applied_term = raft_log.term_at(last_applied_index)
        if last_applied_term is None:
            await self._logger.log(RaftWarning(
                message=f"Cannot snapshot: no term at index {last_applied_index}",
                node_id=self._node_id,
            ))
            return None

        snapshot = RaftSnapshot(
            last_included_index=last_applied_index,
            last_included_term=last_applied_term,
            state_data=state_data,
        )
        self._current_snapshot = snapshot

        await self._logger.log(RaftInfo(
            message=f"Created snapshot at index={last_applied_index}, term={last_applied_term}",
            node_id=self._node_id,
        ))
        return snapshot

    def compact_log(self, raft_log: "RaftLog") -> int:
        """
        Compact the log up to the current snapshot point.

        Removes all entries at or before the snapshot's last_included_index.
        Returns the number of entries removed, or 0 if no snapshot exists.
        """
        if self._current_snapshot is None:
            return 0

        removed = raft_log.compact(self._current_snapshot.last_included_index)
        return removed

    async def apply_snapshot(
        self,
        raft_log: "RaftLog",
        snapshot: RaftSnapshot,
    ) -> bool:
        """
        Apply a received snapshot (from InstallSnapshot RPC).

        Replaces the current snapshot, resets the log's snapshot point,
        and discards all entries up to the snapshot index.

        Returns True if the snapshot was applied (newer than current).
        """
        if self._current_snapshot is not None:
            if snapshot.last_included_index <= self._current_snapshot.last_included_index:
                await self._logger.log(RaftDebug(
                    message=f"Ignoring stale snapshot at index={snapshot.last_included_index}",
                    node_id=self._node_id,
                ))
                return False

        self._current_snapshot = snapshot
        raft_log.compact(snapshot.last_included_index)

        await self._logger.log(RaftInfo(
            message=(
                f"Applied snapshot at index={snapshot.last_included_index}, "
                f"term={snapshot.last_included_term}"
            ),
            node_id=self._node_id,
        ))
        return True

    def build_install_snapshot_message(
        self,
        job_id: str,
        current_term: int,
        leader_id: str,
    ) -> InstallSnapshot | None:
        """
        Build an InstallSnapshot message from the current snapshot.

        Returns None if no snapshot exists.
        """
        if self._current_snapshot is None:
            return None

        return InstallSnapshot(
            job_id=job_id,
            term=current_term,
            leader_id=leader_id,
            last_included_index=self._current_snapshot.last_included_index,
            last_included_term=self._current_snapshot.last_included_term,
            data=self._current_snapshot.state_data,
        )

    def clear(self) -> None:
        """Release the current snapshot. Called on node destroy."""
        self._current_snapshot = None
