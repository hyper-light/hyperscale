"""``SnapshotManager`` -- pickled under the namespace
``hyperscale.distributed.raft.snapshot`` (see that module)."""

from typing import TYPE_CHECKING

from .logging_models import RaftDebug, RaftInfo, RaftWarning
from .models.raft_configuration import RaftConfiguration
from .install_snapshot import InstallSnapshot
from .raft_snapshot import RaftSnapshot

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from .raft_log import RaftLog


class SnapshotManager:
    """
    Manages Raft snapshot lifecycle: creation, application, and log compaction.

    Usage:
        manager = SnapshotManager(logger=logger, node_id="node-1")

        # Compact through an applied index
        snapshot = await manager.create_snapshot(raft_log, state_bytes, index, configuration)
        manager.compact_log(raft_log)

        # Apply received snapshot on follower
        await manager.apply_snapshot(raft_log, snapshot)
    """

    __slots__ = (
        "_logger",
        "_node_id",
        "_current_snapshot",
    )

    def __init__(
        self,
        logger: "Logger",
        node_id: str,
    ) -> None:
        self._logger = logger
        self._node_id = node_id
        self._current_snapshot: RaftSnapshot | None = None

    @property
    def current_snapshot(self) -> RaftSnapshot | None:
        """The most recent snapshot, or None if no snapshot exists."""
        return self._current_snapshot

    async def create_snapshot(
        self,
        raft_log: "RaftLog",
        state_data: bytes,
        last_applied_index: int,
        configuration: RaftConfiguration,
    ) -> RaftSnapshot | None:
        """
        Create a snapshot at the given applied index.

        Captures the serialized application state and the configuration in
        force at that index, and records the log position. Returns None if
        the index is invalid.
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
            configuration=configuration,
        )
        self._current_snapshot = snapshot

        await self._logger.log(RaftDebug(
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

        removed = raft_log.compact_through(
            self._current_snapshot.last_included_index,
            self._current_snapshot.last_included_term,
        )
        return removed

    async def apply_snapshot(
        self,
        raft_log: "RaftLog",
        snapshot: RaftSnapshot,
    ) -> bool:
        """
        Apply a received snapshot (from InstallSnapshot RPC).

        Raft section 7: when the log holds the snapshot's last included
        entry (same index and term), the entries after it are kept;
        otherwise the whole log is discarded -- entries that conflict with
        committed state must not survive it.

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
        if raft_log.term_at(snapshot.last_included_index) != snapshot.last_included_term:
            raft_log.truncate_from(raft_log.snapshot_index + 1)
        raft_log.compact_through(
            snapshot.last_included_index,
            snapshot.last_included_term,
        )

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
            configuration=self._current_snapshot.configuration.dump(),
            data=self._current_snapshot.state_data,
        )

    def clear(self) -> None:
        """Release the current snapshot. Called on node destroy."""
        self._current_snapshot = None
