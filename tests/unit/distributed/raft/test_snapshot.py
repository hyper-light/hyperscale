"""
Unit tests for Raft snapshot support.

Tests snapshot creation, application, log compaction,
and InstallSnapshot message serialization.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.raft.raft_log import RaftLog
from hyperscale.distributed.raft.snapshot import (
    InstallSnapshot,
    InstallSnapshotResponse,
    RaftSnapshot,
    SnapshotManager,
)


@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.log = AsyncMock()
    return logger


@pytest.fixture
def snapshot_manager(mock_logger):
    return SnapshotManager(
        logger=mock_logger,
        node_id="node-1",
        compaction_threshold=5,
    )


@pytest.fixture
def populated_log():
    """Create a RaftLog with 10 entries at term 1."""
    raft_log = RaftLog()
    for i in range(1, 11):
        raft_log.append(term=1, command=f"cmd-{i}".encode(), command_type="TEST", job_id="job-1")
    return raft_log


class TestRaftSnapshot:
    """Tests for RaftSnapshot dataclass."""

    def test_snapshot_fields(self) -> None:
        snap = RaftSnapshot(last_included_index=5, last_included_term=2, state_data=b"state")
        assert snap.last_included_index == 5
        assert snap.last_included_term == 2
        assert snap.state_data == b"state"


class TestInstallSnapshotMessages:
    """Tests for InstallSnapshot and InstallSnapshotResponse serialization."""

    def test_install_snapshot_roundtrip(self) -> None:
        msg = InstallSnapshot(
            job_id="job-1",
            term=3,
            leader_id="node-1",
            last_included_index=10,
            last_included_term=2,
            data=b"snapshot-data",
        )
        serialized = msg.dump()
        recovered = InstallSnapshot.load(serialized)
        assert recovered.job_id == "job-1"
        assert recovered.term == 3
        assert recovered.leader_id == "node-1"
        assert recovered.last_included_index == 10
        assert recovered.last_included_term == 2
        assert recovered.data == b"snapshot-data"

    def test_install_snapshot_response_roundtrip(self) -> None:
        msg = InstallSnapshotResponse(job_id="job-1", term=3, success=True)
        serialized = msg.dump()
        recovered = InstallSnapshotResponse.load(serialized)
        assert recovered.job_id == "job-1"
        assert recovered.term == 3
        assert recovered.success is True


class TestSnapshotManager:
    """Tests for SnapshotManager lifecycle."""

    def test_initial_state(self, snapshot_manager: SnapshotManager) -> None:
        assert snapshot_manager.current_snapshot is None

    def test_should_compact_below_threshold(self, snapshot_manager: SnapshotManager) -> None:
        raft_log = RaftLog()
        for i in range(3):
            raft_log.append(term=1, command=b"x", command_type="T", job_id="j")
        assert snapshot_manager.should_compact(raft_log) is False

    def test_should_compact_above_threshold(
        self, snapshot_manager: SnapshotManager, populated_log: RaftLog
    ) -> None:
        assert snapshot_manager.should_compact(populated_log) is True

    @pytest.mark.asyncio
    async def test_create_snapshot(
        self, snapshot_manager: SnapshotManager, populated_log: RaftLog
    ) -> None:
        snapshot = await snapshot_manager.create_snapshot(
            populated_log,
            state_data=b"serialized-state",
            last_applied_index=7,
        )
        assert snapshot is not None
        assert snapshot.last_included_index == 7
        assert snapshot.last_included_term == 1
        assert snapshot.state_data == b"serialized-state"
        assert snapshot_manager.current_snapshot is snapshot

    @pytest.mark.asyncio
    async def test_create_snapshot_invalid_index(
        self, snapshot_manager: SnapshotManager, populated_log: RaftLog
    ) -> None:
        snapshot = await snapshot_manager.create_snapshot(
            populated_log,
            state_data=b"state",
            last_applied_index=999,
        )
        assert snapshot is None

    def test_compact_log(
        self, snapshot_manager: SnapshotManager, populated_log: RaftLog
    ) -> None:
        # Must have a snapshot first
        snapshot_manager._current_snapshot = RaftSnapshot(
            last_included_index=5, last_included_term=1, state_data=b""
        )
        removed = snapshot_manager.compact_log(populated_log)
        assert removed > 0
        # After compaction, entries 1-5 should be gone
        assert populated_log.get(5) is None

    def test_compact_log_no_snapshot(
        self, snapshot_manager: SnapshotManager, populated_log: RaftLog
    ) -> None:
        removed = snapshot_manager.compact_log(populated_log)
        assert removed == 0

    @pytest.mark.asyncio
    async def test_apply_snapshot(
        self, snapshot_manager: SnapshotManager, populated_log: RaftLog
    ) -> None:
        snapshot = RaftSnapshot(
            last_included_index=8,
            last_included_term=1,
            state_data=b"new-state",
        )
        result = await snapshot_manager.apply_snapshot(populated_log, snapshot)
        assert result is True
        assert snapshot_manager.current_snapshot is snapshot

    @pytest.mark.asyncio
    async def test_apply_stale_snapshot(
        self, snapshot_manager: SnapshotManager, populated_log: RaftLog
    ) -> None:
        # Install a snapshot at index 8
        snapshot_manager._current_snapshot = RaftSnapshot(
            last_included_index=8, last_included_term=1, state_data=b""
        )
        # Try to install older snapshot
        stale = RaftSnapshot(
            last_included_index=5,
            last_included_term=1,
            state_data=b"old",
        )
        result = await snapshot_manager.apply_snapshot(populated_log, stale)
        assert result is False
        assert snapshot_manager.current_snapshot.last_included_index == 8

    def test_build_install_snapshot_message(
        self, snapshot_manager: SnapshotManager
    ) -> None:
        snapshot_manager._current_snapshot = RaftSnapshot(
            last_included_index=10,
            last_included_term=2,
            state_data=b"state-bytes",
        )
        msg = snapshot_manager.build_install_snapshot_message(
            job_id="job-1", current_term=3, leader_id="node-1"
        )
        assert msg is not None
        assert msg.job_id == "job-1"
        assert msg.term == 3
        assert msg.last_included_index == 10
        assert msg.data == b"state-bytes"

    def test_build_install_snapshot_no_snapshot(
        self, snapshot_manager: SnapshotManager
    ) -> None:
        msg = snapshot_manager.build_install_snapshot_message(
            job_id="job-1", current_term=1, leader_id="node-1"
        )
        assert msg is None

    def test_clear(self, snapshot_manager: SnapshotManager) -> None:
        snapshot_manager._current_snapshot = RaftSnapshot(
            last_included_index=5, last_included_term=1, state_data=b""
        )
        snapshot_manager.clear()
        assert snapshot_manager.current_snapshot is None
