"""
Unit tests for ReplicatedStatsStore.

Tests the local buffer, bounded capacity, flush timing, and cleanup.
"""

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.raft.replicated_stats_store import ReplicatedStatsStore


@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.log = AsyncMock()
    return logger


@pytest.fixture
def stats_store(mock_logger):
    return ReplicatedStatsStore(
        logger=mock_logger,
        node_id="node-1",
        max_pending_samples=100,
        flush_interval_seconds=1.0,
    )


class TestReplicatedStatsStore:
    """Tests for ReplicatedStatsStore."""

    def test_add_progress_creates_buffer(self, stats_store: ReplicatedStatsStore) -> None:
        stats_store.add_progress("job-1", 42.0)
        assert stats_store.pending_count("job-1") == 1

    def test_add_progress_multiple(self, stats_store: ReplicatedStatsStore) -> None:
        for i in range(10):
            stats_store.add_progress("job-1", float(i))
        assert stats_store.pending_count("job-1") == 10

    def test_add_progress_custom_timestamp(self, stats_store: ReplicatedStatsStore) -> None:
        stats_store.add_progress("job-1", 1.0, timestamp=100.0)
        assert stats_store.pending_count("job-1") == 1

    def test_add_progress_bounded(self) -> None:
        """Buffer evicts oldest when exceeding max_pending_samples."""
        logger = MagicMock()
        logger.log = AsyncMock()
        store = ReplicatedStatsStore(
            logger=logger,
            node_id="node-1",
            max_pending_samples=5,
        )
        for i in range(10):
            store.add_progress("job-1", float(i))
        assert store.pending_count("job-1") == 5

    def test_total_pending_across_jobs(self, stats_store: ReplicatedStatsStore) -> None:
        stats_store.add_progress("job-1", 1.0)
        stats_store.add_progress("job-1", 2.0)
        stats_store.add_progress("job-2", 3.0)
        assert stats_store.total_pending == 3

    def test_pending_count_unknown_job(self, stats_store: ReplicatedStatsStore) -> None:
        assert stats_store.pending_count("nonexistent") == 0

    def test_should_flush_initially_true(self, stats_store: ReplicatedStatsStore) -> None:
        """First flush is always due (no previous flush time)."""
        assert stats_store.should_flush("job-1") is True

    @pytest.mark.asyncio
    async def test_flush_and_replicate_success(self, stats_store: ReplicatedStatsStore) -> None:
        stats_store.add_progress("job-1", 1.0)
        stats_store.add_progress("job-1", 2.0)

        mock_raft_jm = MagicMock()
        mock_raft_jm.flush_stats_window = AsyncMock(return_value=True)

        result = await stats_store.flush_and_replicate("job-1", mock_raft_jm)
        assert result is True
        mock_raft_jm.flush_stats_window.assert_called_once()

    @pytest.mark.asyncio
    async def test_flush_and_replicate_clears_buffer(
        self, stats_store: ReplicatedStatsStore
    ) -> None:
        stats_store.add_progress("job-1", 1.0)
        mock_raft_jm = MagicMock()
        mock_raft_jm.flush_stats_window = AsyncMock(return_value=True)

        await stats_store.flush_and_replicate("job-1", mock_raft_jm)
        assert stats_store.pending_count("job-1") == 0

    @pytest.mark.asyncio
    async def test_flush_empty_buffer(self, stats_store: ReplicatedStatsStore) -> None:
        mock_raft_jm = MagicMock()
        mock_raft_jm.flush_stats_window = AsyncMock(return_value=True)

        result = await stats_store.flush_and_replicate("job-1", mock_raft_jm)
        assert result is True
        mock_raft_jm.flush_stats_window.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_rejected(self, stats_store: ReplicatedStatsStore) -> None:
        stats_store.add_progress("job-1", 1.0)
        mock_raft_jm = MagicMock()
        mock_raft_jm.flush_stats_window = AsyncMock(return_value=False)

        result = await stats_store.flush_and_replicate("job-1", mock_raft_jm)
        assert result is False

    def test_cleanup_job(self, stats_store: ReplicatedStatsStore) -> None:
        stats_store.add_progress("job-1", 1.0)
        stats_store.cleanup_job("job-1")
        assert stats_store.pending_count("job-1") == 0

    def test_clear(self, stats_store: ReplicatedStatsStore) -> None:
        stats_store.add_progress("job-1", 1.0)
        stats_store.add_progress("job-2", 2.0)
        stats_store.clear()
        assert stats_store.total_pending == 0
