"""
Tests for the RaftLog in-memory log.

Covers:
1. Append and retrieval with 1-based indexing
2. Range queries
3. Truncation on conflict
4. Snapshot compaction
5. Capacity enforcement (backpressure)
6. Edge cases: empty log, out-of-range access
"""

import time

import pytest

from hyperscale.distributed.raft.models import RaftLogEntry
from hyperscale.distributed.raft.raft_log import RaftLog


# =============================================================================
# Helpers
# =============================================================================


def make_entry(term: int, index: int, command_type: str = "NO_OP") -> RaftLogEntry:
    """Create a RaftLogEntry for testing."""
    return RaftLogEntry(
        term=term,
        index=index,
        command=b"",
        command_type=command_type,
        job_id="test-job",
        timestamp=time.monotonic(),
    )


# =============================================================================
# Test Empty Log
# =============================================================================


class TestEmptyLog:
    """Tests on a freshly created RaftLog."""

    def setup_method(self) -> None:
        self.log = RaftLog("test-job")

    def test_empty_length(self) -> None:
        assert len(self.log) == 0

    def test_empty_last_index(self) -> None:
        assert self.log.last_index() == 0

    def test_empty_last_term(self) -> None:
        assert self.log.last_term() == 0

    def test_empty_get_returns_none(self) -> None:
        assert self.log.get(1) is None

    def test_empty_get_range_returns_empty(self) -> None:
        assert self.log.get_range(1, 5) == []

    def test_empty_snapshot_index(self) -> None:
        assert self.log.snapshot_index == 0

    def test_empty_snapshot_term(self) -> None:
        assert self.log.snapshot_term == 0

    def test_empty_is_not_at_capacity(self) -> None:
        assert not self.log.is_at_capacity

    def test_job_id(self) -> None:
        assert self.log.job_id == "test-job"


# =============================================================================
# Test Append and Get
# =============================================================================


class TestAppendAndGet:
    """Tests for appending entries and retrieving them."""

    def setup_method(self) -> None:
        self.log = RaftLog("test-job")

    def test_append_single(self) -> None:
        entry = make_entry(term=1, index=1)
        result = self.log.append(entry)
        assert result == 1
        assert len(self.log) == 1

    def test_append_multiple(self) -> None:
        for idx in range(1, 6):
            self.log.append(make_entry(term=1, index=idx))
        assert len(self.log) == 5

    def test_get_by_index(self) -> None:
        for idx in range(1, 4):
            self.log.append(make_entry(term=1, index=idx))

        entry = self.log.get(2)
        assert entry is not None
        assert entry.index == 2
        assert entry.term == 1

    def test_get_first_entry(self) -> None:
        self.log.append(make_entry(term=1, index=1))
        entry = self.log.get(1)
        assert entry is not None
        assert entry.index == 1

    def test_get_out_of_range_returns_none(self) -> None:
        self.log.append(make_entry(term=1, index=1))
        assert self.log.get(0) is None
        assert self.log.get(2) is None
        assert self.log.get(100) is None

    def test_last_index_after_append(self) -> None:
        for idx in range(1, 4):
            self.log.append(make_entry(term=1, index=idx))
        assert self.log.last_index() == 3

    def test_last_term_after_append(self) -> None:
        self.log.append(make_entry(term=1, index=1))
        self.log.append(make_entry(term=2, index=2))
        assert self.log.last_term() == 2


# =============================================================================
# Test Range Queries
# =============================================================================


class TestGetRange:
    """Tests for range queries."""

    def setup_method(self) -> None:
        self.log = RaftLog("test-job")
        for idx in range(1, 6):
            self.log.append(make_entry(term=1, index=idx))

    def test_full_range(self) -> None:
        entries = self.log.get_range(1, 6)
        assert len(entries) == 5
        assert entries[0].index == 1
        assert entries[-1].index == 5

    def test_partial_range(self) -> None:
        entries = self.log.get_range(2, 4)
        assert len(entries) == 2
        assert entries[0].index == 2
        assert entries[1].index == 3

    def test_single_entry_range(self) -> None:
        entries = self.log.get_range(3, 4)
        assert len(entries) == 1
        assert entries[0].index == 3

    def test_empty_range(self) -> None:
        entries = self.log.get_range(3, 3)
        assert len(entries) == 0

    def test_out_of_bounds_range(self) -> None:
        entries = self.log.get_range(10, 20)
        assert len(entries) == 0


# =============================================================================
# Test term_at
# =============================================================================


class TestTermAt:
    """Tests for term_at lookups."""

    def setup_method(self) -> None:
        self.log = RaftLog("test-job")
        self.log.append(make_entry(term=1, index=1))
        self.log.append(make_entry(term=1, index=2))
        self.log.append(make_entry(term=2, index=3))

    def test_term_at_valid_index(self) -> None:
        assert self.log.term_at(1) == 1
        assert self.log.term_at(3) == 2

    def test_term_at_snapshot_index(self) -> None:
        # snapshot_index=0 with snapshot_term=0
        assert self.log.term_at(0) == 0

    def test_term_at_out_of_range(self) -> None:
        assert self.log.term_at(100) is None


# =============================================================================
# Test Truncation
# =============================================================================


class TestTruncation:
    """Tests for log truncation on conflicts."""

    def setup_method(self) -> None:
        self.log = RaftLog("test-job")
        for idx in range(1, 6):
            self.log.append(make_entry(term=1, index=idx))

    def test_truncate_from_middle(self) -> None:
        removed = self.log.truncate_from(3)
        assert removed == 3
        assert len(self.log) == 2
        assert self.log.last_index() == 2

    def test_truncate_from_start(self) -> None:
        removed = self.log.truncate_from(1)
        assert removed == 5
        assert len(self.log) == 0

    def test_truncate_from_end(self) -> None:
        removed = self.log.truncate_from(5)
        assert removed == 1
        assert len(self.log) == 4

    def test_truncate_beyond_log(self) -> None:
        removed = self.log.truncate_from(10)
        assert removed == 0
        assert len(self.log) == 5

    def test_entries_after_truncate(self) -> None:
        self.log.truncate_from(3)
        assert self.log.get(1) is not None
        assert self.log.get(2) is not None
        assert self.log.get(3) is None


# =============================================================================
# Test Snapshot Compaction
# =============================================================================


class TestCompaction:
    """Tests for snapshot compaction."""

    def setup_method(self) -> None:
        self.log = RaftLog("test-job")
        for idx in range(1, 11):
            term = 1 if idx <= 5 else 2
            self.log.append(make_entry(term=term, index=idx))

    def test_compact_removes_entries(self) -> None:
        removed = self.log.compact_through(5, term=1)
        assert removed == 5
        assert len(self.log) == 5

    def test_compact_updates_snapshot_state(self) -> None:
        self.log.compact_through(5, term=1)
        assert self.log.snapshot_index == 5
        assert self.log.snapshot_term == 1

    def test_compact_preserves_remaining_entries(self) -> None:
        self.log.compact_through(5, term=1)
        assert self.log.get(6) is not None
        assert self.log.get(6).term == 2
        assert self.log.get(10) is not None

    def test_compact_removes_access_to_old_entries(self) -> None:
        self.log.compact_through(5, term=1)
        for idx in range(1, 6):
            assert self.log.get(idx) is None

    def test_compact_idempotent_on_same_index(self) -> None:
        self.log.compact_through(5, term=1)
        removed = self.log.compact_through(5, term=1)
        assert removed == 0

    def test_compact_before_snapshot_index(self) -> None:
        self.log.compact_through(5, term=1)
        removed = self.log.compact_through(3, term=1)
        assert removed == 0

    def test_last_index_after_compact(self) -> None:
        self.log.compact_through(5, term=1)
        assert self.log.last_index() == 10

    def test_last_term_after_compact(self) -> None:
        self.log.compact_through(5, term=1)
        assert self.log.last_term() == 2

    def test_term_at_snapshot_boundary(self) -> None:
        self.log.compact_through(5, term=1)
        assert self.log.term_at(5) == 1
        assert self.log.term_at(4) is None


# =============================================================================
# Test Capacity and Backpressure
# =============================================================================


class TestCapacity:
    """Tests for capacity limits and backpressure signaling."""

    def test_under_capacity(self) -> None:
        log = RaftLog("test-job", max_entries=10)
        for idx in range(1, 6):
            log.append(make_entry(term=1, index=idx))
        assert not log.is_at_capacity

    def test_at_capacity(self) -> None:
        log = RaftLog("test-job", max_entries=5)
        for idx in range(1, 6):
            log.append(make_entry(term=1, index=idx))
        assert log.is_at_capacity

    def test_capacity_after_compact(self) -> None:
        log = RaftLog("test-job", max_entries=5)
        for idx in range(1, 6):
            log.append(make_entry(term=1, index=idx))
        assert log.is_at_capacity

        log.compact_through(3, term=1)
        assert not log.is_at_capacity


# =============================================================================
# Test Clear
# =============================================================================


class TestClear:
    """Tests for log clearing on job completion."""

    def test_clear_resets_all_state(self) -> None:
        log = RaftLog("test-job")
        for idx in range(1, 6):
            log.append(make_entry(term=1, index=idx))
        log.compact_through(2, term=1)

        log.clear()

        assert len(log) == 0
        assert log.last_index() == 0
        assert log.last_term() == 0
        assert log.snapshot_index == 0
        assert log.snapshot_term == 0
