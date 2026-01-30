"""
In-memory Raft log for a single job.

Provides efficient append, lookup, truncation, and compaction.
Log indices are 1-based per the Raft paper convention.
"""

from .models import RaftLogEntry


class RaftLog:
    """
    In-memory Raft log with 1-based indexing.

    Bounded by max_entries to prevent unbounded memory growth.
    Once the limit is reached and a snapshot exists, older entries
    are evicted during compaction.

    Thread safety: NOT thread-safe. Callers must provide
    external synchronization (typically RaftNode's asyncio.Lock).
    """

    __slots__ = (
        "_job_id",
        "_entries",
        "_snapshot_index",
        "_snapshot_term",
        "_max_entries",
    )

    def __init__(self, job_id: str, max_entries: int = 50_000) -> None:
        """
        Initialize an empty log for a job.

        Args:
            job_id: The job this log belongs to.
            max_entries: Maximum entries before compaction is needed.
        """
        self._job_id = job_id
        self._entries: list[RaftLogEntry] = []
        self._snapshot_index: int = 0
        self._snapshot_term: int = 0
        self._max_entries = max_entries

    @property
    def job_id(self) -> str:
        """The job ID this log belongs to."""
        return self._job_id

    @property
    def snapshot_index(self) -> int:
        """Index of the last entry included in snapshot."""
        return self._snapshot_index

    @property
    def snapshot_term(self) -> int:
        """Term of the last entry included in snapshot."""
        return self._snapshot_term

    @property
    def is_at_capacity(self) -> bool:
        """Whether the log has reached its max entry limit."""
        return len(self._entries) >= self._max_entries

    def append(self, entry: RaftLogEntry) -> int:
        """
        Append an entry to the log.

        Returns:
            The index of the appended entry.
        """
        self._entries.append(entry)
        return entry.index

    def get(self, index: int) -> RaftLogEntry | None:
        """
        Get entry at a 1-based index.

        Returns None if index is out of range or compacted.
        """
        actual_index = index - self._snapshot_index - 1
        if actual_index < 0 or actual_index >= len(self._entries):
            return None
        return self._entries[actual_index]

    def get_range(self, start_index: int, end_index: int) -> list[RaftLogEntry]:
        """
        Get entries in range [start_index, end_index).

        Both indices are 1-based. Returns empty list if range is invalid.
        """
        start_actual = max(0, start_index - self._snapshot_index - 1)
        end_actual = min(len(self._entries), end_index - self._snapshot_index - 1)
        return self._entries[start_actual:end_actual]

    def truncate_from(self, index: int) -> int:
        """
        Remove all entries from index onwards.

        Used when a follower's log conflicts with the leader's.

        Returns:
            Number of entries removed.
        """
        actual_index = max(0, index - self._snapshot_index - 1)
        if actual_index >= len(self._entries):
            return 0
        removed_count = len(self._entries) - actual_index
        self._entries = self._entries[:actual_index]
        return removed_count

    def last_index(self) -> int:
        """Last entry's index, or snapshot_index if log is empty."""
        if not self._entries:
            return self._snapshot_index
        return self._entries[-1].index

    def last_term(self) -> int:
        """Last entry's term, or snapshot_term if log is empty."""
        if not self._entries:
            return self._snapshot_term
        return self._entries[-1].term

    def term_at(self, index: int) -> int | None:
        """Get the term at a 1-based index. Returns None if not found."""
        if index == self._snapshot_index:
            return self._snapshot_term
        if entry := self.get(index):
            return entry.term
        return None

    def compact_through(self, index: int, term: int) -> int:
        """
        Remove entries up to and including index (snapshot compaction).

        Updates snapshot_index and snapshot_term. Entries at or before
        the given index are discarded from memory.

        Returns:
            Number of entries compacted.
        """
        if index <= self._snapshot_index:
            return 0

        entries_to_remove = index - self._snapshot_index
        actual_remove = min(entries_to_remove, len(self._entries))
        self._entries = self._entries[actual_remove:]
        self._snapshot_index = index
        self._snapshot_term = term
        return actual_remove

    def clear(self) -> None:
        """Clear all entries. Called on job completion for GC."""
        self._entries.clear()
        self._snapshot_index = 0
        self._snapshot_term = 0

    def __len__(self) -> int:
        """Number of entries currently in the log."""
        return len(self._entries)
