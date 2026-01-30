"""
Core Raft algorithm implementation for a single job.

Implements leader election, log replication, and commit index
advancement per the Raft paper (Sections 5.1-5.4).

Each job gets its own RaftNode instance. All cluster nodes
participate in every job's Raft group.
"""

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from .models import (
    AppendEntries,
    AppendEntriesResponse,
    RaftLogEntry,
    RequestVote,
    RequestVoteResponse,
)
from .raft_log import RaftLog

if TYPE_CHECKING:
    from hyperscale.logging import Logger


# Raft timing constants (seconds)
ELECTION_TIMEOUT_MIN: float = 0.150
ELECTION_TIMEOUT_MAX: float = 0.300
HEARTBEAT_INTERVAL: float = 0.050


class RaftNode:
    """
    Raft state machine for a single job.

    Roles: follower, candidate, leader.
    Thread safety: All public methods acquire _lock.
    Memory: Call destroy() on job completion to release all state.
    """

    __slots__ = (
        "_job_id",
        "_node_id",
        "_members",
        "_member_addrs",
        "_send_message",
        "_apply_command",
        "_on_become_leader",
        "_on_lose_leadership",
        "_logger",
        "_lock",
        "_log",
        "_role",
        "_current_term",
        "_voted_for",
        "_current_leader",
        "_commit_index",
        "_last_applied",
        "_next_index",
        "_match_index",
        "_votes_received",
        "_election_deadline",
        "_last_heartbeat_sent",
        "_destroyed",
    )

    def __init__(
        self,
        job_id: str,
        node_id: str,
        members: set[str],
        member_addrs: dict[str, tuple[str, int]],
        send_message: Callable[..., Awaitable[None]],
        apply_command: Callable[..., Awaitable[None]],
        on_become_leader: Callable[[], None] | None,
        on_lose_leadership: Callable[[], None] | None,
        logger: "Logger",
    ) -> None:
        self._job_id = job_id
        self._node_id = node_id
        self._members = set(members)
        self._member_addrs = dict(member_addrs)
        self._send_message = send_message
        self._apply_command = apply_command
        self._on_become_leader = on_become_leader
        self._on_lose_leadership = on_lose_leadership
        self._logger = logger

        self._lock = asyncio.Lock()
        self._log = RaftLog(job_id)
        self._role = "follower"
        self._current_term = 0
        self._voted_for: str | None = None
        self._current_leader: str | None = None
        self._commit_index = 0
        self._last_applied = 0

        # Leader-only state (initialized on election win)
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}
        self._votes_received: set[str] = set()

        self._election_deadline = self._new_election_deadline()
        self._last_heartbeat_sent: float = 0.0
        self._destroyed = False

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def current_term(self) -> int:
        return self._current_term

    @property
    def role(self) -> str:
        return self._role

    @property
    def current_leader(self) -> str | None:
        return self._current_leader

    @property
    def commit_index(self) -> int:
        return self._commit_index

    def is_leader(self) -> bool:
        return self._role == "leader"

    # =========================================================================
    # Tick (called from consensus coordinator)
    # =========================================================================

    async def tick(self) -> None:
        """Advance the Raft state machine by one tick."""
        async with self._lock:
            if self._destroyed:
                return
            match self._role:
                case "leader":
                    self._tick_leader()
                case "follower" | "candidate" if self._election_timed_out():
                    await self._start_election_locked()

    def _tick_leader(self) -> None:
        """Check if heartbeat is due. Does NOT send -- replicate_to_followers does."""
        pass  # Heartbeats sent via replicate_to_followers in consensus coordinator

    def _election_timed_out(self) -> bool:
        return time.monotonic() >= self._election_deadline

    # =========================================================================
    # Election
    # =========================================================================

    async def start_election(self) -> None:
        """Trigger an election (public, acquires lock)."""
        async with self._lock:
            if self._destroyed:
                return
            await self._start_election_locked()

    async def _start_election_locked(self) -> None:
        """Transition to candidate and request votes. Lock must be held."""
        self._current_term += 1
        self._role = "candidate"
        self._voted_for = self._node_id
        self._votes_received = {self._node_id}
        self._current_leader = None
        self._election_deadline = self._new_election_deadline()

        # Single-node cluster wins immediately
        if len(self._votes_received) >= self._quorum_size():
            self._transition_to_leader()
            return

        await self._broadcast_request_vote()

    async def _broadcast_request_vote(self) -> None:
        """Send RequestVote to all peers."""
        request = RequestVote(
            job_id=self._job_id,
            term=self._current_term,
            candidate_id=self._node_id,
            last_log_index=self._log.last_index(),
            last_log_term=self._log.last_term(),
        )
        for peer_id in self._members:
            if peer_id == self._node_id:
                continue
            if addr := self._member_addrs.get(peer_id):
                await self._send_message(addr, request)

    async def handle_request_vote(self, request: RequestVote) -> RequestVoteResponse:
        """Handle an incoming RequestVote RPC."""
        async with self._lock:
            if self._destroyed:
                return self._vote_response(granted=False)

            # Step down if request has higher term
            if request.term > self._current_term:
                self._step_down(request.term)

            granted = self._should_grant_vote(request)
            if granted:
                self._voted_for = request.candidate_id
                self._election_deadline = self._new_election_deadline()

            return self._vote_response(granted=granted)

    def _should_grant_vote(self, request: RequestVote) -> bool:
        """Determine whether to grant a vote. Complexity: 3."""
        if request.term < self._current_term:
            return False
        vote_available = self._voted_for in (None, request.candidate_id)
        candidate_log_ok = self._candidate_log_is_current(
            request.last_log_index, request.last_log_term
        )
        return vote_available and candidate_log_ok

    def _candidate_log_is_current(self, last_index: int, last_term: int) -> bool:
        """Check if candidate's log is at least as up-to-date as ours (Section 5.4.1)."""
        my_last_term = self._log.last_term()
        if last_term != my_last_term:
            return last_term > my_last_term
        return last_index >= self._log.last_index()

    def _vote_response(self, *, granted: bool) -> RequestVoteResponse:
        return RequestVoteResponse(
            job_id=self._job_id,
            term=self._current_term,
            vote_granted=granted,
            voter_id=self._node_id,
        )

    async def handle_request_vote_response(self, response: RequestVoteResponse) -> None:
        """Handle a vote response from a peer."""
        async with self._lock:
            if self._destroyed or self._role != "candidate":
                return
            if response.term > self._current_term:
                self._step_down(response.term)
                return
            if not response.vote_granted or response.term != self._current_term:
                return

            self._votes_received.add(response.voter_id)
            if len(self._votes_received) >= self._quorum_size():
                self._transition_to_leader()

    def _transition_to_leader(self) -> None:
        """Become leader. Initialize next_index and match_index."""
        self._role = "leader"
        self._current_leader = self._node_id
        next_idx = self._log.last_index() + 1

        self._next_index = {
            peer: next_idx for peer in self._members if peer != self._node_id
        }
        self._match_index = {
            peer: 0 for peer in self._members if peer != self._node_id
        }

        if self._on_become_leader:
            self._on_become_leader()

    # =========================================================================
    # Log Replication
    # =========================================================================

    async def replicate_to_followers(self) -> None:
        """Send AppendEntries to all followers. Called by consensus coordinator."""
        async with self._lock:
            if self._destroyed or self._role != "leader":
                return
            self._last_heartbeat_sent = time.monotonic()
            for peer_id in self._members:
                if peer_id == self._node_id:
                    continue
                await self._send_append_entries_to(peer_id)

    async def _send_append_entries_to(self, peer_id: str) -> None:
        """Build and send AppendEntries to one follower."""
        addr = self._member_addrs.get(peer_id)
        if addr is None:
            return

        next_idx = self._next_index.get(peer_id, 1)
        prev_index = next_idx - 1
        prev_term = self._log.term_at(prev_index) or 0

        entries = self._log.get_range(next_idx, self._log.last_index() + 1)

        request = AppendEntries(
            job_id=self._job_id,
            term=self._current_term,
            leader_id=self._node_id,
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=self._commit_index,
        )
        await self._send_message(addr, request)

    async def handle_append_entries(self, request: AppendEntries) -> AppendEntriesResponse:
        """Handle an incoming AppendEntries RPC."""
        async with self._lock:
            if self._destroyed:
                return self._append_response(success=False, match_index=0)

            if request.term > self._current_term:
                self._step_down(request.term)

            if request.term < self._current_term:
                return self._append_response(success=False, match_index=0)

            # Valid leader heartbeat -- reset election timer
            self._current_leader = request.leader_id
            self._election_deadline = self._new_election_deadline()

            if self._role == "candidate":
                self._role = "follower"

            # Check log consistency
            if not self._log_matches_at(request.prev_log_index, request.prev_log_term):
                conflict = self._find_conflict_info(request.prev_log_index)
                return self._append_response(
                    success=False,
                    match_index=0,
                    conflict_term=conflict[0],
                    conflict_index=conflict[1],
                )

            # Append new entries (truncating conflicts)
            self._apply_entries_from_leader(request.entries)

            # Advance commit index
            if request.leader_commit > self._commit_index:
                self._commit_index = min(
                    request.leader_commit, self._log.last_index()
                )

            return self._append_response(
                success=True, match_index=self._log.last_index()
            )

    def _log_matches_at(self, prev_index: int, prev_term: int) -> bool:
        """Check if our log matches at the given position."""
        if prev_index == 0:
            return True
        term = self._log.term_at(prev_index)
        return term is not None and term == prev_term

    def _find_conflict_info(self, prev_index: int) -> tuple[int | None, int | None]:
        """Find conflict term and first index of that term for fast backtrack."""
        conflict_term = self._log.term_at(prev_index)
        if conflict_term is None:
            return None, self._log.last_index() + 1

        # Walk back to find first entry with conflict_term
        first_index = prev_index
        while first_index > self._log.snapshot_index + 1:
            if (prev_term := self._log.term_at(first_index - 1)) != conflict_term:
                break
            first_index -= 1

        return conflict_term, first_index

    def _apply_entries_from_leader(self, entries: list[RaftLogEntry]) -> None:
        """Append entries from leader, truncating any conflicts."""
        for entry in entries:
            existing_term = self._log.term_at(entry.index)
            if existing_term is not None and existing_term != entry.term:
                self._log.truncate_from(entry.index)
            if self._log.last_index() < entry.index:
                self._log.append(entry)

    def _append_response(
        self,
        *,
        success: bool,
        match_index: int,
        conflict_term: int | None = None,
        conflict_index: int | None = None,
    ) -> AppendEntriesResponse:
        return AppendEntriesResponse(
            job_id=self._job_id,
            term=self._current_term,
            success=success,
            follower_id=self._node_id,
            match_index=match_index,
            conflict_term=conflict_term,
            conflict_index=conflict_index,
        )

    async def handle_append_entries_response(self, response: AppendEntriesResponse) -> None:
        """Handle response from a follower."""
        async with self._lock:
            if self._destroyed or self._role != "leader":
                return
            if response.term > self._current_term:
                self._step_down(response.term)
                return
            if response.term != self._current_term:
                return

            if response.success:
                self._next_index[response.follower_id] = response.match_index + 1
                self._match_index[response.follower_id] = response.match_index
                self._advance_commit_index()
            else:
                self._backtrack_next_index(response)

    def _backtrack_next_index(self, response: AppendEntriesResponse) -> None:
        """Efficiently backtrack next_index using conflict info."""
        current = self._next_index.get(response.follower_id, 1)
        if conflict_index := response.conflict_index:
            self._next_index[response.follower_id] = max(1, conflict_index)
        else:
            self._next_index[response.follower_id] = max(1, current - 1)

    def _advance_commit_index(self) -> None:
        """Advance commit_index to highest index replicated on a majority (Section 5.3/5.4)."""
        for candidate_index in range(self._log.last_index(), self._commit_index, -1):
            term = self._log.term_at(candidate_index)
            if term != self._current_term:
                continue
            replication_count = sum(
                1 for match in self._match_index.values() if match >= candidate_index
            ) + 1  # +1 for leader
            if replication_count >= self._quorum_size():
                self._commit_index = candidate_index
                return

    # =========================================================================
    # Client Interface
    # =========================================================================

    async def propose(self, command: bytes, command_type: str) -> tuple[bool, int]:
        """
        Propose a new command (leader only).

        Returns:
            (success, index) -- success is False if not leader or log at capacity.
        """
        async with self._lock:
            if self._destroyed or self._role != "leader":
                return False, 0
            if self._log.is_at_capacity:
                return False, 0

            entry = RaftLogEntry(
                term=self._current_term,
                index=self._log.last_index() + 1,
                command=command,
                command_type=command_type,
                job_id=self._job_id,
                timestamp=time.monotonic(),
            )
            index = self._log.append(entry)
            return True, index

    async def apply_committed_entries(self) -> int:
        """
        Apply all committed but unapplied entries to the state machine.

        Returns:
            Number of entries applied.
        """
        async with self._lock:
            if self._destroyed:
                return 0

            applied_count = 0
            while self._last_applied < self._commit_index:
                self._last_applied += 1
                if entry := self._log.get(self._last_applied):
                    await self._apply_command(entry)
                    applied_count += 1

            return applied_count

    # =========================================================================
    # Membership
    # =========================================================================

    def update_membership(
        self,
        members: set[str],
        addrs: dict[str, tuple[str, int]],
    ) -> None:
        """Update the set of cluster members. Not lock-protected (called externally)."""
        self._members = set(members)
        self._member_addrs = dict(addrs)

        if self._role == "leader":
            # Initialize tracking for new members
            next_idx = self._log.last_index() + 1
            for member in members:
                if member == self._node_id:
                    continue
                self._next_index.setdefault(member, next_idx)
                self._match_index.setdefault(member, 0)

            # Remove departed members
            departed = set(self._next_index.keys()) - members
            for member_id in departed:
                self._next_index.pop(member_id, None)
                self._match_index.pop(member_id, None)

    # =========================================================================
    # Cleanup
    # =========================================================================

    def destroy(self) -> None:
        """
        Release all state. Called on job completion.

        After destroy(), all public methods become no-ops.
        """
        self._destroyed = True
        self._log.clear()
        self._next_index.clear()
        self._match_index.clear()
        self._votes_received.clear()
        self._members.clear()
        self._member_addrs.clear()

    # =========================================================================
    # Helpers
    # =========================================================================

    def _step_down(self, new_term: int) -> None:
        """Step down to follower for a higher term."""
        was_leader = self._role == "leader"
        self._current_term = new_term
        self._role = "follower"
        self._voted_for = None
        self._election_deadline = self._new_election_deadline()

        if was_leader and self._on_lose_leadership:
            self._on_lose_leadership()

    def _quorum_size(self) -> int:
        """Majority quorum: (cluster_size // 2) + 1."""
        return len(self._members) // 2 + 1

    @staticmethod
    def _new_election_deadline() -> float:
        """Randomized election deadline to prevent split votes."""
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        return time.monotonic() + timeout
