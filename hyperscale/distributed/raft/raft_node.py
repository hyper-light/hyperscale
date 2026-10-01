"""
Core Raft algorithm implementation for a single job.

Implements leader election, log replication, and commit index
advancement per the Raft paper (Sections 5.1-5.4).

Each job gets its own RaftNode instance. All cluster nodes
participate in every job's Raft group.
"""

import asyncio
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

from hyperscale.distributed.runtime import Clock, RealClock, Random, RealRandom


_DEFAULT_CLOCK: Clock = RealClock()
_DEFAULT_RANDOM: Random = RealRandom()

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from hyperscale.logging.lsn import HybridLamportClock


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

    Volatility -- current state, and the storage decision
    -----------------------------------------------------

    ``current_term``, ``voted_for``, and the log live only in memory: a
    process restart loses all three. The Raft paper requires them
    durable; ``RaftWAL`` / ``SnapshotManager`` exist and are tested but
    are not wired here yet.

    Storage decision (2026-09-30, supersedes the earlier reading that
    AD-52 forbids restart-surviving storage): a node MAY write to disk
    but may never ASSUME what that disk is -- present, empty, its own,
    intact. Correctness must hold with no disk at all. So persistence is
    opportunistic: when wired, term/vote/log go to disk where an identity
    stamp and integrity checks prove the disk is this node's and intact,
    and a missing or untrustworthy disk is treated as empty (the node
    rejoins with nothing). Until then the guarantees below are the
    in-memory ones.

    This matters more now that AD-38 REGIONAL durability is a commit in
    the job's group: REGIONAL means "held by a majority of the
    datacenter's managers", which survives any minority of crashes but
    not a simultaneous majority restart.

    What volatility costs, and what contains it:

    * **Double vote.** Node ids are address-derived, so a manager that
      restarts mid-election rejoins under its old identity with
      ``voted_for`` cleared and can vote twice in the same term -- two
      leaders for one job's group. The window is one election round,
      and every dispatch a job leader issues carries a fence token that
      workers and gates validate, so the elder of two leaders is fenced
      out at the first boundary it touches.

    * **Log loss.** A restarted member returns with an empty log and
      catches up from the group's leader like any lagging follower; the
      group's entries survive while a majority of members stays up.

    Disruption by a member that cannot hear the leader is contained
    regardless of storage: elections start with a PreVote round, and
    members that heard from a live leader recently refuse to vote.
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
        "_clock",
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
        "_pre_vote_term",
        "_pre_votes_received",
        "_last_leader_contact",
        "_proposal_waiters",
        "_proposal_timeout_seconds",
        "_election_deadline",
        "_last_heartbeat_sent",
        "_destroyed",
        "_configured_cluster_size",
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
        clock: "HybridLamportClock | None" = None,
        configured_cluster_size: int | None = None,
        proposal_timeout_seconds: float = 5.0,
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
        self._clock = clock

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
        # PreVote round in progress: the term this node would campaign in.
        self._pre_vote_term: int | None = None
        self._pre_votes_received: set[str] = set()
        # Last instant a valid leader's AppendEntries arrived (leader
        # stickiness: votes are withheld while a leader is evidently live).
        self._last_leader_contact: float | None = None
        self._proposal_waiters: dict[int, asyncio.Future[bool]] = {}
        self._proposal_timeout_seconds = proposal_timeout_seconds

        self._election_deadline = self._new_election_deadline()
        self._last_heartbeat_sent: float = 0.0
        self._destroyed = False
        self._configured_cluster_size = max(
            1,
            configured_cluster_size
            if configured_cluster_size is not None
            else len(self._members | {self._node_id}),
        )

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
                    await self._start_pre_vote_locked()

    def _tick_leader(self) -> None:
        """Check if heartbeat is due. Does NOT send -- replicate_to_followers does."""
        pass  # Heartbeats sent via replicate_to_followers in consensus coordinator

    def _election_timed_out(self) -> bool:
        return _DEFAULT_CLOCK.monotonic() >= self._election_deadline

    # =========================================================================
    # Election
    # =========================================================================

    async def start_election(self) -> None:
        """Trigger an election (public, acquires lock)."""
        async with self._lock:
            if self._destroyed:
                return
            await self._start_election_locked()

    async def _start_pre_vote_locked(self) -> None:
        """Ask whether an election could succeed before disrupting anyone.

        Raft thesis 9.6: a member that cannot hear the leader (an
        asymmetric partition, a stalled inbound path) would otherwise
        time out again and again, campaigning at ever higher terms that
        force a healthy leader to step down each time. A PreVote round
        changes no state anywhere; only a majority of grants -- members
        that have not heard from a live leader either and whose logs are
        no fresher -- lets this node bump its term and campaign.
        """
        self._election_deadline = self._new_election_deadline()
        self._pre_vote_term = self._current_term + 1
        self._pre_votes_received = {self._node_id}
        if len(self._pre_votes_received) >= self._quorum_size():
            await self._start_election_locked()
            return
        await self._broadcast_request_vote(term=self._pre_vote_term, pre_vote=True)

    async def _start_election_locked(self) -> None:
        """Transition to candidate and request votes. Lock must be held."""
        self._pre_vote_term = None
        self._pre_votes_received = set()
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

        await self._broadcast_request_vote(term=self._current_term, pre_vote=False)

    async def _broadcast_request_vote(self, *, term: int, pre_vote: bool) -> None:
        """Send RequestVote (or its PreVote form) to all peers."""
        request = RequestVote(
            job_id=self._job_id,
            term=term,
            candidate_id=self._node_id,
            last_log_index=self._log.last_index(),
            last_log_term=self._log.last_term(),
            pre_vote=pre_vote,
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

            if request.pre_vote:
                return self._vote_response(
                    granted=self._should_grant_pre_vote(request), pre_vote=True
                )

            # Leader stickiness (Raft thesis 4.2.3): while a live leader is
            # evidently in charge, a vote request neither moves this term
            # nor wins a vote -- a disruptive member cannot depose it.
            if self._heard_from_leader_recently():
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

    def _should_grant_pre_vote(self, request: RequestVote) -> bool:
        """Would this member vote for ``request`` in its proposed term?
        Answered without changing any state."""
        if self._role == "leader" or self._heard_from_leader_recently():
            return False
        if request.term <= self._current_term:
            return False
        return self._candidate_log_is_current(
            request.last_log_index, request.last_log_term
        )

    def _heard_from_leader_recently(self) -> bool:
        """A valid leader's AppendEntries arrived within the minimum
        election timeout -- no follower could have timed out on it yet."""
        if self._role == "leader":
            return False
        if self._current_leader is None or self._last_leader_contact is None:
            return False
        return (
            _DEFAULT_CLOCK.monotonic() - self._last_leader_contact
            < ELECTION_TIMEOUT_MIN
        )

    def _candidate_log_is_current(self, last_index: int, last_term: int) -> bool:
        """Check if candidate's log is at least as up-to-date as ours (Section 5.4.1)."""
        my_last_term = self._log.last_term()
        if last_term != my_last_term:
            return last_term > my_last_term
        return last_index >= self._log.last_index()

    def _vote_response(self, *, granted: bool, pre_vote: bool = False) -> RequestVoteResponse:
        return RequestVoteResponse(
            job_id=self._job_id,
            term=self._current_term,
            vote_granted=granted,
            voter_id=self._node_id,
            pre_vote=pre_vote,
        )

    async def handle_request_vote_response(self, response: RequestVoteResponse) -> None:
        """Handle a vote response from a peer."""
        async with self._lock:
            if self._destroyed:
                return
            if response.pre_vote:
                await self._handle_pre_vote_response(response)
                return
            if self._role != "candidate":
                return
            if response.term > self._current_term:
                self._step_down(response.term)
                return
            if not response.vote_granted or response.term != self._current_term:
                return

            self._votes_received.add(response.voter_id)
            if len(self._votes_received) >= self._quorum_size():
                self._transition_to_leader()

    async def _handle_pre_vote_response(self, response: RequestVoteResponse) -> None:
        """Count a PreVote grant; campaign for real on a majority. Lock held."""
        if self._pre_vote_term is None or self._role == "leader":
            return
        if response.term > self._current_term:
            self._step_down(response.term)
            return
        if not response.vote_granted:
            return

        self._pre_votes_received.add(response.voter_id)
        if len(self._pre_votes_received) >= self._quorum_size():
            await self._start_election_locked()

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
            await self._send_append_entries_to_followers()

    async def _send_append_entries_to_followers(self) -> None:
        """Send AppendEntries (entries or heartbeat) to every follower."""
        self._last_heartbeat_sent = _DEFAULT_CLOCK.monotonic()
        for peer_id in self._members:
            if peer_id != self._node_id:
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

            # A same-term leader claim means this node's local leadership view
            # is stale. Step down before applying the heartbeat so only one
            # writer remains active for the term.
            if self._role == "leader" and request.leader_id != self._node_id:
                self._step_down(request.term)

            # Valid leader heartbeat -- reset election timer
            self._current_leader = request.leader_id
            self._last_leader_contact = _DEFAULT_CLOCK.monotonic()
            self._election_deadline = self._new_election_deadline()
            self._pre_vote_term = None

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
                await self._apply_committed_locked()

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
                await self._apply_committed_locked()
            else:
                self._backtrack_next_index(response)

    def _backtrack_next_index(self, response: AppendEntriesResponse) -> None:
        """Efficiently backtrack next_index using conflict info."""
        current = self._next_index.get(response.follower_id, 1)
        if conflict_index := response.conflict_index:
            self._next_index[response.follower_id] = max(1, conflict_index)
        else:
            self._next_index[response.follower_id] = max(1, current - 1)

    def last_index_where(self, matches: Callable[[RaftLogEntry], bool]) -> int | None:
        """Newest log index whose entry satisfies ``matches`` (None if none).

        Scans newest-first: per-job logs are short, and callers look for
        an entry they appended recently.
        """
        for index in range(self._log.last_index(), self._log.snapshot_index, -1):
            if (entry := self._log.get(index)) is not None and matches(entry):
                return index
        return None

    def members_holding(self, index: int) -> set[str] | None:
        """Members known to hold log ``index`` (leader only; None otherwise).

        The leader always holds its own entries; a follower counts once
        its acknowledged match index reaches ``index``. This is what a
        placement rule beyond a bare majority (e.g. AD-38 GLOBAL: copies
        in more than one region) is judged against.
        """
        if self._destroyed or self._role != "leader" or index > self._log.last_index():
            return None
        return {self._node_id} | {
            member for member, match in self._match_index.items() if match >= index
        }

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

        The entry timestamp is minted from the shared HybridLamportClock when
        one is supplied; this gives all followers an identical wall-clock-derived
        seconds value when the entry is replicated, so apply handlers can use
        ``entry.timestamp`` as a deterministic time source. When no clock is
        configured, the timestamp falls back to the leader's monotonic clock --
        still replicated (single-source), but not comparable to wall-clock
        consumers; production paths always supply a clock.

        Returns:
            (success, index) -- success is False if not leader or log at capacity.
        """
        async with self._lock:
            if self._destroyed or self._role != "leader":
                return False, 0
            if self._log.is_at_capacity:
                return False, 0

            if self._clock is not None:
                lsn = await self._clock.generate()
                entry_timestamp = lsn.wall_clock / 1000.0
            else:
                entry_timestamp = _DEFAULT_CLOCK.monotonic()

            entry = RaftLogEntry(
                term=self._current_term,
                index=self._log.last_index() + 1,
                command=command,
                command_type=command_type,
                job_id=self._job_id,
                timestamp=entry_timestamp,
            )
            index = self._log.append(entry)
            waiter = asyncio.get_running_loop().create_future()
            self._proposal_waiters[index] = waiter
            # Commit is event-driven: a group that is its own majority
            # commits and applies here, and a larger group replicates the
            # entry now instead of on the next heartbeat tick.
            self._advance_commit_index()
            await self._apply_committed_locked()
            await self._send_append_entries_to_followers()

        try:
            committed = await _DEFAULT_CLOCK.wait_for(
                waiter,
                timeout=self._proposal_timeout_seconds,
            )
            return committed, index
        except asyncio.TimeoutError:
            async with self._lock:
                self._proposal_waiters.pop(index, None)
            return False, index
        except asyncio.CancelledError:
            async with self._lock:
                self._proposal_waiters.pop(index, None)
            raise

    async def apply_committed_entries(self) -> int:
        """
        Apply all committed but unapplied entries to the state machine.

        Returns:
            Number of entries applied.
        """
        async with self._lock:
            if self._destroyed:
                return 0
            return await self._apply_committed_locked()

    async def _apply_committed_locked(self) -> int:
        """Apply entries up to commit_index and resolve their proposals.

        Called wherever commit_index advances, so a proposal resolves as
        soon as it commits rather than on the next tick. Caller holds the
        lock.
        """
        applied_count = 0
        while self._last_applied < self._commit_index:
            self._last_applied += 1
            if entry := self._log.get(self._last_applied):
                await self._apply_command(entry)
                waiter = self._proposal_waiters.pop(self._last_applied, None)
                if waiter is not None and not waiter.done():
                    waiter.set_result(True)
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
        self._fail_pending_proposals()
        self._members.clear()
        self._member_addrs.clear()

    # =========================================================================
    # Helpers
    # =========================================================================

    def _step_down(self, new_term: int) -> None:
        """Step down to follower for a newer or conflicting term."""
        was_leader = self._role == "leader"
        self._current_term = new_term
        self._role = "follower"
        self._voted_for = None
        self._pre_vote_term = None
        self._election_deadline = self._new_election_deadline()
        self._fail_pending_proposals()

        if was_leader and self._on_lose_leadership:
            self._on_lose_leadership()

    def _quorum_size(self) -> int:
        """Majority quorum from configured cluster size."""
        return self._configured_cluster_size // 2 + 1

    def _fail_pending_proposals(self) -> None:
        """Fail all local proposals that have not reached committed apply."""
        for waiter in self._proposal_waiters.values():
            if not waiter.done():
                waiter.set_result(False)
        self._proposal_waiters.clear()

    @staticmethod
    def _new_election_deadline() -> float:
        """Randomized election deadline to prevent split votes."""
        timeout = _DEFAULT_RANDOM.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        return _DEFAULT_CLOCK.monotonic() + timeout
