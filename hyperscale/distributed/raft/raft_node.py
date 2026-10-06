"""
Core Raft algorithm implementation for a single job.

Implements leader election, log replication, and commit index
advancement per the Raft paper (Sections 5.1-5.4).

Each job gets its own RaftNode instance. All cluster nodes
participate in every job's Raft group.
"""

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Iterator
from typing import TYPE_CHECKING

from .models import (
    RAFT_CONFIGURATION_COMMAND,
    RAFT_NO_OP_COMMAND,
    AppendEntries,
    AppendEntriesResponse,
    RaftConfiguration,
    RaftLogEntry,
    RequestVote,
    RequestVoteResponse,
)
from .logging_models import RaftError, RaftWarning
from .models.log_entry import RAFT_LOG_SCHEMA_VERSIONS
from .raft_log import RaftLog
from .snapshot import (
    InstallSnapshot,
    InstallSnapshotResponse,
    RaftSnapshot,
    SnapshotManager,
)
from .store.models import (
    EntriesRecord,
    GroupCreatedRecord,
    GroupReleasedRecord,
    HardStateRecord,
    RecoveredRaftGroup,
    SnapshotRecord,
    TruncateFromRecord,
)
from .store.raft_storage import RaftStorage
from hyperscale.distributed.hlc.clock_offset_exceeded_error import ClockOffsetExceededError

from hyperscale.distributed.runtime import Clock, RealClock, Random, RealRandom


_DEFAULT_CLOCK: Clock = RealClock()
_DEFAULT_RANDOM: Random = RealRandom()

if TYPE_CHECKING:
    from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock
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

    Persistence (D1, ``docs/architecture/D1_RAFT_PERSISTENCE.md``)
    -------------------------------------------------------------

    ``current_term``, ``voted_for``, the log and snapshots are written to
    ``storage`` before any reply or request that depends on them (Raft
    Figure 2). A node whose disk holds its own intact store resumes its
    id and every group's state (``recover``); one with no usable disk, or
    one it cannot trust, starts as a new member -- its id is its process
    incarnation's (``NodeId.full`` embeds its start time), so it is never
    its old self with ``voted_for`` cleared. Either way, membership
    changes only through the log (``reconcile_membership``,
    ``change_membership``): only the configuration's voters count, every
    change needs a quorum of the old voters and of the new, and a new
    member votes only once the group's leader promoted it. Every dispatch
    a job leader issues also carries a fence token that workers and gates
    validate, so the elder of two leaders is fenced out at the first
    boundary it touches.

    With persistence, AD-38 REGIONAL -- a commit in the job's group --
    means held on a majority of the datacenter's managers' disks: it
    survives a simultaneous restart of all of them.

    Disruption by a member that cannot hear the leader is contained
    regardless of storage: elections start with a PreVote round, and
    members that heard from a live leader recently refuse to vote. That
    stickiness needs CheckQuorum (Raft thesis 6.2) beside it: a leader no
    quorum of voters answered for an election timeout -- past the longest
    a reachable member can take to answer on its transport -- steps down.
    Otherwise a leader whose heartbeats still reach its followers but
    whose acknowledgements do not reach it (an asymmetric partition)
    keeps every follower refusing votes, and the group never elects a
    leader that can commit.

    Membership (AD-52 sections 6-7)
    -------------------------------

    The group's ``RaftConfiguration`` -- its voters, and learners that
    follow the log without voting -- starts from ``initial_voters``, which
    every member must be created with alike, and changes only through log
    entries the leader appends (``change_membership``; the coordinators
    drive it toward the cluster's live members through
    ``reconcile_membership``). Each member acts on
    the latest configuration in its log, committed or not. Elections and
    commits count only voters, and need a majority of the voters and never
    fewer than a majority of the cohort's configured size (AD-3). A change
    of voters passes through a joint configuration that needs both the old
    and the new voters' quorums; once it commits the leader appends the
    final one, and a leader that is not among the final voters steps down
    once that commits. Learners and removed members never campaign.
    """

    __slots__ = (
        "_job_id",
        "_node_id",
        "_initial_configuration",
        "_configuration",
        "_configuration_index",
        "_member_addrs",
        "_send_message",
        "_apply_command",
        "_on_become_leader",
        "_on_lose_leadership",
        "_logger",
        "_clock",
        "_may_lead",
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
        "_last_response_at",
        "_leadership_started_at",
        "_last_quorum_contact",
        "_proposal_waiters",
        "_read_round",
        "_snapshot_entries",
        "_snapshot_catch_up_entries",
        "_schema_versions",
        "_reported_schema_versions",
        "_apply_halted_at",
        "_elections_started",
        "_elections_won",
        "_proposals_committed",
        "_proposals_failed",
        "_snapshots_sent",
        "_snapshots_installed",
        "_acknowledged_read_rounds",
        "_read_waiters",
        "_answering_read_round",
        "_leader_lease_seconds",
        "_round_sent_at",
        "_newest_stamped_round",
        "_lease_expires_at",
        "_lease_reads",
        "_votes_withheld_until",
        "_proposal_timeout_seconds",
        "_election_deadline",
        "_last_heartbeat_sent",
        "_destroyed",
        "_quorum_floor",
        "_snapshot_state",
        "_restore_snapshot",
        "_snapshots",
        "_snapshot_configuration",
        "_check_quorum_window",
        "_storage",
        "_persisted_term",
        "_persisted_vote",
        "_pending_entries",
        "_pending_truncate_from",
        "_pending_snapshot",
        "_durable_index",
        "_creation_persisted",
    )

    def __init__(
        self,
        job_id: str,
        node_id: str,
        initial_voters: frozenset[str],
        member_addrs: dict[str, tuple[str, int]],
        send_message: Callable[..., Awaitable[None]],
        apply_command: Callable[..., Awaitable[None]],
        on_become_leader: Callable[[], None] | None,
        on_lose_leadership: Callable[[], None] | None,
        logger: "Logger",
        configured_cluster_size: int | None = None,
        proposal_timeout_seconds: float = 5.0,
        *,
        clock: "HybridLogicalClock",
        may_lead: Callable[[], bool],
        snapshot_state: Callable[[], bytes] | None = None,
        restore_snapshot: Callable[[bytes], Awaitable[None]] | None = None,
        request_timeout_seconds: float = 0.0,
        schema_versions: tuple[int, int] = RAFT_LOG_SCHEMA_VERSIONS,
        snapshot_entries: int = 0,
        snapshot_catch_up_entries: int = 0,
        leader_lease_drift_bound: float | None = None,
        storage: RaftStorage,
    ) -> None:
        """
        ``storage`` keeps the group's term, vote, log and snapshots (D1):
        each is durable before any reply or request that depends on it
        goes out (Raft Figure 2), and the leader counts itself toward a
        commit only for entries it holds durably -- it sends them first,
        so its write overlaps the round trip (Raft thesis 10.2.1). A group
        that resumes from a disk is handed its state by ``recover``.

        ``snapshot_state`` and ``restore_snapshot`` let a long-lived group
        compact its log (Raft section 7): the first serializes the state
        machine as of the last applied entry, the second replaces it with a
        snapshot a leader installs. A group without them never compacts --
        a job's group lives only as long as its job. Such a group snapshots
        once ``snapshot_entries`` entries were applied since its last
        snapshot, and keeps the ``snapshot_catch_up_entries`` before the
        snapshot point in its log: a member (or a membership watch) that
        far behind catches up from the log, one further behind is sent the
        snapshot (etcd's snapshot count and catch-up entries).

        ``request_timeout_seconds`` is how long one exchange with a member
        may take before ``send_message``'s transport gives up on it. A
        transport that delivers one request per member at a time (the
        ``RaftPeerOutbox``) holds the next heartbeat behind a lost one for
        that long, so a reachable member may go that long unheard:
        CheckQuorum counts a member as answering within that plus an
        election timeout. Measured on the membership VOPR, an election
        timeout alone doubled leadership churn under TCP retransmission
        delays. Zero for a transport that never holds one request behind
        another.
        """
        self._job_id = job_id
        self._node_id = node_id
        # The voters every member of the group starts from; later
        # configurations arrive as log entries (``change_membership``).
        self._initial_configuration = RaftConfiguration(voters=frozenset(initial_voters))
        self._configuration = self._initial_configuration
        # The log index of the entry that set ``_configuration``; 0 for the
        # initial configuration.
        self._configuration_index = 0
        self._member_addrs = dict(member_addrs)
        self._send_message = send_message
        self._apply_command = apply_command
        self._on_become_leader = on_become_leader
        self._on_lose_leadership = on_lose_leadership
        self._logger = logger
        self._clock = clock
        # AD-39: false while this node's clock is fenced -- it then neither
        # campaigns, leads, nor mints entries; it still votes and follows.
        self._may_lead = may_lead

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
        # Leader: the last instant each member answered this leader -- from
        # when the leadership began, or the member joined the configuration
        # -- and when this leadership began (CheckQuorum, silent_members).
        self._last_response_at: dict[str, float] = {}
        self._leadership_started_at = 0.0
        # The last instant this member knew a quorum stood behind its
        # group's leader (``last_quorum_contact``).
        self._last_quorum_contact: float | None = None
        self._proposal_waiters: dict[int, asyncio.Future[bool]] = {}
        # ReadIndex (Raft thesis 6.4): the round of the last heartbeats
        # sent, the latest round each member answered in this leadership,
        # and the reads waiting for a quorum to answer theirs.
        self._read_round = 0
        self._check_compaction_settings(snapshot_state, snapshot_entries, snapshot_catch_up_entries)
        self._snapshot_entries = snapshot_entries
        self._snapshot_catch_up_entries = snapshot_catch_up_entries
        # AD-52 section 14: the entry schemas this member reads, the newest
        # each member reported reading, and the entry applying stopped at
        # (one this member cannot read) -- None while it applies freely.
        self._schema_versions = schema_versions
        self._reported_schema_versions: dict[str, int] = {}
        self._apply_halted_at: int | None = None
        # Observability (AD-52 section 18): how many of each happened here.
        self._elections_started = 0
        self._elections_won = 0
        self._proposals_committed = 0
        self._proposals_failed = 0
        self._snapshots_sent = 0
        self._snapshots_installed = 0
        self._acknowledged_read_rounds: dict[str, int] = {}
        self._read_waiters: list[tuple[int, asyncio.Future[bool]]] = []
        # The round of the AppendEntries being answered (echoed).
        self._answering_read_round = 0
        # Leader leases (AD-52 section 11; Raft thesis 6.4.1), opt-in: they
        # assume clock RATES drift apart by at most
        # ``leader_lease_drift_bound`` (offsets do not matter -- every
        # interval is measured on one clock). A follower refuses votes for
        # ELECTION_TIMEOUT_MIN after it hears from its leader, so once a
        # quorum answered a round, no other leader can be elected before
        # that round's send time + ELECTION_TIMEOUT_MIN on any member's
        # clock -- the lease, shortened by the drift bound, during which
        # ReadIndex needs no round of its own. None: no leases.
        self._leader_lease_seconds = self._leader_lease_seconds_for(leader_lease_drift_bound)
        # When each heartbeat round no quorum has answered yet was sent
        # (only while leases are on), and when this leadership's lease runs
        # out.
        self._round_sent_at: dict[int, float] = {}
        # The newest round number ever stamped with a send time. A number
        # is never stamped twice: a member's answer names only the round's
        # number, so an answer to an earlier round reusing it would be
        # credited to the later one -- a lease from a send time that member
        # never heard.
        self._newest_stamped_round = 0
        self._lease_expires_at = 0.0
        # ReadIndex answers a valid lease served without a round.
        self._lease_reads = 0
        # A member that restarts has forgotten the leader it last heard: in
        # a group with leases (every member is configured alike) it
        # withholds votes for one minimum election timeout from its start,
        # as it would after hearing that leader -- a lease granted just
        # before the restart stays exclusive.
        self._votes_withheld_until = self._votes_withheld_until_for(leader_lease_drift_bound)
        self._proposal_timeout_seconds = proposal_timeout_seconds

        self._election_deadline = self._new_election_deadline()
        self._last_heartbeat_sent: float = 0.0
        self._destroyed = False
        # A quorum never has fewer members than a majority of the cohort's
        # configured size, whatever the configuration holds (AD-3: a
        # minority cannot commit).
        self._quorum_floor = self._quorum_floor_for(configured_cluster_size, initial_voters)
        self._snapshot_state = snapshot_state
        self._restore_snapshot = restore_snapshot
        self._snapshots = self._snapshot_manager_for(logger, node_id, snapshot_state, restore_snapshot)
        # The configuration in force at the snapshot point: what a member
        # falls back to when it truncates away every configuration entry
        # its log holds (the initial one until a snapshot is taken).
        self._snapshot_configuration = self._initial_configuration
        self._check_quorum_window = request_timeout_seconds + ELECTION_TIMEOUT_MAX
        self._storage = storage
        # What ``storage`` holds: the term and vote last written, the log
        # through ``_durable_index``, and what is still to be written --
        # entries, the index the durable log is cut from, a snapshot.
        self._persisted_term = 0
        self._persisted_vote: str | None = None
        self._pending_entries: list[RaftLogEntry] = []
        self._pending_truncate_from: int | None = None
        self._pending_snapshot: SnapshotRecord | None = None
        self._durable_index = 0
        # Whether ``storage`` holds the group's creation (its initial
        # voters), written with its first state.
        self._creation_persisted = False

    def _check_compaction_settings(
        self,
        snapshot_state: Callable[[], bytes] | None,
        snapshot_entries: int,
        snapshot_catch_up_entries: int,
    ) -> None:
        """Raises unless a compacting group (one with ``snapshot_state``)
        snapshots after at least one entry and keeps fewer entries --
        those since the snapshot plus the catch-up ones -- than its log
        holds (Raft section 7).

        Raises:
            ValueError: the compaction settings cannot hold.
        """
        if snapshot_state is not None and self._compaction_settings_invalid(
            snapshot_entries, snapshot_catch_up_entries
        ):
            raise ValueError(
                f"a compacting group needs snapshot_entries >= 1 and the entries it holds "
                f"({snapshot_entries} + {snapshot_catch_up_entries}) below the log's {self._log.max_entries}"
            )

    def _compaction_settings_invalid(self, snapshot_entries: int, snapshot_catch_up_entries: int) -> bool:
        """Whether a compacting group would snapshot after no entries,
        keep a negative catch-up, or hold more entries than its log."""
        return (
            snapshot_entries < 1
            or snapshot_catch_up_entries < 0
            or snapshot_entries + snapshot_catch_up_entries >= self._log.max_entries
        )

    @staticmethod
    def _leader_lease_seconds_for(leader_lease_drift_bound: float | None) -> float | None:
        """The lease a quorum's answer grants (Raft thesis 6.4.1): the
        minimum election timeout shortened by the drift bound; None
        without one (no leases)."""
        return (
            None
            if leader_lease_drift_bound is None
            else ELECTION_TIMEOUT_MIN / (1.0 + leader_lease_drift_bound)
        )

    @staticmethod
    def _votes_withheld_until_for(leader_lease_drift_bound: float | None) -> float:
        """Until when a starting member withholds votes: one minimum
        election timeout from now in a group with leases, else never."""
        return (
            float("-inf")
            if leader_lease_drift_bound is None
            else _DEFAULT_CLOCK.monotonic() + ELECTION_TIMEOUT_MIN
        )

    @staticmethod
    def _quorum_floor_for(configured_cluster_size: int | None, initial_voters: frozenset[str]) -> int:
        """A majority of the cohort's configured size -- of the initial
        voters when none is configured -- and never below one (AD-3)."""
        return (
            max(
                1,
                configured_cluster_size
                if configured_cluster_size is not None
                else len(initial_voters),
            )
            // 2
            + 1
        )

    @staticmethod
    def _snapshot_manager_for(
        logger: "Logger",
        node_id: str,
        snapshot_state: Callable[[], bytes] | None,
        restore_snapshot: Callable[[bytes], Awaitable[None]] | None,
    ) -> SnapshotManager | None:
        """A snapshot manager for a group that can both take and restore
        snapshots; None for one that never compacts."""
        return (
            SnapshotManager(logger=logger, node_id=node_id)
            if snapshot_state is not None and restore_snapshot is not None
            else None
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

    @property
    def last_applied_index(self) -> int:
        """The last entry this member applied to its state machine."""
        return self._last_applied

    @property
    def last_log_index(self) -> int:
        """The last entry this member's log holds, committed or not."""
        return self._log.last_index()

    @property
    def configuration(self) -> RaftConfiguration:
        """The latest configuration in this member's log, committed or not."""
        return self._configuration

    @property
    def last_quorum_contact(self) -> float | None:
        """The last instant this member knew a quorum of voters stood
        behind its group's leader: as the leader, when a quorum had last
        answered it within the CheckQuorum window (or it won its election);
        as a follower, when a valid leader's AppendEntries last reached it
        -- under CheckQuorum, that leader had a quorum within its window.
        None until either happened. A group this member last saw
        able to commit long ago may never commit again."""
        return self._last_quorum_contact

    @property
    def initial_voters(self) -> frozenset[str]:
        """The voters the group was created with -- the same on every
        member: whoever creates a group decides them, and every member
        that joins the group later is handed them."""
        return self._initial_configuration.voters

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
            await self._tick_locked()

    async def _tick_locked(self) -> None:
        """A leader checks it may still lead; a follower or candidate whose
        election timeout passed starts a PreVote round (Raft section 5.2,
        thesis 9.6). Lock must be held."""
        match self._role:
            case "leader":
                self._tick_leader()
            case "follower" | "candidate" if self._election_timed_out():
                await self._start_pre_vote_locked()

    def _tick_leader(self) -> None:
        """Relinquish leadership when this node may no longer lead, or no
        quorum of voters answered it within the CheckQuorum window (Raft
        thesis 6.2; ``request_timeout_seconds``).

        Heartbeats are sent by replicate_to_followers in the consensus
        coordinator. Stepping down keeps the term and the vote cast in it.
        A leadership younger than the window has had no chance to hear back
        yet: its election was its quorum's answer.
        """
        if not self._may_lead():
            self._step_down(self._current_term)
            return
        self._check_quorum()

    def _check_quorum(self) -> None:
        """CheckQuorum (Raft thesis 6.2): a leadership at least a window old
        keeps leading only while a quorum of voters -- itself among them --
        answered it within the window; otherwise it steps down, keeping the
        term and its vote."""
        now = _DEFAULT_CLOCK.monotonic()
        if now - self._leadership_started_at < self._check_quorum_window:
            return
        if self._configuration.has_quorum(self._members_answered_within_window(now), self._quorum_floor):
            self._last_quorum_contact = now
            return
        self._step_down(self._current_term)

    def _members_answered_within_window(self, now: float) -> set[str]:
        """This leader and every member that answered it within the
        CheckQuorum window before ``now``."""
        return {self._node_id} | {
            member
            for member, answered_at in self._last_response_at.items()
            if now - answered_at < self._check_quorum_window
        }

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
        # Only a voter campaigns: a learner, or a member its own log has
        # removed, follows.
        if not self._may_campaign():
            return
        self._pre_vote_term = self._current_term + 1
        self._pre_votes_received = {self._node_id}
        if self._configuration.has_quorum(self._pre_votes_received, self._quorum_floor):
            await self._start_election_locked()
            return
        await self._broadcast_request_vote(term=self._pre_vote_term, pre_vote=True)

    def _may_campaign(self) -> bool:
        """Whether this node may stand for election: its clock is not fenced
        (AD-39) and its latest configuration counts it as a voter (Raft
        section 6: learners and removed members never campaign)."""
        return self._may_lead() and self._node_id in self._configuration.all_voters

    async def _start_election_locked(self) -> None:
        """Transition to candidate and request votes. Lock must be held.

        Every path into a campaign (timeout PreVote, a PreVote majority
        arriving later, an explicit start_election) passes here, so a node
        that may not lead never becomes a candidate.
        """
        if not self._may_campaign():
            self._pre_vote_term = None
            self._pre_votes_received = set()
            return
        self._pre_vote_term = None
        self._pre_votes_received = set()
        self._current_term += 1
        self._role = "candidate"
        self._elections_started += 1
        self._voted_for = self._node_id
        self._votes_received = {self._node_id}
        self._current_leader = None
        self._election_deadline = self._new_election_deadline()
        # The term and the vote for itself are durable before they can be
        # counted: a restart must never vote again in this term.
        await self._persist_locked()

        # Single-node cluster wins immediately
        if self._configuration.has_quorum(self._votes_received, self._quorum_floor):
            self._transition_to_leader()
            # The term's first entry counts toward commit once durable.
            await self._persist_locked()
            await self._apply_committed_locked()
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
        # Votes are asked of every voter of the configuration (both sets
        # while joint); a learner's would not count.
        for peer_id in self._other_members(self._configuration.ordered_all_voters):
            if addr := self._member_addrs.get(peer_id):
                await self._send_message(addr, request)

    def _other_members(self, members: Iterable[str]) -> Iterator[str]:
        """``members`` in their order, without this node -- lazily, so a
        caller that awaits between members sees each as it comes."""
        for member in members:
            if member != self._node_id:
                yield member

    async def handle_request_vote(self, request: RequestVote) -> RequestVoteResponse:
        """Handle an incoming RequestVote RPC."""
        async with self._lock:
            if (unpersisted_answer := self._vote_answer_without_persisting(request)) is not None:
                return unpersisted_answer
            return await self._decide_vote_locked(request)

    def _vote_answer_without_persisting(self, request: RequestVote) -> RequestVoteResponse | None:
        """The answer to a vote request that changes no persistent state --
        a destroyed group's, one withheld after a restart (leases), a
        PreVote's, or a refusal under leader stickiness -- or None when the
        request must be decided (and persisted)."""
        if self._destroyed:
            return self._vote_response(granted=False)

        if _DEFAULT_CLOCK.monotonic() < self._votes_withheld_until:
            return self._vote_response(granted=False, pre_vote=request.pre_vote)

        return self._pre_vote_or_sticky_answer(request)

    def _pre_vote_or_sticky_answer(self, request: RequestVote) -> RequestVoteResponse | None:
        """A PreVote's answer (Raft thesis 9.6), or a refusal while a live
        leader is evidently in charge; None for a real vote to decide."""
        if request.pre_vote:
            return self._vote_response(
                granted=self._should_grant_pre_vote(request), pre_vote=True
            )

        # Leader stickiness (Raft thesis 4.2.3): while a live leader is
        # evidently in charge, a vote request neither moves this term
        # nor wins a vote -- a disruptive member cannot depose it.
        if self._heard_from_leader_recently():
            return self._vote_response(granted=False)
        return None

    async def _decide_vote_locked(self, request: RequestVote) -> RequestVoteResponse:
        """Decide a real vote (Raft section 5.2, 5.4.1): a newer term is
        adopted first; the vote, if granted, resets the election timer; the
        term and vote are durable before the answer goes. Lock held."""
        # Step down if request has higher term
        if request.term > self._current_term:
            self._step_down(request.term)

        granted = self._should_grant_vote(request)
        if granted:
            self._voted_for = request.candidate_id
            self._election_deadline = self._new_election_deadline()

        # The term and vote this answer speaks for are durable first.
        await self._persist_locked()
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
        if self._leader_is_live():
            return False
        if request.term <= self._current_term:
            return False
        return self._candidate_log_is_current(
            request.last_log_index, request.last_log_term
        )

    def _leader_is_live(self) -> bool:
        """Whether this member leads, or heard from a valid leader within
        the minimum election timeout (Raft thesis 9.6: no PreVote grant)."""
        return self._role == "leader" or self._heard_from_leader_recently()

    def _heard_from_leader_recently(self) -> bool:
        """A valid leader's AppendEntries arrived within the minimum
        election timeout -- no follower could have timed out on it yet."""
        if self._role == "leader":
            return False
        if not self._leader_contact_known():
            return False
        return (
            _DEFAULT_CLOCK.monotonic() - self._last_leader_contact
            < ELECTION_TIMEOUT_MIN
        )

    def _leader_contact_known(self) -> bool:
        """Whether this member knows its leader and when it last heard it."""
        return self._current_leader is not None and self._last_leader_contact is not None

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
            self._count_vote(response)

    def _count_vote(self, response: RequestVoteResponse) -> None:
        """A candidate adopts a newer term (Raft section 5.1) or counts a
        vote granted in its own term. Lock held."""
        if self._role != "candidate":
            return
        if response.term > self._current_term:
            self._step_down(response.term)
            return
        self._tally_granted_vote(response)

    def _tally_granted_vote(self, response: RequestVoteResponse) -> None:
        """Count a vote granted in this term; a quorum of the configuration
        (both voter sets while joint) wins the election (Raft section 5.2)."""
        if not self._vote_counts_this_term(response):
            return

        self._votes_received.add(response.voter_id)
        if self._configuration.has_quorum(self._votes_received, self._quorum_floor):
            self._transition_to_leader()

    def _vote_counts_this_term(self, response: RequestVoteResponse) -> bool:
        """Whether ``response`` grants a vote in this candidate's term."""
        return response.vote_granted and response.term == self._current_term

    async def _handle_pre_vote_response(self, response: RequestVoteResponse) -> None:
        """Count a PreVote grant; campaign for real on a majority. Lock held."""
        if not self._pre_vote_round_open():
            return
        if response.term > self._current_term:
            self._step_down(response.term)
            return
        await self._count_pre_vote(response)

    def _pre_vote_round_open(self) -> bool:
        """Whether this member, not leading, has a PreVote round under way."""
        return self._pre_vote_term is not None and self._role != "leader"

    async def _count_pre_vote(self, response: RequestVoteResponse) -> None:
        """Count a PreVote grant; a quorum of grants starts the real
        election (Raft thesis 9.6). Lock held."""
        if not response.vote_granted:
            return

        self._pre_votes_received.add(response.voter_id)
        if self._configuration.has_quorum(self._pre_votes_received, self._quorum_floor):
            await self._start_election_locked()

    def _transition_to_leader(self) -> None:
        """Become leader. Initialize next_index and match_index for every
        member of the configuration, voters and learners alike."""
        self._role = "leader"
        self._elections_won += 1
        self._current_leader = self._node_id
        self._leadership_started_at = _DEFAULT_CLOCK.monotonic()
        self._last_quorum_contact = self._leadership_started_at
        self._reset_follower_progress()
        self._append_leader_first_entry()
        # A leader that is its own quorum commits the entry now: no
        # follower's answer will ever advance its commit index, and until
        # something commits in its term nothing earlier applies either.
        self._advance_commit_index()

        if self._on_become_leader:
            self._on_become_leader()

    def _reset_follower_progress(self) -> None:
        """Raft Figure 2 (leader state, reinitialized after election): each
        other member of the configuration was last heard as the leadership
        began, is next sent the entry after the log, and matches nothing."""
        followers = list(self._other_members(self._configuration.ordered_members))
        self._last_response_at = dict.fromkeys(followers, self._leadership_started_at)
        next_idx = self._log.last_index() + 1

        self._next_index = dict.fromkeys(followers, next_idx)
        self._match_index = dict.fromkeys(followers, 0)

    def _append_leader_first_entry(self) -> None:
        """An entry of an earlier term commits only behind one of this term
        (Raft 5.4.2), so a new leader appends one at once (Raft 8): an
        uncommitted configuration change the last leader began, or else a
        blank entry. Without it, entries the last leader committed stayed
        unapplied here and on every follower until the group's job next
        proposed something -- which a job whose leader died never does,
        so a job that ended read as running to every survivor."""
        if self._configuration_index > self._commit_index:
            configuration_entry = self._append_new_entry(self._configuration.dump(), RAFT_CONFIGURATION_COMMAND)
            self._configuration_index = configuration_entry.index
            return
        self._append_new_entry(b"", RAFT_NO_OP_COMMAND)

    def _append_new_entry(self, command: bytes, command_type: str) -> RaftLogEntry:
        """Mint an entry of this term after the log's last -- stamped by the
        node's HLC, in the schema every member reads -- append it, and queue
        it to be written."""
        entry = RaftLogEntry(
            term=self._current_term,
            index=self._log.last_index() + 1,
            command=command,
            command_type=command_type,
            job_id=self._job_id,
            hlc=self._clock.now(),
            schema_version=self._write_schema_version(),
        )
        self._log.append(entry)
        self._pending_entries.append(entry)
        return entry

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
        """Send AppendEntries (entries or heartbeat) to every follower --
        with leases on, as a round of its own (unless a read just opened
        one), whose send time a quorum's answer turns into the lease."""
        self._last_heartbeat_sent = now = _DEFAULT_CLOCK.monotonic()
        if self._leader_lease_seconds is not None:
            self._stamp_heartbeat_round(now)
        for peer_id in self._other_members(self._configuration.ordered_members):
            await self._send_append_entries_to(peer_id)
        # Entries new since the last round are written while they travel;
        # this leader counts itself for them once they are durable.
        await self._persist_locked()

    def _stamp_heartbeat_round(self, now: float) -> None:
        """Give this heartbeat a round number never stamped before and
        record when it was sent (Raft thesis 6.4.1: a lease runs from the
        send time of a round a quorum answered)."""
        if self._read_round <= self._newest_stamped_round:
            self._read_round += 1
        self._newest_stamped_round = self._read_round
        self._round_sent_at[self._read_round] = now
        # A round older than a lease can grant none: answered or not,
        # it goes.
        for stale_round in self._rounds_older_than_lease(now):
            del self._round_sent_at[stale_round]

    def _rounds_older_than_lease(self, now: float) -> list[int]:
        """The rounds sent a lease or more before ``now``."""
        return [
            sent_round
            for sent_round, sent_at in self._round_sent_at.items()
            if now - sent_at >= self._leader_lease_seconds
        ]

    async def _send_append_entries_to(self, peer_id: str) -> None:
        """Build and send AppendEntries to one follower -- or, when the
        entry it needs next is compacted away, the snapshot."""
        addr = self._member_addrs.get(peer_id)
        if addr is None:
            return

        next_idx = self._next_index.get(peer_id, 1)
        if (install := self._install_snapshot_for(next_idx)) is not None:
            self._snapshots_sent += 1
            await self._send_message(addr, install)
            return
        await self._send_message(addr, self._append_entries_from(next_idx))

    def _install_snapshot_for(self, next_idx: int) -> InstallSnapshot | None:
        """The snapshot to send a member whose next entry was compacted away
        (Raft section 7), or None when the log still holds it."""
        if self._snapshots is None or next_idx > self._log.snapshot_index:
            return None
        return self._snapshots.build_install_snapshot_message(
            self._job_id, self._current_term, self._node_id
        )

    def _append_entries_from(self, next_idx: int) -> AppendEntries:
        """AppendEntries carrying the log from ``next_idx`` on, after the
        entry before it (Raft section 5.3's consistency check)."""
        prev_index = next_idx - 1
        prev_term = self._log.term_at(prev_index) or 0

        entries = self._log.get_range(next_idx, self._log.last_index() + 1)

        return AppendEntries(
            job_id=self._job_id,
            term=self._current_term,
            leader_id=self._node_id,
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=self._commit_index,
            read_round=self._read_round,
        )

    async def handle_append_entries(self, request: AppendEntries) -> AppendEntriesResponse:
        """Handle an incoming AppendEntries RPC."""
        async with self._lock:
            self._answering_read_round = request.read_round
            if self._destroyed:
                return self._append_response(success=False, match_index=0)

            try:
                return await self._answer_append_entries_locked(request)
            finally:
                # Whatever this answer speaks for -- term, entries, cuts -- is durable before it goes.
                await self._persist_locked()

    async def _answer_append_entries_locked(self, request: AppendEntries) -> AppendEntriesResponse:
        """Answer a leader's AppendEntries (Raft section 5.3): refuse a stale
        term, follow the leader, refuse at a log mismatch, else append.
        Lock held; the caller persists before the answer goes."""
        if not self._accept_leader_term(request.term, request.leader_id):
            return self._append_response(success=False, match_index=0)

        # Valid leader heartbeat -- reset election timer
        self._follow_leader(request.leader_id)

        # Check log consistency
        if not self._log_matches_at(request.prev_log_index, request.prev_log_term):
            conflict = self._find_conflict_info(request.prev_log_index)
            return self._append_response(
                success=False,
                match_index=0,
                conflict_term=conflict[0],
                conflict_index=conflict[1],
            )

        return await self._append_from_leader_locked(request)

    def _accept_leader_term(self, term: int, leader_id: str) -> bool:
        """Whether a leader's message of ``term`` is to be followed (Raft
        section 5.1): a newer term is adopted first, a stale one refused.
        A same-term leader claim means this node's local leadership view
        is stale: it steps down so only one writer remains for the term."""
        if term > self._current_term:
            self._step_down(term)

        if term < self._current_term:
            return False

        self._step_down_if_rival_leader(term, leader_id)
        return True

    def _step_down_if_rival_leader(self, term: int, leader_id: str) -> None:
        """A leader hearing another leader of its own term steps down: its
        leadership view is stale (Raft section 5.2: one leader per term)."""
        if self._role == "leader" and leader_id != self._node_id:
            self._step_down(term)

    def _follow_leader(self, leader_id: str) -> None:
        """Record a valid leader's contact: it leads, it (and, under
        CheckQuorum, a quorum behind it) was heard now, the election timer
        restarts and any PreVote round or candidacy ends (Raft section 5.2)."""
        self._current_leader = leader_id
        self._last_leader_contact = _DEFAULT_CLOCK.monotonic()
        self._last_quorum_contact = self._last_leader_contact
        self._election_deadline = self._new_election_deadline()
        self._pre_vote_term = None

        if self._role == "candidate":
            self._role = "follower"

    async def _append_from_leader_locked(self, request: AppendEntries) -> AppendEntriesResponse:
        """Append a matching leader's entries and follow its commit index
        (Raft section 5.3) -- unless their HLCs run ahead of this node's
        clock beyond the offset bound (AD-39). Lock held."""
        # AD-39: refuse entries stamped further ahead of this node's
        # clock than the offset bound -- they never reach this log, so
        # a skewed leader cannot commit through this member.
        if (offset_error := self._first_offset_violation(request.entries)) is not None:
            await self._logger.log(RaftWarning(
                message=f"Refused AppendEntries from {request.leader_id}: {offset_error}",
                node_id=self._node_id,
                job_id=self._job_id,
            ))
            return self._append_response(
                success=False, match_index=0, clock_offset_rejected=True
            )

        # Append new entries (truncating conflicts)
        self._apply_entries_from_leader(request.entries)
        self._receive_entry_clocks(request.entries)

        # Advance commit index
        await self._follow_leader_commit_locked(request.leader_commit)

        return self._append_response(
            success=True, match_index=self._log.last_index()
        )

    def _receive_entry_clocks(self, entries: list[RaftLogEntry]) -> None:
        """Merge each appended entry's HLC into this node's clock (AD-39)."""
        for entry in entries:
            self._clock.receive(entry.hlc)

    async def _follow_leader_commit_locked(self, leader_commit: int) -> None:
        """Raft Figure 2: if leaderCommit > commitIndex, commitIndex =
        min(leaderCommit, index of last new entry); then apply. Lock held."""
        if leader_commit > self._commit_index:
            self._commit_index = min(
                leader_commit, self._log.last_index()
            )
            await self._apply_committed_locked()

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

        return conflict_term, self._first_index_of_term(prev_index, conflict_term)

    def _first_index_of_term(self, index: int, term: int) -> int:
        """Walk back from ``index`` (of ``term``) to the first entry of that
        term the log holds after its snapshot (Raft section 5.3's
        accelerated log backtracking)."""
        first_index = index
        while first_index > self._log.snapshot_index + 1:
            if self._log.term_at(first_index - 1) != term:
                break
            first_index -= 1

        return first_index

    def _apply_entries_from_leader(self, entries: list[RaftLogEntry]) -> None:
        """Append entries from leader, truncating any conflicts. A
        configuration entry takes effect as it is appended; truncating the
        one in force falls back to the newest the log still holds."""
        for entry in entries:
            self._take_entry_from_leader(entry)

    def _take_entry_from_leader(self, entry: RaftLogEntry) -> None:
        """Raft Figure 2 (AppendEntries receiver, steps 3-4): an entry that
        conflicts with the log cuts it from its index; one past the log's
        end is appended."""
        if self._conflicts_with_log(entry):
            self._truncate_conflicting_suffix(entry.index)
        if self._log.last_index() < entry.index:
            self._append_leader_entry(entry)

    def _conflicts_with_log(self, entry: RaftLogEntry) -> bool:
        """Whether the log holds a different term at ``entry``'s index
        (Raft section 5.3: the entry and all that follow it are deleted)."""
        existing_term = self._log.term_at(entry.index)
        return existing_term is not None and existing_term != entry.term

    def _truncate_conflicting_suffix(self, index: int) -> None:
        """Delete the log from ``index`` (Raft section 5.3): what is not yet
        written is dropped before it is, what is durable is cut on disk, and
        a configuration cut away falls back to the newest still held."""
        self._log.truncate_from(index)
        # Only what is durable is cut on disk; what is not yet
        # written is dropped before it is.
        self._pending_entries = self._pending_entries_before(index)
        if index <= self._durable_index:
            self._cut_durable_log_from(index)
        if index <= self._configuration_index:
            self._configuration = self._snapshot_configuration
            self._configuration_index = self._log.snapshot_index
            self._adopt_newest_held_configuration()

    def _pending_entries_before(self, index: int) -> list[RaftLogEntry]:
        """The unwritten entries that precede ``index``."""
        return [
            pending_entry for pending_entry in self._pending_entries if pending_entry.index < index
        ]

    def _cut_durable_log_from(self, index: int) -> None:
        """Queue a cut of the durable log from ``index`` -- the lowest cut
        queued so far -- and count only what precedes it as durable."""
        self._pending_truncate_from = (
            index
            if self._pending_truncate_from is None
            else min(self._pending_truncate_from, index)
        )
        self._durable_index = index - 1

    def _adopt_newest_held_configuration(self) -> None:
        """Take the newest configuration entry the log holds after its
        snapshot, if any (Raft section 6: a member uses the latest
        configuration in its log, committed or not)."""
        for index, held_entry in self._held_entries_newest_first(self._log.last_index()):
            if held_entry.command_type == RAFT_CONFIGURATION_COMMAND:
                self._configuration = RaftConfiguration.load(held_entry.command)
                self._configuration_index = index
                return

    def _held_entries_newest_first(self, through_index: int) -> Iterator[tuple[int, RaftLogEntry]]:
        """Each entry the log holds from ``through_index`` back to its
        snapshot, newest first, with its index -- lazily."""
        for index in range(through_index, self._log.snapshot_index, -1):
            if (held_entry := self._log.get(index)) is not None:
                yield index, held_entry

    def _append_leader_entry(self, entry: RaftLogEntry) -> None:
        """Append a leader's entry past the log's end and queue it to be
        written; a configuration entry takes effect at once."""
        self._log.append(entry)
        self._pending_entries.append(entry)
        self._adopt_if_configuration(entry)

    def _adopt_if_configuration(self, entry: RaftLogEntry) -> None:
        """A configuration entry takes effect as it enters the log (Raft
        section 6)."""
        if entry.command_type == RAFT_CONFIGURATION_COMMAND:
            self._configuration = RaftConfiguration.load(entry.command)
            self._configuration_index = entry.index

    def _append_response(
        self,
        *,
        success: bool,
        match_index: int,
        conflict_term: int | None = None,
        conflict_index: int | None = None,
        clock_offset_rejected: bool = False,
    ) -> AppendEntriesResponse:
        return AppendEntriesResponse(
            job_id=self._job_id,
            term=self._current_term,
            success=success,
            follower_id=self._node_id,
            match_index=match_index,
            conflict_term=conflict_term,
            conflict_index=conflict_index,
            clock_offset_rejected=clock_offset_rejected,
            read_round=self._answering_read_round,
            schema_version=self._schema_versions[1],
        )

    def _first_offset_violation(
        self, entries: list[RaftLogEntry]
    ) -> ClockOffsetExceededError | None:
        for entry in entries:
            try:
                self._clock.check(entry.hlc)
            except ClockOffsetExceededError as offset_error:
                return offset_error
        return None

    async def handle_append_entries_response(self, response: AppendEntriesResponse) -> None:
        """Handle response from a follower."""
        async with self._lock:
            if not self._admit_follower_response(response):
                return
            self._record_follower_answer(response)
            await self._act_on_replication_result_locked(response)

    def _admit_follower_response(self, response: AppendEntriesResponse | InstallSnapshotResponse) -> bool:
        """Whether a live leader acts on a member's answer: one of its own
        term from a member it still replicates to. A newer term steps it
        down first (Raft section 5.1). A member that has left the
        configuration is no longer replicated to: a late answer of its must
        not bring its progress back."""
        if not self._leads_live_group():
            return False
        if response.term > self._current_term:
            self._step_down(response.term)
            return False
        return self._answers_this_leadership(response)

    def _leads_live_group(self) -> bool:
        """Whether this member leads a group not yet destroyed."""
        return not self._destroyed and self._role == "leader"

    def _answers_this_leadership(self, response: AppendEntriesResponse | InstallSnapshotResponse) -> bool:
        """Whether ``response`` is of this term, from a member still
        replicated to."""
        return response.term == self._current_term and response.follower_id in self._next_index

    def _record_follower_answer(self, response: AppendEntriesResponse) -> None:
        """Any answer of this term -- refusals too -- is the member standing
        behind this leader (CheckQuorum, Raft thesis 6.2), and behind it in
        the read round it echoes (ReadIndex, thesis 6.4)."""
        self._last_response_at[response.follower_id] = _DEFAULT_CLOCK.monotonic()
        self._reported_schema_versions[response.follower_id] = response.schema_version
        if response.read_round > self._acknowledged_read_rounds.get(response.follower_id, 0):
            self._acknowledged_read_rounds[response.follower_id] = response.read_round
            self._extend_lease_from_answered_rounds()
            self._resolve_confirmed_reads()

    def _extend_lease_from_answered_rounds(self) -> None:
        """With leases on, extend the lease from the newest stamped round a
        quorum answered (Raft thesis 6.4.1)."""
        if self._lease_rounds_outstanding():
            self._extend_lease_to_newest_answered_round()

    def _lease_rounds_outstanding(self) -> bool:
        """Whether leases are on and some stamped round awaits a quorum."""
        return self._leader_lease_seconds is not None and bool(self._round_sent_at)

    def _extend_lease_to_newest_answered_round(self) -> None:
        """The newest round a quorum answered extends the lease from when
        it was sent; it and every older round go."""
        for sent_round in sorted(self._round_sent_at, reverse=True):
            if self._quorum_acknowledged_round(sent_round):
                self._grant_lease_through(sent_round)
                return

    def _quorum_acknowledged_round(self, read_round: int) -> bool:
        """Whether this leader and the members that answered ``read_round``
        or a later one form a quorum (both voter sets while joint)."""
        return self._configuration.has_quorum(
            {self._node_id}
            | {
                member
                for member, acknowledged in self._acknowledged_read_rounds.items()
                if acknowledged >= read_round
            },
            self._quorum_floor,
        )

    def _grant_lease_through(self, sent_round: int) -> None:
        """Extend the lease to ``sent_round``'s send time plus the lease,
        never shortening it, and drop that round and every older one."""
        self._lease_expires_at = max(
            self._lease_expires_at,
            self._round_sent_at[sent_round] + self._leader_lease_seconds,
        )
        for answered_round in self._stamped_rounds_through(sent_round):
            del self._round_sent_at[answered_round]

    def _stamped_rounds_through(self, sent_round: int) -> list[int]:
        """The stamped rounds no newer than ``sent_round``."""
        return [
            round_number for round_number in self._round_sent_at if round_number <= sent_round
        ]

    def _resolve_confirmed_reads(self) -> None:
        """ReadIndex (Raft thesis 6.4): resolve each read whose round a
        quorum answered; keep the rest waiting."""
        if self._read_waiters:
            self._read_waiters = self._unconfirmed_reads()

    def _unconfirmed_reads(self) -> list[tuple[int, asyncio.Future[bool]]]:
        """The reads still waiting once every confirmed one is resolved."""
        pending_reads = []
        for read_round, read_waiter in self._read_waiters:
            if self._read_still_waiting(read_round, read_waiter):
                pending_reads.append((read_round, read_waiter))
        return pending_reads

    def _read_still_waiting(self, read_round: int, read_waiter: asyncio.Future[bool]) -> bool:
        """Whether a read is still waiting: a done one is dropped, one a
        quorum confirmed is resolved True and dropped."""
        if read_waiter.done():
            return False
        if self._quorum_acknowledged_round(read_round):
            read_waiter.set_result(True)
            return False
        return True

    async def _act_on_replication_result_locked(self, response: AppendEntriesResponse) -> None:
        """A success advances the member's progress and the commit index
        (Raft section 5.3); a clock-offset refusal is reported; any other
        refusal backs ``next_index`` off. Lock held."""
        if response.success:
            self._next_index[response.follower_id] = response.match_index + 1
            self._match_index[response.follower_id] = response.match_index
            self._advance_commit_index()
            await self._apply_committed_locked()
            return
        if getattr(response, "clock_offset_rejected", False):
            # Not a log conflict: this leader's clock is ahead of the
            # follower's beyond the bound. Its log stays put; the entries
            # are re-sent (and refused) until the clocks agree.
            await self._logger.log(RaftWarning(
                message=(
                    f"{response.follower_id} refused entries: this leader's clock is "
                    "beyond the HLC offset bound of its own"
                ),
                node_id=self._node_id,
                job_id=self._job_id,
            ))
            return
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
        for index, entry in self._held_entries_newest_first(self._log.last_index()):
            if matches(entry):
                return index
        return None

    def members_holding(self, index: int) -> set[str] | None:
        """Members known to hold log ``index`` (leader only; None otherwise).

        The leader always holds its own entries; a follower counts once
        its acknowledged match index reaches ``index``. This is what a
        placement rule beyond a bare majority (e.g. AD-38 GLOBAL: copies
        in more than one region) is judged against.
        """
        if not self._leads_live_group() or index > self._log.last_index():
            return None
        return {self._node_id} | self._followers_holding(index)

    def _followers_holding(self, index: int) -> set[str]:
        """The members whose acknowledged match index reaches ``index``."""
        return {
            member for member, match in self._match_index.items() if match >= index
        }

    def _advance_commit_index(self) -> None:
        """Advance commit_index to the highest index of this term a quorum
        of the configuration holds (Sections 5.3/5.4; both voter sets while
        joint, Section 6)."""
        for candidate_index in range(self._log.last_index(), self._commit_index, -1):
            if self._quorum_holds_entry_of_this_term(candidate_index):
                self._commit_index = candidate_index
                return

    def _quorum_holds_entry_of_this_term(self, index: int) -> bool:
        """Whether the entry at ``index`` is of the current term (Raft
        section 5.4.2: only those commit by counting replicas) and a quorum
        holds it."""
        if self._log.term_at(index) != self._current_term:
            return False
        return self._configuration.has_quorum(self._holders_of(index), self._quorum_floor)

    def _holders_of(self, index: int) -> set[str]:
        """The members holding ``index``: this leader once it is durable
        here (Raft thesis 10.2.1), and each follower that matched it."""
        return ({self._node_id} if index <= self._durable_index else set()) | self._followers_holding(index)

    # =========================================================================
    # Client Interface
    # =========================================================================

    async def propose(self, command: bytes, command_type: str) -> tuple[bool, int]:
        """
        Propose a new command (leader only).

        The entry's HLC is minted from the node's shared HybridLogicalClock;
        it is replicated with the entry, so every follower reads the same
        wall-clock-derived ``entry.timestamp`` and apply handlers can use it
        as a deterministic time source (AD-38/AD-39).

        Returns:
            (success, index) -- success is False if not leader or log at capacity.
        """
        async with self._lock:
            if not self._may_propose():
                return False, 0
            index, waiter = await self._propose_locked(command, command_type)

        return await self._await_proposal(index, waiter)

    def _may_propose(self) -> bool:
        """Whether this member leads a live group whose log has room and
        whose clock is not fenced (AD-39)."""
        return self._leads_live_group() and not self._log.is_at_capacity and self._may_lead()

    async def _propose_locked(self, command: bytes, command_type: str) -> tuple[int, asyncio.Future[bool]]:
        """Append the command as a new entry with a waiter for its commit,
        then commit, apply and replicate what can be now. Lock held."""
        entry = self._append_new_entry(command, command_type)
        index = entry.index
        waiter = asyncio.get_running_loop().create_future()
        self._proposal_waiters[index] = waiter
        # Commit is event-driven: a group that is its own majority
        # commits and applies here, and a larger group replicates the
        # entry now instead of on the next heartbeat tick.
        self._advance_commit_index()
        await self._apply_committed_locked()
        # Applying can complete a change that removes this leader.
        await self._replicate_while_leading_locked()
        return index, waiter

    async def _replicate_while_leading_locked(self) -> None:
        """A member still leading sends the new entries now; sending
        persisted them, so a group that is its own quorum committed them
        there and applies them. Lock held."""
        if self._role == "leader":
            await self._send_append_entries_to_followers()
            # Sending persisted the entry: a group that is its own
            # quorum committed it there.
            await self._apply_committed_locked()

    async def _await_proposal(self, index: int, waiter: asyncio.Future[bool]) -> tuple[bool, int]:
        """Wait up to the proposal timeout for the entry at ``index`` to
        commit (True) or be lost (False); a timed-out or cancelled waiter
        is dropped."""
        try:
            committed = await _DEFAULT_CLOCK.wait_for(
                waiter,
                timeout=self._proposal_timeout_seconds,
            )
            self._count_proposal_outcome(committed)
            return committed, index
        except asyncio.TimeoutError:
            async with self._lock:
                self._proposal_waiters.pop(index, None)
            self._proposals_failed += 1
            return False, index
        except asyncio.CancelledError:
            async with self._lock:
                self._proposal_waiters.pop(index, None)
            raise

    def _count_proposal_outcome(self, committed: bool) -> None:
        """Count a proposal as committed or failed (AD-52 section 18)."""
        if committed:
            self._proposals_committed += 1
        else:
            self._proposals_failed += 1

    def _write_schema_version(self) -> int:
        """The schema a new entry is written in (AD-52 section 14): the
        newest every member reads -- this one, and each member it replicates
        to as last reported; the oldest this build writes while a member has
        not reported yet."""
        newest_readable = self._schema_versions[1]
        for member in self._other_members(self._configuration.ordered_members):
            if (reported := self._reported_schema_versions.get(member)) is None:
                return self._schema_versions[0]
            newest_readable = min(newest_readable, reported)
        return max(self._schema_versions[0], newest_readable)

    @property
    def write_schema_version(self) -> int:
        """The schema this member writes new entries in now."""
        return self._write_schema_version()

    @property
    def apply_halted_at(self) -> int | None:
        """The entry applying stopped at -- one this member cannot read --
        or None."""
        return self._apply_halted_at

    def metrics(self) -> dict[str, int | dict[str, int]]:
        """This member's view of its group (AD-52 section 18): term and
        indexes, what happened here since it was created, and -- while it
        leads -- each follower's replication lag in entries."""
        return {
            "term": self._current_term,
            "commit_index": self._commit_index,
            "applied_index": self._last_applied,
            "elections_started": self._elections_started,
            "elections_won": self._elections_won,
            "proposals_committed": self._proposals_committed,
            "proposals_failed": self._proposals_failed,
            "snapshots_sent": self._snapshots_sent,
            "snapshots_installed": self._snapshots_installed,
            "lease_reads": self._lease_reads,
            "apply_halted_at": self._apply_halted_at or 0,
            "follower_lag": self._follower_lag(),
        }

    def _follower_lag(self) -> dict[str, int]:
        """While leading, how many committed entries each follower lacks;
        empty otherwise."""
        return {
            follower: max(0, self._commit_index - match_index)
            for follower, match_index in self._match_index.items()
        } if self._role == "leader" else {}

    def applied_entries_after(self, index: int) -> list[RaftLogEntry] | None:
        """The entries this member applied after ``index``, in order -- or
        None when compaction has passed ``index`` (the caller needs the
        state, not the entries). A watcher resumes from the index it last
        saw (AD-52 section 9)."""
        if index < self._log.snapshot_index:
            return None
        return self._log.get_range(index + 1, self._last_applied + 1)

    async def read_index(self) -> int | None:
        """ReadIndex (Raft thesis 6.4): the commit index a linearizable read
        must see, once this leader has confirmed it still leads -- or None
        when it does not lead, has not committed an entry of its own term
        yet (it does not know the commit index until then), or loses its
        quorum's answer within the proposal timeout. Reads arriving
        together share one round of heartbeats; no log entry is written.
        Serve the read once the local apply index reaches the result.
        """
        async with self._lock:
            if not self._may_serve_read():
                return None
            read_index = self._commit_index
            if self._read_confirmed_without_round():
                return read_index
            waiter = await self._open_read_round_locked()

        return await self._read_index_once_confirmed(read_index, waiter)

    def _may_serve_read(self) -> bool:
        """Whether this member leads a live group and committed an entry of
        its own term -- until then it does not know the commit index (Raft
        thesis 6.4)."""
        return self._leads_live_group() and self._log.term_at(self._commit_index) == self._current_term

    def _read_confirmed_without_round(self) -> bool:
        """Whether a read needs no heartbeat round: this leader is its own
        quorum, or holds a valid lease -- no other leader can exist yet
        (Raft thesis 6.4.1); a lease read is counted."""
        if self._configuration.has_quorum({self._node_id}, self._quorum_floor):
            return True
        # A valid lease: no other leader can exist yet, so this one's
        # commit index is the read's without a round of its own.
        if _DEFAULT_CLOCK.monotonic() < self._lease_expires_at:
            self._lease_reads += 1
            return True
        return False

    async def _open_read_round_locked(self) -> asyncio.Future[bool]:
        """Open a heartbeat round for a read and send it; the waiter
        resolves True once a quorum answers that round. Lock held."""
        self._read_round += 1
        waiter = asyncio.get_running_loop().create_future()
        self._read_waiters.append((self._read_round, waiter))
        await self._send_append_entries_to_followers()
        return waiter

    async def _read_index_once_confirmed(self, read_index: int, waiter: asyncio.Future[bool]) -> int | None:
        """``read_index`` once a quorum confirmed this leadership; None
        when it was lost or the proposal timeout passed."""
        return read_index if await self._await_read_confirmation(waiter) else None

    async def _await_read_confirmation(self, waiter: asyncio.Future[bool]) -> bool | None:
        """Wait up to the proposal timeout for the read's round to be
        confirmed; a timed-out (None) or cancelled read's waiter is
        dropped."""
        try:
            return await _DEFAULT_CLOCK.wait_for(waiter, timeout=self._proposal_timeout_seconds)
        except asyncio.TimeoutError:
            async with self._lock:
                self._read_waiters = self._read_waiters_other_than(waiter)
            return None
        except asyncio.CancelledError:
            async with self._lock:
                self._read_waiters = self._read_waiters_other_than(waiter)
            raise

    def _read_waiters_other_than(self, waiter: asyncio.Future[bool]) -> list[tuple[int, asyncio.Future[bool]]]:
        """The waiting reads without ``waiter``."""
        return [entry for entry in self._read_waiters if entry[1] is not waiter]

    async def apply_committed_entries(self) -> int:
        """
        Apply all committed but unapplied entries to the state machine.

        Returns:
            Number of entries applied.
        """
        async with self._lock:
            if self._destroyed:
                return 0
            applied_count = await self._apply_committed_locked()
            await self._compact_locked()
            await self._persist_locked()
            return applied_count

    async def _compact_locked(self) -> None:
        """Snapshot through the last applied entry once ``snapshot_entries``
        were applied since the last snapshot, and drop the log up to
        ``snapshot_catch_up_entries`` before it (Raft section 7, etcd's
        policy): a member that far behind catches up from the log; one the
        cut passes by -- it joins later, falls further behind, or is dead
        and not yet removed -- is sent the snapshot. Compacting after every
        entry left every watcher, and every member one entry behind, only
        snapshots. Lock must be held."""
        if not self._snapshot_due():
            return
        configuration = self._configuration_at_last_applied()
        if (
            await self._snapshots.create_snapshot(
                self._log, self._snapshot_state(), self._last_applied, configuration
            )
            is None
        ):
            return
        self._compact_log_behind_snapshot()
        self._snapshot_configuration = configuration
        self._pending_snapshot = self._current_snapshot_record(configuration)

    def _snapshot_due(self) -> bool:
        """Whether this compacting group applied ``snapshot_entries`` since
        its last snapshot (Raft section 7)."""
        if self._snapshots is None:
            return False
        snapshot = self._snapshots.current_snapshot
        return (
            self._last_applied - (snapshot.last_included_index if snapshot is not None else 0)
            >= self._snapshot_entries
        )

    def _configuration_at_last_applied(self) -> RaftConfiguration:
        """The configuration in force at the last applied entry -- what a
        snapshot through it records (Raft section 7)."""
        if self._configuration_index > self._last_applied:
            return self._newest_configuration_through(self._last_applied)
        return self._configuration

    def _newest_configuration_through(self, index: int) -> RaftConfiguration:
        """The newest configuration entry the log holds at or before
        ``index``, else the snapshot's configuration."""
        for _held_index, entry in self._held_entries_newest_first(index):
            if entry.command_type == RAFT_CONFIGURATION_COMMAND:
                return RaftConfiguration.load(entry.command)
        return self._snapshot_configuration

    def _compact_log_behind_snapshot(self) -> None:
        """Drop the log through ``snapshot_catch_up_entries`` before the
        last applied entry, if that passes the current snapshot point."""
        if (cut := self._last_applied - self._snapshot_catch_up_entries) > self._log.snapshot_index:
            self._log.compact_through(cut, self._log.term_at(cut) or 0)

    def _current_snapshot_record(self, configuration: RaftConfiguration) -> SnapshotRecord:
        """The snapshot just taken, as the record that persists it."""
        snapshot = self._snapshots.current_snapshot
        return SnapshotRecord(
            group_id=self._job_id,
            last_index=snapshot.last_included_index,
            last_term=snapshot.last_included_term,
            configuration=configuration.dump(),
            state=snapshot.state_data,
        )

    async def handle_install_snapshot(self, request: InstallSnapshot) -> InstallSnapshotResponse:
        """Install a leader's snapshot (Raft section 7): the state machine
        is replaced by it, the log keeps only entries that follow it."""
        async with self._lock:
            if self._destroyed or self._snapshots is None:
                return self._install_response(success=False, match_index=0)

            try:
                return await self._answer_install_snapshot_locked(request)
            finally:
                # The term and the installed snapshot are durable before the answer goes.
                await self._persist_locked()

    async def _answer_install_snapshot_locked(self, request: InstallSnapshot) -> InstallSnapshotResponse:
        """Answer a leader's InstallSnapshot (Raft section 7, Figure 13):
        refuse a stale term, follow the leader, and install the snapshot
        unless this member already holds that state. Lock held; the caller
        persists before the answer goes."""
        if not self._accept_leader_term(request.term, request.leader_id):
            return self._install_response(success=False, match_index=0)

        self._follow_leader(request.leader_id)

        if request.last_included_index <= self._commit_index:
            # Already holds that state: replication goes on from it.
            return self._install_response(
                success=True, match_index=request.last_included_index
            )

        return await self._install_snapshot_locked(request)

    async def _install_snapshot_locked(self, request: InstallSnapshot) -> InstallSnapshotResponse:
        """Replace the log's prefix and the state machine with the snapshot;
        the configuration is the newest the log still holds after it, else
        the snapshot's. Lock held."""
        snapshot = RaftSnapshot(
            last_included_index=request.last_included_index,
            last_included_term=request.last_included_term,
            state_data=request.data,
            configuration=RaftConfiguration.load(request.configuration),
        )
        if await self._snapshots.apply_snapshot(self._log, snapshot):
            self._pending_snapshot = SnapshotRecord(
                group_id=self._job_id,
                last_index=request.last_included_index,
                last_term=request.last_included_term,
                configuration=request.configuration,
                state=request.data,
            )
        self._snapshot_configuration = snapshot.configuration
        self._configuration = snapshot.configuration
        self._configuration_index = snapshot.last_included_index
        self._adopt_newest_held_configuration()
        self._commit_index = snapshot.last_included_index
        self._last_applied = snapshot.last_included_index
        await self._restore_snapshot(snapshot.state_data)
        self._snapshots_installed += 1
        return self._install_response(
            success=True, match_index=snapshot.last_included_index
        )

    def _install_response(self, *, success: bool, match_index: int) -> InstallSnapshotResponse:
        return InstallSnapshotResponse(
            job_id=self._job_id,
            term=self._current_term,
            success=success,
            follower_id=self._node_id,
            match_index=match_index,
        )

    async def handle_install_snapshot_response(self, response: InstallSnapshotResponse) -> None:
        """A member installed (or already held) the snapshot: replication
        goes on from it."""
        async with self._lock:
            if not self._admit_follower_response(response):
                return
            self._last_response_at[response.follower_id] = _DEFAULT_CLOCK.monotonic()
            if not response.success:
                return
            self._match_index[response.follower_id] = max(
                self._match_index.get(response.follower_id, 0), response.match_index
            )
            self._next_index[response.follower_id] = self._match_index[response.follower_id] + 1
            self._advance_commit_index()
            await self._apply_committed_locked()

    async def _apply_committed_locked(self) -> int:
        """Apply entries up to commit_index and resolve their proposals.

        Called wherever commit_index advances, so a proposal resolves as
        soon as it commits rather than on the next tick. Caller holds the
        lock.

        Configuration entries are Raft's own: they resolve their waiters but
        never reach the state machine. On the leader, a committed joint
        configuration is followed by the final one, and a committed final
        configuration that leaves this member out steps it down (Section 6).
        """
        applied_count = 0
        keep_applying = True
        while keep_applying:
            applied_now, halted = await self._apply_through_commit_locked()
            applied_count += applied_now
            keep_applying = not halted and await self._follow_committed_configuration_locked()
        return applied_count

    async def _apply_through_commit_locked(self) -> tuple[int, bool]:
        """Apply each committed entry in order: how many were applied, and
        whether applying halted at one this member cannot read. Lock held."""
        applied_count = 0
        while self._last_applied < self._commit_index:
            if await self._halt_at_unreadable_entry_locked():
                return applied_count, True
            applied_count += await self._apply_next_entry_locked()
        return applied_count, False

    async def _halt_at_unreadable_entry_locked(self) -> bool:
        """An entry this member cannot read is never skipped -- its state
        would silently fork from every other member's. Applying stops there
        until the member runs code that reads it (AD-52 section 14); the
        halt is reported once. Lock held."""
        next_entry = self._log.get(self._last_applied + 1)
        if not self._is_unreadable(next_entry):
            return False
        await self._report_apply_halt_locked(next_entry)
        return True

    def _is_unreadable(self, entry: RaftLogEntry | None) -> bool:
        """Whether ``entry`` is held and its schema is outside the ones this
        member reads."""
        return entry is not None and not (
            self._schema_versions[0] <= entry.schema_version <= self._schema_versions[1]
        )

    async def _report_apply_halt_locked(self, next_entry: RaftLogEntry) -> None:
        """Record and log, once per entry, that applying stopped at it."""
        if self._apply_halted_at != next_entry.index:
            self._apply_halted_at = next_entry.index
            await self._logger.log(RaftError(
                message=(
                    f"Applying stopped at entry {next_entry.index} ({next_entry.command_type}): "
                    f"schema version {next_entry.schema_version} is outside "
                    f"{self._schema_versions[0]}-{self._schema_versions[1]} this member reads"
                ),
                node_id=self._node_id,
                job_id=self._job_id,
                term=self._current_term,
            ))

    async def _apply_next_entry_locked(self) -> int:
        """Apply the entry after the last applied one, if the log holds it:
        1 when applied, else 0. Lock held."""
        self._last_applied += 1
        if not (entry := self._log.get(self._last_applied)):
            return 0
        if entry.command_type not in (
            RAFT_CONFIGURATION_COMMAND,
            RAFT_NO_OP_COMMAND,
        ):
            await self._apply_command(entry)
        self._resolve_proposal(self._last_applied)
        return 1

    def _resolve_proposal(self, index: int) -> None:
        """Resolve the local proposal at ``index`` as committed."""
        waiter = self._proposal_waiters.pop(index, None)
        if waiter is not None and not waiter.done():
            waiter.set_result(True)

    async def _follow_committed_configuration_locked(self) -> bool:
        """Raft section 6 on a leader whose configuration committed: a
        joint one is followed by the final one (True: apply again); a
        final one that leaves this leader out steps it down. Lock held."""
        if not self._leads_with_committed_configuration():
            return False

        if not self._configuration.is_joint:
            self._step_down_if_removed()
            return False

        await self._append_final_configuration_locked()
        return True

    def _leads_with_committed_configuration(self) -> bool:
        """Whether this member leads and its configuration committed."""
        return self._role == "leader" and self._configuration_index <= self._commit_index

    def _step_down_if_removed(self) -> None:
        """A leader the committed final configuration leaves out of its
        voters steps down (Raft section 6)."""
        if self._node_id not in self._configuration.voters:
            self._step_down(self._current_term)

    async def _append_final_configuration_locked(self) -> None:
        """The joint configuration committed: the final one follows, and
        members it leaves out are no longer replicated to once it does.
        Lock held."""
        final_configuration = RaftConfiguration(
            voters=self._configuration.voters,
            learners=self._configuration.learners,
        )
        configuration_entry = self._append_new_entry(final_configuration.dump(), RAFT_CONFIGURATION_COMMAND)
        self._configuration = final_configuration
        self._configuration_index = configuration_entry.index
        self._forget_departed_members(final_configuration)
        self._advance_commit_index()
        await self._send_append_entries_to_followers()

    def _forget_departed_members(self, configuration: RaftConfiguration) -> None:
        """Stop replicating to the members ``configuration`` leaves out:
        their progress, answers, schema reports and read rounds go."""
        for departed_member in self._replicated_members_outside(configuration):
            del self._next_index[departed_member]
            self._match_index.pop(departed_member, None)
            self._last_response_at.pop(departed_member, None)
            self._reported_schema_versions.pop(departed_member, None)
            self._acknowledged_read_rounds.pop(departed_member, None)

    def _replicated_members_outside(self, configuration: RaftConfiguration) -> list[str]:
        """The members replicated to that ``configuration`` leaves out."""
        return [
            member for member in self._next_index if member not in configuration.members
        ]

    # =========================================================================
    # Membership
    # =========================================================================

    async def change_membership(
        self,
        voters: frozenset[str],
        learners: frozenset[str],
    ) -> bool:
        """
        Leader only: move the group to ``voters`` and ``learners`` through
        its log (AD-52 sections 6-7).

        Learners change in one step: they never count in a quorum. A change
        of voters appends a joint configuration, the current voters
        outgoing; the final one follows once the joint one commits. One
        change at a time -- the caller asks again once the group settles.

        Returns whether a change began: False when this member is not the
        leader or may not lead now, the group is mid-change (joint, or its
        configuration not yet committed), its log is full, or the
        configuration already is the target.
        """
        if not voters:
            raise ValueError("a Raft configuration needs at least one voter")

        async with self._lock:
            if (next_configuration := self._next_configuration_toward(voters, learners)) is None:
                return False
            await self._begin_configuration_change_locked(next_configuration)
            return True

    def _next_configuration_toward(
        self, voters: frozenset[str], learners: frozenset[str]
    ) -> RaftConfiguration | None:
        """The configuration this leader appends next toward ``voters`` and
        ``learners`` -- None when it may not change membership now or the
        configuration already is the target."""
        if not self._may_change_membership():
            return None

        target_configuration = RaftConfiguration(voters=voters, learners=learners - voters)
        if target_configuration == self._configuration:
            return None

        return self._step_toward(target_configuration)

    def _may_change_membership(self) -> bool:
        """Whether this member leads a live group, may lead now (AD-39), its
        configuration settled (one change at a time, Raft section 6) and its
        log has room."""
        return self._may_lead_live_group() and self._configuration_settled() and not self._log.is_at_capacity

    def _may_lead_live_group(self) -> bool:
        """Whether this member leads a live group and its clock is not
        fenced (AD-39)."""
        return self._leads_live_group() and self._may_lead()

    def _configuration_settled(self) -> bool:
        """Whether the configuration is final (not joint) and committed."""
        return not self._configuration.is_joint and self._configuration_index <= self._commit_index

    def _step_toward(self, target_configuration: RaftConfiguration) -> RaftConfiguration:
        """The target itself when only learners change; otherwise the joint
        configuration with the current voters outgoing (Raft section 6)."""
        return (
            target_configuration
            if target_configuration.voters == self._configuration.voters
            else RaftConfiguration(
                voters=target_configuration.voters,
                learners=target_configuration.learners,
                outgoing_voters=self._configuration.voters,
            )
        )

    async def _begin_configuration_change_locked(self, next_configuration: RaftConfiguration) -> None:
        """Append ``next_configuration`` -- in force at once (Raft section
        6) -- start replicating to its new members, stop replicating to
        learners it removes, then commit, apply and replicate. Lock held."""
        configuration_entry = self._append_new_entry(next_configuration.dump(), RAFT_CONFIGURATION_COMMAND)
        self._configuration = next_configuration
        self._configuration_index = configuration_entry.index
        self._track_joining_members(next_configuration, configuration_entry.index)
        # A learner leaving in one step is no longer replicated to; its
        # progress goes with it (voters leave with the final entry).
        self._forget_departed_members(next_configuration)

        # A group that is its own quorum commits the change once sending
        # persisted it.
        self._advance_commit_index()
        await self._apply_committed_locked()
        await self._replicate_while_leading_locked()

    def _track_joining_members(self, configuration: RaftConfiguration, first_index: int) -> None:
        """Replicate to each member ``configuration`` adds from
        ``first_index``, as heard from now; members already tracked keep
        their progress."""
        joined_at = _DEFAULT_CLOCK.monotonic()
        for member in self._other_members(configuration.ordered_members):
            self._next_index.setdefault(member, first_index)
            self._match_index.setdefault(member, 0)
            self._last_response_at.setdefault(member, joined_at)

    async def reconcile_membership(self, live_members: frozenset[str]) -> bool:
        """
        Leader only: take the next step moving the group's configuration
        toward ``live_members`` -- the cluster's live members, this one
        among them. The coordinators call it each tick for the groups they
        lead (AD-52 slice B): membership changes only through the log, so
        no member can count a vote or a commit the others would not.

        A voter no longer live stops voting -- unless fewer live voters
        would remain than the group's quorum floor: that change could never
        commit, and the group would be stuck in its joint configuration. A
        live member that is not a voter follows as a learner; a learner
        holding every committed entry is promoted, so making it a voter
        cannot stall commits. One change at a time (``change_membership``):
        the next step is taken once the last one committed.

        Returns whether a change began.
        """
        if not self._needs_reconciliation(live_members):
            return False

        voters = self._voters_toward(live_members)
        return await self.change_membership(voters, live_members - voters)

    def _needs_reconciliation(self, live_members: frozenset[str]) -> bool:
        """Whether this member leads a live group whose settled
        configuration is not yet exactly ``live_members`` as voters."""
        return (
            self._leads_live_group()
            and self._configuration_settled()
            and not self._voters_are_exactly(live_members)
        )

    def _voters_are_exactly(self, live_members: frozenset[str]) -> bool:
        """Whether the configuration has no learners and its voters are
        ``live_members``."""
        return not self._configuration.learners and self._configuration.voters == live_members

    def _voters_toward(self, live_members: frozenset[str]) -> frozenset[str]:
        """The live voters -- or every voter, while too few are live to
        meet the quorum floor -- plus the live learners that hold every
        committed entry."""
        configuration = self._configuration
        live_voters = configuration.voters & live_members
        caught_up_learners = self._caught_up_learners(live_members)
        return (
            live_voters if len(live_voters) >= self._quorum_floor else configuration.voters
        ) | caught_up_learners

    def _caught_up_learners(self, live_members: frozenset[str]) -> set[str]:
        """The live learners whose match index reaches the commit index."""
        return {
            learner
            for learner in self._configuration.learners & live_members
            if self._match_index.get(learner, 0) >= self._commit_index
        }

    def silent_members(self, seconds: float) -> frozenset[str]:
        """Leader only: the members of the configuration that have not
        answered this leader for ``seconds`` -- counted from when the
        leadership began, or the member joined the configuration. Empty on
        a member that does not lead."""
        if not self._leads_live_group():
            return frozenset()
        return self._members_unanswered_for(seconds)

    def _members_unanswered_for(self, seconds: float) -> frozenset[str]:
        """The members last heard ``seconds`` or more ago."""
        now = _DEFAULT_CLOCK.monotonic()
        return frozenset(
            member
            for member, answered_at in self._last_response_at.items()
            if now - answered_at >= seconds
        )

    def set_cohort_size(self, cohort_size: int) -> None:
        """The cluster's cohort changed size (AD-52 ``ResizeCluster``):
        quorums are never smaller than a majority of the new one. A local
        setting, not a log entry: within any one configuration a majority
        of its voters already intersects every other, so the floor only
        keeps the group from shrinking to too few voters -- members may
        apply it at slightly different moments."""
        self._quorum_floor = max(1, cohort_size) // 2 + 1

    def update_member_addresses(self, addrs: dict[str, tuple[str, int]]) -> None:
        """Replace the address book members are reached at. Membership
        itself changes only through the log (``change_membership``,
        ``reconcile_membership``)."""
        self._member_addrs = dict(addrs)

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def recover(self, recovered: RecoveredRaftGroup) -> None:
        """Resume this group as this node's disk held it (D1): the term
        and vote, the snapshot and the state it restores, the log after
        it, and the configuration the latest of them set. Called once,
        before the group handles anything. Commit and apply restart from
        the snapshot (Raft: volatile); the leader's commit index brings
        the rest. The node's clock merges every recovered entry, so what
        it mints next never precedes its log.

        Raises:
            ClockOffsetExceededError: this node's clock fell further behind
                its own log than the offset bound (AD-39) -- it must not
                stamp entries until it is fixed.
            ValueError: a snapshot for a group that does not snapshot.
        """
        async with self._lock:
            self._creation_persisted = True
            self._current_term = self._persisted_term = recovered.term
            self._voted_for = self._persisted_vote = recovered.voted_for
            if (snapshot := recovered.snapshot) is not None:
                await self._recover_snapshot_locked(snapshot)
            self._recover_entries(recovered.entries)
            self._durable_index = self._log.last_index()

    async def _recover_snapshot_locked(self, snapshot: SnapshotRecord) -> None:
        """Install the recovered snapshot: log prefix, configuration,
        commit and apply point, and the state machine it restores.

        Raises:
            ValueError: the group does not snapshot.
        """
        if self._snapshots is None or self._restore_snapshot is None:
            raise ValueError(f"group {self._job_id} recovered a snapshot it cannot restore")
        configuration = RaftConfiguration.load(snapshot.configuration)
        await self._snapshots.apply_snapshot(
            self._log,
            RaftSnapshot(
                last_included_index=snapshot.last_index,
                last_included_term=snapshot.last_term,
                state_data=snapshot.state,
                configuration=configuration,
            ),
        )
        self._snapshot_configuration = configuration
        self._configuration = configuration
        self._configuration_index = snapshot.last_index
        self._commit_index = snapshot.last_index
        self._last_applied = snapshot.last_index
        await self._restore_snapshot(snapshot.state)

    def _recover_entries(self, entries: list[RaftLogEntry]) -> None:
        """Append the recovered entries, merging each one's HLC first; a
        configuration entry takes effect as it is appended.

        Raises:
            ClockOffsetExceededError: an entry's HLC is beyond the offset
                bound of this node's clock (AD-39).
        """
        for entry in entries:
            self._clock.receive(entry.hlc)
            self._log.append(entry)
            self._adopt_if_configuration(entry)

    async def release(self) -> None:
        """This node is done with the group for good: its state is
        dropped from disk (D1 P10) and from memory."""
        async with self._lock:
            if not self._destroyed and self._storage.durable:
                await self._storage.write([GroupReleasedRecord(group_id=self._job_id)])
        self.destroy()

    async def _persist_locked(self) -> None:
        """Write what changed since the last write -- term and vote, a cut
        of the durable log, new entries, a snapshot -- as one durable
        unit, in that order; the leader then counts the new entries
        toward commit. Lock must be held.

        A write that fails leaves memory ahead of disk: this member can
        no longer answer as itself, so the group stops here (fail-stop)
        and the error goes on to the caller.
        """
        term = self._current_term
        voted_for = self._voted_for
        hard_state_changed = self._hard_state_changed(term, voted_for)
        if not hard_state_changed and not self._has_unwritten_log_changes():
            return
        await self._write_unwritten_locked(term, voted_for, hard_state_changed)
        self._mark_written(term, voted_for)

    def _hard_state_changed(self, term: int, voted_for: str | None) -> bool:
        """Whether the term or vote differs from what storage holds."""
        return term != self._persisted_term or voted_for != self._persisted_vote

    def _has_unwritten_log_changes(self) -> bool:
        """Whether entries, a cut of the durable log or a snapshot wait to
        be written."""
        return (
            bool(self._pending_entries)
            or self._pending_truncate_from is not None
            or self._pending_snapshot is not None
        )

    async def _write_unwritten_locked(self, term: int, voted_for: str | None, hard_state_changed: bool) -> None:
        """Write the unwritten records to durable storage as one unit;
        volatile storage holds nothing. Lock held.

        Raises:
            BaseException: the write failed -- the group was destroyed.
        """
        if not self._storage.durable:
            return
        records = self._creation_and_hard_state_records(term, voted_for, hard_state_changed)
        records.extend(self._log_change_records())
        await self._write_or_leave_group_locked(records, term)

    def _creation_and_hard_state_records(
        self, term: int, voted_for: str | None, hard_state_changed: bool
    ) -> list[GroupCreatedRecord | HardStateRecord | TruncateFromRecord | EntriesRecord | SnapshotRecord]:
        """The group's creation, if not yet written, then its hard state,
        if changed."""
        records: list[GroupCreatedRecord | HardStateRecord | TruncateFromRecord | EntriesRecord | SnapshotRecord] = []
        if not self._creation_persisted:
            records.append(
                GroupCreatedRecord(
                    group_id=self._job_id,
                    member_id=self._node_id,
                    initial_voters=sorted(self._initial_configuration.voters),
                )
            )
        if hard_state_changed:
            records.append(HardStateRecord(group_id=self._job_id, term=term, voted_for=voted_for))
        return records

    def _log_change_records(self) -> list[TruncateFromRecord | EntriesRecord | SnapshotRecord]:
        """The cut of the durable log, the new entries, then the snapshot
        -- each only if pending."""
        records = self._cut_and_entries_records()
        if self._pending_snapshot is not None:
            records.append(self._pending_snapshot)
        return records

    def _cut_and_entries_records(self) -> list[TruncateFromRecord | EntriesRecord | SnapshotRecord]:
        """The pending cut of the durable log, then the pending entries."""
        records: list[TruncateFromRecord | EntriesRecord | SnapshotRecord] = []
        if self._pending_truncate_from is not None:
            records.append(TruncateFromRecord(group_id=self._job_id, index=self._pending_truncate_from))
        if self._pending_entries:
            records.append(EntriesRecord(group_id=self._job_id, entries=self._pending_entries))
        return records

    async def _write_or_leave_group_locked(
        self,
        records: list[GroupCreatedRecord | HardStateRecord | TruncateFromRecord | EntriesRecord | SnapshotRecord],
        term: int,
    ) -> None:
        """Write ``records``; a failed write destroys the group (fail-stop),
        is logged, and propagates. Lock held.

        Raises:
            BaseException: whatever the write raised.
        """
        try:
            await self._storage.write(records)
        except BaseException as storage_error:
            self.destroy()
            await self._logger.log(RaftError(
                message=f"Raft state could not be written; this member left the group: {storage_error}",
                node_id=self._node_id,
                job_id=self._job_id,
                term=term,
            ))
            raise

    def _mark_written(self, term: int, voted_for: str | None) -> None:
        """Storage now holds the creation, ``term``, ``voted_for`` and the
        whole log; a leader counts itself for the entries now durable
        (Raft thesis 10.2.1)."""
        self._creation_persisted = True
        self._persisted_term = term
        self._persisted_vote = voted_for
        self._pending_entries = []
        self._pending_truncate_from = None
        self._pending_snapshot = None
        self._durable_index = self._log.last_index()
        if self._role == "leader":
            self._advance_commit_index()

    def destroy(self) -> None:
        """
        Release all state. Called on job completion.

        After destroy(), all public methods become no-ops.
        """
        self._destroyed = True
        self._log.clear()
        self._next_index.clear()
        self._match_index.clear()
        self._last_response_at.clear()
        self._votes_received.clear()
        self._fail_pending_proposals()
        self._member_addrs.clear()
        if self._snapshots is not None:
            self._snapshots.clear()

    # =========================================================================
    # Helpers
    # =========================================================================

    def _step_down(self, new_term: int) -> None:
        """Step down to follower for a newer or conflicting term.

        A vote is cast once per term: it is released only when the term
        advances, so a same-term step-down cannot let this node vote
        twice in one term (two leaders).
        """
        was_leader = self._role == "leader"
        if new_term > self._current_term:
            self._voted_for = None
        self._current_term = new_term
        self._role = "follower"
        self._last_response_at = {}
        self._pre_vote_term = None
        self._election_deadline = self._new_election_deadline()
        self._fail_pending_proposals()
        self._notify_lost_leadership(was_leader)

    def _notify_lost_leadership(self, was_leader: bool) -> None:
        """Tell the owner a leadership this member held ended."""
        if was_leader and self._on_lose_leadership:
            self._on_lose_leadership()

    def _fail_pending_proposals(self) -> None:
        """Fail all local proposals that have not reached committed apply,
        and every read waiting for this leadership's confirmation."""
        self._fail_waiters(self._proposal_waiters.values())
        self._proposal_waiters.clear()
        self._fail_waiters(read_waiter for _read_round, read_waiter in self._read_waiters)
        self._read_waiters = []
        self._acknowledged_read_rounds = {}
        self._round_sent_at = {}
        self._lease_expires_at = 0.0

    @staticmethod
    def _fail_waiters(waiters: Iterable[asyncio.Future[bool]]) -> None:
        """Resolve each waiter not yet done as failed (False)."""
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(False)

    @staticmethod
    def _new_election_deadline() -> float:
        """Randomized election deadline to prevent split votes."""
        timeout = _DEFAULT_RANDOM.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        return _DEFAULT_CLOCK.monotonic() + timeout
