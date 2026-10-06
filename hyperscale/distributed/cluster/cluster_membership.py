"""
A cluster's membership, kept by consensus (AD-52 slice C).

One Raft group per cluster -- a datacenter's managers, or the gate tier --
whose configuration IS the cluster's membership: the cluster grows and
shrinks through that group's log, its quorum is its committed voters'
(never fewer than a majority of the configured cohort), and a node that
joins follows the log as a learner before it votes.
"""

import asyncio
from collections import Counter
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING

from hyperscale.distributed.raft.logging_models import RaftDebug, RaftInfo, RaftWarning
from hyperscale.logging.hyperscale_logging_models import ClusterMembershipEvent
from hyperscale.distributed.raft.models.log_entry import RAFT_LOG_SCHEMA_VERSIONS
from hyperscale.distributed.raft.models.raft_configuration import RAFT_CONFIGURATION_COMMAND
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RaftConfiguration,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import HEARTBEAT_INTERVAL, RaftNode
from hyperscale.distributed.raft.raft_peer_outbox import RaftPeerOutbox
from hyperscale.distributed.raft.store.models import GroupReleasedRecord, RecoveredRaftGroup
from hyperscale.distributed.raft.store.raft_storage import RaftStorage
from hyperscale.distributed.raft.snapshot import InstallSnapshot, InstallSnapshotResponse
from hyperscale.distributed.runtime import Clock

from .cluster_join_error import ClusterJoinError
from .decode_join_message import decode_join_message
from .models import (
    ClusterHello,
    ClusterHelloReply,
    ClusterJoinReply,
    ClusterJoinRequest,
    ClusterLeaveReply,
    ClusterLeaveRequest,
    ClusterMemberId,
    ClusterMetricsReply,
    ClusterModeReply,
    ClusterModeRequest,
    ClusterResizeReply,
    ClusterResizeRequest,
    ClusterStatusReply,
    ClusterStatusRequest,
    ClusterWatchReply,
    ClusterWatchRequest,
    FoundCluster,
    FoundClusterReply,
)

if TYPE_CHECKING:
    from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock
    from hyperscale.distributed.jobs.logical_id_generator import LogicalIdGenerator
    from hyperscale.distributed.raft.models import RaftLogEntry
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger

FORMATION_DISCOVERING = "discovering"
FORMATION_FORMING = "forming"
FORMATION_JOINING = "joining"
FORMATION_FORMED = "formed"

CLUSTER_HELLO_ACTION = "cluster_hello"
FOUND_CLUSTER_ACTION = "found_cluster"
CLUSTER_JOIN_ACTION = "cluster_join"
CLUSTER_LEAVE_ACTION = "cluster_leave"
CLUSTER_MODE_ACTION = "cluster_mode"
CLUSTER_RESIZE_ACTION = "cluster_resize"
CLUSTER_STATUS_ACTION = "cluster_status"
CLUSTER_WATCH_ACTION = "cluster_watch"
CLUSTER_METRICS_ACTION = "cluster_metrics"
CLUSTER_REQUEST_VOTE_ACTION = "cluster_raft_request_vote"
CLUSTER_APPEND_ENTRIES_ACTION = "cluster_raft_append_entries"
CLUSTER_INSTALL_SNAPSHOT_ACTION = "cluster_raft_install_snapshot"

# Membership group log entries (each command is a member id): a member
# claims the address it is reached at -- the latest claim of an address
# holds it, one process per address -- and the group's leader releases the
# address of a holder it has not heard from for the tombstone retention.
CLAIM_ADDRESS_COMMAND = "cluster_claim_address"
RELEASE_ADDRESS_COMMAND = "cluster_release_address"
# Sets the cluster's mode (AD-52 section 13): the command is the mode.
CLUSTER_MODE_COMMAND = "cluster_mode"
# Changes the cohort by one address (AD-52 ``ResizeCluster``): the command
# is the new cohort, a JSON list of [host, port].
CLUSTER_RESIZE_COMMAND = "cluster_resize"

CLUSTER_MODE_OPEN = "open"
# No membership change commits: no claim, no release, no reconfiguration.
CLUSTER_MODE_FROZEN = "frozen"
# Frozen, and the tier refuses job submissions.
CLUSTER_MODE_READ_ONLY = "read-only"
CLUSTER_MODES = (CLUSTER_MODE_OPEN, CLUSTER_MODE_FROZEN, CLUSTER_MODE_READ_ONLY)

# A proposal's outcome as the logs name it, indexed by whether it committed.
_PROPOSAL_OUTCOMES = ("not committed", "committed")
# A held group's formation before it forms, indexed by whether it was joined.
_ADOPTED_FORMATIONS = (FORMATION_FORMING, FORMATION_JOINING)
# A watch event's kind for each address command (AD-52 section 9).
_ADDRESS_EVENT_KINDS: Mapping[str, str] = MappingProxyType(
    {CLAIM_ADDRESS_COMMAND: "claim", RELEASE_ADDRESS_COMMAND: "release"}
)
# The membership group's action for each Raft request type.
_RAFT_REQUEST_ACTIONS: Mapping[type, str] = MappingProxyType(
    {
        RequestVote: CLUSTER_REQUEST_VOTE_ACTION,
        AppendEntries: CLUSTER_APPEND_ENTRIES_ACTION,
        InstallSnapshot: CLUSTER_INSTALL_SNAPSHOT_ACTION,
    }
)

SendRequest = Callable[[tuple[str, int], str, bytes], Awaitable[bytes | Exception | None]]

# A node whose cluster has not formed knows of no members.
NO_NODE_ADDRESSES: Mapping[str, tuple[str, int]] = MappingProxyType({})


# Why a greeting or join is refused: before ``start`` resumed this node's
# disk (or after ``stop``), and while running, a founders digest from
# another cohort.
_NOT_RUNNING_REFUSAL = "this member is not running (its disk not yet resumed, or stopped)"
_COHORT_MISMATCH_REFUSAL = "configured with a different founding cohort"

class ClusterMembership:
    """
    This node's place in its cluster's membership group (AD-52 slice C).

    **The cohort.** The founders are the configured cohort, this node among
    them, and only they are members: every quorum needs at least a
    majority of the cohort (AD-3), and each cohort address holds one
    process at a time -- so no two groups can ever both hold a quorum of
    live members. That is what makes it safe to found a cluster without
    storage, and to found it again when the last one can no longer commit.

    **Formation.** Each formation round the node greets every founder
    (``ClusterHello``); each answer is the process at that address right
    now. A formed cluster is joined, never formed beside. Otherwise, once a
    quorum of the cohort answered as discovering, the lowest-addressed of
    them that could see such a quorum itself proposes a founding: those
    founders' member ids under a new cluster uuid (``FoundCluster``). Each
    founder adopts the first founding offered to it that names it, and
    only that one; a founding commits only through Raft -- with a quorum
    of its voters that is also a majority of the cohort -- so however many
    founders proposed, at most one founding commits. A founding that has
    not formed within a round, and fewer of whose voters still confirm it
    than its quorum, is abandoned.

    **Identity.** A member id is this incarnation's node id, its TCP
    address and a participation counter (``ClusterMemberId``). Leaving a
    group moves the node to a new participation: with no storage to
    remember its votes, only a new id keeps it from voting twice in a term.

    **Joining.** A node joins a formed cluster through its group's leader,
    which first greets the joiner's address -- the claim must come from
    the process there now, not a delayed request of one gone -- and then
    commits the joiner's claim of that address (``CLAIM_ADDRESS_COMMAND``).
    Every member applies the claims in the same order, and the latest
    claim of an address holds it.

    **Membership changes.** The group's Raft leader -- whichever member
    that is -- reconciles the configuration each tick toward the holders
    of the addresses. A holder that has not answered the leader for the
    tombstone retention -- unreachable from the leader's perspective,
    AD-52 section 8 -- has its address released through the log
    (``RELEASE_ADDRESS_COMMAND``), so no later leader adds it back; the
    process there claims it anew if it ever answers again.

    **Log compaction.** The group outlives every process in it, so its log
    is compacted through what it applied (``RaftNode``, Raft section 7):
    its state is the address holders, and a member the compaction point
    passes by -- one that joins later -- is sent them as a snapshot.

    **A cluster that can no longer commit.** A formed member runs
    formation rounds again whenever its group has not been operable
    (``RaftNode.last_quorum_contact``) for a round. It abandons the group
    once the answers prove it can never commit again -- a process other
    than the voter answers at the address of so many voters that the rest
    are no quorum -- or once it has been inoperable longer than the
    tombstone retention: then the members it waits on no longer hold the
    cluster back, just as the leader of an operable cluster would have
    removed them. Its members found the cluster anew.
    """

    __slots__ = (
        "_node_id",
        "_address",
        "_configured_digest",
        "_cohort",
        "_cohort_digest",
        "_previous_cohort_digest",
        "_accepted_founders_digests",
        "_admission_refusal",
        "_on_cohort_change",
        "_schema_versions",
        "_snapshot_entries",
        "_snapshot_catch_up_entries",
        "_leader_lease_drift_bound",
        "_storage",
        "_quorum",
        "_send_request",
        "_logger",
        "_task_runner",
        "_hlc",
        "_clock",
        "_may_lead",
        "_cluster_uuids",
        "_formation_interval_seconds",
        "_tombstone_retention_seconds",
        "_request_timeout_seconds",
        "_watch_wait_ceiling_seconds",
        "_participation",
        "_group",
        "_cluster_uuid",
        "_joined",
        "_formed",
        "_formed_event",
        "_adopted_at",
        "_discovering_seen",
        "_address_holders",
        "_mode",
        "_applied_progress",
        "_changes_applied",
        "_foundings_proposed",
        "_operator_requests",
        "_open_watches",
        "_led_term",
        "_signalled_applied_index",
        "_addressed_configuration",
        "_node_addresses",
        "_outbox",
        "_running",
        "_loop_tokens",
    )

    def __init__(
        self,
        node_id: str,
        address: tuple[str, int],
        founder_addresses: frozenset[tuple[str, int]],
        *,
        send_request: SendRequest,
        logger: "Logger",
        task_runner: "TaskRunner",
        hlc: "HybridLogicalClock",
        clock: Clock,
        may_lead: Callable[[], bool],
        cluster_uuids: "LogicalIdGenerator",
        formation_interval_seconds: float,
        tombstone_retention_seconds: float,
        request_timeout_seconds: float,
        watch_wait_ceiling_seconds: float,
        on_cohort_change: Callable[[frozenset[tuple[str, int]]], None] | None,
        snapshot_entries: int,
        snapshot_catch_up_entries: int,
        leader_lease_drift_bound: float | None,
        storage: RaftStorage,
        schema_versions: tuple[int, int] = RAFT_LOG_SCHEMA_VERSIONS,
    ) -> None:
        """
        Args:
            node_id: This process incarnation's node id -- never the same
                for two incarnations
            address: The TCP address this node is reached at
            founder_addresses: The configured cohort's TCP addresses
            send_request: Sends a request to an address, answering with
                the reply bytes, or the failure
            logger: Async logger
            task_runner: Runs the membership loops and the Raft outbox
            hlc: The node's hybrid logical clock (Raft entries' stamps)
            clock: The runtime clock
            may_lead: Whether this node may lead now (AD-39 clock fence)
            cluster_uuids: Mints a founding's cluster uuid
            formation_interval_seconds: Seconds between formation rounds
            tombstone_retention_seconds: Seconds a member may go unheard by
                the group's leader before it no longer holds the cluster
                back (AD-52 section 8)
            request_timeout_seconds: How long ``send_request`` waits on one
                request before giving up (the group's CheckQuorum window
                covers it, ``RaftNode``)
            watch_wait_ceiling_seconds: The longest a membership watch's
                long poll is held here (``CLUSTER_WATCH_WAIT_SECONDS``, the
                wait every watcher configured alike asks for): a watcher's
                own wait is cut to it, so none holds a poll open longer.
            snapshot_entries: Entries applied between the group's snapshots
            snapshot_catch_up_entries: Entries the group keeps before its
                snapshot point, for members and watches catching up
            leader_lease_drift_bound: The clock-rate drift the group's
                leader leases tolerate (AD-52 section 11), or None for no
                leases -- every member of a cluster configured alike
            schema_versions: The log entry schemas this build reads, oldest
                to newest (AD-52 section 14)
            on_cohort_change: Called with the cohort whenever a committed
                resize changes it (AD-52 ``ResizeCluster``): every quorum
                the node counts follows the cohort
        """
        self._node_id = node_id
        self._address = address
        # The cohort this process was launched with, then the one its
        # cluster's log holds once it learns it (kept for the process's
        # life, through any group it leaves).
        self._cohort = founder_addresses | {address}
        self._cohort_digest = hashlib.sha256(
            ",".join(f"{cohort_host}:{cohort_port}" for cohort_host, cohort_port in sorted(self._cohort)).encode()
        ).hexdigest()
        self._configured_digest = self._cohort_digest
        # The cohort before the last resize: nodes not yet relaunched with
        # the new one are still greeted and taken in (one step behind).
        self._previous_cohort_digest: str | None = None
        # The founders digests a greeting, join or founding may carry: this
        # cohort's first, then the one before the last resize once there is
        # one. Empty until ``start`` has resumed the group this node's disk
        # holds, and again after ``stop``: before that, answering
        # "discovering" -- or adopting a founding -- would invite a founding
        # beside the group the disk holds, which ``start`` would then
        # release. Never ``None``, so a message carrying None matches
        # nothing; a tuple, so a value of the wrong type is compared, never
        # hashed.
        self._accepted_founders_digests: tuple[str, ...] = ()
        self._admission_refusal = _NOT_RUNNING_REFUSAL
        self._on_cohort_change = on_cohort_change
        self._schema_versions = schema_versions
        self._snapshot_entries = snapshot_entries
        self._snapshot_catch_up_entries = snapshot_catch_up_entries
        self._leader_lease_drift_bound = leader_lease_drift_bound
        # Where the membership group keeps its persistent state (D1).
        self._storage = storage
        # AD-3: no quorum smaller than a majority of the cohort.
        self._quorum = len(self._cohort) // 2 + 1
        self._send_request = send_request
        self._logger = logger
        self._task_runner = task_runner
        self._hlc = hlc
        self._clock = clock
        self._may_lead = may_lead
        self._cluster_uuids = cluster_uuids
        self._formation_interval_seconds = formation_interval_seconds
        self._tombstone_retention_seconds = tombstone_retention_seconds
        self._request_timeout_seconds = request_timeout_seconds
        self._watch_wait_ceiling_seconds = watch_wait_ceiling_seconds
        # How many groups this node has left for good -- across restarts
        # when its disk keeps them (D1).
        self._participation = storage.participation
        self._group: RaftNode | None = None
        self._cluster_uuid: str | None = None
        # The group was adopted through a join (its cluster had formed),
        # not a founding; and whether it formed here.
        self._joined = False
        self._formed = False
        # Set while this node's group has formed (``wait_formed``).
        self._formed_event = asyncio.Event()
        self._adopted_at = 0.0
        self._discovering_seen = 0
        # The member holding each address: the group's founding voters,
        # then the claims and releases its log applied (replicated state).
        self._address_holders: dict[tuple[str, int], str] = {}
        # The cluster's mode, as its log applied it (replicated state).
        self._mode = CLUSTER_MODE_OPEN
        # Set (and replaced) whenever the group applies further: what a
        # watch's long poll waits on (AD-52 section 9).
        self._applied_progress = asyncio.Event()
        # Observability (AD-52 section 18), for this process's life.
        self._changes_applied: dict[str, int] = {}
        self._foundings_proposed = 0
        # Operator requests by "<kind>:received" and "<kind>:committed".
        self._operator_requests: dict[str, int] = {}
        self._open_watches = 0
        # The last term this member was seen leading in (leader_elected).
        self._led_term = 0
        self._signalled_applied_index = 0
        self._addressed_configuration: RaftConfiguration | None = None
        # The configuration's members by node id, for the rest of the node
        # (``node_addresses``): rebuilt when the configuration changes.
        self._node_addresses: Mapping[str, tuple[str, int]] = NO_NODE_ADDRESSES
        # The cluster group's outbox is the node's second: its sender
        # loops need an alias of their own (RaftPeerOutbox).
        self._outbox = RaftPeerOutbox(
            exchange=self._exchange,
            task_runner=task_runner,
            logger=logger,
            node_id=f"cluster-membership:{node_id}",
        )
        self._running = False
        self._loop_tokens: list[str] = []

    # =========================================================================
    # State
    # =========================================================================

    @property
    def member_id(self) -> str:
        return str(
            ClusterMemberId(
                node_id=self._node_id,
                host=self._address[0],
                port=self._address[1],
                participation=self._participation,
            )
        )

    @property
    def formed(self) -> bool:
        return self._formed

    async def wait_formed(self) -> None:
        """Return once this node's cluster has formed (at once if it has)."""
        await self._formed_event.wait()

    @property
    def cluster_uuid(self) -> str | None:
        return self._cluster_uuid if self._formed else None

    @property
    def formation(self) -> str:
        if self._group is None:
            return FORMATION_DISCOVERING
        if self._formed:
            return FORMATION_FORMED
        return _ADOPTED_FORMATIONS[self._joined]

    @property
    def voters(self) -> frozenset[str]:
        """The cluster's voters as this node's log holds them (empty
        until formed)."""
        if not self._formed or self._group is None:
            return frozenset()
        return self._group.configuration.voters

    @property
    def members(self) -> frozenset[str]:
        """Every member of the cluster -- voters and learners -- as this
        node's log holds them (empty until formed)."""
        if not self._formed or self._group is None:
            return frozenset()
        return self._group.configuration.members

    def node_addresses(self) -> Mapping[str, tuple[str, int]]:
        """The cluster's members as the rest of this node knows them: each
        process's node id and the TCP address it is reached at, this
        node's among them -- every member of the configuration its log
        holds. Empty until formed; the same object until the configuration
        changes."""
        return self._node_addresses if self._formed else NO_NODE_ADDRESSES

    @property
    def mode(self) -> str:
        """The cluster's mode as this node's log applied it."""
        return self._mode

    @property
    def read_only(self) -> bool:
        return self._mode == CLUSTER_MODE_READ_ONLY

    @property
    def leader_member_id(self) -> str | None:
        return None if self._group is None else self._group.current_leader

    def is_leader(self) -> bool:
        return self._group is not None and self._group.is_leader()

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """Resume the membership group this node's disk held (D1) -- as
        the member it took part as, releasing any group it had already
        left -- then run formation and the group."""
        if self._running:
            return
        await self._resume_recovered_groups()
        self._accepted_founders_digests = (self._cohort_digest, *({self._previous_cohort_digest} - {None}))
        self._admission_refusal = _COHORT_MISMATCH_REFUSAL
        self._running = True
        self._loop_tokens = [
            self._task_runner.run(
                self._formation_loop, alias=f"cluster-formation:{self._node_id}"
            ).token,
            self._task_runner.run(self._raft_loop, alias=f"cluster-raft:{self._node_id}").token,
        ]

    async def _resume_recovered_groups(self) -> None:
        """Resume the cluster group this node's disk held as its current
        member (D1); release every other one it recovered."""
        recovered_groups = self._storage.take_recovered_groups(lambda group_id: group_id.startswith("cluster:"))
        for group_id, recovered in sorted(recovered_groups.items()):
            await self._resume_or_release(group_id, recovered)

    async def _resume_or_release(self, group_id: str, recovered: RecoveredRaftGroup) -> None:
        """Resume a recovered group this node took part in as its current
        member, while it holds none; else release it for good (D1)."""
        if self._group is None and recovered.member_id == self.member_id:
            self._adopt(group_id.removeprefix("cluster:"), recovered.initial_voters, joined=True)
            await self._group.recover(recovered)
            await self._log_event("member_resumed", self.member_id)
            return
        await self._storage.write([GroupReleasedRecord(group_id=group_id)])

    async def stop(self) -> None:
        self._running = False
        self._accepted_founders_digests = ()
        self._admission_refusal = _NOT_RUNNING_REFUSAL
        # Watches waiting on this member answer now.
        self._applied_progress.set()
        loop_tokens, self._loop_tokens = self._loop_tokens, []
        for loop_token in loop_tokens:
            await self._task_runner.cancel(loop_token)
        if self._group is not None:
            self._group.destroy()
            self._group = None
        await self._outbox.close()

    async def _formation_loop(self) -> None:
        """A formation round every formation interval -- until formed, and
        again whenever the formed group has not been operable for one. The
        group's leader also releases the addresses of holders it has not
        heard from for the tombstone retention."""
        while self._running:
            await self._formation_tick()
            await self._clock.sleep(self._formation_interval_seconds)

    async def _formation_tick(self) -> None:
        """One pass of the formation loop: a formation round unless the
        formed group is operable, else that group's upkeep."""
        group = self._group
        if not self._group_operable(group):
            await self._advance_formation()
            return
        await self._maintain_formed_group(group)

    def _group_operable(self, group: RaftNode | None) -> bool:
        """Whether this node's group formed and reached a quorum within the
        last formation interval (``RaftNode.last_quorum_contact``)."""
        return self._formed and group is not None and self._quorum_contact_recent(group)

    def _quorum_contact_recent(self, group: RaftNode) -> bool:
        """Whether ``group`` reached a quorum within the formation interval."""
        return (
            quorum_contact := group.last_quorum_contact
        ) is not None and self._clock.monotonic() - quorum_contact < self._formation_interval_seconds

    async def _maintain_formed_group(self, group: RaftNode) -> None:
        """An operable group's upkeep: reclaim this member's released
        address, or -- leading an open cluster -- release silent holders'."""
        if self._address_unheld():
            await self._reclaim_address(group)
        elif self._releases_silent_holders(group):
            await self._release_silent_holders(group)

    def _address_unheld(self) -> bool:
        """Whether an open cluster holds this cohort member's address for
        no one, or for another process."""
        return (
            self._mode == CLUSTER_MODE_OPEN
            and self._address in self._cohort
            and self._address_holders.get(self._address) != self.member_id
        )

    def _releases_silent_holders(self, group: RaftNode) -> bool:
        """Whether this member leads an open cluster's group (AD-52 section 8)."""
        return group.is_leader() and self._mode == CLUSTER_MODE_OPEN

    async def _release_silent_holders(self, group: RaftNode) -> None:
        """Release, through the log, the address of every holder this leader
        has not heard from for the tombstone retention (AD-52 section 8)."""
        for silent_holder in sorted(
            group.silent_members(self._tombstone_retention_seconds)
            & set(self._address_holders.values())
        ):
            committed, _index = await group.propose(
                silent_holder.encode(), RELEASE_ADDRESS_COMMAND
            )
            await self._logger.log(
                RaftInfo(
                    message=(
                        f"Released {silent_holder}'s address: unheard for "
                        f"{self._tombstone_retention_seconds}s "
                        f"({_PROPOSAL_OUTCOMES[committed]})"
                    ),
                    node_id=self._node_id,
                    job_id=self._log_job_id,
                )
            )

    async def _log_event(self, event: str, subject: str) -> None:
        await self._logger.log(
            ClusterMembershipEvent(
                message=f"Cluster {self._cluster_uuid}: {event} {subject}",
                node_id=self._node_id,
                cluster_uuid=self._cluster_uuid or "",
                event=event,
                subject=subject,
            )
        )

    async def _reclaim_address(self, group: RaftNode) -> None:
        """This member is in the group, operable, yet its address is held by
        no one -- the leader released it while it went unheard, and it came
        back. It claims the address again, as the same process with its log
        intact: through the log if it leads, else through the leader, which
        greets the address first. Without this it stayed a voter nobody
        holds, and the leader's reconciliation, never removing below the
        quorum floor, could stall on it and every other departure."""
        if group.is_leader():
            committed, _index = await group.propose(self.member_id.encode(), CLAIM_ADDRESS_COMMAND)
            outcome = _PROPOSAL_OUTCOMES[committed]
        elif (leader := group.current_leader) is None:
            return
        else:
            outcome = await self._reclaim_through_leader(leader)
        await self._logger.log(
            RaftInfo(
                message=f"Reclaimed {self.member_id}'s address, released while it went unheard ({outcome})",
                node_id=self._node_id,
                job_id=self._log_job_id,
            )
        )

    async def _reclaim_through_leader(self, leader: str) -> str:
        """Ask the group's leader to commit this member's claim again; the
        outcome as the reclaim's log names it."""
        response = await self._send_request(
            ClusterMemberId.parse(leader).address,
            CLUSTER_JOIN_ACTION,
            ClusterJoinRequest(
            member_id=self.member_id,
            founders_digest=self._cohort_digest,
            schema_version=self._schema_versions[1],
        ).dump(),
        )
        if isinstance(response, Exception) or not response:
            return f"no answer from {leader}: {response!r}"
        return self._reclaim_reply_outcome(response)

    @staticmethod
    def _reclaim_reply_outcome(response: bytes) -> str:
        """The leader's answer to a reclaim, as the reclaim's log names it."""
        try:
            reply = decode_join_message(response, ClusterJoinReply, "cluster join reply")
            return "committed" if reply.accepted else f"refused: {reply.refusal}"
        except ClusterJoinError as decode_error:
            return f"answered with {decode_error}"

    async def _raft_loop(self) -> None:
        """One Raft tick every heartbeat interval: elections, replication,
        the leader's membership reconciliation, and applying committed
        entries."""
        while self._running:
            if (group := self._group) is not None:
                await self._raft_tick(group)
            await self._clock.sleep(HEARTBEAT_INTERVAL)

    async def _raft_tick(self, group: RaftNode) -> None:
        """One Raft tick of this node's group, then what it applied: wake
        watches, readdress a changed configuration, mark the cluster formed."""
        await group.tick()
        if group.is_leader():
            await self._lead_tick(group)
        await group.apply_committed_entries()
        self._signal_applied_progress(group)
        configuration = group.configuration
        await self._readdress_configuration(group, configuration)
        await self._mark_formed_when_due(group, configuration)

    async def _lead_tick(self, group: RaftNode) -> None:
        """The leader's part of a tick: announce a new term's leadership,
        replicate, and reconcile the configuration toward the holders."""
        if group.current_term != self._led_term:
            self._led_term = group.current_term
            await self._log_event("leader_elected", f"{self.member_id} term {group.current_term}")
        await group.replicate_to_followers()
        await self._reconcile_open_membership(group)

    async def _reconcile_open_membership(self, group: RaftNode) -> None:
        """Reconcile the configuration toward the address holders -- and
        this member, while the cohort holds its address."""
        # Frozen: the configuration stays as it is.
        if self._mode == CLUSTER_MODE_OPEN:
            await group.reconcile_membership(
                frozenset(self._address_holders.values())
                | ({self.member_id} if self._address in self._cohort else set())
            )

    def _signal_applied_progress(self, group: RaftNode) -> None:
        """Wake every watch waiting on the group to apply further (AD-52
        section 9), once per newly applied index."""
        if group.last_applied_index != self._signalled_applied_index:
            self._signalled_applied_index = group.last_applied_index
            self._applied_progress.set()
            self._applied_progress = asyncio.Event()

    async def _readdress_configuration(self, group: RaftNode, configuration: RaftConfiguration) -> None:
        """On a new configuration, log its member changes and readdress its
        members for the group and the rest of the node."""
        if configuration is self._addressed_configuration:
            return
        if (previous := self._addressed_configuration) is not None:
            await self._log_configuration_changes(previous, configuration)
        self._addressed_configuration = configuration
        self._address_members(group, configuration)

    async def _log_configuration_changes(
        self, previous: RaftConfiguration, configuration: RaftConfiguration
    ) -> None:
        """Log the members ``configuration`` added, promoted and removed."""
        await self._log_member_events("member_added", configuration.learners - previous.members)
        await self._log_member_events("member_promoted", configuration.voters & previous.learners)
        await self._log_member_events("member_removed", previous.members - configuration.members)

    async def _log_member_events(self, event: str, members: frozenset[str]) -> None:
        """Log ``event`` for each of ``members``, in order."""
        for member in sorted(members):
            await self._log_event(event, member)

    def _address_members(self, group: RaftNode, configuration: RaftConfiguration) -> None:
        """Give the group each member's address, and the rest of the node
        each member's node id and address (``node_addresses``)."""
        member_ids = {
            member: ClusterMemberId.parse(member) for member in configuration.members
        }
        group.update_member_addresses(
            {member: member_id.address for member, member_id in member_ids.items()}
        )
        self._node_addresses = self._node_addresses_of(member_ids)

    @staticmethod
    def _node_addresses_of(member_ids: dict[str, ClusterMemberId]) -> Mapping[str, tuple[str, int]]:
        """Each member's node id and address, read-only."""
        return MappingProxyType(
            {member_id.node_id: member_id.address for member_id in member_ids.values()}
        )

    async def _mark_formed_when_due(self, group: RaftNode, configuration: RaftConfiguration) -> None:
        """Mark the cluster formed once its group committed with this node a
        member of the configuration."""
        if not self._formation_completed(group, configuration):
            return
        self._formed = True
        self._formed_event.set()
        await self._log_event("cluster_formed", ",".join(sorted(configuration.voters)))
        await self._logger.log(
            RaftInfo(
                message=(
                    f"Cluster {self._cluster_uuid} formed: member {self.member_id} "
                    f"of {sorted(configuration.voters)}"
                ),
                node_id=self._node_id,
                job_id=self._cluster_uuid or "",
            )
        )

    def _formation_completed(self, group: RaftNode, configuration: RaftConfiguration) -> bool:
        """Whether the cluster, not yet formed here, has formed: its group
        committed and this node is a member of the configuration."""
        return (
            not self._formed
            and group.commit_index >= 1
            and self.member_id in configuration.members
        )

    # =========================================================================
    # Formation
    # =========================================================================

    async def _advance_formation(self) -> None:
        """One formation round (AD-52): greet the cohort's founders, then --
        holding a group -- judge it, or join a cluster formed without this
        node, or found one when a quorum of founders is discovering."""
        if self._address not in self._cohort:
            await self._logger.log(
                RaftWarning(
                    message=f"{self._address[0]}:{self._address[1]} is not in the cluster's cohort: not joining",
                    node_id=self._node_id,
                    job_id=self._log_job_id,
                )
            )
            return
        await self._judge_formation(await self._greet_founders())

    async def _judge_formation(self, replies: dict[tuple[str, int], ClusterHelloReply]) -> None:
        """Given the founders' answers: judge the group this node holds, or
        join a cluster formed without it, or found one when due (AD-52)."""
        group = self._group
        if group is not None and await self._settle_held_group(group, replies):
            return
        await self._join_or_found(group, replies)

    async def _settle_held_group(
        self, group: RaftNode, replies: dict[tuple[str, int], ClusterHelloReply]
    ) -> bool:
        """Abandon a held group whose voters are gone, or a formed or joined
        one inoperable too long; True when this round ends here."""
        if await self._abandon_if_voters_gone(group, replies):
            return True
        if not self._holds_adopted_group():
            return False
        await self._abandon_if_inoperable(group)
        return True

    def _holds_adopted_group(self) -> bool:
        """Whether the held group formed here, or was joined."""
        return self._formed or self._joined

    async def _join_or_found(
        self, group: RaftNode | None, replies: dict[tuple[str, int], ClusterHelloReply]
    ) -> None:
        """Join a cluster formed without this node; else judge this node's
        own founding, or -- holding none -- found one when due."""
        accepted = self._accepted_replies(replies)
        if await self._join_cluster_formed_without_this_node(group, accepted):
            return

        if group is not None:
            await self._abandon_unconfirmed_founding(group, accepted)
            return

        await self._found_cluster_when_due(accepted)

    @staticmethod
    def _accepted_replies(
        replies: dict[tuple[str, int], ClusterHelloReply],
    ) -> dict[tuple[str, int], ClusterHelloReply]:
        """The founders' answers that did not refuse the greeting."""
        return {address: reply for address, reply in replies.items() if reply.refusal is None}

    @property
    def _log_job_id(self) -> str:
        """The cluster this node holds, as its formation logs name it."""
        return self._cluster_uuid or ""

    async def _greet_founders(self) -> dict[tuple[str, int], ClusterHelloReply]:
        """Every other founder's answer to this node's greeting, by address
        -- refusals included (a refusing member is still present)."""
        hello = ClusterHello(member_id=self.member_id, founders_digest=self._cohort_digest).dump()
        founders = sorted(self._cohort - {self._address})
        replies: dict[tuple[str, int], ClusterHelloReply] = {}
        for founder_address, response in zip(
            founders, await self._send_to_all(founders, CLUSTER_HELLO_ACTION, hello)
        ):
            if (reply := await self._read_hello_reply(founder_address, response)) is not None:
                replies[founder_address] = reply
        return replies

    async def _send_to_all(
        self, addresses: list[tuple[str, int]], action: str, payload: bytes
    ) -> list[bytes | Exception | None]:
        """Send one request to every address at once; each answer in order."""
        return await asyncio.gather(*(self._send_request(address, action, payload) for address in addresses))

    async def _read_hello_reply(
        self, founder_address: tuple[str, int], response: bytes | Exception | None
    ) -> ClusterHelloReply | None:
        """A founder's answer to this node's greeting; None for none."""
        if isinstance(response, Exception) or not response:
            await self._logger.log(
                RaftDebug(
                    message=(
                        f"Founder {founder_address[0]}:{founder_address[1]} did not answer "
                        f"this node's greeting: {response!r}"
                    ),
                    node_id=self._node_id,
                    job_id=self._log_job_id,
                )
            )
            return None
        return await self._decode_hello_reply(founder_address, response)

    async def _decode_hello_reply(
        self, founder_address: tuple[str, int], response: bytes
    ) -> ClusterHelloReply | None:
        """Decode a founder's greeting reply, logging a refusal; None for an
        answer that is not one."""
        try:
            reply = decode_join_message(response, ClusterHelloReply, "cluster hello reply")
        except ClusterJoinError as decode_error:
            await self._logger.log(
                RaftWarning(
                    message=(
                        f"Founder {founder_address[0]}:{founder_address[1]} answered this "
                        f"node's greeting with {decode_error}"
                    ),
                    node_id=self._node_id,
                    job_id=self._log_job_id,
                )
            )
            return None
        if reply.refusal is not None:
            await self._logger.log(
                RaftWarning(
                    message=(
                        f"Founder {founder_address[0]}:{founder_address[1]} refused this "
                        f"node's greeting: {reply.refusal}"
                    ),
                    node_id=self._node_id,
                    job_id=self._log_job_id,
                )
            )
        return reply

    async def _abandon_if_voters_gone(
        self, group: RaftNode, replies: dict[tuple[str, int], ClusterHelloReply]
    ) -> bool:
        """Abandon a group too many of whose voters are gone to ever commit.
        Each answer is the process at its address now: a voter some other
        process answers for is gone and never votes again."""
        present = {self._address: self.member_id} | {
            address: reply.member_id for address, reply in replies.items()
        }
        configuration = group.configuration
        if configuration.has_quorum(self._present_voters(configuration, present), self._quorum):
            return False
        await self._abandon("too many of its voters are gone: it can never commit again")
        return True

    @staticmethod
    def _present_voters(
        configuration: RaftConfiguration, present: dict[tuple[str, int], str]
    ) -> frozenset[str]:
        """The voters the process at their address still answers for."""
        return frozenset(
            voter
            for voter in configuration.all_voters
            if present.get(ClusterMemberId.parse(voter).address, voter) == voter
        )

    async def _abandon_if_inoperable(self, group: RaftNode) -> None:
        """Abandon a formed or joined group that has not been able to commit
        for longer than the tombstone retention."""
        inoperable_since = group.last_quorum_contact or self._adopted_at
        if self._clock.monotonic() - inoperable_since >= self._tombstone_retention_seconds:
            await self._abandon("it has not been able to commit for longer than the tombstone retention")

    async def _join_cluster_formed_without_this_node(
        self, group: RaftNode | None, accepted: dict[tuple[str, int], ClusterHelloReply]
    ) -> bool:
        """A formed cluster -- not this node's own founding, catching up --
        is joined, never formed beside: the one most founders answer for,
        the lowest cluster id breaking ties."""
        formed_counts = self._formed_cluster_counts(accepted)
        if not formed_counts:
            return False
        joined_uuid = max(sorted(formed_counts), key=formed_counts.__getitem__)
        if group is not None:
            await self._abandon(f"cluster {joined_uuid} formed without it")
        await self._join(self._first_reply_naming(accepted, joined_uuid))
        return True

    def _formed_cluster_counts(self, accepted: dict[tuple[str, int], ClusterHelloReply]) -> Counter[str]:
        """How many founders answer for each formed cluster other than the
        one this node holds."""
        return Counter(
            reply.cluster_uuid for reply in accepted.values() if self._names_other_formed_cluster(reply)
        )

    def _names_other_formed_cluster(self, reply: ClusterHelloReply) -> bool:
        """Whether ``reply`` answers for a formed cluster other than the one
        this node holds."""
        return (
            reply.formation == FORMATION_FORMED
            and reply.cluster_uuid is not None
            and reply.cluster_uuid != self._cluster_uuid
        )

    @staticmethod
    def _first_reply_naming(
        accepted: dict[tuple[str, int], ClusterHelloReply], cluster_uuid: str
    ) -> ClusterHelloReply:
        """The lowest-addressed founder's answer for formed ``cluster_uuid``."""
        return next(
            reply
            for _address, reply in sorted(accepted.items())
            if ClusterMembership._names_formed_cluster(reply, cluster_uuid)
        )

    @staticmethod
    def _names_formed_cluster(reply: ClusterHelloReply, cluster_uuid: str) -> bool:
        """Whether ``reply`` answers for formed ``cluster_uuid``."""
        return reply.formation == FORMATION_FORMED and reply.cluster_uuid == cluster_uuid

    async def _abandon_unconfirmed_founding(
        self, group: RaftNode, accepted: dict[tuple[str, int], ClusterHelloReply]
    ) -> None:
        """A founding that has had a round to form and fewer of whose voters
        still confirm it than its quorum may never commit: abandoned."""
        if self._clock.monotonic() - self._adopted_at < self._formation_interval_seconds:
            return
        confirmed = {self.member_id} | self._confirming_founders(group, accepted)
        if not RaftConfiguration(voters=group.initial_voters).has_quorum(confirmed, self._quorum):
            await self._abandon(f"only {len(confirmed)} of its {len(group.initial_voters)} founders confirm it")

    def _confirming_founders(
        self, group: RaftNode, accepted: dict[tuple[str, int], ClusterHelloReply]
    ) -> set[str]:
        """The founders whose answers still name this node's founding."""
        return {reply.member_id for reply in accepted.values() if self._confirms_founding(group, reply)}

    def _confirms_founding(self, group: RaftNode, reply: ClusterHelloReply) -> bool:
        """Whether ``reply`` comes from a voter of this founding, naming it."""
        return reply.cluster_uuid == self._cluster_uuid and reply.member_id in group.initial_voters

    async def _found_cluster_when_due(self, accepted: dict[tuple[str, int], ClusterHelloReply]) -> None:
        """Found a cluster when this node is the founder to propose one."""
        discovering = self._discovering_founders(accepted)
        self._discovering_seen = 1 + len(discovering)
        if self._is_founding_proposer(accepted, discovering):
            await self._propose_founding(discovering)

    @staticmethod
    def _discovering_founders(
        accepted: dict[tuple[str, int], ClusterHelloReply],
    ) -> dict[tuple[str, int], ClusterHelloReply]:
        """The founders still discovering, by address."""
        return {
            address: reply for address, reply in accepted.items() if reply.formation == FORMATION_DISCOVERING
        }

    def _is_founding_proposer(
        self,
        accepted: dict[tuple[str, int], ClusterHelloReply],
        discovering: dict[tuple[str, int], ClusterHelloReply],
    ) -> bool:
        """A founding under way commits, or is abandoned, within a round: a
        second one beside it would only split the founders between them.
        The lowest-addressed discovering founder that may found proposes:
        one that saw a quorum discovering in its last round, or has not run
        one yet. A founder that cannot see a quorum never blocks one that
        can."""
        if self._discovering_seen < self._quorum or self._founding_under_way(accepted):
            return False
        return self._address == min({self._address} | self._founders_who_may_found(discovering))

    @staticmethod
    def _founding_under_way(accepted: dict[tuple[str, int], ClusterHelloReply]) -> bool:
        """Whether any founder answers that a founding is forming."""
        return any(reply.formation == FORMATION_FORMING for reply in accepted.values())

    def _founders_who_may_found(
        self, discovering: dict[tuple[str, int], ClusterHelloReply]
    ) -> set[tuple[str, int]]:
        """The discovering founders that may propose a founding."""
        return {address for address, reply in discovering.items() if self._may_found(reply)}

    def _may_found(self, reply: ClusterHelloReply) -> bool:
        """A founder that saw a quorum discovering in its last round, or has
        not run one yet."""
        return reply.discovering_seen == 0 or reply.discovering_seen >= self._quorum

    async def _propose_founding(self, discovering: dict[tuple[str, int], ClusterHelloReply]) -> None:
        """Adopt a new cluster with the discovering founders as its voters
        and ask each to adopt it; abandoned unless a quorum does."""
        founding_voters = sorted([self.member_id, *(reply.member_id for reply in discovering.values())])
        cluster_uuid = self._cluster_uuids.generate("cluster")
        self._adopt(cluster_uuid, founding_voters, joined=False)
        self._foundings_proposed += 1
        await self._logger.log(
            RaftInfo(
                message=f"Proposed founding of cluster {cluster_uuid} by {founding_voters}",
                node_id=self._node_id,
                job_id=cluster_uuid,
            )
        )
        founding = FoundCluster(
            cluster_uuid=cluster_uuid,
            founding_voters=founding_voters,
            founders_digest=self._cohort_digest,
        ).dump()
        adopted = 1 + await self._count_founding_adoptions(founding_voters, founding, cluster_uuid)
        await self._abandon_unadopted_founding(cluster_uuid, adopted, len(founding_voters))

    async def _count_founding_adoptions(self, founding_voters: list[str], founding: bytes, cluster_uuid: str) -> int:
        """How many of the other founders adopted the founding."""
        founding_peers = self._other_voters(founding_voters)
        responses = await self._send_to_all(
            [ClusterMemberId.parse(voter).address for voter in founding_peers], FOUND_CLUSTER_ACTION, founding
        )
        return sum(
            [
                await self._read_founding_reply(voter, response, cluster_uuid)
                for voter, response in zip(founding_peers, responses)
            ]
        )

    def _other_voters(self, voters: list[str]) -> list[str]:
        """``voters`` but this node."""
        return [voter for voter in voters if voter != self.member_id]

    async def _read_founding_reply(
        self, voter: str, response: bytes | Exception | None, cluster_uuid: str
    ) -> int:
        """1 when a founder's answer adopts the founding, else 0."""
        if isinstance(response, Exception) or not response:
            await self._logger.log(
                RaftDebug(
                    message=f"Founder {voter} did not answer the founding: {response!r}",
                    node_id=self._node_id,
                    job_id=cluster_uuid,
                )
            )
            return 0
        return await self._decode_founding_reply(voter, response, cluster_uuid)

    async def _decode_founding_reply(self, voter: str, response: bytes, cluster_uuid: str) -> int:
        """1 when a founder's decoded answer adopts the founding, else 0."""
        try:
            return int(decode_join_message(response, FoundClusterReply, "founding reply").adopted)
        except ClusterJoinError as decode_error:
            await self._logger.log(
                RaftWarning(
                    message=f"Founder {voter} answered the founding with {decode_error}",
                    node_id=self._node_id,
                    job_id=cluster_uuid,
                )
            )
            return 0

    async def _abandon_unadopted_founding(self, cluster_uuid: str, adopted: int, founders: int) -> None:
        """Abandon this node's founding, still held, unless a quorum of its
        founders adopted it."""
        if self._cluster_uuid == cluster_uuid and adopted < self._quorum:
            await self._abandon(f"only {adopted} of its {founders} founders adopted it")

    def _adopt(self, cluster_uuid: str, founding_voters: list[str], *, joined: bool) -> None:
        """Create this node's group for the cluster, with the voters every
        member creates it with.

        Raises:
            ValueError, TypeError, AttributeError: ``founding_voters`` is
                not a list of member ids -- raised before any state changes,
                so a damaged founding or join reply leaves the node as it was.
        """
        voter_addresses = {voter: ClusterMemberId.parse(voter).address for voter in founding_voters}
        group = RaftNode(
            job_id=f"cluster:{cluster_uuid}",
            node_id=self.member_id,
            initial_voters=frozenset(founding_voters),
            member_addrs=voter_addresses,
            send_message=self._enqueue,
            apply_command=self._apply,
            on_become_leader=None,
            on_lose_leadership=None,
            logger=self._logger,
            configured_cluster_size=len(self._cohort),
            # A claim that has not committed by the next round is retried.
            proposal_timeout_seconds=self._formation_interval_seconds,
            clock=self._hlc,
            may_lead=self._may_lead,
            snapshot_state=self._snapshot_holders,
            restore_snapshot=self._restore_holders,
            request_timeout_seconds=self._request_timeout_seconds,
            schema_versions=self._schema_versions,
            snapshot_entries=self._snapshot_entries,
            snapshot_catch_up_entries=self._snapshot_catch_up_entries,
            leader_lease_drift_bound=self._leader_lease_drift_bound,
            storage=self._storage,
        )
        self._cluster_uuid = cluster_uuid
        self._joined = joined
        self._formed = False
        self._formed_event.clear()
        self._adopted_at = self._clock.monotonic()
        self._group = group
        self._addressed_configuration = None
        self._node_addresses = NO_NODE_ADDRESSES
        self._address_holders = {address: voter for voter, address in voter_addresses.items()}
        self._mode = CLUSTER_MODE_OPEN

    async def _abandon(self, reason: str) -> None:
        """Leave this node's group for good: it comes back, if ever, under
        a new participation."""
        # The next participation is durable before the group goes: a
        # restart between them finds a group of a member it no longer is
        # and releases it -- never resumes it as a voter that lost it.
        left_as = self.member_id
        self._participation = await self._storage.advance_participation()
        if self._group is not None:
            await self._group.release()
        await self._logger.log(
            RaftWarning(
                message=f"Left cluster {self._cluster_uuid} as {left_as}: {reason}",
                node_id=self._node_id,
                job_id=self._cluster_uuid or "",
            )
        )
        self._group = None
        self._cluster_uuid = None
        self._joined = False
        self._formed = False
        self._formed_event.clear()
        self._addressed_configuration = None
        self._node_addresses = NO_NODE_ADDRESSES
        self._address_holders = {}
        self._mode = CLUSTER_MODE_OPEN

    async def _join(self, formed_reply: ClusterHelloReply) -> None:
        """Ask the formed cluster's membership group to take this node in."""
        target = ClusterMemberId.parse(
            formed_reply.leader_member_id or formed_reply.member_id
        ).address
        response = await self._send_request(
            target,
            CLUSTER_JOIN_ACTION,
            ClusterJoinRequest(
                member_id=self.member_id,
                founders_digest=self._cohort_digest,
                schema_version=self._schema_versions[1],
            ).dump(),
        )
        if (reply := await self._read_join_reply(formed_reply, target, response)) is not None:
            self._adopt_joined_cluster(reply)

    @staticmethod
    def _job_id_of(cluster_uuid: str | None) -> str:
        """A cluster uuid as the logs' job id: empty for none."""
        return cluster_uuid or ""

    async def _read_join_reply(
        self, formed_reply: ClusterHelloReply, target: tuple[str, int], response: bytes | Exception | None
    ) -> ClusterJoinReply | None:
        """The join's accepting reply; None, logged, for no answer, an
        undecodable one, or a refusal."""
        if isinstance(response, Exception) or not response:
            await self._logger.log(
                RaftDebug(
                    message=(
                        f"Join of cluster {formed_reply.cluster_uuid} got no answer from "
                        f"{target[0]}:{target[1]}: {response!r}"
                    ),
                    node_id=self._node_id,
                    job_id=self._job_id_of(formed_reply.cluster_uuid),
                )
            )
            return None
        return await self._decode_join_reply(formed_reply, target, response)

    async def _decode_join_reply(
        self, formed_reply: ClusterHelloReply, target: tuple[str, int], response: bytes
    ) -> ClusterJoinReply | None:
        """Decode the join's reply; None, logged, for one that is not."""
        try:
            reply = decode_join_message(response, ClusterJoinReply, "cluster join reply")
        except ClusterJoinError as decode_error:
            await self._logger.log(
                RaftWarning(
                    message=f"Join of cluster {formed_reply.cluster_uuid} answered with {decode_error}",
                    node_id=self._node_id,
                    job_id=self._job_id_of(formed_reply.cluster_uuid),
                )
            )
            return None
        return await self._accepted_join_reply(formed_reply, target, reply)

    async def _accepted_join_reply(
        self, formed_reply: ClusterHelloReply, target: tuple[str, int], reply: ClusterJoinReply
    ) -> ClusterJoinReply | None:
        """``reply`` when it accepted the join with a cluster; else None,
        logged."""
        if reply.accepted and reply.cluster_uuid is not None:
            return reply
        await self._logger.log(
            RaftDebug(
                message=(
                    f"Join of cluster {formed_reply.cluster_uuid} not accepted by "
                    f"{target[0]}:{target[1]}: {reply.refusal}"
                ),
                node_id=self._node_id,
                job_id=self._job_id_of(formed_reply.cluster_uuid),
            )
        )
        return None

    def _adopt_joined_cluster(self, reply: ClusterJoinReply) -> None:
        """Adopt the cluster an accepted join answered with, unless this
        node already holds a group."""
        if self._group is not None:
            # A founding reached this node while it waited: the next round
            # sees the formed cluster again and leaves the founding for it.
            return
        self._adopt(reply.cluster_uuid, reply.founding_voters, joined=True)

    # =========================================================================
    # Handlers
    # =========================================================================

    async def handle_hello(self, data: bytes) -> bytes:
        hello = ClusterHello.load(data)
        if hello.founders_digest not in self._accepted_founders_digests:
            return ClusterHelloReply(
                member_id=self.member_id,
                formation=self.formation,
                refusal=self._admission_refusal,
                configured_digest=self._configured_digest,
            ).dump()
        group = self._group
        return ClusterHelloReply(
            member_id=self.member_id,
            formation=self.formation,
            cluster_uuid=self._cluster_uuid,
            founding_voters=sorted(group.initial_voters) if group is not None else [],
            leader_member_id=self.leader_member_id,
            discovering_seen=self._discovering_seen,
            configured_digest=self._configured_digest,
        ).dump()

    async def handle_found(self, data: bytes) -> bytes:
        founding = FoundCluster.load(data)
        adopted = self._adopts_founding(founding)
        if adopted:
            self._adopt(founding.cluster_uuid, founding.founding_voters, joined=False)
        return FoundClusterReply(member_id=self.member_id, adopted=adopted).dump()

    def _adopts_founding(self, founding: FoundCluster) -> bool:
        """Whether this node adopts ``founding``: of this cohort, holding no
        group, and named among its voters."""
        return (
            # This cohort's digest -- none before ``start`` resumed the
            # group this node's disk holds (adopting first would release
            # that group for a new cluster).
            founding.founders_digest in self._accepted_founders_digests[:1]
            and self._group is None
            and self.member_id in founding.founding_voters
        )

    def _formed_group(self) -> RaftNode | None:
        """This node's group once its cluster has formed; None before."""
        return self._group if self._formed else None

    async def _forward_to_leader(
        self,
        leader: str,
        action: str,
        payload: bytes,
        unanswered: Callable[[bytes | Exception | None], bytes],
    ) -> bytes:
        """Pass an operator request on to the group's leader, once: its
        answer, or ``unanswered``'s reply when it gave none."""
        response = await self._send_request(ClusterMemberId.parse(leader).address, action, payload)
        if isinstance(response, Exception) or not response:
            return unanswered(response)
        return response

    @staticmethod
    def _format_addresses(addresses: list[tuple[str, int]]) -> list[str]:
        """Each address as ``host:port``, in order."""
        return [f"{cohort_host}:{cohort_port}" for cohort_host, cohort_port in addresses]

    async def handle_join(self, data: bytes) -> bytes:
        request = ClusterJoinRequest.load(data)
        if request.founders_digest not in self._accepted_founders_digests:
            return ClusterJoinReply(
                accepted=False, refusal=self._admission_refusal
            ).dump()
        if (group := self._formed_group()) is None:
            return ClusterJoinReply(accepted=False, refusal="this member's cluster is not formed").dump()
        return await self._admit_joiner(group, request)

    async def _admit_joiner(self, group: RaftNode, request: ClusterJoinRequest) -> bytes:
        """As the group's leader, admit a joiner the cluster may take in:
        greet its address, then commit its claim."""
        if not group.is_leader():
            return ClusterJoinReply(
                accepted=False,
                leader_member_id=group.current_leader,
                refusal="not the membership group's leader",
            ).dump()
        if refusal := self._join_admission_refusal(group, request):
            return ClusterJoinReply(accepted=False, refusal=refusal).dump()
        return await self._admit_cohort_member(group, request)

    def _join_admission_refusal(self, group: RaftNode, request: ClusterJoinRequest) -> str | None:
        """Why the cluster cannot take the joiner in, in order: its mode,
        the joiner's log schema; else None."""
        if self._mode != CLUSTER_MODE_OPEN:
            return f"the cluster's membership is {self._mode}"
        if request.schema_version < group.write_schema_version:
            return (
                f"the cluster writes log schema {group.write_schema_version}; "
                f"the joiner reads only up to {request.schema_version}"
            )
        return None

    async def _admit_cohort_member(self, group: RaftNode, request: ClusterJoinRequest) -> bytes:
        """Refuse a joiner whose address is outside the cohort; verify and
        claim one inside it."""
        if (claimant_address := ClusterMemberId.parse(request.member_id).address) not in self._cohort:
            return ClusterJoinReply(
                accepted=False,
                refusal=f"{claimant_address[0]}:{claimant_address[1]} is not in the cluster's cohort",
            ).dump()
        return await self._verify_and_claim(group, request, claimant_address)

    async def _verify_and_claim(
        self, group: RaftNode, request: ClusterJoinRequest, claimant_address: tuple[str, int]
    ) -> bytes:
        """Greet the joiner's address, then commit its claim."""
        # The claim must come from the process at the address now -- not a
        # delayed request of one gone, whose claim would unseat the live
        # holder.
        response = await self._send_request(
            claimant_address,
            CLUSTER_HELLO_ACTION,
            ClusterHello(member_id=self.member_id, founders_digest=self._cohort_digest).dump(),
        )
        if refusal := self._claimant_presence_refusal(request, response):
            return ClusterJoinReply(accepted=False, refusal=refusal).dump()
        return await self._commit_claim(group, request)

    def _claimant_presence_refusal(
        self, request: ClusterJoinRequest, response: bytes | Exception | None
    ) -> str | None:
        """Why the joiner's address did not vouch for the joiner; else None."""
        if isinstance(response, Exception) or not response:
            return f"the joiner's address did not answer: {response!r}"
        return self._decoded_presence_refusal(request, response)

    @staticmethod
    def _decoded_presence_refusal(request: ClusterJoinRequest, response: bytes) -> str | None:
        """Why the answer at the joiner's address is not the joiner's; else None."""
        try:
            present = decode_join_message(response, ClusterHelloReply, "cluster hello reply")
        except ClusterJoinError as decode_error:
            return f"the joiner's address answered with {decode_error}"
        if present.member_id != request.member_id:
            return f"{present.member_id} answers at the joiner's address, not {request.member_id}"
        return None

    async def _commit_claim(self, group: RaftNode, request: ClusterJoinRequest) -> bytes:
        """Commit the joiner's claim of its address, then accept it."""
        # The claim commits before the joiner hears it was accepted: every
        # member, and any later leader, then knows who holds the address.
        committed, _index = await group.propose(request.member_id.encode(), CLAIM_ADDRESS_COMMAND)
        if not committed:
            return ClusterJoinReply(
                accepted=False,
                leader_member_id=group.current_leader,
                refusal="the claim did not commit",
            ).dump()
        return ClusterJoinReply(
            accepted=True,
            cluster_uuid=self._cluster_uuid,
            founding_voters=sorted(group.initial_voters),
        ).dump()

    async def leave(self) -> ClusterLeaveReply:
        """Drain (AD-52 section 13): have the group release this member's
        address now, so the cluster stops counting a process that is
        shutting down instead of waiting out the tombstone retention.
        The group's leader -- this member, or the one it forwards to --
        commits the release; its reconciliation then removes the member.
        """
        return ClusterLeaveReply.load(
            await self.handle_leave(
                ClusterLeaveRequest(
                    host=self._address[0], port=self._address[1], member_id=self.member_id
                ).dump()
            )
        )

    async def handle_leave(self, data: bytes) -> bytes:
        request = ClusterLeaveRequest.load(data)
        self._operator_requests["leave:received"] = self._operator_requests.get("leave:received", 0) + 1
        if (group := self._formed_group()) is None:
            return ClusterLeaveReply(released=False, refusal="this member's cluster is not formed").dump()
        if not group.is_leader():
            return await self._forward_leave(group, request)
        return await self._release_on_request(group, request)

    async def _forward_leave(self, group: RaftNode, request: ClusterLeaveRequest) -> bytes:
        """Pass a drain on to the group's leader, once; a force-remove, or
        one with no leader known, is refused."""
        leader = group.current_leader
        if not self._leave_forwardable(request, leader):
            return ClusterLeaveReply(
                released=False,
                leader_member_id=leader,
                refusal="not the membership group's leader",
            ).dump()
        return await self._forward_to_leader(
            leader,
            CLUSTER_LEAVE_ACTION,
            ClusterLeaveRequest(
                host=request.host, port=request.port, member_id=request.member_id, forwarded=True
            ).dump(),
            lambda response: ClusterLeaveReply(
                released=False, refusal=f"the group's leader {leader} did not answer: {response!r}"
            ).dump(),
        )

    @staticmethod
    def _leave_forwardable(request: ClusterLeaveRequest, leader: str | None) -> bool:
        """Whether a leave goes on to ``leader``: a drain not yet forwarded."""
        return not (request.forwarded or request.member_id is None or leader is None)

    async def _release_on_request(self, group: RaftNode, request: ClusterLeaveRequest) -> bytes:
        """As the group's leader, release the address a leave names."""
        if self._mode != CLUSTER_MODE_OPEN:
            return ClusterLeaveReply(
                released=False, refusal=f"the cluster's membership is {self._mode}"
            ).dump()
        address = (request.host, request.port)
        if (holder := self._address_holders.get(address)) is None:
            return ClusterLeaveReply(
                released=False, refusal=f"no member holds {request.host}:{request.port}"
            ).dump()
        return await self._release_holder_on_request(group, request, address, holder)

    async def _release_holder_on_request(
        self, group: RaftNode, request: ClusterLeaveRequest, address: tuple[str, int], holder: str
    ) -> bytes:
        """Release ``holder``'s address unless the leave names another
        holder, or force-removes one still alive."""
        if refusal := self._holder_mismatch_refusal(request, holder) or await self._force_remove_refusal(
            request, address, holder
        ):
            return ClusterLeaveReply(released=False, refusal=refusal).dump()
        return await self._commit_release_on_request(group, request, holder)

    @staticmethod
    def _holder_mismatch_refusal(request: ClusterLeaveRequest, holder: str) -> str | None:
        """Why a drain may not release the address: another member holds it."""
        if request.member_id is not None and holder != request.member_id:
            return f"{request.host}:{request.port} is held by {holder}"
        return None

    async def _force_remove_refusal(
        self, request: ClusterLeaveRequest, address: tuple[str, int], holder: str
    ) -> str | None:
        """Why a force-remove may not release the address; None for a drain."""
        if request.member_id is not None:
            return None
        # A force-remove is for a member that is gone: a live one
        # would claim its address again at once.
        if holder == self.member_id:
            return "that is the group's leader, which is alive; stop it to drain it"
        return await self._live_holder_refusal(request, address, holder)

    async def _live_holder_refusal(
        self, request: ClusterLeaveRequest, address: tuple[str, int], holder: str
    ) -> str | None:
        """Greet the address: why its holder is not gone; None when it is."""
        response = await self._send_request(
            address,
            CLUSTER_HELLO_ACTION,
            ClusterHello(member_id=self.member_id, founders_digest=self._cohort_digest).dump(),
        )
        if isinstance(response, Exception) or not response:
            return None
        return self._present_holder_refusal(request, response, holder)

    @staticmethod
    def _present_holder_refusal(request: ClusterLeaveRequest, response: bytes, holder: str) -> str | None:
        """Why the answer at the address keeps its holder; else None."""
        try:
            present = decode_join_message(response, ClusterHelloReply, "cluster hello reply")
        except ClusterJoinError as decode_error:
            return f"{request.host}:{request.port} answered with {decode_error}"
        if present.member_id == holder:
            return f"{holder} still answers at {request.host}:{request.port}"
        return None

    async def _commit_release_on_request(
        self, group: RaftNode, request: ClusterLeaveRequest, holder: str
    ) -> bytes:
        """Commit the release of ``holder``'s address through the log."""
        committed, _index = await group.propose(holder.encode(), RELEASE_ADDRESS_COMMAND)
        await self._logger.log(
            RaftInfo(
                message=(
                    f"Released {holder}'s address on request "
                    f"({'drain' if request.member_id is not None else 'force-remove'}; "
                    f"{_PROPOSAL_OUTCOMES[committed]})"
                ),
                node_id=self._node_id,
                job_id=self._log_job_id,
            )
        )
        if not committed:
            return ClusterLeaveReply(released=False, refusal="the release did not commit; retry").dump()
        self._operator_requests["leave:committed"] = self._operator_requests.get("leave:committed", 0) + 1
        return ClusterLeaveReply(released=True, released_member_id=holder).dump()

    async def handle_mode(self, data: bytes) -> bytes:
        """Set the cluster's mode through the group's log (leader; a member
        passes it to the leader once)."""
        request = ClusterModeRequest.load(data)
        self._operator_requests["mode:received"] = self._operator_requests.get("mode:received", 0) + 1
        if request.mode not in CLUSTER_MODES:
            return ClusterModeReply(
                applied=False, refusal=f"unknown mode {request.mode!r}; one of {CLUSTER_MODES}"
            ).dump()
        if (group := self._formed_group()) is None:
            return ClusterModeReply(applied=False, refusal="this member's cluster is not formed").dump()
        return await self._set_mode_through_group(group, request)

    async def _set_mode_through_group(self, group: RaftNode, request: ClusterModeRequest) -> bytes:
        """Commit the mode as the group's leader; else pass it on."""
        if not group.is_leader():
            return await self._forward_mode(group, request)
        return await self._commit_mode(group, request)

    async def _forward_mode(self, group: RaftNode, request: ClusterModeRequest) -> bytes:
        """Pass a mode change on to the group's leader, once."""
        leader = group.current_leader
        if request.forwarded or leader is None:
            return ClusterModeReply(
                applied=False,
                leader_member_id=leader,
                refusal="not the membership group's leader",
            ).dump()
        return await self._forward_to_leader(
            leader,
            CLUSTER_MODE_ACTION,
            ClusterModeRequest(mode=request.mode, forwarded=True).dump(),
            lambda response: ClusterModeReply(
                applied=False,
                leader_member_id=leader,
                refusal=f"the group's leader {leader} did not answer: {response!r}",
            ).dump(),
        )

    async def _commit_mode(self, group: RaftNode, request: ClusterModeRequest) -> bytes:
        """Commit the mode through the group's log (AD-52 section 13)."""
        committed, _index = await group.propose(request.mode.encode(), CLUSTER_MODE_COMMAND)
        await self._logger.log(
            RaftInfo(
                message=f"Cluster mode {request.mode} ({_PROPOSAL_OUTCOMES[committed]})",
                node_id=self._node_id,
                job_id=self._log_job_id,
            )
        )
        if not committed:
            return ClusterModeReply(applied=False, refusal="the mode change did not commit; retry").dump()
        self._operator_requests["mode:committed"] = self._operator_requests.get("mode:committed", 0) + 1
        return ClusterModeReply(applied=True, mode=request.mode).dump()

    @property
    def cohort(self) -> frozenset[tuple[str, int]]:
        """The cohort this node knows: the one it was launched with until
        its cluster's log resized it."""
        return self._cohort

    async def handle_resize(self, data: bytes) -> bytes:
        """Add an address to the cluster's cohort, or remove one, through
        the group's log (AD-52 ``ResizeCluster``; leader -- a member passes
        it to the leader once)."""
        request = ClusterResizeRequest.load(data)
        self._operator_requests["resize:received"] = self._operator_requests.get("resize:received", 0) + 1
        if (group := self._formed_group()) is None:
            return ClusterResizeReply(applied=False, refusal="this member's cluster is not formed").dump()
        if not group.is_leader():
            return await self._forward_resize(group, request)
        return await self._resize_as_leader(group, request)

    async def _forward_resize(self, group: RaftNode, request: ClusterResizeRequest) -> bytes:
        """Pass a resize on to the group's leader, once."""
        leader = group.current_leader
        if request.forwarded or leader is None:
            return ClusterResizeReply(
                applied=False, leader_member_id=leader, refusal="not the membership group's leader"
            ).dump()
        return await self._forward_to_leader(
            leader,
            CLUSTER_RESIZE_ACTION,
            ClusterResizeRequest(
                host=request.host, port=request.port, add=request.add, forwarded=True
            ).dump(),
            lambda response: ClusterResizeReply(
                applied=False,
                leader_member_id=leader,
                refusal=f"the group's leader {leader} did not answer: {response!r}",
            ).dump(),
        )

    async def _resize_as_leader(self, group: RaftNode, request: ClusterResizeRequest) -> bytes:
        """As the group's leader, commit a resize the cohort and its voters
        allow (AD-52 ``ResizeCluster``)."""
        address = (request.host, request.port)
        if refusal := self._resize_refusal(request, address) or await self._resize_readiness_refusal(group):
            return ClusterResizeReply(applied=False, refusal=refusal).dump()
        return await self._commit_resize(group, request, address)

    def _resize_refusal(self, request: ClusterResizeRequest, address: tuple[str, int]) -> str | None:
        """Why the cohort cannot take this resize, in order: the cluster's
        mode, then the address against the cohort; else None."""
        if self._mode != CLUSTER_MODE_OPEN:
            return f"the cluster's membership is {self._mode}"
        return self._grow_refusal(request, address) if request.add else self._shrink_refusal(request, address)

    def _grow_refusal(self, request: ClusterResizeRequest, address: tuple[str, int]) -> str | None:
        """Why the cohort cannot grow by ``address``: it holds it already."""
        return f"{request.host}:{request.port} is already in the cohort" if address in self._cohort else None

    def _shrink_refusal(self, request: ClusterResizeRequest, address: tuple[str, int]) -> str | None:
        """Why the cohort cannot shrink by ``address``, in order: not in it,
        the leader's own, or a cohort of one; else None."""
        if address not in self._cohort:
            return f"{request.host}:{request.port} is not in the cohort"
        return (
            "the group's leader does not remove its own address: stop it (it drains), then ask again"
            if address == self._address
            else self._single_member_cohort_refusal()
        )

    def _single_member_cohort_refusal(self) -> str | None:
        """Why the cohort cannot shrink at all: it has one address."""
        return "a cohort of one cannot shrink" if len(self._cohort) == 1 else None

    async def _resize_readiness_refusal(self, group: RaftNode) -> str | None:
        """Why the cluster is not ready to resize; else None."""
        # One step at a time: every voter that answers -- a quorum of them
        # at least -- must run with the cohort the cluster holds, so no
        # process is configured more than one resize behind it.
        if self._configured_digest != self._cohort_digest:
            return "the group's leader runs with an older cohort: relaunch it with the current one first"
        return await self._voter_readiness_refusal(group)

    async def _voter_readiness_refusal(self, group: RaftNode) -> str | None:
        """Greet every other voter: why they are not ready -- one runs with
        an older cohort, or no quorum answered; else None."""
        voters = sorted(group.configuration.voters - {self.member_id})
        hello = ClusterHello(member_id=self.member_id, founders_digest=self._cohort_digest).dump()
        answered = {self.member_id}
        for voter, response in zip(voters, await self._greet_voters(voters, hello)):
            if refusal := self._voter_answer_refusal(voter, response, answered):
                return refusal
        return self._answered_quorum_refusal(group, answered)

    async def _greet_voters(self, voters: list[str], hello: bytes) -> list[bytes | Exception | None]:
        """Greet every voter at once; each answer in order."""
        return await self._send_to_all(
            [ClusterMemberId.parse(voter).address for voter in voters], CLUSTER_HELLO_ACTION, hello
        )

    def _voter_answer_refusal(
        self, voter: str, response: bytes | Exception | None, answered: set[str]
    ) -> str | None:
        """Why a voter's answer blocks the resize; else None, adding a voter
        that answered as itself to ``answered``."""
        if isinstance(response, Exception) or not response:
            return None
        return self._decoded_voter_refusal(voter, response, answered)

    def _decoded_voter_refusal(self, voter: str, response: bytes, answered: set[str]) -> str | None:
        """Decode a voter's answer: why it blocks the resize; else None."""
        try:
            reply = decode_join_message(response, ClusterHelloReply, "cluster hello reply")
        except ClusterJoinError as decode_error:
            return f"{voter} answered with {decode_error}"
        return self._voter_digest_refusal(voter, reply, answered)

    def _voter_digest_refusal(self, voter: str, reply: ClusterHelloReply, answered: set[str]) -> str | None:
        """Why the voter's answer blocks the resize -- it runs with an older
        cohort; else None, adding the voter when it answered as itself."""
        if reply.member_id != voter:
            return None
        if reply.configured_digest != self._cohort_digest:
            return f"{voter} runs with an older cohort: relaunch it with the current one first"
        answered.add(voter)
        return None

    def _answered_quorum_refusal(self, group: RaftNode, answered: set[str]) -> str | None:
        """Why too few voters answered for the resize; else None."""
        if group.configuration.has_quorum(answered, self._quorum):
            return None
        return f"only {len(answered)} of {len(group.configuration.voters)} voters answered"

    async def _commit_resize(
        self, group: RaftNode, request: ClusterResizeRequest, address: tuple[str, int]
    ) -> bytes:
        """Commit the resized cohort through the group's log."""
        resized = sorted(self._cohort | {address} if request.add else self._cohort - {address})
        committed, _index = await group.propose(json.dumps(resized).encode(), CLUSTER_RESIZE_COMMAND)
        await self._log_resize(request, committed)
        if not committed:
            return ClusterResizeReply(applied=False, refusal="the resize did not commit; retry").dump()
        self._operator_requests["resize:committed"] = self._operator_requests.get("resize:committed", 0) + 1
        return ClusterResizeReply(
            applied=True,
            cohort=self._format_addresses(resized),
        ).dump()

    async def _log_resize(self, request: ClusterResizeRequest, committed: bool) -> None:
        """Log a resize's proposal and whether it committed."""
        await self._logger.log(
            RaftInfo(
                message=(
                    f"Cohort {'grown by' if request.add else 'shrunk by'} {request.host}:{request.port} "
                    f"({_PROPOSAL_OUTCOMES[committed]})"
                ),
                node_id=self._node_id,
                job_id=self._cluster_uuid or "",
            )
        )

    async def handle_status(self, data: bytes) -> bytes:
        """The cluster's membership as of now (AD-52 section 11): the
        leader confirms it still leads (``RaftNode.read_index``), applies
        through the read index, and answers from its state; a member that
        is not the leader passes the request on once."""
        request = ClusterStatusRequest.load(data)
        if (group := self._formed_group()) is None:
            return ClusterStatusReply(served=False, refusal="this member's cluster is not formed").dump()
        if not group.is_leader():
            return await self._forward_status(group, request)
        return await self._serve_status(group)

    async def _forward_status(self, group: RaftNode, request: ClusterStatusRequest) -> bytes:
        """Pass a status request on to the group's leader, once."""
        leader = group.current_leader
        if request.forwarded or leader is None:
            return ClusterStatusReply(
                served=False, leader_member_id=leader, refusal="not the membership group's leader"
            ).dump()
        return await self._forward_to_leader(
            leader,
            CLUSTER_STATUS_ACTION,
            ClusterStatusRequest(forwarded=True).dump(),
            lambda response: ClusterStatusReply(
                served=False,
                leader_member_id=leader,
                refusal=f"the group's leader {leader} did not answer: {response!r}",
            ).dump(),
        )

    async def _serve_status(self, group: RaftNode) -> bytes:
        """As the confirmed leader, answer from state applied through the
        read index (AD-52 section 11)."""
        if (read_index := await group.read_index()) is None:
            return ClusterStatusReply(
                served=False, refusal="leadership could not be confirmed; retry"
            ).dump()
        if group.last_applied_index < read_index:
            await group.apply_committed_entries()
        configuration = group.configuration
        return ClusterStatusReply(
            served=True,
            cluster_uuid=self._cluster_uuid,
            leader_member_id=self.member_id,
            voters=sorted(configuration.voters),
            learners=sorted(configuration.learners),
            cohort=self._format_addresses(sorted(self._cohort)),
            holders=sorted(self._address_holders.values()),
            mode=self._mode,
            # The state read is as of everything applied here -- at least
            # the read index, and newer is still linearizable.
            read_index=group.last_applied_index,
        ).dump()

    async def handle_watch(self, data: bytes) -> bytes:
        """A membership watch's long poll (AD-52 section 9): the changes this
        member applied after the watcher's index -- waiting up to the
        watcher's wait for one if there are none yet -- or a snapshot to
        resume from when the watcher is of another cluster or behind this
        member's log compaction. Served by any member: a watch follows the
        log as applied, in commit order, with no gap."""
        request = ClusterWatchRequest.load(data)
        if not self._watch_waits(request):
            return self._answer_watch(request, "this member's cluster is not formed")
        progress = self._applied_progress
        self._open_watches += 1
        try:
            # The watcher's wait, cut to this member's ceiling: no
            # longer (an infinite wait parked the poll for good), and
            # NaN or a negative answers at once (``max`` gives 0.0).
            await self._clock.wait_for(
                progress.wait(),
                timeout=min(self._watch_wait_ceiling_seconds, max(0.0, request.wait_seconds)),
            )
        except asyncio.TimeoutError:
            pass  # Nothing changed within the watcher's wait: say so.
        finally:
            self._open_watches -= 1
        return self._answer_watch(request, "this member left its cluster")

    def _watch_waits(self, request: ClusterWatchRequest) -> bool:
        """Whether a watch of this formed cluster has nothing new yet: it
        waits for the group to apply further."""
        group = self._formed_group()
        return (
            group is not None
            and request.cluster_uuid == self._cluster_uuid
            and group.last_applied_index <= request.after_index
        )

    def _answer_watch(self, request: ClusterWatchRequest, refusal: str) -> bytes:
        """Answer a watch from this formed cluster's group; ``refusal`` when
        it has none."""
        if (group := self._formed_group()) is None:
            return ClusterWatchReply(served=False, refusal=refusal).dump()
        return self._watch_reply(group, request)

    def _watch_reply(self, group: RaftNode, request: ClusterWatchRequest) -> bytes:
        """The changes applied after the watcher's index; a snapshot when
        the watcher is of another cluster or behind log compaction."""
        configuration = group.configuration
        if (
            request.cluster_uuid != self._cluster_uuid
            or (entries := group.applied_entries_after(request.after_index)) is None
        ):
            return self._watch_snapshot_reply(group, configuration)
        return self._watch_events_reply(request, entries)

    def _watch_events_reply(self, request: ClusterWatchRequest, entries: list["RaftLogEntry"]) -> bytes:
        """The watch events among ``entries``, through the last of them."""
        events = self._watch_events(entries)
        return ClusterWatchReply(
            served=True,
            cluster_uuid=self._cluster_uuid,
            # A member behind the watcher has nothing new: the watcher keeps
            # its own index, never replaying what it has.
            applied_index=entries[-1].index if entries else request.after_index,
            events=events,
        ).dump()

    def _watch_snapshot_reply(self, group: RaftNode, configuration: RaftConfiguration) -> bytes:
        """The group's state to resume a watch from (AD-52 section 9)."""
        return ClusterWatchReply(
            served=True,
            cluster_uuid=self._cluster_uuid,
            applied_index=group.last_applied_index,
            snapshot=True,
            holders=sorted(self._address_holders.values()),
            mode=self._mode,
            cohort=self._format_addresses(sorted(self._cohort)),
            voters=sorted(configuration.voters),
            learners=sorted(configuration.learners),
        ).dump()

    def _watch_events(self, entries: list["RaftLogEntry"]) -> list[tuple[int, str, str]]:
        """The watch events of the membership changes among ``entries``."""
        return [event for entry in entries if (event := self._watch_event(entry)) is not None]

    def _watch_event(self, entry: "RaftLogEntry") -> tuple[int, str, str] | None:
        """``entry``'s watch event; None for an entry that changes nothing a
        watch follows."""
        if entry.command_type in _ADDRESS_EVENT_KINDS:
            return (entry.index, _ADDRESS_EVENT_KINDS[entry.command_type], entry.command.decode())
        if entry.command_type == CLUSTER_MODE_COMMAND:
            return (entry.index, "mode", entry.command.decode())
        return self._cohort_watch_event(entry)

    def _cohort_watch_event(self, entry: "RaftLogEntry") -> tuple[int, str, str] | None:
        """A resize's or configuration change's watch event; else None."""
        if entry.command_type == CLUSTER_RESIZE_COMMAND:
            return (entry.index, "resize", self._join_resized_cohort(entry.command))
        return self._configuration_watch_event(entry) if entry.command_type == RAFT_CONFIGURATION_COMMAND else None

    @staticmethod
    def _join_resized_cohort(command: bytes) -> str:
        """A resize command's cohort as space-separated ``host:port``."""
        return " ".join(
            f"{cohort_host}:{cohort_port}"
            for cohort_host, cohort_port in json.loads(command)
        )

    @staticmethod
    def _configuration_watch_event(entry: "RaftLogEntry") -> tuple[int, str, str]:
        """A configuration change's watch event."""
        entry_configuration = RaftConfiguration.load(entry.command)
        return (
            entry.index,
            "configuration",
            f"voters={','.join(sorted(entry_configuration.voters))} "
            f"learners={','.join(sorted(entry_configuration.learners))}"
            + (" joint" if entry_configuration.is_joint else ""),
        )

    async def handle_metrics(self, data: bytes) -> bytes:
        """This member's metrics of its cluster's membership (AD-52 section
        18): local, never forwarded -- each member reports what it sees."""
        group = self._group
        raft_metrics = group.metrics() if group is not None else {"follower_lag": {}}
        follower_lag = raft_metrics.pop("follower_lag")
        voters, learners = self._configuration_sizes(group)
        return ClusterMetricsReply(
            member_id=self.member_id,
            formation=self.formation,
            is_leader=self.is_leader(),
            cluster_uuid=self._cluster_uuid,
            mode=self._mode,
            cohort_size=len(self._cohort),
            voters=voters,
            learners=learners,
            holders=len(self._address_holders),
            raft=raft_metrics,
            follower_lag=follower_lag,
            changes_applied=dict(self._changes_applied),
            foundings_proposed=self._foundings_proposed,
            groups_left=self._participation,
            operator_requests=dict(self._operator_requests),
            open_watches=self._open_watches,
        ).dump()

    @staticmethod
    def _configuration_sizes(group: RaftNode | None) -> tuple[int, int]:
        """How many voters and learners ``group``'s configuration holds;
        none without a group."""
        if group is None:
            return 0, 0
        return len(group.configuration.voters), len(group.configuration.learners)

    async def handle_request_vote(self, data: bytes) -> bytes | None:
        request = RequestVote.load(data)
        if (group := self._group) is None or request.job_id != f"cluster:{self._cluster_uuid}":
            return None
        return (await group.handle_request_vote(request)).dump()

    async def handle_append_entries(self, data: bytes) -> bytes | None:
        request = AppendEntries.load(data)
        if (group := self._group) is None or request.job_id != f"cluster:{self._cluster_uuid}":
            return None
        return (await group.handle_append_entries(request)).dump()

    async def handle_install_snapshot(self, data: bytes) -> bytes | None:
        request = InstallSnapshot.load(data)
        if (group := self._group) is None or request.job_id != f"cluster:{self._cluster_uuid}":
            return None
        return (await group.handle_install_snapshot(request)).dump()

    # =========================================================================
    # Raft transport
    # =========================================================================

    async def _enqueue(
        self,
        address: tuple[str, int],
        request: RequestVote | AppendEntries | InstallSnapshot,
    ) -> None:
        """RaftNode sends while holding its lock: the outbox delivers."""
        self._outbox.enqueue(address, request)

    async def _exchange(
        self,
        address: tuple[str, int],
        request: RequestVote | AppendEntries | InstallSnapshot,
    ) -> None:
        action = _RAFT_REQUEST_ACTIONS[type(request)]
        response = await self._send_request(address, action, request.dump())
        if isinstance(response, Exception) or not response:
            # A lost message: Raft rebuilds the request on the next tick.
            await self._logger.log(
                RaftDebug(
                    message=f"Raft {action} to {address[0]}:{address[1]} got no response ({response!r})",
                    node_id=self._node_id,
                    job_id=request.job_id,
                    term=request.term,
                )
            )
            return
        await self._deliver_raft_response(request, response)

    async def _deliver_raft_response(
        self, request: RequestVote | AppendEntries | InstallSnapshot, response: bytes
    ) -> None:
        """Hand a Raft response to the group still holding the request's
        cluster; dropped once this node left it."""
        if (group := self._group) is None or request.job_id != f"cluster:{self._cluster_uuid}":
            return
        await self._dispatch_raft_response(group, request, response)

    @staticmethod
    async def _dispatch_raft_response(
        group: RaftNode, request: RequestVote | AppendEntries | InstallSnapshot, response: bytes
    ) -> None:
        """Hand a Raft response to ``group``'s handler for its request type."""
        if isinstance(request, RequestVote):
            await group.handle_request_vote_response(RequestVoteResponse.load(response))
            return
        await ClusterMembership._deliver_replication_response(group, request, response)

    @staticmethod
    async def _deliver_replication_response(
        group: RaftNode, request: AppendEntries | InstallSnapshot, response: bytes
    ) -> None:
        """Hand an AppendEntries or InstallSnapshot response to ``group``."""
        if isinstance(request, AppendEntries):
            await group.handle_append_entries_response(AppendEntriesResponse.load(response))
        elif isinstance(request, InstallSnapshot):
            await group.handle_install_snapshot_response(
                InstallSnapshotResponse.load(response)
            )

    async def _apply(self, entry: "RaftLogEntry") -> None:
        """Apply a committed claim -- its member now holds its address -- or
        release -- its member, if it still holds the address, no longer
        does."""
        if entry.command_type in (
            CLAIM_ADDRESS_COMMAND,
            RELEASE_ADDRESS_COMMAND,
            CLUSTER_MODE_COMMAND,
            CLUSTER_RESIZE_COMMAND,
        ):
            self._changes_applied[entry.command_type] = self._changes_applied.get(entry.command_type, 0) + 1
        await self._apply_command(entry)

    async def _apply_command(self, entry: "RaftLogEntry") -> None:
        """Apply a committed mode, resize, claim or release."""
        if entry.command_type == CLUSTER_MODE_COMMAND:
            self._mode = entry.command.decode()
            await self._log_event("mode_changed", self._mode)
            return
        if entry.command_type == CLUSTER_RESIZE_COMMAND:
            await self._apply_resize(entry)
            return
        self._apply_address_command(entry)

    async def _apply_resize(self, entry: "RaftLogEntry") -> None:
        """Adopt a committed resize's cohort (AD-52 ``ResizeCluster``)."""
        await self._log_event("cohort_resized", entry.command.decode())
        self._adopt_cohort(
            frozenset(
                (cohort_host, cohort_port) for cohort_host, cohort_port in json.loads(entry.command)
            ),
            self._cohort_digest,
        )

    def _apply_address_command(self, entry: "RaftLogEntry") -> None:
        """Apply a committed claim or release; any other command is not one."""
        if entry.command_type not in (CLAIM_ADDRESS_COMMAND, RELEASE_ADDRESS_COMMAND):
            return
        member = entry.command.decode()
        member_address = ClusterMemberId.parse(member).address
        if entry.command_type == CLAIM_ADDRESS_COMMAND:
            self._address_holders[member_address] = member
            return
        self._release_held_address(member_address, member)

    def _release_held_address(self, member_address: tuple[str, int], member: str) -> None:
        """Release ``member_address`` if ``member`` still holds it."""
        if self._address_holders.get(member_address) == member:
            del self._address_holders[member_address]

    def _adopt_cohort(self, cohort: frozenset[tuple[str, int]], previous_digest: str | None) -> None:
        """Take the cohort the cluster's log holds: quorums follow it, and
        an address it no longer holds is no one's."""
        if cohort == self._cohort:
            return
        self._cohort = cohort
        self._cohort_digest = self._cohort_digest_of(cohort)
        self._previous_cohort_digest = previous_digest
        # A cohort changes only as the group's log applies -- while running,
        # or in ``start``'s recovery, which then opens admission itself.
        self._accepted_founders_digests = (self._cohort_digest, *({previous_digest} - {None}))
        self._quorum = len(cohort) // 2 + 1
        self._address_holders = self._holders_within(cohort)
        self._announce_cohort_size(cohort)

    @staticmethod
    def _cohort_digest_of(cohort: frozenset[tuple[str, int]]) -> str:
        """The founders digest of ``cohort``: its sorted addresses, hashed."""
        return hashlib.sha256(
            ",".join(f"{cohort_host}:{cohort_port}" for cohort_host, cohort_port in sorted(cohort)).encode()
        ).hexdigest()

    def _holders_within(self, cohort: frozenset[tuple[str, int]]) -> dict[tuple[str, int], str]:
        """The address holders whose addresses ``cohort`` still holds."""
        return {
            address: holder for address, holder in self._address_holders.items() if address in cohort
        }

    def _announce_cohort_size(self, cohort: frozenset[tuple[str, int]]) -> None:
        """Hand a changed cohort to the group's quorum and the node's."""
        if self._group is not None:
            self._group.set_cohort_size(len(cohort))
        if self._on_cohort_change is not None:
            self._on_cohort_change(cohort)

    def _snapshot_holders(self) -> bytes:
        """The group's state as of its last applied entry: who holds each
        address, and the cluster's mode."""
        return json.dumps(
            {
                "holders": sorted(self._address_holders.values()),
                "mode": self._mode,
                "cohort": sorted(self._cohort),
                "previous_cohort_digest": self._previous_cohort_digest,
            }
        ).encode()

    async def _restore_holders(self, state: bytes) -> None:
        """Replace the group's state with a snapshot a leader installed."""
        restored = json.loads(state)
        self._address_holders = {
            ClusterMemberId.parse(holder).address: holder for holder in restored["holders"]
        }
        self._mode = restored["mode"]
        self._adopt_cohort(
            frozenset((cohort_host, cohort_port) for cohort_host, cohort_port in restored["cohort"]),
            restored["previous_cohort_digest"],
        )
