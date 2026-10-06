"""
Per-job Raft consensus coordinator for manager nodes.

Manages a dictionary of per-job RaftNode instances. Drives
the Raft tick loop via TaskRunner for background execution.
Bounded by max concurrent Raft instances with backpressure.
"""

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from .logging_models import RaftDebug, RaftError, RaftInfo, RaftWarning
import msgspec

from .ledger_state_machine import LedgerStateMachine
from .models.ledger_append_command import LEDGER_APPEND_COMMAND, LedgerAppendCommand
from .raft_node import HEARTBEAT_INTERVAL, RaftNode
from .store.models import GroupReleasedRecord
from .store.raft_storage import RaftStorage

from hyperscale.distributed.runtime import Clock, RealClock
import asyncio


_DEFAULT_CLOCK: Clock = RealClock()

if TYPE_CHECKING:
    from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger
    from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock


class RaftConsensus:
    """
    Manages per-job Raft instances for a manager node.

    Provides bounded creation, background tick loop, message
    routing, and cleanup of Raft instances on job completion.
    """

    __slots__ = (
        "_node_id",
        "_state_machine",
        "_logger",
        "_task_runner",
        "_send_message",
        "_clock",
        "_may_lead",
        "_cluster_members",
        "_addressed_members",
        "_nodes",
        "_max_instances",
        "_configured_cluster_size",
        "_proposal_timeout_seconds",
        "_request_timeout_seconds",
        "_tick_running",
        "_on_become_leader",
        "_on_lose_leadership",
        "_storage",
    )

    def __init__(
        self,
        node_id: str,
        logger: "Logger",
        task_runner: "TaskRunner",
        send_message: Callable[..., Awaitable[None]],
        max_instances: int = 10_000,
        configured_cluster_size: int = 1,
        proposal_timeout_seconds: float = 5.0,
        on_become_leader: Callable[[str], None] | None = None,
        on_lose_leadership: Callable[[str], None] | None = None,
        *,
        clock: "HybridLogicalClock",
        may_lead: Callable[[], bool],
        ledger_replica: "JobLedgerReplica",
        request_timeout_seconds: float,
        cluster_members: Callable[[], Mapping[str, tuple[str, int]]],
        storage: RaftStorage,
    ) -> None:
        """
        ``request_timeout_seconds`` bounds one exchange on ``send_message``'s
        transport; each job group's CheckQuorum window covers it
        (``RaftNode``).
        """
        self._node_id = node_id
        self._state_machine = LedgerStateMachine(ledger_replica, logger, node_id)
        self._logger = logger
        self._task_runner = task_runner
        self._send_message = send_message
        self._clock = clock
        self._may_lead = may_lead

        # The cluster's members by node id, with their TCP addresses
        # (``ClusterMembership.node_addresses``), and the mapping last given
        # to the groups' address books.
        self._cluster_members = cluster_members
        self._addressed_members: Mapping[str, tuple[str, int]] | None = None
        self._nodes: dict[str, RaftNode] = {}
        self._max_instances = max_instances
        self._configured_cluster_size = max(1, configured_cluster_size)
        self._proposal_timeout_seconds = proposal_timeout_seconds
        self._request_timeout_seconds = request_timeout_seconds
        self._tick_running = False

        self._on_become_leader = on_become_leader
        self._on_lose_leadership = on_lose_leadership
        # Where every job group keeps its persistent state (D1).
        self._storage = storage

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def start_tick_loop(self) -> None:
        """Start the background tick loop via TaskRunner."""
        if self._tick_running:
            return
        self._tick_running = True
        self._task_runner.run(
            self._tick_loop,
            alias="raft_consensus_tick",
        )

    def stop_tick_loop(self) -> None:
        """Signal the tick loop to stop."""
        self._tick_running = False

    async def _tick_loop(self) -> None:
        """
        Background tick loop. Drives election timeouts, heartbeats,
        replication, and entry application for all active Raft nodes.
        """

        while self._tick_running:
            tick_start = _DEFAULT_CLOCK.monotonic()
            # The cluster's committed members (AD-52 slice C): each group
            # this node leads moves its configuration toward them through
            # its log -- none until the cluster has formed.
            members = self._cluster_members()
            if members is not self._addressed_members:
                self._addressed_members = members
                for node in self._nodes.values():
                    node.update_member_addresses(members)
            live_members = frozenset(members) | {self._node_id}

            for job_id, node in list(self._nodes.items()):
                await node.tick()
                if node.is_leader():
                    await node.replicate_to_followers()
                    if members:
                        await node.reconcile_membership(live_members)
                applied = await node.apply_committed_entries()
                if applied > 0:
                    await self._logger.log(RaftDebug(
                        message=f"Applied {applied} entries for job {job_id}",
                        node_id=self._node_id,
                        job_id=job_id,
                        term=node.current_term,
                        role=node.role,
                        commit_index=node.commit_index,
                    ))

            elapsed = _DEFAULT_CLOCK.monotonic() - tick_start
            sleep_time = max(0.0, HEARTBEAT_INTERVAL - elapsed)
            await _DEFAULT_CLOCK.sleep(sleep_time)

    # =========================================================================
    # Job Raft Management
    # =========================================================================

    async def create_job_raft(self, job_id: str, initial_voters: frozenset[str]) -> bool:
        """
        Create the job's Raft instance on this node, with the voters the
        job's group was created with -- the same on every member, decided
        once by whoever created the job (``current_members`` there) and
        handed to every member that joins the group. Members that differed
        in them could each count a quorum the others would not.

        Returns False if at capacity (backpressure); True when created or
        already present.

        Raises:
            ValueError: no voters -- a group without them never elects.
        """
        if not initial_voters:
            raise ValueError(f"the Raft group of job {job_id} needs at least one voter")
        if job_id in self._nodes:
            return True

        if len(self._nodes) >= self._max_instances:
            await self._logger.log(RaftWarning(
                message=f"Raft instance limit reached ({self._max_instances}), rejecting {job_id}",
                node_id=self._node_id,
                job_id=job_id,
            ))
            return False

        node = RaftNode(
            job_id=job_id,
            node_id=self._node_id,
            # The group's agreed voters; the members it gains or loses
            # later change through its log (``reconcile_membership``).
            initial_voters=initial_voters,
            member_addrs=dict(self._cluster_members()),
            send_message=self._send_message,
            apply_command=self._state_machine.apply,
            on_become_leader=self._make_leader_callback(job_id),
            on_lose_leadership=self._make_lose_leadership_callback(job_id),
            logger=self._logger,
            clock=self._clock,
            may_lead=self._may_lead,
            configured_cluster_size=self._configured_cluster_size,
            proposal_timeout_seconds=self._proposal_timeout_seconds,
            request_timeout_seconds=self._request_timeout_seconds,
            storage=self._storage,
        )
        self._nodes[job_id] = node

        await self._logger.log(RaftInfo(
            message=f"Created Raft instance for job {job_id}",
            node_id=self._node_id,
            job_id=job_id,
        ))
        return True

    def set_cohort_size(self, cohort_size: int) -> None:
        """The cluster's cohort was resized (AD-52 ``ResizeCluster``): job
        groups founded from now count its majority as their quorum floor,
        and every group held now does at once."""
        self._configured_cluster_size = max(1, cohort_size)
        for node in self._nodes.values():
            node.set_cohort_size(cohort_size)

    async def destroy_job_raft(self, job_id: str) -> None:
        """The job is done with its Raft instance: it is dropped from
        memory and from this node's disk (D1)."""
        node = self._nodes.pop(job_id, None)
        if node is None:
            return
        await node.release()
        self._state_machine.release_job(job_id)

        await self._logger.log(RaftInfo(
            message=f"Destroyed Raft instance for job {job_id}",
            node_id=self._node_id,
            job_id=job_id,
        ))

    def get_node(self, job_id: str) -> RaftNode | None:
        """Get the RaftNode for a job, or None if not found."""
        return self._nodes.get(job_id)

    @property
    def active_instance_count(self) -> int:
        """Number of active Raft instances."""
        return len(self._nodes)

    # =========================================================================
    # Command Proposal
    # =========================================================================

    async def propose_command(
        self,
        job_id: str,
        command: LedgerAppendCommand,
    ) -> tuple[bool, int]:
        """
        Propose a command through Raft for a specific job.

        Returns (success, log_index). Success is False if not leader,
        at capacity, or no Raft instance exists for the job.
        """
        node = self._nodes.get(job_id)
        if node is None:
            return False, 0

        return await node.propose(msgspec.msgpack.encode(command), LEDGER_APPEND_COMMAND)

    # =========================================================================
    # Message Routing
    # =========================================================================

    async def route_request_vote(self, message) -> object | None:
        """Route a RequestVote to the correct job's RaftNode.

        Groups are never created by an incoming RPC: members create them
        when they start tracking a live job and destroy them at job
        cleanup, so a lagging peer's RPC cannot resurrect a destroyed
        group (which would then elect and heartbeat forever). An RPC for
        an unknown group gets no response -- a lost message to Raft.
        """
        if (node := self._nodes.get(message.job_id)) is None:
            return None
        return await node.handle_request_vote(message)

    async def route_request_vote_response(self, message) -> None:
        """Route a RequestVoteResponse to the correct job's RaftNode."""
        if node := self._nodes.get(message.job_id):
            await node.handle_request_vote_response(message)

    async def route_append_entries(self, message) -> object | None:
        """Route an AppendEntries to the correct job's RaftNode (see
        ``route_request_vote`` for why unknown groups are not created)."""
        if (node := self._nodes.get(message.job_id)) is None:
            return None
        return await node.handle_append_entries(message)

    async def route_append_entries_response(self, message) -> None:
        """Route an AppendEntriesResponse to the correct job's RaftNode."""
        if node := self._nodes.get(message.job_id):
            await node.handle_append_entries_response(message)

    # =========================================================================
    # Membership
    # =========================================================================

    def current_members(self) -> frozenset[str]:
        """The cluster's committed members, this node among them: the
        voters a job's group is created with by the node creating the job.
        Only this node until the cluster has formed -- a job is not
        admitted before then."""
        return frozenset(self._cluster_members()) | {self._node_id}

    def member_address(self, node_id: str) -> tuple[str, int] | None:
        """TCP address of a current member, or None if unknown."""
        return self._cluster_members().get(node_id)

    def member_addresses(self) -> dict[str, tuple[str, int]]:
        """Each other current member's TCP address, by node id."""
        return {
            member: address
            for member, address in self._cluster_members().items()
            if member != self._node_id
        }

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def destroy_all(self) -> None:
        """Drop every Raft instance from memory on node shutdown -- not
        from disk: a restart resumes them (D1)."""
        self._tick_running = False
        for job_id, node in list(self._nodes.items()):
            node.destroy()
            self._state_machine.release_job(job_id)
        self._nodes.clear()

    async def recover_groups(self) -> None:
        """Resume every job group this node's disk held (D1) before the
        tick loop and before any Raft message: each as it was created,
        state and all. A group of a member this node no longer is, is
        released.

        Raises:
            RuntimeError: more groups than this node may hold at once.
        """
        recovered_groups = self._storage.take_recovered_groups(
            lambda group_id: not group_id.startswith("cluster:")
        )
        for job_id, recovered in sorted(recovered_groups.items()):
            if recovered.member_id != self._node_id:
                await self._storage.write([GroupReleasedRecord(group_id=job_id)])
                continue
            if not await self.create_job_raft(job_id, frozenset(recovered.initial_voters)):
                raise RuntimeError(f"cannot resume the Raft group of job {job_id}: at the instance limit")
            await self._nodes[job_id].recover(recovered)

    # =========================================================================
    # Helpers
    # =========================================================================

    def _make_leader_callback(self, job_id: str) -> Callable[[], None]:
        """Create an on_become_leader callback for a job."""
        def callback() -> None:
            if self._on_become_leader:
                self._on_become_leader(job_id)
        return callback

    def _make_lose_leadership_callback(self, job_id: str) -> Callable[[], None]:
        """Create an on_lose_leadership callback for a job."""
        def callback() -> None:
            if self._on_lose_leadership:
                self._on_lose_leadership(job_id)
        return callback
