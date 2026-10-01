"""
Per-job Raft consensus coordinator for gate nodes.

Manages a dictionary of per-job RaftNode instances for gate.
Drives the Raft tick loop via TaskRunner for background execution.
Bounded by max concurrent Raft instances with backpressure.
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import cloudpickle

from .gate_state_machine import GateStateMachine
from .logging_models import RaftDebug, RaftInfo, RaftWarning
from .models import GateRaftCommandType
from .models.gate_commands import GateRaftCommand
from .raft_node import HEARTBEAT_INTERVAL, RaftNode

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

if TYPE_CHECKING:
    from hyperscale.distributed.jobs.gates.gate_job_manager import GateJobManager
    from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
    from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger
    from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock


class GateRaftConsensus:
    """
    Manages per-job Raft instances for a gate node.

    Provides bounded creation, background tick loop, message
    routing, and cleanup of Raft instances on job completion.
    """

    __slots__ = (
        "_node_id",
        "_job_manager",
        "_state_machine",
        "_logger",
        "_task_runner",
        "_send_message",
        "_clock",
        "_may_lead",
        "_members",
        "_member_addrs",
        "_nodes",
        "_max_instances",
        "_cluster_size",
        "_proposal_timeout_seconds",
        "_tick_running",
        "_on_become_leader",
        "_on_lose_leadership",
    )

    def __init__(
        self,
        node_id: str,
        job_manager: "GateJobManager",
        leadership_tracker: "JobLeadershipTracker",
        gate_state: "GateRuntimeState",
        logger: "Logger",
        task_runner: "TaskRunner",
        send_message: Callable[..., Awaitable[None]],
        max_instances: int = 10_000,
        on_become_leader: Callable[[str], None] | None = None,
        on_lose_leadership: Callable[[str], None] | None = None,
        *,
        clock: "HybridLogicalClock",
        may_lead: Callable[[], bool],
        ledger_replica: "JobLedgerReplica",
        cluster_size: Callable[[], int],
        proposal_timeout_seconds: float,
    ) -> None:
        self._node_id = node_id
        self._job_manager = job_manager
        self._state_machine = GateStateMachine(
            job_manager, leadership_tracker, gate_state, logger, node_id,
            ledger_replica=ledger_replica,
        )
        self._logger = logger
        self._task_runner = task_runner
        self._send_message = send_message
        self._clock = clock
        self._may_lead = may_lead

        self._members: set[str] = set()
        self._member_addrs: dict[str, tuple[str, int]] = {}
        self._nodes: dict[str, RaftNode] = {}
        self._max_instances = max_instances
        # The gate tier's static cohort at group creation. Fixing each
        # group's quorum from whichever peers happened to be known (the
        # RaftNode default) let an early group run on too small a
        # majority -- two disjoint majorities, two leaders.
        self._cluster_size = cluster_size
        self._proposal_timeout_seconds = proposal_timeout_seconds
        self._tick_running = False

        self._on_become_leader = on_become_leader
        self._on_lose_leadership = on_lose_leadership

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
            alias="gate_raft_consensus_tick",
        )

    def stop_tick_loop(self) -> None:
        """Signal the tick loop to stop."""
        self._tick_running = False

    async def _tick_loop(self) -> None:
        """
        Background tick loop. Drives election timeouts, heartbeats,
        replication, and entry application for all active Raft nodes.
        """
        import asyncio

        while self._tick_running:
            tick_start = _DEFAULT_CLOCK.monotonic()

            for job_id, node in list(self._nodes.items()):
                await node.tick()
                if node.is_leader():
                    await node.replicate_to_followers()
                applied = await node.apply_committed_entries()
                if applied > 0:
                    await self._logger.log(RaftDebug(
                        message=f"Gate applied {applied} entries for job {job_id}",
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

    async def create_job_raft(self, job_id: str) -> bool:
        """
        Create a Raft instance for a job.

        Returns False if at capacity (backpressure) or already exists.
        """
        if job_id in self._nodes:
            return True

        if len(self._nodes) >= self._max_instances:
            await self._logger.log(RaftWarning(
                message=f"Gate Raft instance limit reached ({self._max_instances}), rejecting {job_id}",
                node_id=self._node_id,
                job_id=job_id,
            ))
            return False

        node = RaftNode(
            job_id=job_id,
            node_id=self._node_id,
            members=self._members,
            member_addrs=self._member_addrs,
            send_message=self._send_message,
            apply_command=self._state_machine.apply,
            on_become_leader=self._make_leader_callback(job_id),
            on_lose_leadership=self._make_lose_leadership_callback(job_id),
            logger=self._logger,
            clock=self._clock,
            may_lead=self._may_lead,
            configured_cluster_size=self._cluster_size(),
            proposal_timeout_seconds=self._proposal_timeout_seconds,
        )
        self._nodes[job_id] = node

        await self._logger.log(RaftInfo(
            message=f"Created gate Raft instance for job {job_id}",
            node_id=self._node_id,
            job_id=job_id,
        ))
        return True

    async def destroy_job_raft(self, job_id: str) -> None:
        """Destroy the Raft instance for a job. Releases all memory."""
        node = self._nodes.pop(job_id, None)
        if node is None:
            return
        node.destroy()
        self._state_machine.release_job(job_id)

        await self._logger.log(RaftInfo(
            message=f"Destroyed gate Raft instance for job {job_id}",
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
        command: GateRaftCommand,
    ) -> tuple[bool, int]:
        """
        Propose a command through Raft for a specific job.

        Returns (success, log_index). Success is False if not leader,
        at capacity, or no Raft instance exists for the job.
        """
        node = self._nodes.get(job_id)
        if node is None:
            return False, 0

        serialized = cloudpickle.dumps(command)
        return await node.propose(serialized, command.command_type.value)

    # =========================================================================
    # Message Routing
    # =========================================================================

    async def route_request_vote(self, message) -> object | None:
        """Route a RequestVote to the correct job's RaftNode."""
        node = self._nodes.get(message.job_id)
        if node is None:
            return None
        return await node.handle_request_vote(message)

    async def route_request_vote_response(self, message) -> None:
        """Route a RequestVoteResponse to the correct job's RaftNode."""
        if node := self._nodes.get(message.job_id):
            await node.handle_request_vote_response(message)

    async def route_append_entries(self, message) -> object | None:
        """Route an AppendEntries to the correct job's RaftNode."""
        node = self._nodes.get(message.job_id)
        if node is None:
            return None
        return await node.handle_append_entries(message)

    async def route_append_entries_response(self, message) -> None:
        """Route an AppendEntriesResponse to the correct job's RaftNode."""
        if node := self._nodes.get(message.job_id):
            await node.handle_append_entries_response(message)

    # =========================================================================
    # Membership
    # =========================================================================

    def on_node_join(self, node_id: str, addr: tuple[str, int]) -> None:
        """Handle a new node joining the cluster."""
        self._members.add(node_id)
        self._member_addrs[node_id] = addr

        for node in self._nodes.values():
            node.update_membership(self._members, self._member_addrs)

    def member_address(self, node_id: str) -> tuple[str, int] | None:
        """TCP address of a current member, or None if unknown."""
        return self._member_addrs.get(node_id)

    def member_addresses(self) -> dict[str, tuple[str, int]]:
        """Each current member's TCP address, by node id."""
        return {
            member: address
            for member in self._members
            if (address := self._member_addrs.get(member)) is not None
        }

    def on_node_leave(self, node_id: str) -> None:
        """Handle a node leaving the cluster."""
        self._members.discard(node_id)
        self._member_addrs.pop(node_id, None)

        for node in self._nodes.values():
            node.update_membership(self._members, self._member_addrs)

    def set_initial_membership(
        self,
        members: set[str],
        addrs: dict[str, tuple[str, int]],
    ) -> None:
        """Set the initial cluster membership."""
        self._members = set(members)
        self._member_addrs = dict(addrs)

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def destroy_all(self) -> None:
        """Destroy all Raft instances. Called on node shutdown."""
        self._tick_running = False
        for job_id in list(self._nodes.keys()):
            await self.destroy_job_raft(job_id)

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
