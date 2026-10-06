"""
VOPR harness for one Raft group's membership (AD-52): real ``RaftNode``
instances on a ``SimulationLoop``, a seeded faulty network, crash-restarts
that come back under a NEW id at the same address (a restarted process's
``NodeId.full`` embeds its start time), and membership changed only as the
coordinators change it -- the leader's ``reconcile_membership`` toward the
live incarnations: a live incarnation joins as a learner, a caught-up
learner is promoted, a dead voter is removed.

Raft's safety properties are checked as the run goes, and recorded as
violations rather than raised so a run reports all of them:

* election safety -- at most one leader per term;
* leader completeness -- a new leader's log holds every entry any member
  applied before it was elected;
* state machine safety -- every member applies the same entry at each
  index;
* durability -- every command a proposer saw committed is in the final
  leader's log; and, read from the disks themselves as the run goes
  (members that keep Raft state only): a candidate's term and self-vote
  are durable before it asks for votes, a granted vote before it is
  answered, the entries a successful append claims before it is answered
  (sampled), and -- where no disk is ever lost -- a committed command on
  a quorum of voters' disks before its proposer hears so.

With ``resume_probability`` above zero each member keeps its Raft state
on a disk of its own (D1, a real ``RaftStore`` on ``SimFilesystem``): a
crash is a power loss, and the restarted process resumes its id and
state from that disk with that probability -- otherwise its disk is lost
and it comes back under a new id. At 1.0 no member's state is ever lost,
so crashes may take any number of members down at once, a whole-group
power loss included (``power_loss_probability``).

The network: every message is delivered after a seeded latency, dropped
with a seeded probability, and never crosses an active partition or
reaches a crashed incarnation. A message addressed to a member id reaches
whichever incarnation now runs at that id's address -- as production's
transport delivers it.
"""

import math
import random
from pathlib import Path

import msgspec

from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.raft.store.raft_storage import RaftStorage
from hyperscale.distributed.raft.store.models import RaftIdentity, RecoveredRaftGroup
from hyperscale.distributed.raft.store.raft_store import IDENTITY_FILE_NAME, STORE_FILE_NAME, RaftStore
from hyperscale.distributed.raft.store.raft_store_codec import RaftStoreCodec
from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RaftLogEntry,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL, RaftNode
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import Logger
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

from .raft_group_simulation_report import RaftGroupSimulationReport
from .seeded_random import SeededRandom
from .sim_filesystem import SimFilesystem
from .virtual_clock import VirtualClock

MemberAddress = tuple[str, int]

JOB_ID = "raft-membership-vopr"
COMMAND_TYPE = "VOPR_COMMAND"


class RaftGroupSimulation:
    """One Raft group under seeded faults and membership churn."""

    def __init__(
        self,
        seed: int,
        member_count: int,
        clock: VirtualClock,
        message_drop_probability: float,
        latency_bounds_seconds: tuple[float, float],
        mean_seconds_between_faults: float,
        crash_probability: float,
        crash_downtime_bounds_seconds: tuple[float, float],
        partition_duration_bounds_seconds: tuple[float, float],
        proposal_interval_seconds: float,
        membership_interval_seconds: float,
        proposal_timeout_seconds: float,
        resume_probability: float = 0.0,
        power_loss_probability: float = 0.0,
        disk_latency_seconds: float = 0.0,
        crash_after_reply_probability: float = 0.0,
        append_durability_check_probability: float = 0.0,
    ) -> None:
        self._random = random.Random(seed)
        self._clock = clock
        self._message_drop_probability = message_drop_probability
        self._latency_bounds_seconds = latency_bounds_seconds
        self._mean_seconds_between_faults = mean_seconds_between_faults
        # Of the faults injected, the share that crash an incarnation; the
        # rest partition the network.
        self._crash_probability = crash_probability
        self._quorum_floor = member_count // 2 + 1
        self._logger = Logger()
        self._crash_downtime_bounds_seconds = crash_downtime_bounds_seconds
        self._partition_duration_bounds_seconds = partition_duration_bounds_seconds
        self._proposal_interval_seconds = proposal_interval_seconds
        self._membership_interval_seconds = membership_interval_seconds
        self._proposal_timeout_seconds = proposal_timeout_seconds
        self._resume_probability = resume_probability
        # Of the crashes injected, the share that take every live member
        # down at once.
        self._power_loss_probability = power_loss_probability
        # How long each disk operation takes (an fsync's latency): the
        # window in which a write is not yet durable.
        self._disk_latency_seconds = disk_latency_seconds
        # A member that just answered a vote or an append loses power at
        # once with this probability (members that resume only): the
        # instant an answer must already be durable.
        self._crash_after_reply_probability = crash_after_reply_probability
        # The share of successful appends whose answer is checked against
        # the answering member's disk (each check reads the whole store).
        self._append_durability_check_probability = append_durability_check_probability
        self._codec = RaftStoreCodec()
        # Each address's disk: what survived its last power loss (None: no
        # disk, or one that was lost), the filesystem and store of the
        # incarnation running there.
        self._disks: dict[MemberAddress, dict | None] = {}
        self._filesystems: dict[MemberAddress, SimFilesystem] = {}
        self._stores: dict[MemberAddress, RaftStore] = {}
        # Addresses whose process is starting (opening its disk).
        self._starting: set[MemberAddress] = set()

        self._member_count = member_count
        self._addresses: list[MemberAddress] = [
            ("127.0.0.1", 10_000 + slot) for slot in range(member_count)
        ]
        self._initial_voters = frozenset(
            self._incarnation_id(slot, 0) for slot in range(member_count)
        )
        # Every id ever started, at its address: production's address book
        # keeps a dead incarnation's entry, so messages to it reach whatever
        # now runs there.
        self._member_addresses: dict[str, MemberAddress] = {
            self._incarnation_id(slot, 0): address
            for slot, address in enumerate(self._addresses)
        }
        self._incarnation_counts = [0] * member_count
        self._live_nodes: dict[MemberAddress, tuple[str, RaftNode]] = {}
        # The address-group each address sits in while a partition is up.
        self._partition_groups: dict[MemberAddress, int] = {}
        self._faults_enabled = True
        self._proposals_enabled = True
        self._proposal_sequence = 0

        self._task_runner = TaskRunner()
        self._tick_tokens: dict[MemberAddress, str] = {}
        self.report = RaftGroupSimulationReport()

    @staticmethod
    def _incarnation_id(slot: int, incarnation: int) -> str:
        return f"member-{slot}-incarnation-{incarnation}"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(
        self,
        fault_seconds: float,
        quiet_seconds: float,
        settle_seconds: float,
    ) -> RaftGroupSimulationReport:
        """Run with faults for ``fault_seconds``; heal every fault and run
        ``quiet_seconds`` more; stop proposing and let replication settle
        for ``settle_seconds``; report."""
        for slot, address in enumerate(self._addresses):
            await self._start_incarnation(slot, address, self._incarnation_id(slot, 0))

        self._task_runner.run(self._propose_loop)
        self._task_runner.run(self._membership_loop)
        self._task_runner.run(self._fault_loop)

        await self._clock.sleep(fault_seconds)
        self._faults_enabled = False
        self._partition_groups.clear()
        for slot, address in enumerate(self._addresses):
            if address not in self._live_nodes and address not in self._starting:
                self._incarnation_counts[slot] += 1
                await self._start_incarnation(
                    slot, address, self._incarnation_id(slot, self._incarnation_counts[slot])
                )

        await self._clock.sleep(quiet_seconds)
        self._proposals_enabled = False
        await self._clock.sleep(settle_seconds)
        self._record_final_state()
        await self._shutdown()
        return self.report

    async def _shutdown(self) -> None:
        for _member_id, node in self._live_nodes.values():
            node.destroy()
        self._live_nodes.clear()
        for store in self._stores.values():
            await store.close()
        self._stores.clear()
        await self._task_runner.shutdown()

    async def _start_incarnation(self, slot: int, address: MemberAddress, member_id: str) -> None:
        """Start the process at ``address``: under ``member_id``, or -- when
        its disk survived -- under the id and state that disk holds."""
        self._starting.add(address)
        try:
            await self._start_incarnation_at(slot, address, member_id)
        finally:
            self._starting.discard(address)

    async def _start_incarnation_at(self, slot: int, address: MemberAddress, member_id: str) -> None:
        storage: RaftStorage = VolatileRaftStorage()
        recovered_group = None
        if self._resume_probability > 0.0:
            filesystem = SimFilesystem(clock=self._clock)
            filesystem.set_slow_disk(self._disk_latency_seconds)
            if (disk := self._disks.pop(address, None)) is not None:
                filesystem.restore_durable(disk)
            # The disk is this process's from now: visible while it opens.
            self._filesystems[address] = filesystem
            store = RaftStore(
                directory=Path(f"/members/{address[1]}/raft"),
                filesystem=filesystem,
                random_source=SeededRandom(self._random.randrange(1 << 32)),
                clock=self._clock,
                logger=self._logger,
                task_runner=self._task_runner,
                set_aside_retained=1,
                storage_health=StorageHealth(),
            )
            recovery = await store.open(member_id, is_this_node=lambda node_id_full: True)
            if recovery.resumed:
                member_id = recovery.identity.node_id_full
                recovered_group = recovery.groups.get(JOB_ID)
                self.report.resumptions += 1
            self._stores[address] = store
            storage = store
        self._member_addresses[member_id] = address
        # The node sending, once made: what a dead incarnation still tries
        # to send never leaves it.
        sending_node: list[RaftNode] = []
        node = RaftNode(
            job_id=JOB_ID,
            node_id=member_id,
            # Every incarnation is created with the voters the group was
            # agreed with; a restarted one is not among them until the
            # leader adds and promotes it.
            initial_voters=self._initial_voters,
            member_addrs=dict(self._member_addresses),
            send_message=lambda destination, message, sender=address: self._send_from_node(
                sender, sending_node[0], destination, message
            ),
            apply_command=lambda entry, applier=member_id: self._record_applied(applier, entry),
            on_become_leader=lambda leader=member_id: self._record_leader(leader),
            on_lose_leadership=None,
            logger=self._logger,
            configured_cluster_size=self._member_count,
            proposal_timeout_seconds=self._proposal_timeout_seconds,
            clock=new_hybrid_logical_clock(node_id=slot + 1, clock=self._clock),
            may_lead=lambda: True,
            storage=storage,
        )
        sending_node.append(node)
        if recovered_group is not None:
            # Resumed before any message can reach it.
            await node.recover(recovered_group)
        self._live_nodes[address] = (member_id, node)
        for _live_id, live_node in self._live_nodes.values():
            live_node.update_member_addresses(dict(self._member_addresses))
        self._tick_tokens[address] = self._task_runner.run(
            self._tick_loop, address, node, alias=f"tick-{member_id}"
        ).token

    async def _crash(self, address: MemberAddress) -> None:
        member_id, node = self._live_nodes.pop(address)
        await self._task_runner.cancel(self._tick_tokens.pop(address))
        node.destroy()
        self.report.crashes += 1
        if (store := self._stores.pop(address, None)) is not None:
            # Power loss: what was fsynced survives -- if the disk does.
            filesystem = self._filesystems.pop(address)
            filesystem.crash()
            self._disks[address] = (
                filesystem.dump_durable() if self._random.random() < self._resume_probability else None
            )
            # The dead process's writer drains into the disk it left.
            await store.close()

    # ------------------------------------------------------------------
    # Loops
    # ------------------------------------------------------------------

    async def _tick_loop(self, address: MemberAddress, node: RaftNode) -> None:
        """The production coordinator's cadence: tick, replicate if
        leading, apply -- every heartbeat interval."""
        while self._live_nodes.get(address, (None, None))[1] is node:
            await node.tick()
            if node.is_leader():
                await node.replicate_to_followers()
            await node.apply_committed_entries()
            await self._clock.sleep(HEARTBEAT_INTERVAL)

    async def _propose_loop(self) -> None:
        while self._proposals_enabled:
            await self._clock.sleep(self._proposal_interval_seconds)
            if (leader := self._current_leader()) is not None:
                self._proposal_sequence += 1
                self._task_runner.run(
                    self._propose,
                    leader,
                    f"command-{self._proposal_sequence}".encode(),
                    alias="propose",
                )

    async def _propose(self, leader: RaftNode, command: bytes) -> None:
        committed, index = await leader.propose(command, COMMAND_TYPE)
        if committed:
            self.report.proposals_committed += 1
            self.report.acknowledged_commands.append(command)
            # Only where no disk is ever lost: a member that answered may
            # lose its disk (and its copy) once the commit stands.
            if self._resume_probability >= 1.0:
                holders = {
                    member_id
                    for address in self._addresses
                    if (held := self._durable_holding(address)) is not None
                    for member_id, group in (held,)
                    if group.base_index < index <= group.last_index
                    and group.entries[index - group.base_index - 1].command == command
                }
                if not leader.configuration.has_quorum(holders, self._quorum_floor):
                    self.report.violations.append(
                        f"durability: {command!r} (index {index}) was acknowledged as committed while "
                        f"on the disks of {sorted(holders)} only"
                    )

    async def _membership_loop(self) -> None:
        """The coordinators' membership step: the leader reconciles its
        configuration with the live incarnations (the cluster's live
        members, as failure detection reports them), one change at a
        time."""
        while self._proposals_enabled:
            await self._clock.sleep(self._membership_interval_seconds)
            if (leader := self._current_leader()) is None:
                continue
            live_ids = frozenset(member_id for member_id, _node in self._live_nodes.values())
            if await leader.reconcile_membership(live_ids):
                self.report.configuration_changes_started += 1

    async def _fault_loop(self) -> None:
        while self._faults_enabled:
            await self._clock.sleep(self._random.expovariate(1.0 / self._mean_seconds_between_faults))
            if not self._faults_enabled:
                return
            if self._random.random() < self._crash_probability:
                await self._inject_crash()
            else:
                self._inject_partition()

    async def _inject_crash(self) -> None:
        # Crash one live incarnation; it comes back after its downtime under
        # a new id at the same address. Never one that leaves fewer live
        # voters than a quorum: a group that loses a majority of its voters
        # cannot change its membership again (AD-52 non-goal: catastrophic
        # recovery), so the run would only show that.
        #
        # Members that always resume from their disks lose nothing: any of
        # them may crash, all at once included (a power loss).
        if self._resume_probability >= 1.0:
            crashable = sorted(self._live_nodes)
            if self._random.random() >= self._power_loss_probability:
                crashable = [self._random.choice(crashable)] if crashable else []
            else:
                self.report.power_losses += 1
        else:
            if (leader := self._current_leader()) is None or leader.configuration.is_joint:
                return
            live_voters = leader.configuration.voters & frozenset(
                member_id for member_id, _node in self._live_nodes.values()
            )
            candidates = [
                address
                for address, (member_id, _node) in sorted(self._live_nodes.items())
                if member_id not in live_voters or len(live_voters) > self._quorum_floor
            ]
            crashable = [self._random.choice(candidates)] if candidates else []
        # Log-uniform: restarts as quick as an election round (the window a
        # restarted member's new incarnation could vote in the term its old
        # one voted in) as often as long outages.
        shortest_downtime, longest_downtime = self._crash_downtime_bounds_seconds
        for address in crashable:
            await self._crash(address)
            downtime = math.exp(
                self._random.uniform(math.log(shortest_downtime), math.log(longest_downtime))
            )
            self._task_runner.run(
                self._restart_after,
                self._addresses.index(address),
                address,
                downtime,
                alias=f"restart-{address[1]}-{self.report.crashes}",
            )

    async def _restart_after(self, slot: int, address: MemberAddress, downtime: float) -> None:
        await self._clock.sleep(downtime)
        if address in self._live_nodes or address in self._starting:
            return
        self._incarnation_counts[slot] += 1
        await self._start_incarnation(slot, address, self._incarnation_id(slot, self._incarnation_counts[slot]))

    def _inject_partition(self) -> None:
        # Split the addresses in two for a while.
        self.report.partitions += 1
        shuffled = list(self._addresses)
        self._random.shuffle(shuffled)
        boundary = self._random.randint(1, len(shuffled) - 1)
        self._partition_groups = {
            address: 0 if position < boundary else 1 for position, address in enumerate(shuffled)
        }
        self._task_runner.run(
            self._heal_partition_after,
            self._random.uniform(*self._partition_duration_bounds_seconds),
            alias=f"heal-{boundary}-{self._proposal_sequence}",
        )

    async def _heal_partition_after(self, duration: float) -> None:
        await self._clock.sleep(duration)
        self._partition_groups.clear()

    # ------------------------------------------------------------------
    # Network
    # ------------------------------------------------------------------

    async def _send_from_node(
        self, sender: MemberAddress, sending_node: RaftNode, destination: MemberAddress, message: object
    ) -> None:
        """Send unless ``sending_node`` died: a crashed process's pending
        work can still finish here (its writes drain into the disk it
        left), but nothing it sends reaches anyone."""
        if self._live_nodes.get(sender, (None, None))[1] is not sending_node:
            return
        await self._send(sender, destination, message)

    async def _send(self, sender: MemberAddress, destination: MemberAddress, message: object) -> None:
        if (
            isinstance(message, RequestVote)
            and not message.pre_vote
            and (held := self._durable_holding(sender)) is not None
            and (held[1].term, held[1].voted_for) != (message.term, message.candidate_id)
        ):
            self.report.violations.append(
                f"durability: {message.candidate_id} asked for votes in term {message.term} with "
                f"term {held[1].term} (vote {held[1].voted_for}) on its disk"
            )
        if self._faults_enabled and self._random.random() < self._message_drop_probability:
            return
        self._task_runner.run(
            self._deliver,
            sender,
            destination,
            message,
            self._random.uniform(*self._latency_bounds_seconds),
            alias="deliver",
        )

    async def _deliver(
        self,
        sender: MemberAddress,
        destination: MemberAddress,
        message: object,
        latency: float,
    ) -> None:
        await self._clock.sleep(latency)
        if (
            self._partition_groups
            and self._partition_groups.get(sender) != self._partition_groups.get(destination)
        ):
            return
        if (receiver := self._live_nodes.get(destination)) is None:
            return
        _receiver_id, receiver_node = receiver

        if isinstance(message, RequestVote):
            response = await receiver_node.handle_request_vote(message)
            if (
                response.vote_granted
                and not response.pre_vote
                and self._live_nodes.get(destination, (None, None))[1] is receiver_node
                and (held := self._durable_holding(destination)) is not None
                and (held[1].term, held[1].voted_for) != (response.term, message.candidate_id)
            ):
                self.report.violations.append(
                    f"durability: {response.voter_id} granted {message.candidate_id} its vote in term "
                    f"{response.term} with term {held[1].term} (vote {held[1].voted_for}) on its disk"
                )
            await self._send_from_node(destination, receiver_node, sender, response)
            await self._maybe_crash_after_reply(destination, receiver_node)
        elif isinstance(message, RequestVoteResponse):
            await receiver_node.handle_request_vote_response(message)
        elif isinstance(message, AppendEntries):
            response = await receiver_node.handle_append_entries(message)
            if (
                response.success
                and self._resume_probability > 0.0
                and self._live_nodes.get(destination, (None, None))[1] is receiver_node
                and self._random.random() < self._append_durability_check_probability
                and (held := self._durable_holding(destination)) is not None
                and held[1].last_index < response.match_index
            ):
                self.report.violations.append(
                    f"durability: {response.follower_id} answered holding through {response.match_index} "
                    f"with only {held[1].last_index} on its disk"
                )
            await self._send_from_node(destination, receiver_node, sender, response)
            await self._maybe_crash_after_reply(destination, receiver_node)
        elif isinstance(message, AppendEntriesResponse):
            await receiver_node.handle_append_entries_response(message)

    async def _maybe_crash_after_reply(self, address: MemberAddress, node: RaftNode) -> None:
        """The member at ``address`` answered: with the configured
        probability it loses power now, and comes back within an election
        round -- able to vote again in the term it just voted in, were its
        vote not already on its disk."""
        if (
            not self._faults_enabled
            or self._resume_probability < 1.0
            or self._live_nodes.get(address, (None, None))[1] is not node
            or self._random.random() >= self._crash_after_reply_probability
        ):
            return
        await self._crash(address)
        self.report.crashes_after_reply += 1
        self._task_runner.run(
            self._restart_after,
            self._addresses.index(address),
            address,
            self._random.uniform(self._crash_downtime_bounds_seconds[0], ELECTION_TIMEOUT_MAX),
            alias=f"restart-{address[1]}-{self.report.crashes}",
        )

    def _durable_holding(self, address: MemberAddress) -> tuple[str, RecoveredRaftGroup] | None:
        """The member id whose disk is at ``address`` and what that disk
        holds of the group right now -- None when it keeps no Raft state.
        Read from the bytes a power loss would leave."""
        if self._resume_probability <= 0.0:
            return None
        if (filesystem := self._filesystems.get(address)) is not None:
            files = filesystem.dump_durable()["files"]
        elif (disk := self._disks.get(address)) is not None:
            files = disk["files"]
        else:
            return None
        directory = Path(f"/members/{address[1]}/raft")
        identity_bytes = files.get(str(directory / IDENTITY_FILE_NAME))
        store_bytes = files.get(str(directory / STORE_FILE_NAME))
        if identity_bytes is None or store_bytes is None:
            return None
        identity = msgspec.msgpack.decode(identity_bytes, type=RaftIdentity)
        groups, _whole_length = self._codec.replay(store_bytes, identity.stamp)
        return identity.node_id_full, groups.get(JOB_ID, RecoveredRaftGroup())

    # ------------------------------------------------------------------
    # Observation and invariants
    # ------------------------------------------------------------------

    def _current_leader(self) -> RaftNode | None:
        leaders = [node for _member_id, node in self._live_nodes.values() if node.is_leader()]
        if not leaders:
            return None
        return max(leaders, key=lambda node: node.current_term)

    def _record_leader(self, leader_id: str) -> None:
        node = next(
            (node for member_id, node in self._live_nodes.values() if member_id == leader_id),
            None,
        )
        if node is None:
            return
        term = node.current_term
        if (previous := self.report.leaders_by_term.setdefault(term, leader_id)) != leader_id:
            self.report.violations.append(
                f"election safety: term {term} has two leaders, {previous} and {leader_id}"
            )
        # Leader completeness: every entry applied anywhere is in this log.
        for index, (entry_term, command) in self.report.committed_entries.items():
            held_entry = node._log.get(index)
            if held_entry is None or held_entry.term != entry_term or held_entry.command != command:
                self.report.violations.append(
                    f"leader completeness: {leader_id} leads term {term} without the "
                    f"committed entry at index {index} (term {entry_term})"
                )

    async def _record_applied(self, applier_id: str, entry: RaftLogEntry) -> None:
        committed = self.report.committed_entries.setdefault(
            entry.index, (entry.term, entry.command)
        )
        if committed != (entry.term, entry.command):
            self.report.violations.append(
                f"state machine safety: {applier_id} applied (term {entry.term}) at index "
                f"{entry.index}, another member applied (term {committed[0]})"
            )

    def _record_final_state(self) -> None:
        self.report.live_member_ids = frozenset(
            member_id for member_id, _node in self._live_nodes.values()
        )
        self.report.final_leaders = [
            member_id for member_id, node in self._live_nodes.values() if node.is_leader()
        ]
        self.report.final_commit_indexes = {
            member_id: node.commit_index for member_id, node in self._live_nodes.values()
        }
        if (leader := self._current_leader()) is not None:
            self.report.final_voters = leader.configuration.voters
            self.report.final_learners = leader.configuration.learners
            # Durability: nothing a proposer saw committed was lost.
            held_commands = {
                entry.command for entry in leader._log.get_range(1, leader.last_log_index + 1)
            }
            self.report.violations.extend(
                f"durability: acknowledged {command!r} is not in the final leader's log"
                for command in self.report.acknowledged_commands
                if command not in held_commands
            )
