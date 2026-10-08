"""
A snapshot a leader installs replaces what the member held -- built step
by step on real ``RaftNode`` instances, every message delivered by hand,
on virtual time.

A group that can snapshot its state compacts its log through what it
applied; a member whose next entry its leader no longer holds is sent the
snapshot (Raft section 7). Two members it reaches must take more than the
state:

* BEHIND A CONFIGURATION CHANGE: D is cut off while E is removed from the
  group; the new leader compacted the change away. D takes the
  configuration in force at the snapshot point -- it would otherwise still
  count E, which no longer belongs to the group, in its quorums.
* HOLDING A DEPOSED LEADER'S ENTRIES: C led a term in which nothing it
  appended after its term start reached anyone; the indexes it wrote were
  committed under a later term. C's entries conflict with the snapshot's
  last entry, so every one of them goes -- an entry kept after a snapshot
  entry it never followed breaks the log matching property every leader
  relies on.
"""

import contextvars
import json
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar
from unittest.mock import AsyncMock, MagicMock

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RaftLogEntry,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.models.log_entry import RAFT_LOG_SCHEMA_VERSIONS
from hyperscale.distributed.raft.raft_node import ELECTION_TIMEOUT_MAX, RaftNode
from hyperscale.distributed.raft.snapshot import InstallSnapshot, InstallSnapshotResponse
from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from tests.simulation.harness.sim import SeededRandom, SimulationLoop, VirtualClock
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

MemberAddress = tuple[str, int]
OutboundMessage = tuple[MemberAddress, MemberAddress, object]
Admits = Callable[[MemberAddress, MemberAddress], bool]
ScenarioResult = TypeVar("ScenarioResult")

JOB_ID = "snapshot-install"
COMMAND_TYPE = "TEST"


class SnapshottingMember:
    """A real RaftNode over a list of applied commands it can snapshot;
    every send lands in the shared outbox."""

    def __init__(
        self,
        member_id: str,
        address: MemberAddress,
        voters: frozenset[str],
        addresses: dict[str, MemberAddress],
        outbox: list[OutboundMessage],
        clock: VirtualClock,
        schema_versions: tuple[int, int] = RAFT_LOG_SCHEMA_VERSIONS,
    ) -> None:
        self.applied: list[str] = []

        async def send(destination: MemberAddress, message: object) -> None:
            outbox.append((address, destination, message))

        async def apply(entry: RaftLogEntry) -> None:
            self.applied.append(entry.command.decode())

        async def restore(state: bytes) -> None:
            self.applied = json.loads(state)

        logger = MagicMock()
        logger.log = AsyncMock()
        self.node = RaftNode(
            job_id=JOB_ID,
            node_id=member_id,
            initial_voters=voters,
            member_addrs=dict(addresses),
            send_message=send,
            apply_command=apply,
            on_become_leader=None,
            on_lose_leadership=None,
            logger=logger,
            configured_cluster_size=len(voters),
            clock=new_hybrid_logical_clock(clock=clock),
            may_lead=lambda: True,
            snapshot_state=lambda: json.dumps(self.applied).encode(),
            restore_snapshot=restore,
            schema_versions=schema_versions,
            # Snapshot after every entry, keeping no tail: these scenarios
            # need the compaction point to pass members by.
            snapshot_entries=1,
            snapshot_catch_up_entries=0, storage=VolatileRaftStorage()
        )


class Group:
    """Members, the messages between them, and the links that carry them."""

    def __init__(
        self,
        member_ids: list[str],
        clock: VirtualClock,
        schema_versions: dict[str, tuple[int, int]] | None = None,
    ) -> None:
        self.clock = clock
        self.addresses = {
            member_id: ("127.0.0.1", 10_000 + slot) for slot, member_id in enumerate(member_ids)
        }
        self.outbox: list[OutboundMessage] = []
        self.members = {
            member_id: SnapshottingMember(
                member_id,
                self.addresses[member_id],
                frozenset(member_ids),
                self.addresses,
                self.outbox,
                clock,
                (schema_versions or {}).get(member_id, RAFT_LOG_SCHEMA_VERSIONS),
            )
            for member_id in member_ids
        }
        self.nodes_by_address = {
            self.addresses[member_id]: member.node for member_id, member in self.members.items()
        }

    def node(self, member_id: str) -> RaftNode:
        return self.members[member_id].node

    def among(self, *member_ids: str) -> Admits:
        linked = {self.addresses[member_id] for member_id in member_ids}
        return lambda sender, destination: sender in linked and destination in linked

    async def deliver(self, admits: Admits) -> None:
        """Deliver every queued message ``admits``, replies included, until
        none it admits is left; the rest are lost."""
        while admissible := [queued for queued in self.outbox if admits(queued[0], queued[1])]:
            self.outbox[:] = [queued for queued in self.outbox if queued not in admissible]
            for sender, destination, message in admissible:
                receiver = self.nodes_by_address[destination]
                match message:
                    case RequestVote():
                        reply = await receiver.handle_request_vote(message)
                        self.outbox.append((destination, sender, reply))
                    case RequestVoteResponse():
                        await receiver.handle_request_vote_response(message)
                    case AppendEntries():
                        reply = await receiver.handle_append_entries(message)
                        self.outbox.append((destination, sender, reply))
                    case AppendEntriesResponse():
                        await receiver.handle_append_entries_response(message)
                    case InstallSnapshot():
                        reply = await receiver.handle_install_snapshot(message)
                        self.outbox.append((destination, sender, reply))
                    case InstallSnapshotResponse():
                        await receiver.handle_install_snapshot_response(message)
        self.outbox.clear()

    async def replicate(self, leader_id: str, admits: Admits) -> None:
        await self.node(leader_id).replicate_to_followers()
        await self.deliver(admits)

    async def elect(self, candidate_id: str, admits: Admits) -> None:
        """Let every member's memory of the last leader lapse, then elect
        ``candidate_id`` and replicate its term start."""
        await self.clock.sleep(ELECTION_TIMEOUT_MAX)
        await self.node(candidate_id).start_election()
        await self.deliver(admits)
        assert self.node(candidate_id).is_leader()
        await self.replicate(candidate_id, admits)

    async def append_as_leader(
        self, leader_id: str, commands: list[str], schema_version: int | None = None
    ) -> None:
        """Append ``commands`` to the leader's log as ``propose`` does,
        without waiting on their commit -- in ``schema_version`` if given,
        else the version the leader writes."""
        leader = self.node(leader_id)
        async with leader._lock:
            for command in commands:
                leader._log.append(
                    RaftLogEntry(
                        term=leader.current_term,
                        index=leader._log.last_index() + 1,
                        command=command.encode(),
                        command_type=COMMAND_TYPE,
                        job_id=JOB_ID,
                        hlc=leader._clock.now(),
                        schema_version=(
                            schema_version if schema_version is not None else leader.write_schema_version
                        ),
                    )
                )


def _simulate(
    scenario: Callable[[VirtualClock], Coroutine[Any, Any, ScenarioResult]],
) -> ScenarioResult:
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock, random_source=SeededRandom(seed=0))

    def run_through_deadline() -> ScenarioResult:
        scenario_task = loop.create_task(scenario(clock))
        loop.run_window(60.0)
        assert scenario_task.done(), "scenario still running at virtual 60s"
        return scenario_task.result()

    try:
        return contextvars.copy_context().run(run_through_deadline)
    finally:
        loop.close()
        restore_defaults(snapshot)


def test_a_member_behind_a_configuration_change_takes_the_snapshots_configuration() -> None:
    async def scenario(clock: VirtualClock) -> None:
        group = Group(["A", "B", "C", "D", "E"], clock)
        await group.elect("A", group.among("A", "B", "C", "D", "E"))

        # D is cut off; A removes E with B and C -- the joint configuration,
        # then the final one.
        with_b_and_c = group.among("A", "B", "C")
        assert await group.node("A").change_membership(frozenset({"A", "B", "C", "D"}), frozenset())
        await group.deliver(with_b_and_c)
        await group.replicate("A", with_b_and_c)
        await group.replicate("A", with_b_and_c)
        assert group.node("A").configuration.voters == frozenset({"A", "B", "C", "D"})
        assert not group.node("A").configuration.is_joint

        # B applies all of it and compacts it away.
        await group.node("B").apply_committed_entries()
        assert group.node("B")._log.snapshot_index == group.node("B").last_log_index > 0

        # A dies; B leads with C and D, and D -- behind B's snapshot -- is
        # sent it.
        group.node("A").destroy()
        await group.elect("B", group.among("B", "C", "D"))
        await group.replicate("B", group.among("B", "C", "D"))
        await group.replicate("B", group.among("B", "C", "D"))

        behind_member = group.node("D")
        assert behind_member._log.snapshot_index > 0, "D was never sent the snapshot"
        assert behind_member.configuration.voters == frozenset({"A", "B", "C", "D"})
        assert "E" not in behind_member.configuration.members
        assert behind_member.last_log_index == group.node("B").last_log_index

    _simulate(scenario)


def test_a_deposed_leaders_entries_do_not_survive_a_snapshot_they_conflict_with() -> None:
    async def scenario(clock: VirtualClock) -> None:
        group = Group(["A", "B", "C"], clock)

        # C leads term 1 with everyone; then, cut off, it appends entries
        # no one else receives -- past where the others will compact.
        await group.elect("C", group.among("A", "B", "C"))
        await group.append_as_leader("C", [f"deposed-{number}" for number in range(1, 8)])
        deposed_term = group.node("C").current_term

        # A and B elect A in term 2 and commit commands at those indexes.
        await group.elect("A", group.among("A", "B"))
        await group.append_as_leader("A", ["committed-1", "committed-2", "committed-3", "committed-4"])
        await group.replicate("A", group.among("A", "B"))
        await group.replicate("A", group.among("A", "B"))
        await group.node("B").apply_committed_entries()
        snapshot_index = group.node("B")._log.snapshot_index
        assert group.node("C").last_log_index > snapshot_index

        # A dies; B leads term 3 with C, and sends C its snapshot.
        group.node("A").destroy()
        await group.elect("B", group.among("B", "C"))
        await group.replicate("B", group.among("B", "C"))

        deposed_member = group.members["C"]
        deposed_log = deposed_member.node._log
        assert deposed_log.snapshot_index == snapshot_index, "C was never sent the snapshot"
        # The entry C held at the snapshot point was not the snapshot's:
        # nothing C wrote after it survives.
        assert deposed_log.last_index() == snapshot_index, [
            (index, deposed_log.get(index).term)
            for index in range(snapshot_index + 1, deposed_log.last_index() + 1)
        ]
        assert deposed_member.applied == group.members["B"].applied
        assert not any(command.startswith("deposed") for command in deposed_member.applied)

        # Replication goes on from the snapshot.
        await group.replicate("B", group.among("B", "C"))
        assert deposed_log.last_index() == group.node("B").last_log_index
        assert all(
            deposed_log.get(index).term != deposed_term
            for index in range(snapshot_index + 1, deposed_log.last_index() + 1)
        )

    _simulate(scenario)
