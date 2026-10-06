"""
A Raft leader no quorum answers steps down (CheckQuorum, Raft thesis 6.2)
-- real ``RaftNode`` instances on virtual time.

Members refuse votes while they hear from a live leader (leader
stickiness, which keeps a member that cannot hear the leader from
deposing it). A leader whose heartbeats still reached its followers, but
whose followers' answers no longer reached it -- an asymmetric partition
-- kept every follower refusing votes while it could commit nothing
itself: the group had no leader able to commit for as long as the
partition lasted. Now a leader no quorum of voters answered within an
election timeout steps down, and the followers elect one that commits.

Three members on a ``SimulationLoop``, ticked at the production cadence;
once the first leader is elected, every message from a follower to it is
lost.
"""

import contextvars

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RaftLogEntry,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import (
    ELECTION_TIMEOUT_MAX,
    HEARTBEAT_INTERVAL,
    RaftNode,
)
from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import Logger, LoggingConfig
from tests.simulation.harness.sim import SeededRandom, SimulationLoop, VirtualClock
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

MemberAddress = tuple[str, int]

MEMBER_IDS = ("member-a", "member-b", "member-c")
ADDRESSES: dict[str, MemberAddress] = {
    member_id: ("127.0.0.1", 10_000 + slot) for slot, member_id in enumerate(MEMBER_IDS)
}
# A LAN's one-way delay, well under the 150-300ms election timeout.
LATENCY_SECONDS = 0.005
# Ten election timeouts: room for the first election, the step-down and
# the next election, many times over.
ELECTION_WINDOW_SECONDS = 10 * ELECTION_TIMEOUT_MAX
SEED = 7


class AsymmetricGroup:
    """Three Raft members ticked on virtual time over a network that can
    lose every message from the followers to one member."""

    def __init__(self, clock: VirtualClock) -> None:
        self._clock = clock
        self._task_runner = TaskRunner()
        self._logger = Logger()
        self.unheard_member: str | None = None
        self.committed_commands: list[bytes] = []
        self.nodes = {
            member_id: RaftNode(
                job_id="check-quorum",
                node_id=member_id,
                initial_voters=frozenset(MEMBER_IDS),
                member_addrs=dict(ADDRESSES),
                send_message=lambda destination, message, sender=member_id: self._send(
                    sender, destination, message
                ),
                apply_command=self._record_applied,
                on_become_leader=None,
                on_lose_leadership=None,
                logger=self._logger,
                configured_cluster_size=len(MEMBER_IDS),
                clock=new_hybrid_logical_clock(node_id=slot + 1, clock=clock),
                may_lead=lambda: True, storage=VolatileRaftStorage()
            )
            for slot, member_id in enumerate(MEMBER_IDS)
        }
        self._running = True

    def start(self) -> None:
        for member_id, node in self.nodes.items():
            self._task_runner.run(self._tick_loop, node, alias=f"tick-{member_id}")

    async def stop(self) -> None:
        self._running = False
        for node in self.nodes.values():
            node.destroy()
        await self._task_runner.shutdown()

    def leaders(self) -> list[str]:
        return [member_id for member_id, node in self.nodes.items() if node.is_leader()]

    async def until_a_leader(self, excluding: str | None = None) -> str | None:
        """The first leader other than ``excluding`` within the election
        window, or None."""
        deadline = self._clock.monotonic() + ELECTION_WINDOW_SECONDS
        while self._clock.monotonic() < deadline:
            if leaders := [leader for leader in self.leaders() if leader != excluding]:
                return leaders[0]
            await self._clock.sleep(HEARTBEAT_INTERVAL)
        return None

    async def _tick_loop(self, node: RaftNode) -> None:
        """The production coordinator's cadence."""
        while self._running:
            await node.tick()
            if node.is_leader():
                await node.replicate_to_followers()
            await node.apply_committed_entries()
            await self._clock.sleep(HEARTBEAT_INTERVAL)

    async def _send(self, sender: str, destination: MemberAddress, message: object) -> None:
        self._task_runner.run(self._deliver, sender, destination, message, alias="deliver")

    async def _deliver(self, sender: str, destination: MemberAddress, message: object) -> None:
        await self._clock.sleep(LATENCY_SECONDS)
        receiver_id = next(member_id for member_id, address in ADDRESSES.items() if address == destination)
        if receiver_id == self.unheard_member and sender != receiver_id:
            return
        receiver = self.nodes[receiver_id]
        if isinstance(message, RequestVote):
            await self._send(receiver_id, ADDRESSES[sender], await receiver.handle_request_vote(message))
        elif isinstance(message, RequestVoteResponse):
            await receiver.handle_request_vote_response(message)
        elif isinstance(message, AppendEntries):
            await self._send(receiver_id, ADDRESSES[sender], await receiver.handle_append_entries(message))
        elif isinstance(message, AppendEntriesResponse):
            await receiver.handle_append_entries_response(message)

    async def _record_applied(self, entry: RaftLogEntry) -> None:
        if entry.command not in self.committed_commands:
            self.committed_commands.append(entry.command)


def simulate(scenario, run_until: float):
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock, random_source=SeededRandom(seed=SEED))

    def run_through_deadline():
        LoggingConfig().disable()
        scenario_task = loop.create_task(scenario(clock))
        loop.run_window(run_until)
        assert scenario_task.done(), f"still running at virtual {run_until}"
        return scenario_task.result()

    try:
        return contextvars.copy_context().run(run_through_deadline)
    finally:
        loop.close()
        restore_defaults(snapshot)


def test_a_leader_its_followers_cannot_answer_steps_down_for_one_that_commits() -> None:
    async def scenario(clock: VirtualClock):
        group = AsymmetricGroup(clock)
        group.start()
        try:
            first_leader = await group.until_a_leader()
            assert first_leader is not None
            # The first leader's heartbeats still reach its followers;
            # nothing they send reaches it.
            group.unheard_member = first_leader

            next_leader = await group.until_a_leader(excluding=first_leader)
            committed = (
                False
                if next_leader is None
                else (await group.nodes[next_leader].propose(b"after-the-partition", "TEST"))[0]
            )
            return first_leader, next_leader, committed, group.leaders(), group.committed_commands
        finally:
            await group.stop()

    first_leader, next_leader, committed, leaders, committed_commands = simulate(
        scenario, run_until=4 * ELECTION_WINDOW_SECONDS
    )

    assert next_leader is not None, f"{first_leader} kept every follower refusing votes"
    assert committed and b"after-the-partition" in committed_commands
    # The first leader stepped down: it is not a leader beside the new one.
    assert leaders == [next_leader]
