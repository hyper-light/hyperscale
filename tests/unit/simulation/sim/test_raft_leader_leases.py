"""
Leader leases (AD-52 section 11; Raft thesis 6.4.1) -- real ``RaftNode``
instances on virtual time.

With leases on, a leader whose heartbeat round a quorum answered holds a
lease until that round's send time + the minimum election timeout,
shortened by the clock drift bound; while it holds one, ReadIndex needs no
round of its own. The lease is safe because a follower refuses votes for
the minimum election timeout after hearing its leader -- and a member that
restarts, having forgotten that leader, withholds them as long from its
start.

* A leader with a lease answers reads without a round; without leases
  every read takes one.
* A leader cut off from every member keeps answering from its lease, and
  never once the others have elected a leader of their own.
* A restarted member refuses votes for one minimum election timeout.
"""

import contextvars

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.env import Env
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RaftLogEntry,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import (
    ELECTION_TIMEOUT_MAX,
    ELECTION_TIMEOUT_MIN,
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
    member_id: ("127.0.0.1", 11_000 + slot) for slot, member_id in enumerate(MEMBER_IDS)
}
# A LAN's one-way delay, well under the 150-300ms election timeout.
LATENCY_SECONDS = 0.005
# Ten election timeouts: room for an election many times over.
ELECTION_WINDOW_SECONDS = 10 * ELECTION_TIMEOUT_MAX
# How often the isolated leader is asked for a read: far finer than a lease.
READ_INTERVAL_SECONDS = 0.002
DRIFT_BOUND = Env().RAFT_CLOCK_DRIFT_BOUND
SEEDS = (3, 7, 11, 19, 29)


class LeaseGroup:
    """Three Raft members ticked on virtual time over a network that can
    cut one member off entirely."""

    def __init__(self, clock: VirtualClock, drift_bound: float | None) -> None:
        self._clock = clock
        self._drift_bound = drift_bound
        self._task_runner = TaskRunner()
        self._logger = Logger()
        self.isolated_member: str | None = None
        self.nodes = {member_id: self._new_node(member_id, slot) for slot, member_id in enumerate(MEMBER_IDS)}
        self._running = True

    def _new_node(self, member_id: str, slot: int) -> RaftNode:
        return RaftNode(
            job_id="leases",
            node_id=member_id,
            initial_voters=frozenset(MEMBER_IDS),
            member_addrs=dict(ADDRESSES),
            send_message=lambda destination, message, sender=member_id: self._send(sender, destination, message),
            apply_command=self._ignore_applied,
            on_become_leader=None,
            on_lose_leadership=None,
            logger=self._logger,
            configured_cluster_size=len(MEMBER_IDS),
            clock=new_hybrid_logical_clock(node_id=slot + 1, clock=self._clock),
            may_lead=lambda: True,
            leader_lease_drift_bound=self._drift_bound, storage=VolatileRaftStorage()
        )

    def restart(self, member_id: str) -> RaftNode:
        """Replace ``member_id`` with a fresh incarnation (its log and every
        memory of the leader gone)."""
        self.nodes[member_id].destroy()
        self.nodes[member_id] = self._new_node(member_id, MEMBER_IDS.index(member_id))
        return self.nodes[member_id]

    def start(self) -> None:
        for member_id in MEMBER_IDS:
            self._task_runner.run(self._tick_loop, member_id, alias=f"tick-{member_id}")

    async def stop(self) -> None:
        self._running = False
        for node in self.nodes.values():
            node.destroy()
        await self._task_runner.shutdown()

    def leaders(self) -> list[str]:
        return [member_id for member_id, node in self.nodes.items() if node.is_leader()]

    async def until_a_leader(self, excluding: str | None = None) -> str | None:
        deadline = self._clock.monotonic() + ELECTION_WINDOW_SECONDS
        while self._clock.monotonic() < deadline:
            if leaders := [leader for leader in self.leaders() if leader != excluding]:
                return leaders[0]
            await self._clock.sleep(HEARTBEAT_INTERVAL / 10)
        return None

    async def until_a_committed_term_start(self, leader: str) -> None:
        """ReadIndex answers once the leader committed an entry of its term."""
        node = self.nodes[leader]
        deadline = self._clock.monotonic() + ELECTION_WINDOW_SECONDS
        while await node.read_index() is None:
            assert self._clock.monotonic() < deadline, f"{leader} never committed its term's entry"
            await self._clock.sleep(HEARTBEAT_INTERVAL)

    async def _tick_loop(self, member_id: str) -> None:
        """The production coordinator's cadence."""
        while self._running:
            node = self.nodes[member_id]
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
        if self.isolated_member in (sender, receiver_id) and sender != receiver_id:
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

    async def _ignore_applied(self, entry: RaftLogEntry) -> None:
        return None


def simulate(scenario, seed: int, run_until: float):
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock, random_source=SeededRandom(seed=seed))

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


def read_rounds_and_lease_reads(seed: int, drift_bound: float | None) -> tuple[int, int, int]:
    """Ten reads on a settled leader: (lease reads, reads answered, the
    commit index they answered)."""

    async def scenario(clock: VirtualClock):
        group = LeaseGroup(clock, drift_bound)
        group.start()
        try:
            leader = await group.until_a_leader()
            assert leader is not None
            await group.until_a_committed_term_start(leader)
            node = group.nodes[leader]
            lease_reads_before = node.metrics()["lease_reads"]
            answers = []
            for _ in range(10):
                answers.append(await node.read_index())
                await clock.sleep(HEARTBEAT_INTERVAL / 5)
            assert len(set(answers)) == 1 and answers[0] == node.commit_index, answers
            return node.metrics()["lease_reads"] - lease_reads_before, len(answers), answers[0]
        finally:
            await group.stop()

    return simulate(scenario, seed, run_until=4 * ELECTION_WINDOW_SECONDS)


def test_a_leader_with_a_lease_answers_reads_without_a_round_of_their_own() -> None:
    for seed in SEEDS:
        lease_reads, answered, _commit_index = read_rounds_and_lease_reads(seed, DRIFT_BOUND)
        # Heartbeats renew the lease every interval, far inside its length:
        # every read finds one.
        assert lease_reads == answered, (seed, lease_reads, answered)

        lease_reads, answered, _commit_index = read_rounds_and_lease_reads(seed, None)
        assert lease_reads == 0, (seed, lease_reads)


def isolated_leader_reads(seed: int) -> tuple[float, list[float], float | None]:
    """Cut the leader off; ask it for reads until the others elect one of
    their own. Returns when the cut began, when each of the cut leader's
    reads was answered, and when the next leader was elected."""

    async def scenario(clock: VirtualClock):
        group = LeaseGroup(clock, DRIFT_BOUND)
        group.start()
        try:
            leader = await group.until_a_leader()
            assert leader is not None
            await group.until_a_committed_term_start(leader)
            node = group.nodes[leader]
            cut_at = clock.monotonic()
            group.isolated_member = leader

            answered_at: list[float] = []
            next_leader_elected_at: float | None = None
            deadline = cut_at + ELECTION_WINDOW_SECONDS
            while clock.monotonic() < deadline and next_leader_elected_at is None:
                if await node.read_index() is not None:
                    answered_at.append(clock.monotonic())
                if [other for other in group.leaders() if other != leader]:
                    next_leader_elected_at = clock.monotonic()
                await clock.sleep(READ_INTERVAL_SECONDS)
            return cut_at, answered_at, next_leader_elected_at
        finally:
            await group.stop()

    return simulate(scenario, seed, run_until=4 * ELECTION_WINDOW_SECONDS)


def test_a_cut_off_leader_answers_from_its_lease_and_never_beside_a_new_leader() -> None:
    lease_seconds = ELECTION_TIMEOUT_MIN / (1.0 + DRIFT_BOUND)
    for seed in SEEDS:
        cut_at, answered_at, next_leader_elected_at = isolated_leader_reads(seed)

        assert next_leader_elected_at is not None, f"seed {seed}: the others never elected a leader"
        # The lease outlives the cut: the cut leader still answers reads.
        assert answered_at, f"seed {seed}: no read answered from the lease after the cut"
        # Never once another leader exists -- nor past its last answered
        # round's lease (that round left at most one interval before the cut).
        assert max(answered_at) < next_leader_elected_at, (seed, max(answered_at), next_leader_elected_at)
        assert max(answered_at) < cut_at + lease_seconds, (seed, max(answered_at), cut_at)


def restarted_member_votes(seed: int) -> tuple[bool, bool]:
    """Restart a follower, then ask it for a vote at once and again one
    minimum election timeout later."""

    async def scenario(clock: VirtualClock):
        group = LeaseGroup(clock, DRIFT_BOUND)
        try:
            restarted = group.restart("member-c")
            request = RequestVote(
                job_id="leases",
                term=restarted.current_term + 1,
                candidate_id="member-a",
                last_log_index=0,
                last_log_term=0,
            )
            at_once = (await restarted.handle_request_vote(request)).vote_granted
            await clock.sleep(ELECTION_TIMEOUT_MIN)
            later = (await restarted.handle_request_vote(request)).vote_granted
            return at_once, later
        finally:
            await group.stop()

    return simulate(scenario, seed, run_until=ELECTION_WINDOW_SECONDS)


def test_a_restarted_member_withholds_votes_for_one_minimum_election_timeout() -> None:
    for seed in SEEDS:
        at_once, later = restarted_member_votes(seed)
        assert (at_once, later) == (False, True), (seed, at_once, later)
