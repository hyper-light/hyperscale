"""
A leaderless group elects in ONE round -- probed, then pinned, on virtual
time.

THE BUG THIS PINS (traced on the pre-fix code): a follower stood for
election a fixed 0.5s after its leader's lease lapsed. Every follower saw
the leader's last heartbeat at the same instant, so every one of them
stood at the same instant too, voted for itself, and refused the others'
claims: the first round always split, and the group sat leaderless for a
full randomized vote wait (``election_timeout_base`` + jitter) before a
second round elected anyone -- on every failover, and at every boot. Raft
(section 5.2) avoids exactly this with a randomized election timeout;
``LocalLeaderElection`` now draws the wait past the lease from the
configured jitter, so the first node to stand collects the votes of peers
still waiting.

Real ``LocalLeaderElection`` instances (configured from ``Env``, each with
its own seeded random source) on a ``SimulationLoop``; the one stand-in is
the wire: a bus that delivers each leadership message after a fixed link
latency and applies it exactly as the server's leadership handlers do.
"""

import asyncio
import contextvars
import random
from typing import Any, Callable, Coroutine, TypeVar

from hyperscale.distributed.env import Env
from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from hyperscale.distributed.swim.leadership import LocalLeaderElection
from hyperscale.distributed.swim.leadership.leader_eligibility import LeaderEligibility
from hyperscale.distributed.swim.leadership.leader_state import LeaderState
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import LoggingConfig
from tests.simulation.harness.sim import SimulationLoop, VirtualClock


ScenarioResult = TypeVar("ScenarioResult")

_ADDRESSES = (("127.0.0.1", 9001), ("127.0.0.1", 9011), ("127.0.0.1", 9021))

# One-way delivery time of a leadership message: a LAN round trip is two
# of these, far below the election jitter it must not straddle.
_LINK_LATENCY_SECONDS = 0.005

# Long enough for a boot election, a failover, and the vote wait a split
# round would add (``election_timeout_base`` + jitter), several times over.
_OBSERVE_SECONDS = 60.0

_SEEDS = (11, 23, 37, 41, 59)

# How finely the leadership watcher samples who leads.
_LEADERSHIP_SAMPLE_SECONDS = 0.001


def _simulate(
    scenario: Callable[[VirtualClock], Coroutine[Any, Any, ScenarioResult]],
    until: float,
) -> ScenarioResult:
    """Run ``scenario`` on a fresh ``SimulationLoop`` through virtual
    ``until``, every ``hyperscale.distributed`` clock on its virtual time and
    logging off, in a context of its own; ``run_window`` raises if anything
    spins at one virtual instant."""
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock)

    def run_through_deadline() -> ScenarioResult:
        LoggingConfig().disable()
        scenario_task = loop.create_task(scenario(clock))
        loop.run_window(until)
        assert scenario_task.done(), f"the scenario was still running at virtual {until}"
        return scenario_task.result()

    try:
        return contextvars.copy_context().run(run_through_deadline)
    finally:
        loop.close()
        restore_defaults(snapshot)


def _encoded_address(address: tuple[str, int]) -> bytes:
    return f"{address[0]}:{address[1]}".encode()


def _decoded_address(encoded: bytes) -> tuple[str, int]:
    host, _, port = encoded.decode().rpartition(":")
    return host, int(port)


async def _apply_leadership_message(
    elections: dict[tuple[str, int], LocalLeaderElection],
    sender: tuple[str, int],
    recipient: tuple[str, int],
    message: bytes,
    send: Callable[[tuple[str, int], tuple[str, int], bytes], None],
) -> None:
    """Apply ``message`` at ``recipient`` as the server's leadership
    handlers do (swim/message_handling/leadership/)."""
    election = elections[recipient]
    message_type, _, payload = message.partition(b":")
    body, _, encoded_target = payload.rpartition(b">")
    target = _decoded_address(encoded_target)
    fields = body.split(b":")

    if message_type == b"pre-vote-req":
        if response := election.handle_pre_vote_request(target, int(fields[0]), int(fields[1])):
            send(recipient, target, response)

    elif message_type == b"pre-vote-resp":
        if election.state.pre_voting_in_progress:
            election.handle_pre_vote_response(sender, int(fields[0]), fields[1] == b"1")

    elif message_type == b"leader-claim":
        if vote := election.handle_claim(target, int(fields[0]), int(fields[1])):
            send(recipient, target, vote)

    elif message_type == b"leader-vote":
        term = int(fields[0])
        if election.state.is_candidate() and election.handle_vote(sender, term):
            election.state.become_leader(term)
            election.state.current_leader = recipient
            for peer in elections:
                if peer != recipient:
                    send(recipient, peer, b"leader-elected:" + str(term).encode() + b">" + _encoded_address(recipient))

    elif message_type == b"leader-elected":
        if target != recipient:
            await election.handle_elected(target, int(fields[0]))

    elif message_type == b"leader-heartbeat":
        if target != recipient:
            await election.handle_heartbeat(
                target, int(fields[0]), int(fields[1]), int(fields[2]) / 1000.0
            )
            if acknowledgement := election.heartbeat_acknowledgement(target, int(fields[0])):
                send(recipient, target, acknowledgement)

    elif message_type == b"leader-heartbeat-ack":
        election.handle_heartbeat_ack(sender, int(fields[0]), int(fields[1]))

    elif message_type == b"leader-stepdown":
        await election.handle_stepdown(target, int(fields[0]))


async def _run_group(
    clock: VirtualClock,
    seed: int,
    stop_leader_after: float | None,
    addresses: tuple[tuple[str, int], ...] = _ADDRESSES,
    cohort: frozenset[tuple[str, int]] = frozenset(_ADDRESSES),
    refusing_leadership: frozenset[tuple[str, int]] = frozenset(),
) -> tuple[
    list[tuple[float, tuple[str, int], int]],
    list[tuple[float, tuple[str, int], int]],
    dict[tuple[str, int], tuple[str, int, tuple[str, int] | None]],
    dict[tuple[str, int], float],
]:
    """Boot an election per address, leaderless, each counting its
    majority over ``cohort`` and admitting only the cohort's votes (as
    managers and gates do); ``refusing_leadership`` never stand, but vote.
    Optionally stop whichever leads ``stop_leader_after`` seconds in.
    Returns every election a node stood for (when, who, term), every
    leader stop (when, who, term), each running node's final (role, term,
    leader), and when each node first led."""
    leader_config = Env().get_leader_election_config()
    task_runner = TaskRunner()
    live_addresses = set(addresses)
    elections: dict[tuple[str, int], LocalLeaderElection] = {}
    election_starts: list[tuple[float, tuple[str, int], int]] = []
    leader_stops: list[tuple[float, tuple[str, int], int]] = []
    first_led_at: dict[tuple[str, int], float] = {}

    async def deliver(sender: tuple[str, int], recipient: tuple[str, int], message: bytes) -> None:
        await clock.sleep(_LINK_LATENCY_SECONDS)
        if recipient in live_addresses:
            await _apply_leadership_message(elections, sender, recipient, message, send)

    def send(sender: tuple[str, int], recipient: tuple[str, int], message: bytes) -> None:
        task_runner.run(deliver, sender, recipient, message)

    for index, address in enumerate(addresses):
        election = LocalLeaderElection(
            dc_id="sim",
            heartbeat_interval=leader_config["heartbeat_interval"],
            election_timeout_base=leader_config["election_timeout_base"],
            election_timeout_jitter=leader_config["election_timeout_jitter"],
            pre_vote_timeout=leader_config["pre_vote_timeout"],
            state=LeaderState(lease_duration=leader_config["lease_duration"]),
            eligibility=LeaderEligibility(max_leader_lhm=leader_config["max_leader_lhm"]),
            _clock=clock,
            _random=random.Random(f"{seed}-{index}"),
        )

        async def broadcast(message: bytes, sender: tuple[str, int] = address) -> None:
            for peer in addresses:
                if peer != sender:
                    send(sender, peer, message)

        def record_election_start(candidate: tuple[str, int] = address) -> None:
            election_starts.append(
                (clock.monotonic(), candidate, elections[candidate].state.current_term)
            )

        election.set_callbacks(
            broadcast_message=broadcast,
            get_member_count=lambda: len(cohort),
            get_lhm_score=lambda: 0,
            self_addr=address,
            task_runner=task_runner,
            on_election_started=record_election_start,
            should_refuse_leadership=lambda refuses=address in refusing_leadership: refuses,
            is_cohort_voter=cohort.__contains__,
        )
        elections[address] = election

    async def watch_leadership() -> None:
        while True:
            for address in sorted(live_addresses):
                if address not in first_led_at and elections[address].state.is_leader():
                    first_led_at[address] = clock.monotonic()
            await clock.sleep(_LEADERSHIP_SAMPLE_SECONDS)

    task_runner.run(watch_leadership)
    for election in elections.values():
        await election.start()

    try:
        if stop_leader_after is not None:
            await clock.sleep(stop_leader_after)
            leaders = [address for address, election in elections.items() if election.state.is_leader()]
            assert len(leaders) == 1, f"expected one leader before the stop, found {leaders}"
            stopped_leader = leaders[0]
            leader_stops.append(
                (clock.monotonic(), stopped_leader, elections[stopped_leader].state.current_term)
            )
            live_addresses.discard(stopped_leader)
            await elections[stopped_leader].stop()
            await clock.sleep(_OBSERVE_SECONDS - stop_leader_after)
        else:
            await clock.sleep(_OBSERVE_SECONDS)

        return (
            election_starts,
            leader_stops,
            {
                address: (
                    elections[address].state.role,
                    elections[address].state.current_term,
                    elections[address].state.current_leader,
                )
                for address in sorted(live_addresses)
            },
            first_led_at,
        )
    finally:
        for address in sorted(live_addresses):
            await elections[address].stop()
        await task_runner.shutdown()


def test_a_leaderless_group_elects_in_one_round() -> None:
    """Three nodes boot leaderless together: exactly one stands, and it is
    elected for term 1 -- no split round, whatever the seed."""
    election_timeout_jitter = Env().LEADER_ELECTION_TIMEOUT_JITTER
    for seed in _SEEDS:

        async def scenario(clock: VirtualClock, seed: int = seed) -> Any:
            return await _run_group(clock, seed, stop_leader_after=None)

        election_starts, _, final_states, _ = _simulate(scenario, _OBSERVE_SECONDS + 1.0)

        assert len(election_starts) == 1, f"seed {seed}: {election_starts}"
        stood_at, winner, term = election_starts[0]
        assert term == 1, f"seed {seed}: {election_starts}"
        # Booted with no leader at 0.0, the winner stood within the
        # randomized wait.
        assert 0.0 <= stood_at < election_timeout_jitter, f"seed {seed}: stood at {stood_at}"
        assert final_states[winner] == ("leader", 1, winner), f"seed {seed}: {final_states}"
        assert all(
            state == ("follower", 1, winner)
            for address, state in final_states.items()
            if address != winner
        ), f"seed {seed}: {final_states}"


def test_followers_replace_a_stopped_leader_in_one_round() -> None:
    """The leader stops; its two followers -- who saw its last heartbeat at
    the same instant -- elect one of themselves for the next term in a
    single round."""
    election_timeout_jitter = Env().LEADER_ELECTION_TIMEOUT_JITTER
    lease_duration = Env().LEADER_LEASE_DURATION
    heartbeat_interval = Env().LEADER_HEARTBEAT_INTERVAL
    stop_leader_after = 20.0
    for seed in _SEEDS:

        async def scenario(clock: VirtualClock, seed: int = seed) -> Any:
            return await _run_group(clock, seed, stop_leader_after=stop_leader_after)

        election_starts, leader_stops, final_states, _ = _simulate(scenario, _OBSERVE_SECONDS + 1.0)

        (stopped_at, stopped_leader, stopped_term), = leader_stops
        failover_starts = [start for start in election_starts if start[0] > stopped_at]
        assert len(failover_starts) == 1, f"seed {seed}: {failover_starts}"
        stood_at, winner, term = failover_starts[0]
        assert term == stopped_term + 1, f"seed {seed}: {failover_starts}"
        # Its followers' leases lapse at most a lease after the leader's last
        # beat (at most one heartbeat interval before the stop); the winner
        # stood within the randomized wait after that.
        assert stood_at < stopped_at + lease_duration + election_timeout_jitter, (
            f"seed {seed}: stood at {stood_at}, leader stopped at {stopped_at}"
        )
        assert stood_at >= stopped_at - heartbeat_interval + lease_duration, (
            f"seed {seed}: stood at {stood_at} before the lease could lapse"
        )
        assert final_states[winner] == ("leader", term, winner), f"seed {seed}: {final_states}"
        assert all(
            state == ("follower", term, winner)
            for address, state in final_states.items()
            if address != winner
        ), f"seed {seed}: {final_states}"


def test_a_cohort_of_one_leads_the_moment_it_stands() -> None:
    """Its own vote is the majority: nothing can arrive that the outcome
    waits on, so it leads at once -- not after a pre-vote wait and a full
    vote wait (``pre_vote_timeout`` + ``election_timeout_base`` + jitter)
    with no leader and no work accepted, on every boot of a one-manager
    datacenter or a lone gate."""
    (address, *_) = _ADDRESSES
    election_timeout_jitter = Env().LEADER_ELECTION_TIMEOUT_JITTER
    for seed in _SEEDS:

        async def scenario(clock: VirtualClock, seed: int = seed) -> Any:
            return await _run_group(
                clock, seed, stop_leader_after=None, addresses=(address,), cohort=frozenset({address})
            )

        election_starts, _, final_states, first_led_at = _simulate(scenario, _OBSERVE_SECONDS + 1.0)

        ((stood_at, _, term),) = election_starts
        assert term == 1, f"seed {seed}: {election_starts}"
        # Booted leaderless at 0.0, it leads within the randomized wait to
        # stand -- no pre-vote wait before its vote phase, none after.
        assert first_led_at[address] - stood_at <= _LEADERSHIP_SAMPLE_SECONDS, (
            f"seed {seed}: stood at {stood_at}, led at {first_led_at[address]}"
        )
        assert first_led_at[address] < election_timeout_jitter + _LEADERSHIP_SAMPLE_SECONDS, (
            f"seed {seed}: led at {first_led_at[address]}"
        )
        assert final_states[address] == ("leader", 1, address), f"seed {seed}: {final_states}"


def test_a_vote_from_outside_the_cohort_never_completes_a_majority() -> None:
    """One member of a three-member cohort runs, beside a node outside the
    cohort that grants every vote asked of it (a removed member still
    running). Its vote must not carry the lone member -- a minority of its
    cohort -- to leadership. The control admits the same node to the
    cohort: then the two of them are a majority, and the member leads."""
    (member, absent_member, other_absent_member) = _ADDRESSES
    outsider = ("127.0.0.1", 9031)
    cohort_without_outsider = frozenset({member, absent_member, other_absent_member})
    cohort_with_outsider = frozenset({member, absent_member, outsider})
    for seed in _SEEDS:
        outcomes = {}
        for label, cohort in (("outside", cohort_without_outsider), ("inside", cohort_with_outsider)):

            async def scenario(clock: VirtualClock, seed: int = seed, cohort: frozenset = cohort) -> Any:
                return await _run_group(
                    clock,
                    seed,
                    stop_leader_after=None,
                    addresses=(member, outsider),
                    cohort=cohort,
                    refusing_leadership=frozenset({outsider}),
                )

            outcomes[label] = _simulate(scenario, _OBSERVE_SECONDS + 1.0)

        _, _, final_states, first_led_at = outcomes["outside"]
        assert first_led_at == {}, f"seed {seed}: {first_led_at}"
        assert final_states[member][0] != "leader", f"seed {seed}: {final_states}"

        _, _, final_states, first_led_at = outcomes["inside"]
        assert set(first_led_at) == {member}, f"seed {seed}: {first_led_at}"
        assert final_states[member][0] == "leader", f"seed {seed}: {final_states}"
