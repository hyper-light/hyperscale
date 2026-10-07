"""
``LocalLeaderElection.seconds_until_next_decision`` names the instant the
election loop next decides -- probed on virtual time, across seeds.

A manager refusing a submission for want of a known leader hands this
reading to the client as its retry hint, so it must be exact: a reading
that ran out before the loop acted would send the client back to the same
refusal; one that ran past it would keep the client away from a leader.

Real ``LocalLeaderElection`` instances (configured from ``Env``, each with
its own seeded random source) on a ``SimulationLoop``. The one stand-in is
the wire: these nodes have no peers that answer, so nothing but the loop's
own timers moves them.
"""

import random
from typing import Any

from hyperscale.distributed.env import Env
from hyperscale.distributed.swim.leadership import LocalLeaderElection
from hyperscale.distributed.swim.leadership.leader_eligibility import LeaderEligibility
from hyperscale.distributed.swim.leadership.leader_state import LeaderState
from hyperscale.distributed.taskex import TaskRunner
from tests.simulation.harness.sim import VirtualClock
from tests.unit.simulation.sim.test_leader_election_first_round import _SEEDS, _simulate

_ADDRESS = ("127.0.0.1", 9001)

# How finely the sampler reads the election.
_SAMPLE_SECONDS = 0.001

# Long enough for several pre-vote rounds of an isolated node.
_OBSERVE_SECONDS = 20.0

# Float slack on a deadline reconstructed as reading instant + reading.
_DEADLINE_TOLERANCE_SECONDS = 1e-9


async def _no_peers(message: bytes) -> None:
    """The broadcast of a node nobody hears."""


def _election(clock: VirtualClock, seed: int, cohort_size: int, task_runner: TaskRunner) -> LocalLeaderElection:
    leader_config = Env().get_leader_election_config()
    election = LocalLeaderElection(
        dc_id="sim",
        heartbeat_interval=leader_config["heartbeat_interval"],
        election_timeout_base=leader_config["election_timeout_base"],
        election_timeout_jitter=leader_config["election_timeout_jitter"],
        pre_vote_timeout=leader_config["pre_vote_timeout"],
        state=LeaderState(lease_duration=leader_config["lease_duration"]),
        eligibility=LeaderEligibility(max_leader_lhm=leader_config["max_leader_lhm"]),
        _clock=clock,
        _random=random.Random(seed),
    )
    election.set_callbacks(
        broadcast_message=_no_peers,
        get_member_count=lambda: cohort_size,
        get_lhm_score=lambda: 0,
        self_addr=_ADDRESS,
        task_runner=task_runner,
    )
    return election


async def _sample_deadlines(
    clock: VirtualClock,
    seed: int,
    cohort_size: int,
) -> tuple[list[tuple[float, float]], float | None]:
    """Boot one leaderless node and read it every sample until it leads or
    the observation ends: every (instant, deadline it named) while it was
    leaderless and waiting, and when it first led."""
    task_runner = TaskRunner()
    election = _election(clock, seed, cohort_size, task_runner)
    readings: list[tuple[float, float]] = []
    await election.start()
    try:
        while clock.monotonic() < _OBSERVE_SECONDS:
            if election.state.is_leader():
                return readings, clock.monotonic()
            if (remaining := election.seconds_until_next_decision()) > 0.0:
                readings.append((clock.monotonic(), clock.monotonic() + remaining))
            await clock.sleep(_SAMPLE_SECONDS)
        return readings, None
    finally:
        await election.stop()
        await task_runner.shutdown()


def test_a_lone_node_leads_exactly_when_its_reading_said() -> None:
    """A cohort of one decides at its candidacy deadline and leads then:
    every reading taken while it waited names that instant."""
    for seed in _SEEDS:

        async def scenario(clock: VirtualClock, seed: int = seed) -> Any:
            return await _sample_deadlines(clock, seed, cohort_size=1)

        readings, first_led_at = _simulate(scenario, _OBSERVE_SECONDS + 1.0)

        assert first_led_at is not None, f"seed {seed}: never led"
        assert readings, f"seed {seed}: led at {first_led_at} before any reading"
        assert all(
            abs(deadline - first_led_at) <= _SAMPLE_SECONDS
            for _instant, deadline in readings
        ), f"seed {seed}: led at {first_led_at}, readings named {sorted({deadline for _, deadline in readings})}"


def test_an_isolated_nodes_readings_name_each_round_it_runs() -> None:
    """A node of three that hears nobody stands, waits out each pre-vote
    round and stands again. Each reading names the instant its current wait
    ends: the loop acts then -- arming its next wait -- never sooner (no
    peer can wake it) and never later."""
    for seed in _SEEDS:

        async def scenario(clock: VirtualClock, seed: int = seed) -> Any:
            return await _sample_deadlines(clock, seed, cohort_size=3)

        readings, first_led_at = _simulate(scenario, _OBSERVE_SECONDS + 1.0)

        assert first_led_at is None, f"seed {seed}: an isolated node of three led at {first_led_at}"
        distinct_deadlines: list[float] = []
        first_seen_at: list[float] = []
        for instant, deadline in readings:
            if not distinct_deadlines or abs(deadline - distinct_deadlines[-1]) > _DEADLINE_TOLERANCE_SECONDS:
                distinct_deadlines.append(deadline)
                first_seen_at.append(instant)
        assert len(distinct_deadlines) > 2, f"seed {seed}: {distinct_deadlines}"
        for previous_deadline, armed_at in zip(distinct_deadlines, first_seen_at[1:]):
            assert previous_deadline - _DEADLINE_TOLERANCE_SECONDS <= armed_at, (
                f"seed {seed}: the loop re-armed at {armed_at}, before the {previous_deadline} it named"
            )
            assert armed_at <= previous_deadline + _SAMPLE_SECONDS + _DEADLINE_TOLERANCE_SECONDS, (
                f"seed {seed}: the loop re-armed at {armed_at}, after the {previous_deadline} it named"
            )
