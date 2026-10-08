"""
``ClusterMembership.seconds_until_next_formation_round`` names when an
unformed member's next formation round begins -- probed on virtual time,
across seeds.

A manager or gate refusing a submission because its cluster has not formed
hands this reading to the submitter as its retry hint. Between rounds it
must name the next round's start exactly; while a round runs, it names one
formation interval from now, and no round may start before the instant any
reading named -- a submitter told to come back then would otherwise have
stayed away while a round it could have waited for ran.

A member of a three-member cohort whose founders never answer (nothing
listens at their addresses), on the cluster VOPR's simulated network.
"""

from typing import Any

from hyperscale.distributed.cluster.cluster_membership import CLUSTER_HELLO_ACTION
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import Logger
from tests.simulation.harness.sim import VirtualClock
from tests.unit.simulation.sim.test_cluster_mismatch_vopr import (
    COHORT_PORTS,
    FORMATION_INTERVAL_SECONDS,
    HOST,
    MismatchNetwork,
    _simulate,
    start_member,
)

SEEDS = range(6)
# How finely the sampler reads the member.
SAMPLE_SECONDS = 0.001
OBSERVE_SECONDS = 12.0
# Float slack on a deadline reconstructed as reading instant + reading.
DEADLINE_TOLERANCE_SECONDS = 1e-9


async def _sample_rounds(clock: VirtualClock, seed: int) -> tuple[list[tuple[float, float]], list[float], bool]:
    """Every (instant, round start it named) and every instant a formation
    round began (its first founder greeting), sampled while the member is
    unformed; and whether it formed."""
    network = MismatchNetwork(seed, clock)
    task_runner = TaskRunner()
    address = (HOST, COHORT_PORTS[0])
    member = await start_member(network, clock, task_runner, Logger(), f"isolated-{seed}", address, COHORT_PORTS)
    readings: list[tuple[float, float]] = []
    round_starts: list[float] = []
    greetings_seen = 0
    try:
        while clock.monotonic() < OBSERVE_SECONDS:
            greetings = [sent for sent in network.sent if sent[0] == address and sent[2] == CLUSTER_HELLO_ACTION]
            if len(greetings) > greetings_seen:
                round_starts.append(clock.monotonic())
                greetings_seen = len(greetings)
            readings.append((clock.monotonic(), clock.monotonic() + member.seconds_until_next_formation_round()))
            await clock.sleep(SAMPLE_SECONDS)
        return readings, round_starts, member.formed
    finally:
        await member.stop()
        await task_runner.shutdown()


def test_an_unformed_members_readings_name_its_next_round() -> None:
    for seed in SEEDS:

        async def scenario(clock: VirtualClock, seed: int = seed) -> Any:
            return await _sample_rounds(clock, seed)

        readings, round_starts, formed = _simulate(scenario, seed, OBSERVE_SECONDS + 1.0)

        assert not formed, f"seed {seed}: a member whose founders never answer formed"
        assert len(round_starts) > 2, f"seed {seed}: {round_starts}"
        assert all(
            deadline - instant <= FORMATION_INTERVAL_SECONDS + DEADLINE_TOLERANCE_SECONDS
            for instant, deadline in readings
        ), f"seed {seed}: a reading past one formation interval"
        for round_start in round_starts:
            # No reading named an instant after a round that then began
            # before it: the submitter would have stayed away from it.
            early = [
                (instant, deadline)
                for instant, deadline in readings
                if instant < round_start - SAMPLE_SECONDS and deadline > round_start + SAMPLE_SECONDS
            ]
            assert not early, f"seed {seed}: round at {round_start} began inside readings {early[:3]}"
        for round_start in round_starts[1:]:
            # Between rounds the reading names the next one's start.
            _instant, named_start = [reading for reading in readings if reading[0] < round_start][-1]
            assert abs(named_start - round_start) <= SAMPLE_SECONDS, (
                f"seed {seed}: the reading before the round at {round_start} named {named_start}"
            )
