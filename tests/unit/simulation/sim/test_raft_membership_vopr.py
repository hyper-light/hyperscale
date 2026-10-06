"""
VOPR: a Raft group's membership changes through its log (AD-52 slice A),
under seeded faults -- real ``RaftNode`` instances on a ``SimulationLoop``
(see ``RaftGroupSimulation``).

Five members, a faulty network (latency, drops, partitions), and crashes
that come back under a NEW id at the same address, as a restarted
manager's does. A driver changes membership only through the leader's
``change_membership``: a restarted incarnation joins as a learner, is
promoted once it holds every committed entry, and the dead incarnation's
voter is removed -- through joint configurations.

Across every seed, throughout the run:
* election safety -- at most one leader per term (the restart double vote
  this closes: a member's old and new incarnation never both count);
* leader completeness -- every new leader holds every entry applied
  anywhere;
* state machine safety -- every member applies the same entry at each
  index.
Once faults heal: one leader, every live member at the same commit index,
and the voters exactly the live incarnations.

With Raft state on disk (D1), the same holds -- plus no command a
proposer saw committed is ever lost -- when every crashed member resumes
its id and state from its disk, even with any number of members down at
once (whole-group power losses included); and when only some do, the
rest coming back with lost disks under new ids. Members that resume
vote in terms they voted in before: only their persisted votes keep
election safety.
"""

import contextvars
from typing import Any, Callable, Coroutine, TypeVar

from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from hyperscale.logging import LoggingConfig
from tests.simulation.harness.sim import SeededRandom, SimulationLoop, VirtualClock
from tests.simulation.harness.sim.raft_group_simulation import RaftGroupSimulation
from tests.simulation.harness.sim.raft_group_simulation_report import (
    RaftGroupSimulationReport,
)

ScenarioResult = TypeVar("ScenarioResult")

# Five members tolerate two failures: enough for a crash and a partition
# to overlap while a quorum survives.
MEMBER_COUNT = 5
# A LAN's one-way delay, well under the 150-300ms election timeout.
LATENCY_BOUNDS_SECONDS = (0.001, 0.02)
MESSAGE_DROP_PROBABILITY = 0.05
# A fault every two seconds on average: several membership changes in
# flight per fault window.
MEAN_SECONDS_BETWEEN_FAULTS = 2.0
CRASH_PROBABILITY = 0.5
# From well inside one election round (150-300ms) to ten of them: a
# restart can land in the very term its old incarnation voted in.
CRASH_DOWNTIME_BOUNDS_SECONDS = (0.01, 3.0)
PARTITION_DURATION_BOUNDS_SECONDS = (0.3, 2.0)
PROPOSAL_INTERVAL_SECONDS = 0.1
MEMBERSHIP_INTERVAL_SECONDS = 0.25
PROPOSAL_TIMEOUT_SECONDS = 2.0
FAULT_SECONDS = 30.0
# Long enough to remove every dead voter and promote every restarted
# learner, one change at a time, after the last fault.
QUIET_SECONDS = 15.0
# A few heartbeats for the last commits to reach every member.
SETTLE_SECONDS = 1.0
SEEDS = range(20)


def _simulate(
    scenario: Callable[[VirtualClock], Coroutine[Any, Any, ScenarioResult]],
    seed: int,
) -> ScenarioResult:
    """Run ``scenario`` on a fresh ``SimulationLoop``: every
    ``hyperscale.distributed`` clock on its virtual time, randomness seeded,
    logging off, in a context of its own; ``run_window`` raises if anything
    spins at one virtual instant."""
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock, random_source=SeededRandom(seed=seed))
    run_until = FAULT_SECONDS + QUIET_SECONDS + SETTLE_SECONDS + 1.0

    def run_through_deadline() -> ScenarioResult:
        LoggingConfig().disable()
        scenario_task = loop.create_task(scenario(clock))
        loop.run_window(run_until)
        assert scenario_task.done(), f"seed {seed}: still running at virtual {run_until}"
        return scenario_task.result()

    try:
        return contextvars.copy_context().run(run_through_deadline)
    finally:
        loop.close()
        restore_defaults(snapshot)


def _run_seed(
    seed: int,
    resume_probability: float = 0.0,
    power_loss_probability: float = 0.0,
    disk_latency_seconds: float = 0.0,
    crash_after_reply_probability: float = 0.0,
    append_durability_check_probability: float = 0.0,
) -> RaftGroupSimulationReport:
    async def scenario(clock: VirtualClock) -> RaftGroupSimulationReport:
        simulation = RaftGroupSimulation(
            seed=seed,
            member_count=MEMBER_COUNT,
            clock=clock,
            message_drop_probability=MESSAGE_DROP_PROBABILITY,
            latency_bounds_seconds=LATENCY_BOUNDS_SECONDS,
            mean_seconds_between_faults=MEAN_SECONDS_BETWEEN_FAULTS,
            crash_probability=CRASH_PROBABILITY,
            crash_downtime_bounds_seconds=CRASH_DOWNTIME_BOUNDS_SECONDS,
            partition_duration_bounds_seconds=PARTITION_DURATION_BOUNDS_SECONDS,
            proposal_interval_seconds=PROPOSAL_INTERVAL_SECONDS,
            membership_interval_seconds=MEMBERSHIP_INTERVAL_SECONDS,
            proposal_timeout_seconds=PROPOSAL_TIMEOUT_SECONDS,
            resume_probability=resume_probability,
            power_loss_probability=power_loss_probability,
            disk_latency_seconds=disk_latency_seconds,
            crash_after_reply_probability=crash_after_reply_probability,
            append_durability_check_probability=append_durability_check_probability,
        )
        return await simulation.run(FAULT_SECONDS, QUIET_SECONDS, SETTLE_SECONDS)

    return _simulate(scenario, seed)


def test_membership_changes_keep_raft_safe_and_converge() -> None:
    for seed in SEEDS:
        report = _run_seed(seed)

        assert report.violations == [], f"seed {seed}: {report.violations}"
        # The run exercised what it is about.
        assert report.crashes > 0 and report.configuration_changes_started > 0, (
            f"seed {seed}: crashes={report.crashes} "
            f"changes={report.configuration_changes_started}"
        )
        assert report.proposals_committed > 0, f"seed {seed}: nothing committed"

        assert len(report.final_leaders) == 1, f"seed {seed}: leaders {report.final_leaders}"
        assert len(set(report.final_commit_indexes.values())) == 1, (
            f"seed {seed}: commit indexes {report.final_commit_indexes}"
        )
        assert report.final_voters == report.live_member_ids, (
            f"seed {seed}: voters {sorted(report.final_voters)} vs live "
            f"{sorted(report.live_member_ids)} (learners {sorted(report.final_learners)})"
        )


def test_a_seed_replays_identically() -> None:
    first_run = _run_seed(SEEDS[0])
    second_run = _run_seed(SEEDS[0])
    assert (
        first_run.leaders_by_term,
        first_run.committed_entries,
        first_run.final_voters,
        first_run.final_commit_indexes,
    ) == (
        second_run.leaders_by_term,
        second_run.committed_entries,
        second_run.final_voters,
        second_run.final_commit_indexes,
    )


def _assert_safe_and_converged(seed: int, report: RaftGroupSimulationReport) -> None:
    assert report.violations == [], f"seed {seed}: {report.violations}"
    assert report.proposals_committed > 0, f"seed {seed}: nothing committed"
    assert len(report.final_leaders) == 1, f"seed {seed}: leaders {report.final_leaders}"
    assert len(set(report.final_commit_indexes.values())) == 1, (
        f"seed {seed}: commit indexes {report.final_commit_indexes}"
    )
    assert report.final_voters == report.live_member_ids, (
        f"seed {seed}: voters {sorted(report.final_voters)} vs live {sorted(report.live_member_ids)}"
    )


DURABLE_SEEDS = range(10)
# A quarter of the crashes take every member down at once.
POWER_LOSS_PROBABILITY = 0.25
# An SSD's fsync (0.5-5ms is typical): long enough that a write is in
# flight when the power goes.
DISK_LATENCY_SECONDS = 0.002
# One answer in fifty is followed at once by a power loss.
CRASH_AFTER_REPLY_PROBABILITY = 0.02
# One successful append in ten is checked against the answering disk.
APPEND_DURABILITY_CHECK_PROBABILITY = 0.1


def test_members_that_resume_from_disk_stay_safe_through_majority_and_total_power_loss() -> None:
    reports = [
        _run_seed(
            seed,
            resume_probability=1.0,
            power_loss_probability=POWER_LOSS_PROBABILITY,
            disk_latency_seconds=DISK_LATENCY_SECONDS,
            crash_after_reply_probability=CRASH_AFTER_REPLY_PROBABILITY,
            append_durability_check_probability=APPEND_DURABILITY_CHECK_PROBABILITY,
        )
        for seed in DURABLE_SEEDS
    ]

    for seed, report in zip(DURABLE_SEEDS, reports):
        _assert_safe_and_converged(seed, report)
        # Every restart resumed: the members are the ones the group began with.
        assert report.live_member_ids == {f"member-{slot}-incarnation-0" for slot in range(MEMBER_COUNT)}, (
            f"seed {seed}: {sorted(report.live_member_ids)}"
        )
    assert sum(report.resumptions for report in reports) > len(DURABLE_SEEDS)
    assert sum(report.power_losses for report in reports) > 0
    assert sum(report.crashes_after_reply for report in reports) > len(DURABLE_SEEDS)


def test_members_that_resume_or_lose_their_disks_stay_safe() -> None:
    reports = [
        _run_seed(
            seed,
            resume_probability=0.5,
            disk_latency_seconds=DISK_LATENCY_SECONDS,
            append_durability_check_probability=APPEND_DURABILITY_CHECK_PROBABILITY,
        )
        for seed in DURABLE_SEEDS
    ]

    for seed, report in zip(DURABLE_SEEDS, reports):
        _assert_safe_and_converged(seed, report)
    assert sum(report.resumptions for report in reports) > 0
    assert sum(report.configuration_changes_started for report in reports) > 0
