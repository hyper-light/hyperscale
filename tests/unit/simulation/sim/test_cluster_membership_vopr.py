"""
VOPR: a cluster's membership group (AD-52 slice C) -- formation without
storage, joins, crash-restarts under new node ids, and a cluster that can
no longer commit being founded anew -- on real ``ClusterMembership``
instances (see ``ClusterMembershipSimulation``).

A machine can also be lost for good: once faults heal, the cluster
converges without it -- the group's leader releases the address its last
process held (it has not answered for the tombstone retention).

Across every seed, throughout the run:
* election safety -- per cluster, at most one leader per term;
* state machine safety -- per cluster, every member holds the same entry
  committed at each index;
* no two clusters could commit at once;
* no handler raises and no membership loop dies.
Once faults heal: one cluster, every live process formed in it, its
voters exactly the live processes, one leader, one commit index.

Without faults, however the founders' starts interleave -- all at once, or
staggered -- exactly one founding commits and every founder forms in it.
Two founders whose rounds overlap within a greeting's round trip may each
propose a founding; at most one commits, and the other's founders leave
it for the formed cluster (a founder could only rule that race out by
confirming its view over a second round, costing every formation an
interval to spare a rare one a round or two).
"""

import contextvars
from typing import Any, Callable, Coroutine, TypeVar

import pytest

from hyperscale.distributed.env import Env

from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from hyperscale.logging import LoggingConfig
from tests.simulation.harness.sim import SeededRandom, SimulationLoop, VirtualClock
from tests.simulation.harness.sim.cluster_membership_simulation import (
    ClusterMembershipSimulation,
)
from tests.simulation.harness.sim.cluster_membership_simulation_report import (
    ClusterMembershipSimulationReport,
)

ScenarioResult = TypeVar("ScenarioResult")

# A LAN's one-way delay, well under the 150-300ms election timeout.
LATENCY_BOUNDS_SECONDS = (0.001, 0.02)
# A lost segment costs TCP one retransmission timeout: 200ms minimum (the
# Linux RTO floor), longer after backoff.
RETRANSMISSION_PROBABILITY = 0.02
RETRANSMISSION_DELAY_BOUNDS_SECONDS = (0.2, 1.0)
CONNECTION_RESET_PROBABILITY = 0.005
REQUEST_TIMEOUT_SECONDS = 1.0
FORMATION_INTERVAL_SECONDS = 1.0
# Production holds an unheard member for minutes (AD-52 section 8); the
# simulation scales it down so a run sees members released, and clusters
# left after it, many times over.
TOMBSTONE_RETENTION_SECONDS = 5.0
# Founders start up to a few rounds apart: some find the cluster formed
# and join it.
START_DELAY_BOUNDS_SECONDS = (0.0, 6.0)
MEAN_SECONDS_BETWEEN_FAULTS = 3.0
FAULT_WEIGHTS = {"crash": 0.4, "partition": 0.35, "one_way_partition": 0.25}
# From inside one election (150-300ms) -- a restart that can land in the
# term its old process voted in -- to well past the tombstone retention.
CRASH_DOWNTIME_BOUNDS_SECONDS = (0.05, 20.0)
# Partitions that heal before the tombstone retention, and ones that
# outlast it.
PARTITION_DURATION_BOUNDS_SECONDS = (0.5, 8.0)
OBSERVATION_INTERVAL_SECONDS = 0.01
FAULT_SECONDS = 60.0
# Long enough to leave a cluster that cannot commit (up to one tombstone
# retention), found the next and grow it to every process, after the last
# fault heals.
QUIET_SECONDS = 40.0
SEEDS = range(20)


def _simulate(
    scenario: Callable[[VirtualClock], Coroutine[Any, Any, ScenarioResult]],
    seed: int,
    run_until: float,
) -> ScenarioResult:
    """Run ``scenario`` on a fresh ``SimulationLoop``: every
    ``hyperscale.distributed`` clock on its virtual time, randomness seeded,
    logging off, in a context of its own; ``run_window`` raises if anything
    spins at one virtual instant."""
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock, random_source=SeededRandom(seed=seed))

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
    cohort_size: int,
    *,
    start_delay_bounds_seconds: tuple[float, float] = START_DELAY_BOUNDS_SECONDS,
    fault_weights: dict[str, float] = FAULT_WEIGHTS,
    retransmission_probability: float = RETRANSMISSION_PROBABILITY,
    connection_reset_probability: float = CONNECTION_RESET_PROBABILITY,
    fault_seconds: float = FAULT_SECONDS,
    quiet_seconds: float = QUIET_SECONDS,
    lost_address_count: int = 0,
    tombstone_retention_seconds: float = TOMBSTONE_RETENTION_SECONDS,
    leader_lease_drift_bound: float | None = None,
    **departures: float,
) -> ClusterMembershipSimulationReport:
    async def scenario(clock: VirtualClock) -> ClusterMembershipSimulationReport:
        simulation = ClusterMembershipSimulation(
            seed=seed,
            cohort_size=cohort_size,
            clock=clock,
            latency_bounds_seconds=LATENCY_BOUNDS_SECONDS,
            retransmission_probability=retransmission_probability,
            retransmission_delay_bounds_seconds=RETRANSMISSION_DELAY_BOUNDS_SECONDS,
            connection_reset_probability=connection_reset_probability,
            request_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
            start_delay_bounds_seconds=start_delay_bounds_seconds,
            mean_seconds_between_faults=MEAN_SECONDS_BETWEEN_FAULTS,
            fault_weights=fault_weights,
            crash_downtime_bounds_seconds=CRASH_DOWNTIME_BOUNDS_SECONDS,
            partition_duration_bounds_seconds=PARTITION_DURATION_BOUNDS_SECONDS,
            formation_interval_seconds=FORMATION_INTERVAL_SECONDS,
            tombstone_retention_seconds=tombstone_retention_seconds,
            observation_interval_seconds=OBSERVATION_INTERVAL_SECONDS,
            leader_lease_drift_bound=leader_lease_drift_bound,
        )
        return await simulation.run(fault_seconds, quiet_seconds, lost_address_count, **departures)

    return _simulate(
        scenario,
        seed,
        # Departures settle once, and the operator waits one more settle
        # before a force-remove.
        run_until=fault_seconds
        + 2 * departures.get("settle_seconds", 0.0)
        + departures.get("frozen_seconds", 0.0)
        # A resize: a settle per relaunched member, and the new one's.
        + (cohort_size + 3) * departures.get("settle_seconds", 0.0) * bool(departures.get("resize"))
        + quiet_seconds
        + 5.0,
    )


def _assert_converged(seed: int, report: ClusterMembershipSimulationReport) -> None:
    assert report.violations == [], f"seed {seed}: {report.violations}"
    assert len(report.final_cluster_uuids) == 1, f"seed {seed}: clusters {report.final_cluster_uuids}"
    assert set(report.final_formations.values()) == {"formed"}, (
        f"seed {seed}: formations {report.final_formations}"
    )
    assert len(report.final_leaders) == 1, f"seed {seed}: leaders {report.final_leaders}"
    assert (report.final_voters, report.final_learners, report.final_is_joint) == (
        report.live_member_ids,
        frozenset(),
        False,
    ), (
        f"seed {seed}: voters {sorted(report.final_voters)} learners "
        f"{sorted(report.final_learners)} joint {report.final_is_joint} vs live "
        f"{sorted(report.live_member_ids)}"
    )
    assert len(set(report.final_commit_indexes.values())) == 1, (
        f"seed {seed}: commit indexes {report.final_commit_indexes}"
    )


@pytest.mark.parametrize("cohort_size", [3, 5])
def test_membership_stays_safe_under_faults_and_converges(cohort_size: int) -> None:
    reports = [_run_seed(seed, cohort_size) for seed in SEEDS]

    for seed, report in zip(SEEDS, reports):
        _assert_converged(seed, report)
    # The runs exercised what they are about -- across the seeds: one seed's
    # fault stream may, by its draw, crash nothing.
    assert sum(report.crashes for report in reports) > len(SEEDS)
    assert sum(report.partitions for report in reports) > len(SEEDS)
    assert sum(report.one_way_partitions for report in reports) > 0
    # Clusters left and founded anew after a quorum of their voters' processes died.
    assert sum(len(report.clusters_formed) for report in reports) > len(SEEDS)
    # Linearizable status reads were served throughout -- and refused when
    # leadership could not be confirmed (a read checked stale fails above).
    assert sum(report.status_reads_served for report in reports) > len(SEEDS)
    assert sum(report.status_reads_refused for report in reports) > 0
    # The watch followed every cluster through crashes, refoundings and
    # member switches, and ended on the final state.
    assert all(report.watch_matches_final_state for report in reports), [
        seed for seed, report in zip(SEEDS, reports) if not report.watch_matches_final_state
    ]
    assert sum(report.watch_events for report in reports) > len(SEEDS)
    assert sum(report.watch_snapshots for report in reports) > len(SEEDS)
    # Its connectivity was reported once per change (checked as it ran),
    # starting connected at its first answer.
    assert all(
        report.watch_connectivity_transitions and report.watch_connectivity_transitions[0][1] is False
        for report in reports
    ), [report.watch_connectivity_transitions[:2] for report in reports]


@pytest.mark.parametrize("cohort_size", [3, 5])
def test_lease_reads_stay_linearizable_under_faults(cohort_size: int) -> None:
    """Leader leases on (AD-52 section 11): a leader a quorum answered
    serves status reads from its lease, through the same crashes,
    partitions and refoundings -- and every read served is still
    linearizable (a stale one is a violation)."""
    reports = [
        _run_seed(seed, cohort_size, leader_lease_drift_bound=Env().RAFT_CLOCK_DRIFT_BOUND) for seed in SEEDS
    ]

    for seed, report in zip(SEEDS, reports):
        _assert_converged(seed, report)
    assert sum(report.crashes for report in reports) > len(SEEDS)
    assert sum(report.partitions for report in reports) > len(SEEDS)
    assert sum(report.status_reads_served for report in reports) > len(SEEDS)
    # Leases answered reads -- the runs exercised what they are about.
    assert sum(report.status_reads_served_by_lease for report in reports) > 0


@pytest.mark.parametrize("cohort_size", [3, 5])
def test_the_cluster_converges_without_a_machine_lost_for_good(cohort_size: int) -> None:
    for seed in SEEDS:
        report = _run_seed(seed, cohort_size, lost_address_count=1)

        _assert_converged(seed, report)
        assert len(report.live_member_ids) == cohort_size - 1, (
            f"seed {seed}: live {sorted(report.live_member_ids)}"
        )


@pytest.mark.parametrize("cohort_size", [3, 5])
@pytest.mark.parametrize(
    "start_delay_bounds_seconds",
    [(0.0, 0.0), START_DELAY_BOUNDS_SECONDS],
    ids=["simultaneous-starts", "staggered-starts"],
)
def test_without_faults_exactly_one_founding_commits(
    cohort_size: int, start_delay_bounds_seconds: tuple[float, float]
) -> None:
    for seed in SEEDS:
        report = _run_seed(
            seed,
            cohort_size,
            start_delay_bounds_seconds=start_delay_bounds_seconds,
            fault_weights={"partition": 1.0},
            retransmission_probability=0.0,
            connection_reset_probability=0.0,
            # No fault window: the founders start, and the run is quiet.
            fault_seconds=0.0,
            quiet_seconds=start_delay_bounds_seconds[1] + 10 * FORMATION_INTERVAL_SECONDS,
        )

        _assert_converged(seed, report)
        assert len(report.clusters_formed) == 1, f"seed {seed}: formed {report.clusters_formed}"


def test_a_seed_replays_identically() -> None:
    first_run = _run_seed(SEEDS[0], 5)
    second_run = _run_seed(SEEDS[0], 5)
    assert (
        first_run.leaders_by_term,
        first_run.committed_entries,
        first_run.clusters_formed,
        first_run.final_voters,
        first_run.final_commit_indexes,
    ) == (
        second_run.leaders_by_term,
        second_run.committed_entries,
        second_run.clusters_formed,
        second_run.final_voters,
        second_run.final_commit_indexes,
    )


# What production holds an unheard member for (CLUSTER_TOMBSTONE_RETENTION_
# SECONDS): far beyond the windows below, so only a drain or a force-remove
# can release a departed member's address within them.
PRODUCTION_TOMBSTONE_RETENTION_SECONDS = 600.0
# Founders start at once and form within a few rounds.
FORMATION_SETTLE_SECONDS = 10 * FORMATION_INTERVAL_SECONDS


@pytest.mark.parametrize(
    ("cohort_size", "drained", "force_removed"),
    [(3, 1, 0), (3, 0, 1), (5, 1, 1)],
)
def test_departed_members_leave_without_waiting_out_the_retention(
    cohort_size: int, drained: int, force_removed: int
) -> None:
    """AD-52 section 13: a member that drains as it stops, and one an
    operator removes after it died, leave the configuration within a few
    rounds -- the leader releases their addresses through the log, its
    reconciliation removes them -- while a force-remove of a member that
    still answers is refused."""
    for seed in SEEDS:
        report = _run_seed(
            seed,
            cohort_size,
            start_delay_bounds_seconds=(0.0, 0.0),
            fault_weights={"partition": 1.0},
            retransmission_probability=0.0,
            connection_reset_probability=0.0,
            fault_seconds=0.0,
            quiet_seconds=FORMATION_SETTLE_SECONDS,
            tombstone_retention_seconds=PRODUCTION_TOMBSTONE_RETENTION_SECONDS,
            settle_seconds=FORMATION_SETTLE_SECONDS,
            drained_address_count=drained,
            force_removed_address_count=force_removed,
            live_removal_attempts=1,
        )

        _assert_converged(seed, report)
        assert len(report.live_member_ids) == cohort_size - drained - force_removed
        assert all(released for _member, released, _refusal in report.drains), (
            f"seed {seed}: drains {report.drains}"
        )
        assert all(released for _member, released, _refusal in report.force_removals), (
            f"seed {seed}: force-removals {report.force_removals}"
        )
        assert None not in report.live_removal_refusals, (
            f"seed {seed}: a live member was force-removed"
        )


@pytest.mark.parametrize("cohort_size", [3, 5])
def test_departures_after_faults_stay_safe_and_converge(cohort_size: int) -> None:
    """Drains and force-removes right after faults heal -- leaders still
    settling -- break no safety invariant, and the cluster converges on
    the processes left."""
    for seed in SEEDS:
        report = _run_seed(
            seed,
            cohort_size,
            drained_address_count=1,
            force_removed_address_count=1 if cohort_size == 5 else 0,
            live_removal_attempts=1,
        )

        _assert_converged(seed, report)
        assert None not in report.live_removal_refusals, (
            f"seed {seed}: a live member was force-removed"
        )


@pytest.mark.parametrize("cohort_size", [3, 5])
def test_a_frozen_membership_holds_until_opened(cohort_size: int) -> None:
    """AD-52 section 13: frozen, no membership change commits -- a member
    dead for twice the tombstone retention stays a voter, a drain is
    refused -- and once opened, the cluster converges on the live
    processes."""
    for seed in SEEDS:
        report = _run_seed(
            seed,
            cohort_size,
            start_delay_bounds_seconds=(0.0, 0.0),
            fault_weights={"partition": 1.0},
            retransmission_probability=0.0,
            connection_reset_probability=0.0,
            fault_seconds=0.0,
            quiet_seconds=2 * TOMBSTONE_RETENTION_SECONDS + FORMATION_SETTLE_SECONDS,
            settle_seconds=FORMATION_SETTLE_SECONDS,
            frozen_seconds=2 * TOMBSTONE_RETENTION_SECONDS,
        )

        assert report.mode_changes == [("frozen", True, None), ("open", True, None)], (
            f"seed {seed}: {report.mode_changes}"
        )
        assert report.dead_member_while_frozen in report.voters_while_frozen, (
            f"seed {seed}: frozen voters {sorted(report.voters_while_frozen)}"
        )
        assert report.frozen_drain_refusal == "the cluster's membership is frozen", (
            f"seed {seed}: drain while frozen: {report.frozen_drain_refusal}"
        )
        _assert_converged(seed, report)
        assert len(report.live_member_ids) == cohort_size - 1
        # One cluster throughout: the watch is snapshotted once, when it
        # starts, then follows changes -- the retained log tail keeps it
        # from falling behind compaction (compacting after every entry
        # left it nothing but snapshots).
        assert report.watch_snapshots == 1, f"seed {seed}: {report.watch_snapshots} snapshots"
        assert report.watch_events > 0


def _resize_run(seed: int, cohort_size: int, resize: str) -> ClusterMembershipSimulationReport:
    return _run_seed(
        seed,
        cohort_size,
        start_delay_bounds_seconds=(0.0, 0.0),
        fault_weights={"partition": 1.0},
        retransmission_probability=0.0,
        connection_reset_probability=0.0,
        fault_seconds=0.0,
        quiet_seconds=2 * FORMATION_SETTLE_SECONDS,
        tombstone_retention_seconds=PRODUCTION_TOMBSTONE_RETENTION_SECONDS,
        settle_seconds=FORMATION_SETTLE_SECONDS,
        resize=resize,
    )


@pytest.mark.parametrize("cohort_size", [3, 4])
def test_the_cohort_grows_by_one_and_rolls_onto_it(cohort_size: int) -> None:
    """AD-52 ``ResizeCluster``: a resize adds an address; the process
    launched there with the new cohort joins; a second resize is refused
    while the others still run with the old cohort; relaunched one at a
    time onto the new one, the cluster converges on every process, each
    holding the new cohort."""
    for seed in SEEDS:
        report = _resize_run(seed, cohort_size, "grow")

        assert report.resizes[0][1:] == (True, None), f"seed {seed}: {report.resizes}"
        assert report.early_resize_refusal is not None and "older cohort" in report.early_resize_refusal, (
            f"seed {seed}: second resize: {report.early_resize_refusal}"
        )
        _assert_converged(seed, report)
        assert len(report.live_member_ids) == cohort_size + 1
        assert len(report.final_cohorts) == 1 and len(next(iter(report.final_cohorts))) == cohort_size + 1
        # Every member that followed the log (installed no snapshot) counted
        # the one resize it applied; none keeps a watch counted open.
        for member, changes_applied in report.final_changes_applied.items():
            if report.final_snapshots_installed.get(member, 0) == 0:
                assert changes_applied.get("cluster_resize") == 1, (seed, member, changes_applied)
        assert report.watches_open_after_stop == 0, f"seed {seed}"


@pytest.mark.parametrize("cohort_size", [4, 5])
def test_the_cohort_shrinks_by_one(cohort_size: int) -> None:
    """A resize removes a live member's address: it leaves the
    configuration, and the cluster converges on the rest."""
    for seed in SEEDS:
        report = _resize_run(seed, cohort_size, "shrink")

        assert report.resizes[0][1:] == (True, None), f"seed {seed}: {report.resizes}"
        _assert_converged(seed, report)
        assert len(report.live_member_ids) == cohort_size - 1
        assert len(report.final_cohorts) == 1 and len(next(iter(report.final_cohorts))) == cohort_size - 1


@pytest.mark.parametrize("cohort_size", [3, 4, 5])
def test_processes_a_resize_behind_never_split_the_cluster(cohort_size: int) -> None:
    """Every process restarts at once right after a resize, the old ones
    still launched with the cohort before it: one resize apart, any
    majority of either cohort shares an address with any majority of the
    other, so at most one cluster can commit -- whichever forms."""
    for seed in SEEDS:
        report = _resize_run(seed, cohort_size, "grow_then_restart_stale")

        assert report.resizes[0][1:] == (True, None), f"seed {seed}: {report.resizes}"
        assert report.violations == [], f"seed {seed}: {report.violations}"
        assert len(report.final_cluster_uuids) == 1, f"seed {seed}: clusters {report.final_cluster_uuids}"
