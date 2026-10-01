"""
AD-38 REGIONAL durability of a manager's job ledger, end to end under
multi-process SIM: three peered managers, a worker, a client.

REGIONAL used to be unreachable: managers had no regional replicator, so
every ledger record was LOCAL — on one disk of one manager. When that
manager died its successor's ledger did not know the job at all, so the
job's later events (its terminal above all) appended nothing anywhere.
Measured on the old code with the job leader killed mid-job: the
survivor's ledger stayed empty for the rest of the run.

Pinned: each ledger record commits through the job's Raft group (every
member mirrors JobCreated + JobAccepted, and the leader's terminal lands
REGIONAL: its regional watermark reaches its newest LSN); with the job
leader killed after its REGIONAL records, a survivor takes the job over
and adopts the job's ledger history into its own ledger.

The kill instant is derived from an unfaulted run of the same topology
(replay-identical up to the kill): midway between the leader's REGIONAL
acknowledgement of JobAccepted and the job finishing.

Known gap, not judged here: after the leader dies the job never reaches
a terminal on the survivor (the same on the old code) -- AD-36 mid-flight
failover.
"""

from tests.simulation.harness.sim.multiprocess.peered_manager_demo import (
    PEERED_MANAGERS,
    run_peered_manager_job,
)

_CEILING = 30.0
_FAILOVER_CEILING = 60.0
# Retention outlasts the run: these scenarios judge the ledger, not cleanup.
_JOB_RETENTION_SECONDS = 600.0
_JOB_CLEANUP_INTERVAL_SECONDS = 60.0
_MANAGER_HOSTS = [host for host, _, _ in PEERED_MANAGERS]


def _rows(log: list, tag: str) -> list[tuple[object, float]]:
    return [(entry[1], entry[2]) for entry in log if entry[0] == tag]


def _job_leader(results: dict) -> str:
    """The member whose own ledger recorded the job."""
    (leader,) = [
        host
        for host in _MANAGER_HOSTS
        if any(active > 0 or terminal > 0 for (active, terminal, _, _), _ in _rows(results[host], "ledger"))
    ]
    return leader


def test_ledger_records_reach_every_member_and_the_terminal_is_regional():
    results = run_peered_manager_job(
        _CEILING, _JOB_RETENTION_SECONDS, _JOB_CLEANUP_INTERVAL_SECONDS
    )

    finished = [entry for entry in results["client"] if entry[0] == "job-finished"]
    assert [entry[1] for entry in finished] == ["completed"], results["client"]

    for host in _MANAGER_HOSTS:
        replica_counts = [count for count, _ in _rows(results[host], "replica-events")]
        # JobCreated + JobAccepted mirrored; released with the group.
        assert max(replica_counts) >= 2, (host, results[host])
        assert replica_counts[-1] == 0, (host, results[host])

    leader = _job_leader(results)
    (active, terminal, synced_lsn, regional_lsn), _ = _rows(results[leader], "ledger")[-1]
    assert (active, terminal) == (0, 1), results[leader]
    assert regional_lsn == synced_lsn, results[leader]


def test_survivor_adopts_the_jobs_ledger_record_when_the_leader_dies():
    unfaulted = run_peered_manager_job(
        _CEILING,
        _JOB_RETENTION_SECONDS,
        _JOB_CLEANUP_INTERVAL_SECONDS,
        sustained=True,
    )
    leader = _job_leader(unfaulted)
    accepted_regional_at = next(
        time for (_, _, _, regional_lsn), time in _rows(unfaulted[leader], "ledger") if regional_lsn >= 1
    )
    (finished_at,) = [entry[2] for entry in unfaulted["client"] if entry[0] == "job-finished"]
    kill_at = (accepted_regional_at + finished_at) / 2
    assert accepted_regional_at < kill_at < finished_at

    faulted = run_peered_manager_job(
        _FAILOVER_CEILING,
        _JOB_RETENTION_SECONDS,
        _JOB_CLEANUP_INTERVAL_SECONDS,
        sustained=True,
        kill=(leader, kill_at),
    )

    adopters = [
        host
        for host in _MANAGER_HOSTS
        if host != leader
        and any(
            active == 1 and synced_lsn >= 1 and time > kill_at
            for (active, _, synced_lsn, _), time in _rows(faulted[host], "ledger")
        )
    ]
    assert len(adopters) == 1, {host: faulted[host] for host in _MANAGER_HOSTS}
