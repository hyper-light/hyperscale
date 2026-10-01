"""
AD-38 for the GATE job ledger, end to end under multi-process SIM: three
peered gates (each with a durable ledger, in configurable regions), one
datacenter, a client submitting through a non-seed gate.

Measured on the old code (same topology, east/east/west): the leader
gate's ledger reached LOCAL only (regional and global watermarks 0) —
gates had no replicators and never formed per-job groups — and the two
peer gates held the job for the rest of the run, because no gate ever
told its peers a job ended (their copy stayed SUBMITTED, which the
terminal-only cleanup sweep never removes).

Pinned:
* east/east/west: every gate mirrors the job's ledger events through the
  job's gate group; the leader's records reach GLOBAL (its global
  watermark equals its newest LSN: holders span two regions); every
  group retires with the terminal and every gate releases the job within
  retention + one cleanup interval + one sample of finishing.
* east/east/east: GLOBAL is unachievable, so it is never claimed — the
  records are REGIONAL (regional watermark == newest LSN, global 0) and
  the job still completes.
"""

from tests.simulation.harness.sim.multiprocess.gate_ledger_demo import (
    GATE_HOSTS,
    WATCH_INTERVAL_SECONDS,
    run_gate_ledger_job,
)

_CEILING = 90.0
_JOB_MAX_AGE_SECONDS = 20.0
_JOB_CLEANUP_INTERVAL_SECONDS = 2.0


def _rows(log: list, tag: str) -> list[tuple[object, float]]:
    return [(entry[1], entry[2]) for entry in log if entry[0] == tag]


def _finished_at(results: dict) -> float:
    (finished,) = [entry for entry in results["client"] if entry[0] == "job-finished"]
    assert finished[1] == "completed", results["client"]
    return finished[2]


def _ledger_leader(results: dict) -> str:
    """The gate whose own ledger recorded the job."""
    (leader,) = [
        host
        for host in GATE_HOSTS
        if any(active + terminal > 0 for (active, terminal, *_), _ in _rows(results[host], "ledger"))
    ]
    return leader


def test_two_region_tier_records_reach_global_and_every_gate_releases_the_job():
    results = run_gate_ledger_job(
        ("east", "east", "west"),
        _CEILING,
        _JOB_MAX_AGE_SECONDS,
        _JOB_CLEANUP_INTERVAL_SECONDS,
    )
    finished_at = _finished_at(results)

    leader = _ledger_leader(results)
    (active, terminal, synced_lsn, regional_lsn, global_lsn), _ = _rows(results[leader], "ledger")[-1]
    assert (active, terminal) == (0, 1), results[leader]
    assert regional_lsn == global_lsn == synced_lsn, results[leader]

    release_bound = (
        finished_at
        + _JOB_MAX_AGE_SECONDS
        + _JOB_CLEANUP_INTERVAL_SECONDS
        + WATCH_INTERVAL_SECONDS
    )
    for host in GATE_HOSTS:
        log = results[host]
        assert max(count for count, _ in _rows(log, "replica-events")) >= 1, (host, log)
        assert _rows(log, "replica-events")[-1][0] == 0, (host, log)
        assert max(count for count, _ in _rows(log, "raft-groups")) == 1, (host, log)
        assert _rows(log, "raft-groups")[-1][0] == 0, (host, log)
        final_jobs, released_at = _rows(log, "jobs")[-1]
        assert final_jobs == 0 and released_at <= release_bound, (host, log)


def test_single_region_tier_never_claims_global():
    results = run_gate_ledger_job(
        ("east", "east", "east"),
        _CEILING,
        _JOB_MAX_AGE_SECONDS,
        _JOB_CLEANUP_INTERVAL_SECONDS,
    )
    _finished_at(results)

    leader = _ledger_leader(results)
    (active, terminal, synced_lsn, regional_lsn, global_lsn), _ = _rows(results[leader], "ledger")[-1]
    assert (active, terminal) == (0, 1), results[leader]
    assert regional_lsn == synced_lsn, results[leader]
    assert global_lsn == 0, results[leader]
