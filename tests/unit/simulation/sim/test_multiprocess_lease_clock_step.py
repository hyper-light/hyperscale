"""
Leases across a wall-clock step at the lease boundary, under
multi-process SIM (``lease_clock_step_demo``). An NTP step moves a node's
wall clock (``VirtualClock.set_wall_offset``) and never its monotonic
clock or its timers; a lease is a span of time on ONE clock, so it must be
judged on the monotonic clock and a step must neither extend it nor end it
early.

AD-52 section 11 leader leases (three peered managers, leases on). The
membership group's leader is cut off from both followers at
``_PARTITION_AT`` and, at that same instant, its wall clock steps
backward (or forward) by ``_STEP_SECONDS``. Its last lease is the one the
last heartbeat round its followers answered before the cut granted:

* No double leadership: no two members ever lead the group in one term,
  and the cut-off leader serves no read -- from its lease or otherwise --
  once a follower has taken over; nor any from its lease past the newest
  lease the cut could have left it.
* Not expired early: it keeps serving reads from its lease until the
  oldest lease the cut could have left it ends.
* The followers elect a successor, and the cut-off leader steps down
  within its CheckQuorum window.

The gate's per-job ``JobLease`` (one gate fronting one datacenter): the
gate's wall clock steps backward just after one renewal of a running
job's lease and forward just after another. At every sample the lease's
own verdict of when it ends -- the sample time plus
``JobLease.remaining_seconds`` -- is the latest grant plus the lease
duration, it is active throughout the job, it is released once the job
is over, and the gate forgets it -- record and fence token -- once it has
been over for the job's retention (it used to keep every job's lease for
its lifetime).

Every scenario has a replay twin.
"""

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.hlc.clock_offset_monitor import FENCE_FRACTION_OF_MAX_OFFSET
from hyperscale.distributed.raft.raft_node import (
    ELECTION_TIMEOUT_MAX,
    ELECTION_TIMEOUT_MIN,
    HEARTBEAT_INTERVAL,
)
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.lease_clock_step_demo import (
    job_lease_gate_entry,
    lease_reading_manager_entry,
    steady_gate_client_entry,
)
from tests.simulation.harness.sim.multiprocess.peered_manager_demo import (
    LINK_LATENCY_SECONDS,
    PEERED_MANAGERS,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
    worker_entry,
)

_ENV = Env()
_SEED = 23
# A status read every link latency: finer than a heartbeat round, so a
# lease's end is seen within one read.
_READ_INTERVAL_SECONDS = LINK_LATENCY_SECONDS
_LEASE_SECONDS = ELECTION_TIMEOUT_MIN / (1.0 + _ENV.RAFT_CLOCK_DRIFT_BOUND)
# Half the AD-39 fence threshold: the stepped leader is never fenced (a
# fence would end its leadership for a reason of its own), and the step
# still exceeds a whole lease, so a lease judged on the wall clock would
# outlive -- or fall short of -- its bound by more than a lease.
_STEP_SECONDS = _ENV.HLC_MAX_CLOCK_OFFSET_MS * FENCE_FRACTION_OF_MAX_OFFSET / 1000 / 2
# The group formed and elected sim-mgr-a long before (probed 2026-10-05:
# elected at 0.44, serving lease reads from 0.52; asserted below).
_LEADER = "sim-mgr-a"
_PARTITION_AT = 10.0
# The last round whose answer was SENT before the cut: rounds go out every
# heartbeat interval and an answer is sent one link latency after its
# round. Its send time bounds the leader's last lease on both sides.
_LAST_ANSWERED_ROUND_EARLIEST = _PARTITION_AT - LINK_LATENCY_SECONDS - HEARTBEAT_INTERVAL
_LAST_ANSWERED_ROUND_LATEST = _PARTITION_AT - LINK_LATENCY_SECONDS
_LEASE_END_EARLIEST = _LAST_ANSWERED_ROUND_EARLIEST + _LEASE_SECONDS
_LEASE_END_LATEST = _LAST_ANSWERED_ROUND_LATEST + _LEASE_SECONDS
# A follower last hears the leader one link latency after the cut; its
# election deadline falls at most ELECTION_TIMEOUT_MAX later, is noticed
# on its next tick, and a pre-vote round and a vote round take a round
# trip each (one election round at this seed: probed, no split vote).
_SUCCESSOR_ELECTED_BY = (
    _PARTITION_AT
    + LINK_LATENCY_SECONDS
    + ELECTION_TIMEOUT_MAX
    + HEARTBEAT_INTERVAL
    + 4 * LINK_LATENCY_SECONDS
    + _READ_INTERVAL_SECONDS
)
# CheckQuorum: the cut-off leader steps down once no follower answered for
# its window (the manager's standard request timeout + the maximum
# election timeout), counted from the last answer it received -- at most
# one link latency after the cut -- and noticed on its next tick.
_CHECK_QUORUM_WINDOW_SECONDS = _ENV.MANAGER_TCP_TIMEOUT_STANDARD + ELECTION_TIMEOUT_MAX
_STEPPED_DOWN_BY = (
    _PARTITION_AT
    + LINK_LATENCY_SECONDS
    + _CHECK_QUORUM_WINDOW_SECONDS
    + HEARTBEAT_INTERVAL
    + _READ_INTERVAL_SECONDS
)
_RAFT_CEILING = _STEPPED_DOWN_BY + 2.0

_GATE_SEED = 31
_JOB_LEASE_DURATION_SECONDS = 4.0
# A lease shorter than the one-second floor the gate's renewal interval
# used to have: under it, the lease lapsed between renewals.
_SHORT_JOB_LEASE_DURATION_SECONDS = 0.8
_GATE_WATCH_INTERVAL_SECONDS = 0.05
# The gate's lease cleanup runs every second and forgets a job's ended
# lease (with its fence token) once it is as old as the job's retention.
_LEASE_CLEANUP_INTERVAL_SECONDS = 1.0
_JOB_RETENTION_SECONDS = 4.0
# The gate acquires the lease at 4.376867 and, with the four-second lease,
# renews it every two seconds (probed 2026-10-05): the wall clock steps
# back just after the first renewal and forward -- to as far ahead -- just
# after the fourth. The short lease renews every 0.4s, so both steps land
# within a renewal interval of a grant too.
_GATE_STEPS = [
    ("wall_skew", 6.45, -_STEP_SECONDS),
    ("wall_skew", 12.45, _STEP_SECONDS),
]
_WORKFLOW_SECONDS = 12.0
_JOB_TIMEOUT_SECONDS = 60.0
_GATE_CEILING = 40.0


def _run_leader_lease(step_seconds: float) -> dict:
    coordinator = SimulationCoordinator(
        latency=LINK_LATENCY_SECONDS, max_virtual_time=_RAFT_CEILING, seed=_SEED
    )
    for host, tcp_port, udp_port in PEERED_MANAGERS:
        coordinator.add_process(
            host,
            lease_reading_manager_entry,
            host,
            tcp_port,
            udp_port,
            "sim-dc",
            [(peer, peer_tcp) for peer, peer_tcp, _ in PEERED_MANAGERS if peer != host],
            [(peer, peer_udp) for peer, _, peer_udp in PEERED_MANAGERS if peer != host],
            [("wall_skew", _PARTITION_AT, step_seconds)] if host == _LEADER else [],
            _READ_INTERVAL_SECONDS,
        )
    for host, _, _ in PEERED_MANAGERS:
        if host != _LEADER:
            coordinator.schedule_partition(_LEADER, host, _PARTITION_AT)
    return coordinator.run()


def _run_job_lease(lease_duration_seconds: float) -> dict:
    coordinator = SimulationCoordinator(
        latency=LINK_LATENCY_SECONDS, max_virtual_time=_GATE_CEILING, seed=_GATE_SEED
    )
    coordinator.add_process(
        "gate",
        job_lease_gate_entry,
        "sim-gate",
        9000,
        9001,
        {"sim-dc": [("sim-mgr", 9000)]},
        {"sim-dc": [("sim-mgr", 9001)]},
        _GATE_STEPS,
        lease_duration_seconds,
        _LEASE_CLEANUP_INTERVAL_SECONDS,
        _JOB_RETENTION_SECONDS,
        _GATE_WATCH_INTERVAL_SECONDS,
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc", ("sim-gate", 9000), ("sim-gate", 9001)
    )
    coordinator.add_process(
        "worker", worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "client",
        steady_gate_client_entry,
        "sim-cli",
        9500,
        ("sim-gate", 9000),
        _WORKFLOW_SECONDS,
        _JOB_TIMEOUT_SECONDS,
        _GATE_CEILING,
    )
    return coordinator.run()


def _leadership_spans(log: list) -> list[tuple[int, float, float]]:
    """``(term, from, until)`` for each span the member led its group."""
    spans: list[tuple[int, float, float]] = []
    leading_since: tuple[int, float] | None = None
    for _tag, leads, term, at_time in (entry for entry in log if entry[0] == "leads"):
        if leading_since is not None:
            spans.append((leading_since[0], leading_since[1], at_time))
            leading_since = None
        if leads:
            leading_since = (term, at_time)
    if leading_since is not None:
        spans.append((leading_since[0], leading_since[1], float("inf")))
    return spans


def _served_read_times(log: list) -> list[float]:
    """The instants of every read that was served, by lease or by round:
    the bounds of each lease run, and each change to a round-served read."""
    lease_reads = [time for entry in log if entry[0] == "lease-reads" for time in entry[1:3]]
    round_reads = [entry[2] for entry in log if entry[0] == "read" and entry[1] == "round"]
    return lease_reads + round_reads


def _assert_leader_lease_held_across_the_step(results: dict) -> None:
    leader_log = results[_LEADER]
    followers = [host for host, _, _ in PEERED_MANAGERS if host != _LEADER]

    # Precondition: the stepped member leads, from its lease, at the cut.
    leader_spans = _leadership_spans(leader_log)
    assert any(start < _PARTITION_AT < until for _term, start, until in leader_spans), leader_log
    lease_runs = [entry for entry in leader_log if entry[0] == "lease-reads"]
    assert any(first < _PARTITION_AT - HEARTBEAT_INTERVAL for _tag, first, _last, _term in lease_runs), leader_log

    # No two members lead in one term.
    terms_led = [term for host, _, _ in PEERED_MANAGERS for term, _start, _until in _leadership_spans(results[host])]
    assert len(terms_led) == len(set(terms_led)), results

    # A follower takes over, within one election round.
    successor_starts = [
        start for host in followers for _term, start, _until in _leadership_spans(results[host]) if start > _PARTITION_AT
    ]
    assert successor_starts, results
    successor_elected_at = min(successor_starts)
    assert successor_elected_at <= _SUCCESSOR_ELECTED_BY, (successor_elected_at, results)

    # The cut-off leader's last lease read: never past the newest lease the
    # cut left it, never short of the oldest one.
    last_lease_read_at = max(last for _tag, _first, last, _term in lease_runs)
    assert _LEASE_END_EARLIEST - _READ_INTERVAL_SECONDS <= last_lease_read_at < _LEASE_END_LATEST, (
        _LEASE_END_EARLIEST,
        _LEASE_END_LATEST,
        last_lease_read_at,
        leader_log,
    )
    # And it serves no read at all once its successor leads.
    assert all(read_at < successor_elected_at for read_at in _served_read_times(leader_log)), (
        successor_elected_at,
        leader_log,
    )

    # It steps down within its CheckQuorum window of the cut.
    (stepped_down_at,) = [until for _term, start, until in leader_spans if start < _PARTITION_AT < until]
    assert _PARTITION_AT < stepped_down_at <= _STEPPED_DOWN_BY, (stepped_down_at, leader_log)


def test_a_backward_wall_step_at_the_cut_never_extends_the_leader_lease():
    _assert_leader_lease_held_across_the_step(_run_leader_lease(-_STEP_SECONDS))


def test_a_forward_wall_step_at_the_cut_never_ends_the_leader_lease_early():
    _assert_leader_lease_held_across_the_step(_run_leader_lease(_STEP_SECONDS))


def test_leader_lease_across_a_wall_step_is_replay_deterministic():
    assert _run_leader_lease(-_STEP_SECONDS) == _run_leader_lease(-_STEP_SECONDS)


@pytest.mark.parametrize(
    "lease_duration_seconds", [_JOB_LEASE_DURATION_SECONDS, _SHORT_JOB_LEASE_DURATION_SECONDS]
)
def test_a_job_lease_neither_stretches_nor_shrinks_across_wall_steps(lease_duration_seconds: float):
    # The gate renews every half lease (``_renew_job_lease``).
    renewal_interval_seconds = lease_duration_seconds / 2
    results = _run_job_lease(lease_duration_seconds)
    gate_log = results["gate"]
    client_log = results["client"]

    (finished,) = [entry for entry in client_log if entry[0] == "job-finished"]
    assert finished[1] == "completed", client_log

    samples = [entry for entry in gate_log if entry[0] == "lease-sample"]
    assert samples and samples[0][1], gate_log
    # The steps land on a lease being renewed: grants on both sides of each.
    grant_times = [sampled_at for _tag, granted, _active, _remaining, sampled_at in samples if granted]
    for _kind, step_at, _delta in _GATE_STEPS:
        assert any(grant < step_at for grant in grant_times), gate_log
        assert any(grant > step_at for grant in grant_times), gate_log

    # Each sample's verdict of the lease's end is the latest grant -- made
    # after the previous sample, by this one -- plus the lease duration.
    previous_sampled_at = 0.0
    granted_after = granted_by = 0.0
    for _tag, granted, active, remaining, sampled_at in samples:
        if granted:
            granted_after, granted_by = previous_sampled_at, sampled_at
        judged_end = sampled_at + remaining
        assert active, (sampled_at, gate_log)
        assert (
            granted_after + lease_duration_seconds
            < judged_end
            <= granted_by + lease_duration_seconds + 1e-6
        ), (granted_after, granted_by, sampled_at, remaining)
        previous_sampled_at = sampled_at

    # Released once the job is over: at the first renewal after its end.
    (released_at,) = [entry[1] for entry in gate_log if entry[0] == "lease-released"]
    assert finished[2] < released_at <= finished[2] + renewal_interval_seconds + _GATE_WATCH_INTERVAL_SECONDS, (
        released_at,
        client_log,
    )
    # Forgotten -- the lease record and its fence token -- once the ended
    # lease is as old as the job's retention, by the next cleanup pass;
    # held until then.
    records = [entry[1:] for entry in gate_log if entry[0] == "lease-records"]
    assert [(leases, fence_tokens) for leases, fence_tokens, _at in records] == [(0, 0), (1, 1), (0, 0)], gate_log
    forgotten_at = records[-1][2]
    released_after = released_at - _GATE_WATCH_INTERVAL_SECONDS
    assert (
        released_after + _JOB_RETENTION_SECONDS
        < forgotten_at
        <= released_at + _JOB_RETENTION_SECONDS + _LEASE_CLEANUP_INTERVAL_SECONDS + _GATE_WATCH_INTERVAL_SECONDS
    ), (released_at, forgotten_at)


def test_job_lease_across_wall_steps_is_replay_deterministic():
    assert _run_job_lease(_JOB_LEASE_DURATION_SECONDS) == _run_job_lease(_JOB_LEASE_DURATION_SECONDS)
