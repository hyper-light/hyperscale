"""
AD-39 majority clock fencing, end to end over the multi-process
coordinator (``clock_fence_demo``), in both tiers: three peered managers
(client submitting to the manager tier), and three peered gates fronting
one datacenter (client submitting through gate-b). One node's wall
clock is stepped 2s fast (4x the 500ms offset bound), later stepped back.

Pinned:

* The skewed manager fences within one probe round of the step, and only
  it does: each healthy manager sees one peer of three beyond the bound,
  short of a quorum, so a fast clock never fences the healthy majority.
* While fenced it holds no leadership -- not the datacenter (SWIM)
  leadership, not any per-job Raft group -- and does not accept jobs.
  A sitting leader that is fenced steps down at once and a healthy
  manager takes over.
* The job completes while a manager is fenced. A client whose only
  gate is fenced is refused (transiently) until it unfences, then its
  job is accepted and completes.
* It unfences within one probe round of its clock agreeing again (plus,
  in general, however long its HLC still led its clock -- the timestamps
  it minted while fast).
* Replay-deterministic.
"""

import functools

import pytest

from hyperscale.distributed.env import Env
from tests.simulation.harness.sim.multiprocess.clock_fence_demo import (
    WATCH_INTERVAL_SECONDS,
    run_clock_fence_scenario,
    run_gate_clock_fence_scenario,
)
from tests.simulation.harness.sim.multiprocess.gate_ledger_demo import GATE_HOSTS

_SKEW_SECONDS = 2.0
_PROBE_INTERVAL_SECONDS = Env().HLC_OFFSET_PROBE_INTERVAL_SECONDS
_MAX_OFFSET_SECONDS = Env().HLC_MAX_CLOCK_OFFSET_MS / 1000
# Coordinator link latency is 0.01s each way: one probe round trip.
_PROBE_ROUND_TRIP_SECONDS = 0.02
_DETECTION_BOUND_SECONDS = _PROBE_INTERVAL_SECONDS + _PROBE_ROUND_TRIP_SECONDS + WATCH_INTERVAL_SECONDS
_UNFENCE_BOUND_SECONDS = max(0.0, _SKEW_SECONDS - _MAX_OFFSET_SECONDS) + _DETECTION_BOUND_SECONDS
_RESTORE_AT = 40.0
_CEILING = 60.0
_MANAGERS = ("sim-mgr-a", "sim-mgr-b", "sim-mgr-c")
_SKEWED = "sim-mgr-c"
# The mid-run schedule skews whichever manager leads the datacenter over
# the second before the skew -- read from the fault-free twin, never
# hardcoded (which manager wins the election is a function of the seed
# and of every change that shifts the deterministic schedule).
_MID_RUN_SKEW_AT = 20.0
_PRE_SKEW_WINDOW = (_MID_RUN_SKEW_AT - 1.0, _MID_RUN_SKEW_AT)
_SCENARIO_SEED = 23
# Seeds whose fault-free twins elect each of the three managers (probed:
# 23 -> sim-mgr-b, 2 -> sim-mgr-c, 6 -> sim-mgr-a), so the sweep fences
# every manager as the sitting leader.
_LEADER_FENCE_SEEDS = (_SCENARIO_SEED, 1, 2, 3, 4, 5, 6, 7)
_SKEWED_GATE = GATE_HOSTS[1]  # the client's only gate
_GATE_RESTORE_AT = 20.0
_GATE_CEILING = 50.0


def _transitions(log: list, tag: str) -> list[tuple[object, float]]:
    return [(entry[1], entry[2]) for entry in log if entry[0] == tag]


def _windows(log: list, tag: str, value: object) -> list[tuple[float, float]]:
    """The ``[start, end)`` spans during which ``tag`` held ``value``."""
    spans: list[tuple[float, float]] = []
    started_at: float | None = None
    for current, at_time in _transitions(log, tag):
        if current == value and started_at is None:
            started_at = at_time
        elif current != value and started_at is not None:
            spans.append((started_at, at_time))
            started_at = None
    if started_at is not None:
        spans.append((started_at, float("inf")))
    return spans


def _overlaps(spans: list[tuple[float, float]], window: tuple[float, float]) -> bool:
    return any(start < window[1] and window[0] < end for start, end in spans)


def _assert_fenced_node_holds_nothing(log: list, leader_tag: str = "dc-leader") -> None:
    for fenced_window in _windows(log, "fenced", True):
        assert not _overlaps(_windows(log, leader_tag, True), fenced_window), log
        assert all(
            not _overlaps(_windows(log, "raft-leading", count), fenced_window)
            for count, _ in _transitions(log, "raft-leading")
            if count
        ), log
        if _transitions(log, "accepting"):
            assert not _overlaps(_windows(log, "accepting", True), fenced_window), log


def _assert_only_the_skewed_node_fences(results: dict, nodes: tuple[str, ...], skewed: str) -> None:
    for node in nodes:
        fenced_ever = any(value for value, _ in _transitions(results[node], "fenced"))
        assert fenced_ever == (node == skewed), (node, results[node])


def _assert_unfenced_within_bound(log: list, restore_at: float = _RESTORE_AT) -> None:
    ((fenced_at, unfenced_at),) = _windows(log, "fenced", True)
    assert restore_at < unfenced_at <= restore_at + _UNFENCE_BOUND_SECONDS, (fenced_at, unfenced_at)


def _assert_job_completed(client_log: list) -> None:
    (finished,) = [entry for entry in client_log if entry[0] == "job-finished"]
    assert finished[1] == "completed", client_log


def _run_fenced_from_boot() -> dict:
    return run_clock_fence_scenario(
        _CEILING, _SKEWED, [("wall_skew", 0.0, _SKEW_SECONDS), ("wall_skew", _RESTORE_AT, 0.0)]
    )


@functools.cache
def _leader_before_mid_run_skew(seed: int) -> str:
    """The manager leading the datacenter over the second before the
    mid-run skew in the scenario's fault-free twin (same seed, topology
    and ceiling, no skew). A skew step cannot change anything before its
    own instant, so the twin IS the faulted run up to the skew; the
    faulted run re-asserts the premise from its own rows."""
    twin = run_clock_fence_scenario(_CEILING, _SKEWED, [], seed=seed)
    pre_skew_leaders = [
        manager
        for manager in _MANAGERS
        if _overlaps(_windows(twin[manager], "dc-leader", True), _PRE_SKEW_WINDOW)
    ]
    assert len(pre_skew_leaders) == 1, ("the fault-free twin must have one sitting leader before the skew", twin)
    return pre_skew_leaders[0]


def _run_leader_fenced_mid_run(seed: int = _SCENARIO_SEED) -> tuple[dict, str]:
    """Skew the sitting datacenter leader mid-run; returns the run's
    results and the skewed manager."""
    skewed_manager = _leader_before_mid_run_skew(seed)
    results = run_clock_fence_scenario(
        _CEILING,
        skewed_manager,
        [("wall_skew", _MID_RUN_SKEW_AT, _SKEW_SECONDS), ("wall_skew", _RESTORE_AT, 0.0)],
        seed=seed,
    )
    return results, skewed_manager


def test_a_manager_fast_from_boot_fences_and_the_job_completes_without_it():
    results = _run_fenced_from_boot()
    skewed_log = results[_SKEWED]

    (started,) = [entry[1] for entry in skewed_log if entry[0] == "manager-started"]
    ((fenced_at, _),) = _windows(skewed_log, "fenced", True)
    assert fenced_at <= started + _DETECTION_BOUND_SECONDS, skewed_log

    _assert_only_the_skewed_node_fences(results, _MANAGERS, _SKEWED)
    _assert_fenced_node_holds_nothing(skewed_log)
    _assert_unfenced_within_bound(skewed_log)
    _assert_job_completed(results["client"])


@pytest.mark.parametrize("seed", _LEADER_FENCE_SEEDS)
def test_a_fenced_leader_steps_down_and_a_healthy_manager_takes_over(seed: int):
    results, skewed_manager = _run_leader_fenced_mid_run(seed)
    skewed_log = results[skewed_manager]

    assert _overlaps(_windows(skewed_log, "dc-leader", True), _PRE_SKEW_WINDOW), (
        "precondition: the skewed manager leads before the skew",
        skewed_log,
    )
    ((fenced_at, unfenced_at),) = _windows(skewed_log, "fenced", True)
    assert _MID_RUN_SKEW_AT < fenced_at <= _MID_RUN_SKEW_AT + _DETECTION_BOUND_SECONDS, skewed_log

    _assert_only_the_skewed_node_fences(results, _MANAGERS, skewed_manager)
    _assert_fenced_node_holds_nothing(skewed_log)
    _assert_unfenced_within_bound(skewed_log)

    successors = [
        manager
        for manager in _MANAGERS
        if manager != skewed_manager and _overlaps(_windows(results[manager], "dc-leader", True), (fenced_at, unfenced_at))
    ]
    assert len(successors) == 1, results


def test_a_fenced_gate_refuses_jobs_until_it_unfences():
    results = run_gate_clock_fence_scenario(
        _GATE_CEILING, _SKEWED_GATE, [("wall_skew", 0.0, _SKEW_SECONDS), ("wall_skew", _GATE_RESTORE_AT, 0.0)]
    )
    gate_log = results[_SKEWED_GATE]

    (started,) = [entry[1] for entry in gate_log if entry[0] == "gate-started"]
    ((fenced_at, unfenced_at),) = _windows(gate_log, "fenced", True)
    assert fenced_at <= started + _DETECTION_BOUND_SECONDS, gate_log
    _assert_only_the_skewed_node_fences(results, GATE_HOSTS, _SKEWED_GATE)
    _assert_fenced_node_holds_nothing(gate_log, leader_tag="gate-leader")
    _assert_unfenced_within_bound(gate_log, restore_at=_GATE_RESTORE_AT)

    client_log = results["client"]
    (submitted_at,) = [entry[1] for entry in client_log if entry[0] == "job-submitted"]
    assert submitted_at > unfenced_at, client_log
    _assert_job_completed(client_log)


def test_clock_fencing_is_replay_deterministic():
    assert _run_leader_fenced_mid_run() == _run_leader_fenced_mid_run()
