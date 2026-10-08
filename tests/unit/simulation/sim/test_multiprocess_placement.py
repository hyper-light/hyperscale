"""
Policy-driven placement under multi-process SIM (D-62): a gate fronting two
datacenters, each one manager and one four-core worker (``job_control_demo``).

dc-slow's worker answers every workflow dispatch half a second late, so its
D-5 dispatch latency digest runs slow; dc-fast's answers at the network's
pace. Before any job goes through the gate, one gateless job warms each
datacenter's digest: a ping job in dc-slow, and in dc-fast a long job that
keeps holding half its cores. AD-36 alone therefore prefers dc-slow -- idle,
equally near -- and both datacenters meet the cluster's latency SLO (its
targets are set above either), so health never steers the job.

The instant the gate's routing view grades both digests and sees dc-fast's
load, a client submits a ping job through the gate under a dispatch latency
budget:

* Within reach: a budget dc-fast meets and dc-slow exceeds. The job is
  placed in dc-fast alone -- dc-slow is excluded, not even a fallback -- and
  completes there.
* Out of reach: a budget below any dispatch round trip (one network
  latency; a round trip takes two). No datacenter meets it, so the policy
  falls back: the job is placed nearest to the budget -- dc-fast, AD-36's
  second choice -- with dc-slow its fallback, and completes there.
"""

import pytest

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_control_demo import (
    job_control_client_entry,
    job_control_gate_entry,
    job_control_manager_entry,
    job_control_worker_entry,
)

_SEED = 62
_LATENCY = 0.01
_CEILING = 90.0
_FAST = "dc-fast"
_SLOW = "dc-slow"
_DATACENTERS = (_FAST, _SLOW)
_GATE_ADDRESS = ("sim-gate", 9000)
_GATE_UDP_ADDRESS = ("sim-gate", 9001)
_WORKER_CORES = 4
_SLOW_DISPATCH_ANSWER_DELAY_SECONDS = 0.5
_MILLISECONDS_PER_SECOND = 1000.0
# Halfway to dc-slow's delay: dc-fast's round trips (two network
# latencies) are well within it, dc-slow's (the delay and more) over it.
_REACHABLE_BUDGET_MS = _SLOW_DISPATCH_ANSWER_DELAY_SECONDS * _MILLISECONDS_PER_SECOND / 2
# One network latency: shorter than any round trip.
_UNREACHABLE_BUDGET_MS = _LATENCY * _MILLISECONDS_PER_SECOND
# The gate grades a digest from its first sample (the warm-up gives each
# datacenter one); latency SLO targets far above dc-slow's delay keep
# health classification out of the decision.
_LENIENT_TARGET_MS = _SLOW_DISPATCH_ANSWER_DELAY_SECONDS * _MILLISECONDS_PER_SECOND * 100
_GATE_ENV = {
    "SLO_MIN_SAMPLE_COUNT": 1,
    "SLO_P50_TARGET_MS": _LENIENT_TARGET_MS,
    "SLO_P95_TARGET_MS": _LENIENT_TARGET_MS,
    "SLO_P99_TARGET_MS": _LENIENT_TARGET_MS,
}
_WARM_UP_TIMEOUT_SECONDS = 120.0
_GATED_JOB_TIMEOUT_SECONDS = 30.0
_PING_CLASS = "SimPingWorkflow"


def _rows(log: list, tag: str) -> list[tuple]:
    return [entry for entry in log if entry[0] == tag]


def _manager_host(datacenter: str) -> str:
    return f"sim-mgr-{datacenter}"


def _add_datacenter(coordinator: SimulationCoordinator, datacenter: str, answer_delay_seconds: float) -> None:
    coordinator.add_process(
        f"manager-{datacenter}",
        job_control_manager_entry,
        _manager_host(datacenter),
        9000,
        9001,
        datacenter,
        {},
        [_GATE_ADDRESS],
        [_GATE_UDP_ADDRESS],
    )
    coordinator.add_process(
        f"worker-{datacenter}",
        job_control_worker_entry,
        f"sim-wkr-{datacenter}",
        9010,
        9011,
        datacenter,
        [(_manager_host(datacenter), 9000)],
        _WORKER_CORES,
        0,
        answer_delay_seconds,
    )


def _add_warm_up(coordinator: SimulationCoordinator, datacenter: str, workflow_kind: str) -> None:
    coordinator.add_process(
        f"warm-up-{datacenter}",
        job_control_client_entry,
        f"sim-warm-{datacenter}",
        9500,
        [(_manager_host(datacenter), 9000)],
        workflow_kind,
        1,
        _WARM_UP_TIMEOUT_SECONDS,
        _CEILING,
    )


def _routing_view_is_warm(row: tuple) -> bool:
    """The gate grades both digests and sees dc-fast's warm-up holding cores."""
    if row[0] != "datacenter-view":
        return False
    view = {datacenter: (p95_ms, available_cores) for datacenter, p95_ms, available_cores in row[1]}
    return all(p95_ms is not None for p95_ms, _cores in view.values()) and view[_FAST][1] < _WORKER_CORES


def _run_placement(budget_ms: float, seed: int = _SEED) -> dict:
    coordinator = SimulationCoordinator(latency=_LATENCY, max_virtual_time=_CEILING, seed=seed)
    coordinator.add_process(
        "gate",
        job_control_gate_entry,
        "sim-gate",
        9000,
        9001,
        {datacenter: [(_manager_host(datacenter), 9000)] for datacenter in _DATACENTERS},
        {datacenter: [(_manager_host(datacenter), 9001)] for datacenter in _DATACENTERS},
        _GATE_ENV,
    )
    _add_datacenter(coordinator, _FAST, 0.0)
    _add_datacenter(coordinator, _SLOW, _SLOW_DISPATCH_ANSWER_DELAY_SECONDS)
    _add_warm_up(coordinator, _FAST, "long")
    _add_warm_up(coordinator, _SLOW, "ping")
    submitted: list[float] = []

    def submit_through_gate(view_row: tuple) -> None:
        if submitted:
            return
        submitted.append(view_row[-1])
        coordinator.schedule_admission(
            "client-gated",
            view_row[-1] + _LATENCY,
            job_control_client_entry,
            "sim-cli-g",
            9500,
            [_GATE_ADDRESS],
            "ping",
            1,
            _GATED_JOB_TIMEOUT_SECONDS,
            _CEILING,
            "gate",
            budget_ms,
        )

    coordinator.schedule_on_event("gate", _routing_view_is_warm, submit_through_gate)
    return coordinator.run()


def _warm_view_row(results: dict) -> tuple:
    """The gate's routing view the gated job was submitted at."""
    return next(row for row in results["gate"] if _routing_view_is_warm(row))


def _gated_job_admissions(results: dict) -> list[str]:
    """The datacenters whose manager admitted a ping job after the gated
    job was submitted -- dc-slow's warm-up ping came before."""
    submitted_at = _warm_view_row(results)[-1]
    return [
        datacenter
        for datacenter in _DATACENTERS
        for _tag, job_class, at_time in _rows(results[f"manager-{datacenter}"], "admission-admitted")
        if job_class == _PING_CLASS and at_time > submitted_at
    ]


def _assert_warm_view(results: dict) -> None:
    """The view the gated job was routed by: dc-slow's digest over dc-fast's."""
    warm_view = _warm_view_row(results)
    p95_by_datacenter = {datacenter: p95_ms for datacenter, p95_ms, _cores in warm_view[1]}
    assert p95_by_datacenter[_SLOW] >= _SLOW_DISPATCH_ANSWER_DELAY_SECONDS * _MILLISECONDS_PER_SECOND, warm_view
    assert p95_by_datacenter[_FAST] < _REACHABLE_BUDGET_MS, warm_view
    assert p95_by_datacenter[_FAST] > _UNREACHABLE_BUDGET_MS, warm_view


_SCENARIOS = {
    # budget, the submission's (primaries, fallbacks, excluded, relaxed)
    "within-reach": (_REACHABLE_BUDGET_MS, ((_FAST,), (), (_SLOW,), False)),
    "out-of-reach": (_UNREACHABLE_BUDGET_MS, ((_FAST,), (_SLOW,), (), True)),
}


@pytest.mark.parametrize("scenario", sorted(_SCENARIOS))
def test_a_latency_budget_places_the_job_by_the_datacenters_dispatch_digests(scenario: str):
    budget_ms, expected_submission_decision = _SCENARIOS[scenario]
    expected_primaries = expected_submission_decision[0]
    results = _run_placement(budget_ms)
    _assert_warm_view(results)

    # Every routing of the gated job -- at submission, then at dispatch
    # within the datacenters it was accepted for -- chose dc-fast, over
    # AD-36's own preference for the idle dc-slow. Within reach dc-slow was
    # excluded outright; out of reach the budget was relaxed and dc-slow
    # stayed the fallback.
    decisions = [row[1:-1] for row in _rows(results["gate"], "route-decision")]
    assert decisions, results["gate"]
    assert all(primaries == expected_primaries for primaries, *_rest in decisions), decisions
    assert decisions[0] == expected_submission_decision, decisions

    # It ran in dc-fast alone and completed.
    assert _gated_job_admissions(results) == [_FAST], {
        datacenter: _rows(results[f"manager-{datacenter}"], "admission-admitted") for datacenter in _DATACENTERS
    }
    finished = [(ordinal, status) for _tag, ordinal, status, _at in _rows(results["client-gated"], "job-finished")]
    assert finished == [(0, "completed")], results["client-gated"]


def test_placement_scenario_is_replay_deterministic():
    assert _run_placement(_REACHABLE_BUDGET_MS) == _run_placement(_REACHABLE_BUDGET_MS)
