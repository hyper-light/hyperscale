"""
The VOPR: seed-driven random fault schedules over real job traffic,
every one invariant-checked and byte-identically replayable.

Phase 6's exit ramp: one integer expands to a full fault schedule
(kills, partitions, loss, delay, duplication at seeded virtual times)
against the production manager/worker/client stack; the run must reach
a client-observed terminal outcome and replay exactly. A failing seed
IS the bug report — ``pytest tests/simulation/vopr --sim-replay=<seed>``
reproduces it forever.
"""

import time as wall_time

import pytest

from .fault_plan import generate_fault_plan
from .vopr_runner import check_invariants, run_fault_plan

_DEFAULT_SWEEP_SEEDS = (101, 102, 103, 104)


def _sweep_seeds(request) -> tuple[int, ...]:
    count = request.config.getoption("--sim-vopr-count")
    if count is None:
        return _DEFAULT_SWEEP_SEEDS
    return tuple(range(101, 101 + count))


def test_generated_fault_schedules_hold_invariants_and_replay(request):
    """Sweep the default seed corpus: every generated schedule must
    satisfy the invariants AND replay byte-identically. Also measures
    and reports the sweep's scenarios-per-minute rate (each scenario =
    two full multi-process cluster runs: judge + replay)."""
    seeds = _sweep_seeds(request)
    sweep_started = wall_time.monotonic()

    for seed in seeds:
        plan = generate_fault_plan(seed)
        first_results = run_fault_plan(plan)

        violations = check_invariants(plan, first_results)
        assert not violations, (
            f"seed {seed} violated invariants (replay with "
            f"--sim-replay={seed}):\n  plan={plan.events}\n  "
            + "\n  ".join(violations)
        )

        second_results = run_fault_plan(plan)
        assert first_results == second_results, (
            f"seed {seed} did not replay byte-identically "
            f"(--sim-replay={seed}): plan={plan.events}"
        )

    elapsed = wall_time.monotonic() - sweep_started
    scenarios_per_minute = len(seeds) / (elapsed / 60.0)
    print(
        f"\nVOPR sweep: {len(seeds)} schedules (x2 runs each) in "
        f"{elapsed:.1f}s = {scenarios_per_minute:.1f} scenarios/minute "
        "at full multi-process production fidelity"
    )


def test_plan_generation_is_deterministic_and_covers_fault_kinds():
    """The generator itself is stable: same seed -> same plan, and the
    seed space actually exercises every fault kind plus the fault-free
    baseline (guarding against a generator regression that quietly
    narrows coverage)."""
    for seed in range(200, 260):
        assert generate_fault_plan(seed) == generate_fault_plan(seed)

    kinds_seen = set()
    fault_free_seen = False
    for seed in range(200, 260):
        plan = generate_fault_plan(seed)
        if not plan.events:
            fault_free_seen = True
        kinds_seen.update(event[0] for event in plan.events)

    assert kinds_seen == {"kill", "partition", "drop", "delay", "duplicate"}
    assert fault_free_seen


def test_sim_replay(request):
    """Entry point for ``--sim-replay=<seed>``: expand, print, run
    twice, judge. Skips when the option is absent (the sweep is the
    default coverage)."""
    seed = request.config.getoption("--sim-replay")
    if seed is None:
        pytest.skip("no --sim-replay seed given")

    plan = generate_fault_plan(seed)
    print(f"\nVOPR replay seed={seed} ceiling={plan.ceiling}")
    for event in plan.events or [("no-faults",)]:
        print(f"  {event}")

    first_results = run_fault_plan(plan)
    violations = check_invariants(plan, first_results)

    print("client log:")
    for entry in first_results.get("client") or []:
        print(f"  {entry}")

    assert not violations, "\n".join(violations)
    assert first_results == run_fault_plan(plan), "replay diverged"
