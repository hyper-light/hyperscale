"""
The multi-DC VOPR: seed-driven random fault schedules over real
cross-datacenter job traffic, every one invariant-checked and
byte-identically replayable.

One integer expands to a full fault schedule (total-DC loss, gate<->DC
partitions, manager power-loss restarts, per-DC storage faults, gate
link loss, client-link cuts/delay/duplication at seeded virtual times)
against the production gate/manager/worker/client stack in the L3
two-datacenter topology; the run must reach a client-observed terminal
outcome, keep placement exactly-once, converge classification, and
replay exactly. A failing seed IS the bug report —
``pytest tests/simulation/vopr_mdc --sim-replay=<seed>`` reproduces it
forever (the option lives in ``tests/simulation/conftest.py``, shared
by every VOPR-style suite).
"""

import time as wall_time

import pytest

from .fault_plan import generate_mdc_fault_plan
from .vopr_runner import check_mdc_invariants, run_mdc_fault_plan

# Default sweep corpus (each seed = two full 10-process cluster runs:
# judge + replay twin). Chosen from the generator's space for coverage
# density, then probe-validated:
#
# * 302 — overlapping dc_loss(dc-west) + dc_partition(dc-east) + two
#   client_duplicate windows: a total DC loss racing a cut to the
#   OTHER DC (the loud-timeout strand path, both DCs faulted at once)
# * 303 — fault-free baseline: same invariants, completion required
# * 308 — manager_restart(dc-west): power loss + durable resume inside
#   one DC of the L3 pair
# * 312 — client_partition over the submission window: production
#   retry-to-acceptance through a cut client link
_DEFAULT_SWEEP_SEEDS = (302, 303, 308, 312)


def _sweep_seeds(request) -> tuple[int, ...]:
    count = request.config.getoption("--sim-vopr-count")
    if count is None:
        return _DEFAULT_SWEEP_SEEDS
    return tuple(range(301, 301 + count))


def test_generated_mdc_fault_schedules_hold_invariants_and_replay(request):
    """Sweep the default seed corpus: every generated multi-DC schedule
    must satisfy the invariants AND replay byte-identically. Also
    reports the sweep's scenarios-per-minute rate (each scenario = two
    full 10-process cluster runs: judge + replay)."""
    seeds = _sweep_seeds(request)
    sweep_started = wall_time.monotonic()

    for seed in seeds:
        plan = generate_mdc_fault_plan(seed)
        first_results = run_mdc_fault_plan(plan)

        violations = check_mdc_invariants(plan, first_results)
        assert not violations, (
            f"seed {seed} violated invariants (replay with "
            f"--sim-replay={seed}):\n  plan={plan.events}\n  "
            + "\n  ".join(violations)
        )

        second_results = run_mdc_fault_plan(plan)
        assert first_results == second_results, (
            f"seed {seed} did not replay byte-identically "
            f"(--sim-replay={seed}): plan={plan.events}"
        )

    elapsed = wall_time.monotonic() - sweep_started
    scenarios_per_minute = len(seeds) / (elapsed / 60.0)
    print(
        f"\nmulti-DC VOPR sweep: {len(seeds)} schedules (x2 runs each) in "
        f"{elapsed:.1f}s = {scenarios_per_minute:.1f} scenarios/minute "
        "at full multi-process production fidelity"
    )


def test_mdc_plan_generation_is_deterministic_and_covers_fault_kinds():
    """The generator itself is stable: same seed -> same plan, and the
    seed space actually exercises every fault kind plus the fault-free
    baseline (guarding against a generator regression that quietly
    narrows coverage). Structural constraints hold across the space:
    at most one total loss, kills never mixed with restarts, storage
    and restart never on the same DC."""
    for seed in range(300, 400):
        assert generate_mdc_fault_plan(seed) == generate_mdc_fault_plan(seed)

    kinds_seen: set[str] = set()
    fault_free_seen = False
    for seed in range(300, 400):
        plan = generate_mdc_fault_plan(seed)
        if not plan.events:
            fault_free_seen = True
        kinds_seen.update(event[0] for event in plan.events)

        kills = [event for event in plan.events if event[0] == "dc_loss"]
        restarts = [
            event for event in plan.events if event[0] == "manager_restart"
        ]
        assert len(kills) <= 1, plan.events
        assert not (kills and restarts), plan.events
        storage_dcs = {
            event[1]
            for event in plan.events
            if event[0] in ("slow_disk", "disk_full")
        }
        assert not storage_dcs.intersection(
            event[1] for event in restarts
        ), plan.events

    assert kinds_seen == {
        "dc_loss",
        "dc_partition",
        "manager_restart",
        "slow_disk",
        "disk_full",
        "gate_link_drop",
        "client_partition",
        "client_delay",
        "client_duplicate",
    }
    assert fault_free_seen


def test_sim_replay(request):
    """Entry point for ``--sim-replay=<seed>``: expand, print, run
    twice, judge. Skips when the option is absent (the sweep is the
    default coverage)."""
    seed = request.config.getoption("--sim-replay")
    if seed is None:
        pytest.skip("no --sim-replay seed given")

    plan = generate_mdc_fault_plan(seed)
    print(f"\nmulti-DC VOPR replay seed={seed} ceiling={plan.ceiling}")
    for event in plan.events or [("no-faults",)]:
        print(f"  {event}")

    first_results = run_mdc_fault_plan(plan)
    violations = check_mdc_invariants(plan, first_results)

    print("client log:")
    for entry in first_results.get("client-a") or []:
        print(f"  {entry}")
    print("gate dc-health log:")
    for entry in first_results.get("sim-gate-a") or []:
        if entry[0] == "dc-health":
            print(f"  {entry}")

    assert not violations, "\n".join(violations)
    assert first_results == run_mdc_fault_plan(plan), "replay diverged"
