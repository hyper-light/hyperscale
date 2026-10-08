"""
The F1 long-horizon SOAK: seed-driven endurance scenarios — an occupied
1800-virtual-second horizon of sequential jobs, recurring mild fault
windows, and (half the seed space) a mid-horizon host kill with
late-join recovery — every horizon invariant-checked end to end and
byte-identically replayed as a whole.

OPT-IN: one scenario is two full multi-process cluster runs of an
1800s horizon (~9-11 wall minutes total at the probed 0.13-0.17
wall-seconds per virtual second), so the heavy tests skip unless the
environment opts in:

    HYPERSCALE_SIM_SOAK=1 uv run pytest tests/simulation/soak

The env-var gate (not a pytest option) is deliberate: the shared
``tests/simulation/conftest.py`` owns every suite-wide option and no
suite adds its own conftest. ``--sim-vopr-count=N`` (the shared knob)
widens the sweep to seeds 901..900+N — budget ~9-11 minutes per seed.
``--sim-replay=<seed>`` reproduces one horizon forever (explicitly
passing the flag IS the opt-in for that path). The generator-stability
test runs ungated — it is pure plan expansion, no cluster.
"""

import os
import time as wall_time

import pytest

from .soak_plan import (
    _CEILING,
    _SEGMENT_SECONDS,
    _TAIL_BUDGET_SECONDS,
    generate_soak_plan,
)
from .soak_runner import check_soak_invariants, run_soak_plan

_SOAK_OPTED_IN = os.environ.get("HYPERSCALE_SIM_SOAK") == "1"

_HEAVY = pytest.mark.skipif(
    not _SOAK_OPTED_IN,
    reason=(
        "long-horizon soak is opt-in: set HYPERSCALE_SIM_SOAK=1 "
        "(one scenario = two ~1800-virtual-second cluster runs, "
        "~9-11 wall minutes)"
    ),
)

# Default sweep corpus: ONE curated seed (judge + replay twin of a
# full horizon is the wall budget for the default run). Seed 901 is
# the mild-window endurance horizon at the default ceiling: 37
# sequential jobs and 21 drop/delay/duplicate/slow_disk windows across
# 1800 virtual seconds, every job required to COMPLETE. Seed 903 is
# the fault-free occupied baseline. The kill-bearing seed 902 is
# skip-pinned below as this suite's FIRST MEASURED CATCH (post-kill
# dispatch starvation) — swarm sweeps via ``--sim-vopr-count`` still
# reach kill seeds and will log them as failing reproducers until the
# production fix lands (budget ~9-11 wall minutes PER SEED).
_DEFAULT_SWEEP_SEEDS = (901,)


def _sweep_seeds(request) -> tuple[int, ...]:
    count = request.config.getoption("--sim-vopr-count")
    if count is None:
        return _DEFAULT_SWEEP_SEEDS
    return tuple(range(901, 901 + count))


@_HEAVY
def test_soak_horizons_hold_invariants_and_replay(request):
    """Sweep the soak corpus: every generated horizon must satisfy the
    per-job and horizon invariants AND replay byte-identically as a
    WHOLE (the twin run doubles the wall cost — budgeted). Reports the
    sweep's occupied-virtual-seconds-per-wall-minute rate."""
    seeds = _sweep_seeds(request)
    sweep_started = wall_time.monotonic()

    for seed in seeds:
        plan = generate_soak_plan(seed)
        first_results = run_soak_plan(plan)

        violations = check_soak_invariants(plan, first_results)
        assert not violations, (
            f"seed {seed} violated soak invariants (replay with "
            f"--sim-replay={seed}):\n  jobs={len(plan.submit_times)} "
            f"events={plan.events}\n  " + "\n  ".join(violations)
        )

        second_results = run_soak_plan(plan)
        assert first_results == second_results, (
            f"seed {seed} did not replay byte-identically "
            f"(--sim-replay={seed}): events={plan.events}"
        )

    elapsed = wall_time.monotonic() - sweep_started
    virtual_covered = sum(generate_soak_plan(seed).ceiling for seed in seeds) * 2
    print(
        f"\nsoak sweep: {len(seeds)} horizon(s) (x2 runs each) in "
        f"{elapsed:.1f}s = {virtual_covered / (elapsed / 60.0):.0f} "
        "occupied virtual seconds per wall minute at full multi-process "
        "production fidelity"
    )


@_HEAVY
def test_kill_horizon_completes_every_job():
    """FIXED-BUG PIN — this suite's first long-horizon catch, now the
    design bar it always aspired to: a kill-bearing horizon with a
    late-joining replacement COMPLETES every job.

    The original catch (seed 902 @ 1800): jobs 1-16 completed, then
    job 17 and every later job was accepted and starved to a loud
    manager timeout against an idle, healthy replacement worker. Root
    cause was the SWIM detector-death chain (the AD-53 burst batch
    cancelled the main probe cycle's shared ack future; the cycle read
    the stray cancel as shutdown and exited PERMANENTLY, killing the
    worker-heartbeat carrier; WorkerPool liveness staled to EVICT and
    allocation starved) — fixed by the shared+shielded ack futures and
    genuine-cancel discrimination. Post-fix (verified via the
    truncate-1000 onset replay before this test was un-skipped): jobs
    17-22 complete on the replacement with zero violations; this test
    pins the full horizon judge + byte-identical twin. Onset triage:
    uv run python -m tests.simulation.soak.soak_runner --seed 902
    --truncate-ceiling 1000 --print-manager-log --print-worker-logs."""
    plan = generate_soak_plan(902)
    first_results = run_soak_plan(plan)
    violations = check_soak_invariants(plan, first_results)
    assert not violations, "\n".join(violations)
    assert first_results == run_soak_plan(plan), "replay diverged"


def test_soak_plan_generation_is_deterministic_and_covers_structure():
    """The generator itself is stable and structurally sound across the
    seed space: same seed -> same plan; the space exercises every event
    kind, kill and kill-free horizons, and the fault-free baseline; and
    every plan obeys the calibration constraints the invariants rely on
    (sequential submit schedule with tail budget, one kill in the
    middle band with a post-kill job, disjoint same-kind same-link
    windows). Pure plan expansion — runs ungated."""
    for seed in range(900, 940):
        assert generate_soak_plan(seed) == generate_soak_plan(seed)

    kinds_seen: set[str] = set()
    kill_seen = False
    kill_free_seen = False
    fault_free_seen = False

    for seed in range(900, 940):
        plan = generate_soak_plan(seed)
        kinds_seen.update(event[0] for event in plan.events)
        if not plan.events:
            fault_free_seen = True

        assert len(plan.submit_times) == len(plan.durations), plan
        previous_submit = None
        for submit_time in plan.submit_times:
            if previous_submit is not None:
                gap = submit_time - previous_submit
                assert 30.0 <= gap <= 60.001, plan.submit_times
            previous_submit = submit_time
        assert plan.submit_times, "an occupied horizon must schedule jobs"
        assert (
            plan.submit_times[-1] <= plan.ceiling - _TAIL_BUDGET_SECONDS
        ), plan.submit_times
        assert all(
            8.0 <= duration <= 20.0 for duration in plan.durations
        ), plan.durations

        kills = [event for event in plan.events if event[0] == "host_kill"]
        assert len(kills) <= 1, plan.events
        if kills:
            kill_seen = True
            _tag, kill_at, worker_b_start = kills[0]
            assert (
                0.35 * plan.ceiling <= kill_at <= 0.55 * plan.ceiling
            ), kills
            assert 10.0 <= worker_b_start - kill_at <= 20.0, kills
            # The invariants demand the replacement worker executes
            # work: the default horizon always schedules jobs past it.
            assert any(
                submit_time > worker_b_start
                for submit_time in plan.submit_times
            ), plan
        else:
            kill_free_seen = True

        windows_by_key: dict[tuple, list[tuple[float, float]]] = {}
        for event in plan.events:
            if event[0] in ("drop", "duplicate"):
                key = (event[0], event[1], event[2])
                windows_by_key.setdefault(key, []).append(
                    (event[4], event[5])
                )
            elif event[0] == "delay":
                windows_by_key.setdefault(
                    ("delay", event[1], event[2]), []
                ).append((event[5], event[6]))
            elif event[0] == "slow_disk":
                windows_by_key.setdefault(("slow_disk",), []).append(
                    (event[1], event[3])
                )
        for key, windows in windows_by_key.items():
            ordered = sorted(windows)
            for (_, first_end), (second_start, _) in zip(
                ordered, ordered[1:]
            ):
                assert second_start >= first_end, (key, ordered)
            for window_start, window_end in ordered:
                assert 0.0 <= window_start < window_end <= plan.ceiling, (
                    key,
                    ordered,
                )

    assert kinds_seen == {
        "host_kill",
        "drop",
        "delay",
        "duplicate",
        "slow_disk",
    }
    assert kill_seen and kill_free_seen and fault_free_seen

    # Segment containment is the disjointness mechanism — pin the
    # constant it relies on so a drive-by retune cannot silently break
    # the first-match-wins reasoning.
    assert _SEGMENT_SECONDS == 120.0
    assert _CEILING >= 1000.0


def test_sim_replay(request):
    """Entry point for ``--sim-replay=<seed>``: expand, print, run
    twice, judge. Skips when the option is absent (passing the flag is
    the opt-in for this path — no env gate)."""
    seed = request.config.getoption("--sim-replay")
    if seed is None:
        pytest.skip("no --sim-replay seed given")

    plan = generate_soak_plan(seed)
    print(
        f"\nsoak replay seed={seed} ceiling={plan.ceiling} "
        f"jobs={len(plan.submit_times)}"
    )
    for event in plan.events or [("no-faults",)]:
        print(f"  {event}")

    first_results = run_soak_plan(plan)
    violations = check_soak_invariants(plan, first_results)

    print("client log:")
    for entry in first_results.get("client") or []:
        print(f"  {entry}")
    print("manager worker-count log:")
    for entry in first_results.get("manager") or []:
        if entry[0] == "worker-count":
            print(f"  {entry}")

    assert not violations, "\n".join(violations)
    assert first_results == run_soak_plan(plan), "replay diverged"
