"""
Seed-driven LONG-HORIZON scenario generation (the F1 soak input side).

``generate_soak_plan(seed)`` expands one integer into a complete,
self-describing endurance scenario over the GATELESS L2 topology
(manager + 2-core worker + one multi-job client): a horizon-long job
schedule (submissions every 30-60 virtual seconds, seeded per-job
durations) plus recurring MILD fault windows and at most one
mid-horizon host kill with a late-joining replacement worker. The SAME
seed always yields the SAME plan, and the plan drives a coordinator run
that replays byte-identically — a failing seed is a permanent
reproducer (``pytest tests/simulation/soak --sim-replay=<seed>``, the
flag owned by ``tests/simulation/conftest.py``).

Why GATELESS: in gate topologies second/late jobs fail BY DESIGN today
(the pinned dispatch gaps of commit c8b6cf99), so a multi-job soak
through a gate would assert known bugs instead of endurance. The L2
manager path completes repeated jobs; that is the surface where long
horizons can catch slow leaks, repeated detection/rejoin cycles, and
WAL growth.

Fault menu (every window calibrated survivable, so COMPLETION of every
job is the invariant — see ``soak_runner.check_soak_invariants``):

* mild network windows (drop 5-20%, delay 20-80ms + jitter, duplicate
  20-50%) recurring across the horizon on the manager<->worker links,
  0-2 per 120s segment, windows capped at their segment boundary so
  same-kind same-link windows can NEVER overlap (the coordinator's
  first-match-wins rule stays irrelevant);
* slow_disk windows on the manager (5-40ms per storage operation,
  bounded — storage delay must be ridden THROUGH, exactly the VOPR
  calibration), disjoint by the same segment containment;
* at most ONE host kill (worker + both executor children at one
  instant — the ``test_multiprocess_worker_retry`` recipe) in the
  [0.35, 0.55] * ceiling band, with a replacement worker joining
  10-20s later: the manager must reap the dead host inside the SWIM
  design bound and keep completing EVERY job on the replacement for
  the rest of the horizon. (Client-visible job windows measure ~1s
  wide inside 30-60s gaps, so the kill lands between jobs in
  practice — probed at seeds 901/907; the in-flight retry mechanism
  itself is pinned by ``test_multiprocess_worker_retry``.) MEASURED
  GAP: at the default ceiling this class currently STARVES post-kill
  dispatch (seed 902 — every post-kill job after the first is
  accepted then times out on the manager's 30s timeout-sweep grid;
  see the skip-pinned aspirational in ``test_soak.py``), so kill
  seeds are failing reproducers until the production fix lands.

Deliberate exclusions (documented, not oversights): ``disk_full`` is
permanent by construction (no clear knob) — it turns an endurance run
into a truth-telling run, which is the calibrated VOPR's job;
``restart`` re-arms absolute-time storage schedules at gen-2 boot
(the vopr_mdc constraint) and its long-horizon value is already pinned
by the restart scenarios — composing it with recurring storage windows
is a different program item (B8/E1, other agents' zones).
"""

import random
from dataclasses import dataclass, field


# Horizon ceiling, virtual seconds. Sized from PROBED throughput of
# this 5-8 process topology (tests.simulation.soak.soak_runner CLI):
# seed 901 at ceiling 300 ran 38.0s wall / twin 37.0s (0.127 wall-s
# per virtual-s); kill seed 907 at ceiling 600 ran 103.4s wall (0.172
# — the late joiner adds three barrier participants for the back
# half). At the ~0.17 kill-plan rate one 1800s horizon costs ~5 wall
# minutes per run, so the default sweep scenario — judge run PLUS the
# byte-identical replay twin of the WHOLE horizon — budgets ~10-11
# wall minutes, inside the ~15 minute ceiling for the opt-in test
# while giving ~36 sequential jobs and 15 fault segments per seed.
_CEILING = 1800.0

# Job-level timeout carried on every submission. Generous relative to
# the 8-20s workflow durations so the INTENDED schedule outcome — not
# an accidental timeout — decides each job, including the kill-window
# job whose retry path spans SWIM detection (up to ~71s witness-less)
# plus the late joiner's registration.
JOB_TIMEOUT_SECONDS = 90.0

# Client-side wait_for_job deadline per job: expiry logs
# ("job<k>-wait-timed-out", t) and the entry then waits unbounded, so
# a stranded job stays visible in the log rather than killing the run.
WAIT_TIMEOUT_SECONDS = 150.0

# Last submission must leave duration_max(20) + JOB_TIMEOUT(90) + a
# retry/convergence margin(30) of ceiling for its terminal.
_TAIL_BUDGET_SECONDS = 140.0

# Mild-window segmentation: 0-2 windows drawn per segment, each window
# contained inside its segment (disjointness across the horizon for
# free).
_SEGMENT_SECONDS = 120.0

_MILD_KINDS = ("drop", "delay", "duplicate", "slow_disk")


@dataclass(slots=True)
class SoakPlan:
    """One generated endurance scenario: seed, ceiling, the job
    schedule, and the fault events.

    Events are plain value tuples (readable in a replay session,
    structurally comparable in tests):

    * ``("host_kill", at_time, worker_b_start_at)`` — SIGKILL worker-a
      AND both of its executor children at ``at_time`` (host death);
      replacement worker-b begins its startup at ``worker_b_start_at``
    * ``("drop", src, dst, probability, at_time, until_time)`` — seeded
      UDP loss on one directed manager<->worker link
    * ``("delay", src, dst, extra, jitter, at_time, until_time)`` —
      added latency (streams included) on one directed link
    * ``("duplicate", src, dst, probability, at_time, until_time)`` —
      UDP duplication on one directed link
    * ``("slow_disk", at_time, delay_seconds, until_time)`` — the
      manager's disk charges virtual time per operation in the window

    ``submit_times``/``durations`` are parallel tuples: job ``k``
    (1-based) submits at ``submit_times[k-1]`` (or at the previous
    job's terminal, whichever is later — the client is strictly
    sequential) with a ``durations[k-1]``-second workflow ``duration``
    attribute. MEASURED CAVEAT (probes, seeds 901/907): under SIM the
    ACTION-chain ``SimSoakWorkflow`` executes ONE pass per VU (~1.0s
    worker-side active window, ~1.04s client-visible latency)
    regardless of ``duration`` — duration-GOVERNED workloads were the
    K1 runner constraint (the old ``WorkflowRunner._generate`` busy-waited
    frozen virtual time). Its successor, ``_run_long_lived_vu``, takes a
    timed sleep after ``_FROZEN_CLOCK_SPINS`` iterations on a frozen
    clock, which should lift it -- not yet re-probed under SIM. The drawn
    duration still seeds the submission payload/timeout shape and becomes
    load-bearing once that probe passes; horizon OCCUPANCY today comes
    from the 30-60s submission cadence, not per-job execution length.
    """

    seed: int
    ceiling: float = _CEILING
    submit_times: tuple[float, ...] = ()
    durations: tuple[float, ...] = ()
    events: list[tuple] = field(default_factory=list)

    def host_kill(self) -> tuple | None:
        """The plan's host-kill event, if one was drawn."""
        kills = [event for event in self.events if event[0] == "host_kill"]
        return kills[0] if kills else None


def generate_soak_plan(seed: int, ceiling: float = _CEILING) -> SoakPlan:
    """Expand ``seed`` into a deterministic long-horizon scenario.

    ``ceiling`` parameterizes the horizon so probe tooling can measure
    wall cost at small ceilings; the suite always uses the default.
    Draw order is fixed (jobs, then the kill coin, then segment
    windows) so every seed maps to one schedule forever.
    """
    plan_random = random.Random(seed)
    plan = SoakPlan(seed=seed, ceiling=ceiling)

    submit_times: list[float] = []
    durations: list[float] = []
    next_submit = plan_random.uniform(2.0, 6.0)
    while next_submit <= ceiling - _TAIL_BUDGET_SECONDS:
        submit_times.append(round(next_submit, 3))
        durations.append(round(plan_random.uniform(8.0, 20.0), 3))
        next_submit += plan_random.uniform(30.0, 60.0)
    plan.submit_times = tuple(submit_times)
    plan.durations = tuple(durations)

    # Quiet-horizon baseline: ~10% of the space is an OCCUPIED but
    # fault-free horizon (segments draw 0-2 windows each, so an
    # all-zero draw is a ~(1/3)^15 accident — the baseline must be in
    # the space DELIBERATELY, the I3 rule). Same invariants: every job
    # completes, membership never flaps, the whole horizon replays.
    if plan_random.random() < 0.1:
        return plan

    kill_at: float | None = None
    if plan_random.random() < 0.5:
        kill_at = round(plan_random.uniform(0.35 * ceiling, 0.55 * ceiling), 3)
        worker_b_start = round(kill_at + plan_random.uniform(10.0, 20.0), 3)
        plan.events.append(("host_kill", kill_at, worker_b_start))

    for segment_index in range(int(ceiling // _SEGMENT_SECONDS)):
        segment_start = segment_index * _SEGMENT_SECONDS
        segment_end = segment_start + _SEGMENT_SECONDS
        drawn_keys: set[tuple] = set()
        for _ in range(plan_random.randrange(0, 3)):
            event_kind = plan_random.choice(_MILD_KINDS)
            window_start = round(
                segment_start + plan_random.uniform(0.0, 85.0), 3
            )
            window_end = round(
                min(
                    window_start + plan_random.uniform(10.0, 30.0),
                    segment_end,
                ),
                3,
            )
            if event_kind == "slow_disk":
                if ("slow_disk",) in drawn_keys:
                    continue  # one disk window per segment: disjoint toggles
                drawn_keys.add(("slow_disk",))
                plan.events.append(
                    (
                        "slow_disk",
                        window_start,
                        round(plan_random.uniform(0.005, 0.04), 3),
                        window_end,
                    )
                )
                continue

            # Network windows target the worker ALIVE at the window's
            # start (fault windows key on send time, so a window aimed
            # at the dead host would be inert).
            worker_id = (
                "worker-b"
                if kill_at is not None and window_start >= kill_at
                else "worker-a"
            )
            link = plan_random.choice(
                (("manager", worker_id), (worker_id, "manager"))
            )
            link_key = (event_kind, *link)
            if link_key in drawn_keys:
                continue  # same-kind same-link windows stay disjoint
            drawn_keys.add(link_key)

            if event_kind == "drop":
                plan.events.append(
                    (
                        "drop",
                        *link,
                        round(plan_random.uniform(0.05, 0.20), 3),
                        window_start,
                        window_end,
                    )
                )
            elif event_kind == "delay":
                plan.events.append(
                    (
                        "delay",
                        *link,
                        round(plan_random.uniform(0.02, 0.08), 3),
                        round(plan_random.uniform(0.0, 0.04), 3),
                        window_start,
                        window_end,
                    )
                )
            else:
                plan.events.append(
                    (
                        "duplicate",
                        *link,
                        round(plan_random.uniform(0.2, 0.5), 3),
                        window_start,
                        window_end,
                    )
                )

    return plan
