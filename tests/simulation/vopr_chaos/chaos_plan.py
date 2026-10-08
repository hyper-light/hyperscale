"""
Seed-driven CHAOS-WINDOW schedule generation — the E1-E4 saturating
half of the VOPR program (TigerBeetle's density philosophy on this
repo's coordinator).

``generate_chaos_plan(seed)`` expands one integer into a complete,
self-describing chaos scenario: a seed-drawn TOPOLOGY (F3 — gateless
L2, 3-gate L3, or two-datacenter MDC, all existing entries), a
seed-drawn WORKLOAD occupying the pre-chaos horizon, and a SATURATED
fault schedule — 5-15 events with overlapping windows stacked across
kinds and links — every one of which ENDS by the plan's ``chaos_end``
(heals, knob-clears, respawns and thaws included), followed by a
fault-free convergence run to the ceiling. The SAME seed always yields
the SAME plan, and the plan drives a coordinator run that replays
byte-identically (``pytest tests/simulation/vopr_chaos
--sim-replay=<seed>``).

THE CHAOS-WINDOW CONTRACT (E2/E3 — why density here is sound where the
calibrated VOPRs forbid it): DURING ``[0, chaos_end)`` faults may
exceed every survivable calibration — partitions past the ~38s
detection bound (nodes get declared dead and must re-admit after
heal), 30-90% loss, kill+partition+storage stacks — and job death or
timeout is a legitimate outcome; the invariants demanded THROUGHOUT
are safety only (per-job linearization, cross-node coherence, loud
outcomes, no unknown vocabulary, determinism-audit absence). LIVENESS
is demanded only AFTER quiesce: every pre-chaos job reaches a
client-observed terminal within ``convergence_budget``, membership
converges to the live topology, and a probe job submitted after
``chaos_end`` completes (the cluster-is-alive check). E4 keeps
liveness fair: the generator constrains every non-flagged plan to
leave a viable core (≥1 manager rebooted by ``chaos_end``, ≥1
registered-able worker, gate quorum in L3), and the two deliberate
exceptions are FLAGGED plan flavors with inverted expectations —
``doomed`` (J2 permanent manager kill: the loud outcome IS the
invariant) and ``workerless`` (K6 retry-cap surface: accepted jobs
must die loudly on the AD-34 grid, never silently).

TIME-STRUCTURE CALIBRATION (probed on this tree — every number's WHY):

* L2 (manager + 1-2 workers + sequential multi-job client + probe):
  ceiling 600, chaos_end 380, budget 200. Worst post-quiesce legs the
  budget must clear: witness-less SWIM reap of a chaos-edge kill
  ([25, 85.5]s, the traced AD-30 max leg) + post-reboot dispatch
  backoff pacing (probed 34.6s submit-to-complete after a worker
  restart) + one AD-26 grant (+30s) + one AD-34 sweep tick (+30s)
  ~= 185s < 200. A job accepted AT the chaos edge and stranded
  resolves by timeout(60) + grant(30) + tick(30) = 120 < 200.
* L3 (3 peered gates + manager DC + worker + client through the
  tier + probe): ceiling 260, chaos_end 110, budget 130. A gate
  killed at the chaos edge is detected by survivors inside the
  witness-less band (kill + [25, 85.5] <= 195.5) and re-election
  completes ~10.5s later (probed) — all leadership movement done by
  ~206, ahead of the final 30s stability window [230, 260]. The
  probe (submits 122) completes ~+6.2s on the probed baseline; the
  260s fault-free run and the probe-job completion were BOTH probed
  on this tree (the pre-fix-wave client-orphan spin that capped
  ``vopr_gates`` at 145s no longer fires, and second/late jobs
  through gates complete since 771b7e99).
* MDC (1 gate + 2 DCs + client through the gate + probe): ceiling
  260, chaos_end 110, budget 130. A job stranded by dc_loss resolves
  as the gate tracker's loud ``timed_out`` at submit + 60 + <=15s
  tick (measured 77s in vopr_mdc); the gate reclassifies a lost DC
  ``unhealthy`` at kill + ~30 (heartbeat staleness) — both inside
  chaos_end + budget with >60s to spare.

FAULT-VOCABULARY CALIBRATION (per kind — ranges and WHYs):

* ``partition`` 8-110s (L2 manager<->worker) / 8-70s (gate<->gate) /
  8-60s (gate<->manager): deliberately straddles the ~38s sustained-
  silence detection bound — short cuts must be ridden out, long cuts
  MUST declare death and then re-admit after heal (7c477a0b's
  re-admission surface). 30% draw one-way (``bidirectional=False`` —
  A2's first generated use; one-way silence must not split-brain).
* client-link cuts 6-45s (L2) / 6-30s (L3/MDC): the L1 client edge —
  sized against the 1s submission-retry cadence and the gateless
  ~61s blackout rejection cycle so acceptance retries and the poll
  fallback both get exercised and still heal well before quiesce.
* ``drop`` 0.30-0.90 (A4 beyond-survivable density, chaos-only),
  ``duplicate`` 0.20-0.90, both UDP-scoped (the coordinator's
  physical model: TCP masks loss and never re-delivers).
* ``delay`` extra 0-50ms + jitter 100-300ms (A6): jitter far exceeds
  busy-window inter-send gaps, so same-link datagrams REORDER
  deterministically; applies to client TCP too (late pushes race
  polls — the ordering-guard surface).
* ``corrupt`` 0.05-0.40 on UDP membership links (A8): delivered
  bit-flipped frames must be REJECTED whole (auth/parse) —
  indistinguishable from loss, never half-applied, never a crash.
* storage windows on the durable (manager) surface, all healed by
  ``chaos_end``: ``slow_disk`` 5-40ms/op x 10-30s (ride-through);
  ``disk_full_window`` 512-4096 further bytes x 10-25s (ENOSPC may
  fail jobs LOUDLY; 2c15a8b7's archive isolation must hold);
  ``read_corruption`` p 0.3-1.0 x 8-25s (B4: CRC-failing reads are
  loud or clean-truncated, never applied); ``io_error`` p 0.05-0.25
  x 8-25s (B6: EIO retried or loud — the knob self-windows on the
  virtual clock, so one window per plan per target); ``misdirect``
  p 0.05-0.20 x 8-25s (B5: sibling-file confusion caught by
  framing/CRC). Windows may straddle manager restarts — the chaos
  entries arm knobs BOOT-AWARE, so a generation rebooting inside a
  window recovers against the faulted disk (B8, deliberate).
* ``pause`` 10-60s (L2) / 8-45s (L3/MDC) — C5 SIGSTOP freezes drawn
  on managers, workers, and gates: windows both below and beyond the
  detection bound, so thawed nodes rejoin from both fates; a frozen
  manager sweeps its buffered AD-34 ticks at the thaw and a frozen
  gate's buffered completion push must deliver-or-converge via the
  poll path. Per-victim windows are disjoint (the coordinator raises
  on overlap) and never START while the victim is dead.
* ``wall_skew`` +/-5-60s steps (D1/D2), up to two per plan, manager
  targets only: models NTP steps (backwards included). The offset
  PERSISTS past chaos_end deliberately — skew is environment state,
  not a healable fault, and convergence must hold UNDER constant
  skew (HLC absorbs wall steps; timers are monotonic-based). This is
  the one documented exception to "every fault ends by chaos_end".
* ``restart`` (manager power loss) down 10-30s (L2) / 8-20s
  (L3/MDC), half with fsync-reorder torn debris; 35% draw the C6
  DOUBLE: R2 lands 2-12s after gen-2's boot, so the second power
  loss hits recovery itself (recovery idempotency). Both respawns
  complete >=2s before chaos_end (E2).
* ``host_kill`` + staggered late-join replacement 8-18s later (the
  committed worker-retry recipe); the K6 storm flavor kills the
  replacement too (25-45s after it starts, letting it register) and
  joins a second replacement — every kill leaves the core viable.
* ``worker_restart`` (C4, committed recipe): both executors killed
  at the restart instant, gen-2 respawns the pool under the same
  ids; the pinned truth is that an in-flight job on the restarted
  worker dies as a loud AD-34 timeout (no in-flight reassignment),
  which chaos accepts — terminals, not completions, are the bar.
* ``gate_kill`` at 25-105s (L3): after the probed acceptance instant
  (~5.5-8s) and mutually exclusive with client<->gate cuts (which
  can push acceptance past any kill — the vopr_gates exclusion);
  quorum survives (2 of 3), and the post-quiesce probe THROUGH the
  survivors is the J2 dispatch-works claim.
* ``dc_loss`` (MDC) at 12-100s: total loss of one DC's four
  processes; the job may be placed there (loud gate ``timed_out``)
  or not (unaffected) — both legitimate; the gate must classify the
  lost DC ``unhealthy`` and the probe must complete on the survivor.

STRUCTURAL RULES the generator enforces (mirrors of coordinator
semantics — each would raise or silently rewrite the scenario if
violated): same-kind same-link windows are DISJOINT (first-match-wins
in the coordinator would silently ignore the second window; density
comes from stacking kinds and links, which fully compose per
datagram); restarts never land inside another restart's down window;
pauses never start while their victim is dead and never overlap on
one victim; kills target processes that exist at the kill instant
(executor kills only after their worker's pool has spawned); storage
and skew windows never target a process that dc_loss kills; at most
one io_error window per target (the knob re-arms whole, so a second
window would silently replace the first).
"""

import random
from dataclasses import dataclass, field

TOPOLOGY_L2 = "l2"
TOPOLOGY_L3 = "l3"
TOPOLOGY_MDC = "mdc"

GATE_PROCESS_IDS = ("gate-a", "gate-b", "gate-c")

MDC_DATACENTER_IDS = ("dc-east", "dc-west")

# Canonical host names per topology (the executor-id derivation below
# depends on them — the production spawner names pool children
# ``executor-<worker_host>-<port>`` with ports 9009/9011 for 2 cores).
L2_WORKER_HOSTS = {
    "worker-a": "sim-wkr-a",
    "worker-b": "sim-wkr-b",
    "worker-r1": "sim-wkr-r1",
    "worker-r2": "sim-wkr-r2",
}
L3_WORKER_HOSTS = {"worker": "sim-wkr"}
MDC_WORKER_HOSTS = {
    "worker-dc-east": "sim-wkr-east",
    "worker-dc-west": "sim-wkr-west",
}

MDC_DC_PROCESS_IDS: dict[str, tuple[str, ...]] = {
    "dc-east": (
        "manager-dc-east",
        "worker-dc-east",
        "executor-sim-wkr-east-9009",
        "executor-sim-wkr-east-9011",
    ),
    "dc-west": (
        "manager-dc-west",
        "worker-dc-west",
        "executor-sim-wkr-west-9009",
        "executor-sim-wkr-west-9011",
    ),
}

_EXECUTOR_PORTS = (9009, 9011)

# Per-topology time structure (docstring calibration table).
_TIME_STRUCTURE: dict[str, tuple[float, float, float]] = {
    # topology: (ceiling, chaos_end, convergence_budget)
    TOPOLOGY_L2: (600.0, 380.0, 200.0),
    TOPOLOGY_L3: (260.0, 110.0, 130.0),
    TOPOLOGY_MDC: (260.0, 110.0, 130.0),
}

# Probe submits this long after quiesce: clears the thaw bursts of
# pauses resuming AT chaos_end before the cluster-is-alive check.
_PROBE_DELAY_SECONDS = 12.0

JOB_TIMEOUT_SECONDS = 60.0
WAIT_TIMEOUT_SECONDS = 120.0
L3_JOB_TIMEOUT_SECONDS = 45.0

_CHAOS_SPAN_START = 4.0


def worker_executor_ids(topology: str, worker_process_id: str) -> tuple[str, str]:
    """The two executor-pool child ids of one worker process."""
    hosts = {
        TOPOLOGY_L2: L2_WORKER_HOSTS,
        TOPOLOGY_L3: L3_WORKER_HOSTS,
        TOPOLOGY_MDC: MDC_WORKER_HOSTS,
    }[topology]
    worker_host = hosts[worker_process_id]
    return tuple(
        f"executor-{worker_host}-{port}" for port in _EXECUTOR_PORTS
    )


@dataclass(slots=True)
class ChaosPlan:
    """One generated chaos scenario: topology, workload, time
    structure, and the saturated fault schedule.

    Events are plain value tuples (readable in a replay session,
    structurally comparable in tests):

    * ``("partition", src, dst, at, heal, bidirectional)`` — cable cut
      (streams included); ``bidirectional=0`` cuts only ``src -> dst``
    * ``("drop" | "duplicate" | "corrupt", src, dst, probability, at,
      until)`` — seeded per-datagram UDP faults on one directed link
    * ``("delay", src, dst, extra, jitter, at, until)`` — added latency
      (streams included) on one directed link
    * ``("kill", process_id, at)`` — permanent SIGKILL (J2 manager
      permakill; L3 gate kill)
    * ``("host_kill", worker_id, at, replacement_id, replacement_start)``
      — worker + both executors die at one instant; ``replacement_id``
      of ``None`` is the flagged workerless flavor
    * ``("worker_restart", worker_id, at, down)`` — the C4 recipe:
      both executors killed at ``at`` + worker power-loss reboot
    * ``("restart", process_id, at, down, fsync_reorder_seed)`` —
      manager power loss + reboot from the surviving durable disk
    * ``("pause", process_id, at, resume)`` — C5 SIGSTOP freeze
    * ``("slow_disk", target, at, delay, until)`` /
      ``("disk_full_window", target, at, bytes, until)`` /
      ``("read_corruption", target, at, probability, knob_seed, until)``
      / ``("io_error", target, at, probability, knob_seed, until)`` /
      ``("misdirect", target, at, probability, knob_seed, until)`` —
      storage windows on the target manager's disk, healed at
      ``until``
    * ``("wall_skew", target, at, delta)`` — NTP-step wall offset
    * ``("dc_loss", dc_id, at)`` — MDC total-datacenter SIGKILL
    """

    seed: int
    topology: str
    ceiling: float
    chaos_end: float
    convergence_budget: float
    probe_submit_at: float
    initial_worker_count: int = 1
    submit_times: tuple[float, ...] = ()
    durations: tuple[float, ...] = ()
    job_timeout_seconds: float = JOB_TIMEOUT_SECONDS
    wait_timeout_seconds: float = WAIT_TIMEOUT_SECONDS
    events: list[tuple] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Flavor / topology derivations (all pure event scans)
    # ------------------------------------------------------------------

    def is_doomed(self) -> bool:
        """J2 permanent manager kill: liveness inverts to loudness."""
        return any(
            event[0] == "kill" and event[1].startswith("manager")
            for event in self.events
        )

    def is_workerless(self) -> bool:
        """The K6 no-replacement flavor: the last worker dies and jobs
        accepted afterwards must die LOUDLY on the AD-34 grid."""
        return any(
            event[0] == "host_kill" and event[3] is None
            for event in self.events
        )

    def host_kills(self) -> list[tuple]:
        return [event for event in self.events if event[0] == "host_kill"]

    def replacement_workers(self) -> list[tuple[str, float]]:
        """``(worker_process_id, start_at)`` late joiners, kill order."""
        return [
            (event[3], event[4])
            for event in self.host_kills()
            if event[3] is not None
        ]

    def initial_worker_ids(self) -> tuple[str, ...]:
        if self.topology == TOPOLOGY_L2:
            return ("worker-a", "worker-b")[: self.initial_worker_count]
        if self.topology == TOPOLOGY_L3:
            return ("worker",)
        return tuple(sorted(MDC_WORKER_HOSTS))

    def worker_ids(self) -> tuple[str, ...]:
        """Every worker process the run ever admits, join order."""
        return self.initial_worker_ids() + tuple(
            worker_id for worker_id, _start in self.replacement_workers()
        )

    def live_worker_ids_at_end(self) -> tuple[str, ...]:
        """Workers alive at the ceiling — the E4 membership target."""
        dead = {event[1] for event in self.host_kills()}
        for event in self.events:
            if event[0] == "dc_loss":
                dead.update(
                    process_id
                    for process_id in MDC_DC_PROCESS_IDS[event[1]]
                    if process_id.startswith("worker")
                )
        return tuple(
            worker_id
            for worker_id in self.worker_ids()
            if worker_id not in dead
        )

    def killed_process_ids(self) -> tuple[str, ...]:
        """Processes SIGKILLed and never respawned — their milestone
        logs are ERASED by kill semantics (the trace oracle degrades
        to surviving evidence)."""
        killed: list[str] = []
        for event in self.events:
            if event[0] == "kill":
                killed.append(event[1])
            elif event[0] == "host_kill":
                killed.append(event[1])
                killed.extend(worker_executor_ids(self.topology, event[1]))
            elif event[0] == "dc_loss":
                killed.extend(MDC_DC_PROCESS_IDS[event[1]])
        return tuple(killed)

    def lost_datacenters(self) -> set[str]:
        return {event[1] for event in self.events if event[0] == "dc_loss"}

    def expected_health_by_datacenter(self) -> dict[str, str]:
        """Gate-view health every surviving gate must converge to."""
        if self.topology == TOPOLOGY_L3:
            return {"dc-1": "unhealthy" if self.is_doomed() else "healthy"}
        lost = self.lost_datacenters()
        return {
            datacenter_id: "unhealthy" if datacenter_id in lost else "healthy"
            for datacenter_id in MDC_DATACENTER_IDS
        }

    def convergence_deadline(self) -> float:
        return round(self.chaos_end + self.convergence_budget, 3)


def generate_chaos_plan(seed: int) -> ChaosPlan:
    """Expand ``seed`` into a deterministic chaos scenario.

    Fixed draw order (topology, baseline coin, workload, process-fault
    skeleton, pauses, window fill) so every seed maps to one schedule
    forever. Topology weights: L2 0.5 (the multi-job workhorse), L3
    0.3, MDC 0.2. A ~5% BASELINE slice generates a fault-free occupied
    horizon per topology — the I3 rule (the baseline must be in the
    space deliberately; it pins the topology menu itself and must hold
    the full liveness set). Non-baseline plans draw a 5-15 event
    target; placement-constrained draws retry boundedly, so realized
    density can rarely fall slightly below target while every plan
    stays structurally sound.
    """
    plan_random = random.Random(seed)
    topology_draw = plan_random.random()
    if topology_draw < 0.5:
        topology = TOPOLOGY_L2
    elif topology_draw < 0.8:
        topology = TOPOLOGY_L3
    else:
        topology = TOPOLOGY_MDC

    ceiling, chaos_end, convergence_budget = _TIME_STRUCTURE[topology]
    plan = ChaosPlan(
        seed=seed,
        topology=topology,
        ceiling=ceiling,
        chaos_end=chaos_end,
        convergence_budget=convergence_budget,
        probe_submit_at=round(chaos_end + _PROBE_DELAY_SECONDS, 3),
    )
    is_baseline = plan_random.random() < 0.05

    if topology == TOPOLOGY_L2:
        _draw_l2_workload(plan_random, plan)
        if not is_baseline:
            _generate_l2_events(plan_random, plan)
    else:
        duration = round(plan_random.uniform(6.0, 10.0), 3)
        plan.submit_times = (0.0,)
        plan.durations = (duration,)
        if topology == TOPOLOGY_L3:
            plan.job_timeout_seconds = L3_JOB_TIMEOUT_SECONDS
        if not is_baseline:
            if topology == TOPOLOGY_L3:
                _generate_l3_events(plan_random, plan)
            else:
                _generate_mdc_events(plan_random, plan)

    return plan


# ----------------------------------------------------------------------
# Workload
# ----------------------------------------------------------------------


def _draw_l2_workload(plan_random: random.Random, plan: ChaosPlan) -> None:
    """2-4 sequential jobs, first at 3-8s, gaps 40-80s, all submitting
    >=40s before chaos_end (acceptance retries under blackout are
    timeout-paced ~61s, so the LAST job's acceptance may legitimately
    slip past quiesce — it is still judged against the convergence
    deadline). Durations 6-15s; worker count 1-2 (F3)."""
    plan.initial_worker_count = 1 if plan_random.random() < 0.65 else 2
    job_count = plan_random.randrange(2, 5)
    submit_times: list[float] = []
    durations: list[float] = []
    next_submit = plan_random.uniform(3.0, 8.0)
    for _ in range(job_count):
        if next_submit > plan.chaos_end - 40.0:
            break
        submit_times.append(round(next_submit, 3))
        durations.append(round(plan_random.uniform(6.0, 15.0), 3))
        next_submit += plan_random.uniform(40.0, 80.0)
    plan.submit_times = tuple(submit_times)
    plan.durations = tuple(durations)


# ----------------------------------------------------------------------
# Shared draw helpers
# ----------------------------------------------------------------------


def _place_window(
    plan_random: random.Random,
    intervals_by_key: dict[tuple, list[tuple[float, float]]],
    key: tuple,
    span_start: float,
    span_end: float,
    min_length: float,
    max_length: float,
    attempts: int = 12,
) -> tuple[float, float] | None:
    """Draw a window of seeded length inside the chaos span, disjoint
    from every prior window under ``key`` (the coordinator's
    first-match-wins rule makes overlapping same-kind same-link
    windows silently inert — the generator refuses to emit them).
    Bounded retries keep generation deterministic and total."""
    existing = intervals_by_key.setdefault(key, [])
    for _ in range(attempts):
        length = plan_random.uniform(min_length, max_length)
        latest_start = span_end - length
        if latest_start <= span_start:
            return None
        window_start = round(plan_random.uniform(span_start, latest_start), 3)
        window_end = round(window_start + length, 3)
        if all(
            window_end <= other_start or window_start >= other_end
            for other_start, other_end in existing
        ):
            existing.append((window_start, window_end))
            return window_start, window_end
    return None


def _pause_start_is_live(
    pause_start: float, dead_intervals: list[tuple[float, float]]
) -> bool:
    """Pauses must ACTIVATE while the victim is alive (the coordinator
    raises on freezing a corpse); a later kill/restart inside the
    window is legal, documented composition."""
    return all(
        not (dead_start <= pause_start < dead_end)
        for dead_start, dead_end in dead_intervals
    )


def _live_worker_at(plan: ChaosPlan, instant: float) -> str | None:
    """The most recently joined worker alive at ``instant`` — network
    windows aim at it so they are never inert (the soak rule)."""
    alive: list[str] = list(plan.initial_worker_ids())
    for event in plan.host_kills():
        _kind, victim_id, kill_at, replacement_id, replacement_start = event
        if kill_at <= instant and victim_id in alive:
            alive.remove(victim_id)
        if replacement_id is not None and replacement_start <= instant:
            alive.append(replacement_id)
    return alive[-1] if alive else None


# ----------------------------------------------------------------------
# L2 generation
# ----------------------------------------------------------------------


def _generate_l2_events(plan_random: random.Random, plan: ChaosPlan) -> None:
    """Skeleton (process-fault flavor) first, then pauses, then window
    fill to the 5-15 density target (module-docstring calibrations)."""
    chaos_end = plan.chaos_end
    dead_intervals: dict[str, list[tuple[float, float]]] = {}
    flavor_draw = plan_random.random()

    if flavor_draw < 0.06:
        kill_at = round(plan_random.uniform(60.0, chaos_end - 60.0), 3)
        plan.events.append(("kill", "manager", kill_at))
        dead_intervals["manager"] = [(kill_at, float("inf"))]
    elif flavor_draw < 0.11:
        plan.initial_worker_count = 1  # workerless: exactly one to lose
        kill_at = round(plan_random.uniform(60.0, chaos_end - 40.0), 3)
        plan.events.append(("host_kill", "worker-a", kill_at, None, 0.0))
        dead_intervals["worker-a"] = [(kill_at, float("inf"))]
    elif flavor_draw < 0.23:
        first_kill = round(plan_random.uniform(40.0, chaos_end - 160.0), 3)
        first_join = round(first_kill + plan_random.uniform(8.0, 18.0), 3)
        plan.events.append(
            ("host_kill", "worker-a", first_kill, "worker-r1", first_join)
        )
        dead_intervals["worker-a"] = [(first_kill, float("inf"))]
        second_kill = round(
            min(first_join + plan_random.uniform(25.0, 45.0), chaos_end - 60.0),
            3,
        )
        second_join = round(second_kill + plan_random.uniform(8.0, 18.0), 3)
        plan.events.append(
            ("host_kill", "worker-r1", second_kill, "worker-r2", second_join)
        )
        dead_intervals["worker-r1"] = [(second_kill, float("inf"))]
    elif flavor_draw < 0.43:
        kill_at = round(plan_random.uniform(40.0, chaos_end - 100.0), 3)
        join_at = round(kill_at + plan_random.uniform(8.0, 18.0), 3)
        plan.events.append(
            ("host_kill", "worker-a", kill_at, "worker-r1", join_at)
        )
        dead_intervals["worker-a"] = [(kill_at, float("inf"))]
    elif flavor_draw < 0.51:
        restart_at = round(plan_random.uniform(30.0, chaos_end - 80.0), 3)
        down_seconds = round(plan_random.uniform(5.0, 20.0), 3)
        plan.events.append(("worker_restart", "worker-a", restart_at, down_seconds))
        dead_intervals["worker-a"] = [(restart_at, restart_at + down_seconds)]
    elif flavor_draw < 0.63:
        executor_id = plan_random.choice(
            worker_executor_ids(TOPOLOGY_L2, "worker-a")
        )
        plan.events.append(
            ("kill", executor_id, round(plan_random.uniform(20.0, chaos_end - 60.0), 3))
        )
    elif flavor_draw < 0.83:
        _draw_manager_restart_chain(
            plan_random,
            plan,
            "manager",
            dead_intervals,
            first_at_span=(15.0, chaos_end - 110.0),
            down_span=(10.0, 30.0),
        )

    target_event_count = plan_random.randrange(5, 16)
    _draw_pauses(
        plan_random,
        plan,
        victims=("manager", "worker-a"),
        dead_intervals=dead_intervals,
        length_span=(10.0, 60.0),
    )

    intervals_by_key: dict[tuple, list[tuple[float, float]]] = {}
    wall_skew_count = 0
    io_error_targets: set[str] = set()
    while len(plan.events) < target_event_count:
        event_kind = plan_random.choices(
            (
                "partition",
                "client_partition",
                "drop",
                "delay",
                "duplicate",
                "corrupt",
                "slow_disk",
                "disk_full_window",
                "read_corruption",
                "io_error",
                "misdirect",
                "wall_skew",
            ),
            weights=(14, 9, 13, 12, 9, 9, 7, 5, 6, 5, 4, 7),
        )[0]

        if event_kind == "wall_skew":
            if wall_skew_count >= 2 or plan.is_doomed():
                continue
            wall_skew_count += 1
            plan.events.append(
                (
                    "wall_skew",
                    "manager",
                    round(plan_random.uniform(10.0, plan.chaos_end - 10.0), 3),
                    round(
                        plan_random.uniform(5.0, 60.0)
                        * plan_random.choice((-1.0, 1.0)),
                        3,
                    ),
                )
            )
            continue

        if event_kind in (
            "slow_disk",
            "disk_full_window",
            "read_corruption",
            "io_error",
            "misdirect",
        ):
            if not _draw_storage_window(
                plan_random,
                plan,
                event_kind,
                "manager",
                intervals_by_key,
                io_error_targets,
            ):
                continue
            continue

        if event_kind == "client_partition":
            # The probe client is silent during chaos (its lifecycle
            # starts after quiesce), so client-link faults target the
            # MAIN client only — probe links stay deliberately clean:
            # the probe is the fault-free-tail canary.
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("partition", "client", "manager"),
                _CHAOS_SPAN_START,
                plan.chaos_end,
                6.0,
                45.0,
            )
            if placed is None:
                continue
            bidirectional = 1 if plan_random.random() < 0.7 else 0
            endpoints = (
                ("client", "manager")
                if plan_random.random() < 0.5
                else ("manager", "client")
            )
            plan.events.append(
                ("partition", *endpoints, placed[0], placed[1], bidirectional)
            )
            continue

        window_span = {
            "partition": (8.0, 110.0),
            "drop": (10.0, 40.0),
            "delay": (10.0, 40.0),
            "duplicate": (10.0, 40.0),
            "corrupt": (10.0, 30.0),
        }[event_kind]
        probe_start = plan_random.uniform(
            _CHAOS_SPAN_START, plan.chaos_end - window_span[0]
        )
        worker_id = _live_worker_at(plan, probe_start)
        if worker_id is None:
            continue
        if event_kind == "partition":
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("partition", "manager", worker_id),
                _CHAOS_SPAN_START,
                plan.chaos_end,
                *window_span,
            )
            if placed is None:
                continue
            bidirectional = 1 if plan_random.random() < 0.7 else 0
            endpoints = (
                ("manager", worker_id)
                if plan_random.random() < 0.5
                else (worker_id, "manager")
            )
            plan.events.append(
                ("partition", *endpoints, placed[0], placed[1], bidirectional)
            )
            continue

        link = plan_random.choice(
            ((("manager", worker_id)), ((worker_id, "manager")))
        )
        if event_kind == "delay" and plan_random.random() < 0.25:
            link = plan_random.choice(
                (("manager", "client"), ("client", "manager"))
            )
        placed = _place_window(
            plan_random,
            intervals_by_key,
            (event_kind, *link),
            _CHAOS_SPAN_START,
            plan.chaos_end,
            *window_span,
        )
        if placed is None:
            continue
        if event_kind == "drop":
            plan.events.append(
                ("drop", *link, round(plan_random.uniform(0.30, 0.90), 3), *placed)
            )
        elif event_kind == "delay":
            plan.events.append(
                (
                    "delay",
                    *link,
                    round(plan_random.uniform(0.0, 0.05), 3),
                    round(plan_random.uniform(0.10, 0.30), 3),
                    *placed,
                )
            )
        elif event_kind == "duplicate":
            plan.events.append(
                (
                    "duplicate",
                    *link,
                    round(plan_random.uniform(0.20, 0.90), 3),
                    *placed,
                )
            )
        else:
            plan.events.append(
                (
                    "corrupt",
                    *link,
                    round(plan_random.uniform(0.05, 0.40), 3),
                    *placed,
                )
            )


# ----------------------------------------------------------------------
# L3 generation
# ----------------------------------------------------------------------


def _generate_l3_events(plan_random: random.Random, plan: ChaosPlan) -> None:
    """Gate-tier chaos: quorum-preserving gate kill, gate/manager/client
    link chaos, gate pauses (freezes across completion pushes and
    leadership), manager storage/skew/restarts (calibrations in the
    module docstring)."""
    chaos_end = plan.chaos_end
    dead_intervals: dict[str, list[tuple[float, float]]] = {}
    killed_gate: str | None = None
    flavor_draw = plan_random.random()

    if flavor_draw < 0.05:
        kill_at = round(plan_random.uniform(40.0, chaos_end - 40.0), 3)
        plan.events.append(("kill", "manager", kill_at))
        dead_intervals["manager"] = [(kill_at, float("inf"))]
    elif flavor_draw < 0.30:
        killed_gate = plan_random.choice(GATE_PROCESS_IDS)
        kill_at = round(plan_random.uniform(25.0, chaos_end - 5.0), 3)
        plan.events.append(("kill", killed_gate, kill_at))
        dead_intervals[killed_gate] = [(kill_at, float("inf"))]
    elif flavor_draw < 0.50:
        _draw_manager_restart_chain(
            plan_random,
            plan,
            "manager",
            dead_intervals,
            first_at_span=(12.0, chaos_end - 60.0),
            down_span=(8.0, 20.0),
        )

    target_event_count = plan_random.randrange(5, 16)
    _draw_pauses(
        plan_random,
        plan,
        victims=GATE_PROCESS_IDS + ("manager",),
        dead_intervals=dead_intervals,
        length_span=(8.0, 45.0),
    )

    gate_pairs = (
        ("gate-a", "gate-b"),
        ("gate-a", "gate-c"),
        ("gate-b", "gate-c"),
    )
    udp_links = tuple(
        directed
        for gate_x, gate_y in gate_pairs
        for directed in ((gate_x, gate_y), (gate_y, gate_x))
    ) + tuple(
        directed
        for gate_id in GATE_PROCESS_IDS
        for directed in ((gate_id, "manager"), ("manager", gate_id))
    )
    # Client-link delay targets the MAIN client only: the probe is
    # silent during chaos and its links stay deliberately clean.
    delay_links = udp_links + tuple(
        directed
        for gate_id in GATE_PROCESS_IDS
        for directed in (("client", gate_id), (gate_id, "client"))
    )

    intervals_by_key: dict[tuple, list[tuple[float, float]]] = {}
    wall_skew_count = 0
    io_error_targets: set[str] = set()
    isolation_drawn = False
    while len(plan.events) < target_event_count:
        event_kind = plan_random.choices(
            (
                "gate_gate_partition",
                "gate_isolation",
                "gate_manager_partition",
                "client_gate_partition",
                "drop",
                "delay",
                "duplicate",
                "corrupt",
                "slow_disk",
                "disk_full_window",
                "read_corruption",
                "io_error",
                "misdirect",
                "wall_skew",
            ),
            weights=(12, 6, 10, 9, 12, 11, 8, 8, 6, 4, 5, 4, 3, 6),
        )[0]

        if event_kind == "wall_skew":
            if wall_skew_count >= 2 or plan.is_doomed():
                continue
            wall_skew_count += 1
            plan.events.append(
                (
                    "wall_skew",
                    "manager",
                    round(plan_random.uniform(10.0, chaos_end - 10.0), 3),
                    round(
                        plan_random.uniform(5.0, 60.0)
                        * plan_random.choice((-1.0, 1.0)),
                        3,
                    ),
                )
            )
            continue

        if event_kind in (
            "slow_disk",
            "disk_full_window",
            "read_corruption",
            "io_error",
            "misdirect",
        ):
            _draw_storage_window(
                plan_random,
                plan,
                event_kind,
                "manager",
                intervals_by_key,
                io_error_targets,
            )
            continue

        if event_kind == "gate_gate_partition":
            gate_x, gate_y = plan_random.choice(gate_pairs)
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("partition", gate_x, gate_y),
                _CHAOS_SPAN_START,
                chaos_end,
                8.0,
                70.0,
            )
            if placed is None:
                continue
            bidirectional = 1 if plan_random.random() < 0.7 else 0
            endpoints = (
                (gate_x, gate_y)
                if plan_random.random() < 0.5
                else (gate_y, gate_x)
            )
            plan.events.append(
                ("partition", *endpoints, placed[0], placed[1], bidirectional)
            )
        elif event_kind == "gate_isolation":
            # A3: cut one gate from BOTH peers (never from the manager)
            # over one window — the 2-gate majority island retains the
            # tier; the islander must re-admit after heal. One per plan
            # (two could partition the whole tier pairwise).
            if isolation_drawn:
                continue
            victim_gate = plan_random.choice(GATE_PROCESS_IDS)
            peer_gates = [
                gate_id for gate_id in GATE_PROCESS_IDS if gate_id != victim_gate
            ]
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("isolation", victim_gate),
                _CHAOS_SPAN_START,
                chaos_end,
                12.0,
                60.0,
            )
            if placed is None:
                continue
            isolation_drawn = True
            for peer_gate in peer_gates:
                intervals_by_key.setdefault(
                    ("partition", *sorted((victim_gate, peer_gate))), []
                ).append(placed)
                plan.events.append(
                    ("partition", victim_gate, peer_gate, placed[0], placed[1], 1)
                )
        elif event_kind == "gate_manager_partition":
            gate_id = plan_random.choice(GATE_PROCESS_IDS)
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("partition", gate_id, "manager"),
                _CHAOS_SPAN_START,
                chaos_end,
                8.0,
                60.0,
            )
            if placed is None:
                continue
            bidirectional = 1 if plan_random.random() < 0.7 else 0
            endpoints = (
                (gate_id, "manager")
                if plan_random.random() < 0.5
                else ("manager", gate_id)
            )
            plan.events.append(
                ("partition", *endpoints, placed[0], placed[1], bidirectional)
            )
        elif event_kind == "client_gate_partition":
            if killed_gate is not None:
                continue  # the vopr_gates exclusion: cuts push acceptance past kills
            gate_id = plan_random.choice(GATE_PROCESS_IDS)
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("partition", "client", gate_id),
                _CHAOS_SPAN_START,
                chaos_end,
                6.0,
                30.0,
            )
            if placed is None:
                continue
            bidirectional = 1 if plan_random.random() < 0.7 else 0
            endpoints = (
                ("client", gate_id)
                if plan_random.random() < 0.5
                else (gate_id, "client")
            )
            plan.events.append(
                ("partition", *endpoints, placed[0], placed[1], bidirectional)
            )
        elif event_kind == "delay":
            link = plan_random.choice(delay_links)
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("delay", *link),
                _CHAOS_SPAN_START,
                chaos_end,
                10.0,
                30.0,
            )
            if placed is None:
                continue
            plan.events.append(
                (
                    "delay",
                    *link,
                    round(plan_random.uniform(0.0, 0.05), 3),
                    round(plan_random.uniform(0.10, 0.30), 3),
                    *placed,
                )
            )
        else:
            link = plan_random.choice(udp_links)
            placed = _place_window(
                plan_random,
                intervals_by_key,
                (event_kind, *link),
                _CHAOS_SPAN_START,
                chaos_end,
                10.0,
                40.0 if event_kind != "corrupt" else 30.0,
            )
            if placed is None:
                continue
            probability_span = {
                "drop": (0.30, 0.90),
                "duplicate": (0.20, 0.90),
                "corrupt": (0.05, 0.40),
            }[event_kind]
            plan.events.append(
                (
                    event_kind,
                    *link,
                    round(plan_random.uniform(*probability_span), 3),
                    *placed,
                )
            )


# ----------------------------------------------------------------------
# MDC generation
# ----------------------------------------------------------------------


def _generate_mdc_events(plan_random: random.Random, plan: ChaosPlan) -> None:
    """Two-datacenter chaos: total DC loss, per-DC manager restarts and
    storage windows, cross-DC wall skew (HLC), gate/client link chaos
    (calibrations in the module docstring)."""
    chaos_end = plan.chaos_end
    dead_intervals: dict[str, list[tuple[float, float]]] = {}
    lost_datacenter: str | None = None
    flavor_draw = plan_random.random()

    if flavor_draw < 0.20:
        lost_datacenter = plan_random.choice(MDC_DATACENTER_IDS)
        kill_at = round(plan_random.uniform(12.0, chaos_end - 10.0), 3)
        plan.events.append(("dc_loss", lost_datacenter, kill_at))
        for process_id in MDC_DC_PROCESS_IDS[lost_datacenter]:
            dead_intervals[process_id] = [(kill_at, float("inf"))]
    elif flavor_draw < 0.40:
        restart_datacenter = plan_random.choice(MDC_DATACENTER_IDS)
        _draw_manager_restart_chain(
            plan_random,
            plan,
            f"manager-{restart_datacenter}",
            dead_intervals,
            first_at_span=(12.0, chaos_end - 60.0),
            down_span=(8.0, 20.0),
        )

    faultable_datacenters = tuple(
        datacenter_id
        for datacenter_id in MDC_DATACENTER_IDS
        if datacenter_id != lost_datacenter
    )
    target_event_count = plan_random.randrange(5, 16)
    pause_victims = tuple(
        process_id
        for datacenter_id in faultable_datacenters
        for process_id in (f"manager-{datacenter_id}", f"worker-{datacenter_id}")
    ) + ("gate",)
    _draw_pauses(
        plan_random,
        plan,
        victims=pause_victims,
        dead_intervals=dead_intervals,
        length_span=(8.0, 40.0),
    )

    udp_links = tuple(
        directed
        for datacenter_id in MDC_DATACENTER_IDS
        for directed in (
            ("gate", f"manager-{datacenter_id}"),
            (f"manager-{datacenter_id}", "gate"),
        )
    )
    # Probe links stay clean during chaos (the probe is silent then).
    delay_links = udp_links + (("client", "gate"), ("gate", "client"))

    intervals_by_key: dict[tuple, list[tuple[float, float]]] = {}
    wall_skew_count = 0
    io_error_targets: set[str] = set()
    while len(plan.events) < target_event_count:
        event_kind = plan_random.choices(
            (
                "gate_dc_partition",
                "client_partition",
                "drop",
                "delay",
                "duplicate",
                "corrupt",
                "slow_disk",
                "disk_full_window",
                "read_corruption",
                "io_error",
                "misdirect",
                "wall_skew",
            ),
            weights=(14, 9, 12, 11, 8, 8, 7, 5, 6, 5, 4, 7),
        )[0]

        if event_kind == "wall_skew":
            if wall_skew_count >= 2 or not faultable_datacenters:
                continue
            wall_skew_count += 1
            skewed_dc = plan_random.choice(faultable_datacenters)
            plan.events.append(
                (
                    "wall_skew",
                    f"manager-{skewed_dc}",
                    round(plan_random.uniform(10.0, chaos_end - 10.0), 3),
                    round(
                        plan_random.uniform(5.0, 60.0)
                        * plan_random.choice((-1.0, 1.0)),
                        3,
                    ),
                )
            )
            continue

        if event_kind in (
            "slow_disk",
            "disk_full_window",
            "read_corruption",
            "io_error",
            "misdirect",
        ):
            if not faultable_datacenters:
                continue
            target_dc = plan_random.choice(faultable_datacenters)
            _draw_storage_window(
                plan_random,
                plan,
                event_kind,
                f"manager-{target_dc}",
                intervals_by_key,
                io_error_targets,
            )
            continue

        if event_kind == "gate_dc_partition":
            datacenter_id = plan_random.choice(MDC_DATACENTER_IDS)
            manager_id = f"manager-{datacenter_id}"
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("partition", "gate", manager_id),
                _CHAOS_SPAN_START,
                chaos_end,
                8.0,
                60.0,
            )
            if placed is None:
                continue
            bidirectional = 1 if plan_random.random() < 0.7 else 0
            endpoints = (
                ("gate", manager_id)
                if plan_random.random() < 0.5
                else (manager_id, "gate")
            )
            plan.events.append(
                ("partition", *endpoints, placed[0], placed[1], bidirectional)
            )
        elif event_kind == "client_partition":
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("partition", "client", "gate"),
                _CHAOS_SPAN_START,
                chaos_end,
                6.0,
                30.0,
            )
            if placed is None:
                continue
            bidirectional = 1 if plan_random.random() < 0.7 else 0
            endpoints = (
                ("client", "gate")
                if plan_random.random() < 0.5
                else ("gate", "client")
            )
            plan.events.append(
                ("partition", *endpoints, placed[0], placed[1], bidirectional)
            )
        elif event_kind == "delay":
            link = plan_random.choice(delay_links)
            placed = _place_window(
                plan_random,
                intervals_by_key,
                ("delay", *link),
                _CHAOS_SPAN_START,
                chaos_end,
                10.0,
                30.0,
            )
            if placed is None:
                continue
            plan.events.append(
                (
                    "delay",
                    *link,
                    round(plan_random.uniform(0.0, 0.05), 3),
                    round(plan_random.uniform(0.10, 0.30), 3),
                    *placed,
                )
            )
        else:
            link = plan_random.choice(udp_links)
            placed = _place_window(
                plan_random,
                intervals_by_key,
                (event_kind, *link),
                _CHAOS_SPAN_START,
                chaos_end,
                10.0,
                40.0 if event_kind != "corrupt" else 30.0,
            )
            if placed is None:
                continue
            probability_span = {
                "drop": (0.30, 0.90),
                "duplicate": (0.20, 0.90),
                "corrupt": (0.05, 0.40),
            }[event_kind]
            plan.events.append(
                (
                    event_kind,
                    *link,
                    round(plan_random.uniform(*probability_span), 3),
                    *placed,
                )
            )


# ----------------------------------------------------------------------
# Shared skeleton pieces
# ----------------------------------------------------------------------


def _draw_manager_restart_chain(
    plan_random: random.Random,
    plan: ChaosPlan,
    manager_process_id: str,
    dead_intervals: dict[str, list[tuple[float, float]]],
    first_at_span: tuple[float, float],
    down_span: tuple[float, float],
) -> None:
    """One manager power-loss reboot, 35% with the C6 DOUBLE: the
    second loss lands 2-12s after gen-2's boot so it hits recovery
    itself. Sequencing is by construction (R2 >= R1 + down1 + 2 — a
    restart inside a down window targets an unknown pid and raises)
    and both respawns land >=2s before chaos_end (E2)."""
    first_at = round(plan_random.uniform(*first_at_span), 3)
    first_down = round(plan_random.uniform(*down_span), 3)
    first_seed = (
        plan_random.randrange(1, 1_000_000)
        if plan_random.random() < 0.5
        else None
    )
    plan.events.append(("restart", manager_process_id, first_at, first_down, first_seed))
    intervals = dead_intervals.setdefault(manager_process_id, [])
    intervals.append((first_at, first_at + first_down))

    if plan_random.random() >= 0.35:
        return
    second_at = round(first_at + first_down + plan_random.uniform(2.0, 12.0), 3)
    second_down = round(plan_random.uniform(down_span[0], min(down_span[1], 25.0)), 3)
    if second_at + second_down > plan.chaos_end - 2.0:
        return  # the double must still respawn inside the chaos window
    second_seed = (
        plan_random.randrange(1, 1_000_000)
        if plan_random.random() < 0.5
        else None
    )
    plan.events.append(
        ("restart", manager_process_id, second_at, second_down, second_seed)
    )
    intervals.append((second_at, second_at + second_down))


def _draw_pauses(
    plan_random: random.Random,
    plan: ChaosPlan,
    victims: tuple[str, ...],
    dead_intervals: dict[str, list[tuple[float, float]]],
    length_span: tuple[float, float],
) -> None:
    """0-2 SIGSTOP windows across the victim set: disjoint per victim
    (the coordinator raises on overlap), activation always at a live
    instant, resume strictly inside the chaos window. A kill or
    power-loss landing INSIDE a drawn window is the documented
    freeze-composition, not a conflict."""
    pause_windows_by_victim: dict[str, list[tuple[float, float]]] = {}
    for _ in range(plan_random.randrange(0, 3)):
        victim_id = plan_random.choice(victims)
        for _attempt in range(8):
            pause_length = plan_random.uniform(*length_span)
            latest_start = plan.chaos_end - 1.0 - pause_length
            if latest_start <= _CHAOS_SPAN_START:
                break
            pause_start = round(
                plan_random.uniform(_CHAOS_SPAN_START, latest_start), 3
            )
            pause_end = round(pause_start + pause_length, 3)
            victim_windows = pause_windows_by_victim.setdefault(victim_id, [])
            disjoint = all(
                pause_end <= other_start or pause_start >= other_end
                for other_start, other_end in victim_windows
            )
            if disjoint and _pause_start_is_live(
                pause_start, dead_intervals.get(victim_id, [])
            ):
                victim_windows.append((pause_start, pause_end))
                plan.events.append(("pause", victim_id, pause_start, pause_end))
                break


def _draw_storage_window(
    plan_random: random.Random,
    plan: ChaosPlan,
    event_kind: str,
    target: str,
    intervals_by_key: dict[tuple, list[tuple[float, float]]],
    io_error_targets: set[str],
) -> bool:
    """One storage-knob window on ``target``'s disk (docstring
    calibrations). Same-knob windows stay disjoint per target; the
    self-windowing io_error knob is limited to one window per target
    (re-arming would silently replace the first window's seed)."""
    if event_kind == "io_error" and target in io_error_targets:
        return False
    length_span = {
        "slow_disk": (10.0, 30.0),
        "disk_full_window": (10.0, 25.0),
        "read_corruption": (8.0, 25.0),
        "io_error": (8.0, 25.0),
        "misdirect": (8.0, 25.0),
    }[event_kind]
    placed = _place_window(
        plan_random,
        intervals_by_key,
        (event_kind, target),
        _CHAOS_SPAN_START,
        plan.chaos_end,
        *length_span,
    )
    if placed is None:
        return False
    if event_kind == "slow_disk":
        plan.events.append(
            (
                "slow_disk",
                target,
                placed[0],
                round(plan_random.uniform(0.005, 0.04), 3),
                placed[1],
            )
        )
    elif event_kind == "disk_full_window":
        plan.events.append(
            (
                "disk_full_window",
                target,
                placed[0],
                plan_random.randrange(512, 4096),
                placed[1],
            )
        )
    else:
        probability_span = {
            "read_corruption": (0.30, 1.00),
            "io_error": (0.05, 0.25),
            "misdirect": (0.05, 0.20),
        }[event_kind]
        plan.events.append(
            (
                event_kind,
                target,
                placed[0],
                round(plan_random.uniform(*probability_span), 3),
                plan_random.randrange(1, 1_000_000),
                placed[1],
            )
        )
        if event_kind == "io_error":
            io_error_targets.add(target)
    return True
