"""
Seed-driven fault-schedule generation for the MULTI-DATACENTER (L3)
topology — the VOPR input side of the cross-DC program.

``generate_mdc_fault_plan(seed)`` expands one integer into a complete,
self-describing fault schedule over the canonical L3 topology: ONE gate
fronting TWO datacenters (dc-east and dc-west, each a real
``ManagerServer`` + 2-core ``WorkerServer`` + 2 executor children) and
one client submitting an 8-virtual-second soak workflow through the
gate. The SAME seed always yields the SAME plan, and the plan drives a
coordinator run that replays byte-identically — a failing seed is a
permanent reproducer (``pytest tests/simulation/vopr_mdc
--sim-replay=<seed>``).

Every range below is calibrated against timelines MEASURED by probe
(seed 61 anchors, cross-checked on the sweep seeds; the shape is
schedule-stable): gate up ~2.1s, submission accepted ~2.1s (earlier
attempts are production-loud rejections), dispatch reaches the winning
worker ~9.5s, the 8s workflow executes over ~[9.5, 16.3], the client
observes run-window completion ~10.5s. A dead DC flips ``unhealthy`` at
kill + ~29-30s (the gate's 30s manager-heartbeat staleness bound, NOT
SWIM death), and a job stranded on a dead DC reaches the client as a
LOUD ``timeout`` terminal at submit + job_timeout(60) + up to one 15s
AD-34 tracker tick (measured 77.01 for submit 2.12).
"""

import random
from dataclasses import dataclass, field

# The canonical L3 pair. Executor ids follow the production spawner's
# ``executor-<worker_host>-<worker_port>`` naming (verified by probe:
# each 2-core worker spawns exactly 9009/9011).
DATACENTER_IDS = ("dc-east", "dc-west")

DC_PROCESS_IDS: dict[str, tuple[str, ...]] = {
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

_CEILING = 180.0

# Job-level timeout the gate's AD-34 tracker enforces. Stranding events
# resolve as ``timeout`` at submit + this + up to one 15s tracker tick;
# 60s keeps that terminal (~77s) AND every recovery tail well inside
# the 180s ceiling.
JOB_TIMEOUT_SECONDS = 60.0

# Soak workflow length: long enough that mid-execution windows exist
# ([~9.5, ~16.3] measured), short enough that the fault-free baseline
# completes by ~10.5s.
WORKFLOW_DURATION_SECONDS = 8.0

# Fault kinds that may legitimately end the job in a non-completed
# (but always LOUD) terminal:
#
# * dc_loss — a job placed on the killed DC strands; the gate's AD-34
#   tracker times it out (measured: client sees ``timeout``; there is
#   NO mid-flight AD-36 re-dispatch to the surviving DC).
# * dc_partition — the manager's completion notification to the gate is
#   a SINGLE 5s-timeout send with no retry (manager cleans the job up
#   even when it fails), so a window covering that send loses the
#   completion and the job resolves as gate ``timeout`` even though the
#   workflow ran (measured at heal windows both < and > the 30s
#   staleness bound).
# * manager_restart — resume normally completes the job across the
#   reboot, but a crash landing between the ledger record and the
#   submission-payload write legitimately degrades to the loud
#   durable-FAILED path (same reasoning as the single-DC VOPR).
# * disk_full — an exhausted manager WAL may fail the job loudly.
STRANDING_KINDS = frozenset(
    {"dc_loss", "dc_partition", "manager_restart", "disk_full"}
)


@dataclass(slots=True)
class MdcFaultPlan:
    """One generated multi-DC scenario: seed, ceiling, fault events.

    Events are plain value tuples (readable in a replay session,
    structurally comparable in tests):

    * ``("dc_loss", dc, at)`` — SIGKILL that DC's manager AND worker
      AND both executors at ``at`` (total-datacenter loss)
    * ``("dc_partition", dc, at, heal)`` — cable-cut gate <-> that
      DC's manager (streams included) over ``[at, heal)``
    * ``("manager_restart", dc, at, down, fsync_reorder_seed)`` — power
      loss + reboot of that DC's manager from its surviving durable
      disk (managers have the full durable resume tier)
    * ``("slow_disk", dc, at, delay, until)`` — that DC's manager disk
      charges ``delay`` virtual seconds per storage op in the window
    * ``("disk_full", dc, at, remaining_bytes)`` — that DC's manager
      disk accepts that many more bytes, then raises ENOSPC forever
    * ``("gate_link_drop", dc, probability, at, until)`` — seeded UDP
      loss gate <-> that DC's manager, both directions (SWIM probe /
      heartbeat loss; TCP job traffic is unaffected by design)
    * ``("client_partition", at, heal)`` — cable-cut client <-> gate
    * ``("client_delay", extra, jitter, at, until)`` — added latency
      client <-> gate, both directions (applies to TCP too: late
      pushes race the client's status polls)
    * ``("client_duplicate", probability, at, until)`` — UDP
      duplication client <-> gate. The job path client<->gate is pure
      TCP (measured inert), so this PINS the claim that no
      fault-eligible datagrams ride the client link; if UDP appears
      there later, the dedup taxonomy must absorb the copies.
    """

    seed: int
    ceiling: float = _CEILING
    events: list[tuple] = field(default_factory=list)

    def can_strand(self) -> bool:
        """Whether any scheduled event may legitimately end the job in
        a non-completed (loud) terminal."""
        return any(event[0] in STRANDING_KINDS for event in self.events)

    def lost_datacenters(self) -> set[str]:
        """Datacenters killed outright by a dc_loss event (their final
        gate classification is legitimately ``unhealthy``)."""
        return {event[1] for event in self.events if event[0] == "dc_loss"}


def generate_mdc_fault_plan(seed: int) -> MdcFaultPlan:
    """Expand ``seed`` into a deterministic multi-DC fault schedule.

    Draws 0-4 events (fault-free baselines are deliberately in the
    space, and 2-4 draws give overlapping-fault density). Ranges keep
    every schedule survivable-or-loud by design:

    * dc_loss at [10, 30]: from mid-execution (dispatch lands ~9.5) to
      just past the baseline completion — both the stranded-job path
      (loud AD-34 ``timeout``) and the idle-DC-death path stay inside
      the ceiling with ~100s to spare for detection + terminal.
      At most one per plan, and never together with manager_restart
      (restarting an already-killed process would resurrect it — the
      coordinator's restart primitive respawns from spec).
    * dc_partition at [6, 40], windows 8-35s: windows may or may not
      cover the single completion-push send and may or may not exceed
      the 30s staleness bound — all four quadrants are legitimate
      schedules. Heal lands by t<=75, leaving >100s for heartbeat
      resumption and reclassification (measured recovery: heal + ~9s).
    * manager_restart at [5, 20], down 15-40s: reboot completes by
      t=60, leaving 120s of ceiling for WAL replay, worker
      re-admission, resume re-dispatch, and the client's completion —
      and the AD-34 job timeout (60s + tick) stays clear of the resume
      tail measured by probe. Half the reboots carry fsync_reorder
      torn-crash debris (recovery must read straight through it).
    * slow_disk starts [0, 12): the job's WAL-append lifetime is
      ~[2.1, 10.5] (submission ledger through completion records), so
      later windows would fault an idle disk. Delays 5-40ms per op,
      windows 10-30s — bounded, so the job must complete THROUGH the
      slow disk (never a legitimate strand).
    * disk_full arms [8, 20) with 256-2048 further bytes: after the
      cluster can form but inside the job's WAL lifetime, so the
      winning manager's ledger may genuinely exhaust mid-job (loud
      failure allowed; silence never). The losing DC's manager
      exhausting must not affect completion.
    * storage events and manager_restart never target the same DC in
      one plan: the child re-arms storage knobs at ABSOLUTE virtual
      times on reboot, which would replay a past disk_full instant at
      gen-2 boot — a different scenario than the one drawn.
    * gate_link_drop 5-25% over 10-30s windows: UDP-only by the
      coordinator's fault model, so SWIM probes/heartbeats blur but
      TCP dispatch traffic is untouched — classification may flap,
      must end healthy, and the job must complete.
    * client_partition at [0, 40], windows 6-20s: covers submission
      (production retry-to-acceptance) and completion delivery (the
      client's poll fallback + the gate's push retry converge after
      heal — measured); heals by t<=60, so the terminal always lands
      inside the ceiling.
    * client_delay 20-80ms (+0-40ms seeded jitter) over 10-30s
      windows: TCP included — late pushes race status polls; the
      client-log oracle catches any ordering violation.
    * client_duplicate 20-60% over 10-40s windows: measured inert
      (no datagrams on the client link) — pins that claim cheaply.
    """
    plan_random = random.Random(seed)
    plan = MdcFaultPlan(seed=seed)

    for _ in range(plan_random.randrange(0, 5)):
        event_kind = plan_random.choice(
            (
                "dc_loss",
                "dc_partition",
                "manager_restart",
                "slow_disk",
                "disk_full",
                "gate_link_drop",
                "client_partition",
                "client_delay",
                "client_duplicate",
            )
        )
        kinds_drawn = {event[0] for event in plan.events}

        if event_kind == "dc_loss":
            if "dc_loss" in kinds_drawn or "manager_restart" in kinds_drawn:
                continue  # one total loss per plan; no kill+restart mix
            plan.events.append(
                (
                    "dc_loss",
                    plan_random.choice(DATACENTER_IDS),
                    round(plan_random.uniform(10.0, 30.0), 3),
                )
            )
        elif event_kind == "dc_partition":
            if "dc_partition" in kinds_drawn:
                continue  # one cut window per plan keeps links single-toggled
            partition_start = round(plan_random.uniform(6.0, 40.0), 3)
            plan.events.append(
                (
                    "dc_partition",
                    plan_random.choice(DATACENTER_IDS),
                    partition_start,
                    round(partition_start + plan_random.uniform(8.0, 35.0), 3),
                )
            )
        elif event_kind == "manager_restart":
            if "manager_restart" in kinds_drawn or "dc_loss" in kinds_drawn:
                continue
            storage_dcs = {
                event[1]
                for event in plan.events
                if event[0] in ("slow_disk", "disk_full")
            }
            restart_candidates = [
                datacenter_id
                for datacenter_id in DATACENTER_IDS
                if datacenter_id not in storage_dcs
            ]
            if not restart_candidates:
                continue
            fsync_reorder_seed = (
                plan_random.randrange(1, 1_000_000)
                if plan_random.random() < 0.5
                else None
            )
            plan.events.append(
                (
                    "manager_restart",
                    plan_random.choice(restart_candidates),
                    round(plan_random.uniform(5.0, 20.0), 3),
                    round(plan_random.uniform(15.0, 40.0), 3),
                    fsync_reorder_seed,
                )
            )
        elif event_kind == "slow_disk":
            if "slow_disk" in kinds_drawn:
                continue  # windows toggle one knob; keep them disjoint
            candidates = _storage_candidates(plan)
            if not candidates:
                continue
            slow_start = round(plan_random.uniform(0.0, 12.0), 3)
            plan.events.append(
                (
                    "slow_disk",
                    plan_random.choice(candidates),
                    slow_start,
                    round(plan_random.uniform(0.005, 0.04), 3),
                    round(slow_start + plan_random.uniform(10.0, 30.0), 3),
                )
            )
        elif event_kind == "disk_full":
            if "disk_full" in kinds_drawn:
                continue  # one exhaustion point per schedule
            candidates = _storage_candidates(plan)
            if not candidates:
                continue
            plan.events.append(
                (
                    "disk_full",
                    plan_random.choice(candidates),
                    round(plan_random.uniform(8.0, 20.0), 3),
                    plan_random.randrange(256, 2048),
                )
            )
        elif event_kind == "gate_link_drop":
            drop_start = round(plan_random.uniform(0.0, 40.0), 3)
            plan.events.append(
                (
                    "gate_link_drop",
                    plan_random.choice(DATACENTER_IDS),
                    round(plan_random.uniform(0.05, 0.25), 3),
                    drop_start,
                    round(drop_start + plan_random.uniform(10.0, 30.0), 3),
                )
            )
        elif event_kind == "client_partition":
            if "client_partition" in kinds_drawn:
                continue  # one client cut per plan
            client_cut_start = round(plan_random.uniform(0.0, 40.0), 3)
            plan.events.append(
                (
                    "client_partition",
                    client_cut_start,
                    round(client_cut_start + plan_random.uniform(6.0, 20.0), 3),
                )
            )
        elif event_kind == "client_delay":
            delay_start = round(plan_random.uniform(0.0, 40.0), 3)
            plan.events.append(
                (
                    "client_delay",
                    round(plan_random.uniform(0.02, 0.08), 3),
                    round(plan_random.uniform(0.0, 0.04), 3),
                    delay_start,
                    round(delay_start + plan_random.uniform(10.0, 30.0), 3),
                )
            )
        else:
            duplicate_start = round(plan_random.uniform(0.0, 40.0), 3)
            plan.events.append(
                (
                    "client_duplicate",
                    round(plan_random.uniform(0.2, 0.6), 3),
                    duplicate_start,
                    round(duplicate_start + plan_random.uniform(10.0, 40.0), 3),
                )
            )

    return plan


def _storage_candidates(plan: MdcFaultPlan) -> list[str]:
    """DCs a storage event may target: never a DC whose manager also
    restarts in this plan (absolute-time knob re-arming on reboot
    would mutate the drawn scenario)."""
    restart_dcs = {
        event[1] for event in plan.events if event[0] == "manager_restart"
    }
    return [
        datacenter_id
        for datacenter_id in DATACENTER_IDS
        if datacenter_id not in restart_dcs
    ]
