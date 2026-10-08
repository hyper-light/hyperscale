"""
Seed-driven fault-schedule generation for the GATE-CLUSTER topology
(the VOPR input side of the L3 gate-tier program).

``generate_fault_plan(seed)`` expands one integer into a complete,
self-describing fault schedule over the canonical gate-cluster job
topology (three peered gates + one manager datacenter + a 2-core worker
+ a client submitting THROUGH the gate tier): gate kills, gate<->gate /
gate<->manager / client<->gate partitions, packet loss, latency, and
UDP duplication at seeded virtual times. The SAME seed always yields
the SAME plan, and the plan drives a coordinator run that replays
byte-identically — so any failing seed is a permanent, shareable
reproducer (``pytest tests/simulation/vopr_gates --sim-replay=<seed>``).

Event parameters are drawn from ranges calibrated against PROBED
timelines of this exact topology (probe scripts in the Phase 7 series
scratch space; observed values quoted per range below), so every
generated schedule is one the system is REQUIRED to survive loudly:
the invariants in ``vopr_runner`` accept no outcome other than a
client-observed terminal job state, a clean status-order history, one
converged gate leader, and a byte-identical replay.

Fault-scoping notes (mirrors the coordinator's physical model):

* ``drop`` / ``duplicate`` are UDP-datagram semantics, so they target
  the membership plane (gate<->gate and gate<->manager SWIM/gossip
  links). The client speaks only TCP — production packet loss under a
  reliable stream is retransmission-masked and surfaces as LATENCY,
  which the ``delay`` kind injects on client links directly.
* ``partition`` cuts streams too (the cable-cut model), so
  client<->gate partitions are the client-connectivity fault class:
  submission-window cuts force the retry cycle across surviving gates
  and delivery-window cuts force the status-poll recovery path.
* Gate RESTART is deliberately NOT in the generated space: gates have
  no durable tier yet (Phase 8 gap — a restarted gate forgets its
  jobs), so restart outcomes are pinned per-scenario in
  ``tests/unit/simulation/sim/test_multiprocess_gate_faults.py``
  instead of being asserted survivable for arbitrary seeds.
"""

import random
from dataclasses import dataclass, field

# The three peered gate processes of the canonical topology — every
# gate-scoped draw picks from these. Killing any ONE gate keeps the
# tier quorate (quorum of a 3-gate cluster is 2), which is what makes
# a single kill a survivable-by-design event.
GATE_PROCESS_IDS = ("gate-a", "gate-b", "gate-c")

_GATE_PAIRS = (
    ("gate-a", "gate-b"),
    ("gate-a", "gate-c"),
    ("gate-b", "gate-c"),
)

# UDP-carrying links (membership plane): drop/duplicate rules match
# datagrams only, so they are scoped to links that actually carry SWIM
# probes and gossip — gate<->gate and gate<->manager, both directions.
_UDP_LINKS = (
    ("gate-a", "gate-b"),
    ("gate-b", "gate-a"),
    ("gate-a", "gate-c"),
    ("gate-c", "gate-a"),
    ("gate-b", "gate-c"),
    ("gate-c", "gate-b"),
    ("gate-a", "manager"),
    ("manager", "gate-a"),
    ("gate-b", "manager"),
    ("manager", "gate-b"),
    ("gate-c", "manager"),
    ("manager", "gate-c"),
)

# Delay applies to streams AND datagrams (physical latency), so the
# client's TCP paths join the delay-eligible links: late status pushes
# racing the poll fallback is exactly the ordering hazard the
# JobStatusOracle exists to catch.
_DELAY_LINKS = _UDP_LINKS + (
    ("client", "gate-a"),
    ("gate-a", "client"),
    ("client", "gate-b"),
    ("gate-b", "client"),
    ("client", "gate-c"),
    ("gate-c", "client"),
)

# Probed baseline (seed 211, no faults, 6s sustained-load workflow):
# gate peers discovered by t=0.5, first gate leader elected ~1.5, DC
# healthy ~5.5, submission accepted ~5.5, dispatch ~8.8, client-visible
# completion at dispatch + duration + push (~14.8). The ceiling leaves
# a killed-gate schedule (survivor detection probed ~70-74.5s +
# leadership takeover + result redelivery) ample post-fault room —
# and deliberately stops BELOW ~149.9: the manager's client-orphan
# machinery arms a 120s grace deadline off a ~t=30.x check tick, and
# reaching that deadline busy-spins the VIRTUAL clock (a probed
# production liveness gap — Timeout._on_timeout re-arms at the same
# instant; wall clocks advance through it, the virtual clock cannot).
# 145 keeps every generated schedule on the safe side of the earliest
# plausible spin instant while exceeding every recovery bound.
_CEILING = 145.0


@dataclass(slots=True)
class FaultPlan:
    """One generated gate-tier scenario: seed, ceiling, fault events.

    Events are plain value tuples so a plan prints readably in a replay
    session and compares structurally in tests:

    * ``("gate_kill", victim_gate, at_time)``
    * ``("gate_gate_partition", gate_x, gate_y, at_time, heal_time)``
    * ``("gate_manager_partition", gate_x, at_time, heal_time)``
    * ``("client_gate_partition", gate_x, at_time, heal_time)``
    * ``("drop", src, dst, probability, at_time, until_time)``
    * ``("delay", src, dst, extra, jitter, at_time, until_time)``
    * ``("duplicate", src, dst, probability, at_time, until_time)``
    """

    seed: int
    ceiling: float = _CEILING
    events: list[tuple] = field(default_factory=list)


def generate_fault_plan(seed: int) -> FaultPlan:
    """Expand ``seed`` into a deterministic gate-tier fault schedule.

    Draws 0-4 fault events (fault-free baselines are deliberately in
    the space — they must hold the same invariants), so schedules
    range from quiet to kill+partition+noise DENSITY with overlapping
    windows. Ranges keep every schedule survivable by design:

    * at most one gate kill per schedule, drawn in [12, 30): safely
      after the probed submission instant (acceptance lands ~5.5-8s
      across probed seeds — the job must be ACCEPTED before the kill,
      both so the redirect/takeover machinery has something to move
      and because of a probed current-behavior gap: from ~2.5s after
      any gate kill, the SURVIVING gates classify the datacenter
      unhealthy permanently and reject new submissions, so a kill
      preceding acceptance would strand submission by design — that
      gap is pinned/documented in the long-horizon scenarios, not
      generated); kills are also mutually exclusive with
      client<->gate partitions, which can legitimately push
      acceptance past any kill instant (probed: a submission-window
      cut delays acceptance to ~10.6s, and longer cuts push it
      further); the kill lands inside the probed execution window
      (dispatch ~8.8 + 6s), and survivor death detection (probed
      ~70-74.5s, the witness-less AD-30 max leg) plus takeover plus
      result redelivery land well inside the 145s ceiling;
    * partitions start in [6, 40) and heal within 6-16s — the same
      inside-the-no-false-death calibration as the L2 VOPR (the
      failure detector's sustained-silence death window is ~38s+, so a
      healed cut must be ridden out without declaring any node dead);
      client<->gate cuts start in [2, 30) so some cover the probed
      submission window (~0-6s) and force the cross-gate retry cycle;
    * loss/duplication scoped to UDP membership links at the same
      rates the L2 VOPR proved survivable (drop 5-25% for 10-30s,
      duplicate 20-60% for 10-40s) — the dedup taxonomy and Lifeguard
      reliability multipliers must absorb them without false deaths;
    * delay adds 20-80ms (+ up to 40ms seeded jitter) for 10-30s on
      any link INCLUDING client TCP paths — bounded so nothing can
      legitimately stall past the ceiling; the system must complete
      THROUGH latency, and late pushes racing polls must never regress
      the client-observed status order.

    At most one partition per category per schedule: overlapping
    same-category cuts could otherwise compose into a full gate
    isolation (both peer links) or a tier-wide submission blackout,
    which are pinned EXTREME scenarios, not survivable-by-construction
    generated ones.
    """
    plan_random = random.Random(seed)
    plan = FaultPlan(seed=seed)

    for _ in range(plan_random.randrange(0, 5)):
        event_kind = plan_random.choice(
            (
                "gate_kill",
                "gate_gate_partition",
                "gate_manager_partition",
                "client_gate_partition",
                "drop",
                "delay",
                "duplicate",
            )
        )
        if event_kind == "gate_kill":
            if any(
                event[0] in ("gate_kill", "client_gate_partition")
                for event in plan.events
            ):
                # At most one kill (the tier must stay quorate), and
                # never alongside a client<->gate cut: the cut can push
                # acceptance past the kill, and post-kill submission is
                # a documented current-behavior gap (survivors blind to
                # the DC), so the combination cannot be survivable-by-
                # construction today.
                continue
            plan.events.append(
                (
                    "gate_kill",
                    plan_random.choice(GATE_PROCESS_IDS),
                    round(plan_random.uniform(12.0, 30.0), 3),
                )
            )
        elif event_kind == "gate_gate_partition":
            if any(
                event[0] == "gate_gate_partition" for event in plan.events
            ):
                continue  # one per category: two could isolate a gate
            gate_x, gate_y = plan_random.choice(_GATE_PAIRS)
            partition_start = round(plan_random.uniform(6.0, 40.0), 3)
            plan.events.append(
                (
                    "gate_gate_partition",
                    gate_x,
                    gate_y,
                    partition_start,
                    round(partition_start + plan_random.uniform(6.0, 16.0), 3),
                )
            )
        elif event_kind == "gate_manager_partition":
            if any(
                event[0] == "gate_manager_partition" for event in plan.events
            ):
                continue  # one per category: two could blind the tier's DC view
            partition_start = round(plan_random.uniform(6.0, 40.0), 3)
            plan.events.append(
                (
                    "gate_manager_partition",
                    plan_random.choice(GATE_PROCESS_IDS),
                    partition_start,
                    round(partition_start + plan_random.uniform(6.0, 16.0), 3),
                )
            )
        elif event_kind == "client_gate_partition":
            if any(
                event[0] in ("client_gate_partition", "gate_kill")
                for event in plan.events
            ):
                # One per category (three could black out submission),
                # and never alongside a gate kill — see the gate_kill
                # branch for the acceptance-ordering rationale.
                continue
            partition_start = round(plan_random.uniform(2.0, 30.0), 3)
            plan.events.append(
                (
                    "client_gate_partition",
                    plan_random.choice(GATE_PROCESS_IDS),
                    partition_start,
                    round(partition_start + plan_random.uniform(6.0, 16.0), 3),
                )
            )
        elif event_kind == "drop":
            link_src, link_dst = plan_random.choice(_UDP_LINKS)
            drop_start = round(plan_random.uniform(0.0, 40.0), 3)
            plan.events.append(
                (
                    "drop",
                    link_src,
                    link_dst,
                    round(plan_random.uniform(0.05, 0.25), 3),
                    drop_start,
                    round(drop_start + plan_random.uniform(10.0, 30.0), 3),
                )
            )
        elif event_kind == "delay":
            link_src, link_dst = plan_random.choice(_DELAY_LINKS)
            delay_start = round(plan_random.uniform(0.0, 40.0), 3)
            plan.events.append(
                (
                    "delay",
                    link_src,
                    link_dst,
                    round(plan_random.uniform(0.02, 0.08), 3),
                    round(plan_random.uniform(0.0, 0.04), 3),
                    delay_start,
                    round(delay_start + plan_random.uniform(10.0, 30.0), 3),
                )
            )
        else:
            link_src, link_dst = plan_random.choice(_UDP_LINKS)
            duplicate_start = round(plan_random.uniform(0.0, 40.0), 3)
            plan.events.append(
                (
                    "duplicate",
                    link_src,
                    link_dst,
                    round(plan_random.uniform(0.2, 0.6), 3),
                    duplicate_start,
                    round(duplicate_start + plan_random.uniform(10.0, 40.0), 3),
                )
            )

    return plan
