"""
Seed-driven fault-schedule generation (the VOPR input side).

``generate_fault_plan(seed)`` expands one integer into a complete,
self-describing fault schedule over the canonical L2 job topology
(manager + 2-core worker + client): kills, partitions, packet loss,
latency, and UDP duplication at seeded virtual times. The SAME seed
always yields the SAME plan, and the plan drives a coordinator run that
replays byte-identically — so any failing seed is a permanent, shareable
reproducer (``pytest tests/simulation/vopr --sim-replay=<seed>``).

Event parameters are drawn from ranges calibrated against the
production stack's real tolerances (e.g. partitions stay shorter than
the ~38s sustained-silence window after which the failure detector
legitimately declares death), so every generated schedule is one the
system is REQUIRED to survive: the invariants in ``vopr_runner`` accept
no outcome other than a client-observed terminal job state and a
byte-identical replay.
"""

import random
from dataclasses import dataclass, field


# The two executor-pool children of the canonical worker — the only
# legal kill victims: an L2 has one manager and one worker, whose death
# makes job failure the EXPECTED outcome; killing one of two executors
# must instead be absorbed by the workflow-retry path.
_EXECUTOR_IDS = ("executor-sim-wkr-9009", "executor-sim-wkr-9011")

_NODE_LINKS = (
    ("manager", "worker"),
    ("worker", "manager"),
)

_CEILING = 100.0


@dataclass(slots=True)
class FaultPlan:
    """One generated scenario: a seed, a ceiling, and its fault events.

    Events are plain value tuples so a plan prints readably in a replay
    session and compares structurally in tests:

    * ``("kill", victim_id, at_time)``
    * ``("partition", a, b, at_time, heal_time)``
    * ``("drop", src, dst, probability, at_time, until_time)``
    * ``("delay", src, dst, extra, jitter, at_time, until_time)``
    * ``("duplicate", src, dst, probability, at_time, until_time)``
    * ``("slow_disk", at_time, delay_seconds, until_time)`` — the
      manager's disk (WAL group commits, idempotency ledger,
      checkpoints) charges virtual time per operation in the window
    * ``("disk_full", at_time, remaining_bytes)`` — the manager's disk
      accepts that many more bytes, then every write raises ENOSPC
    """

    seed: int
    ceiling: float = _CEILING
    events: list[tuple] = field(default_factory=list)


def generate_fault_plan(seed: int) -> FaultPlan:
    """Expand ``seed`` into a deterministic fault schedule.

    Draws 0-3 fault events (schedules with NO faults are deliberately in
    the space — the fault-free baseline must hold under the same
    invariants). Ranges keep every schedule survivable by design:

    * at most one executor kill, after dispatch has plausibly begun;
    * partitions heal within 6-16s — well inside the failure detector's
      ~38s sustained-silence death window, so the job path must ride
      them out without losing the worker;
    * loss/delay/duplication scoped to inter-node links (executor-pool
      pipe IPC has no WAN faults — same scoping as REAL mode);
    * slow_disk delays are bounded (5-40ms per operation, 10-30s
      windows) so storage never legitimately stalls the job past the
      ceiling — the system must complete THROUGH a slow disk. Windows
      START in [0, 12): the job dispatches at roughly t=8-15 (cluster
      formation gates it), so later windows would fault an idle disk;
    * disk_full budgets are drawn small (256-2048 further bytes) and
      arm in [8, 20) — after the cluster can form (a manager whose disk
      is full from BOOT cannot legitimately accept work at all) but
      inside the job's WAL-append lifetime, so the manager's ledger
      genuinely exhausts mid-job. The system may fail the job but must
      fail it LOUDLY (a client-observed terminal state, never
      silence). fsync_reorder is exercised by the
      in-process crash/recovery scenarios instead: it only manifests
      through a crash + RESTART cycle, and the coordinator deliberately
      has no process-restart primitive.
    """
    plan_random = random.Random(seed)
    plan = FaultPlan(seed=seed)

    for _ in range(plan_random.randrange(0, 4)):
        event_kind = plan_random.choice(
            (
                "kill",
                "partition",
                "drop",
                "delay",
                "duplicate",
                "slow_disk",
                "disk_full",
            )
        )
        if event_kind == "kill":
            if any(event[0] == "kill" for event in plan.events):
                continue  # at most one kill per schedule
            plan.events.append(
                (
                    "kill",
                    plan_random.choice(_EXECUTOR_IDS),
                    round(plan_random.uniform(15.0, 45.0), 3),
                )
            )
        elif event_kind == "partition":
            partition_start = round(plan_random.uniform(10.0, 45.0), 3)
            plan.events.append(
                (
                    "partition",
                    "manager",
                    "worker",
                    partition_start,
                    round(partition_start + plan_random.uniform(6.0, 16.0), 3),
                )
            )
        elif event_kind == "drop":
            link_src, link_dst = plan_random.choice(_NODE_LINKS)
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
            link_src, link_dst = plan_random.choice(_NODE_LINKS)
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
        elif event_kind == "duplicate":
            link_src, link_dst = plan_random.choice(_NODE_LINKS)
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
        elif event_kind == "slow_disk":
            if any(event[0] == "slow_disk" for event in plan.events):
                continue  # windows toggle one knob; keep them disjoint
            slow_start = round(plan_random.uniform(0.0, 12.0), 3)
            plan.events.append(
                (
                    "slow_disk",
                    slow_start,
                    round(plan_random.uniform(0.005, 0.04), 3),
                    round(slow_start + plan_random.uniform(10.0, 30.0), 3),
                )
            )
        else:
            if any(event[0] == "disk_full" for event in plan.events):
                continue  # one exhaustion point per schedule
            plan.events.append(
                (
                    "disk_full",
                    round(plan_random.uniform(8.0, 20.0), 3),
                    plan_random.randrange(256, 2048),
                )
            )

    return plan
