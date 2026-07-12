"""
Deterministic NETWORK faults over the multi-process coordinator —
partition/heal, probabilistic loss, added delay, and UDP duplication,
all evaluated at the coordinator's single datagram chokepoint and drawn
from its seeded fault generator, so every fault pattern replays
byte-identically.

This gives the SIM tier the Phase 4 network-fault classes that
previously existed only in REAL mode's ``FaultInjectingTransport``:

* ``schedule_partition`` — cable-cut windows (SWIM must detect the
  peer loss through production suspicion, and the worker's rejoin
  machinery must recover the registration after heal).
* ``schedule_drop_rate`` — seeded probabilistic loss; production retry
  paths must carry a real job through it.
* ``schedule_delay`` — added per-link latency with seeded jitter.
* ``schedule_duplicate`` — duplicated datagrams; the dedup taxonomy
  (gossip dedup-eligible, control messages idempotent by identity)
  must absorb copies without double-effect.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.eviction_recovery_demo import (
    evicting_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
    worker_entry,
)

_PARTITION_CEILING = 220.0
_NEVER = 100_000.0


def _run_partition_heal() -> dict:
    """Manager <-> worker cable-cut window, then heal.

    The manager must lose the worker through PRODUCTION failure
    detection (SWIM suspicion -> death -> deregistration), and after
    the heal the worker must be re-registered — via its own
    staleness/rejoin machinery or the manager's re-sent eviction
    notice, whichever lands first. The cut must be LONG: the failure
    detector deliberately resists declaring death on transient loss
    (LHM growth stretches suspicion — the "no false DEAD under packet
    loss" property). Detection latency also MOVES when the topology
    changes (the WAL-enabled manager consumes different jitter draws,
    shifting probe schedules by tens of seconds), so the cut window is
    sized generously: cut at t=20, heal at t=110 — roughly double the
    detector's sustained-silence latency — with the ceiling leaving
    equal room for the post-heal rejoin. ``evicting_manager_entry``
    with a never-firing evict time is reused purely for its
    worker-count transition watcher.
    """
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_PARTITION_CEILING, seed=53
    )
    coordinator.add_process(
        "manager",
        evicting_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        _NEVER,
    )
    coordinator.add_process(
        "worker",
        worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.schedule_partition("manager", "worker", 20.0, heal_time=110.0)
    return coordinator.run()


def test_partition_detected_and_membership_recovers_after_heal():
    results = _run_partition_heal()

    manager_log = results["manager"]
    count_transitions = [
        entry[1] for entry in manager_log if entry[0] == "worker-count"
    ]
    # Registered (1), lost during the partition window (0), re-registered
    # after heal (1) — production SWIM detection and rejoin, no test
    # backdoors.
    assert count_transitions == [0, 1, 0, 1], manager_log

    lost_time = [
        entry[2]
        for entry in manager_log
        if entry[0] == "worker-count" and entry[1] == 0
    ][-1]
    recovered_time = [
        entry[2]
        for entry in manager_log
        if entry[0] == "worker-count" and entry[1] == 1
    ][-1]
    # Assert the detector's DESIGN bound for a witness-less topology —
    # not merely "inside the window": a generous window can hide
    # multi-x detection drift. Traced decomposition (probe scripts in
    # the Phase 7 series): in a 2-node cluster a suspicion can never
    # gather a second confirmer, so Lifeguard runs the MAX leg of the
    # AD-30 bracket — global_max_timeout (30s) x the bounded prob-OR
    # composition of reliability multipliers (elevated LHM + low
    # Vivaldi confidence composed to ~2.1x here) — then the
    # unwitnessed-death gate adds k final confirm probes (~4s). Cut at
    # 20, suspicion arms on the first missed round (~3-5s later),
    # death lands ~71s after the cut. The ceiling guards the
    # composition staying BOUNDED: pre-fix it compounded to 10-30x
    # (see the AD-30 notes in hierarchical_failure_detector), which
    # this assertion would catch as >85s.
    detection_latency = lost_time - 20.0
    assert 25.0 <= detection_latency <= 85.0, (
        f"partition death latency {detection_latency}s is outside the "
        "witness-less design bound (max-leg x bounded composition + "
        f"gate probes): {manager_log}"
    )
    assert recovered_time > 110.0, manager_log


def test_partition_heal_is_replay_deterministic():
    assert _run_partition_heal() == _run_partition_heal()


def _run_lossy_dispatch() -> dict:
    """A real job through 10% seeded packet loss on the inter-NODE
    links (manager <-> worker, both directions).

    Scoped to node links, not wildcard: the worker's executor pool
    rides same-host pipe IPC that physical packet loss never touches —
    the same scoping REAL mode's ``FaultInjectingTransport`` gets by
    wrapping only harness-managed node servers.
    """
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=90.0, seed=59
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker",
        worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.add_process(
        "client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000)
    )
    coordinator.schedule_drop_rate("manager", "worker", 0.10)
    coordinator.schedule_drop_rate("worker", "manager", 0.10)
    return coordinator.run()


def test_job_completes_through_packet_loss():
    results = _run_lossy_dispatch()
    client_log = results["client"]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log


def test_lossy_dispatch_is_replay_deterministic():
    assert _run_lossy_dispatch() == _run_lossy_dispatch()


def _run_slow_duplicating_dispatch() -> dict:
    """A real job through added jittered delay plus 50% UDP duplication.

    Duplicates exercise the message-idempotency stack end to end: the
    gossip dedup cache absorbs dissemination copies, and control
    messages (leadership heartbeats with monotonic sequences, votes
    keyed by (term, voter), fencing-token job leadership) must be
    idempotent by IDENTITY, so double delivery has no double effect.
    """
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=90.0, seed=61
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker",
        worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.add_process(
        "client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000)
    )
    # Scoped to inter-node links (same rationale as the loss scenario:
    # executor-pool pipe IPC has no WAN latency or UDP duplication).
    for link_src, link_dst in (
        ("manager", "worker"),
        ("worker", "manager"),
        ("client", "manager"),
        ("manager", "client"),
    ):
        coordinator.schedule_delay(
            link_src,
            link_dst,
            0.05,
            at_time=5.0,
            until_time=60.0,
            jitter_seconds=0.02,
        )
    coordinator.schedule_duplicate("manager", "worker", 0.5)
    coordinator.schedule_duplicate("worker", "manager", 0.5)
    return coordinator.run()


def test_job_completes_through_delay_and_duplication():
    results = _run_slow_duplicating_dispatch()
    client_log = results["client"]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log


def test_delay_and_duplication_is_replay_deterministic():
    assert (
        _run_slow_duplicating_dispatch() == _run_slow_duplicating_dispatch()
    )
