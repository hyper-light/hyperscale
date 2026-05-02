"""Timeout knobs for the harness lifecycle and condition-waits."""

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class HarnessTimeouts:
    """All timeouts in seconds (REAL mode wall-clock; SIM mode virtual)."""

    stabilization_default: float = 30.0
    """Default budget for `wait_until` cluster-stabilization predicates."""

    stop_default: float = 30.0
    """Per-server graceful stop budget before forced reap escalation.

    Generous because larger topologies (L2 with 3 managers + 2 workers
    stopping in parallel) can have shutdowns that block on cancellation
    of in-flight async ops. With too-tight a budget, wait_for cancels
    server.stop mid-flight and the server's bg tasks survive as leaks.
    """

    condition_default: float = 15.0
    """Default budget for `wait_until` predicates without an explicit timeout."""

    workload_default: float = 120.0
    """Default budget for end-to-end workload completion."""

    reap_per_node: float = 15.0
    """Total per-node reap budget across graceful → forced → SIGKILL."""

    pid_track_interval: float = 1.0
    """Interval between worker-subprocess PID re-snapshots."""

    invariant_poll_interval: float = 0.1
    """Interval between continuous-invariant checks (Phase 2+)."""

    quiescence_poll_interval: float = 0.05
    """How often the supervisor checks for asyncio task quiescence during
    shutdown. Small enough that a healthy system settles in tens of ms."""

    quiescence_stable_ticks: int = 3
    """Number of consecutive polls during which the count of harness-
    spawned tasks must NOT increase before declaring quiescence. Three
    ticks at 50 ms = 150 ms of "no new work being scheduled." This
    distinguishes "tasks still wrapping up after stop()" from
    "system is making forward progress and we have not actually
    quiesced yet."""

    quiescence_max_seconds: float = 30.0
    """Hard ceiling on the quiescence wait. The expected path is
    convergence in tens to low hundreds of ms; if we hit this, the
    cluster is genuinely refusing to settle and the surviving tasks
    will surface as leaks in the subsequent force-cancel phase."""

    force_cancel_settle_seconds: float = 3.0
    """Total budget for the persistent-cancel loop in
    ``Supervisor._force_cancel_survivors``. The loop re-cancels and
    waits in rounds until tasks either exit or the budget is gone."""

    force_cancel_round_seconds: float = 0.25
    """Per-round wait inside ``_force_cancel_survivors``. After each
    round, surviving tasks get cancelled again — this defeats the
    'while self._running: except CancelledError: pass' pattern that
    swallows the first cancel. Several short rounds beat one long
    wait because each round forces a fresh re-cancel."""
