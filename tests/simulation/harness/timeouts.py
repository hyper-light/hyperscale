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
