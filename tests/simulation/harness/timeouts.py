"""Timeout knobs for the harness lifecycle and condition-waits."""

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class HarnessTimeouts:
    """All timeouts in seconds (REAL mode wall-clock; SIM mode virtual)."""

    stabilization_default: float = 30.0
    """Default budget for `wait_until` cluster-stabilization predicates."""

    stop_default: float = 10.0
    """Per-server graceful stop budget before forced reap escalation."""

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
