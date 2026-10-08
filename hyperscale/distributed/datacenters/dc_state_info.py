"""``DCStateInfo`` -- pickled under the namespace
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

from dataclasses import dataclass, field

from .cross_dc_correlation_shared import _DEFAULT_CLOCK
from .dc_health_state import DCHealthState
from .latency_sample import LatencySample


@dataclass(slots=True)
class DCStateInfo:
    """Per-datacenter state tracking with anti-flapping."""

    datacenter_id: str
    current_state: DCHealthState = DCHealthState.HEALTHY
    state_entered_at: float = 0.0
    last_failure_at: float = 0.0
    last_recovery_at: float = 0.0
    failure_count_in_window: int = 0
    recovery_count_in_window: int = 0
    consecutive_failures: int = 0
    consecutive_recoveries: int = 0

    # Latency tracking
    latency_samples: list[LatencySample] = field(default_factory=list)
    avg_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    latency_elevated: bool = False

    # LHM tracking (Local Health Multiplier score reported by DC).
    # ``current_lhm_score`` retains the original semantics for back-compat
    # (single score representing the DC's overall stress) — it's
    # populated as the maximum across reporting tiers so a worker
    # tier saturating still raises the DC-level signal.
    current_lhm_score: int = 0
    lhm_stressed: bool = False
    # AD-19 addendum (Phase D): per-tier LHM tracking. Keys are
    # ``"manager"``, ``"worker"``, ``"gate"``. Values are the
    # most-recently-reported LHM score for that tier in this DC.
    # Lets correlation analysis distinguish "all workers stressed"
    # (likely systemic load) from "one manager stressed" (likely
    # isolated overload) before triggering eviction decisions.
    per_tier_lhm_scores: dict[str, int] = field(default_factory=dict)

    # Extension tracking
    active_extensions: int = 0  # Number of workers currently with extensions

    def is_confirmed_failed(self, confirmation_seconds: float) -> bool:
        """Check if failure is confirmed (sustained long enough)."""
        if self.current_state not in (DCHealthState.FAILING, DCHealthState.FAILED):
            return False
        elapsed = _DEFAULT_CLOCK.monotonic() - self.state_entered_at
        return elapsed >= confirmation_seconds

    def is_confirmed_recovered(self, confirmation_seconds: float) -> bool:
        """Check if recovery is confirmed (sustained long enough)."""
        if self.current_state != DCHealthState.RECOVERING:
            return self.current_state == DCHealthState.HEALTHY
        elapsed = _DEFAULT_CLOCK.monotonic() - self.state_entered_at
        return elapsed >= confirmation_seconds

    def is_flapping(self, threshold: int, window_seconds: float) -> bool:
        """Check if DC is flapping (too many state changes)."""
        if self.current_state == DCHealthState.FLAPPING:
            return True
        # Check if total transitions in window exceed threshold
        now = _DEFAULT_CLOCK.monotonic()
        window_start = now - window_seconds
        if self.state_entered_at >= window_start:
            total_transitions = (
                self.failure_count_in_window + self.recovery_count_in_window
            )
            return total_transitions >= threshold
        return False
