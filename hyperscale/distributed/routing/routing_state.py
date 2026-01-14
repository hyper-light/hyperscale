"""
Routing state for tracking datacenter selection decisions (AD-36 Section 13.4).

Provides per-job routing state for hysteresis and stickiness.
"""

import time
from dataclasses import dataclass, field
from enum import Enum


class RoutingDecisionReason(str, Enum):
    """Reason for a routing decision."""

    INITIAL_SELECTION = "initial_selection"
    HOLD_DOWN_RETAINED = "hold_down_retained"
    IMPROVEMENT_THRESHOLD_MET = "improvement_threshold_met"
    BUCKET_DROP_FORCED = "bucket_drop_forced"
    EXCLUSION_FORCED = "exclusion_forced"
    DEGRADATION_FORCED = "degradation_forced"
    COOLDOWN_PENALTY = "cooldown_penalty"


@dataclass(slots=True)
class DatacenterRoutingScore:
    datacenter_id: str
    health_bucket: str
    rtt_ucb_ms: float
    load_factor: float
    quality_penalty: float
    final_score: float
    is_preferred: bool = False
    health_severity_weight: float = 1.0

    @classmethod
    def calculate(
        cls,
        datacenter_id: str,
        health_bucket: str,
        rtt_ucb_ms: float,
        utilization: float,
        queue_depth: int,
        circuit_breaker_pressure: float,
        coordinate_quality: float,
        is_preferred: bool = False,
        preference_multiplier: float = 0.9,
        health_severity_weight: float = 1.0,
    ) -> "DatacenterRoutingScore":
        """
        Calculate routing score for a datacenter (AD-36 Part 4).

        Formula:
            load_factor = 1.0 + A_UTIL*util + A_QUEUE*queue + A_CB*cb
            quality_penalty = 1.0 + A_QUALITY*(1.0 - quality)
            score = rtt_ucb * load_factor * quality_penalty * preference_mult * health_severity_weight

        Lower scores are better.
        """
        a_util = 0.5
        a_queue = 0.3
        a_cb = 0.2
        a_quality = 0.5
        queue_smoothing = 10.0
        load_factor_max = 5.0
        quality_penalty_max = 2.0

        queue_normalized = queue_depth / (queue_depth + queue_smoothing)
        load_factor = (
            1.0
            + a_util * utilization
            + a_queue * queue_normalized
            + a_cb * circuit_breaker_pressure
        )
        load_factor = min(load_factor, load_factor_max)

        quality_penalty = 1.0 + a_quality * (1.0 - coordinate_quality)
        quality_penalty = min(quality_penalty, quality_penalty_max)

        final_score = (
            rtt_ucb_ms * load_factor * quality_penalty * health_severity_weight
        )

        if is_preferred:
            final_score *= preference_multiplier

        return cls(
            datacenter_id=datacenter_id,
            health_bucket=health_bucket,
            rtt_ucb_ms=rtt_ucb_ms,
            load_factor=load_factor,
            quality_penalty=quality_penalty,
            final_score=final_score,
            is_preferred=is_preferred,
            health_severity_weight=health_severity_weight,
        )


@dataclass(slots=True)
class JobRoutingState:
    """
    Per-job routing state for hysteresis and stickiness (AD-36 Section 13.4.5).

    Tracks the current primary datacenter and decision timing to prevent
    routing oscillation.
    """

    job_id: str
    primary_datacenter: str | None = None
    primary_selected_at: float = 0.0
    last_score: float = 0.0
    switch_count: int = 0
    forced_switch_at: float | None = None

    # Cooldown tracking for failed DCs
    failed_datacenters: dict[str, float] = field(default_factory=dict)

    def should_switch(
        self,
        new_datacenter: str,
        new_score: float,
        hold_down_seconds: float = 30.0,
        improvement_ratio: float = 0.8,  # 20% improvement required
    ) -> tuple[bool, RoutingDecisionReason]:
        """
        Determine if we should switch to a new datacenter (AD-36 Part 5).

        Args:
            new_datacenter: Candidate datacenter
            new_score: Score of candidate
            hold_down_seconds: Minimum time before voluntary switch
            improvement_ratio: Required score improvement ratio

        Returns:
            (should_switch, reason)
        """
        now = time.monotonic()

        # No current primary - always switch
        if self.primary_datacenter is None:
            return True, RoutingDecisionReason.INITIAL_SELECTION

        # Same datacenter - no switch
        if new_datacenter == self.primary_datacenter:
            return False, RoutingDecisionReason.HOLD_DOWN_RETAINED

        # Check hold-down timer
        time_since_selection = now - self.primary_selected_at
        if time_since_selection < hold_down_seconds:
            return False, RoutingDecisionReason.HOLD_DOWN_RETAINED

        # Check improvement threshold
        if new_score < self.last_score * improvement_ratio:
            return True, RoutingDecisionReason.IMPROVEMENT_THRESHOLD_MET

        return False, RoutingDecisionReason.HOLD_DOWN_RETAINED

    def force_switch(
        self,
        reason: RoutingDecisionReason,
    ) -> None:
        """Mark that a forced switch is required."""
        self.forced_switch_at = time.monotonic()
        self.primary_datacenter = None

    def reset_primary_selection(self) -> None:
        """Reset the primary selection to force re-routing."""
        self.primary_datacenter = None
        self.primary_selected_at = 0.0
        self.last_score = 0.0
        self.forced_switch_at = time.monotonic()

    def select_primary(
        self,
        datacenter: str,
        score: float,
    ) -> None:
        """Record selection of a primary datacenter."""
        self.primary_datacenter = datacenter
        self.primary_selected_at = time.monotonic()
        self.last_score = score
        self.switch_count += 1
        self.forced_switch_at = None

    def record_failure(
        self,
        datacenter: str,
        cooldown_seconds: float = 120.0,
    ) -> None:
        """Record a dispatch failure to a datacenter."""
        self.failed_datacenters[datacenter] = time.monotonic() + cooldown_seconds

    def is_in_cooldown(self, datacenter: str) -> bool:
        """Check if a datacenter is in cooldown from recent failure."""
        cooldown_until = self.failed_datacenters.get(datacenter)
        if cooldown_until is None:
            return False
        return time.monotonic() < cooldown_until

    def cleanup_expired_cooldowns(self) -> None:
        """Remove expired cooldowns."""
        now = time.monotonic()
        expired = [dc for dc, until in self.failed_datacenters.items() if now >= until]
        for dc in expired:
            del self.failed_datacenters[dc]


@dataclass
class RoutingStateManager:
    """
    Manages routing state for all jobs (AD-36 Section 13.4).

    Provides hysteresis and stickiness across routing decisions.
    """

    _job_states: dict[str, JobRoutingState] = field(default_factory=dict)

    # Configuration
    hold_down_seconds: float = 30.0
    improvement_ratio: float = 0.8
    cooldown_seconds: float = 120.0

    def get_or_create_state(self, job_id: str) -> JobRoutingState:
        """Get or create routing state for a job."""
        if job_id not in self._job_states:
            self._job_states[job_id] = JobRoutingState(job_id=job_id)
        return self._job_states[job_id]

    def remove_state(self, job_id: str) -> None:
        """Remove routing state for a completed job."""
        self._job_states.pop(job_id, None)

    def reset_primary_for_datacenters(self, datacenter_ids: set[str]) -> int:
        """Reset routing state for jobs in affected datacenters."""
        return len(self.reset_primary_for_datacenters_with_jobs(datacenter_ids))

    def reset_primary_for_datacenters_with_jobs(
        self,
        datacenter_ids: set[str],
    ) -> list[str]:
        """Reset routing state for jobs and return affected job IDs."""
        if not datacenter_ids:
            return []

        reset_jobs: list[str] = []
        for job_id, job_state in self._job_states.items():
            if job_state.primary_datacenter in datacenter_ids:
                job_state.reset_primary_selection()
                reset_jobs.append(job_id)

        return reset_jobs

    def cleanup_stale_states(self, max_age_seconds: float = 3600.0) -> int:
        """Remove stale job states older than max_age."""
        now = time.monotonic()
        stale = [
            job_id
            for job_id, state in self._job_states.items()
            if state.primary_selected_at > 0
            and now - state.primary_selected_at > max_age_seconds
        ]
        for job_id in stale:
            del self._job_states[job_id]
        return len(stale)

    def get_job_count(self) -> int:
        """Get number of tracked jobs."""
        return len(self._job_states)
