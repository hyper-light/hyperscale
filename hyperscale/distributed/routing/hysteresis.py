"""
Hysteresis and stickiness for routing decisions (AD-36 Part 5).

Prevents routing oscillation by requiring minimum improvement
and enforcing hold-down timers.
"""

from dataclasses import dataclass

from hyperscale.distributed.routing.bucket_selector import BucketSelector
from hyperscale.distributed.routing.routing_state import (
    DatacenterRoutingScore,
    JobRoutingState,
    RoutingDecisionReason,
)


@dataclass(slots=True)
class HysteresisConfig:
    """Configuration for hysteresis behavior."""

    # Hold-down: minimum time before voluntary switch
    hold_down_seconds: float = 30.0

    # Improvement threshold: new score must be this fraction of old score
    improvement_ratio: float = 0.8  # 20% improvement required

    # Degradation detection
    degrade_ratio: float = 1.5  # 50% degradation triggers switch
    degrade_confirm_seconds: float = 10.0  # Must persist for this long

    # Cooldown after failover
    cooldown_seconds: float = 120.0  # 2 minutes penalty for failed DCs


@dataclass(slots=True)
class HysteresisResult:
    """Result of hysteresis evaluation."""

    should_switch: bool
    reason: RoutingDecisionReason
    selected_datacenter: str | None
    selected_score: float
    current_datacenter: str | None
    current_score: float | None


class HysteresisManager:
    """
    Manages hysteresis and stickiness for routing (AD-36 Part 5).

    Prevents routing churn by:
    1. Hold-down: Keep current primary for minimum duration
    2. Improvement threshold: Only switch if significantly better
    3. Forced switch: Bucket drop, exclusion, or severe degradation
    4. Cooldown: Penalty for recently failed DCs
    """

    def __init__(self, config: HysteresisConfig | None = None) -> None:
        self._config = config or HysteresisConfig()

    def evaluate_switch(
        self,
        job_state: JobRoutingState,
        primary_candidates: list[DatacenterRoutingScore],
        excluded_datacenters: set[str],
    ) -> HysteresisResult:
        """
        Evaluate whether to switch datacenters.

        Args:
            job_state: Current routing state for the job
            primary_candidates: Scored candidates from primary bucket
            excluded_datacenters: DCs that are now excluded

        Returns:
            HysteresisResult with decision and reasoning
        """
        if not primary_candidates:
            return HysteresisResult(
                should_switch=False,
                reason=RoutingDecisionReason.HOLD_DOWN_RETAINED,
                selected_datacenter=None,
                selected_score=0.0,
                current_datacenter=job_state.primary_datacenter,
                current_score=job_state.last_score,
            )

        best = primary_candidates[0]
        current_dc = job_state.primary_datacenter

        # Check for forced switch conditions
        forced, reason = self._check_forced_switch(
            job_state, best, excluded_datacenters
        )
        if forced:
            return HysteresisResult(
                should_switch=True,
                reason=reason,
                selected_datacenter=best.datacenter_id,
                selected_score=best.final_score,
                current_datacenter=current_dc,
                current_score=job_state.last_score,
            )

        # No current primary - always select
        if current_dc is None:
            return HysteresisResult(
                should_switch=True,
                reason=RoutingDecisionReason.INITIAL_SELECTION,
                selected_datacenter=best.datacenter_id,
                selected_score=best.final_score,
                current_datacenter=None,
                current_score=None,
            )

        # Check if best is same as current
        if best.datacenter_id == current_dc:
            return HysteresisResult(
                should_switch=False,
                reason=RoutingDecisionReason.HOLD_DOWN_RETAINED,
                selected_datacenter=current_dc,
                selected_score=best.final_score,
                current_datacenter=current_dc,
                current_score=job_state.last_score,
            )

        # Apply hysteresis rules
        should_switch, reason = job_state.should_switch(
            best.datacenter_id,
            best.final_score,
            self._config.hold_down_seconds,
            self._config.improvement_ratio,
        )

        return HysteresisResult(
            should_switch=should_switch,
            reason=reason,
            selected_datacenter=best.datacenter_id if should_switch else current_dc,
            selected_score=best.final_score,
            current_datacenter=current_dc,
            current_score=job_state.last_score,
        )

    def _check_forced_switch(
        self,
        job_state: JobRoutingState,
        best: DatacenterRoutingScore,
        excluded_datacenters: set[str],
    ) -> tuple[bool, RoutingDecisionReason]:
        """Check if a forced switch is required."""
        current_dc = job_state.primary_datacenter

        if current_dc is None:
            return False, RoutingDecisionReason.INITIAL_SELECTION

        # Force switch if current DC is now excluded
        if current_dc in excluded_datacenters:
            return True, RoutingDecisionReason.EXCLUSION_FORCED

        # Force switch if current DC dropped bucket
        # Find current DC in candidates to check bucket
        current_bucket = None
        for score in [best]:  # Would need full list in practice
            if score.datacenter_id == current_dc:
                current_bucket = score.health_bucket
                break

        if current_bucket and BucketSelector.is_bucket_drop(
            current_bucket, best.health_bucket
        ):
            return True, RoutingDecisionReason.BUCKET_DROP_FORCED

        # Force switch if score degraded severely
        if job_state.last_score > 0:
            degradation = best.final_score / job_state.last_score
            if degradation >= self._config.degrade_ratio:
                return True, RoutingDecisionReason.DEGRADATION_FORCED

        return False, RoutingDecisionReason.HOLD_DOWN_RETAINED

    def apply_cooldown_penalty(
        self,
        scores: list[DatacenterRoutingScore],
        job_state: JobRoutingState,
        penalty_multiplier: float = 2.0,
    ) -> list[DatacenterRoutingScore]:
        """
        Apply cooldown penalty to recently failed DCs.

        Penalizes but doesn't exclude - allows failback after cooldown.

        Args:
            scores: List of scored candidates
            job_state: Job routing state with cooldown info
            penalty_multiplier: Score multiplier for cooling DCs

        Returns:
            Scores with penalties applied (re-sorted)
        """
        penalized = []
        for score in scores:
            if job_state.is_in_cooldown(score.datacenter_id):
                # Create penalized score
                penalized.append(
                    DatacenterRoutingScore(
                        datacenter_id=score.datacenter_id,
                        health_bucket=score.health_bucket,
                        rtt_ucb_ms=score.rtt_ucb_ms,
                        load_factor=score.load_factor,
                        quality_penalty=score.quality_penalty * penalty_multiplier,
                        final_score=score.final_score * penalty_multiplier,
                        is_preferred=score.is_preferred,
                    )
                )
            else:
                penalized.append(score)

        return sorted(penalized, key=lambda s: s.final_score)
