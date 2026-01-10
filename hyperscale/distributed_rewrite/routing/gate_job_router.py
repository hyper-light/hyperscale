"""
Gate job router with Vivaldi-based multi-factor routing (AD-36).

Integrates all routing components to make datacenter selection decisions.
"""

from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed_rewrite.routing.bootstrap import BootstrapModeManager
from hyperscale.distributed_rewrite.routing.bucket_selector import BucketSelector
from hyperscale.distributed_rewrite.routing.candidate_filter import (
    CandidateFilter,
    DatacenterCandidate,
)
from hyperscale.distributed_rewrite.routing.fallback_chain import (
    FallbackChain,
    FallbackChainBuilder,
)
from hyperscale.distributed_rewrite.routing.hysteresis import (
    HysteresisConfig,
    HysteresisManager,
)
from hyperscale.distributed_rewrite.routing.routing_state import (
    DatacenterRoutingScore,
    JobRoutingState,
    RoutingDecisionReason,
    RoutingStateManager,
)
from hyperscale.distributed_rewrite.routing.scoring import RoutingScorer, ScoringConfig
from hyperscale.distributed_rewrite.swim.coordinates.coordinate_tracker import (
    CoordinateTracker,
)


@dataclass(slots=True)
class RoutingDecision:
    """Result of a routing decision."""

    job_id: str
    primary_datacenters: list[str]
    fallback_datacenters: list[str]
    primary_bucket: str | None
    reason: RoutingDecisionReason
    in_bootstrap_mode: bool
    scores: dict[str, float]

    # State tracking
    switched: bool
    previous_primary: str | None


@dataclass
class GateJobRouterConfig:
    """Configuration for the gate job router."""

    # Scoring
    scoring_config: ScoringConfig = field(default_factory=ScoringConfig)

    # Hysteresis
    hysteresis_config: HysteresisConfig = field(default_factory=HysteresisConfig)

    # Selection limits
    max_primary_dcs: int = 2

    # Cooldown penalty
    cooldown_penalty_multiplier: float = 2.0


class GateJobRouter:
    """
    Vivaldi-based job router for gates (AD-36).

    Routes jobs to optimal datacenters while:
    - Preserving AD-17 health bucket ordering
    - Using Vivaldi RTT UCB for latency awareness
    - Applying multi-factor scoring (RTT × load × quality)
    - Enforcing hysteresis to prevent routing churn
    - Supporting graceful bootstrap mode

    Usage:
        router = GateJobRouter(
            coordinate_tracker=coord_tracker,
            get_datacenter_candidates=my_dc_getter,
        )

        decision = router.route_job(
            job_id="job-123",
            preferred_datacenters={"us-east-1"},
        )

        # Use decision.primary_datacenters and decision.fallback_datacenters
    """

    def __init__(
        self,
        coordinate_tracker: CoordinateTracker | None = None,
        get_datacenter_candidates: Callable[[], list[DatacenterCandidate]] | None = None,
        config: GateJobRouterConfig | None = None,
    ) -> None:
        self._config = config or GateJobRouterConfig()
        self._coordinate_tracker = coordinate_tracker

        # Injected data source
        self._get_datacenter_candidates = get_datacenter_candidates or (lambda: [])

        # Components
        self._candidate_filter = CandidateFilter()
        self._bucket_selector = BucketSelector()
        self._scorer = RoutingScorer(self._config.scoring_config)
        self._bootstrap_manager = BootstrapModeManager()
        self._hysteresis_manager = HysteresisManager(self._config.hysteresis_config)
        self._fallback_builder = FallbackChainBuilder(self._bucket_selector)
        self._state_manager = RoutingStateManager(
            hold_down_seconds=self._config.hysteresis_config.hold_down_seconds,
            improvement_ratio=self._config.hysteresis_config.improvement_ratio,
            cooldown_seconds=self._config.hysteresis_config.cooldown_seconds,
        )

    def route_job(
        self,
        job_id: str,
        preferred_datacenters: set[str] | None = None,
    ) -> RoutingDecision:
        """
        Route a job to optimal datacenters (AD-36 Part 9).

        Flow:
        1. Get datacenter candidates
        2. Filter (exclude UNHEALTHY, no managers, etc.)
        3. Select primary health bucket
        4. Check bootstrap mode
        5. Score candidates
        6. Apply hysteresis
        7. Build fallback chain

        Args:
            job_id: Job identifier
            preferred_datacenters: Optional set of preferred DC IDs

        Returns:
            RoutingDecision with primary and fallback datacenters
        """
        # Get job routing state
        job_state = self._state_manager.get_or_create_state(job_id)
        job_state.cleanup_expired_cooldowns()

        # Step 1: Get candidates
        candidates = self._get_datacenter_candidates()

        # Enrich with Vivaldi data
        self._enrich_with_vivaldi(candidates)

        # Step 2: Filter candidates
        eligible, excluded = self._candidate_filter.filter_datacenters(candidates)

        if not eligible:
            return self._empty_decision(job_id, job_state)

        # Step 3: Select primary bucket
        bucket_result = self._bucket_selector.select_bucket(eligible)

        if not bucket_result.primary_candidates:
            return self._empty_decision(job_id, job_state)

        # Step 4: Check bootstrap mode
        in_bootstrap = self._check_bootstrap_mode()

        # Step 5: Score candidates
        if in_bootstrap:
            # Use capacity-based ranking
            sorted_primary = self._bootstrap_manager.rank_by_capacity(
                bucket_result.primary_candidates
            )
            primary_scores = [
                DatacenterRoutingScore(
                    datacenter_id=c.datacenter_id,
                    health_bucket=c.health_bucket,
                    rtt_ucb_ms=c.rtt_ucb_ms,
                    load_factor=1.0,
                    quality_penalty=1.0,
                    final_score=idx,  # Use rank as score
                    is_preferred=c.datacenter_id in (preferred_datacenters or set()),
                )
                for idx, c in enumerate(sorted_primary)
            ]
        else:
            # Use full scoring
            primary_scores = self._scorer.score_datacenters(
                bucket_result.primary_candidates,
                preferred_datacenters,
            )

        # Apply cooldown penalties
        primary_scores = self._hysteresis_manager.apply_cooldown_penalty(
            primary_scores,
            job_state,
            self._config.cooldown_penalty_multiplier,
        )

        # Step 6: Apply hysteresis
        excluded_set = {c.datacenter_id for c in excluded}
        hysteresis_result = self._hysteresis_manager.evaluate_switch(
            job_state,
            primary_scores,
            excluded_set,
        )

        # Update state if switching
        switched = False
        previous_primary = job_state.primary_datacenter

        if hysteresis_result.should_switch and hysteresis_result.selected_datacenter:
            job_state.select_primary(
                hysteresis_result.selected_datacenter,
                hysteresis_result.selected_score,
            )
            switched = True

        # Step 7: Build fallback chain
        fallback_scores = {
            s.datacenter_id: s
            for s in self._scorer.score_datacenters(
                bucket_result.fallback_candidates,
                preferred_datacenters,
            )
        }

        chain = self._fallback_builder.build_chain(
            primary_scores,
            bucket_result.fallback_candidates,
            fallback_scores,
            max_primary=self._config.max_primary_dcs,
        )

        return RoutingDecision(
            job_id=job_id,
            primary_datacenters=chain.primary_datacenters,
            fallback_datacenters=chain.fallback_datacenters,
            primary_bucket=chain.primary_bucket,
            reason=hysteresis_result.reason,
            in_bootstrap_mode=in_bootstrap,
            scores=chain.scores,
            switched=switched,
            previous_primary=previous_primary,
        )

    def _enrich_with_vivaldi(
        self,
        candidates: list[DatacenterCandidate],
    ) -> None:
        """Enrich candidates with Vivaldi coordinate data."""
        if self._coordinate_tracker is None:
            return

        for candidate in candidates:
            peer_coord = self._coordinate_tracker.get_peer_coordinate(
                candidate.datacenter_id
            )
            if peer_coord is not None:
                candidate.has_coordinate = True
                candidate.rtt_ucb_ms = self._coordinate_tracker.estimate_rtt_ucb_ms(
                    peer_coord
                )
                candidate.coordinate_quality = self._coordinate_tracker.coordinate_quality(
                    peer_coord
                )

    def _check_bootstrap_mode(self) -> bool:
        """Check if we're in coordinate-unaware bootstrap mode."""
        if self._coordinate_tracker is None:
            return True

        coord = self._coordinate_tracker.get_coordinate()
        return self._bootstrap_manager.is_in_bootstrap_mode(
            coord.sample_count,
            coord.error,
        )

    def _empty_decision(
        self,
        job_id: str,
        job_state: JobRoutingState,
    ) -> RoutingDecision:
        """Return empty decision when no candidates available."""
        return RoutingDecision(
            job_id=job_id,
            primary_datacenters=[],
            fallback_datacenters=[],
            primary_bucket=None,
            reason=RoutingDecisionReason.EXCLUSION_FORCED,
            in_bootstrap_mode=True,
            scores={},
            switched=False,
            previous_primary=job_state.primary_datacenter,
        )

    def record_dispatch_failure(
        self,
        job_id: str,
        datacenter_id: str,
    ) -> None:
        """Record a dispatch failure for cooldown tracking."""
        job_state = self._state_manager.get_or_create_state(job_id)
        job_state.record_failure(
            datacenter_id,
            self._config.hysteresis_config.cooldown_seconds,
        )

    def cleanup_job_state(self, job_id: str) -> None:
        """Clean up routing state for a completed job."""
        self._state_manager.remove_state(job_id)

    def get_metrics(self) -> dict:
        """Get router metrics."""
        bootstrap_status = {}
        if self._coordinate_tracker:
            coord = self._coordinate_tracker.get_coordinate()
            bootstrap_status = self._bootstrap_manager.get_bootstrap_status(
                coord.sample_count,
                coord.error,
            )

        return {
            "tracked_jobs": self._state_manager.get_job_count(),
            "bootstrap_status": bootstrap_status,
        }
