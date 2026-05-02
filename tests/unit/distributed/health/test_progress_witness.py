"""
Unit tests for the AD-26 Phase H6 progress-witness statistical
machinery: BOCPD, K-S two-sample test, hierarchical α-budget, and
the ``ThroughputWitness`` façade.
"""

import math
import random

import pytest

from hyperscale.distributed.health.progress_witness import (
    BOCPDConfig,
    BayesianOnlineChangePointDetector,
    HierarchicalAlphaBudget,
    HierarchicalAlphaConfig,
    KSResult,
    ThroughputWitness,
    ThroughputWitnessConfig,
    TwoSampleKolmogorovSmirnov,
    WitnessVerdictKind,
)


# ============================================================================
# BOCPD detector
# ============================================================================


class TestBOCPDDetector:
    def test_constructs_with_default_config(self) -> None:
        det = BayesianOnlineChangePointDetector()
        assert det.observation_count == 0
        assert det.posterior.change_point_probability() in (0.0, 1.0)

    def test_observe_increments_count_and_returns_posterior(self) -> None:
        det = BayesianOnlineChangePointDetector()
        post = det.observe(1.0)
        assert det.observation_count == 1
        # Run-length 0 (i.e. the just-observed change-point) has full
        # mass after the very first observation, by construction.
        assert post.probabilities[0] == pytest.approx(1.0)

    def test_stationary_stream_keeps_change_probability_low(self) -> None:
        """A stable Gaussian stream should never trigger a high
        change-point probability after warm-up."""
        det = BayesianOnlineChangePointDetector(BOCPDConfig(hazard_lambda=500.0))
        rng = random.Random(42)
        for _ in range(50):
            det.observe(rng.gauss(10.0, 0.5))
        # After 50 stationary observations the change-point posterior
        # at the most-recent step should be small. We test it against
        # a generous bound (0.2) — the precise value depends on the
        # random seed but is far below 1.0.
        cp_prob = det.posterior.change_point_probability()
        assert cp_prob < 0.2, (
            f"Expected stationary stream to keep change-point probability "
            f"< 0.2, got {cp_prob}"
        )

    def test_step_change_triggers_high_change_probability(self) -> None:
        """A step change in the mean should produce an elevated
        change-point probability shortly after the step."""
        det = BayesianOnlineChangePointDetector(BOCPDConfig(hazard_lambda=500.0))
        rng = random.Random(42)
        # 30 samples around mean 10
        for _ in range(30):
            det.observe(rng.gauss(10.0, 0.5))
        baseline_cp = det.posterior.change_point_probability()
        # Step to mean 1 (much smaller, well outside the prior range)
        for _ in range(5):
            det.observe(rng.gauss(1.0, 0.5))
        post_step_cp = det.posterior.change_point_probability()
        # The change-point probability after the step should be
        # noticeably higher than during the stationary baseline.
        assert post_step_cp > baseline_cp, (
            f"Expected change-point probability to rise after step "
            f"(baseline={baseline_cp}, post={post_step_cp})"
        )

    def test_reset_clears_state(self) -> None:
        det = BayesianOnlineChangePointDetector()
        for _ in range(10):
            det.observe(1.0)
        det.reset()
        assert det.observation_count == 0
        assert det.posterior.probabilities == [1.0]

    def test_run_length_truncation_bounds_memory(self) -> None:
        """The run-length posterior must not grow past
        ``run_length_max`` regardless of observation count."""
        det = BayesianOnlineChangePointDetector(BOCPDConfig(run_length_max=20))
        for _ in range(100):
            det.observe(0.5)
        assert len(det.posterior.probabilities) <= 20


# ============================================================================
# K-S two-sample test
# ============================================================================


class TestKolmogorovSmirnov:
    def test_identical_samples_have_high_p_value(self) -> None:
        """A K-S test of two identical samples should fail to reject
        the null at any reasonable α."""
        sample = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = TwoSampleKolmogorovSmirnov.test(sample, list(sample))
        assert result.statistic == 0.0
        assert result.p_value > 0.5

    def test_disjoint_samples_reject_null(self) -> None:
        """Two well-separated samples should reject stationarity
        with a small p-value."""
        a = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        b = [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]
        result = TwoSampleKolmogorovSmirnov.test(a, b)
        assert result.statistic == pytest.approx(1.0)
        assert result.p_value < 0.05

    def test_is_stationary_uses_alpha_threshold(self) -> None:
        result = KSResult(statistic=0.5, p_value=0.04, n1=10, n2=10)
        assert not result.is_stationary(alpha=0.05)
        assert result.is_stationary(alpha=0.01)

    def test_empty_sample_raises(self) -> None:
        with pytest.raises(ValueError):
            TwoSampleKolmogorovSmirnov.test([], [1.0])

    def test_n1_n2_recorded(self) -> None:
        result = TwoSampleKolmogorovSmirnov.test([1, 2, 3], [4, 5])
        assert result.n1 == 3
        assert result.n2 == 2


# ============================================================================
# Hierarchical α-budget allocator
# ============================================================================


class TestHierarchicalAlphaBudget:
    def test_proportional_split_at_each_level(self) -> None:
        cfg = HierarchicalAlphaConfig(
            alpha_system=0.01,
            alpha_workflow_floor=1e-9,
            alpha_workflow_ceiling=1.0,
        )
        budget = HierarchicalAlphaBudget(cfg)
        alpha_dc = budget.split_to_dc(dc_share=0.5)
        assert alpha_dc == pytest.approx(0.005)
        alpha_manager = budget.split_to_manager(alpha_dc, manager_share=0.5)
        assert alpha_manager == pytest.approx(0.0025)
        alpha_worker = budget.split_to_worker(alpha_manager, worker_share=0.5)
        assert alpha_worker == pytest.approx(0.00125)
        alpha_workflow = budget.split_to_workflow(alpha_worker, workflow_share=0.5)
        assert alpha_workflow == pytest.approx(0.000625)

    def test_workflow_alpha_clamps_at_floor(self) -> None:
        cfg = HierarchicalAlphaConfig(
            alpha_system=0.01,
            alpha_workflow_floor=1e-3,
            alpha_workflow_ceiling=0.05,
        )
        budget = HierarchicalAlphaBudget(cfg)
        # 1000-deep nested split would give vanishingly small α; floor
        # clamps to 1e-3.
        alpha_workflow = budget.split_to_workflow(
            alpha_worker=1e-12, workflow_share=1e-12
        )
        assert alpha_workflow == pytest.approx(1e-3)

    def test_workflow_alpha_clamps_at_ceiling(self) -> None:
        cfg = HierarchicalAlphaConfig(
            alpha_system=1.0,
            alpha_workflow_floor=1e-9,
            alpha_workflow_ceiling=0.05,
        )
        budget = HierarchicalAlphaBudget(cfg)
        # Even with α_system=1.0 fully passed through, the ceiling
        # clamps the per-workflow budget at 0.05.
        alpha_workflow = budget.split_to_workflow(alpha_worker=1.0, workflow_share=1.0)
        assert alpha_workflow == pytest.approx(0.05)

    def test_zero_active_returns_ceiling(self) -> None:
        budget = HierarchicalAlphaBudget(HierarchicalAlphaConfig())
        # Degenerate scale — fall back to the ceiling.
        alpha = budget.workflow_alpha_from_counts(
            active_in_cluster=0,
            active_in_dc=0,
            active_on_manager=0,
            active_on_worker=0,
        )
        assert alpha == pytest.approx(budget.config.alpha_workflow_ceiling)

    def test_workflow_alpha_from_counts_chains_correctly(self) -> None:
        """Sanity: ``workflow_alpha_from_counts`` matches the four-step
        chain when the ratios decompose cleanly."""
        cfg = HierarchicalAlphaConfig(
            alpha_system=0.01,
            alpha_workflow_floor=1e-12,
            alpha_workflow_ceiling=1.0,
        )
        budget = HierarchicalAlphaBudget(cfg)
        # 100 cluster, 50 in DC, 25 on manager, 5 on worker.
        # dc_share=0.5, manager_share=0.5, worker_share=0.2,
        # workflow_share=0.2 → product = 0.5 × 0.5 × 0.2 × 0.2 = 0.01
        # alpha_system=0.01 → 0.01 × 0.01 = 1e-4
        alpha = budget.workflow_alpha_from_counts(
            active_in_cluster=100,
            active_in_dc=50,
            active_on_manager=25,
            active_on_worker=5,
        )
        assert alpha == pytest.approx(1e-4)


# ============================================================================
# ThroughputWitness façade
# ============================================================================


class TestThroughputWitness:
    def test_cold_start_returns_cold_start_kind(self) -> None:
        witness = ThroughputWitness(
            ThroughputWitnessConfig(cold_start_min_observations=5)
        )
        for _ in range(4):
            verdict = witness.observe(
                worker_id="w1",
                workflow_id="wf1",
                throughput=100.0,
                active_in_cluster=1,
                active_in_dc=1,
                active_on_manager=1,
                active_on_worker=1,
            )
            assert verdict.kind == WitnessVerdictKind.COLD_START

    def test_post_warmup_stationary_stream_is_stationary(self) -> None:
        witness = ThroughputWitness(
            ThroughputWitnessConfig(
                cold_start_min_observations=5,
                bocpd=BOCPDConfig(hazard_lambda=500.0),
            )
        )
        rng = random.Random(7)
        last_kind = None
        for _ in range(40):
            verdict = witness.observe(
                worker_id="w1",
                workflow_id="wf1",
                throughput=rng.gauss(100.0, 1.0),
                active_in_cluster=1,
                active_in_dc=1,
                active_on_manager=1,
                active_on_worker=1,
            )
            last_kind = verdict.kind
        assert last_kind == WitnessVerdictKind.STATIONARY

    def test_throughput_drop_classified_as_regime_change_down(self) -> None:
        witness = ThroughputWitness(
            ThroughputWitnessConfig(
                cold_start_min_observations=5,
                bocpd=BOCPDConfig(hazard_lambda=200.0),
                alpha=HierarchicalAlphaConfig(
                    # Permissive workflow-level alpha so the test
                    # doesn't need 100s of observations to converge.
                    alpha_workflow_ceiling=0.5,
                    alpha_workflow_floor=0.01,
                ),
            )
        )
        rng = random.Random(7)
        # 40 stationary samples around 100 to establish baseline
        for _ in range(40):
            witness.observe(
                worker_id="w1",
                workflow_id="wf1",
                throughput=rng.gauss(100.0, 0.5),
                active_in_cluster=1,
                active_in_dc=1,
                active_on_manager=1,
                active_on_worker=1,
            )
        # Drop to 10 — well outside the prior — for several samples
        any_regime_down = False
        for _ in range(10):
            verdict = witness.observe(
                worker_id="w1",
                workflow_id="wf1",
                throughput=rng.gauss(10.0, 0.5),
                active_in_cluster=1,
                active_in_dc=1,
                active_on_manager=1,
                active_on_worker=1,
            )
            if verdict.kind == WitnessVerdictKind.REGIME_CHANGE_DOWN:
                any_regime_down = True
                break
        assert any_regime_down, (
            "ThroughputWitness should detect a 10x throughput drop "
            "as REGIME_CHANGE_DOWN"
        )

    def test_throughput_surge_classified_as_regime_change_up(self) -> None:
        witness = ThroughputWitness(
            ThroughputWitnessConfig(
                cold_start_min_observations=5,
                bocpd=BOCPDConfig(hazard_lambda=200.0),
                alpha=HierarchicalAlphaConfig(
                    alpha_workflow_ceiling=0.5,
                    alpha_workflow_floor=0.01,
                ),
            )
        )
        rng = random.Random(7)
        for _ in range(40):
            witness.observe(
                worker_id="w1",
                workflow_id="wf1",
                throughput=rng.gauss(10.0, 0.5),
                active_in_cluster=1,
                active_in_dc=1,
                active_on_manager=1,
                active_on_worker=1,
            )
        any_regime_up = False
        for _ in range(10):
            verdict = witness.observe(
                worker_id="w1",
                workflow_id="wf1",
                throughput=rng.gauss(100.0, 0.5),
                active_in_cluster=1,
                active_in_dc=1,
                active_on_manager=1,
                active_on_worker=1,
            )
            if verdict.kind == WitnessVerdictKind.REGIME_CHANGE_UP:
                any_regime_up = True
                break
        assert any_regime_up, (
            "ThroughputWitness should detect a 10x throughput surge "
            "as REGIME_CHANGE_UP"
        )

    def test_reset_stream_clears_per_stream_state(self) -> None:
        witness = ThroughputWitness()
        for _ in range(20):
            witness.observe(
                worker_id="w1",
                workflow_id="wf1",
                throughput=10.0,
                active_in_cluster=1,
                active_in_dc=1,
                active_on_manager=1,
                active_on_worker=1,
            )
        assert ("w1", "wf1") in witness._streams
        witness.reset_stream("w1", "wf1")
        assert ("w1", "wf1") not in witness._streams

    def test_separate_streams_are_independent(self) -> None:
        """Two ``(worker, workflow)`` keys must not share BOCPD state."""
        witness = ThroughputWitness()
        for _ in range(20):
            witness.observe(
                worker_id="w1",
                workflow_id="wf1",
                throughput=100.0,
                active_in_cluster=2,
                active_in_dc=1,
                active_on_manager=1,
                active_on_worker=1,
            )
            witness.observe(
                worker_id="w2",
                workflow_id="wf2",
                throughput=10.0,
                active_in_cluster=2,
                active_in_dc=1,
                active_on_manager=1,
                active_on_worker=1,
            )
        # Each detector saw 20 observations. The means should differ.
        s1 = witness._streams[("w1", "wf1")]
        s2 = witness._streams[("w2", "wf2")]
        assert s1.detector.observation_count == 20
        assert s2.detector.observation_count == 20
        # Predictive means converge to the per-stream sample mean after
        # enough observations.
        m1 = s1.detector.posterior.expected_predictive_mean(BOCPDConfig(), 100.0)
        m2 = s2.detector.posterior.expected_predictive_mean(BOCPDConfig(), 10.0)
        assert m1 > m2

    def test_verdict_carries_full_trace_for_forensics(self) -> None:
        witness = ThroughputWitness()
        verdict = witness.observe(
            worker_id="w1",
            workflow_id="wf1",
            throughput=42.0,
            active_in_cluster=1,
            active_in_dc=1,
            active_on_manager=1,
            active_on_worker=1,
        )
        assert verdict.observation == 42.0
        assert verdict.observation_count == 1
        assert math.isfinite(verdict.change_point_probability)
        assert math.isfinite(verdict.alpha_workflow)
        assert math.isfinite(verdict.predictive_mean_before)
        assert math.isfinite(verdict.predictive_mean_after)
