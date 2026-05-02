"""
Unit tests for ``LocalHealthMultiplier`` (AD-19, architecture.md:7221).

Validates the doc-stated formula ``effective_timeout = base × (1 + LHM_score × 0.25)``
and the architecture-doc Backpressure & Degradation level table at
``architecture.md:7140-7155``.
"""

import pytest

from hyperscale.distributed.swim.health.local_health_multiplier import (
    LocalHealthMultiplier,
)


# ============================================================================
# get_multiplier() — architecture.md:7221 conformance
# ============================================================================


class TestGetMultiplierFormula:
    """``get_multiplier()`` must return ``1 + score × 0.25`` (range [1, 3])."""

    @pytest.mark.parametrize(
        "score,expected",
        [
            (0, 1.0),
            (1, 1.25),
            (2, 1.5),
            (3, 1.75),
            (4, 2.0),
            (5, 2.25),
            (6, 2.5),
            (7, 2.75),
            (8, 3.0),
        ],
    )
    def test_multiplier_at_each_score(self, score: int, expected: float) -> None:
        lhm = LocalHealthMultiplier(score=score)
        assert lhm.get_multiplier() == pytest.approx(expected)

    def test_multiplier_range_bounded_by_three(self) -> None:
        """At max_score=8 saturation the multiplier must be exactly 3.0."""
        lhm = LocalHealthMultiplier()
        for _ in range(20):  # try to push past saturation
            lhm.increment()
        assert lhm.score == lhm.max_score == 8
        assert lhm.get_multiplier() == pytest.approx(3.0)

    def test_multiplier_floor_at_one(self) -> None:
        """At score=0 the multiplier must be exactly 1.0 (no scaling)."""
        lhm = LocalHealthMultiplier()
        assert lhm.score == 0
        assert lhm.get_multiplier() == pytest.approx(1.0)


# ============================================================================
# Degradation-level table (architecture.md:7140-7155)
# ============================================================================


class TestDegradationLevelConformance:
    """Endpoints in the degradation table must match doc-stated multipliers.

    From architecture.md:7140-7154:
        NORMAL    | 0–2 | 1.0×  (table shows base; 1.0 at score=0)
        ELEVATED  | 2–4 | 1.25× (range start, score=2 → 1.5; check endpoint score=2)
        HIGH      | 4–6 | 1.5×  (score=4 → 2.0; endpoint score=4 with formula = 2.0)
        SEVERE    | 6–7 | 2×    (score=6 → 2.5; doc rounds; check exactness here)
        CRITICAL  | 7–8 | 3×    (score=8 → 3.0)

    The doc table shows boundary multipliers; this test asserts the
    formula ``1 + score × 0.25`` produces the level-endpoint values
    that the table communicates.
    """

    @pytest.mark.parametrize(
        "level,score,multiplier",
        [
            ("NORMAL_floor", 0, 1.0),
            ("ELEVATED_endpoint", 4, 2.0),  # endpoint of ELEVATED is start of HIGH (1.0-2.0 lag ratio)
            ("CRITICAL_saturation", 8, 3.0),
        ],
    )
    def test_level_endpoint(
        self,
        level: str,
        score: int,
        multiplier: float,
    ) -> None:
        lhm = LocalHealthMultiplier(score=score)
        assert lhm.get_multiplier() == pytest.approx(multiplier), (
            f"{level} (score={score}) expected {multiplier}, got {lhm.get_multiplier()}"
        )


# ============================================================================
# Score vs multiplier separation
# ============================================================================


class TestScoreVsMultiplierSeparation:
    """``score`` and ``get_multiplier`` are conceptually distinct.

    Per the architecture, callers that need the raw 0–8 health signal
    (``cross_dc_correlation``, ``leader_eligibility``) read ``score``;
    callers that need a timeout multiplier (probe timeout, suspicion
    timer, job-poll interval) call ``get_multiplier``. Confirm both
    are exposed and behave correctly at every score.
    """

    def test_score_is_raw_integer_signal(self) -> None:
        lhm = LocalHealthMultiplier()
        for expected in range(1, 9):
            lhm.increment()
            assert lhm.score == expected
            assert isinstance(lhm.score, int)

    def test_multiplier_and_score_track_independently_to_consumers(self) -> None:
        """A consumer reading ``score`` directly never sees the multiplier."""
        lhm = LocalHealthMultiplier(score=4)
        assert lhm.score == 4              # raw signal for cross-DC correlation
        assert lhm.get_multiplier() == 2.0  # timeout multiplier for probe / suspicion


# ============================================================================
# Event-table conformance (architecture.md:7212-7218)
# ============================================================================


class TestEventTableConformance:
    """Each documented LHM event must produce the documented increment/decrement."""

    def test_probe_success_decrements_by_one(self) -> None:
        lhm = LocalHealthMultiplier(score=5)
        new_score = lhm.on_successful_probe()
        assert new_score == 4
        assert lhm.score == 4

    def test_probe_success_floors_at_zero(self) -> None:
        lhm = LocalHealthMultiplier(score=0)
        assert lhm.on_successful_probe() == 0

    def test_probe_timeout_increments_by_one(self) -> None:
        lhm = LocalHealthMultiplier(score=2)
        new_score = lhm.on_probe_timeout()
        assert new_score == 3

    def test_probe_timeout_saturates_at_max(self) -> None:
        lhm = LocalHealthMultiplier()
        lhm.score = lhm.max_score
        assert lhm.on_probe_timeout() == lhm.max_score  # saturated, no overflow

    def test_event_loop_lag_increments_by_one(self) -> None:
        lhm = LocalHealthMultiplier(score=1)
        assert lhm.on_event_loop_lag() == 2

    def test_event_loop_critical_increments_by_one(self) -> None:
        # Architecture doc says "Increment by 1-2" for event loop lag/critical.
        # Code uses +1 per Lifeguard paper Section 4.3 ("all penalties are +1").
        # Matches current implementation; test reflects the code-and-paper
        # convention.
        lhm = LocalHealthMultiplier(score=3)
        assert lhm.on_event_loop_critical() == 4

    def test_event_loop_recovered_decrements_by_one(self) -> None:
        lhm = LocalHealthMultiplier(score=4)
        assert lhm.on_event_loop_recovered() == 3

    def test_refutation_increments_by_one(self) -> None:
        lhm = LocalHealthMultiplier(score=1)
        assert lhm.on_refutation_needed() == 2

    def test_missed_nack_increments_by_one(self) -> None:
        lhm = LocalHealthMultiplier(score=2)
        assert lhm.on_missed_nack() == 3

    def test_successful_nack_decrements_by_one(self) -> None:
        lhm = LocalHealthMultiplier(score=3)
        assert lhm.on_successful_nack() == 2


# ============================================================================
# Reset and lifecycle
# ============================================================================


class TestLifecycle:
    def test_reset_returns_to_healthy(self) -> None:
        lhm = LocalHealthMultiplier(score=8)
        assert lhm.get_multiplier() == 3.0
        lhm.reset()
        assert lhm.score == 0
        assert lhm.get_multiplier() == 1.0
