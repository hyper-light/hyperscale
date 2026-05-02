"""
Unit tests for the AD-26/AD-34 Phase H2 deadline-resolver override
hierarchy.
"""

import pytest

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.health.deadline_resolver import (
    DEFAULT_TIMEOUT_MULTIPLIER,
    resolve_worker_deadline_seconds,
)


# ============================================================================
# Workflow class fixtures
# ============================================================================


class _BaseDefaultWorkflow(Workflow):
    """Default duration='1m', timeout='30s' — both base-class defaults.

    Used to test the framework-default branch of the override hierarchy
    (no class-level override and no explicit submission timeout).
    """

    duration = "1m"


class _ClassTimeoutOverrideWorkflow(Workflow):
    """Timeout overridden at the class level — exercises branch 2."""

    duration = "5m"
    timeout = "1m"


class _DurationOnlyOverrideWorkflow(Workflow):
    """Only duration overridden — timeout stays at base default '30s'.

    Should fall through to the framework-default branch (×multiplier),
    NOT the class-timeout-override branch.
    """

    duration = "10m"


# ============================================================================
# Override hierarchy
# ============================================================================


class TestExplicitSubmissionWins:
    """Branch 1 — explicit ``JobSubmission.timeout_seconds_explicit=True``."""

    def test_explicit_wins_over_class_override(self) -> None:
        wf = _ClassTimeoutOverrideWorkflow()
        deadline = resolve_worker_deadline_seconds(
            workflow=wf,
            submission_timeout_seconds=120.0,
            submission_timeout_explicit=True,
        )
        assert deadline == 120.0

    def test_explicit_wins_over_default(self) -> None:
        wf = _BaseDefaultWorkflow()
        deadline = resolve_worker_deadline_seconds(
            workflow=wf,
            submission_timeout_seconds=42.0,
            submission_timeout_explicit=True,
        )
        assert deadline == 42.0

    def test_zero_explicit_falls_through_to_class_or_default(self) -> None:
        """``timeout_seconds_explicit=True`` with value 0 is treated as
        not-actually-explicit and falls through to the next branch."""
        wf = _ClassTimeoutOverrideWorkflow()
        deadline = resolve_worker_deadline_seconds(
            workflow=wf,
            submission_timeout_seconds=0.0,
            submission_timeout_explicit=True,
        )
        # Falls through to class-override branch: duration + timeout
        assert deadline == 5 * 60 + 60  # 5m + 1m = 360s


class TestClassTimeoutOverride:
    """Branch 2 — workflow class overrides ``timeout``."""

    def test_class_timeout_used_when_no_explicit(self) -> None:
        wf = _ClassTimeoutOverrideWorkflow()  # duration=5m, timeout=1m
        deadline = resolve_worker_deadline_seconds(
            workflow=wf,
            submission_timeout_seconds=0.0,
            submission_timeout_explicit=False,
        )
        # duration + timeout = 300 + 60
        assert deadline == 360.0

    def test_class_timeout_takes_priority_over_default_multiplier(self) -> None:
        """Even if the multiplier would give a different result, an
        explicit class-level timeout wins."""
        wf = _ClassTimeoutOverrideWorkflow()
        # multiplier path would give 5*60 * 1.5 = 450s
        # class-override path gives 5*60 + 60 = 360s
        deadline = resolve_worker_deadline_seconds(
            workflow=wf,
            submission_timeout_seconds=0.0,
            submission_timeout_explicit=False,
        )
        assert deadline == 360.0
        assert deadline != 450.0


class TestFrameworkDefault:
    """Branch 3 — neither submission nor workflow class overrides."""

    def test_default_uses_duration_times_multiplier(self) -> None:
        wf = _BaseDefaultWorkflow()  # duration=1m, timeout=30s (base default)
        deadline = resolve_worker_deadline_seconds(
            workflow=wf,
            submission_timeout_seconds=0.0,
            submission_timeout_explicit=False,
        )
        assert deadline == 60.0 * DEFAULT_TIMEOUT_MULTIPLIER  # 90s

    def test_duration_only_override_uses_default_multiplier(self) -> None:
        """A workflow that overrides only ``duration`` (not ``timeout``)
        should fall through to the framework default — class detection
        must not falsely treat the inherited ``timeout`` as an override.
        """
        wf = _DurationOnlyOverrideWorkflow()  # duration=10m, timeout=30s default
        deadline = resolve_worker_deadline_seconds(
            workflow=wf,
            submission_timeout_seconds=0.0,
            submission_timeout_explicit=False,
        )
        assert deadline == 600.0 * DEFAULT_TIMEOUT_MULTIPLIER  # 900s

    def test_custom_multiplier_honored(self) -> None:
        wf = _BaseDefaultWorkflow()
        deadline = resolve_worker_deadline_seconds(
            workflow=wf,
            submission_timeout_seconds=0.0,
            submission_timeout_explicit=False,
            default_multiplier=2.0,
        )
        assert deadline == 120.0  # 60s × 2.0


# ============================================================================
# Edge cases
# ============================================================================


class TestEdgeCases:
    def test_negative_submission_timeout_with_explicit_falls_through(self) -> None:
        """Defensive: a negative submission timeout shouldn't be honored
        even if ``explicit=True``."""
        wf = _BaseDefaultWorkflow()
        deadline = resolve_worker_deadline_seconds(
            workflow=wf,
            submission_timeout_seconds=-5.0,
            submission_timeout_explicit=True,
        )
        # Falls through to default-multiplier branch
        assert deadline == 60.0 * DEFAULT_TIMEOUT_MULTIPLIER

    def test_default_multiplier_is_one_point_five(self) -> None:
        """Module-level constant matches the architecture-stated default."""
        assert DEFAULT_TIMEOUT_MULTIPLIER == pytest.approx(1.5)
