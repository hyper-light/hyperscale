"""
Unit tests for the AD-26 Phase H3 ``WorkflowProgressSnapshot``.

Validates the strict-monotonic-progress criterion (all dimensions
non-regressed AND at least one strictly advanced) used by the
multi-witness extension decision in H5.
"""

import pytest

from hyperscale.distributed.health.workflow_progress_snapshot import (
    WorkflowProgressSnapshot,
)


def _snap(
    cores_completed: int = 0,
    step_transitions: int = 0,
    actions_completed: int = 0,
    snapshot_time: float = 0.0,
    workflow_id: str = "wf-1",
    cores_total: int = 100,
) -> WorkflowProgressSnapshot:
    return WorkflowProgressSnapshot(
        workflow_id=workflow_id,
        cores_completed=cores_completed,
        cores_total=cores_total,
        step_transitions=step_transitions,
        actions_completed=actions_completed,
        snapshot_time=snapshot_time,
    )


# ============================================================================
# all_non_regressed
# ============================================================================


class TestAllNonRegressed:
    def test_identical_snapshots_pass(self) -> None:
        a = _snap(5, 10, 100, 1.0)
        b = _snap(5, 10, 100, 2.0)
        assert a.all_non_regressed(b)

    def test_all_dimensions_advanced_passes(self) -> None:
        before = _snap(5, 10, 100)
        after = _snap(6, 11, 101)
        assert after.all_non_regressed(before)

    def test_single_regression_fails(self) -> None:
        before = _snap(5, 10, 100)
        after_cores_regressed = _snap(4, 11, 101)
        assert not after_cores_regressed.all_non_regressed(before)

    def test_step_regression_fails(self) -> None:
        before = _snap(5, 10, 100)
        after = _snap(6, 9, 101)
        assert not after.all_non_regressed(before)

    def test_actions_regression_fails(self) -> None:
        before = _snap(5, 10, 100)
        after = _snap(6, 11, 99)
        assert not after.all_non_regressed(before)


# ============================================================================
# any_advanced
# ============================================================================


class TestAnyAdvanced:
    def test_no_change_does_not_advance(self) -> None:
        a = _snap(5, 10, 100, 1.0)
        b = _snap(5, 10, 100, 999.0)
        assert not b.any_advanced(a)

    def test_only_cores_advanced(self) -> None:
        before = _snap(5, 10, 100)
        after = _snap(6, 10, 100)
        assert after.any_advanced(before)

    def test_only_steps_advanced(self) -> None:
        before = _snap(5, 10, 100)
        after = _snap(5, 11, 100)
        assert after.any_advanced(before)

    def test_only_actions_advanced(self) -> None:
        before = _snap(5, 10, 100)
        after = _snap(5, 10, 101)
        assert after.any_advanced(before)


# ============================================================================
# is_meaningful_progress (the canonical decision used by H5)
# ============================================================================


class TestIsMeaningfulProgress:
    def test_clean_advance_is_meaningful(self) -> None:
        before = _snap(5, 10, 100)
        after = _snap(6, 11, 101)
        assert after.is_meaningful_progress(before)

    def test_one_dimension_advance_with_others_static_is_meaningful(self) -> None:
        """A workflow with one wedged VU still counts as making progress
        if any other dimension advances. Catches load-test cases where
        cores_completed flat (one wedged VU) but actions_completed rises
        (other VUs still doing work)."""
        before = _snap(5, 10, 100)
        after = _snap(5, 10, 101)  # only actions advanced
        assert after.is_meaningful_progress(before)

    def test_all_static_is_not_meaningful(self) -> None:
        """The classic stuck-workflow case — no dimension changes."""
        a = _snap(5, 10, 100)
        b = _snap(5, 10, 100, snapshot_time=999.0)
        assert not b.is_meaningful_progress(a)

    def test_advance_with_regression_is_not_meaningful(self) -> None:
        """Tamper-resistance: one counter advances while another
        regresses. Most likely either a worker bug (counters went
        backward) or an attempt to game the witness. Either way,
        deny."""
        before = _snap(5, 10, 100)
        after_cores_up_steps_down = _snap(6, 9, 100)
        assert not after_cores_up_steps_down.is_meaningful_progress(before)

    def test_advance_with_actions_regression_is_not_meaningful(self) -> None:
        before = _snap(5, 10, 100)
        after = _snap(6, 11, 99)
        assert not after.is_meaningful_progress(before)


# ============================================================================
# initial baseline
# ============================================================================


class TestInitial:
    def test_initial_baseline_is_zero_progress(self) -> None:
        s = WorkflowProgressSnapshot.initial(workflow_id="wf-1", cores_total=64)
        assert s.workflow_id == "wf-1"
        assert s.cores_completed == 0
        assert s.cores_total == 64
        assert s.step_transitions == 0
        assert s.actions_completed == 0
        assert s.snapshot_time == 0.0

    def test_first_real_snapshot_advances_from_initial(self) -> None:
        """Sanity: the very first reported snapshot from a worker
        producing any work at all should pass the meaningful-progress
        check against an ``initial`` baseline."""
        baseline = WorkflowProgressSnapshot.initial(workflow_id="wf-1", cores_total=10)
        first_real = _snap(
            cores_completed=1,
            step_transitions=2,
            actions_completed=15,
            snapshot_time=1.5,
        )
        assert first_real.is_meaningful_progress(baseline)


# ============================================================================
# Immutability
# ============================================================================


class TestImmutability:
    def test_snapshot_is_frozen(self) -> None:
        s = _snap()
        with pytest.raises(Exception):
            # frozen dataclass — assignment must fail
            s.cores_completed = 99  # type: ignore[misc]
