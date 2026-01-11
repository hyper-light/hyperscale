"""
Integration tests for Worker Health Model (AD-19).

These tests verify that:
1. WorkerHealthState dataclass has all required fields
2. Three signals (liveness, readiness, progress) work correctly
3. Routing decisions are based on combined signals
4. Progress state detection works correctly
5. Health state updates work correctly
"""

import pytest
import time

from hyperscale.distributed_rewrite.health import (
    ProgressState,
    RoutingDecision,
    WorkerHealthConfig,
    WorkerHealthState,
)


class TestProgressState:
    """Test ProgressState enum."""

    def test_progress_state_values(self):
        """ProgressState should have correct values."""
        assert ProgressState.IDLE.value == "idle"
        assert ProgressState.NORMAL.value == "normal"
        assert ProgressState.SLOW.value == "slow"
        assert ProgressState.DEGRADED.value == "degraded"
        assert ProgressState.STUCK.value == "stuck"


class TestRoutingDecision:
    """Test RoutingDecision enum."""

    def test_routing_decision_values(self):
        """RoutingDecision should have correct values."""
        assert RoutingDecision.ROUTE.value == "route"
        assert RoutingDecision.DRAIN.value == "drain"
        assert RoutingDecision.INVESTIGATE.value == "investigate"
        assert RoutingDecision.EVICT.value == "evict"


class TestWorkerHealthConfig:
    """Test WorkerHealthConfig dataclass."""

    def test_default_config_values(self):
        """WorkerHealthConfig should have sensible defaults."""
        config = WorkerHealthConfig()

        assert config.liveness_timeout_seconds == 30.0
        assert config.max_consecutive_liveness_failures == 3
        assert config.normal_rate_threshold == 0.8
        assert config.slow_rate_threshold == 0.3

    def test_custom_config(self):
        """WorkerHealthConfig should accept custom values."""
        config = WorkerHealthConfig(
            liveness_timeout_seconds=60.0,
            max_consecutive_liveness_failures=5,
            normal_rate_threshold=0.9,
            slow_rate_threshold=0.5,
        )

        assert config.liveness_timeout_seconds == 60.0
        assert config.max_consecutive_liveness_failures == 5
        assert config.normal_rate_threshold == 0.9
        assert config.slow_rate_threshold == 0.5


class TestWorkerHealthStateLiveness:
    """Test WorkerHealthState liveness signal."""

    def test_initial_state_is_live(self):
        """Worker should start as live."""
        state = WorkerHealthState(worker_id="worker-1")
        assert state.liveness is True

    def test_liveness_false_after_timeout(self):
        """Worker should be not live after timeout."""
        state = WorkerHealthState(worker_id="worker-1")
        # Set last response to 35 seconds ago
        state.last_liveness_response = time.monotonic() - 35.0
        assert state.liveness is False

    def test_liveness_false_after_consecutive_failures(self):
        """Worker should be not live after consecutive failures."""
        state = WorkerHealthState(worker_id="worker-1")
        state.consecutive_liveness_failures = 3
        assert state.liveness is False

    def test_update_liveness_success(self):
        """update_liveness with success should reset failures."""
        state = WorkerHealthState(worker_id="worker-1")
        state.consecutive_liveness_failures = 2

        state.update_liveness(success=True)

        assert state.consecutive_liveness_failures == 0
        assert state.liveness is True

    def test_update_liveness_failure(self):
        """update_liveness with failure should increment failures."""
        state = WorkerHealthState(worker_id="worker-1")
        state.consecutive_liveness_failures = 0

        state.update_liveness(success=False)

        assert state.consecutive_liveness_failures == 1


class TestWorkerHealthStateReadiness:
    """Test WorkerHealthState readiness signal."""

    def test_readiness_true_when_accepting_with_capacity(self):
        """Worker should be ready when accepting work and has capacity."""
        state = WorkerHealthState(worker_id="worker-1")
        state.accepting_work = True
        state.available_capacity = 5
        assert state.readiness is True

    def test_readiness_false_when_not_accepting(self):
        """Worker should not be ready when not accepting work."""
        state = WorkerHealthState(worker_id="worker-1")
        state.accepting_work = False
        state.available_capacity = 5
        assert state.readiness is False

    def test_readiness_false_when_no_capacity(self):
        """Worker should not be ready when no capacity."""
        state = WorkerHealthState(worker_id="worker-1")
        state.accepting_work = True
        state.available_capacity = 0
        assert state.readiness is False

    def test_update_readiness(self):
        """update_readiness should update both fields."""
        state = WorkerHealthState(worker_id="worker-1")

        state.update_readiness(accepting=True, capacity=10)

        assert state.accepting_work is True
        assert state.available_capacity == 10


class TestWorkerHealthStateProgress:
    """Test WorkerHealthState progress signal."""

    def test_progress_idle_when_no_work(self):
        """Progress should be idle when no work assigned."""
        state = WorkerHealthState(worker_id="worker-1")
        state.workflows_assigned = 0
        assert state.progress_state == ProgressState.IDLE

    def test_progress_normal_at_expected_rate(self):
        """Progress should be normal at expected rate."""
        state = WorkerHealthState(worker_id="worker-1")
        state.workflows_assigned = 10
        state.completions_last_interval = 10
        state.expected_completion_rate = 1.0
        assert state.progress_state == ProgressState.NORMAL

    def test_progress_normal_above_80_percent(self):
        """Progress should be normal at 80%+ of expected rate."""
        state = WorkerHealthState(worker_id="worker-1")
        state.workflows_assigned = 10
        state.completions_last_interval = 8  # 80% of expected
        state.expected_completion_rate = 1.0
        assert state.progress_state == ProgressState.NORMAL

    def test_progress_slow_between_30_and_80_percent(self):
        """Progress should be slow at 30-80% of expected rate."""
        state = WorkerHealthState(worker_id="worker-1")
        state.workflows_assigned = 10
        state.completions_last_interval = 5  # 50% of expected
        state.expected_completion_rate = 1.0
        assert state.progress_state == ProgressState.SLOW

    def test_progress_degraded_below_30_percent(self):
        """Progress should be degraded below 30% of expected rate."""
        state = WorkerHealthState(worker_id="worker-1")
        state.workflows_assigned = 10
        state.completions_last_interval = 2  # 20% of expected
        state.expected_completion_rate = 1.0
        assert state.progress_state == ProgressState.DEGRADED

    def test_progress_stuck_with_zero_completions(self):
        """Progress should be stuck with zero completions."""
        state = WorkerHealthState(worker_id="worker-1")
        state.workflows_assigned = 10
        state.completions_last_interval = 0
        state.expected_completion_rate = 1.0
        assert state.progress_state == ProgressState.STUCK

    def test_update_progress(self):
        """update_progress should update all fields."""
        state = WorkerHealthState(worker_id="worker-1")

        state.update_progress(assigned=15, completed=12, expected_rate=1.5)

        assert state.workflows_assigned == 15
        assert state.completions_last_interval == 12
        assert state.expected_completion_rate == 1.5


class TestWorkerHealthStateRoutingDecision:
    """Test WorkerHealthState routing decisions."""

    def test_route_when_all_healthy(self):
        """Should route when all signals healthy."""
        state = WorkerHealthState(worker_id="worker-1")
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=10, expected_rate=1.0)

        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_evict_when_not_live(self):
        """Should evict when not live."""
        state = WorkerHealthState(worker_id="worker-1")
        state.consecutive_liveness_failures = 5
        state.update_readiness(accepting=True, capacity=5)

        assert state.get_routing_decision() == RoutingDecision.EVICT

    def test_evict_when_stuck(self):
        """Should evict when stuck (even if live)."""
        state = WorkerHealthState(worker_id="worker-1")
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=0, expected_rate=1.0)

        assert state.get_routing_decision() == RoutingDecision.EVICT

    def test_drain_when_not_ready(self):
        """Should drain when live but not ready."""
        state = WorkerHealthState(worker_id="worker-1")
        state.update_liveness(success=True)
        state.update_readiness(accepting=False, capacity=0)
        state.update_progress(assigned=10, completed=10, expected_rate=1.0)

        assert state.get_routing_decision() == RoutingDecision.DRAIN

    def test_investigate_when_degraded(self):
        """Should investigate when live and ready but degraded."""
        state = WorkerHealthState(worker_id="worker-1")
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=2, expected_rate=1.0)

        assert state.get_routing_decision() == RoutingDecision.INVESTIGATE


class TestWorkerHealthStateDiagnostics:
    """Test WorkerHealthState diagnostics."""

    def test_diagnostics_includes_all_fields(self):
        """get_diagnostics should return comprehensive state."""
        state = WorkerHealthState(worker_id="worker-1")
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=8, expected_rate=1.0)

        diag = state.get_diagnostics()

        assert diag["worker_id"] == "worker-1"
        assert diag["liveness"] is True
        assert diag["readiness"] is True
        assert diag["progress_state"] == "normal"
        assert diag["routing_decision"] == "route"
        assert diag["accepting_work"] is True
        assert diag["available_capacity"] == 5
        assert diag["workflows_assigned"] == 10
        assert diag["completions_last_interval"] == 8


class TestWorkerHealthScenarios:
    """Test realistic worker health scenarios."""

    def test_healthy_worker_lifecycle(self):
        """
        Simulate healthy worker lifecycle.

        Scenario: Worker starts, receives work, completes normally.
        """
        state = WorkerHealthState(worker_id="worker-1")

        # Worker connects
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=10)
        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Worker receives work
        state.update_progress(assigned=5, completed=0, expected_rate=1.0)
        state.update_readiness(accepting=True, capacity=5)

        # Worker completes work
        state.update_progress(assigned=5, completed=5, expected_rate=1.0)
        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_worker_becomes_overloaded(self):
        """
        Simulate worker becoming overloaded.

        Scenario: Worker has too much work, stops accepting new work.
        """
        state = WorkerHealthState(worker_id="worker-1")

        # Initially healthy
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=10)
        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Worker gets saturated
        state.update_readiness(accepting=False, capacity=0)
        state.update_progress(assigned=100, completed=50, expected_rate=1.0)

        # Should drain, not evict (still making progress)
        assert state.get_routing_decision() == RoutingDecision.DRAIN

    def test_worker_becomes_stuck(self):
        """
        Simulate worker becoming stuck.

        Scenario: Worker stops making progress (deadlock, hang, etc.)
        """
        state = WorkerHealthState(worker_id="worker-1")

        # Initially healthy
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=10)
        state.update_progress(assigned=5, completed=5, expected_rate=1.0)
        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Worker becomes stuck (no completions despite work)
        state.update_progress(assigned=10, completed=0, expected_rate=1.0)
        assert state.get_routing_decision() == RoutingDecision.EVICT

    def test_worker_crashes_and_recovers(self):
        """
        Simulate worker crash and recovery.

        Scenario: Worker becomes unreachable, then comes back.
        """
        state = WorkerHealthState(worker_id="worker-1")

        # Initially healthy
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=10)
        assert state.liveness is True

        # Worker crashes (consecutive failures)
        for _ in range(4):
            state.update_liveness(success=False)

        assert state.liveness is False
        assert state.get_routing_decision() == RoutingDecision.EVICT

        # Worker recovers
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=10)

        assert state.liveness is True
        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_worker_degraded_performance(self):
        """
        Simulate worker with degraded performance.

        Scenario: Worker is slow but making some progress.
        """
        state = WorkerHealthState(worker_id="worker-1")

        # Worker is live and ready
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)

        # But progress is degraded (below 30% of expected)
        state.update_progress(assigned=10, completed=1, expected_rate=1.0)

        # Should investigate, not evict
        assert state.progress_state == ProgressState.DEGRADED
        assert state.get_routing_decision() == RoutingDecision.INVESTIGATE

    def test_worker_slow_but_acceptable(self):
        """
        Simulate worker that is slow but acceptable.

        Scenario: Worker is below expected rate but above threshold.
        """
        state = WorkerHealthState(worker_id="worker-1")

        # Worker is live and ready
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)

        # Progress is slow (50% of expected)
        state.update_progress(assigned=10, completed=5, expected_rate=1.0)

        # Should still route (slow is acceptable)
        assert state.progress_state == ProgressState.SLOW
        assert state.get_routing_decision() == RoutingDecision.ROUTE
