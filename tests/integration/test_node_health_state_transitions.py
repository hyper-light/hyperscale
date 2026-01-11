"""
State transition tests for Node Health and Recovery (AD-19).

Tests all state transitions and recovery scenarios for worker health:
- Liveness signal transitions (alive -> dead -> recovered)
- Readiness signal transitions (ready -> not ready -> ready)
- Progress state transitions through all states
- Combined signal routing decision transitions
- Recovery scenarios from all unhealthy states
- Edge cases in state transitions
"""

import pytest
import time
from dataclasses import replace
from unittest.mock import patch

from hyperscale.distributed_rewrite.health.worker_health import (
    WorkerHealthState,
    WorkerHealthConfig,
    ProgressState,
    RoutingDecision,
)


class TestLivenessSignalTransitions:
    """Test liveness signal state transitions."""

    def test_liveness_starts_healthy(self) -> None:
        """Test that worker starts with healthy liveness."""
        state = WorkerHealthState(worker_id="worker-1")
        assert state.liveness is True

    def test_liveness_fails_after_timeout(self) -> None:
        """Test liveness becomes false after timeout."""
        config = WorkerHealthConfig(liveness_timeout_seconds=1.0)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Simulate time passage beyond timeout
        state.last_liveness_response = time.monotonic() - 2.0

        assert state.liveness is False

    def test_liveness_fails_after_consecutive_failures(self) -> None:
        """Test liveness becomes false after max consecutive failures."""
        config = WorkerHealthConfig(max_consecutive_liveness_failures=3)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Record failures
        state.update_liveness(success=False)
        assert state.liveness is True  # 1 failure

        state.update_liveness(success=False)
        assert state.liveness is True  # 2 failures

        state.update_liveness(success=False)
        assert state.liveness is False  # 3 failures - dead

    def test_liveness_recovers_after_success(self) -> None:
        """Test liveness recovers after successful probe."""
        config = WorkerHealthConfig(max_consecutive_liveness_failures=2)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Fail twice
        state.update_liveness(success=False)
        state.update_liveness(success=False)
        assert state.liveness is False

        # Recover
        state.update_liveness(success=True)
        assert state.liveness is True
        assert state.consecutive_liveness_failures == 0

    def test_liveness_immediate_recovery_resets_failures(self) -> None:
        """Test that any success resets consecutive failures."""
        config = WorkerHealthConfig(max_consecutive_liveness_failures=5)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Fail 4 times (one short of threshold)
        for _ in range(4):
            state.update_liveness(success=False)

        assert state.consecutive_liveness_failures == 4
        assert state.liveness is True

        # One success resets
        state.update_liveness(success=True)
        assert state.consecutive_liveness_failures == 0

        # Can fail again without immediate death
        state.update_liveness(success=False)
        assert state.liveness is True


class TestReadinessSignalTransitions:
    """Test readiness signal state transitions."""

    def test_readiness_starts_with_capacity_required(self) -> None:
        """Test that readiness requires capacity."""
        state = WorkerHealthState(worker_id="worker-1")

        # Default has accepting=True but capacity=0
        assert state.accepting_work is True
        assert state.available_capacity == 0
        assert state.readiness is False

    def test_readiness_with_accepting_and_capacity(self) -> None:
        """Test readiness becomes true with accepting and capacity."""
        state = WorkerHealthState(worker_id="worker-1")

        state.update_readiness(accepting=True, capacity=5)

        assert state.readiness is True

    def test_readiness_lost_when_not_accepting(self) -> None:
        """Test readiness lost when worker stops accepting."""
        state = WorkerHealthState(worker_id="worker-1")
        state.update_readiness(accepting=True, capacity=5)

        assert state.readiness is True

        # Stop accepting
        state.update_readiness(accepting=False, capacity=5)

        assert state.readiness is False

    def test_readiness_lost_when_no_capacity(self) -> None:
        """Test readiness lost when capacity exhausted."""
        state = WorkerHealthState(worker_id="worker-1")
        state.update_readiness(accepting=True, capacity=5)

        assert state.readiness is True

        # Exhaust capacity
        state.update_readiness(accepting=True, capacity=0)

        assert state.readiness is False

    def test_readiness_recovery(self) -> None:
        """Test readiness recovery when both conditions met."""
        state = WorkerHealthState(worker_id="worker-1")
        state.update_readiness(accepting=False, capacity=0)

        assert state.readiness is False

        # Partially recover
        state.update_readiness(accepting=True, capacity=0)
        assert state.readiness is False

        # Fully recover
        state.update_readiness(accepting=True, capacity=3)
        assert state.readiness is True


class TestProgressStateTransitions:
    """Test progress state transitions through all states."""

    def test_progress_idle_when_no_work(self) -> None:
        """Test progress is IDLE when no work assigned."""
        state = WorkerHealthState(worker_id="worker-1")
        state.update_progress(assigned=0, completed=0, expected_rate=1.0)

        assert state.progress_state == ProgressState.IDLE

    def test_progress_normal_at_good_rate(self) -> None:
        """Test progress is NORMAL at >= 80% expected rate."""
        config = WorkerHealthConfig(normal_rate_threshold=0.8)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # 10 assigned, 8 completed = 80% rate
        state.update_progress(assigned=10, completed=8, expected_rate=1.0)

        assert state.progress_state == ProgressState.NORMAL

    def test_progress_slow_at_moderate_rate(self) -> None:
        """Test progress is SLOW at 30-80% expected rate."""
        config = WorkerHealthConfig(normal_rate_threshold=0.8, slow_rate_threshold=0.3)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # 10 assigned, 5 completed = 50% rate
        state.update_progress(assigned=10, completed=5, expected_rate=1.0)

        assert state.progress_state == ProgressState.SLOW

    def test_progress_degraded_at_low_rate(self) -> None:
        """Test progress is DEGRADED at <30% expected rate with some completions."""
        config = WorkerHealthConfig(slow_rate_threshold=0.3)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # 10 assigned, 2 completed = 20% rate
        state.update_progress(assigned=10, completed=2, expected_rate=1.0)

        assert state.progress_state == ProgressState.DEGRADED

    def test_progress_stuck_with_zero_completions(self) -> None:
        """Test progress is STUCK when no completions despite work."""
        state = WorkerHealthState(worker_id="worker-1")

        # 5 assigned, 0 completed
        state.update_progress(assigned=5, completed=0, expected_rate=1.0)

        assert state.progress_state == ProgressState.STUCK

    def test_progress_state_cycle(self) -> None:
        """Test full cycle through all progress states."""
        config = WorkerHealthConfig(normal_rate_threshold=0.8, slow_rate_threshold=0.3)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        states_visited = []

        # IDLE -> NORMAL -> SLOW -> DEGRADED -> STUCK -> NORMAL
        scenarios = [
            (0, 0, ProgressState.IDLE),
            (10, 10, ProgressState.NORMAL),
            (10, 5, ProgressState.SLOW),
            (10, 2, ProgressState.DEGRADED),
            (10, 0, ProgressState.STUCK),
            (10, 10, ProgressState.NORMAL),  # Recovery
        ]

        for assigned, completed, expected_state in scenarios:
            state.update_progress(assigned=assigned, completed=completed, expected_rate=1.0)
            assert state.progress_state == expected_state
            states_visited.append(state.progress_state)

        # Verify we visited all states
        assert ProgressState.IDLE in states_visited
        assert ProgressState.NORMAL in states_visited
        assert ProgressState.SLOW in states_visited
        assert ProgressState.DEGRADED in states_visited
        assert ProgressState.STUCK in states_visited


class TestRoutingDecisionTransitions:
    """Test routing decision transitions based on combined signals."""

    def test_route_when_all_healthy(self) -> None:
        """Test ROUTE decision when all signals healthy."""
        state = WorkerHealthState(worker_id="worker-1")

        # Set up healthy state
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=8, expected_rate=1.0)

        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_drain_when_not_ready_but_live(self) -> None:
        """Test DRAIN decision when not ready but live."""
        state = WorkerHealthState(worker_id="worker-1")

        state.update_liveness(success=True)
        state.update_readiness(accepting=False, capacity=0)  # Not ready
        state.update_progress(assigned=5, completed=4, expected_rate=1.0)

        assert state.get_routing_decision() == RoutingDecision.DRAIN

    def test_investigate_when_progress_degraded(self) -> None:
        """Test INVESTIGATE decision when progress degraded but ready."""
        config = WorkerHealthConfig(slow_rate_threshold=0.3)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=2, expected_rate=1.0)  # Degraded

        assert state.get_routing_decision() == RoutingDecision.INVESTIGATE

    def test_evict_when_not_live(self) -> None:
        """Test EVICT decision when not live."""
        config = WorkerHealthConfig(max_consecutive_liveness_failures=1)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        state.update_liveness(success=False)  # Dead

        # Other signals don't matter
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=10, expected_rate=1.0)

        assert state.get_routing_decision() == RoutingDecision.EVICT

    def test_evict_when_stuck(self) -> None:
        """Test EVICT decision when progress is stuck."""
        state = WorkerHealthState(worker_id="worker-1")

        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=5, completed=0, expected_rate=1.0)  # Stuck

        assert state.get_routing_decision() == RoutingDecision.EVICT

    def test_decision_priority_evict_over_drain(self) -> None:
        """Test that EVICT takes priority over DRAIN."""
        config = WorkerHealthConfig(max_consecutive_liveness_failures=1)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        state.update_liveness(success=False)  # Dead
        state.update_readiness(accepting=False, capacity=0)  # Also not ready

        # Should be EVICT, not DRAIN
        assert state.get_routing_decision() == RoutingDecision.EVICT


class TestRoutingDecisionCycles:
    """Test full cycles through routing decision states."""

    def test_healthy_to_evict_to_healthy_cycle(self) -> None:
        """Test cycle: ROUTE -> EVICT -> ROUTE recovery."""
        config = WorkerHealthConfig(max_consecutive_liveness_failures=2)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Start healthy
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=8)

        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Die
        state.update_liveness(success=False)
        state.update_liveness(success=False)

        assert state.get_routing_decision() == RoutingDecision.EVICT

        # Recover
        state.update_liveness(success=True)

        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_healthy_to_drain_to_healthy_cycle(self) -> None:
        """Test cycle: ROUTE -> DRAIN -> ROUTE recovery."""
        state = WorkerHealthState(worker_id="worker-1")

        # Start healthy
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=8)

        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Stop accepting (e.g., graceful shutdown)
        state.update_readiness(accepting=False, capacity=0)

        assert state.get_routing_decision() == RoutingDecision.DRAIN

        # Resume accepting
        state.update_readiness(accepting=True, capacity=5)

        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_healthy_to_investigate_to_healthy_cycle(self) -> None:
        """Test cycle: ROUTE -> INVESTIGATE -> ROUTE recovery."""
        config = WorkerHealthConfig(slow_rate_threshold=0.3)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Start healthy
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=10)

        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Degrade
        state.update_progress(assigned=10, completed=1)

        assert state.get_routing_decision() == RoutingDecision.INVESTIGATE

        # Recover performance
        state.update_progress(assigned=10, completed=9)

        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_full_state_machine_cycle(self) -> None:
        """Test full cycle through all routing decisions."""
        config = WorkerHealthConfig(
            max_consecutive_liveness_failures=2,
            slow_rate_threshold=0.3,
        )
        state = WorkerHealthState(worker_id="worker-1", config=config)

        decisions_visited = []

        # ROUTE: All healthy
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=10)
        decisions_visited.append(state.get_routing_decision())

        # INVESTIGATE: Degraded progress
        state.update_progress(assigned=10, completed=1)
        decisions_visited.append(state.get_routing_decision())

        # DRAIN: Not ready
        state.update_progress(assigned=10, completed=10)  # Fix progress
        state.update_readiness(accepting=False, capacity=0)
        decisions_visited.append(state.get_routing_decision())

        # EVICT: Dead
        state.update_liveness(success=False)
        state.update_liveness(success=False)
        decisions_visited.append(state.get_routing_decision())

        # Verify all decisions visited
        assert RoutingDecision.ROUTE in decisions_visited
        assert RoutingDecision.INVESTIGATE in decisions_visited
        assert RoutingDecision.DRAIN in decisions_visited
        assert RoutingDecision.EVICT in decisions_visited


class TestRecoveryScenarios:
    """Test various recovery scenarios."""

    def test_recovery_from_timeout(self) -> None:
        """Test recovery from liveness timeout."""
        config = WorkerHealthConfig(liveness_timeout_seconds=1.0)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        state.update_readiness(accepting=True, capacity=5)
        state.update_progress(assigned=10, completed=8)

        # Simulate timeout
        state.last_liveness_response = time.monotonic() - 2.0
        assert state.liveness is False
        assert state.get_routing_decision() == RoutingDecision.EVICT

        # Recover with new probe
        state.update_liveness(success=True)
        assert state.liveness is True
        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_recovery_from_stuck(self) -> None:
        """Test recovery from stuck progress state."""
        state = WorkerHealthState(worker_id="worker-1")

        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=5)

        # Stuck
        state.update_progress(assigned=5, completed=0)
        assert state.progress_state == ProgressState.STUCK
        assert state.get_routing_decision() == RoutingDecision.EVICT

        # Recovery: Start completing work
        state.update_progress(assigned=5, completed=4)
        assert state.progress_state == ProgressState.NORMAL
        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_recovery_from_capacity_exhaustion(self) -> None:
        """Test recovery from capacity exhaustion."""
        state = WorkerHealthState(worker_id="worker-1")

        state.update_liveness(success=True)
        state.update_progress(assigned=10, completed=10)

        # At capacity
        state.update_readiness(accepting=True, capacity=0)
        assert state.readiness is False
        assert state.get_routing_decision() == RoutingDecision.DRAIN

        # Capacity freed
        state.update_readiness(accepting=True, capacity=3)
        assert state.readiness is True
        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_recovery_requires_all_signals(self) -> None:
        """Test that full recovery requires all signals healthy."""
        config = WorkerHealthConfig(
            max_consecutive_liveness_failures=1,
            slow_rate_threshold=0.3,
        )
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Setup: dead, not ready, degraded
        state.update_liveness(success=False)
        state.update_readiness(accepting=False, capacity=0)
        state.update_progress(assigned=10, completed=1)

        assert state.get_routing_decision() == RoutingDecision.EVICT

        # Fix liveness only
        state.update_liveness(success=True)
        # Still not ROUTE due to readiness and progress
        assert state.get_routing_decision() != RoutingDecision.ROUTE

        # Fix readiness
        state.update_readiness(accepting=True, capacity=5)
        # Still INVESTIGATE due to degraded progress
        assert state.get_routing_decision() == RoutingDecision.INVESTIGATE

        # Fix progress
        state.update_progress(assigned=10, completed=9)
        assert state.get_routing_decision() == RoutingDecision.ROUTE


class TestEdgeCases:
    """Test edge cases in state transitions."""

    def test_zero_workflows_assigned(self) -> None:
        """Test progress state when zero workflows assigned."""
        state = WorkerHealthState(worker_id="worker-1")

        state.update_progress(assigned=0, completed=5)  # 5 completions but 0 assigned

        # Should be IDLE when no assigned work
        assert state.progress_state == ProgressState.IDLE

    def test_very_high_completion_rate(self) -> None:
        """Test with completions exceeding assigned (batch completion)."""
        state = WorkerHealthState(worker_id="worker-1")

        # More completions than assigned (possible with batching)
        state.update_progress(assigned=5, completed=10)

        # Should still be NORMAL
        assert state.progress_state == ProgressState.NORMAL

    def test_negative_capacity_handling(self) -> None:
        """Test handling of negative capacity (should not happen)."""
        state = WorkerHealthState(worker_id="worker-1")

        state.update_readiness(accepting=True, capacity=-1)

        # Negative capacity should mean not ready
        assert state.readiness is False

    def test_exact_threshold_boundaries(self) -> None:
        """Test progress states at exact threshold boundaries."""
        config = WorkerHealthConfig(
            normal_rate_threshold=0.8,
            slow_rate_threshold=0.3,
        )
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Exactly at 80% threshold
        state.update_progress(assigned=100, completed=80, expected_rate=1.0)
        assert state.progress_state == ProgressState.NORMAL

        # Just below 80%
        state.update_progress(assigned=100, completed=79, expected_rate=1.0)
        assert state.progress_state == ProgressState.SLOW

        # Exactly at 30% threshold
        state.update_progress(assigned=100, completed=30, expected_rate=1.0)
        assert state.progress_state == ProgressState.SLOW

        # Just below 30%
        state.update_progress(assigned=100, completed=29, expected_rate=1.0)
        assert state.progress_state == ProgressState.DEGRADED

    def test_diagnostics_reflect_current_state(self) -> None:
        """Test that diagnostics accurately reflect current state."""
        config = WorkerHealthConfig(slow_rate_threshold=0.3)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=3)
        state.update_progress(assigned=10, completed=8, expected_rate=1.0)

        diagnostics = state.get_diagnostics()

        assert diagnostics["worker_id"] == "worker-1"
        assert diagnostics["liveness"] is True
        assert diagnostics["readiness"] is True
        assert diagnostics["progress_state"] == "normal"
        assert diagnostics["routing_decision"] == "route"
        assert diagnostics["accepting_work"] is True
        assert diagnostics["available_capacity"] == 3
        assert diagnostics["workflows_assigned"] == 10
        assert diagnostics["completions_last_interval"] == 8


class TestConcurrentUpdates:
    """Test state consistency with concurrent updates."""

    def test_rapid_liveness_updates(self) -> None:
        """Test rapid alternating liveness updates."""
        config = WorkerHealthConfig(max_consecutive_liveness_failures=5)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Rapid alternating updates
        for i in range(100):
            state.update_liveness(success=i % 2 == 0)

        # Should never have reached 5 consecutive failures
        assert state.consecutive_liveness_failures < 5
        assert state.liveness is True

    def test_interleaved_signal_updates(self) -> None:
        """Test interleaved updates to all signals."""
        state = WorkerHealthState(worker_id="worker-1")

        for i in range(50):
            state.update_liveness(success=True)
            state.update_readiness(accepting=i % 3 != 0, capacity=i % 10)
            state.update_progress(assigned=i + 1, completed=i)

        # State should be consistent
        diagnostics = state.get_diagnostics()
        assert diagnostics["workflows_assigned"] == 50
        assert diagnostics["completions_last_interval"] == 49


class TestCustomConfigurationBehavior:
    """Test behavior with custom configuration values."""

    def test_very_short_timeout(self) -> None:
        """Test with very short liveness timeout."""
        config = WorkerHealthConfig(liveness_timeout_seconds=0.001)  # 1ms
        state = WorkerHealthState(worker_id="worker-1", config=config)

        state.update_liveness(success=True)

        # Wait a tiny bit
        time.sleep(0.002)

        # Should be timed out
        assert state.liveness is False

    def test_very_high_failure_threshold(self) -> None:
        """Test with very high failure threshold."""
        config = WorkerHealthConfig(max_consecutive_liveness_failures=1000)
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # Fail many times but not enough
        for _ in range(999):
            state.update_liveness(success=False)

        assert state.liveness is True  # Still under threshold

        state.update_liveness(success=False)
        assert state.liveness is False  # Now at threshold

    def test_custom_progress_thresholds(self) -> None:
        """Test with custom progress thresholds."""
        config = WorkerHealthConfig(
            normal_rate_threshold=0.95,  # Very strict
            slow_rate_threshold=0.9,  # Also strict
        )
        state = WorkerHealthState(worker_id="worker-1", config=config)

        # 90% completion rate
        state.update_progress(assigned=100, completed=90, expected_rate=1.0)

        # Should be SLOW with these strict thresholds
        assert state.progress_state == ProgressState.SLOW
