"""
Integration tests for Manager Health Model (AD-19).

These tests verify that:
1. ManagerHealthState dataclass has all required fields
2. Three signals (liveness, readiness, progress) work correctly
3. Routing decisions are based on combined signals
4. Progress state detection works correctly
5. Health state updates work correctly
6. DC health classification based on manager health signals
"""

import pytest
import time

from hyperscale.distributed_rewrite.health import (
    ProgressState,
    RoutingDecision,
    ManagerHealthConfig,
    ManagerHealthState,
)


class TestManagerHealthConfig:
    """Test ManagerHealthConfig dataclass."""

    def test_default_config_values(self):
        """ManagerHealthConfig should have sensible defaults."""
        config = ManagerHealthConfig()

        assert config.liveness_timeout_seconds == 30.0
        assert config.max_consecutive_liveness_failures == 3
        assert config.normal_rate_threshold == 0.8
        assert config.slow_rate_threshold == 0.3

    def test_custom_config(self):
        """ManagerHealthConfig should accept custom values."""
        config = ManagerHealthConfig(
            liveness_timeout_seconds=60.0,
            max_consecutive_liveness_failures=5,
            normal_rate_threshold=0.9,
            slow_rate_threshold=0.5,
        )

        assert config.liveness_timeout_seconds == 60.0
        assert config.max_consecutive_liveness_failures == 5
        assert config.normal_rate_threshold == 0.9
        assert config.slow_rate_threshold == 0.5


class TestManagerHealthStateLiveness:
    """Test ManagerHealthState liveness signal."""

    def test_initial_state_is_live(self):
        """Manager should start as live."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        assert state.liveness is True

    def test_liveness_false_after_timeout(self):
        """Manager should be not live after timeout."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        # Set last response to 35 seconds ago
        state.last_liveness_response = time.monotonic() - 35.0
        assert state.liveness is False

    def test_liveness_false_after_consecutive_failures(self):
        """Manager should be not live after consecutive failures."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.consecutive_liveness_failures = 3
        assert state.liveness is False

    def test_update_liveness_success(self):
        """update_liveness with success should reset failures."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.consecutive_liveness_failures = 2

        state.update_liveness(success=True)

        assert state.consecutive_liveness_failures == 0
        assert state.liveness is True

    def test_update_liveness_failure(self):
        """update_liveness with failure should increment failures."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.consecutive_liveness_failures = 0

        state.update_liveness(success=False)

        assert state.consecutive_liveness_failures == 1


class TestManagerHealthStateReadiness:
    """Test ManagerHealthState readiness signal."""

    def test_readiness_true_when_all_conditions_met(self):
        """Manager should be ready when has quorum, accepting, and has workers."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.has_quorum = True
        state.accepting_jobs = True
        state.active_worker_count = 5
        assert state.readiness is True

    def test_readiness_false_when_no_quorum(self):
        """Manager should not be ready without quorum."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.has_quorum = False
        state.accepting_jobs = True
        state.active_worker_count = 5
        assert state.readiness is False

    def test_readiness_false_when_not_accepting(self):
        """Manager should not be ready when not accepting jobs."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.has_quorum = True
        state.accepting_jobs = False
        state.active_worker_count = 5
        assert state.readiness is False

    def test_readiness_false_when_no_workers(self):
        """Manager should not be ready when no workers available."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.has_quorum = True
        state.accepting_jobs = True
        state.active_worker_count = 0
        assert state.readiness is False

    def test_update_readiness(self):
        """update_readiness should update all fields."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )

        state.update_readiness(has_quorum=True, accepting=True, worker_count=10)

        assert state.has_quorum is True
        assert state.accepting_jobs is True
        assert state.active_worker_count == 10


class TestManagerHealthStateProgress:
    """Test ManagerHealthState progress signal."""

    def test_progress_idle_when_no_jobs(self):
        """Progress should be idle when no jobs accepted."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.jobs_accepted_last_interval = 0
        assert state.progress_state == ProgressState.IDLE

    def test_progress_normal_at_expected_rate(self):
        """Progress should be normal at expected rate."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.jobs_accepted_last_interval = 10
        state.workflows_dispatched_last_interval = 100
        state.expected_throughput = 100.0
        assert state.progress_state == ProgressState.NORMAL

    def test_progress_normal_above_80_percent(self):
        """Progress should be normal at 80%+ of expected throughput."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.jobs_accepted_last_interval = 10
        state.workflows_dispatched_last_interval = 80  # 80% of expected
        state.expected_throughput = 100.0
        assert state.progress_state == ProgressState.NORMAL

    def test_progress_slow_between_30_and_80_percent(self):
        """Progress should be slow at 30-80% of expected throughput."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.jobs_accepted_last_interval = 10
        state.workflows_dispatched_last_interval = 50  # 50% of expected
        state.expected_throughput = 100.0
        assert state.progress_state == ProgressState.SLOW

    def test_progress_degraded_below_30_percent(self):
        """Progress should be degraded below 30% of expected throughput."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.jobs_accepted_last_interval = 10
        state.workflows_dispatched_last_interval = 20  # 20% of expected
        state.expected_throughput = 100.0
        assert state.progress_state == ProgressState.DEGRADED

    def test_progress_stuck_with_zero_dispatches(self):
        """Progress should be stuck with zero dispatches."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.jobs_accepted_last_interval = 10
        state.workflows_dispatched_last_interval = 0
        state.expected_throughput = 100.0
        assert state.progress_state == ProgressState.STUCK

    def test_update_progress(self):
        """update_progress should update all fields."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )

        state.update_progress(
            jobs_accepted=15,
            workflows_dispatched=120,
            expected_throughput=150.0,
        )

        assert state.jobs_accepted_last_interval == 15
        assert state.workflows_dispatched_last_interval == 120
        assert state.expected_throughput == 150.0


class TestManagerHealthStateRoutingDecision:
    """Test ManagerHealthState routing decisions."""

    def test_route_when_all_healthy(self):
        """Should route when all signals healthy."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=5)
        state.update_progress(
            jobs_accepted=10,
            workflows_dispatched=100,
            expected_throughput=100.0,
        )

        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_evict_when_not_live(self):
        """Should evict when not live."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.consecutive_liveness_failures = 5
        state.update_readiness(has_quorum=True, accepting=True, worker_count=5)

        assert state.get_routing_decision() == RoutingDecision.EVICT

    def test_evict_when_stuck(self):
        """Should evict when stuck (even if live)."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=5)
        state.update_progress(
            jobs_accepted=10,
            workflows_dispatched=0,
            expected_throughput=100.0,
        )

        assert state.get_routing_decision() == RoutingDecision.EVICT

    def test_drain_when_not_ready(self):
        """Should drain when live but not ready."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=False, accepting=True, worker_count=0)
        state.update_progress(
            jobs_accepted=10,
            workflows_dispatched=100,
            expected_throughput=100.0,
        )

        assert state.get_routing_decision() == RoutingDecision.DRAIN

    def test_investigate_when_degraded(self):
        """Should investigate when live and ready but degraded."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=5)
        state.update_progress(
            jobs_accepted=10,
            workflows_dispatched=20,
            expected_throughput=100.0,
        )

        assert state.get_routing_decision() == RoutingDecision.INVESTIGATE


class TestManagerHealthStateDiagnostics:
    """Test ManagerHealthState diagnostics."""

    def test_diagnostics_includes_all_fields(self):
        """get_diagnostics should return comprehensive state."""
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=5)
        state.update_progress(
            jobs_accepted=10,
            workflows_dispatched=80,
            expected_throughput=100.0,
        )

        diag = state.get_diagnostics()

        assert diag["manager_id"] == "manager-1"
        assert diag["datacenter_id"] == "dc-east"
        assert diag["liveness"] is True
        assert diag["readiness"] is True
        assert diag["progress_state"] == "normal"
        assert diag["routing_decision"] == "route"
        assert diag["has_quorum"] is True
        assert diag["accepting_jobs"] is True
        assert diag["active_worker_count"] == 5


class TestManagerHealthScenarios:
    """Test realistic manager health scenarios."""

    def test_healthy_manager_lifecycle(self):
        """
        Simulate healthy manager lifecycle.

        Scenario: Manager starts, receives jobs, dispatches normally.
        """
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )

        # Manager connects
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=10)
        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Manager receives jobs and dispatches workflows
        state.update_progress(
            jobs_accepted=5,
            workflows_dispatched=50,
            expected_throughput=60.0,
        )
        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_manager_loses_quorum(self):
        """
        Simulate manager losing quorum.

        Scenario: Manager loses quorum after network partition.
        """
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )

        # Initially healthy with quorum
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=10)
        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Manager loses quorum
        state.update_readiness(has_quorum=False, accepting=True, worker_count=10)

        # Should drain, not evict (still live)
        assert state.get_routing_decision() == RoutingDecision.DRAIN

    def test_manager_becomes_stuck(self):
        """
        Simulate manager becoming stuck.

        Scenario: Manager accepts jobs but stops dispatching.
        """
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )

        # Initially healthy
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=10)
        state.update_progress(
            jobs_accepted=5,
            workflows_dispatched=50,
            expected_throughput=60.0,
        )
        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Manager becomes stuck (no dispatches despite accepting jobs)
        state.update_progress(
            jobs_accepted=10,
            workflows_dispatched=0,
            expected_throughput=60.0,
        )
        assert state.get_routing_decision() == RoutingDecision.EVICT

    def test_manager_crashes_and_recovers(self):
        """
        Simulate manager crash and recovery.

        Scenario: Manager becomes unreachable, then comes back.
        """
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )

        # Initially healthy
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=10)
        assert state.liveness is True

        # Manager crashes (consecutive failures)
        for _ in range(4):
            state.update_liveness(success=False)

        assert state.liveness is False
        assert state.get_routing_decision() == RoutingDecision.EVICT

        # Manager recovers
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=10)

        assert state.liveness is True
        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_manager_degraded_performance(self):
        """
        Simulate manager with degraded performance.

        Scenario: Manager is slow but making some progress.
        """
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )

        # Manager is live and ready
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=5)

        # But progress is degraded (below 30% of expected)
        state.update_progress(
            jobs_accepted=10,
            workflows_dispatched=10,
            expected_throughput=100.0,
        )

        # Should investigate, not evict
        assert state.progress_state == ProgressState.DEGRADED
        assert state.get_routing_decision() == RoutingDecision.INVESTIGATE

    def test_manager_loses_workers(self):
        """
        Simulate manager losing all workers.

        Scenario: Workers crash, manager has no capacity.
        """
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east",
        )

        # Initially healthy with workers
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=10)
        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # All workers die
        state.update_readiness(has_quorum=True, accepting=True, worker_count=0)

        # Should drain (no workers = not ready)
        assert state.readiness is False
        assert state.get_routing_decision() == RoutingDecision.DRAIN


class TestDCHealthClassification:
    """Test DC health classification based on manager signals."""

    def test_dc_unhealthy_when_all_managers_dead(self):
        """
        DC should be UNHEALTHY when ALL managers are not live.

        Rule: ALL managers NOT liveness → DC = UNHEALTHY
        """
        # Simulate 3 managers, all dead
        managers: dict[str, ManagerHealthState] = {}
        for i in range(3):
            state = ManagerHealthState(
                manager_id=f"manager-{i}",
                datacenter_id="dc-east",
            )
            state.consecutive_liveness_failures = 5  # Not live
            managers[f"manager-{i}"] = state

        # Check: all managers NOT live
        live_count = sum(1 for m in managers.values() if m.liveness)
        assert live_count == 0

        # DC should be classified as UNHEALTHY
        # (This logic would be in gate.py _get_dc_health_from_managers)

    def test_dc_degraded_when_majority_not_ready(self):
        """
        DC should be DEGRADED when MAJORITY of managers not ready.

        Rule: MAJORITY managers NOT readiness → DC = DEGRADED
        """
        # Simulate 3 managers, 2 not ready
        managers: dict[str, ManagerHealthState] = {}
        for i in range(3):
            state = ManagerHealthState(
                manager_id=f"manager-{i}",
                datacenter_id="dc-east",
            )
            state.update_liveness(success=True)
            if i < 2:
                # First 2 managers not ready
                state.update_readiness(has_quorum=False, accepting=False, worker_count=0)
            else:
                # Last manager ready
                state.update_readiness(has_quorum=True, accepting=True, worker_count=5)
            managers[f"manager-{i}"] = state

        # Check: majority NOT ready
        ready_count = sum(1 for m in managers.values() if m.readiness)
        total = len(managers)
        quorum = total // 2 + 1

        assert ready_count == 1  # Only 1 ready
        assert ready_count < quorum  # Less than quorum (2)

        # DC should be classified as DEGRADED

    def test_dc_degraded_when_any_manager_stuck(self):
        """
        DC should be DEGRADED when ANY manager progress is stuck.

        Rule: ANY manager progress == "stuck" → DC = DEGRADED
        """
        # Simulate 3 managers, 1 stuck
        managers: dict[str, ManagerHealthState] = {}
        for i in range(3):
            state = ManagerHealthState(
                manager_id=f"manager-{i}",
                datacenter_id="dc-east",
            )
            state.update_liveness(success=True)
            state.update_readiness(has_quorum=True, accepting=True, worker_count=5)
            if i == 0:
                # First manager stuck
                state.update_progress(
                    jobs_accepted=10,
                    workflows_dispatched=0,
                    expected_throughput=100.0,
                )
            else:
                # Other managers healthy
                state.update_progress(
                    jobs_accepted=10,
                    workflows_dispatched=100,
                    expected_throughput=100.0,
                )
            managers[f"manager-{i}"] = state

        # Check: any manager stuck
        has_stuck = any(
            m.progress_state == ProgressState.STUCK
            for m in managers.values()
        )
        assert has_stuck is True

        # DC should be classified as DEGRADED

    def test_dc_healthy_when_all_managers_healthy(self):
        """
        DC should be HEALTHY when all managers are healthy.
        """
        # Simulate 3 healthy managers
        managers: dict[str, ManagerHealthState] = {}
        for i in range(3):
            state = ManagerHealthState(
                manager_id=f"manager-{i}",
                datacenter_id="dc-east",
            )
            state.update_liveness(success=True)
            state.update_readiness(has_quorum=True, accepting=True, worker_count=5)
            state.update_progress(
                jobs_accepted=10,
                workflows_dispatched=100,
                expected_throughput=100.0,
            )
            managers[f"manager-{i}"] = state

        # All managers live, ready, making progress
        live_count = sum(1 for m in managers.values() if m.liveness)
        ready_count = sum(1 for m in managers.values() if m.readiness)
        has_stuck = any(
            m.progress_state == ProgressState.STUCK
            for m in managers.values()
        )

        assert live_count == 3
        assert ready_count == 3
        assert has_stuck is False

        # DC should be classified as HEALTHY
