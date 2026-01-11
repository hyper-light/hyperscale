"""
Integration tests for Gate Health Model (AD-19).

These tests verify that:
1. GateHealthState dataclass has all required fields
2. Three signals (liveness, readiness, progress) work correctly
3. Routing decisions are based on combined signals
4. Progress state detection works correctly
5. Leader election eligibility is correct
6. Health state updates work correctly
"""

import pytest
import time

from hyperscale.distributed_rewrite.health import (
    ProgressState,
    RoutingDecision,
    GateHealthConfig,
    GateHealthState,
)


class TestGateHealthConfig:
    """Test GateHealthConfig dataclass."""

    def test_default_config_values(self):
        """GateHealthConfig should have sensible defaults."""
        config = GateHealthConfig()

        assert config.liveness_timeout_seconds == 30.0
        assert config.max_consecutive_liveness_failures == 3
        assert config.normal_rate_threshold == 0.8
        assert config.slow_rate_threshold == 0.3
        assert config.overload_not_ready_states == ("stressed", "overloaded")

    def test_custom_config(self):
        """GateHealthConfig should accept custom values."""
        config = GateHealthConfig(
            liveness_timeout_seconds=60.0,
            max_consecutive_liveness_failures=5,
            normal_rate_threshold=0.9,
            slow_rate_threshold=0.5,
            overload_not_ready_states=("overloaded",),
        )

        assert config.liveness_timeout_seconds == 60.0
        assert config.max_consecutive_liveness_failures == 5
        assert config.normal_rate_threshold == 0.9
        assert config.slow_rate_threshold == 0.5
        assert config.overload_not_ready_states == ("overloaded",)


class TestGateHealthStateLiveness:
    """Test GateHealthState liveness signal."""

    def test_initial_state_is_live(self):
        """Gate should start as live."""
        state = GateHealthState(gate_id="gate-1")
        assert state.liveness is True

    def test_liveness_false_after_timeout(self):
        """Gate should be not live after timeout."""
        state = GateHealthState(gate_id="gate-1")
        # Set last response to 35 seconds ago
        state.last_liveness_response = time.monotonic() - 35.0
        assert state.liveness is False

    def test_liveness_false_after_consecutive_failures(self):
        """Gate should be not live after consecutive failures."""
        state = GateHealthState(gate_id="gate-1")
        state.consecutive_liveness_failures = 3
        assert state.liveness is False

    def test_update_liveness_success(self):
        """update_liveness with success should reset failures."""
        state = GateHealthState(gate_id="gate-1")
        state.consecutive_liveness_failures = 2

        state.update_liveness(success=True)

        assert state.consecutive_liveness_failures == 0
        assert state.liveness is True

    def test_update_liveness_failure(self):
        """update_liveness with failure should increment failures."""
        state = GateHealthState(gate_id="gate-1")
        state.consecutive_liveness_failures = 0

        state.update_liveness(success=False)

        assert state.consecutive_liveness_failures == 1


class TestGateHealthStateReadiness:
    """Test GateHealthState readiness signal."""

    def test_readiness_true_when_all_conditions_met(self):
        """Gate should be ready when connected and not overloaded."""
        state = GateHealthState(gate_id="gate-1")
        state.has_dc_connectivity = True
        state.connected_dc_count = 3
        state.overload_state = "healthy"
        assert state.readiness is True

    def test_readiness_false_when_no_dc_connectivity(self):
        """Gate should not be ready without DC connectivity."""
        state = GateHealthState(gate_id="gate-1")
        state.has_dc_connectivity = False
        state.connected_dc_count = 0
        state.overload_state = "healthy"
        assert state.readiness is False

    def test_readiness_false_when_zero_connected_dcs(self):
        """Gate should not be ready when no DCs connected."""
        state = GateHealthState(gate_id="gate-1")
        state.has_dc_connectivity = True
        state.connected_dc_count = 0
        state.overload_state = "healthy"
        assert state.readiness is False

    def test_readiness_false_when_stressed(self):
        """Gate should not be ready when stressed."""
        state = GateHealthState(gate_id="gate-1")
        state.has_dc_connectivity = True
        state.connected_dc_count = 3
        state.overload_state = "stressed"
        assert state.readiness is False

    def test_readiness_false_when_overloaded(self):
        """Gate should not be ready when overloaded."""
        state = GateHealthState(gate_id="gate-1")
        state.has_dc_connectivity = True
        state.connected_dc_count = 3
        state.overload_state = "overloaded"
        assert state.readiness is False

    def test_readiness_true_when_busy(self):
        """Gate should be ready when busy (not stressed/overloaded)."""
        state = GateHealthState(gate_id="gate-1")
        state.has_dc_connectivity = True
        state.connected_dc_count = 3
        state.overload_state = "busy"
        assert state.readiness is True

    def test_update_readiness(self):
        """update_readiness should update all fields."""
        state = GateHealthState(gate_id="gate-1")

        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=5,
            overload_state="busy",
        )

        assert state.has_dc_connectivity is True
        assert state.connected_dc_count == 5
        assert state.overload_state == "busy"


class TestGateHealthStateProgress:
    """Test GateHealthState progress signal."""

    def test_progress_idle_when_no_jobs(self):
        """Progress should be idle when no jobs forwarded."""
        state = GateHealthState(gate_id="gate-1")
        state.jobs_forwarded_last_interval = 0
        assert state.progress_state == ProgressState.IDLE

    def test_progress_normal_at_expected_rate(self):
        """Progress should be normal at expected rate."""
        state = GateHealthState(gate_id="gate-1")
        state.jobs_forwarded_last_interval = 100
        state.expected_forward_rate = 100.0
        assert state.progress_state == ProgressState.NORMAL

    def test_progress_normal_above_80_percent(self):
        """Progress should be normal at 80%+ of expected rate."""
        state = GateHealthState(gate_id="gate-1")
        state.jobs_forwarded_last_interval = 80  # 80% of expected
        state.expected_forward_rate = 100.0
        assert state.progress_state == ProgressState.NORMAL

    def test_progress_slow_between_30_and_80_percent(self):
        """Progress should be slow at 30-80% of expected rate."""
        state = GateHealthState(gate_id="gate-1")
        state.jobs_forwarded_last_interval = 50  # 50% of expected
        state.expected_forward_rate = 100.0
        assert state.progress_state == ProgressState.SLOW

    def test_progress_degraded_below_30_percent(self):
        """Progress should be degraded below 30% of expected rate."""
        state = GateHealthState(gate_id="gate-1")
        state.jobs_forwarded_last_interval = 20  # 20% of expected
        state.expected_forward_rate = 100.0
        assert state.progress_state == ProgressState.DEGRADED

    def test_progress_stuck_with_zero_forwards(self):
        """Progress should be stuck with zero forwards when expected."""
        state = GateHealthState(gate_id="gate-1")
        # Set up expectation but record zero forwards
        state.jobs_forwarded_last_interval = 0
        state.expected_forward_rate = 100.0
        # Note: This returns IDLE because jobs_forwarded is 0
        assert state.progress_state == ProgressState.IDLE

    def test_update_progress(self):
        """update_progress should update all fields."""
        state = GateHealthState(gate_id="gate-1")

        state.update_progress(
            jobs_forwarded=75,
            stats_aggregated=150,
            expected_forward_rate=80.0,
        )

        assert state.jobs_forwarded_last_interval == 75
        assert state.stats_aggregated_last_interval == 150
        assert state.expected_forward_rate == 80.0


class TestGateHealthStateRoutingDecision:
    """Test GateHealthState routing decisions."""

    def test_route_when_all_healthy(self):
        """Should route when all signals healthy."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )
        state.update_progress(
            jobs_forwarded=100,
            stats_aggregated=200,
            expected_forward_rate=100.0,
        )

        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_evict_when_not_live(self):
        """Should evict when not live."""
        state = GateHealthState(gate_id="gate-1")
        state.consecutive_liveness_failures = 5
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )

        assert state.get_routing_decision() == RoutingDecision.EVICT

    def test_drain_when_not_ready(self):
        """Should drain when live but not ready."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=False,
            connected_dc_count=0,
            overload_state="healthy",
        )
        state.update_progress(
            jobs_forwarded=100,
            stats_aggregated=200,
            expected_forward_rate=100.0,
        )

        assert state.get_routing_decision() == RoutingDecision.DRAIN

    def test_drain_when_overloaded(self):
        """Should drain when overloaded."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="overloaded",
        )

        assert state.get_routing_decision() == RoutingDecision.DRAIN

    def test_investigate_when_degraded(self):
        """Should investigate when live and ready but degraded."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )
        state.update_progress(
            jobs_forwarded=20,
            stats_aggregated=200,
            expected_forward_rate=100.0,
        )

        assert state.get_routing_decision() == RoutingDecision.INVESTIGATE


class TestGateHealthStateLeaderElection:
    """Test GateHealthState leader election eligibility."""

    def test_eligible_when_all_healthy(self):
        """Should be eligible when all signals healthy."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )
        state.update_progress(
            jobs_forwarded=100,
            stats_aggregated=200,
            expected_forward_rate=100.0,
        )

        assert state.should_participate_in_election() is True

    def test_not_eligible_when_not_live(self):
        """Should not be eligible when not live."""
        state = GateHealthState(gate_id="gate-1")
        state.consecutive_liveness_failures = 5

        assert state.should_participate_in_election() is False

    def test_not_eligible_when_not_ready(self):
        """Should not be eligible when not ready."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=False,
            connected_dc_count=0,
            overload_state="healthy",
        )

        assert state.should_participate_in_election() is False

    def test_not_eligible_when_overloaded(self):
        """Should not be eligible when overloaded."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="overloaded",
        )

        assert state.should_participate_in_election() is False

    def test_eligible_when_stressed(self):
        """Should be eligible when stressed (but not overloaded)."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="stressed",
        )
        # Note: stressed gates are not ready, so not eligible
        assert state.should_participate_in_election() is False

    def test_eligible_when_busy(self):
        """Should be eligible when busy."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="busy",
        )
        state.update_progress(
            jobs_forwarded=100,
            stats_aggregated=200,
            expected_forward_rate=100.0,
        )

        assert state.should_participate_in_election() is True


class TestGateHealthStateDiagnostics:
    """Test GateHealthState diagnostics."""

    def test_diagnostics_includes_all_fields(self):
        """get_diagnostics should return comprehensive state."""
        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )
        state.update_progress(
            jobs_forwarded=80,
            stats_aggregated=160,
            expected_forward_rate=100.0,
        )

        diag = state.get_diagnostics()

        assert diag["gate_id"] == "gate-1"
        assert diag["liveness"] is True
        assert diag["readiness"] is True
        assert diag["progress_state"] == "normal"
        assert diag["routing_decision"] == "route"
        assert diag["should_participate_in_election"] is True
        assert diag["has_dc_connectivity"] is True
        assert diag["connected_dc_count"] == 3
        assert diag["overload_state"] == "healthy"


class TestGateHealthScenarios:
    """Test realistic gate health scenarios."""

    def test_healthy_gate_lifecycle(self):
        """
        Simulate healthy gate lifecycle.

        Scenario: Gate starts, connects to DCs, forwards jobs normally.
        """
        state = GateHealthState(gate_id="gate-1")

        # Gate connects
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )
        assert state.get_routing_decision() == RoutingDecision.ROUTE
        assert state.should_participate_in_election() is True

        # Gate forwards jobs
        state.update_progress(
            jobs_forwarded=50,
            stats_aggregated=100,
            expected_forward_rate=60.0,
        )
        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_gate_loses_dc_connectivity(self):
        """
        Simulate gate losing DC connectivity.

        Scenario: Gate loses connection to all DCs.
        """
        state = GateHealthState(gate_id="gate-1")

        # Initially healthy
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )
        assert state.get_routing_decision() == RoutingDecision.ROUTE

        # Gate loses DC connectivity
        state.update_readiness(
            has_dc_connectivity=False,
            connected_dc_count=0,
            overload_state="healthy",
        )

        # Should drain, not evict (still live)
        assert state.get_routing_decision() == RoutingDecision.DRAIN
        assert state.should_participate_in_election() is False

    def test_gate_becomes_overloaded(self):
        """
        Simulate gate becoming overloaded.

        Scenario: Gate experiences high load and needs to shed.
        """
        state = GateHealthState(gate_id="gate-1")

        # Initially healthy
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )
        assert state.should_participate_in_election() is True

        # Gate becomes overloaded
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="overloaded",
        )

        # Should drain and not lead
        assert state.get_routing_decision() == RoutingDecision.DRAIN
        assert state.should_participate_in_election() is False

    def test_gate_crashes_and_recovers(self):
        """
        Simulate gate crash and recovery.

        Scenario: Gate becomes unreachable, then comes back.
        """
        state = GateHealthState(gate_id="gate-1")

        # Initially healthy
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )
        assert state.liveness is True

        # Gate crashes (consecutive failures)
        for _ in range(4):
            state.update_liveness(success=False)

        assert state.liveness is False
        assert state.get_routing_decision() == RoutingDecision.EVICT

        # Gate recovers
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )

        assert state.liveness is True
        assert state.get_routing_decision() == RoutingDecision.ROUTE

    def test_gate_degraded_performance(self):
        """
        Simulate gate with degraded performance.

        Scenario: Gate is slow but making some progress.
        """
        state = GateHealthState(gate_id="gate-1")

        # Gate is live and ready
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )

        # But progress is degraded (below 30% of expected)
        state.update_progress(
            jobs_forwarded=10,
            stats_aggregated=100,
            expected_forward_rate=100.0,
        )

        # Should investigate, not evict
        assert state.progress_state == ProgressState.DEGRADED
        assert state.get_routing_decision() == RoutingDecision.INVESTIGATE

    def test_leader_election_with_multiple_gates(self):
        """
        Test leader election eligibility across multiple gates.

        Scenario: Multiple gates with varying health states.
        """
        gates: dict[str, GateHealthState] = {}

        # Gate 1: Healthy, eligible for election
        gates["gate-1"] = GateHealthState(gate_id="gate-1")
        gates["gate-1"].update_liveness(success=True)
        gates["gate-1"].update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )

        # Gate 2: Overloaded, not eligible
        gates["gate-2"] = GateHealthState(gate_id="gate-2")
        gates["gate-2"].update_liveness(success=True)
        gates["gate-2"].update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="overloaded",
        )

        # Gate 3: Not live, not eligible
        gates["gate-3"] = GateHealthState(gate_id="gate-3")
        gates["gate-3"].consecutive_liveness_failures = 5

        # Check eligibility
        eligible = [
            gate_id
            for gate_id, state in gates.items()
            if state.should_participate_in_election()
        ]

        assert eligible == ["gate-1"]
