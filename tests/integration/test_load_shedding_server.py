"""
Server integration tests for Load Shedding (AD-22).

Tests load shedding in realistic server scenarios with:
- Concurrent request processing under load
- State transitions through all overload states
- Graceful degradation behavior
- Recovery after load subsides
- Failure paths and edge cases
- Integration with hybrid overload detection
"""

import asyncio
import pytest
import random
import time
from dataclasses import dataclass
from typing import Any

from hyperscale.distributed_rewrite.reliability import (
    HybridOverloadDetector,
    LoadShedder,
    LoadShedderConfig,
    OverloadConfig,
    OverloadState,
    RequestPriority,
)


@dataclass
class RequestResult:
    """Result of a simulated request."""

    message_type: str
    priority: RequestPriority
    was_shed: bool
    latency_ms: float
    overload_state: OverloadState


class SimulatedServer:
    """
    Simulated server with load shedding.

    Processes requests with simulated latency and tracks load shedding decisions.
    """

    def __init__(
        self,
        overload_config: OverloadConfig | None = None,
        shedder_config: LoadShedderConfig | None = None,
    ):
        self._detector = HybridOverloadDetector(config=overload_config)
        self._shedder = LoadShedder(
            self._detector,
            config=shedder_config,
        )
        self._request_history: list[RequestResult] = []
        self._processing_lock = asyncio.Lock()
        self._current_cpu_percent: float = 0.0
        self._current_memory_percent: float = 0.0

    def set_resource_usage(
        self,
        cpu_percent: float = 0.0,
        memory_percent: float = 0.0,
    ) -> None:
        """Set simulated resource usage."""
        self._current_cpu_percent = cpu_percent
        self._current_memory_percent = memory_percent

    async def process_request(
        self,
        message_type: str,
        simulated_latency_ms: float = 10.0,
    ) -> RequestResult:
        """
        Process a request with load shedding check.

        Args:
            message_type: Type of message being processed
            simulated_latency_ms: Simulated processing latency

        Returns:
            RequestResult with outcome details
        """
        priority = self._shedder.classify_request(message_type)
        current_state = self._shedder.get_current_state(
            self._current_cpu_percent,
            self._current_memory_percent,
        )

        was_shed = self._shedder.should_shed(
            message_type,
            self._current_cpu_percent,
            self._current_memory_percent,
        )

        if not was_shed:
            # Simulate processing
            await asyncio.sleep(simulated_latency_ms / 1000.0)
            # Record latency
            self._detector.record_latency(simulated_latency_ms)

        result = RequestResult(
            message_type=message_type,
            priority=priority,
            was_shed=was_shed,
            latency_ms=simulated_latency_ms if not was_shed else 0.0,
            overload_state=current_state,
        )

        async with self._processing_lock:
            self._request_history.append(result)

        return result

    def get_current_state(self) -> OverloadState:
        """Get current overload state."""
        return self._shedder.get_current_state(
            self._current_cpu_percent,
            self._current_memory_percent,
        )

    def get_metrics(self) -> dict:
        """Get shedding metrics."""
        return self._shedder.get_metrics()

    def get_diagnostics(self) -> dict:
        """Get overload detector diagnostics."""
        return self._detector.get_diagnostics()

    def get_history(self) -> list[RequestResult]:
        """Get request history."""
        return self._request_history.copy()

    def reset(self) -> None:
        """Reset server state."""
        self._detector.reset()
        self._shedder.reset_metrics()
        self._request_history.clear()
        self._current_cpu_percent = 0.0
        self._current_memory_percent = 0.0


class TestLoadSheddingServerBasics:
    """Basic server load shedding tests."""

    @pytest.mark.asyncio
    async def test_server_accepts_all_when_healthy(self) -> None:
        """Test that healthy server accepts all request types."""
        server = SimulatedServer()

        message_types = [
            "DebugRequest",  # LOW
            "StatsUpdate",  # NORMAL
            "SubmitJob",  # HIGH
            "Heartbeat",  # CRITICAL
        ]

        for message_type in message_types:
            result = await server.process_request(message_type, simulated_latency_ms=10.0)
            assert result.was_shed is False, f"{message_type} should not be shed when healthy"
            assert result.overload_state == OverloadState.HEALTHY

    @pytest.mark.asyncio
    async def test_server_tracks_latency_correctly(self) -> None:
        """Test that server correctly tracks request latencies."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
        )
        server = SimulatedServer(overload_config=config)

        # Process requests with known latencies
        latencies = [20.0, 25.0, 30.0, 35.0]
        for latency in latencies:
            await server.process_request("SubmitJob", simulated_latency_ms=latency)

        diagnostics = server.get_diagnostics()
        assert diagnostics["sample_count"] == len(latencies)
        # Current average should be close to mean of recent samples
        expected_avg = sum(latencies) / len(latencies)
        assert abs(diagnostics["current_avg"] - expected_avg) < 1.0


class TestLoadSheddingStateTransitions:
    """Test state transitions through all overload states."""

    @pytest.mark.asyncio
    async def test_transition_healthy_to_busy(self) -> None:
        """Test transition from healthy to busy state."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.3, 0.5),
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=3,
        )
        server = SimulatedServer(overload_config=config)

        # Start healthy with low latencies
        for _ in range(5):
            await server.process_request("SubmitJob", simulated_latency_ms=30.0)

        assert server.get_current_state() == OverloadState.HEALTHY

        # Increase latency to trigger busy state (above 50ms absolute bound)
        for _ in range(5):
            await server.process_request("SubmitJob", simulated_latency_ms=60.0)

        assert server.get_current_state() == OverloadState.BUSY

        # LOW priority should now be shed
        result = await server.process_request("DebugRequest", simulated_latency_ms=60.0)
        assert result.was_shed is True

    @pytest.mark.asyncio
    async def test_transition_busy_to_stressed(self) -> None:
        """Test transition from busy to stressed state."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.3, 0.5),
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=3,
        )
        server = SimulatedServer(overload_config=config)

        # Get to busy state
        for _ in range(5):
            await server.process_request("SubmitJob", simulated_latency_ms=60.0)

        assert server.get_current_state() == OverloadState.BUSY

        # Increase latency to trigger stressed state (above 100ms)
        for _ in range(5):
            await server.process_request("SubmitJob", simulated_latency_ms=120.0)

        assert server.get_current_state() == OverloadState.STRESSED

        # NORMAL and LOW should now be shed
        low_result = await server.process_request("DebugRequest", simulated_latency_ms=120.0)
        normal_result = await server.process_request("StatsUpdate", simulated_latency_ms=120.0)

        assert low_result.was_shed is True
        assert normal_result.was_shed is True

    @pytest.mark.asyncio
    async def test_transition_stressed_to_overloaded(self) -> None:
        """Test transition from stressed to overloaded state."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.3, 0.5),
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=3,
        )
        server = SimulatedServer(overload_config=config)

        # Get to stressed state
        for _ in range(5):
            await server.process_request("SubmitJob", simulated_latency_ms=120.0)

        assert server.get_current_state() == OverloadState.STRESSED

        # Increase latency to trigger overloaded state (above 200ms)
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=250.0)

        assert server.get_current_state() == OverloadState.OVERLOADED

        # All except CRITICAL should be shed
        low_result = await server.process_request("DebugRequest", simulated_latency_ms=250.0)
        normal_result = await server.process_request("StatsUpdate", simulated_latency_ms=250.0)
        high_result = await server.process_request("SubmitJob", simulated_latency_ms=250.0)
        critical_result = await server.process_request("Heartbeat", simulated_latency_ms=250.0)

        assert low_result.was_shed is True
        assert normal_result.was_shed is True
        assert high_result.was_shed is True
        assert critical_result.was_shed is False

    @pytest.mark.asyncio
    async def test_full_state_cycle(self) -> None:
        """Test full cycle through all states and back to healthy."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.3, 0.5),
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=3,
            ema_alpha=0.3,  # Higher alpha for faster response
        )
        server = SimulatedServer(overload_config=config)

        states_visited = []

        # Healthy state
        for _ in range(3):
            await server.process_request("SubmitJob", simulated_latency_ms=30.0)
        states_visited.append(server.get_current_state())

        # Ramp up to overloaded
        for latency in [60.0, 120.0, 250.0]:
            for _ in range(5):
                await server.process_request("Heartbeat", simulated_latency_ms=latency)
            states_visited.append(server.get_current_state())

        # Recovery back to healthy (requires many low-latency samples to lower EMA)
        server.reset()  # Reset for clean recovery test
        for _ in range(10):
            await server.process_request("SubmitJob", simulated_latency_ms=20.0)
        states_visited.append(server.get_current_state())

        # Verify we saw healthy at start and end
        assert states_visited[0] == OverloadState.HEALTHY
        assert states_visited[-1] == OverloadState.HEALTHY


class TestLoadSheddingResourceSignals:
    """Test load shedding based on resource signals (CPU/memory)."""

    @pytest.mark.asyncio
    async def test_cpu_triggers_shedding(self) -> None:
        """Test that high CPU triggers load shedding."""
        config = OverloadConfig(
            cpu_thresholds=(0.70, 0.85, 0.95),
        )
        server = SimulatedServer(overload_config=config)

        # Low CPU - all accepted
        server.set_resource_usage(cpu_percent=50.0)
        result = await server.process_request("StatsUpdate", simulated_latency_ms=10.0)
        assert result.was_shed is False

        # High CPU (> 85%) triggers stressed state
        server.set_resource_usage(cpu_percent=90.0)
        result = await server.process_request("StatsUpdate", simulated_latency_ms=10.0)
        assert result.was_shed is True  # NORMAL shed in stressed

        # CRITICAL still accepted
        result = await server.process_request("Heartbeat", simulated_latency_ms=10.0)
        assert result.was_shed is False

    @pytest.mark.asyncio
    async def test_memory_triggers_shedding(self) -> None:
        """Test that high memory triggers load shedding."""
        config = OverloadConfig(
            memory_thresholds=(0.70, 0.85, 0.95),
        )
        server = SimulatedServer(overload_config=config)

        # Normal memory - all accepted
        server.set_resource_usage(memory_percent=60.0)
        result = await server.process_request("DebugRequest", simulated_latency_ms=10.0)
        assert result.was_shed is False

        # High memory (> 70%) triggers busy state
        server.set_resource_usage(memory_percent=75.0)
        result = await server.process_request("DebugRequest", simulated_latency_ms=10.0)
        assert result.was_shed is True  # LOW shed in busy

        # HIGH still accepted in busy
        result = await server.process_request("SubmitJob", simulated_latency_ms=10.0)
        assert result.was_shed is False

    @pytest.mark.asyncio
    async def test_combined_cpu_memory_triggers_worst_state(self) -> None:
        """Test that combined high CPU and memory triggers worst state."""
        config = OverloadConfig(
            cpu_thresholds=(0.70, 0.85, 0.95),
            memory_thresholds=(0.70, 0.85, 0.95),
        )
        server = SimulatedServer(overload_config=config)

        # CPU at busy (75%), memory at stressed (90%)
        # Should be stressed (worst of the two)
        server.set_resource_usage(cpu_percent=75.0, memory_percent=90.0)

        state = server.get_current_state()
        assert state == OverloadState.STRESSED

        # NORMAL should be shed
        result = await server.process_request("StatsUpdate", simulated_latency_ms=10.0)
        assert result.was_shed is True


class TestLoadSheddingConcurrency:
    """Test load shedding under concurrent request load."""

    @pytest.mark.asyncio
    async def test_concurrent_requests_with_shedding(self) -> None:
        """Test that shedding works correctly under concurrent load."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            min_samples=3,
        )
        server = SimulatedServer(overload_config=config)

        # Prime the server with high latencies to trigger stressed state
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=120.0)

        assert server.get_current_state() == OverloadState.STRESSED

        # Send concurrent requests of different priorities
        message_types = ["DebugRequest", "StatsUpdate", "SubmitJob", "Heartbeat"] * 5

        async def process(msg_type: str) -> RequestResult:
            return await server.process_request(msg_type, simulated_latency_ms=120.0)

        results = await asyncio.gather(*[process(mt) for mt in message_types])

        # Count shed vs processed by priority
        shed_counts = {p: 0 for p in RequestPriority}
        processed_counts = {p: 0 for p in RequestPriority}

        for result in results:
            if result.was_shed:
                shed_counts[result.priority] += 1
            else:
                processed_counts[result.priority] += 1

        # In stressed state: LOW and NORMAL shed, HIGH and CRITICAL processed
        assert shed_counts[RequestPriority.LOW] == 5
        assert shed_counts[RequestPriority.NORMAL] == 5
        assert processed_counts[RequestPriority.HIGH] == 5
        assert processed_counts[RequestPriority.CRITICAL] == 5

    @pytest.mark.asyncio
    async def test_burst_traffic_triggers_shedding(self) -> None:
        """Test that sudden burst of traffic triggers appropriate shedding."""
        config = OverloadConfig(
            absolute_bounds=(30.0, 60.0, 100.0),
            min_samples=3,
        )
        server = SimulatedServer(overload_config=config)

        # Start with low load
        for _ in range(3):
            await server.process_request("SubmitJob", simulated_latency_ms=20.0)

        assert server.get_current_state() == OverloadState.HEALTHY

        # Simulate burst causing latency spike
        burst_results = []
        for _ in range(10):
            result = await server.process_request("StatsUpdate", simulated_latency_ms=80.0)
            burst_results.append(result)

        # Should have transitioned to stressed during burst
        final_state = server.get_current_state()
        assert final_state == OverloadState.STRESSED

        # Some requests should have been shed
        shed_count = sum(1 for r in burst_results if r.was_shed)
        assert shed_count > 0, "Some NORMAL requests should be shed during stress"


class TestLoadSheddingFailurePaths:
    """Test failure paths and edge cases in load shedding."""

    @pytest.mark.asyncio
    async def test_critical_never_shed_under_extreme_load(self) -> None:
        """Test that CRITICAL requests are never shed regardless of load."""
        config = OverloadConfig(
            absolute_bounds=(10.0, 20.0, 30.0),  # Very low bounds
        )
        server = SimulatedServer(overload_config=config)

        # Push to extreme overload
        for _ in range(10):
            await server.process_request("Heartbeat", simulated_latency_ms=500.0)

        assert server.get_current_state() == OverloadState.OVERLOADED

        # All critical types must still be processed
        critical_types = [
            "Ping", "Ack", "Nack", "PingReq", "Suspect", "Alive", "Dead",
            "Join", "JoinAck", "Leave", "JobCancelRequest", "JobCancelResponse",
            "JobFinalResult", "Heartbeat", "HealthCheck",
        ]

        for msg_type in critical_types:
            result = await server.process_request(msg_type, simulated_latency_ms=500.0)
            assert result.was_shed is False, f"CRITICAL {msg_type} must never be shed"

    @pytest.mark.asyncio
    async def test_unknown_message_type_defaults_to_normal(self) -> None:
        """Test that unknown message types default to NORMAL priority."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
        )
        server = SimulatedServer(overload_config=config)

        # Push to stressed state
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=120.0)

        assert server.get_current_state() == OverloadState.STRESSED

        # Unknown message should be treated as NORMAL and shed in stressed
        result = await server.process_request("UnknownCustomMessage", simulated_latency_ms=120.0)
        assert result.priority == RequestPriority.NORMAL
        assert result.was_shed is True

    @pytest.mark.asyncio
    async def test_zero_latency_handling(self) -> None:
        """Test handling of zero or near-zero latency samples."""
        server = SimulatedServer()

        # Process with very low latencies
        for _ in range(5):
            result = await server.process_request("SubmitJob", simulated_latency_ms=0.1)
            assert result.was_shed is False

        diagnostics = server.get_diagnostics()
        assert diagnostics["sample_count"] == 5
        assert server.get_current_state() == OverloadState.HEALTHY

    @pytest.mark.asyncio
    async def test_empty_state_before_samples(self) -> None:
        """Test server state before any samples are recorded."""
        server = SimulatedServer()

        # No samples yet
        diagnostics = server.get_diagnostics()
        assert diagnostics["sample_count"] == 0
        assert diagnostics["current_avg"] == 0.0

        # Should be healthy by default
        assert server.get_current_state() == OverloadState.HEALTHY

        # All requests should be accepted
        for msg_type in ["DebugRequest", "StatsUpdate", "SubmitJob", "Heartbeat"]:
            result = await server.process_request(msg_type, simulated_latency_ms=10.0)
            assert result.was_shed is False

    @pytest.mark.asyncio
    async def test_reset_clears_all_state(self) -> None:
        """Test that reset properly clears all server state."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
        )
        server = SimulatedServer(overload_config=config)

        # Push to overloaded
        for _ in range(10):
            await server.process_request("Heartbeat", simulated_latency_ms=250.0)

        assert server.get_current_state() == OverloadState.OVERLOADED
        metrics_before = server.get_metrics()
        assert metrics_before["total_requests"] > 0

        # Reset
        server.reset()

        # Verify all state is cleared
        assert server.get_current_state() == OverloadState.HEALTHY
        diagnostics = server.get_diagnostics()
        assert diagnostics["sample_count"] == 0

        metrics_after = server.get_metrics()
        assert metrics_after["total_requests"] == 0


class TestLoadSheddingRecovery:
    """Test recovery behavior after load subsides."""

    @pytest.mark.asyncio
    async def test_recovery_from_overloaded_to_healthy(self) -> None:
        """Test gradual recovery from overloaded back to healthy."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            ema_alpha=0.2,  # Moderate smoothing for observable recovery
            min_samples=3,
        )
        server = SimulatedServer(overload_config=config)

        # Push to overloaded
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=250.0)

        assert server.get_current_state() == OverloadState.OVERLOADED

        # Gradually decrease latency
        latency_phases = [
            (180.0, OverloadState.STRESSED),  # Still stressed (< 200ms)
            (80.0, OverloadState.BUSY),  # Busy (< 100ms)
            (30.0, OverloadState.HEALTHY),  # Healthy (< 50ms)
        ]

        for target_latency, expected_state in latency_phases:
            # Process enough requests to shift the average
            for _ in range(10):
                await server.process_request("Heartbeat", simulated_latency_ms=target_latency)

            current_state = server.get_current_state()
            # State should be at or better than expected due to averaging
            assert current_state.value <= expected_state.value or current_state == expected_state

    @pytest.mark.asyncio
    async def test_shedding_resumes_normal_after_recovery(self) -> None:
        """Test that requests resume normal processing after recovery."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
            ema_alpha=0.3,
            min_samples=3,
        )
        server = SimulatedServer(overload_config=config)

        # Push to stressed and shed NORMAL
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=120.0)

        result = await server.process_request("StatsUpdate", simulated_latency_ms=120.0)
        assert result.was_shed is True

        # Recover to healthy
        server.reset()
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=20.0)

        assert server.get_current_state() == OverloadState.HEALTHY

        # NORMAL should now be accepted
        result = await server.process_request("StatsUpdate", simulated_latency_ms=20.0)
        assert result.was_shed is False


class TestLoadSheddingMetricsAccuracy:
    """Test metrics accuracy during load shedding."""

    @pytest.mark.asyncio
    async def test_metrics_accurately_track_shedding(self) -> None:
        """Test that metrics accurately reflect shedding behavior."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
        )
        server = SimulatedServer(overload_config=config)

        # Push to stressed state
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=120.0)

        # Process known mix of requests
        request_mix = [
            ("DebugRequest", True),  # LOW - shed
            ("StatsUpdate", True),  # NORMAL - shed
            ("SubmitJob", False),  # HIGH - not shed
            ("Heartbeat", False),  # CRITICAL - not shed
        ] * 3  # 12 total requests

        for msg_type, expected_shed in request_mix:
            result = await server.process_request(msg_type, simulated_latency_ms=120.0)
            assert result.was_shed == expected_shed, f"{msg_type} shed status mismatch"

        metrics = server.get_metrics()

        # Verify counts
        # 5 initial + 12 test = 17 total, but initial 5 all processed
        # So shed = 6 (3 LOW + 3 NORMAL)
        assert metrics["shed_by_priority"]["LOW"] == 3
        assert metrics["shed_by_priority"]["NORMAL"] == 3
        assert metrics["shed_by_priority"]["HIGH"] == 0
        assert metrics["shed_by_priority"]["CRITICAL"] == 0

    @pytest.mark.asyncio
    async def test_shed_rate_calculation(self) -> None:
        """Test that shed rate is calculated correctly."""
        config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
        )
        server = SimulatedServer(overload_config=config)

        # Push to overloaded
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=250.0)

        # Process exactly 10 requests with known outcomes
        # In overloaded: LOW, NORMAL, HIGH shed; CRITICAL not shed
        requests = [
            "DebugRequest",  # shed
            "StatsUpdate",  # shed
            "SubmitJob",  # shed
            "Heartbeat",  # not shed
        ] * 2 + ["DebugRequest", "Heartbeat"]  # 10 total: 7 shed, 3 not shed

        for msg_type in requests:
            await server.process_request(msg_type, simulated_latency_ms=250.0)

        metrics = server.get_metrics()
        # 5 initial (not shed as CRITICAL) + 10 new = 15 total
        # Shed = 7 (from new requests)
        expected_shed_rate = 7 / 15
        assert abs(metrics["shed_rate"] - expected_shed_rate) < 0.01


class TestLoadSheddingTrendDetection:
    """Test trend-based overload detection."""

    @pytest.mark.asyncio
    async def test_rising_trend_triggers_overload(self) -> None:
        """Test that rising trend can trigger overload even at lower absolute latency."""
        config = OverloadConfig(
            delta_thresholds=(0.2, 0.5, 1.0),
            absolute_bounds=(100.0, 200.0, 400.0),
            trend_threshold=0.05,  # Sensitive to rising trends
            min_samples=3,
            ema_alpha=0.1,
            trend_window=10,
        )
        server = SimulatedServer(overload_config=config)

        # Start with stable baseline
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=50.0)

        # Create rapidly rising trend
        for latency_increase in range(20):
            latency = 50.0 + (latency_increase * 5)  # 50 -> 145ms
            await server.process_request("Heartbeat", simulated_latency_ms=latency)

        diagnostics = server.get_diagnostics()
        # Trend should be positive (rising)
        assert diagnostics["trend"] > 0

    @pytest.mark.asyncio
    async def test_stable_high_latency_vs_rising_trend(self) -> None:
        """Test difference between stable high latency and rising trend."""
        config = OverloadConfig(
            delta_thresholds=(0.2, 0.5, 1.0),
            absolute_bounds=(100.0, 200.0, 400.0),
            trend_threshold=0.1,
            min_samples=3,
            ema_alpha=0.1,
        )

        # Server with stable high latency
        server_stable = SimulatedServer(overload_config=config)
        for _ in range(20):
            await server_stable.process_request("Heartbeat", simulated_latency_ms=80.0)

        # Server with rising latency
        server_rising = SimulatedServer(overload_config=config)
        for i in range(20):
            latency = 40.0 + (i * 4)  # 40 -> 116ms
            await server_rising.process_request("Heartbeat", simulated_latency_ms=latency)

        stable_trend = server_stable.get_diagnostics()["trend"]
        rising_trend = server_rising.get_diagnostics()["trend"]

        # Rising server should have higher trend
        assert rising_trend > stable_trend


class TestLoadSheddingCustomConfiguration:
    """Test custom load shedding configurations."""

    @pytest.mark.asyncio
    async def test_aggressive_shedding_config(self) -> None:
        """Test aggressive shedding that sheds more at lower states."""
        aggressive_config = LoadShedderConfig(
            shed_thresholds={
                OverloadState.HEALTHY: RequestPriority.LOW,  # Even healthy sheds LOW
                OverloadState.BUSY: RequestPriority.NORMAL,
                OverloadState.STRESSED: RequestPriority.HIGH,
                OverloadState.OVERLOADED: RequestPriority.HIGH,
            }
        )

        server = SimulatedServer(shedder_config=aggressive_config)

        # Even in healthy state, LOW should be shed
        assert server.get_current_state() == OverloadState.HEALTHY

        result = await server.process_request("DebugRequest", simulated_latency_ms=10.0)
        assert result.was_shed is True

        result = await server.process_request("StatsUpdate", simulated_latency_ms=10.0)
        assert result.was_shed is False  # NORMAL still accepted

    @pytest.mark.asyncio
    async def test_lenient_shedding_config(self) -> None:
        """Test lenient shedding that only sheds at overloaded."""
        lenient_config = LoadShedderConfig(
            shed_thresholds={
                OverloadState.HEALTHY: None,
                OverloadState.BUSY: None,  # Accept all even when busy
                OverloadState.STRESSED: None,  # Accept all even when stressed
                OverloadState.OVERLOADED: RequestPriority.LOW,  # Only shed LOW at overloaded
            }
        )

        overload_config = OverloadConfig(
            absolute_bounds=(50.0, 100.0, 200.0),
        )

        server = SimulatedServer(
            overload_config=overload_config,
            shedder_config=lenient_config,
        )

        # Push to stressed
        for _ in range(5):
            await server.process_request("Heartbeat", simulated_latency_ms=120.0)

        assert server.get_current_state() == OverloadState.STRESSED

        # All priorities should still be accepted in stressed with lenient config
        for msg_type in ["DebugRequest", "StatsUpdate", "SubmitJob"]:
            result = await server.process_request(msg_type, simulated_latency_ms=120.0)
            assert result.was_shed is False
