"""
Integration tests for Load Shedding (AD-22).

Tests:
- RequestPriority classification
- LoadShedder behavior under different overload states
- Shed thresholds by overload state
- Metrics tracking
"""

import pytest

from hyperscale.distributed_rewrite.reliability import (
    HybridOverloadDetector,
    LoadShedder,
    LoadShedderConfig,
    OverloadConfig,
    OverloadState,
    RequestPriority,
)


class TestRequestPriority:
    """Test RequestPriority enum behavior."""

    def test_priority_ordering(self) -> None:
        """Test that priorities are correctly ordered (lower = higher priority)."""
        assert RequestPriority.CRITICAL < RequestPriority.HIGH
        assert RequestPriority.HIGH < RequestPriority.NORMAL
        assert RequestPriority.NORMAL < RequestPriority.LOW

    def test_priority_values(self) -> None:
        """Test priority numeric values."""
        assert RequestPriority.CRITICAL == 0
        assert RequestPriority.HIGH == 1
        assert RequestPriority.NORMAL == 2
        assert RequestPriority.LOW == 3


class TestLoadShedderClassification:
    """Test message type classification."""

    def test_critical_message_types(self) -> None:
        """Test that critical messages are classified correctly."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        critical_messages = [
            "Ping",
            "Ack",
            "Nack",
            "JobCancelRequest",
            "JobCancelResponse",
            "JobFinalResult",
            "Heartbeat",
            "HealthCheck",
        ]

        for message_type in critical_messages:
            assert shedder.classify_request(message_type) == RequestPriority.CRITICAL, (
                f"{message_type} should be CRITICAL"
            )

    def test_high_priority_message_types(self) -> None:
        """Test that high priority messages are classified correctly."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        high_messages = [
            "SubmitJob",
            "SubmitJobResponse",
            "JobAssignment",
            "WorkflowDispatch",
            "WorkflowComplete",
            "StateSync",
        ]

        for message_type in high_messages:
            assert shedder.classify_request(message_type) == RequestPriority.HIGH, (
                f"{message_type} should be HIGH"
            )

    def test_normal_priority_message_types(self) -> None:
        """Test that normal priority messages are classified correctly."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        normal_messages = [
            "JobProgress",
            "JobStatusRequest",
            "JobStatusResponse",
            "StatsUpdate",
            "RegisterCallback",
        ]

        for message_type in normal_messages:
            assert shedder.classify_request(message_type) == RequestPriority.NORMAL, (
                f"{message_type} should be NORMAL"
            )

    def test_low_priority_message_types(self) -> None:
        """Test that low priority messages are classified correctly."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        low_messages = [
            "DetailedStatsRequest",
            "DetailedStatsResponse",
            "DebugRequest",
            "DiagnosticsRequest",
        ]

        for message_type in low_messages:
            assert shedder.classify_request(message_type) == RequestPriority.LOW, (
                f"{message_type} should be LOW"
            )

    def test_unknown_message_defaults_to_normal(self) -> None:
        """Test that unknown messages default to NORMAL priority."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        assert shedder.classify_request("UnknownMessage") == RequestPriority.NORMAL
        assert shedder.classify_request("CustomRequest") == RequestPriority.NORMAL


class TestLoadShedderBehavior:
    """Test load shedding behavior under different states."""

    def test_healthy_accepts_all(self) -> None:
        """Test that healthy state accepts all requests."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Healthy state (no latencies recorded)
        assert shedder.get_current_state() == OverloadState.HEALTHY

        # All priorities should be accepted
        assert shedder.should_shed("DebugRequest") is False  # LOW
        assert shedder.should_shed("StatsUpdate") is False  # NORMAL
        assert shedder.should_shed("SubmitJob") is False  # HIGH
        assert shedder.should_shed("Heartbeat") is False  # CRITICAL

    def test_busy_sheds_low_only(self) -> None:
        """Test that busy state sheds only LOW priority."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.3, 0.5),  # Lower thresholds
            absolute_bounds=(50.0, 100.0, 200.0),  # Lower bounds
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # Push to busy state by recording increasing latencies
        for latency in [40.0, 55.0, 60.0, 65.0]:
            detector.record_latency(latency)

        # Verify we're in busy state
        state = shedder.get_current_state()
        assert state == OverloadState.BUSY

        # LOW should be shed
        assert shedder.should_shed("DebugRequest") is True

        # Others should be accepted
        assert shedder.should_shed("StatsUpdate") is False  # NORMAL
        assert shedder.should_shed("SubmitJob") is False  # HIGH
        assert shedder.should_shed("Heartbeat") is False  # CRITICAL

    def test_stressed_sheds_normal_and_low(self) -> None:
        """Test that stressed state sheds NORMAL and LOW priority."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.2, 0.5),  # Lower thresholds
            absolute_bounds=(50.0, 100.0, 200.0),  # Lower bounds
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # Push to stressed state with higher latencies
        for latency in [80.0, 105.0, 110.0, 115.0]:
            detector.record_latency(latency)

        state = shedder.get_current_state()
        assert state == OverloadState.STRESSED

        # LOW and NORMAL should be shed
        assert shedder.should_shed("DebugRequest") is True
        assert shedder.should_shed("StatsUpdate") is True

        # HIGH and CRITICAL should be accepted
        assert shedder.should_shed("SubmitJob") is False
        assert shedder.should_shed("Heartbeat") is False

    def test_overloaded_sheds_all_except_critical(self) -> None:
        """Test that overloaded state sheds all except CRITICAL."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.2, 0.3),  # Lower thresholds
            absolute_bounds=(50.0, 100.0, 150.0),  # Lower bounds
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # Push to overloaded state with very high latencies
        for latency in [180.0, 200.0, 220.0, 250.0]:
            detector.record_latency(latency)

        state = shedder.get_current_state()
        assert state == OverloadState.OVERLOADED

        # All except CRITICAL should be shed
        assert shedder.should_shed("DebugRequest") is True
        assert shedder.should_shed("StatsUpdate") is True
        assert shedder.should_shed("SubmitJob") is True

        # CRITICAL should never be shed
        assert shedder.should_shed("Heartbeat") is False
        assert shedder.should_shed("JobCancelRequest") is False

    def test_critical_never_shed_in_any_state(self) -> None:
        """Test that CRITICAL requests are never shed."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.2, 0.3),
            absolute_bounds=(50.0, 100.0, 150.0),
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        critical_messages = ["Ping", "Ack", "JobCancelRequest", "JobFinalResult", "Heartbeat"]

        # Test in healthy state
        for msg in critical_messages:
            assert shedder.should_shed(msg) is False

        # Push to overloaded
        for latency in [180.0, 200.0, 220.0, 250.0]:
            detector.record_latency(latency)

        assert shedder.get_current_state() == OverloadState.OVERLOADED

        # Still never shed critical
        for msg in critical_messages:
            assert shedder.should_shed(msg) is False


class TestLoadShedderWithResourceSignals:
    """Test load shedding with CPU/memory resource signals."""

    def test_cpu_triggers_shedding(self) -> None:
        """Test that high CPU triggers shedding."""
        config = OverloadConfig(
            cpu_stress_threshold=80.0,
            cpu_overload_threshold=95.0,
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # High CPU should trigger stressed state
        assert shedder.should_shed("StatsUpdate", cpu_percent=85.0) is True
        assert shedder.should_shed("SubmitJob", cpu_percent=85.0) is False

        # Very high CPU should trigger overloaded
        assert shedder.should_shed("SubmitJob", cpu_percent=98.0) is True
        assert shedder.should_shed("Heartbeat", cpu_percent=98.0) is False

    def test_memory_triggers_shedding(self) -> None:
        """Test that high memory triggers shedding."""
        config = OverloadConfig(
            memory_stress_threshold=85.0,
            memory_overload_threshold=95.0,
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # High memory should trigger stressed state
        assert shedder.should_shed("StatsUpdate", memory_percent=90.0) is True

        # Very high memory should trigger overloaded
        assert shedder.should_shed("SubmitJob", memory_percent=98.0) is True


class TestLoadShedderMetrics:
    """Test metrics tracking in LoadShedder."""

    def test_metrics_tracking(self) -> None:
        """Test that metrics are correctly tracked."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.2, 0.3),
            absolute_bounds=(50.0, 100.0, 150.0),
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # Process some requests in healthy state
        shedder.should_shed("SubmitJob")
        shedder.should_shed("StatsUpdate")
        shedder.should_shed("DebugRequest")

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 3
        assert metrics["shed_requests"] == 0
        assert metrics["shed_rate"] == 0.0

        # Push to overloaded
        for latency in [180.0, 200.0, 220.0, 250.0]:
            detector.record_latency(latency)

        # Process more requests
        shedder.should_shed("SubmitJob")  # HIGH - shed
        shedder.should_shed("StatsUpdate")  # NORMAL - shed
        shedder.should_shed("DebugRequest")  # LOW - shed
        shedder.should_shed("Heartbeat")  # CRITICAL - not shed

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 7
        assert metrics["shed_requests"] == 3
        assert metrics["shed_rate"] == 3 / 7

    def test_metrics_by_priority(self) -> None:
        """Test that metrics are tracked by priority level."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.2, 0.3),
            absolute_bounds=(50.0, 100.0, 150.0),
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # Push to overloaded
        for latency in [180.0, 200.0, 220.0, 250.0]:
            detector.record_latency(latency)

        # Shed some requests
        shedder.should_shed("SubmitJob")  # HIGH
        shedder.should_shed("StatsUpdate")  # NORMAL
        shedder.should_shed("DebugRequest")  # LOW
        shedder.should_shed("DebugRequest")  # LOW again

        metrics = shedder.get_metrics()
        assert metrics["shed_by_priority"]["HIGH"] == 1
        assert metrics["shed_by_priority"]["NORMAL"] == 1
        assert metrics["shed_by_priority"]["LOW"] == 2
        assert metrics["shed_by_priority"]["CRITICAL"] == 0

    def test_metrics_reset(self) -> None:
        """Test that metrics can be reset."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        shedder.should_shed("SubmitJob")
        shedder.should_shed("StatsUpdate")

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 2

        shedder.reset_metrics()

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 0
        assert metrics["shed_requests"] == 0


class TestLoadShedderCustomConfig:
    """Test custom configuration for LoadShedder."""

    def test_custom_shed_thresholds(self) -> None:
        """Test custom shedding thresholds."""
        # Custom config that sheds NORMAL+ even when busy
        custom_config = LoadShedderConfig(
            shed_thresholds={
                OverloadState.HEALTHY: None,
                OverloadState.BUSY: RequestPriority.NORMAL,  # More aggressive
                OverloadState.STRESSED: RequestPriority.HIGH,
                OverloadState.OVERLOADED: RequestPriority.HIGH,
            }
        )

        overload_config = OverloadConfig(
            delta_thresholds=(0.1, 0.3, 0.5),
            absolute_bounds=(50.0, 100.0, 200.0),
        )
        detector = HybridOverloadDetector(config=overload_config)
        shedder = LoadShedder(detector, config=custom_config)

        # Push to busy state
        for latency in [40.0, 55.0, 60.0, 65.0]:
            detector.record_latency(latency)

        assert shedder.get_current_state() == OverloadState.BUSY

        # With custom config, NORMAL should be shed even in busy state
        assert shedder.should_shed("StatsUpdate") is True  # NORMAL
        assert shedder.should_shed("SubmitJob") is False  # HIGH

    def test_register_custom_message_priority(self) -> None:
        """Test registering custom message type priorities."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Register a custom message type
        shedder.register_message_priority("MyCustomMessage", RequestPriority.CRITICAL)

        assert shedder.classify_request("MyCustomMessage") == RequestPriority.CRITICAL

        # Override an existing message type
        shedder.register_message_priority("DebugRequest", RequestPriority.HIGH)

        assert shedder.classify_request("DebugRequest") == RequestPriority.HIGH


class TestLoadShedderPriorityDirect:
    """Test direct priority-based shedding."""

    def test_should_shed_priority_directly(self) -> None:
        """Test shedding by priority without message classification."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.2, 0.3),
            absolute_bounds=(50.0, 100.0, 150.0),
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # Push to overloaded
        for latency in [180.0, 200.0, 220.0, 250.0]:
            detector.record_latency(latency)

        # Test direct priority shedding
        assert shedder.should_shed_priority(RequestPriority.LOW) is True
        assert shedder.should_shed_priority(RequestPriority.NORMAL) is True
        assert shedder.should_shed_priority(RequestPriority.HIGH) is True
        assert shedder.should_shed_priority(RequestPriority.CRITICAL) is False
