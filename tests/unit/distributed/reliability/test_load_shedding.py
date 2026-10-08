"""
Integration tests for Load Shedding (AD-22).

Tests:
- RequestPriority classification
- LoadShedder behavior under different overload states
- Shed thresholds by overload state
- Metrics tracking
"""

from hyperscale.distributed.reliability import (
    CONTROL_HANDLERS,
    DATA_HANDLERS,
    DISPATCH_HANDLERS,
    TELEMETRY_HANDLERS,
    HybridOverloadDetector,
    LoadShedder,
    LoadShedderConfig,
    OverloadConfig,
    OverloadState,
    RequestPriority,
    classify_handler_to_priority,
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
    """Test AD-37 handler name classification."""

    def test_control_handlers_are_critical(self) -> None:
        """Test that every CONTROL handler is classified CRITICAL."""
        for handler_name in sorted(CONTROL_HANDLERS):
            assert classify_handler_to_priority(handler_name) == RequestPriority.CRITICAL, (
                f"{handler_name} should be CRITICAL"
            )

    def test_dispatch_handlers_are_high(self) -> None:
        """Test that every DISPATCH handler is classified HIGH."""
        for handler_name in sorted(DISPATCH_HANDLERS):
            assert classify_handler_to_priority(handler_name) == RequestPriority.HIGH, (
                f"{handler_name} should be HIGH"
            )

    def test_data_handlers_are_normal(self) -> None:
        """Test that every DATA handler is classified NORMAL."""
        for handler_name in sorted(DATA_HANDLERS):
            assert classify_handler_to_priority(handler_name) == RequestPriority.NORMAL, (
                f"{handler_name} should be NORMAL"
            )

    def test_telemetry_handlers_are_low(self) -> None:
        """Test that every TELEMETRY handler is classified LOW."""
        for handler_name in sorted(TELEMETRY_HANDLERS):
            assert classify_handler_to_priority(handler_name) == RequestPriority.LOW, (
                f"{handler_name} should be LOW"
            )

    def test_unknown_handler_defaults_to_normal(self) -> None:
        """Test that unknown handler names default to NORMAL priority."""
        assert classify_handler_to_priority("not_a_real_handler") == RequestPriority.NORMAL


class TestLoadShedderBehavior:
    """Test load shedding behavior under different states."""

    def test_healthy_accepts_all(self) -> None:
        """Test that healthy state accepts all requests."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Healthy state (no latencies recorded)
        assert shedder.get_current_state() == OverloadState.HEALTHY

        # All priorities should be accepted
        assert shedder.should_shed_handler("cluster_metrics") is False  # LOW
        assert shedder.should_shed_handler("workflow_progress") is False  # NORMAL
        assert shedder.should_shed_handler("job_submission") is False  # HIGH
        assert shedder.should_shed_handler("raft_append_entries") is False  # CRITICAL

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
        assert shedder.should_shed_handler("cluster_metrics") is True

        # Others should be accepted
        assert shedder.should_shed_handler("workflow_progress") is False  # NORMAL
        assert shedder.should_shed_handler("job_submission") is False  # HIGH
        assert shedder.should_shed_handler("raft_append_entries") is False  # CRITICAL

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
        assert shedder.should_shed_handler("cluster_metrics") is True
        assert shedder.should_shed_handler("workflow_progress") is True

        # HIGH and CRITICAL should be accepted
        assert shedder.should_shed_handler("job_submission") is False
        assert shedder.should_shed_handler("raft_append_entries") is False

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
        assert shedder.should_shed_handler("cluster_metrics") is True
        assert shedder.should_shed_handler("workflow_progress") is True
        assert shedder.should_shed_handler("job_submission") is True

        # CRITICAL should never be shed
        assert shedder.should_shed_handler("raft_append_entries") is False
        assert shedder.should_shed_handler("cancel_job") is False

    def test_critical_never_shed_in_any_state(self) -> None:
        """Test that CRITICAL requests are never shed."""
        config = OverloadConfig(
            delta_thresholds=(0.1, 0.2, 0.3),
            absolute_bounds=(50.0, 100.0, 150.0),
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        critical_handler_names = sorted(CONTROL_HANDLERS)

        # Test in healthy state
        for handler_name in critical_handler_names:
            assert shedder.should_shed_handler(handler_name) is False

        # Push to overloaded
        for latency in [180.0, 200.0, 220.0, 250.0]:
            detector.record_latency(latency)

        assert shedder.get_current_state() == OverloadState.OVERLOADED

        # Still never shed critical
        for handler_name in critical_handler_names:
            assert shedder.should_shed_handler(handler_name) is False


class TestLoadShedderWithResourceSignals:
    """Test load shedding with CPU/memory resource signals."""

    def test_cpu_triggers_shedding(self) -> None:
        """Test that high CPU triggers shedding."""
        # cpu_thresholds: (busy, stressed, overloaded) as 0-1 range
        config = OverloadConfig(
            cpu_thresholds=(0.70, 0.80, 0.95),
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # High CPU (85%) should trigger stressed state (>80% threshold)
        assert shedder.should_shed_handler("workflow_progress", cpu_percent=85.0) is True
        assert shedder.should_shed_handler("job_submission", cpu_percent=85.0) is False

        # Very high CPU (98%) should trigger overloaded (>95% threshold)
        assert shedder.should_shed_handler("job_submission", cpu_percent=98.0) is True
        assert shedder.should_shed_handler("raft_append_entries", cpu_percent=98.0) is False

    def test_memory_triggers_shedding(self) -> None:
        """Test that high memory triggers shedding."""
        # memory_thresholds: (busy, stressed, overloaded) as 0-1 range
        config = OverloadConfig(
            memory_thresholds=(0.70, 0.85, 0.95),
        )
        detector = HybridOverloadDetector(config=config)
        shedder = LoadShedder(detector)

        # High memory (90%) should trigger stressed state (>85% threshold)
        assert shedder.should_shed_handler("workflow_progress", memory_percent=90.0) is True

        # Very high memory (98%) should trigger overloaded (>95% threshold)
        assert shedder.should_shed_handler("job_submission", memory_percent=98.0) is True


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
        shedder.should_shed_handler("job_submission")
        shedder.should_shed_handler("workflow_progress")
        shedder.should_shed_handler("cluster_metrics")

        metrics = shedder.get_metrics()
        assert metrics["total_requests"] == 3
        assert metrics["shed_requests"] == 0
        assert metrics["shed_rate"] == 0.0

        # Push to overloaded
        for latency in [180.0, 200.0, 220.0, 250.0]:
            detector.record_latency(latency)

        # Process more requests
        shedder.should_shed_handler("job_submission")  # HIGH - shed
        shedder.should_shed_handler("workflow_progress")  # NORMAL - shed
        shedder.should_shed_handler("cluster_metrics")  # LOW - shed
        shedder.should_shed_handler("raft_append_entries")  # CRITICAL - not shed

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
        shedder.should_shed_handler("job_submission")  # HIGH
        shedder.should_shed_handler("workflow_progress")  # NORMAL
        shedder.should_shed_handler("cluster_metrics")  # LOW
        shedder.should_shed_handler("cluster_metrics")  # LOW again

        metrics = shedder.get_metrics()
        assert metrics["shed_by_priority"]["HIGH"] == 1
        assert metrics["shed_by_priority"]["NORMAL"] == 1
        assert metrics["shed_by_priority"]["LOW"] == 2
        assert metrics["shed_by_priority"]["CRITICAL"] == 0

    def test_metrics_reset(self) -> None:
        """Test that metrics can be reset."""
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        shedder.should_shed_handler("job_submission")
        shedder.should_shed_handler("workflow_progress")

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
        assert shedder.should_shed_handler("workflow_progress") is True  # NORMAL
        assert shedder.should_shed_handler("job_submission") is False  # HIGH


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
