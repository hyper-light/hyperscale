#!/usr/bin/env python
"""
Comprehensive edge case tests for health probes (AD-19).

Tests cover:
- Threshold-based state transitions
- Timeout handling
- Error recovery patterns
- Composite probe behavior
- Periodic check lifecycle
- State reset behavior
- Edge cases in probe checks
- Concurrent probe operations
"""

import asyncio
import time

import pytest

from hyperscale.distributed.health.probes import (
    CompositeProbe,
    HealthProbe,
    LivenessProbe,
    ProbeConfig,
    ProbeResponse,
    ProbeResult,
    ProbeState,
    ReadinessProbe,
    StartupProbe,
)


# =============================================================================
# Test Threshold-Based State Transitions
# =============================================================================


class TestFailureThresholds:
    """Tests for failure threshold state transitions."""

    @pytest.mark.asyncio
    async def test_single_failure_does_not_make_unhealthy(self):
        """One failure doesn't transition to unhealthy."""
        failures = 0

        async def failing_check():
            nonlocal failures
            failures += 1
            return False, "Failed"

        probe = HealthProbe(
            name="test",
            check=failing_check,
            config=ProbeConfig(failure_threshold=3),
        )

        await probe.check()

        assert probe.is_healthy()  # Still healthy after 1 failure
        assert probe.get_state().consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_threshold_failures_makes_unhealthy(self):
        """Exactly threshold failures transitions to unhealthy."""
        async def failing_check():
            return False, "Failed"

        probe = HealthProbe(
            name="test",
            check=failing_check,
            config=ProbeConfig(failure_threshold=3),
        )

        # 2 failures - still healthy
        await probe.check()
        await probe.check()
        assert probe.is_healthy()

        # 3rd failure - unhealthy
        await probe.check()
        assert not probe.is_healthy()

    @pytest.mark.asyncio
    async def test_failures_accumulate_across_checks(self):
        """Consecutive failures accumulate correctly."""
        failure_count = 0

        async def counting_check():
            nonlocal failure_count
            failure_count += 1
            return False, f"Failure {failure_count}"

        probe = HealthProbe(
            name="test",
            check=counting_check,
            config=ProbeConfig(failure_threshold=5),
        )

        for expected in range(1, 6):
            await probe.check()
            assert probe.get_state().consecutive_failures == expected

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self):
        """Success resets consecutive failure count."""
        should_fail = True

        async def toggle_check():
            return not should_fail, "toggled"

        probe = HealthProbe(
            name="test",
            check=toggle_check,
            config=ProbeConfig(failure_threshold=3),
        )

        # 2 failures
        await probe.check()
        await probe.check()
        assert probe.get_state().consecutive_failures == 2

        # 1 success resets
        should_fail = False
        await probe.check()
        assert probe.get_state().consecutive_failures == 0
        assert probe.get_state().consecutive_successes == 1


class TestSuccessThresholds:
    """Tests for success threshold state transitions."""

    @pytest.mark.asyncio
    async def test_single_success_with_threshold_one(self):
        """One success is enough with success_threshold=1."""
        async def passing_check():
            return True, "OK"

        probe = HealthProbe(
            name="test",
            check=passing_check,
            config=ProbeConfig(success_threshold=1),
        )

        # Start unhealthy
        probe._state.healthy = False

        await probe.check()
        assert probe.is_healthy()

    @pytest.mark.asyncio
    async def test_multiple_successes_needed_for_recovery(self):
        """Multiple successes needed when success_threshold > 1."""
        async def passing_check():
            return True, "OK"

        probe = HealthProbe(
            name="test",
            check=passing_check,
            config=ProbeConfig(success_threshold=3),
        )

        # Start unhealthy
        probe._state.healthy = False

        # 2 successes - still unhealthy
        await probe.check()
        await probe.check()
        assert not probe.is_healthy()

        # 3rd success - now healthy
        await probe.check()
        assert probe.is_healthy()

    @pytest.mark.asyncio
    async def test_failure_resets_success_count(self):
        """Failure resets consecutive success count."""
        should_pass = True

        async def toggle_check():
            return should_pass, "toggled"

        probe = HealthProbe(
            name="test",
            check=toggle_check,
            config=ProbeConfig(success_threshold=3),
        )

        # Start unhealthy
        probe._state.healthy = False

        # 2 successes
        await probe.check()
        await probe.check()
        assert probe.get_state().consecutive_successes == 2

        # 1 failure resets
        should_pass = False
        await probe.check()
        assert probe.get_state().consecutive_successes == 0
        assert probe.get_state().consecutive_failures == 1


class TestStateTransitionEdgeCases:
    """Tests for edge cases in state transitions."""

    @pytest.mark.asyncio
    async def test_alternating_success_failure(self):
        """Alternating results never reach threshold."""
        call_count = 0

        async def alternating_check():
            nonlocal call_count
            call_count += 1
            return call_count % 2 == 1, f"Call {call_count}"

        probe = HealthProbe(
            name="test",
            check=alternating_check,
            config=ProbeConfig(
                failure_threshold=3,
                success_threshold=3,
            ),
        )

        # Start unhealthy
        probe._state.healthy = False

        # 10 alternating checks
        for _ in range(10):
            await probe.check()

        # Never accumulates enough of either
        assert probe.get_state().consecutive_successes <= 1
        assert probe.get_state().consecutive_failures <= 1
        assert not probe.is_healthy()  # Started unhealthy, never recovered

    @pytest.mark.asyncio
    async def test_starts_healthy_by_default(self):
        """Probes start in healthy state."""
        async def check():
            return True, "OK"

        probe = HealthProbe(name="test", check=check)

        assert probe.is_healthy()
        assert probe.get_state().healthy

    @pytest.mark.asyncio
    async def test_threshold_of_one(self):
        """Threshold of 1 means immediate state transition."""
        async def failing_check():
            return False, "Failed"

        probe = HealthProbe(
            name="test",
            check=failing_check,
            config=ProbeConfig(failure_threshold=1),
        )

        assert probe.is_healthy()

        await probe.check()

        assert not probe.is_healthy()


# =============================================================================
# Test Timeout Handling
# =============================================================================


class TestTimeoutHandling:
    """Tests for probe timeout behavior."""

    @pytest.mark.asyncio
    async def test_slow_check_times_out(self):
        """Check that exceeds timeout is treated as failure."""
        async def slow_check():
            await asyncio.sleep(5.0)
            return True, "Should not reach"

        probe = HealthProbe(
            name="test",
            check=slow_check,
            config=ProbeConfig(timeout_seconds=0.1),
        )

        response = await probe.check()

        assert response.result == ProbeResult.TIMEOUT
        assert "timed out" in response.message.lower()
        assert probe.get_state().consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_timeout_counts_as_failure(self):
        """Timeout contributes to failure threshold."""
        async def slow_check():
            await asyncio.sleep(1.0)
            return True, "Never reached"

        probe = HealthProbe(
            name="test",
            check=slow_check,
            config=ProbeConfig(
                timeout_seconds=0.01,
                failure_threshold=2,
            ),
        )

        # 2 timeouts = 2 failures = unhealthy
        await probe.check()
        assert probe.is_healthy()  # 1 failure

        await probe.check()
        assert not probe.is_healthy()  # 2 failures

    @pytest.mark.asyncio
    async def test_timeout_latency_recorded(self):
        """Timeout records actual latency (approximately timeout value)."""
        async def slow_check():
            await asyncio.sleep(10.0)
            return True, "Never reached"

        probe = HealthProbe(
            name="test",
            check=slow_check,
            config=ProbeConfig(timeout_seconds=0.1),
        )

        response = await probe.check()

        # Latency should be approximately the timeout
        assert 90 <= response.latency_ms <= 200  # Allow some tolerance

    @pytest.mark.asyncio
    async def test_fast_check_within_timeout(self):
        """Fast check completes before timeout."""
        async def fast_check():
            return True, "Fast"

        probe = HealthProbe(
            name="test",
            check=fast_check,
            config=ProbeConfig(timeout_seconds=10.0),
        )

        response = await probe.check()

        assert response.result == ProbeResult.SUCCESS
        assert response.latency_ms < 100  # Should be very fast


# =============================================================================
# Test Error Handling
# =============================================================================


class TestErrorHandling:
    """Tests for probe error handling."""

    @pytest.mark.asyncio
    async def test_exception_in_check_is_failure(self):
        """Exception in check function is treated as failure."""
        async def error_check():
            raise ValueError("Something went wrong")

        probe = HealthProbe(
            name="test",
            check=error_check,
            config=ProbeConfig(failure_threshold=2),
        )

        response = await probe.check()

        assert response.result == ProbeResult.ERROR
        assert "Something went wrong" in response.message
        assert probe.get_state().consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_various_exception_types(self):
        """Different exception types are all handled."""
        exceptions = [
            RuntimeError("Runtime error"),
            ConnectionError("Connection failed"),
            OSError("OS error"),
            KeyError("Missing key"),
        ]

        for exc in exceptions:
            async def check():
                raise exc

            probe = HealthProbe(name="test", check=check)
            response = await probe.check()

            assert response.result == ProbeResult.ERROR
            assert str(exc) in response.message or type(exc).__name__ in response.message

    @pytest.mark.asyncio
    async def test_error_counts_toward_failure_threshold(self):
        """Errors contribute to failure threshold."""
        async def error_check():
            raise RuntimeError("Error")

        probe = HealthProbe(
            name="test",
            check=error_check,
            config=ProbeConfig(failure_threshold=3),
        )

        await probe.check()
        await probe.check()
        assert probe.is_healthy()  # 2 errors

        await probe.check()
        assert not probe.is_healthy()  # 3 errors = unhealthy

    @pytest.mark.asyncio
    async def test_recovery_after_errors(self):
        """Can recover to healthy after error failures."""
        should_error = True

        async def maybe_error():
            if should_error:
                raise RuntimeError("Error")
            return True, "OK"

        probe = HealthProbe(
            name="test",
            check=maybe_error,
            config=ProbeConfig(
                failure_threshold=1,
                success_threshold=1,
            ),
        )

        # Error makes unhealthy
        await probe.check()
        assert not probe.is_healthy()

        # Success recovers
        should_error = False
        await probe.check()
        assert probe.is_healthy()


# =============================================================================
# Test Composite Probe
# =============================================================================


class TestCompositeProbe:
    """Tests for CompositeProbe behavior."""

    @pytest.mark.asyncio
    async def test_all_healthy_means_composite_healthy(self):
        """Composite is healthy only if all probes healthy."""
        async def pass_check():
            return True, "OK"

        probe1 = HealthProbe(name="probe1", check=pass_check)
        probe2 = HealthProbe(name="probe2", check=pass_check)
        probe3 = HealthProbe(name="probe3", check=pass_check)

        composite = CompositeProbe(name="composite")
        composite.add_probe(probe1)
        composite.add_probe(probe2)
        composite.add_probe(probe3)

        # Run all checks
        await composite.check_all()

        assert composite.is_healthy()

    @pytest.mark.asyncio
    async def test_one_unhealthy_makes_composite_unhealthy(self):
        """One unhealthy probe makes composite unhealthy."""
        async def pass_check():
            return True, "OK"

        async def fail_check():
            return False, "Failed"

        probe1 = HealthProbe(
            name="probe1",
            check=pass_check,
        )
        probe2 = HealthProbe(
            name="probe2",
            check=fail_check,
            config=ProbeConfig(failure_threshold=1),
        )
        probe3 = HealthProbe(
            name="probe3",
            check=pass_check,
        )

        composite = CompositeProbe(name="composite")
        composite.add_probe(probe1)
        composite.add_probe(probe2)
        composite.add_probe(probe3)

        # Run all checks
        await composite.check_all()

        assert not composite.is_healthy()

    @pytest.mark.asyncio
    async def test_get_unhealthy_probes(self):
        """get_unhealthy_probes() returns correct names."""
        async def pass_check():
            return True, "OK"

        async def fail_check():
            return False, "Failed"

        probe1 = HealthProbe(name="healthy-1", check=pass_check)
        probe2 = HealthProbe(
            name="unhealthy-1",
            check=fail_check,
            config=ProbeConfig(failure_threshold=1),
        )
        probe3 = HealthProbe(
            name="unhealthy-2",
            check=fail_check,
            config=ProbeConfig(failure_threshold=1),
        )

        composite = CompositeProbe()
        composite.add_probe(probe1)
        composite.add_probe(probe2)
        composite.add_probe(probe3)

        await composite.check_all()

        unhealthy = composite.get_unhealthy_probes()
        assert len(unhealthy) == 2
        assert "unhealthy-1" in unhealthy
        assert "unhealthy-2" in unhealthy
        assert "healthy-1" not in unhealthy

    @pytest.mark.asyncio
    async def test_remove_probe(self):
        """Can remove probes by name."""
        async def check():
            return True, "OK"

        probe1 = HealthProbe(name="probe1", check=check)
        probe2 = HealthProbe(name="probe2", check=check)

        composite = CompositeProbe()
        composite.add_probe(probe1)
        composite.add_probe(probe2)

        removed = composite.remove_probe("probe1")
        assert removed is probe1

        # probe2 still there
        status = composite.get_status()
        assert "probe2" in status["probes"]
        assert "probe1" not in status["probes"]

    def test_remove_nonexistent_probe(self):
        """Removing nonexistent probe returns None."""
        composite = CompositeProbe()

        result = composite.remove_probe("does-not-exist")
        assert result is None

    @pytest.mark.asyncio
    async def test_empty_composite_is_healthy(self):
        """Empty composite is considered healthy."""
        composite = CompositeProbe()
        assert composite.is_healthy()

    @pytest.mark.asyncio
    async def test_check_all_returns_all_responses(self):
        """check_all() returns response for each probe."""
        async def check1():
            return True, "Check 1 OK"

        async def check2():
            return False, "Check 2 failed"

        probe1 = HealthProbe(name="check1", check=check1)
        probe2 = HealthProbe(name="check2", check=check2)

        composite = CompositeProbe()
        composite.add_probe(probe1)
        composite.add_probe(probe2)

        results = await composite.check_all()

        assert len(results) == 2
        assert results["check1"].result == ProbeResult.SUCCESS
        assert results["check2"].result == ProbeResult.FAILURE


# =============================================================================
# Test Periodic Check Lifecycle
# =============================================================================


class TestPeriodicChecks:
    """Tests for periodic check behavior."""

    @pytest.mark.asyncio
    async def test_start_periodic_runs_checks(self):
        """Periodic checks run at configured interval."""
        check_count = 0

        async def counting_check():
            nonlocal check_count
            check_count += 1
            return True, f"Check {check_count}"

        probe = HealthProbe(
            name="test",
            check=counting_check,
            config=ProbeConfig(period_seconds=0.05),
        )

        await probe.start_periodic()

        # Wait for a few checks
        await asyncio.sleep(0.2)

        await probe.stop_periodic()

        # Should have run multiple times
        assert check_count >= 3

    @pytest.mark.asyncio
    async def test_stop_periodic_stops_checks(self):
        """stop_periodic() stops further checks."""
        check_count = 0

        async def counting_check():
            nonlocal check_count
            check_count += 1
            return True, f"Check {check_count}"

        probe = HealthProbe(
            name="test",
            check=counting_check,
            config=ProbeConfig(period_seconds=0.05),
        )

        await probe.start_periodic()
        await asyncio.sleep(0.1)
        await probe.stop_periodic()

        count_after_stop = check_count

        # Wait more time
        await asyncio.sleep(0.1)

        # Count should not have increased
        assert check_count == count_after_stop

    @pytest.mark.asyncio
    async def test_initial_delay(self):
        """initial_delay_seconds delays first check."""
        check_count = 0
        first_check_time = None

        async def counting_check():
            nonlocal check_count, first_check_time
            if first_check_time is None:
                first_check_time = asyncio.get_event_loop().time()
            check_count += 1
            return True, "OK"

        probe = HealthProbe(
            name="test",
            check=counting_check,
            config=ProbeConfig(
                period_seconds=0.05,
                initial_delay_seconds=0.15,
            ),
        )

        start_time = asyncio.get_event_loop().time()

        # start_periodic awaits the initial delay before starting the task
        await probe.start_periodic()

        # Wait for first check to happen
        await asyncio.sleep(0.1)

        await probe.stop_periodic()

        # Verify that the first check happened after the initial delay
        assert first_check_time is not None
        assert first_check_time >= start_time + 0.14  # Allow small tolerance

    @pytest.mark.asyncio
    async def test_start_periodic_idempotent(self):
        """Calling start_periodic twice is safe."""
        check_count = 0

        async def counting_check():
            nonlocal check_count
            check_count += 1
            return True, "OK"

        probe = HealthProbe(
            name="test",
            check=counting_check,
            config=ProbeConfig(period_seconds=0.05),
        )

        await probe.start_periodic()
        await probe.start_periodic()  # Second call should be no-op

        await asyncio.sleep(0.15)
        await probe.stop_periodic()

        # Should only have one check loop running
        # Check count should be reasonable (not doubled)
        assert check_count < 10

    @pytest.mark.asyncio
    async def test_composite_start_stop_all(self):
        """Composite can start/stop all probes."""
        check_counts = {"a": 0, "b": 0}

        async def check_a():
            check_counts["a"] += 1
            return True, "A"

        async def check_b():
            check_counts["b"] += 1
            return True, "B"

        probe_a = HealthProbe(
            name="a",
            check=check_a,
            config=ProbeConfig(period_seconds=0.05),
        )
        probe_b = HealthProbe(
            name="b",
            check=check_b,
            config=ProbeConfig(period_seconds=0.05),
        )

        composite = CompositeProbe()
        composite.add_probe(probe_a)
        composite.add_probe(probe_b)

        await composite.start_all()
        await asyncio.sleep(0.15)
        await composite.stop_all()

        # Both should have run
        assert check_counts["a"] >= 2
        assert check_counts["b"] >= 2


# =============================================================================
# Test State Reset
# =============================================================================


class TestStateReset:
    """Tests for probe state reset."""

    @pytest.mark.asyncio
    async def test_reset_clears_failures(self):
        """reset() clears consecutive failures."""
        async def fail_check():
            return False, "Failed"

        probe = HealthProbe(
            name="test",
            check=fail_check,
            config=ProbeConfig(failure_threshold=5),
        )

        await probe.check()
        await probe.check()
        assert probe.get_state().consecutive_failures == 2

        probe.reset()

        assert probe.get_state().consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_reset_clears_successes(self):
        """reset() clears consecutive successes."""
        async def pass_check():
            return True, "OK"

        probe = HealthProbe(name="test", check=pass_check)

        await probe.check()
        await probe.check()
        assert probe.get_state().consecutive_successes == 2

        probe.reset()

        assert probe.get_state().consecutive_successes == 0

    @pytest.mark.asyncio
    async def test_reset_restores_healthy(self):
        """reset() restores healthy state."""
        async def fail_check():
            return False, "Failed"

        probe = HealthProbe(
            name="test",
            check=fail_check,
            config=ProbeConfig(failure_threshold=1),
        )

        await probe.check()
        assert not probe.is_healthy()

        probe.reset()

        assert probe.is_healthy()

    def test_reset_clears_totals(self):
        """reset() creates fresh state with zero totals."""
        probe = HealthProbe(
            name="test",
            check=lambda: (True, "OK"),
        )

        # Manually set some state
        probe._state.total_checks = 100
        probe._state.total_failures = 50

        probe.reset()

        assert probe.get_state().total_checks == 0
        assert probe.get_state().total_failures == 0


# =============================================================================
# Test Probe Types
# =============================================================================


class TestLivenessProbe:
    """Tests for LivenessProbe specifics."""

    @pytest.mark.asyncio
    async def test_default_liveness_always_passes(self):
        """Default liveness probe always passes."""
        probe = LivenessProbe()

        response = await probe.check()

        assert response.result == ProbeResult.SUCCESS
        assert "alive" in response.message.lower()

    def test_liveness_default_config(self):
        """Liveness probe has appropriate defaults."""
        probe = LivenessProbe()

        # Should have quick timeout
        assert probe._config.timeout_seconds == 1.0
        assert probe._config.failure_threshold == 3
        assert probe._config.success_threshold == 1

    @pytest.mark.asyncio
    async def test_custom_liveness_check(self):
        """Can provide custom liveness check."""
        async def custom_check():
            return True, "Custom alive check"

        probe = LivenessProbe(check=custom_check)
        response = await probe.check()

        assert "Custom alive check" in response.message


class TestReadinessProbe:
    """Tests for ReadinessProbe specifics."""

    @pytest.mark.asyncio
    async def test_default_readiness_passes(self):
        """Default readiness probe passes."""
        probe = ReadinessProbe()

        response = await probe.check()

        assert response.result == ProbeResult.SUCCESS
        assert "ready" in response.message.lower()

    def test_readiness_has_longer_timeout(self):
        """Readiness probe allows longer timeout than liveness."""
        readiness = ReadinessProbe()
        liveness = LivenessProbe()

        assert readiness._config.timeout_seconds >= liveness._config.timeout_seconds


class TestStartupProbe:
    """Tests for StartupProbe specifics."""

    @pytest.mark.asyncio
    async def test_default_startup_passes(self):
        """Default startup probe passes."""
        probe = StartupProbe()

        response = await probe.check()

        assert response.result == ProbeResult.SUCCESS

    def test_startup_has_high_failure_threshold(self):
        """Startup probe allows many failures (for slow startup)."""
        probe = StartupProbe()

        # Startup should allow many failures
        assert probe._config.failure_threshold >= 10


# =============================================================================
# Test Response Details
# =============================================================================


class TestProbeResponseDetails:
    """Tests for ProbeResponse detail tracking."""

    @pytest.mark.asyncio
    async def test_latency_recorded(self):
        """Latency is recorded in response."""
        async def slow_check():
            await asyncio.sleep(0.05)
            return True, "Slow"

        probe = HealthProbe(name="test", check=slow_check)
        response = await probe.check()

        assert response.latency_ms >= 45  # Should be ~50ms

    @pytest.mark.asyncio
    async def test_timestamp_recorded(self):
        """Timestamp is recorded in response."""
        async def check():
            return True, "OK"

        probe = HealthProbe(name="test", check=check)

        before = time.monotonic()
        response = await probe.check()
        after = time.monotonic()

        assert before <= response.timestamp <= after

    @pytest.mark.asyncio
    async def test_total_checks_incremented(self):
        """total_checks is incremented on each check."""
        async def check():
            return True, "OK"

        probe = HealthProbe(name="test", check=check)

        for expected in range(1, 6):
            await probe.check()
            assert probe.get_state().total_checks == expected

    @pytest.mark.asyncio
    async def test_total_failures_incremented(self):
        """total_failures is incremented on failures."""
        async def fail_check():
            return False, "Failed"

        probe = HealthProbe(name="test", check=fail_check)

        for expected in range(1, 6):
            await probe.check()
            assert probe.get_state().total_failures == expected

    @pytest.mark.asyncio
    async def test_success_rate_calculation(self):
        """Can calculate success rate from state."""
        should_pass = True

        async def toggle_check():
            return should_pass, "toggled"

        probe = HealthProbe(name="test", check=toggle_check)

        # 7 successes
        for _ in range(7):
            await probe.check()

        # 3 failures
        should_pass = False
        for _ in range(3):
            await probe.check()

        state = probe.get_state()
        success_count = state.total_checks - state.total_failures
        success_rate = success_count / state.total_checks

        assert success_rate == 0.7


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestProbeEdgeCases:
    """Tests for additional edge cases."""

    @pytest.mark.asyncio
    async def test_check_returning_wrong_type(self):
        """Check returning wrong type is handled."""
        async def bad_check():
            return "not a tuple"  # type: ignore

        probe = HealthProbe(name="test", check=bad_check)

        # Should handle gracefully (as error)
        response = await probe.check()
        assert response.result == ProbeResult.ERROR

    @pytest.mark.asyncio
    async def test_very_high_thresholds(self):
        """High thresholds work correctly."""
        async def fail_check():
            return False, "Failed"

        probe = HealthProbe(
            name="test",
            check=fail_check,
            config=ProbeConfig(failure_threshold=1000),
        )

        # 999 failures - still healthy
        for _ in range(999):
            await probe.check()

        assert probe.is_healthy()

        # 1000th failure - unhealthy
        await probe.check()
        assert not probe.is_healthy()

    @pytest.mark.asyncio
    async def test_zero_timeout(self):
        """Zero timeout immediately times out."""
        async def check():
            return True, "OK"

        probe = HealthProbe(
            name="test",
            check=check,
            config=ProbeConfig(timeout_seconds=0.0),
        )

        response = await probe.check()

        # Zero timeout should cause immediate timeout
        assert response.result == ProbeResult.TIMEOUT

    @pytest.mark.asyncio
    async def test_check_message_preserved(self):
        """Check message is preserved in state."""
        async def message_check():
            return True, "Detailed status message"

        probe = HealthProbe(name="test", check=message_check)
        await probe.check()

        assert probe.get_state().last_message == "Detailed status message"

    @pytest.mark.asyncio
    async def test_last_result_tracked(self):
        """last_result tracks the most recent result."""
        should_pass = True

        async def toggle_check():
            return should_pass, "toggled"

        probe = HealthProbe(name="test", check=toggle_check)

        await probe.check()
        assert probe.get_state().last_result == ProbeResult.SUCCESS

        should_pass = False
        await probe.check()
        assert probe.get_state().last_result == ProbeResult.FAILURE

    @pytest.mark.asyncio
    async def test_concurrent_checks_safe(self):
        """Multiple concurrent checks don't corrupt state."""
        check_count = 0

        async def counting_check():
            nonlocal check_count
            check_count += 1
            await asyncio.sleep(0.01)
            return True, f"Check {check_count}"

        probe = HealthProbe(name="test", check=counting_check)

        # Run 10 concurrent checks
        await asyncio.gather(*[probe.check() for _ in range(10)])

        # All checks should have run
        assert check_count == 10
        assert probe.get_state().total_checks == 10

    def test_probe_name_preserved(self):
        """Probe name is accessible."""
        async def check():
            return True, "OK"

        probe = HealthProbe(name="my-custom-probe", check=check)
        assert probe.name == "my-custom-probe"

    @pytest.mark.asyncio
    async def test_composite_get_status(self):
        """get_status() returns comprehensive status."""
        async def pass_check():
            return True, "OK"

        async def fail_check():
            return False, "Failed"

        probe1 = HealthProbe(name="healthy", check=pass_check)
        probe2 = HealthProbe(
            name="unhealthy",
            check=fail_check,
            config=ProbeConfig(failure_threshold=1),
        )

        composite = CompositeProbe(name="test-composite")
        composite.add_probe(probe1)
        composite.add_probe(probe2)

        await composite.check_all()

        status = composite.get_status()

        assert status["name"] == "test-composite"
        assert status["healthy"] is False
        assert "healthy" in status["probes"]
        assert "unhealthy" in status["probes"]
        assert status["probes"]["healthy"]["healthy"] is True
        assert status["probes"]["unhealthy"]["healthy"] is False
