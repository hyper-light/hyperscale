"""
Failure path tests for Health Probes (AD-19).

Tests failure scenarios and edge cases:
- Check function exceptions and error handling
- Timeout edge cases and recovery
- Threshold boundary conditions
- Concurrent probe operations
- Resource cleanup and state management
- Recovery from degraded states
- State corruption prevention
"""

import asyncio
import pytest
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.health import (
    HealthProbe,
    LivenessProbe,
    ReadinessProbe,
    StartupProbe,
    CompositeProbe,
    ProbeConfig,
    ProbeResult,
)


class TestProbeExceptionHandling:
    """Test exception handling in probe checks."""

    @pytest.mark.asyncio
    async def test_check_raises_runtime_error(self) -> None:
        """Test handling of RuntimeError in check function."""
        async def failing_check() -> tuple[bool, str]:
            raise RuntimeError("Simulated runtime error")

        probe = HealthProbe(
            name="runtime_error",
            check=failing_check,
            config=ProbeConfig(failure_threshold=1),
        )

        response = await probe.check()

        assert response.result == ProbeResult.ERROR
        assert "RuntimeError" in response.message or "runtime error" in response.message.lower()
        assert probe.is_healthy() is False

    @pytest.mark.asyncio
    async def test_check_raises_value_error(self) -> None:
        """Test handling of ValueError in check function."""
        async def failing_check() -> tuple[bool, str]:
            raise ValueError("Invalid value")

        probe = HealthProbe(
            name="value_error",
            check=failing_check,
            config=ProbeConfig(failure_threshold=1),
        )

        response = await probe.check()

        assert response.result == ProbeResult.ERROR
        assert probe.is_healthy() is False

    @pytest.mark.asyncio
    async def test_check_raises_asyncio_cancelled(self) -> None:
        """Test handling of asyncio.CancelledError in check function."""
        async def cancelled_check() -> tuple[bool, str]:
            raise asyncio.CancelledError()

        probe = HealthProbe(
            name="cancelled",
            check=cancelled_check,
            config=ProbeConfig(failure_threshold=1),
        )

        # CancelledError should propagate as it's special in asyncio
        with pytest.raises(asyncio.CancelledError):
            await probe.check()

    @pytest.mark.asyncio
    async def test_check_raises_keyboard_interrupt(self) -> None:
        """Test handling of KeyboardInterrupt in check function."""
        async def interrupt_check() -> tuple[bool, str]:
            raise KeyboardInterrupt()

        probe = HealthProbe(
            name="interrupt",
            check=interrupt_check,
            config=ProbeConfig(failure_threshold=1),
        )

        # KeyboardInterrupt should propagate
        with pytest.raises(KeyboardInterrupt):
            await probe.check()

    @pytest.mark.asyncio
    async def test_check_raises_memory_error(self) -> None:
        """Test handling of MemoryError in check function."""
        async def memory_check() -> tuple[bool, str]:
            raise MemoryError("Out of memory")

        probe = HealthProbe(
            name="memory",
            check=memory_check,
            config=ProbeConfig(failure_threshold=1),
        )

        response = await probe.check()

        # MemoryError should be caught and reported as ERROR
        assert response.result == ProbeResult.ERROR
        assert probe.is_healthy() is False

    @pytest.mark.asyncio
    async def test_check_returns_none_value(self) -> None:
        """Test handling of check returning unexpected None."""
        async def none_check() -> tuple[bool, str]:
            return None  # type: ignore

        probe = HealthProbe(
            name="none_return",
            check=none_check,
            config=ProbeConfig(failure_threshold=1),
        )

        # Should handle gracefully (implementation dependent)
        try:
            response = await probe.check()
            # If it handles it, should be ERROR or FAILURE
            assert response.result in (ProbeResult.ERROR, ProbeResult.FAILURE)
        except (TypeError, AttributeError):
            # Also acceptable if it raises on invalid return
            pass


class TestTimeoutEdgeCases:
    """Test timeout edge cases."""

    @pytest.mark.asyncio
    async def test_check_exactly_at_timeout(self) -> None:
        """Test check that completes exactly at timeout boundary."""
        async def edge_timeout_check() -> tuple[bool, str]:
            await asyncio.sleep(0.09)  # Just under 0.1s timeout
            return True, "Just in time"

        probe = HealthProbe(
            name="edge_timeout",
            check=edge_timeout_check,
            config=ProbeConfig(timeout_seconds=0.1),
        )

        response = await probe.check()
        # Should succeed since it's just under timeout
        assert response.result == ProbeResult.SUCCESS

    @pytest.mark.asyncio
    async def test_check_slightly_over_timeout(self) -> None:
        """Test check that completes slightly over timeout."""
        async def over_timeout_check() -> tuple[bool, str]:
            await asyncio.sleep(0.15)  # Over 0.1s timeout
            return True, "Too late"

        probe = HealthProbe(
            name="over_timeout",
            check=over_timeout_check,
            config=ProbeConfig(timeout_seconds=0.1, failure_threshold=1),
        )

        response = await probe.check()
        assert response.result == ProbeResult.TIMEOUT
        assert probe.is_healthy() is False

    @pytest.mark.asyncio
    async def test_zero_timeout(self) -> None:
        """Test probe with zero timeout."""
        async def instant_check() -> tuple[bool, str]:
            return True, "Instant"

        # Zero timeout should be handled gracefully or use default
        probe = HealthProbe(
            name="zero_timeout",
            check=instant_check,
            config=ProbeConfig(timeout_seconds=0.0),
        )

        # Should either use default timeout or handle 0 gracefully
        try:
            response = await probe.check()
            # If it works, should timeout immediately or use default
            assert response.result in (ProbeResult.SUCCESS, ProbeResult.TIMEOUT)
        except ValueError:
            # Also acceptable to reject zero timeout
            pass

    @pytest.mark.asyncio
    async def test_very_large_timeout(self) -> None:
        """Test probe with very large timeout."""
        check_called = False

        async def large_timeout_check() -> tuple[bool, str]:
            nonlocal check_called
            check_called = True
            return True, "Completed"

        probe = HealthProbe(
            name="large_timeout",
            check=large_timeout_check,
            config=ProbeConfig(timeout_seconds=3600.0),  # 1 hour
        )

        response = await probe.check()
        assert check_called is True
        assert response.result == ProbeResult.SUCCESS

    @pytest.mark.asyncio
    async def test_timeout_recovery(self) -> None:
        """Test recovery after timeout."""
        should_timeout = True

        async def intermittent_check() -> tuple[bool, str]:
            if should_timeout:
                await asyncio.sleep(1.0)
            return True, "OK"

        probe = HealthProbe(
            name="timeout_recovery",
            check=intermittent_check,
            config=ProbeConfig(
                timeout_seconds=0.1,
                failure_threshold=1,
                success_threshold=1,
            ),
        )

        # First check times out
        response = await probe.check()
        assert response.result == ProbeResult.TIMEOUT
        assert probe.is_healthy() is False

        # Recovery check
        should_timeout = False
        response = await probe.check()
        assert response.result == ProbeResult.SUCCESS
        assert probe.is_healthy() is True


class TestThresholdBoundaryConditions:
    """Test threshold boundary conditions."""

    @pytest.mark.asyncio
    async def test_failure_threshold_one(self) -> None:
        """Test with failure_threshold=1 (immediate failure)."""
        success = True

        async def check() -> tuple[bool, str]:
            return success, "OK" if success else "FAIL"

        probe = HealthProbe(
            name="threshold_one",
            check=check,
            config=ProbeConfig(failure_threshold=1, success_threshold=1),
        )

        assert probe.is_healthy() is True

        # Single failure should trigger unhealthy
        success = False
        await probe.check()
        assert probe.is_healthy() is False

    @pytest.mark.asyncio
    async def test_success_threshold_higher_than_failure(self) -> None:
        """Test when success_threshold > failure_threshold."""
        success = True

        async def check() -> tuple[bool, str]:
            return success, "OK" if success else "FAIL"

        probe = HealthProbe(
            name="high_success_threshold",
            check=check,
            config=ProbeConfig(
                failure_threshold=2,
                success_threshold=3,  # Higher than failure
            ),
        )

        # Get to unhealthy state
        success = False
        await probe.check()
        await probe.check()
        assert probe.is_healthy() is False

        # Now need 3 successes to recover
        success = True
        await probe.check()
        assert probe.is_healthy() is False  # Only 1 success

        await probe.check()
        assert probe.is_healthy() is False  # Only 2 successes

        await probe.check()
        assert probe.is_healthy() is True  # 3 successes - recovered

    @pytest.mark.asyncio
    async def test_very_high_threshold(self) -> None:
        """Test with very high failure threshold."""
        success = False

        async def check() -> tuple[bool, str]:
            return success, "OK" if success else "FAIL"

        probe = HealthProbe(
            name="high_threshold",
            check=check,
            config=ProbeConfig(failure_threshold=100),
        )

        # Should stay healthy through many failures
        for _ in range(50):
            await probe.check()
        assert probe.is_healthy() is True

        # Continue to threshold
        for _ in range(50):
            await probe.check()
        assert probe.is_healthy() is False

    @pytest.mark.asyncio
    async def test_alternating_success_failure(self) -> None:
        """Test alternating success/failure resets consecutive counts."""
        toggle = True

        async def alternating_check() -> tuple[bool, str]:
            return toggle, "OK" if toggle else "FAIL"

        probe = HealthProbe(
            name="alternating",
            check=alternating_check,
            config=ProbeConfig(failure_threshold=3, success_threshold=3),
        )

        # Alternating should never reach threshold
        for _ in range(10):
            await probe.check()
            toggle = not toggle

        # Should remain healthy (never hit 3 consecutive failures)
        assert probe.is_healthy() is True


class TestConcurrentProbeOperations:
    """Test concurrent probe operations."""

    @pytest.mark.asyncio
    async def test_concurrent_checks_same_probe(self) -> None:
        """Test concurrent checks on same probe."""
        check_count = 0

        async def slow_check() -> tuple[bool, str]:
            nonlocal check_count
            check_count += 1
            await asyncio.sleep(0.1)
            return True, f"Check {check_count}"

        probe = HealthProbe(
            name="concurrent",
            check=slow_check,
            config=ProbeConfig(timeout_seconds=1.0),
        )

        # Run multiple checks concurrently
        results = await asyncio.gather(*[probe.check() for _ in range(5)])

        # All should complete
        assert len(results) == 5
        assert all(r.result == ProbeResult.SUCCESS for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_composite_check_all(self) -> None:
        """Test concurrent check_all on composite probe."""
        async def delay_check() -> tuple[bool, str]:
            await asyncio.sleep(0.05)
            return True, "OK"

        probes = [
            HealthProbe(f"probe_{i}", delay_check, ProbeConfig())
            for i in range(5)
        ]

        composite = CompositeProbe("concurrent_composite")
        for p in probes:
            composite.add_probe(p)

        # Multiple concurrent check_all calls
        results = await asyncio.gather(*[composite.check_all() for _ in range(3)])

        assert len(results) == 3
        for result in results:
            assert len(result) == 5

    @pytest.mark.asyncio
    async def test_check_during_periodic_execution(self) -> None:
        """Test manual check while periodic checking is running."""
        check_count = 0

        async def counting_check() -> tuple[bool, str]:
            nonlocal check_count
            check_count += 1
            return True, f"Check {check_count}"

        probe = HealthProbe(
            name="periodic_manual",
            check=counting_check,
            config=ProbeConfig(period_seconds=0.1),
        )

        await probe.start_periodic()

        # Run manual checks during periodic
        for _ in range(3):
            await probe.check()
            await asyncio.sleep(0.05)

        await probe.stop_periodic()

        # Should have counts from both periodic and manual
        assert check_count >= 5  # At least periodic + manual


class TestCompositeProbeFailurePaths:
    """Test failure paths in CompositeProbe."""

    @pytest.mark.asyncio
    async def test_remove_nonexistent_probe(self) -> None:
        """Test removing a probe that doesn't exist."""
        composite = CompositeProbe("test")

        result = composite.remove_probe("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_add_duplicate_probe_name(self) -> None:
        """Test adding probe with duplicate name."""
        async def check1() -> tuple[bool, str]:
            return True, "Check 1"

        async def check2() -> tuple[bool, str]:
            return False, "Check 2"

        probe1 = HealthProbe("duplicate", check1)
        probe2 = HealthProbe("duplicate", check2)  # Same name

        composite = CompositeProbe("test")
        composite.add_probe(probe1)
        composite.add_probe(probe2)  # Should replace or reject

        # Verify behavior (implementation dependent)
        probe_names = list(composite.get_status()["probes"].keys())
        # Should either have one probe named "duplicate" or handle the conflict
        assert "duplicate" in probe_names

    @pytest.mark.asyncio
    async def test_empty_composite_is_healthy(self) -> None:
        """Test that empty composite probe is healthy."""
        composite = CompositeProbe("empty")

        assert composite.is_healthy() is True
        assert composite.get_unhealthy_probes() == []

    @pytest.mark.asyncio
    async def test_all_probes_unhealthy(self) -> None:
        """Test composite when all probes are unhealthy."""
        async def failing_check() -> tuple[bool, str]:
            return False, "Failing"

        probes = [
            HealthProbe(f"probe_{i}", failing_check, ProbeConfig(failure_threshold=1))
            for i in range(3)
        ]

        composite = CompositeProbe("all_failing")
        for p in probes:
            composite.add_probe(p)

        # Fail all probes
        await composite.check_all()

        assert composite.is_healthy() is False
        unhealthy = composite.get_unhealthy_probes()
        assert len(unhealthy) == 3

    @pytest.mark.asyncio
    async def test_check_all_with_one_timing_out(self) -> None:
        """Test check_all when one probe times out."""
        async def fast_check() -> tuple[bool, str]:
            return True, "Fast"

        async def slow_check() -> tuple[bool, str]:
            await asyncio.sleep(1.0)
            return True, "Slow"

        fast_probe = HealthProbe("fast", fast_check, ProbeConfig(timeout_seconds=0.5))
        slow_probe = HealthProbe("slow", slow_check, ProbeConfig(timeout_seconds=0.1, failure_threshold=1))

        composite = CompositeProbe("mixed_timing")
        composite.add_probe(fast_probe)
        composite.add_probe(slow_probe)

        results = await composite.check_all()

        assert results["fast"].result == ProbeResult.SUCCESS
        assert results["slow"].result == ProbeResult.TIMEOUT


class TestStateManagement:
    """Test probe state management and cleanup."""

    @pytest.mark.asyncio
    async def test_reset_clears_state(self) -> None:
        """Test that reset clears all probe state."""
        success = False

        async def check() -> tuple[bool, str]:
            return success, "OK" if success else "FAIL"

        probe = HealthProbe(
            name="reset_test",
            check=check,
            config=ProbeConfig(failure_threshold=2),
        )

        # Get to unhealthy state
        await probe.check()
        await probe.check()
        assert probe.is_healthy() is False

        state_before = probe.get_state()
        assert state_before.consecutive_failures >= 2

        # Reset
        probe.reset()

        state_after = probe.get_state()
        assert state_after.consecutive_failures == 0
        assert state_after.consecutive_successes == 0
        assert state_after.total_checks == 0
        assert probe.is_healthy() is True

    @pytest.mark.asyncio
    async def test_state_persists_across_checks(self) -> None:
        """Test that state persists correctly across many checks."""
        check_number = 0

        async def counting_check() -> tuple[bool, str]:
            nonlocal check_number
            check_number += 1
            return True, f"Check {check_number}"

        probe = HealthProbe("state_persist", counting_check)

        for _ in range(100):
            await probe.check()

        state = probe.get_state()
        assert state.total_checks == 100
        # ProbeState tracks total_checks and total_failures, successes = total_checks - total_failures
        assert state.total_failures == 0
        assert state.total_checks - state.total_failures == 100  # All successes

    @pytest.mark.asyncio
    async def test_stop_periodic_cleanup(self) -> None:
        """Test that stopping periodic execution cleans up properly."""
        async def check() -> tuple[bool, str]:
            return True, "OK"

        probe = HealthProbe(
            name="cleanup_test",
            check=check,
            config=ProbeConfig(period_seconds=0.1),
        )

        await probe.start_periodic()
        await asyncio.sleep(0.3)

        # Stop should clean up
        await probe.stop_periodic()

        # Multiple stops should be safe
        await probe.stop_periodic()
        await probe.stop_periodic()


class TestProbeRecovery:
    """Test probe recovery scenarios."""

    @pytest.mark.asyncio
    async def test_recovery_after_multiple_errors(self) -> None:
        """Test recovery after multiple error conditions."""
        error_count = 0

        async def flaky_check() -> tuple[bool, str]:
            nonlocal error_count
            if error_count < 3:
                error_count += 1
                raise ValueError(f"Error {error_count}")
            return True, "Recovered"

        probe = HealthProbe(
            name="recovery",
            check=flaky_check,
            config=ProbeConfig(failure_threshold=5, success_threshold=1),
        )

        # Cause multiple errors
        for _ in range(3):
            await probe.check()

        # Should still be healthy (under threshold)
        assert probe.is_healthy() is True

        # Recover
        response = await probe.check()
        assert response.result == ProbeResult.SUCCESS
        assert probe.is_healthy() is True

    @pytest.mark.asyncio
    async def test_rapid_state_transitions(self) -> None:
        """Test rapid transitions between healthy and unhealthy."""
        success = True

        async def toggle_check() -> tuple[bool, str]:
            return success, "OK" if success else "FAIL"

        probe = HealthProbe(
            name="rapid_transition",
            check=toggle_check,
            config=ProbeConfig(failure_threshold=1, success_threshold=1),
        )

        # Rapid transitions
        states = []
        for i in range(20):
            success = i % 2 == 0  # Alternate
            await probe.check()
            states.append(probe.is_healthy())

        # Should have captured state changes
        assert True in states
        assert False in states

    @pytest.mark.asyncio
    async def test_recovery_from_prolonged_failure(self) -> None:
        """Test recovery after prolonged failure period."""
        failure_duration = 50
        check_number = 0

        async def prolonged_failure_check() -> tuple[bool, str]:
            nonlocal check_number
            check_number += 1
            if check_number <= failure_duration:
                return False, f"Failing {check_number}/{failure_duration}"
            return True, "Finally recovered"

        probe = HealthProbe(
            name="prolonged",
            check=prolonged_failure_check,
            config=ProbeConfig(failure_threshold=10, success_threshold=1),
        )

        # Run through failures
        for _ in range(failure_duration):
            await probe.check()

        assert probe.is_healthy() is False

        # One success should recover (success_threshold=1)
        response = await probe.check()
        assert response.result == ProbeResult.SUCCESS
        assert probe.is_healthy() is True


class TestEdgeCaseInputs:
    """Test edge case inputs."""

    @pytest.mark.asyncio
    async def test_empty_probe_name(self) -> None:
        """Test probe with empty name."""
        async def check() -> tuple[bool, str]:
            return True, "OK"

        probe = HealthProbe(name="", check=check)
        assert probe.name == ""
        response = await probe.check()
        assert response.result == ProbeResult.SUCCESS

    @pytest.mark.asyncio
    async def test_unicode_probe_name(self) -> None:
        """Test probe with unicode name."""
        async def check() -> tuple[bool, str]:
            return True, "OK"

        probe = HealthProbe(name="健康检查_🏥", check=check)
        assert probe.name == "健康检查_🏥"
        response = await probe.check()
        assert response.result == ProbeResult.SUCCESS

    @pytest.mark.asyncio
    async def test_very_long_message(self) -> None:
        """Test check returning very long message."""
        long_message = "x" * 10000

        async def long_message_check() -> tuple[bool, str]:
            return True, long_message

        probe = HealthProbe(name="long_message", check=long_message_check)
        response = await probe.check()

        assert response.result == ProbeResult.SUCCESS
        # Message should be preserved (or truncated, depending on implementation)
        assert len(response.message) > 0

    @pytest.mark.asyncio
    async def test_negative_config_values(self) -> None:
        """Test handling of negative config values."""
        async def check() -> tuple[bool, str]:
            return True, "OK"

        # These should either raise or be handled gracefully
        try:
            probe = HealthProbe(
                name="negative_config",
                check=check,
                config=ProbeConfig(
                    timeout_seconds=-1.0,
                    failure_threshold=-1,
                ),
            )
            # If it accepts negative values, should still work somehow
            response = await probe.check()
            # Behavior is implementation dependent
        except (ValueError, TypeError):
            # Rejecting negative values is acceptable
            pass
