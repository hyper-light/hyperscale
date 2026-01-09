"""
Integration tests for Unified Retry Framework with Jitter (AD-21).

These tests verify that:
1. JitterStrategy enum has correct values
2. RetryConfig dataclass has all required fields
3. RetryExecutor correctly calculates delays with jitter
4. Retries are attempted for retryable exceptions
5. Non-retryable exceptions are raised immediately
6. execute_with_fallback properly uses fallback on failure
"""

import asyncio
import pytest
import time

from hyperscale.distributed_rewrite.reliability import (
    JitterStrategy,
    RetryConfig,
    RetryExecutor,
)
from hyperscale.distributed_rewrite.reliability.retry import (
    calculate_jittered_delay,
    add_jitter,
)


class TestJitterStrategy:
    """Test JitterStrategy enum."""

    def test_jitter_strategy_values(self):
        """JitterStrategy should have correct values."""
        assert JitterStrategy.FULL.value == "full"
        assert JitterStrategy.EQUAL.value == "equal"
        assert JitterStrategy.DECORRELATED.value == "decorrelated"
        assert JitterStrategy.NONE.value == "none"


class TestRetryConfig:
    """Test RetryConfig dataclass."""

    def test_default_config_values(self):
        """RetryConfig should have sensible defaults."""
        config = RetryConfig()

        assert config.max_attempts == 3
        assert config.base_delay == 0.5
        assert config.max_delay == 30.0
        assert config.jitter == JitterStrategy.FULL
        assert ConnectionError in config.retryable_exceptions
        assert TimeoutError in config.retryable_exceptions
        assert OSError in config.retryable_exceptions

    def test_custom_config(self):
        """RetryConfig should accept custom values."""
        config = RetryConfig(
            max_attempts=5,
            base_delay=1.0,
            max_delay=60.0,
            jitter=JitterStrategy.EQUAL,
            retryable_exceptions=(ValueError, KeyError),
        )

        assert config.max_attempts == 5
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.jitter == JitterStrategy.EQUAL
        assert ValueError in config.retryable_exceptions
        assert KeyError in config.retryable_exceptions

    def test_custom_is_retryable_function(self):
        """RetryConfig should accept custom is_retryable function."""

        def custom_check(exc: Exception) -> bool:
            return "temporary" in str(exc).lower()

        config = RetryConfig(is_retryable=custom_check)
        assert config.is_retryable is not None


class TestRetryExecutorDelayCalculation:
    """Test RetryExecutor delay calculation with different jitter strategies."""

    def test_full_jitter_delay_in_range(self):
        """Full jitter delay should be in [0, calculated_delay]."""
        config = RetryConfig(
            base_delay=1.0,
            max_delay=30.0,
            jitter=JitterStrategy.FULL,
        )
        executor = RetryExecutor(config)

        for attempt in range(5):
            delay = executor.calculate_delay(attempt)
            max_possible = min(30.0, 1.0 * (2**attempt))
            assert 0 <= delay <= max_possible

    def test_equal_jitter_delay_has_minimum(self):
        """Equal jitter delay should have minimum of half the calculated delay."""
        config = RetryConfig(
            base_delay=1.0,
            max_delay=30.0,
            jitter=JitterStrategy.EQUAL,
        )
        executor = RetryExecutor(config)

        for attempt in range(5):
            delay = executor.calculate_delay(attempt)
            temp = min(30.0, 1.0 * (2**attempt))
            min_delay = temp / 2
            max_delay = temp
            assert min_delay <= delay <= max_delay

    def test_no_jitter_delay_is_deterministic(self):
        """No jitter delay should be deterministic exponential backoff."""
        config = RetryConfig(
            base_delay=1.0,
            max_delay=30.0,
            jitter=JitterStrategy.NONE,
        )
        executor = RetryExecutor(config)

        # Attempt 0: 1.0 * 2^0 = 1.0
        assert executor.calculate_delay(0) == 1.0

        # Attempt 1: 1.0 * 2^1 = 2.0
        assert executor.calculate_delay(1) == 2.0

        # Attempt 2: 1.0 * 2^2 = 4.0
        assert executor.calculate_delay(2) == 4.0

    def test_decorrelated_jitter_bounded_growth(self):
        """Decorrelated jitter should have bounded growth."""
        config = RetryConfig(
            base_delay=1.0,
            max_delay=30.0,
            jitter=JitterStrategy.DECORRELATED,
        )
        executor = RetryExecutor(config)

        previous_delay = config.base_delay
        for attempt in range(5):
            delay = executor.calculate_delay(attempt)
            # Delay should be in [base, previous * 3] but capped at max_delay
            assert delay <= 30.0
            previous_delay = delay

    def test_delay_respects_max_delay_cap(self):
        """Delay should never exceed max_delay."""
        config = RetryConfig(
            base_delay=1.0,
            max_delay=10.0,
            jitter=JitterStrategy.NONE,
        )
        executor = RetryExecutor(config)

        # Attempt 10: 1.0 * 2^10 = 1024.0, but capped at 10.0
        assert executor.calculate_delay(10) == 10.0

    def test_reset_clears_decorrelated_state(self):
        """Reset should reset decorrelated jitter state."""
        config = RetryConfig(
            base_delay=1.0,
            jitter=JitterStrategy.DECORRELATED,
        )
        executor = RetryExecutor(config)

        # Advance decorrelated state
        for _ in range(5):
            executor.calculate_delay(0)

        executor.reset()

        # After reset, state should be back to base_delay
        assert executor._previous_delay == config.base_delay


class TestRetryExecutorExecution:
    """Test RetryExecutor async execution."""

    @pytest.mark.asyncio
    async def test_successful_operation_returns_result(self):
        """Successful operation should return result immediately."""
        executor = RetryExecutor()

        async def success_op():
            return "success"

        result = await executor.execute(success_op, "test_op")
        assert result == "success"

    @pytest.mark.asyncio
    async def test_retries_on_retryable_exception(self):
        """Should retry on retryable exceptions."""
        config = RetryConfig(
            max_attempts=3,
            base_delay=0.01,  # Fast for testing
            jitter=JitterStrategy.NONE,
        )
        executor = RetryExecutor(config)

        attempt_count = 0

        async def failing_then_success():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ConnectionError("temporary failure")
            return "success"

        result = await executor.execute(failing_then_success, "test_op")
        assert result == "success"
        assert attempt_count == 3

    @pytest.mark.asyncio
    async def test_raises_after_max_attempts(self):
        """Should raise after exhausting max_attempts."""
        config = RetryConfig(
            max_attempts=3,
            base_delay=0.01,
            jitter=JitterStrategy.NONE,
        )
        executor = RetryExecutor(config)

        attempt_count = 0

        async def always_fails():
            nonlocal attempt_count
            attempt_count += 1
            raise ConnectionError("persistent failure")

        with pytest.raises(ConnectionError):
            await executor.execute(always_fails, "test_op")

        assert attempt_count == 3

    @pytest.mark.asyncio
    async def test_non_retryable_exception_raises_immediately(self):
        """Non-retryable exception should raise immediately."""
        config = RetryConfig(
            max_attempts=3,
            retryable_exceptions=(ConnectionError,),
        )
        executor = RetryExecutor(config)

        attempt_count = 0

        async def raises_non_retryable():
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            await executor.execute(raises_non_retryable, "test_op")

        assert attempt_count == 1

    @pytest.mark.asyncio
    async def test_custom_is_retryable_function(self):
        """Custom is_retryable function should be used."""

        def is_temporary(exc: Exception) -> bool:
            return "temporary" in str(exc).lower()

        config = RetryConfig(
            max_attempts=3,
            base_delay=0.01,
            is_retryable=is_temporary,
        )
        executor = RetryExecutor(config)

        attempt_count = 0

        async def raises_temporary_then_success():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 2:
                raise RuntimeError("temporary error")
            return "success"

        result = await executor.execute(raises_temporary_then_success, "test_op")
        assert result == "success"
        assert attempt_count == 2

    @pytest.mark.asyncio
    async def test_execute_with_fallback_uses_fallback(self):
        """execute_with_fallback should use fallback on exhaustion."""
        config = RetryConfig(
            max_attempts=2,
            base_delay=0.01,
        )
        executor = RetryExecutor(config)

        async def always_fails():
            raise ConnectionError("failure")

        async def fallback():
            return "fallback_result"

        result = await executor.execute_with_fallback(
            always_fails,
            fallback,
            "test_op",
        )
        assert result == "fallback_result"

    @pytest.mark.asyncio
    async def test_execute_with_fallback_prefers_primary(self):
        """execute_with_fallback should prefer primary if it succeeds."""
        config = RetryConfig(max_attempts=2)
        executor = RetryExecutor(config)

        async def primary():
            return "primary_result"

        async def fallback():
            return "fallback_result"

        result = await executor.execute_with_fallback(
            primary,
            fallback,
            "test_op",
        )
        assert result == "primary_result"


class TestStandaloneFunctions:
    """Test standalone jitter utility functions."""

    def test_calculate_jittered_delay_full(self):
        """calculate_jittered_delay with full jitter should be in range."""
        for _ in range(10):
            delay = calculate_jittered_delay(
                attempt=2,
                base_delay=1.0,
                max_delay=30.0,
                jitter=JitterStrategy.FULL,
            )
            # 1.0 * 2^2 = 4.0
            assert 0 <= delay <= 4.0

    def test_calculate_jittered_delay_equal(self):
        """calculate_jittered_delay with equal jitter should have minimum."""
        for _ in range(10):
            delay = calculate_jittered_delay(
                attempt=2,
                base_delay=1.0,
                max_delay=30.0,
                jitter=JitterStrategy.EQUAL,
            )
            # 1.0 * 2^2 = 4.0, min = 2.0
            assert 2.0 <= delay <= 4.0

    def test_calculate_jittered_delay_none(self):
        """calculate_jittered_delay with no jitter should be deterministic."""
        delay = calculate_jittered_delay(
            attempt=2,
            base_delay=1.0,
            max_delay=30.0,
            jitter=JitterStrategy.NONE,
        )
        assert delay == 4.0

    def test_add_jitter_within_factor(self):
        """add_jitter should add jitter within factor of interval."""
        interval = 30.0
        jitter_factor = 0.1

        for _ in range(20):
            result = add_jitter(interval, jitter_factor)
            min_expected = interval - (interval * jitter_factor)  # 27.0
            max_expected = interval + (interval * jitter_factor)  # 33.0
            assert min_expected <= result <= max_expected

    def test_add_jitter_default_factor(self):
        """add_jitter should use default 10% factor."""
        for _ in range(20):
            result = add_jitter(100.0)
            assert 90.0 <= result <= 110.0


class TestRetryScenarios:
    """Test realistic retry scenarios."""

    @pytest.mark.asyncio
    async def test_network_reconnection_scenario(self):
        """
        Simulate network reconnection with retries.

        Scenario: Client loses connection, retries with backoff,
        and eventually reconnects.
        """
        config = RetryConfig(
            max_attempts=5,
            base_delay=0.01,
            jitter=JitterStrategy.FULL,
        )
        executor = RetryExecutor(config)

        connection_attempt = 0
        recovery_after = 3

        async def connect():
            nonlocal connection_attempt
            connection_attempt += 1
            if connection_attempt < recovery_after:
                raise ConnectionError("Connection refused")
            return "connected"

        result = await executor.execute(connect, "connect")
        assert result == "connected"
        assert connection_attempt == recovery_after

    @pytest.mark.asyncio
    async def test_timeout_recovery_scenario(self):
        """
        Simulate timeout recovery with retries.

        Scenario: Operation times out initially but succeeds
        on subsequent attempts.
        """
        config = RetryConfig(
            max_attempts=4,
            base_delay=0.01,
            jitter=JitterStrategy.EQUAL,  # Guarantees minimum delay
        )
        executor = RetryExecutor(config)

        attempt = 0

        async def slow_operation():
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise TimeoutError("Operation timed out")
            return "completed"

        result = await executor.execute(slow_operation, "slow_op")
        assert result == "completed"

    @pytest.mark.asyncio
    async def test_fallback_to_cache_scenario(self):
        """
        Simulate falling back to cached data.

        Scenario: Primary data source unavailable, fall back
        to cached/stale data.
        """
        config = RetryConfig(
            max_attempts=2,
            base_delay=0.01,
        )
        executor = RetryExecutor(config)

        async def fetch_fresh_data():
            raise ConnectionError("Data source unavailable")

        async def fetch_cached_data():
            return {"data": "cached", "stale": True}

        result = await executor.execute_with_fallback(
            fetch_fresh_data,
            fetch_cached_data,
            "fetch_data",
        )
        assert result["data"] == "cached"
        assert result["stale"] is True

    @pytest.mark.asyncio
    async def test_thundering_herd_prevention(self):
        """
        Test that jitter spreads out retry attempts.

        Scenario: Multiple clients retry simultaneously, jitter
        should spread their attempts to prevent thundering herd.
        """
        config = RetryConfig(
            max_attempts=1,
            base_delay=1.0,
            max_delay=10.0,
            jitter=JitterStrategy.FULL,
        )

        delays = []
        for _ in range(100):
            executor = RetryExecutor(config)
            delay = executor.calculate_delay(0)
            delays.append(delay)

        # Check that delays are spread out (not all the same)
        unique_delays = set(round(d, 6) for d in delays)
        assert len(unique_delays) > 50  # Should have significant variation

        # Check that delays span the range
        assert min(delays) < 0.5  # Some near 0
        assert max(delays) > 0.5  # Some near 1.0
