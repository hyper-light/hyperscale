"""
Integration tests for CircuitBreakerManager.

Tests:
- Happy path: circuit creation, success/failure recording, state transitions
- Negative path: invalid inputs, missing circuits
- Failure modes: circuit open/half-open/closed transitions
- Concurrent access and race conditions
- Edge cases: boundary conditions, cleanup operations
"""

import time
from concurrent.futures import ThreadPoolExecutor

from hyperscale.distributed.health.circuit_breaker_manager import (
    CircuitBreakerManager,
    CircuitBreakerConfig,
)
from hyperscale.distributed.swim.core import CircuitState


class MockEnv:
    """Mock Env for testing CircuitBreakerManager."""

    def __init__(
        self,
        max_errors: int = 5,
        window_seconds: float = 60.0,
        half_open_after: float = 30.0,
    ):
        self._max_errors = max_errors
        self._window_seconds = window_seconds
        self._half_open_after = half_open_after

    def get_circuit_breaker_config(self) -> dict:
        return {
            'max_errors': self._max_errors,
            'window_seconds': self._window_seconds,
            'half_open_after': self._half_open_after,
        }


# =============================================================================
# Happy Path Tests
# =============================================================================


class TestCircuitBreakerManagerHappyPath:
    """Test normal operation of CircuitBreakerManager."""

    def test_initialization(self) -> None:
        """Test CircuitBreakerManager initializes with correct config."""
        env = MockEnv(max_errors=10, window_seconds=120.0, half_open_after=60.0)
        manager = CircuitBreakerManager(env)

        assert manager._config.max_errors == 10
        assert manager._config.window_seconds == 120.0
        assert manager._config.half_open_after == 60.0
        assert len(manager._circuits) == 0

    def test_get_circuit_creates_new_circuit(self) -> None:
        """Test get_circuit creates a new circuit for unknown manager."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        circuit = manager.get_circuit(addr)

        assert circuit is not None
        assert addr in manager._circuits
        assert circuit.circuit_state == CircuitState.CLOSED

    def test_get_circuit_returns_existing_circuit(self) -> None:
        """Test get_circuit returns the same circuit for known manager."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        circuit1 = manager.get_circuit(addr)
        circuit2 = manager.get_circuit(addr)

        assert circuit1 is circuit2

    def test_record_success_on_existing_circuit(self) -> None:
        """Test recording success updates the circuit."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Create circuit first
        manager.get_circuit(addr)
        manager.record_success(addr)

        # Success on closed circuit should keep it closed
        assert not manager.is_circuit_open(addr)

    def test_record_failure_increments_error_count(self) -> None:
        """Test recording failure increments error count."""
        env = MockEnv(max_errors=5)
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Record 3 failures (below threshold)
        for _ in range(3):
            manager.record_failure(addr)

        circuit = manager.get_circuit(addr)
        assert circuit.error_count == 3
        assert circuit.circuit_state == CircuitState.CLOSED

    def test_get_circuit_status(self) -> None:
        """Test get_circuit_status returns correct status dict."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        manager.get_circuit(addr)
        manager.record_failure(addr)

        status = manager.get_circuit_status(addr)

        assert status is not None
        assert status["manager_addr"] == "192.168.1.1:8080"
        assert status["circuit_state"] == "CLOSED"
        assert status["error_count"] == 1
        assert "error_rate" in status

    def test_get_all_circuit_status(self) -> None:
        """Test get_all_circuit_status returns all managers."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr1 = ("192.168.1.1", 8080)
        addr2 = ("192.168.1.2", 8080)

        manager.get_circuit(addr1)
        manager.get_circuit(addr2)

        status = manager.get_all_circuit_status()

        assert "managers" in status
        assert "open_circuits" in status
        assert "192.168.1.1:8080" in status["managers"]
        assert "192.168.1.2:8080" in status["managers"]
        assert status["open_circuits"] == []

    def test_remove_circuit(self) -> None:
        """Test remove_circuit removes the circuit for a manager."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        manager.get_circuit(addr)
        assert addr in manager._circuits

        manager.remove_circuit(addr)
        assert addr not in manager._circuits

    def test_clear_all(self) -> None:
        """Test clear_all removes all circuits."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)

        # Create multiple circuits
        for idx in range(5):
            manager.get_circuit((f"192.168.1.{idx}", 8080))

        assert len(manager._circuits) == 5

        manager.clear_all()
        assert len(manager._circuits) == 0


# =============================================================================
# Negative Path Tests
# =============================================================================


class TestCircuitBreakerManagerNegativePath:
    """Test error handling and edge cases."""

    def test_is_circuit_open_unknown_manager(self) -> None:
        """Test is_circuit_open returns False for unknown manager."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # No circuit exists, should return False
        assert manager.is_circuit_open(addr) is False

    def test_get_circuit_status_unknown_manager(self) -> None:
        """Test get_circuit_status returns None for unknown manager."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        status = manager.get_circuit_status(addr)
        assert status is None

    def test_record_success_unknown_manager(self) -> None:
        """Test record_success on unknown manager is a no-op."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Should not raise, should be a no-op
        manager.record_success(addr)

        # Should not create a circuit
        assert addr not in manager._circuits

    def test_record_failure_creates_circuit(self) -> None:
        """Test record_failure creates circuit if not exists."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # record_failure should create the circuit
        manager.record_failure(addr)

        assert addr in manager._circuits
        assert manager.get_circuit(addr).error_count == 1

    def test_remove_circuit_unknown_manager(self) -> None:
        """Test remove_circuit on unknown manager is a no-op."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Should not raise
        manager.remove_circuit(addr)
        assert addr not in manager._circuits


# =============================================================================
# Failure Mode Tests - Circuit State Transitions
# =============================================================================


class TestCircuitBreakerManagerFailureModes:
    """Test circuit breaker state transitions."""

    def test_circuit_opens_after_max_errors(self) -> None:
        """Test circuit opens after max_errors failures."""
        env = MockEnv(max_errors=5)
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Record exactly max_errors failures
        for _ in range(5):
            manager.record_failure(addr)

        assert manager.is_circuit_open(addr) is True
        circuit = manager.get_circuit(addr)
        assert circuit.circuit_state == CircuitState.OPEN

    def test_circuit_stays_closed_below_threshold(self) -> None:
        """Test circuit stays closed below max_errors threshold."""
        env = MockEnv(max_errors=5)
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Record max_errors - 1 failures
        for _ in range(4):
            manager.record_failure(addr)

        assert manager.is_circuit_open(addr) is False

    def test_circuit_transitions_to_half_open(self) -> None:
        """Test circuit transitions to half-open after timeout."""
        env = MockEnv(max_errors=5, half_open_after=0.1)  # 100ms
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Open the circuit
        for _ in range(5):
            manager.record_failure(addr)
        assert manager.is_circuit_open(addr) is True

        # Wait for half_open_after timeout
        time.sleep(0.15)

        # Circuit should now be half-open
        circuit = manager.get_circuit(addr)
        assert circuit.circuit_state == CircuitState.HALF_OPEN

    def test_circuit_closes_on_success_in_half_open(self) -> None:
        """Test circuit closes when success recorded in half-open state."""
        env = MockEnv(max_errors=5, half_open_after=0.05)  # 50ms
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Open the circuit
        for _ in range(5):
            manager.record_failure(addr)

        # Wait for half-open
        time.sleep(0.1)

        circuit = manager.get_circuit(addr)
        assert circuit.circuit_state == CircuitState.HALF_OPEN

        # Record success
        manager.record_success(addr)

        assert circuit.circuit_state == CircuitState.CLOSED
        assert manager.is_circuit_open(addr) is False

    def test_circuit_reopens_on_failure_in_half_open(self) -> None:
        """Test circuit reopens when failure recorded in half-open state."""
        env = MockEnv(max_errors=1, half_open_after=0.05)  # 50ms
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Open the circuit
        manager.record_failure(addr)
        assert manager.is_circuit_open(addr) is True

        # Wait for half-open
        time.sleep(0.1)

        circuit = manager.get_circuit(addr)
        assert circuit.circuit_state == CircuitState.HALF_OPEN

        # Record failure - should re-open
        manager.record_failure(addr)

        assert circuit.circuit_state == CircuitState.OPEN
        assert manager.is_circuit_open(addr) is True

    def test_open_circuits_listed_correctly(self) -> None:
        """Test get_all_circuit_status lists open circuits correctly."""
        env = MockEnv(max_errors=2)
        manager = CircuitBreakerManager(env)
        addr1 = ("192.168.1.1", 8080)
        addr2 = ("192.168.1.2", 8080)
        addr3 = ("192.168.1.3", 8080)

        # Open circuit for addr1
        manager.record_failure(addr1)
        manager.record_failure(addr1)

        # Create but don't open circuit for addr2
        manager.get_circuit(addr2)

        # Open circuit for addr3
        manager.record_failure(addr3)
        manager.record_failure(addr3)

        status = manager.get_all_circuit_status()

        assert len(status["open_circuits"]) == 2
        assert "192.168.1.1:8080" in status["open_circuits"]
        assert "192.168.1.3:8080" in status["open_circuits"]
        assert "192.168.1.2:8080" not in status["open_circuits"]


# =============================================================================
# Concurrent Access Tests
# =============================================================================


class TestCircuitBreakerManagerConcurrency:
    """Test thread safety and concurrent access."""

    def test_concurrent_get_circuit_same_addr(self) -> None:
        """Test concurrent get_circuit calls for same address."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)
        results: list = []

        def get_circuit_worker() -> None:
            circuit = manager.get_circuit(addr)
            results.append(circuit)

        # Run multiple threads concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_circuit_worker) for _ in range(100)]
            for future in futures:
                future.result()

        # All results should be the same circuit instance
        assert len(results) == 100
        assert all(circuit is results[0] for circuit in results)

    def test_concurrent_get_circuit_different_addrs(self) -> None:
        """Test concurrent get_circuit calls for different addresses."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        results: dict = {}

        def get_circuit_worker(idx: int) -> None:
            addr = (f"192.168.1.{idx}", 8080)
            circuit = manager.get_circuit(addr)
            results[addr] = circuit

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_circuit_worker, idx) for idx in range(50)]
            for future in futures:
                future.result()

        # Should have 50 different circuits
        assert len(manager._circuits) == 50
        assert len(results) == 50

    def test_concurrent_record_failures(self) -> None:
        """Test concurrent failure recording."""
        env = MockEnv(max_errors=100)
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        def record_failure_worker() -> None:
            manager.record_failure(addr)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(record_failure_worker) for _ in range(50)]
            for future in futures:
                future.result()

        # Error count should be exactly 50
        circuit = manager.get_circuit(addr)
        assert circuit.error_count == 50

    def test_concurrent_mixed_operations(self) -> None:
        """Test concurrent success/failure recording."""
        env = MockEnv(max_errors=100)
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Pre-create the circuit
        manager.get_circuit(addr)

        def success_worker() -> None:
            manager.record_success(addr)

        def failure_worker() -> None:
            manager.record_failure(addr)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for idx in range(100):
                if idx % 2 == 0:
                    futures.append(executor.submit(success_worker))
                else:
                    futures.append(executor.submit(failure_worker))
            for future in futures:
                future.result()

        # Should complete without errors
        # Circuit should exist and be in a valid state
        circuit = manager.get_circuit(addr)
        assert circuit.circuit_state in (
            CircuitState.CLOSED,
            CircuitState.OPEN,
            CircuitState.HALF_OPEN,
        )

    def test_concurrent_remove_and_get(self) -> None:
        """Test concurrent remove and get operations."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Pre-create the circuit
        manager.get_circuit(addr)

        def remove_worker() -> None:
            manager.remove_circuit(addr)

        def get_worker() -> None:
            manager.get_circuit(addr)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for idx in range(100):
                if idx % 2 == 0:
                    futures.append(executor.submit(remove_worker))
                else:
                    futures.append(executor.submit(get_worker))
            for future in futures:
                future.result()

        # Should complete without errors - circuit may or may not exist


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestCircuitBreakerManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_max_errors_one(self) -> None:
        """Test circuit with max_errors=1 opens immediately."""
        env = MockEnv(max_errors=1)
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        manager.record_failure(addr)

        assert manager.is_circuit_open(addr) is True

    def test_max_errors_zero_behavior(self) -> None:
        """Test behavior with max_errors=0 (edge case)."""
        # This tests the underlying ErrorStats behavior
        env = MockEnv(max_errors=0)
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # With max_errors=0, first failure should not open circuit
        # (len(timestamps) >= 0 is always true, but this depends on ErrorStats impl)
        manager.record_failure(addr)

        # The actual behavior depends on ErrorStats implementation
        # Just verify it doesn't crash
        circuit = manager.get_circuit(addr)
        assert circuit is not None

    def test_very_short_window(self) -> None:
        """Test with very short window_seconds."""
        env = MockEnv(max_errors=5, window_seconds=0.1)  # 100ms window
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        # Record failures
        for _ in range(3):
            manager.record_failure(addr)

        # Wait for window to expire
        time.sleep(0.15)

        # Old errors should be pruned
        circuit = manager.get_circuit(addr)
        assert circuit.error_count < 3

    def test_very_short_half_open_after(self) -> None:
        """Test with very short half_open_after."""
        env = MockEnv(max_errors=1, half_open_after=0.01)  # 10ms
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        manager.record_failure(addr)
        assert manager.is_circuit_open(addr) is True

        # Very short wait
        time.sleep(0.02)

        circuit = manager.get_circuit(addr)
        assert circuit.circuit_state == CircuitState.HALF_OPEN

    def test_ipv6_address(self) -> None:
        """Test with IPv6 address tuple."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("::1", 8080)

        circuit = manager.get_circuit(addr)
        assert circuit is not None

        status = manager.get_circuit_status(addr)
        assert status["manager_addr"] == "::1:8080"

    def test_large_port_number(self) -> None:
        """Test with maximum port number."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 65535)

        circuit = manager.get_circuit(addr)
        assert circuit is not None

        status = manager.get_circuit_status(addr)
        assert status["manager_addr"] == "192.168.1.1:65535"

    def test_many_managers(self) -> None:
        """Test with many manager circuits."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)

        # Create 1000 circuits
        for idx in range(1000):
            host = f"192.168.{idx // 256}.{idx % 256}"
            manager.get_circuit((host, 8080))

        assert len(manager._circuits) == 1000

        # Clear all
        manager.clear_all()
        assert len(manager._circuits) == 0

    def test_circuit_config_matches_env(self) -> None:
        """Test that circuit config matches env settings."""
        env = MockEnv(max_errors=7, window_seconds=45.0, half_open_after=15.0)
        manager = CircuitBreakerManager(env)
        addr = ("192.168.1.1", 8080)

        circuit = manager.get_circuit(addr)

        assert circuit.max_errors == 7
        assert circuit.window_seconds == 45.0
        assert circuit.half_open_after == 15.0

    def test_duplicate_addr_different_ports(self) -> None:
        """Test same host with different ports are separate circuits."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)

        addr1 = ("192.168.1.1", 8080)
        addr2 = ("192.168.1.1", 8081)

        circuit1 = manager.get_circuit(addr1)
        circuit2 = manager.get_circuit(addr2)

        assert circuit1 is not circuit2
        assert len(manager._circuits) == 2

    def test_status_after_clear_all(self) -> None:
        """Test get_all_circuit_status after clear_all."""
        env = MockEnv()
        manager = CircuitBreakerManager(env)

        manager.get_circuit(("192.168.1.1", 8080))
        manager.clear_all()

        status = manager.get_all_circuit_status()

        assert status["managers"] == {}
        assert status["open_circuits"] == []
