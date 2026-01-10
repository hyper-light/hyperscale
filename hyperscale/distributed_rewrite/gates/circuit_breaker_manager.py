"""
Circuit Breaker Manager for Gate-to-Manager connections.

Manages per-manager circuit breakers to isolate failures and prevent
cascading failures when a manager becomes unhealthy.
"""

from dataclasses import dataclass, field

from hyperscale.distributed_rewrite.swim.core import (
    ErrorStats,
    CircuitState,
)
from hyperscale.distributed_rewrite.env import Env


@dataclass(slots=True)
class CircuitBreakerConfig:
    """Configuration for circuit breakers."""
    max_errors: int = 5
    window_seconds: float = 60.0
    half_open_after: float = 30.0


class CircuitBreakerManager:
    """
    Manages circuit breakers for gate-to-manager connections.

    Each manager has its own circuit breaker so that failures to one
    manager don't affect dispatch to other managers.
    """

    __slots__ = ('_circuits', '_config')

    def __init__(self, env: Env):
        """
        Initialize the circuit breaker manager.

        Args:
            env: Environment configuration with circuit breaker settings.
        """
        cb_config = env.get_circuit_breaker_config()
        self._config = CircuitBreakerConfig(
            max_errors=cb_config['max_errors'],
            window_seconds=cb_config['window_seconds'],
            half_open_after=cb_config['half_open_after'],
        )
        self._circuits: dict[tuple[str, int], ErrorStats] = {}

    def get_circuit(self, manager_addr: tuple[str, int]) -> ErrorStats:
        """
        Get or create a circuit breaker for a specific manager.

        Args:
            manager_addr: (host, port) tuple for the manager.

        Returns:
            ErrorStats circuit breaker for this manager.
        """
        if manager_addr not in self._circuits:
            self._circuits[manager_addr] = ErrorStats(
                max_errors=self._config.max_errors,
                window_seconds=self._config.window_seconds,
                half_open_after=self._config.half_open_after,
            )
        return self._circuits[manager_addr]

    def is_circuit_open(self, manager_addr: tuple[str, int]) -> bool:
        """
        Check if a manager's circuit breaker is open.

        Args:
            manager_addr: (host, port) tuple for the manager.

        Returns:
            True if the circuit is open (manager should not be contacted).
        """
        circuit = self._circuits.get(manager_addr)
        if not circuit:
            return False
        return circuit.circuit_state == CircuitState.OPEN

    def get_circuit_status(self, manager_addr: tuple[str, int]) -> dict | None:
        """
        Get circuit breaker status for a specific manager.

        Args:
            manager_addr: (host, port) tuple for the manager.

        Returns:
            Dict with circuit status, or None if manager has no circuit breaker.
        """
        circuit = self._circuits.get(manager_addr)
        if not circuit:
            return None
        return {
            "manager_addr": f"{manager_addr[0]}:{manager_addr[1]}",
            "circuit_state": circuit.circuit_state.name,
            "error_count": circuit.error_count,
            "error_rate": circuit.error_rate,
        }

    def get_all_circuit_status(self) -> dict:
        """
        Get circuit breaker status for all managers.

        Returns:
            Dict with all manager circuit statuses and list of open circuits.
        """
        return {
            "managers": {
                f"{addr[0]}:{addr[1]}": self.get_circuit_status(addr)
                for addr in self._circuits.keys()
            },
            "open_circuits": [
                f"{addr[0]}:{addr[1]}" for addr in self._circuits.keys()
                if self.is_circuit_open(addr)
            ],
        }

    def record_success(self, manager_addr: tuple[str, int]) -> None:
        """
        Record a successful operation to a manager.

        Args:
            manager_addr: (host, port) tuple for the manager.
        """
        circuit = self._circuits.get(manager_addr)
        if circuit:
            circuit.record_success()

    def record_failure(self, manager_addr: tuple[str, int]) -> None:
        """
        Record a failed operation to a manager.

        Args:
            manager_addr: (host, port) tuple for the manager.
        """
        circuit = self.get_circuit(manager_addr)
        circuit.record_failure()

    def remove_circuit(self, manager_addr: tuple[str, int]) -> None:
        """
        Remove a circuit breaker for a manager (e.g., when manager is removed).

        Args:
            manager_addr: (host, port) tuple for the manager.
        """
        self._circuits.pop(manager_addr, None)

    def clear_all(self) -> None:
        """Clear all circuit breakers."""
        self._circuits.clear()
