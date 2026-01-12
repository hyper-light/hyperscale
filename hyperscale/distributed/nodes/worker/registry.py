"""
Worker registry module.

Handles manager registration, health tracking, and peer management.
"""

import asyncio
import time
from typing import TYPE_CHECKING

from hyperscale.distributed.models import ManagerInfo
from hyperscale.distributed.swim.core import ErrorStats, CircuitState

if TYPE_CHECKING:
    from hyperscale.logging import Logger


class WorkerRegistry:
    """
    Manages manager registration and health tracking for worker.

    Handles registration with managers, tracks health status,
    and manages circuit breakers for communication failures.
    """

    def __init__(
        self,
        logger: "Logger",
        recovery_jitter_min: float = 0.0,
        recovery_jitter_max: float = 1.0,
        recovery_semaphore_size: int = 5,
    ) -> None:
        """
        Initialize worker registry.

        Args:
            logger: Logger instance for logging
            recovery_jitter_min: Minimum jitter for recovery operations
            recovery_jitter_max: Maximum jitter for recovery operations
            recovery_semaphore_size: Concurrent recovery limit
        """
        self._logger = logger
        self._recovery_jitter_min = recovery_jitter_min
        self._recovery_jitter_max = recovery_jitter_max
        self._recovery_semaphore = asyncio.Semaphore(recovery_semaphore_size)

        # Manager tracking
        self._known_managers: dict[str, ManagerInfo] = {}
        self._healthy_manager_ids: set[str] = set()
        self._primary_manager_id: str | None = None
        self._manager_unhealthy_since: dict[str, float] = {}

        # Circuit breakers per manager
        self._manager_circuits: dict[str, ErrorStats] = {}
        self._manager_addr_circuits: dict[tuple[str, int], ErrorStats] = {}

        # State management
        self._manager_state_locks: dict[str, asyncio.Lock] = {}
        self._manager_state_epoch: dict[str, int] = {}

    def add_manager(self, manager_id: str, manager_info: ManagerInfo) -> None:
        """Add or update a known manager."""
        self._known_managers[manager_id] = manager_info

    def get_manager(self, manager_id: str) -> ManagerInfo | None:
        """Get manager info by ID."""
        return self._known_managers.get(manager_id)

    def get_manager_by_addr(self, addr: tuple[str, int]) -> ManagerInfo | None:
        """Get manager info by TCP address."""
        for manager in self._known_managers.values():
            if (manager.tcp_host, manager.tcp_port) == addr:
                return manager
        return None

    def mark_manager_healthy(self, manager_id: str) -> None:
        """Mark a manager as healthy."""
        self._healthy_manager_ids.add(manager_id)
        self._manager_unhealthy_since.pop(manager_id, None)

    def mark_manager_unhealthy(self, manager_id: str) -> None:
        """Mark a manager as unhealthy."""
        self._healthy_manager_ids.discard(manager_id)
        if manager_id not in self._manager_unhealthy_since:
            self._manager_unhealthy_since[manager_id] = time.monotonic()

    def is_manager_healthy(self, manager_id: str) -> bool:
        """Check if a manager is healthy."""
        return manager_id in self._healthy_manager_ids

    def get_healthy_manager_tcp_addrs(self) -> list[tuple[str, int]]:
        """Get TCP addresses of all healthy managers."""
        return [
            (manager.tcp_host, manager.tcp_port)
            for manager_id in self._healthy_manager_ids
            if (manager := self._known_managers.get(manager_id))
        ]

    def get_primary_manager_tcp_addr(self) -> tuple[str, int] | None:
        """Get TCP address of the primary manager."""
        if not self._primary_manager_id:
            return None
        if manager := self._known_managers.get(self._primary_manager_id):
            return (manager.tcp_host, manager.tcp_port)
        return None

    def set_primary_manager(self, manager_id: str | None) -> None:
        """Set the primary manager."""
        self._primary_manager_id = manager_id

    def get_or_create_manager_lock(self, manager_id: str) -> asyncio.Lock:
        """Get or create a state lock for a manager."""
        return self._manager_state_locks.setdefault(manager_id, asyncio.Lock())

    def increment_manager_epoch(self, manager_id: str) -> int:
        """Increment and return the epoch for a manager."""
        current = self._manager_state_epoch.get(manager_id, 0)
        self._manager_state_epoch[manager_id] = current + 1
        return self._manager_state_epoch[manager_id]

    def get_manager_epoch(self, manager_id: str) -> int:
        """Get current epoch for a manager."""
        return self._manager_state_epoch.get(manager_id, 0)

    def get_or_create_circuit(
        self,
        manager_id: str,
        error_threshold: int = 5,
        error_rate_threshold: float = 0.5,
        half_open_after: float = 30.0,
    ) -> ErrorStats:
        """Get or create a circuit breaker for a manager."""
        if manager_id not in self._manager_circuits:
            self._manager_circuits[manager_id] = ErrorStats(
                error_threshold=error_threshold,
                error_rate_threshold=error_rate_threshold,
                half_open_after=half_open_after,
            )
        return self._manager_circuits[manager_id]

    def get_or_create_circuit_by_addr(
        self,
        addr: tuple[str, int],
        error_threshold: int = 5,
        error_rate_threshold: float = 0.5,
        half_open_after: float = 30.0,
    ) -> ErrorStats:
        """Get or create a circuit breaker by manager address."""
        if addr not in self._manager_addr_circuits:
            self._manager_addr_circuits[addr] = ErrorStats(
                error_threshold=error_threshold,
                error_rate_threshold=error_rate_threshold,
                half_open_after=half_open_after,
            )
        return self._manager_addr_circuits[addr]

    def is_circuit_open(self, manager_id: str) -> bool:
        """Check if a manager's circuit breaker is open."""
        if circuit := self._manager_circuits.get(manager_id):
            return circuit.circuit_state == CircuitState.OPEN
        return False

    def is_circuit_open_by_addr(self, addr: tuple[str, int]) -> bool:
        """Check if a manager's circuit breaker is open by address."""
        if circuit := self._manager_addr_circuits.get(addr):
            return circuit.circuit_state == CircuitState.OPEN
        return False

    def get_circuit_status(self, manager_id: str | None = None) -> dict:
        """Get circuit breaker status for a specific manager or summary."""
        if manager_id:
            if not (circuit := self._manager_circuits.get(manager_id)):
                return {"error": f"No circuit breaker for manager {manager_id}"}
            return {
                "manager_id": manager_id,
                "circuit_state": circuit.circuit_state.name,
                "error_count": circuit.error_count,
                "error_rate": circuit.error_rate,
            }

        return {
            "managers": {
                mid: {
                    "circuit_state": cb.circuit_state.name,
                    "error_count": cb.error_count,
                }
                for mid, cb in self._manager_circuits.items()
            },
            "open_circuits": [
                mid
                for mid, cb in self._manager_circuits.items()
                if cb.circuit_state == CircuitState.OPEN
            ],
            "healthy_managers": len(self._healthy_manager_ids),
            "primary_manager": self._primary_manager_id,
        }

    async def select_new_primary_manager(self) -> str | None:
        """
        Select a new primary manager from healthy managers.

        Prefers the leader if known, otherwise picks any healthy manager.

        Returns:
            Selected manager ID or None
        """
        # Prefer the leader if we know one
        for manager_id in self._healthy_manager_ids:
            if manager := self._known_managers.get(manager_id):
                if manager.is_leader:
                    self._primary_manager_id = manager_id
                    return manager_id

        # Otherwise pick any healthy manager
        if self._healthy_manager_ids:
            self._primary_manager_id = next(iter(self._healthy_manager_ids))
            return self._primary_manager_id

        self._primary_manager_id = None
        return None

    def find_manager_by_udp_addr(self, udp_addr: tuple[str, int]) -> str | None:
        """Find manager ID by UDP address."""
        for manager_id, manager in self._known_managers.items():
            if (manager.udp_host, manager.udp_port) == udp_addr:
                return manager_id
        return None
