"""``WorkerRegistry.get_circuit_status(manager_id)`` -- one manager's circuit breaker."""

from __future__ import annotations

from typing import TypedDict


class ManagerCircuitStatus(TypedDict):
    """The manager, its circuit state name, and the errors in the breaker's window."""

    manager_id: str
    circuit_state: str
    error_count: int
    error_rate: float
