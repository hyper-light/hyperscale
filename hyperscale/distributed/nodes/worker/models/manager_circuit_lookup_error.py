"""``WorkerRegistry.get_circuit_status(manager_id)`` for a manager with no circuit breaker."""

from __future__ import annotations

from typing import TypedDict


class ManagerCircuitLookupError(TypedDict):
    """Why no circuit status could be given."""

    error: str
