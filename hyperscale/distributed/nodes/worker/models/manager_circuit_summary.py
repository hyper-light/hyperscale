"""``WorkerRegistry.get_circuit_status()`` -- every manager's circuit breaker."""

from __future__ import annotations

from typing import TypedDict

from .manager_circuit_brief import ManagerCircuitBrief


class ManagerCircuitSummary(TypedDict):
    """Each manager's circuit by manager id, the managers whose circuit is open, the healthy
    manager count, and the primary manager."""

    managers: dict[str, ManagerCircuitBrief]
    open_circuits: list[str]
    healthy_managers: int
    primary_manager: str | None
