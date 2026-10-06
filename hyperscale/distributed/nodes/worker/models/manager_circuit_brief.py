"""One manager's entry in a ``ManagerCircuitSummary``."""

from __future__ import annotations

from typing import TypedDict


class ManagerCircuitBrief(TypedDict):
    """The manager's circuit state name and the errors in the breaker's window."""

    circuit_state: str
    error_count: int
