"""``ManagerState.get_gate_metrics`` -- the gates a manager knows of and their health."""

from __future__ import annotations

from typing import TypedDict


class ManagerGateMetrics(TypedDict):
    """Known, healthy and unhealthy gate counts, and whether a gate leader is known."""

    known_gate_count: int
    healthy_gate_count: int
    unhealthy_gate_count: int
    has_gate_leader: bool
