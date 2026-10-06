from __future__ import annotations

from dataclasses import dataclass

from hyperscale.distributed.resources.manager_resource_report import (
    ManagerResourceReport,
)


@dataclass(slots=True, frozen=True)
class ManagerResourceGossipEntry:
    """AD-41: one manager's resource report as gossip carries it -- the
    address of the manager that made it and how long ago it made it, as the
    sender held it."""

    manager_address: tuple[str, int]
    report: ManagerResourceReport
    age_seconds: float
