from __future__ import annotations

import math

from hyperscale.distributed.resources.datacenter_resource_view import (
    DatacenterResourceView,
)
from hyperscale.distributed.resources.manager_resource_report import (
    ManagerResourceReport,
)
from hyperscale.distributed.runtime import Clock

ManagerAddress = tuple[str, int]


class DatacenterResourceAggregator:
    """AD-41: combine managers' resource reports per datacenter -- on a
    gate, from each manager's heartbeat; on a manager, from its own report
    and its peers' gossip (``ManagerResourceGossip``), so a client running
    jobs on one datacenter without a gate sees what a gate would.

    Every manager reports the workload of the workflows it leads -- a
    partition of the datacenter's running workflows -- so the
    datacenter's workload is the sum over its managers' fresh reports.
    Worker capacity is the same pool on every manager, so it comes from
    the most recent report. A report not refreshed within
    ``staleness_seconds`` (its manager died or stopped reporting) is
    dropped: it neither counts nor lingers.

    A report can arrive second-hand (gossip forwards the reports a manager
    holds), so each is recorded with how long ago its manager made it: the
    freshest copy of a manager's report wins, and a forwarded copy ages
    from when its manager made it, not from when it was forwarded -- a dead
    manager's report cannot be kept alive by being passed around.
    """

    __slots__ = ("_clock", "_staleness_seconds", "_reports")

    def __init__(self, clock: Clock, staleness_seconds: float) -> None:
        self._clock = clock
        self._staleness_seconds = staleness_seconds
        self._reports: dict[str, dict[ManagerAddress, tuple[ManagerResourceReport, float]]] = {}

    def record(
        self,
        datacenter: str,
        manager_address: ManagerAddress,
        report: ManagerResourceReport,
        age_seconds: float,
    ) -> None:
        """Keep ``report`` -- made ``age_seconds`` ago by the manager at
        ``manager_address`` -- unless a fresher one of that manager's is
        already held."""
        reported_at = self._clock.monotonic() - age_seconds
        reports = self._reports.setdefault(datacenter, {})
        if (held := reports.get(manager_address)) is None or reported_at > held[1]:
            reports[manager_address] = (report, reported_at)
        self._drop_stale(datacenter)

    def fresh_reports(
        self,
        datacenter: str,
    ) -> list[tuple[ManagerAddress, ManagerResourceReport, float]]:
        """Every fresh report held for ``datacenter``: its manager's
        address, the report, and how long ago that manager made it."""
        self._drop_stale(datacenter)
        now = self._clock.monotonic()
        return [
            (manager_address, report, now - reported_at)
            for manager_address, (report, reported_at) in self._reports.get(datacenter, {}).items()
        ]

    def forget_datacenter(self, datacenter: str) -> None:
        self._reports.pop(datacenter, None)

    def view(self, datacenter: str) -> DatacenterResourceView | None:
        """The datacenter's pressure, or None without a fresh report that
        knows its capacity."""
        self._drop_stale(datacenter)
        if not (fresh_reports := self._reports.get(datacenter)):
            return None

        newest_report, _ = max(fresh_reports.values(), key=lambda entry: entry[1])
        if newest_report.cpu_capacity_percent <= 0.0 or newest_report.memory_capacity_bytes <= 0:
            return None

        reports = [report for report, _ in fresh_reports.values()]
        workload_cpu_percent = sum(report.workload.cpu_percent for report in reports)
        workload_memory_bytes = sum(report.workload.memory_bytes for report in reports)
        manager_metrics = [report.manager_metrics for report in reports if report.manager_metrics]
        return DatacenterResourceView(
            datacenter=datacenter,
            reporting_manager_count=len(reports),
            workload_cpu_percent=workload_cpu_percent,
            workload_cpu_uncertainty=math.sqrt(sum(report.workload.cpu_variance for report in reports)),
            workload_memory_bytes=workload_memory_bytes,
            workload_memory_uncertainty=math.sqrt(sum(report.workload.memory_variance for report in reports)),
            cpu_capacity_percent=newest_report.cpu_capacity_percent,
            memory_capacity_bytes=newest_report.memory_capacity_bytes,
            cpu_pressure=min(1.0, workload_cpu_percent / newest_report.cpu_capacity_percent),
            memory_pressure=min(1.0, workload_memory_bytes / newest_report.memory_capacity_bytes),
            manager_cpu_percent=sum(metrics.cpu_percent for metrics in manager_metrics),
            manager_memory_bytes=sum(metrics.memory_bytes for metrics in manager_metrics),
        )

    def _drop_stale(self, datacenter: str) -> None:
        if (reports := self._reports.get(datacenter)) is None:
            return
        cutoff = self._clock.monotonic() - self._staleness_seconds
        for manager_address in [
            manager_address for manager_address, (_, received_at) in reports.items() if received_at < cutoff
        ]:
            del reports[manager_address]
        if not reports:
            del self._reports[datacenter]
