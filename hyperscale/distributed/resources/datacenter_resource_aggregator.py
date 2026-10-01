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
    """AD-41 gate side: combine managers' resource reports per datacenter.

    Every manager reports the workload of the workflows it leads -- a
    partition of the datacenter's running workflows -- so the
    datacenter's workload is the sum over its managers' fresh reports.
    Worker capacity is the same pool on every manager, so it comes from
    the most recently received report. A report not refreshed within
    ``staleness_seconds`` (its manager died or stopped heartbeating) is
    dropped: it neither counts nor lingers.
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
    ) -> None:
        """Keep ``report`` as the manager's latest."""
        self._reports.setdefault(datacenter, {})[manager_address] = (
            report,
            self._clock.monotonic(),
        )
        self._drop_stale(datacenter)

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
