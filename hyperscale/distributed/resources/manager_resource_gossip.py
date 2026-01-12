from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from time import monotonic

from hyperscale.distributed.resources.manager_cluster_view import (
    ManagerClusterResourceView,
)
from hyperscale.distributed.resources.manager_local_view import ManagerLocalView
from hyperscale.distributed.resources.process_resource_monitor import (
    ProcessResourceMonitor,
)
from hyperscale.distributed.resources.resource_metrics import ResourceMetrics
from hyperscale.distributed.resources.worker_resource_report import WorkerResourceReport
from hyperscale.logging import Logger


@dataclass(slots=True)
class ManagerResourceGossip:
    """Collect, gossip, and aggregate resource views for a manager."""

    node_id: str
    datacenter: str
    logger: Logger | None = None
    staleness_threshold_seconds: float = 30.0

    _self_monitor: ProcessResourceMonitor = field(init=False)
    _self_metrics: ResourceMetrics | None = field(default=None, init=False)
    _worker_reports: dict[str, WorkerResourceReport] = field(
        default_factory=dict, init=False
    )
    _worker_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _peer_views: dict[str, tuple[ManagerLocalView, float]] = field(
        default_factory=dict, init=False
    )
    _peer_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _version: int = field(default=0, init=False)
    _cached_local_view: ManagerLocalView | None = field(default=None, init=False)
    _cached_cluster_view: ManagerClusterResourceView | None = field(
        default=None, init=False
    )

    def __post_init__(self) -> None:
        self._self_monitor = ProcessResourceMonitor()

    async def sample_self(self) -> ResourceMetrics:
        """Sample this manager's resource usage."""
        self._self_metrics = await self._self_monitor.sample()
        self._cached_local_view = None
        return self._self_metrics

    async def update_worker_report(self, report: WorkerResourceReport) -> bool:
        """Update worker report from a heartbeat."""
        async with self._worker_lock:
            existing = self._worker_reports.get(report.node_id)
            if existing is None or report.version > existing.version:
                self._worker_reports[report.node_id] = report
                self._cached_local_view = None
                self._cached_cluster_view = None
                return True
            return False

    async def receive_peer_view(self, view: ManagerLocalView) -> bool:
        """Receive a peer's local view via gossip."""
        if view.manager_node_id == self.node_id:
            return False

        async with self._peer_lock:
            existing = self._peer_views.get(view.manager_node_id)
            existing_version = existing[0].version if existing else -1
            if existing is None or view.version > existing_version:
                self._peer_views[view.manager_node_id] = (view, monotonic())
                self._cached_cluster_view = None
                return True
            return False

    async def compute_local_view(self) -> ManagerLocalView:
        """Compute this manager's local view for gossiping."""
        if self._cached_local_view is not None:
            return self._cached_local_view

        async with self._worker_lock:
            if self._self_metrics is None:
                await self.sample_self()

            worker_count, worker_cpu, worker_mem, live_reports = (
                self._collect_live_reports()
            )
            self._version += 1

            local_view = ManagerLocalView(
                manager_node_id=self.node_id,
                datacenter=self.datacenter,
                self_metrics=self._self_metrics,
                worker_count=worker_count,
                worker_aggregate_cpu_percent=worker_cpu,
                worker_aggregate_memory_bytes=worker_mem,
                worker_reports=live_reports,
                version=self._version,
            )

            self._cached_local_view = local_view
            return local_view

    async def compute_cluster_view(
        self,
        total_cores_available: int = 0,
        total_cores_allocated: int = 0,
    ) -> ManagerClusterResourceView:
        """Compute the aggregated cluster view for gates."""
        if self._cached_cluster_view is not None:
            return self._cached_cluster_view

        local_view = await self.compute_local_view()
        all_views = await self._collect_peer_views(local_view)
        cluster_view = self._aggregate_views(
            all_views,
            total_cores_available=total_cores_available,
            total_cores_allocated=total_cores_allocated,
        )
        self._cached_cluster_view = cluster_view
        return cluster_view

    def _collect_live_reports(
        self,
    ) -> tuple[int, float, int, dict[str, WorkerResourceReport]]:
        worker_count = 0
        worker_cpu = 0.0
        worker_mem = 0
        live_reports: dict[str, WorkerResourceReport] = {}

        for worker_id, report in self._worker_reports.items():
            if report.is_stale(self.staleness_threshold_seconds):
                continue
            if report.aggregate_metrics.is_stale(self.staleness_threshold_seconds):
                continue
            worker_count += 1
            worker_cpu += report.aggregate_metrics.cpu_percent
            worker_mem += report.aggregate_metrics.memory_bytes
            live_reports[worker_id] = report

        return worker_count, worker_cpu, worker_mem, live_reports

    async def _collect_peer_views(
        self,
        local_view: ManagerLocalView,
    ) -> dict[str, ManagerLocalView]:
        views: dict[str, ManagerLocalView] = {self.node_id: local_view}

        async with self._peer_lock:
            for manager_id, (view, received_at) in self._peer_views.items():
                if (monotonic() - received_at) > self.staleness_threshold_seconds:
                    continue
                views[manager_id] = view

        return views

    def _aggregate_views(
        self,
        views: dict[str, ManagerLocalView],
        total_cores_available: int,
        total_cores_allocated: int,
    ) -> ManagerClusterResourceView:
        manager_cpu = 0.0
        manager_mem = 0
        worker_count = 0
        worker_cpu = 0.0
        worker_mem = 0
        vector_clock: dict[str, int] = {}

        for manager_id, view in views.items():
            manager_cpu += view.self_metrics.cpu_percent
            manager_mem += view.self_metrics.memory_bytes
            worker_count += view.worker_count
            worker_cpu += view.worker_aggregate_cpu_percent
            worker_mem += view.worker_aggregate_memory_bytes
            vector_clock[manager_id] = view.version

        max_expected_cpu = max(1, worker_count * 400)
        cpu_pressure = min(1.0, worker_cpu / max_expected_cpu)
        memory_pressure = min(1.0, worker_mem / max(manager_mem + worker_mem, 1))

        return ManagerClusterResourceView(
            datacenter=self.datacenter,
            computing_manager_id=self.node_id,
            manager_count=len(views),
            manager_aggregate_cpu_percent=manager_cpu,
            manager_aggregate_memory_bytes=manager_mem,
            manager_views=views,
            worker_count=worker_count,
            worker_aggregate_cpu_percent=worker_cpu,
            worker_aggregate_memory_bytes=worker_mem,
            total_cores_available=total_cores_available,
            total_cores_allocated=total_cores_allocated,
            cpu_pressure=cpu_pressure,
            memory_pressure=memory_pressure,
            vector_clock=vector_clock,
            timestamp_monotonic=monotonic(),
        )
