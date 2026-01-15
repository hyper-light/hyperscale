from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from time import monotonic

import psutil

from hyperscale.distributed.resources.adaptive_kalman_filter import AdaptiveKalmanFilter
from hyperscale.distributed.resources.resource_metrics import ResourceMetrics


@dataclass(slots=True)
class ProcessResourceMonitor:
    """Monitor resource usage for a process tree with Kalman filtering."""

    root_pid: int = field(default_factory=os.getpid)
    cpu_process_noise: float = 15.0
    cpu_measurement_noise: float = 50.0
    memory_process_noise: float = 1e6
    memory_measurement_noise: float = 1e7

    _process: psutil.Process | None = field(default=None, init=False)
    _cpu_filter: AdaptiveKalmanFilter = field(init=False)
    _memory_filter: AdaptiveKalmanFilter = field(init=False)
    _last_metrics: ResourceMetrics | None = field(default=None, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _total_memory: int = field(default=0, init=False)
    _cpu_count: int = field(default=1, init=False)

    def __post_init__(self) -> None:
        try:
            self._process = psutil.Process(self.root_pid)
        except psutil.NoSuchProcess:
            self._process = None

        self._cpu_filter = AdaptiveKalmanFilter(
            initial_process_noise=self.cpu_process_noise,
            initial_measurement_noise=self.cpu_measurement_noise,
        )
        self._memory_filter = AdaptiveKalmanFilter(
            initial_process_noise=self.memory_process_noise,
            initial_measurement_noise=self.memory_measurement_noise,
        )

        self._total_memory = psutil.virtual_memory().total
        self._cpu_count = psutil.cpu_count() or 1

    async def sample(self) -> ResourceMetrics:
        """Sample the process tree and return filtered metrics."""
        async with self._lock:
            return await asyncio.to_thread(self._sample_sync)

    def get_last_metrics(self) -> ResourceMetrics | None:
        """Return the last successful metrics sample."""
        return self._last_metrics

    def get_system_info(self) -> tuple[int, int]:
        """Return the total system memory and CPU count."""
        return self._total_memory, self._cpu_count

    def _sample_sync(self) -> ResourceMetrics:
        if self._process is None:
            return self._empty_metrics()

        try:
            processes = self._collect_processes()
            raw_cpu, raw_memory, total_fds, live_count = self._aggregate_samples(
                processes
            )
            metrics = self._build_metrics(raw_cpu, raw_memory, total_fds, live_count)
            self._last_metrics = metrics
            return metrics
        except psutil.NoSuchProcess:
            return (
                self._last_metrics
                if self._last_metrics is not None
                else self._empty_metrics()
            )

    def _collect_processes(self) -> list[psutil.Process]:
        children = self._process.children(recursive=True)
        return [self._process] + children

    def _aggregate_samples(
        self, processes: list[psutil.Process]
    ) -> tuple[float, int, int, int]:
        raw_cpu = 0.0
        raw_memory = 0
        total_fds = 0
        live_count = 0

        for process in processes:
            sample = self._sample_process(process)
            if sample is None:
                continue
            cpu, memory, file_descriptors = sample
            raw_cpu += cpu
            raw_memory += memory
            total_fds += file_descriptors
            live_count += 1

        return raw_cpu, raw_memory, total_fds, live_count

    def _sample_process(self, process: psutil.Process) -> tuple[float, int, int] | None:
        try:
            cpu = process.cpu_percent(interval=None)
            mem_info = process.memory_info()
            file_descriptors = self._get_file_descriptors(process)
            return cpu, mem_info.rss, file_descriptors
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return None

    def _get_file_descriptors(self, process: psutil.Process) -> int:
        try:
            return process.num_fds()
        except (psutil.AccessDenied, AttributeError):
            return 0

    def _build_metrics(
        self,
        raw_cpu: float,
        raw_memory: int,
        total_fds: int,
        live_count: int,
    ) -> ResourceMetrics:
        cpu_estimate, cpu_uncertainty = self._cpu_filter.update(raw_cpu)
        memory_estimate, memory_uncertainty = self._memory_filter.update(
            float(raw_memory)
        )

        cpu_estimate = max(0.0, cpu_estimate)
        memory_estimate = max(0.0, memory_estimate)

        memory_percent = 0.0
        if self._total_memory > 0:
            memory_percent = (memory_estimate / self._total_memory) * 100.0

        return ResourceMetrics(
            cpu_percent=cpu_estimate,
            cpu_uncertainty=cpu_uncertainty,
            memory_bytes=int(memory_estimate),
            memory_uncertainty=memory_uncertainty,
            memory_percent=memory_percent,
            file_descriptor_count=total_fds,
            timestamp_monotonic=monotonic(),
            sample_count=self._cpu_filter.get_sample_count(),
            process_count=live_count,
        )

    def _empty_metrics(self) -> ResourceMetrics:
        return ResourceMetrics(
            cpu_percent=0.0,
            cpu_uncertainty=0.0,
            memory_bytes=0,
            memory_uncertainty=0.0,
            memory_percent=0.0,
            file_descriptor_count=0,
            timestamp_monotonic=monotonic(),
            sample_count=0,
            process_count=0,
        )
