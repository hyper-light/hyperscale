import itertools
import statistics

import psutil

from hyperscale.monitoring.base.monitor import BaseMonitor


class CPUMonitor(BaseMonitor):
    def __init__(self) -> None:
        super().__init__()
        self._process: psutil.Process | None = None

    def update_monitor(self, monitor_name: str):
        if self._process is None:
            self._process = psutil.Process()

        self.active[monitor_name].append(self._process.cpu_percent())

    def aggregate_worker_stats(self):
        monitor_stats = self._collect_worker_stats()

        for monitor_name, metrics in monitor_stats.items():
            self.stage_metrics[monitor_name] = [
                statistics.median(cpu_usage)
                for cpu_usage in itertools.zip_longest(*metrics, fillvalue=0)
            ]
