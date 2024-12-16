import itertools
import os

import psutil

from hyperscale.monitoring.base.monitor import BaseMonitor


class MemoryMonitor(BaseMonitor):
    def __init__(self) -> None:
        super().__init__()
        self.total_memory = psutil.virtual_memory().total

    def update_monitor(self, monitor_name: str):
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()

        self.active[monitor_name].append(mem_info.rss)

    def aggregate_worker_stats(self):
        monitor_stats = self._collect_worker_stats()

        for monitor_name, metrics in monitor_stats.items():
            self.collected[monitor_name] = [
                sum(memory_usage)
                for memory_usage in itertools.zip_longest(*metrics, fillvalue=0)
            ]

            self.stage_metrics[monitor_name] = self.collected[monitor_name]
