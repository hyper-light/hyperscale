import time
from typing import Awaitable, Callable

import psutil

from hyperscale.core.jobs.models.env import Env
from hyperscale.core.monitoring.base.monitor import BaseMonitor


class MemoryMonitor(BaseMonitor):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self._process: psutil.Process | None = None
        self.total_memory = psutil.virtual_memory().total
        self.limit: int | float = env.MERCURY_SYNC_PROCESS_JOB_MEMORY_LIMIT

    def check_lock(
        self,
        measuable: Callable[
            [int],
            Awaitable[int | float],
        ],
        run_id: int,
        workflow_name: str,
    ):
        return self._check_lock(
            measuable,
            run_id,
            workflow_name,
            self.limit,
        )

    def update_monitor(
        self,
        run_id: int,
        workflow_name: str,
    ):
        try:
            if self._process is None:
                self._process = psutil.Process()

            cutoff_time = (time.monotonic() - self._sample_window) * 1.1

            self.active[run_id][workflow_name] = [
                (timestamp, sample)
                for timestamp, sample in self.active[run_id]
                if timestamp >= cutoff_time
            ]

            mem_info = self._process.memory_info()

            self.active[run_id][workflow_name].append(
                (time.monotonic(), mem_info.rss / 1024**2)
            )

        except Exception:
            pass
