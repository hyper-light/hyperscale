import time
from typing import Callable

import psutil

from hyperscale.core.jobs.models.env import Env
from hyperscale.core.monitoring.base.monitor import BaseMonitor


class CPUMonitor(BaseMonitor):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self._process: psutil.Process | None = None
        self.limit: int | float = env.MERCURY_SYNC_PROCESS_JOB_CPU_LIMIT

    def check_lock(
        self,
        measuable: Callable[
            [int],
            int | float,
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

            # The workflow's own samples, kept for one window plus 10%
            # slack. (This iterated ``self.active[run_id]`` -- the dict of
            # workflow NAMES -- so every update raised unpacking a name
            # and was swallowed below, and the cutoff scaled the absolute
            # timestamp by 1.1 instead of the window: no sample was ever
            # recorded, and every reported average was 0.)
            cutoff_time = time.monotonic() - self._sample_window * 1.1

            self.active[run_id][workflow_name] = [
                (timestamp, sample)
                for timestamp, sample in self.active[run_id][workflow_name]
                if timestamp >= cutoff_time
            ]

            self.active[run_id][workflow_name].append(
                (time.monotonic(), self._process.cpu_percent())
            )

        except psutil.Error:
            # The measured process vanished or is unreadable: no sample
            # this tick. Anything else is a bug and must surface.
            return
