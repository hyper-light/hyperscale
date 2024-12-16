import asyncio
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Awaitable, Callable, Dict, List, Union

from hyperscale.core_rewrite.engines.client.time_parser import TimeParser
from hyperscale.core_rewrite.jobs.models.env import Env


class BaseMonitor:
    def __init__(self, env: Env) -> None:
        self._sample_window: float = TimeParser(
            env.MERCURY_SYNC_MONITOR_SAMPLE_WINDOW
        ).time

        sample_interval = env.MERCURY_SYNC_MONITOR_SAMPLE_INTERVAL
        if isinstance(sample_interval, str):
            sample_interval = TimeParser(sample_interval).time

        self._sample_interval: float = sample_interval

        self.active: Dict[str, List[tuple[float, int]]] = defaultdict(list)

        self._background_monitors: Dict[str, asyncio.Future] = {}

        self._running_monitors: Dict[int, bool] = {}

        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._executor: Union[ThreadPoolExecutor, None] = None
        self._locked_runs: Dict[int, asyncio.Lock] = {}
        self.limit: int | float = 0

    def _check_lock(
        self,
        measuable: Callable[
            [int],
            Awaitable[int | float],
        ],
        run_id: int,
        limit: int | float,
    ):
        sample = measuable(run_id)
        return sample >= limit

    def release_lock(self, run_id: int):
        if self._locked_runs.get(run_id) and self._locked_runs[run_id].locked():
            self._locked_runs[run_id].release()

    async def lock(self, run_id: int):
        if self._locked_runs.get(run_id) is None:
            self._locked_runs[run_id] = asyncio.Lock()

        await self._locked_runs[run_id].acquire()

    def get_workflow_last(self, run_id: int):
        if len(self.active[run_id]) < 1:
            return 0

        _, last_amount = self.active[run_id][-1]

        return last_amount

    def get_moving_avg(self, run_id: int):
        if len(self.active[run_id]) < 1:
            return 0

        average = statistics.mean([sample for _, sample in self.active[run_id]])

        return average

    def get_moving_median(self, run_id: int):
        if len(self.active[run_id]) < 1:
            return 0

        median = statistics.median([sample for _, sample in self.active[run_id]])

        return median

    async def start_background_monitor(self, run_id: int):
        self._running_monitors[run_id] = True
        self._background_monitors[run_id] = asyncio.ensure_future(
            self._update_background_monitor(run_id)
        )

    def update_monitor(str, run_id: int) -> Union[int, float]:
        raise NotImplementedError(
            "Monitor background update method must be implemented in non-base Monitor class."
        )

    async def _update_background_monitor(self, run_id: int):
        try:
            while self._running_monitors.get(run_id):
                await asyncio.to_thread(self.update_monitor, run_id)
                await asyncio.sleep(self._sample_interval)

                if self.get_moving_median(run_id) < self.limit:
                    self.release_lock(run_id)

        except Exception:
            import traceback

            print(traceback.format_exc())

    async def stop_background_monitor(self, run_id: int):
        self._running_monitors[run_id] = False

        self.release_lock(run_id)

        if self._locked_runs.get(run_id):
            del self._locked_runs[run_id]

        if self._background_monitors.get(run_id):
            try:
                self._background_monitors[run_id].set_result(None)

            except Exception:
                pass

            del self._background_monitors[run_id]

    async def stop_all_background_monitors(self):
        if len(self.active) > 0:
            await asyncio.gather(
                *[self.stop_background_monitor(run_id) for run_id in self.active]
            )

            self.active.clear()

    def abort_all_background_monitors(self):
        for run_id, monitor in self._background_monitors.items():
            self._running_monitors[run_id] = False

            self.release_lock(run_id)

            if self._locked_runs.get(run_id):
                del self._locked_runs[run_id]

            try:
                monitor.set_result(None)

            except Exception:
                pass

            del self.active[run_id]

        self._background_monitors.clear()
