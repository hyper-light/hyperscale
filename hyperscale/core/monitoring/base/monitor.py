import asyncio
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Awaitable, Callable, Dict, List, Union

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.jobs.models.env import Env


class BaseMonitor:
    def __init__(self, env: Env) -> None:
        self._sample_window: float = TimeParser(
            env.MERCURY_SYNC_MONITOR_SAMPLE_WINDOW
        ).time

        sample_interval = env.MERCURY_SYNC_MONITOR_SAMPLE_INTERVAL
        if isinstance(sample_interval, str):
            sample_interval = TimeParser(sample_interval).time

        self._sample_interval: float = sample_interval

        self.active: Dict[int, Dict[str, List[tuple[float, int]]]] = defaultdict(
            lambda: defaultdict(list)
        )

        self._background_monitors: Dict[int, Dict[str, asyncio.Future]] = defaultdict(
            dict
        )

        self._running_monitors: Dict[int, Dict[str, bool]] = defaultdict(
            lambda: defaultdict(lambda: False)
        )

        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._executor: Union[ThreadPoolExecutor, None] = None
        self._locked_runs: Dict[int, Dict[str, asyncio.Lock]] = defaultdict(
            lambda: defaultdict(asyncio.Lock)
        )
        self.limit: int | float = 0

    def _check_lock(
        self,
        measuable: Callable[
            [int, str],
            Awaitable[int | float],
        ],
        run_id: int,
        workflow_name: str,
        limit: int | float,
    ):
        sample = measuable(run_id, workflow_name)
        return sample >= limit

    def release_lock(
        self,
        run_id: int,
        workflow_name: str,
    ):
        if (
            self._locked_runs.get(run_id)
            and self._locked_runs[run_id][workflow_name].locked()
        ):
            self._locked_runs[run_id][workflow_name].release()

    async def lock(
        self,
        run_id: int,
        workflow_name: str,
    ):
        if self._locked_runs[run_id].get(workflow_name) is None:
            self._locked_runs[run_id][workflow_name] = asyncio.Lock()

        await self._locked_runs[run_id][workflow_name].acquire()

    def get_workflow_last(
        self,
        run_id: int,
        workflow_name: str,
    ):
        if len(self.active[run_id][workflow_name]) < 1:
            return 0

        _, last_amount = self.active[run_id][workflow_name][-1]

        return last_amount

    def get_moving_avg(
        self,
        run_id: int,
        workflow_name: str,
    ):
        if len(self.active[run_id][workflow_name]) < 1:
            return 0

        average = statistics.mean(
            [sample for _, sample in self.active[run_id][workflow_name]]
        )

        return average

    def get_moving_median(
        self,
        run_id: int,
        workflow_name: str,
    ):
        if len(self.active[run_id][workflow_name]) < 1:
            return 0

        median = statistics.median(
            [sample for _, sample in self.active[run_id][workflow_name]]
        )

        return median

    async def start_background_monitor(
        self,
        run_id: int,
        workflow_name: str,
    ):
        self._running_monitors[run_id][workflow_name] = True
        self._background_monitors[run_id][workflow_name] = asyncio.ensure_future(
            self._update_background_monitor(run_id, workflow_name)
        )

    def update_monitor(str, _: int, __: str) -> Union[int, float]:
        raise NotImplementedError(
            "Monitor background update method must be implemented in non-base Monitor class."
        )

    async def _update_background_monitor(
        self,
        run_id: int,
        workflow_name: str,
    ):
        # The running loop, not ``self._loop``: that attribute was never
        # assigned, so the first sample raised AttributeError, the blanket
        # ``except Exception: pass`` here swallowed it, and every monitor
        # stopped before recording anything (all CPU/memory stats were 0).
        loop = asyncio.get_running_loop()
        while self._running_monitors[run_id].get(workflow_name):
            await loop.run_in_executor(
                self._executor,
                self.update_monitor,
                run_id,
                workflow_name,
            )
            await asyncio.sleep(self._sample_interval)

            if (
                self.get_moving_median(
                    run_id,
                    workflow_name,
                )
                < self.limit
            ):
                self.release_lock(run_id, workflow_name)

    async def stop_background_monitor(
        self,
        run_id: int,
        workflow_name: str,
    ):
        self._running_monitors[run_id][workflow_name] = False

        self.release_lock(run_id, workflow_name)

        if self._locked_runs[run_id].get(workflow_name):
            del self._locked_runs[run_id][workflow_name]

        # Cancel and await the sampling task (``set_result`` on a Task
        # always raised, was swallowed, and left the task running until
        # its next flag check). Awaiting surfaces a genuine failure of the
        # sampling loop instead of discarding it.
        if (monitor := self._background_monitors[run_id].pop(workflow_name, None)) is not None:
            monitor.cancel()
            try:
                await monitor
            except asyncio.CancelledError:
                pass

    def release_run(
        self,
        run_id: int,
        workflow_name: str,
    ):
        """
        Drop everything kept for the workflow's run -- its samples, its
        flags and its lock -- once nothing reads them. The run's
        sampler is stopped by then (stop_background_monitor); one still
        running is cancelled.
        """
        if (monitor := self._background_monitors.get(run_id, {}).pop(workflow_name, None)) is not None:
            monitor.cancel()

        for run_states in (
            self.active,
            self._running_monitors,
            self._locked_runs,
            self._background_monitors,
        ):
            if (workflow_states := run_states.get(run_id)) is not None:
                workflow_states.pop(workflow_name, None)
                if not workflow_states:
                    del run_states[run_id]

    async def stop_all_background_monitors(self):
        if len(self.active) > 0:
            await asyncio.gather(
                *[
                    self.stop_background_monitor(
                        run_id,
                        workflow_name,
                    )
                    for run_id, workflow_monitors in self.active.items()
                    for workflow_name in workflow_monitors
                ]
            )

            self.active.clear()

    def abort_all_background_monitors(self):
        for run_id, workflow_monitors in self._background_monitors.items():
            for workflow_name, monitor in workflow_monitors.items():
                self._running_monitors[run_id][workflow_name] = False

                self.release_lock(run_id, workflow_name)

                if self._locked_runs[run_id].get(workflow_name):
                    del self._locked_runs[run_id][workflow_name]

                monitor.cancel()

                del self.active[run_id][workflow_name]

        self._background_monitors.clear()
