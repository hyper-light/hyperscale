import asyncio
import functools
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List

import psutil


from hyperscale.reporting.metric import MetricsSet
from .honeycomb_config import HoneycombConfig

try:
    import libhoney

    has_connector = True

except Exception:
    libhoney = object
    has_connector = False


class Honeycomb:
    def __init__(self, config: HoneycombConfig) -> None:
        self.api_key = config.api_key
        self.dataset = config.dataset
        self.client = None
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=True))
        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

    async def connect(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting to Honeycomb.IO"
        )
        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                libhoney.init, writekey=self.api_key, dataset=self.dataset
            ),
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to Honeycomb.IO"
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to Honeycomb.IO"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            group_metric = {
                "name": metrics_set.name,
                "stage": metrics_set.stage,
                **metrics_set.common_stats,
            }

            honeycomb_group_metric = libhoney.Event(data=group_metric)

            await self._loop.run_in_executor(
                self._executor, honeycomb_group_metric.send
            )

        await self._loop.run_in_executor(self._executor, libhoney.flush)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Shared Metrics to Honeycomb.IO"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to Honeycomb.IO"
        )

        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for group_name, group in metrics_set.groups.items():
                metric_record = {**group.stats, **group.custom, "group": group_name}

                honeycomb_event = libhoney.Event(data=metric_record)

                await self._loop.run_in_executor(self._executor, honeycomb_event.send)

        await self._loop.run_in_executor(self._executor, libhoney.flush)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics to Honeycomb.IO"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Custom Metrics to Honeycomb.IO"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for custm_metric_name, custom_metric in metrics_set.custom_metrics.items():
                metric_record = {
                    "name": metrics_set.name,
                    "stage": metrics_set.stage,
                    "group": "custom",
                    custm_metric_name: custom_metric.metric_value,
                }

                honeycomb_event = libhoney.Event(data=metric_record)

                await self._loop.run_in_executor(self._executor, honeycomb_event.send)

        await self._loop.run_in_executor(self._executor, libhoney.flush)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Custom Metrics to Honeycomb.IO"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Error Metrics to Honeycomb.IO"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for error in metrics_set.errors:
                error_event = libhoney.Event(
                    data={
                        "name": metrics_set.name,
                        "stage": metrics_set.stage,
                        "error_message": error.get("message"),
                        "error_count": error.get("count"),
                    }
                )

                await self._loop.run_in_executor(self._executor, error_event.send)

        await self._loop.run_in_executor(self._executor, libhoney.flush)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to Honeycomb.IO"
        )

    async def close(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closing connection Honeycomb.IO"
        )

        await self._loop.run_in_executor(self._executor, libhoney.close)

        self._executor.shutdown(wait=False, cancel_futures=True)

        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Session Closed - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closed connection Honeycomb.IO"
        )
