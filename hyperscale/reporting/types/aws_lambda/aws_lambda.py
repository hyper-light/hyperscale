import asyncio
import functools
import json
import signal
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Union

import psutil

from hyperscale.reporting.metric import MetricsSet
from hyperscale.reporting.types import ReporterTypes

from .aws_lambda_config import AWSLambdaConfig

try:
    import boto3

    has_connector = True
except Exception:
    boto3 = object
    has_connector = False


def handle_loop_stop(
    signame, executor: ThreadPoolExecutor, loop: asyncio.AbstractEventLoop
):
    try:
        executor.shutdown(wait=False, cancel_futures=True)
        loop.stop()
    except Exception:
        pass


class AWSLambda:
    def __init__(self, config: AWSLambdaConfig) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name

        self.events_lambda_name = config.events_lambda
        self.streams_lambda_name = config.streams_lambda

        self.metrics_lambda_name = config.metrics_lambda
        self.shared_metrics_lambda_name = f"{config.metrics_lambda}_shared"
        self.error_metrics_lambda_name = f"{config.metrics_lambda}_error"
        self.system_metrics_lambda_name = config.system_metrics_lambda

        self.experiments_lambda_name = config.experiments_lambda
        self.variants_lambda_name = f"{config.experiments_lambda}_variants"
        self.mutations_lambda_name = f"{config.experiments_lambda}_mutations"

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._client = None
        self._loop = asyncio.get_event_loop()
        self.session_uuid = str(uuid.uuid4())

        self.reporter_type = ReporterTypes.AWSLambda
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame, self._executor, self._loop
                ),
            )

        self._client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                boto3.client,
                "lambda",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            ),
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.shared_metrics_lambda_name,
                Payload=json.dumps(
                    [
                        {
                            "name": metrics_set.name,
                            "stage": metrics_set.stage,
                            "group": "common",
                            **metrics_set.common_stats,
                        }
                        for metrics_set in metrics_sets
                    ]
                ),
            ),
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        for metrics_set in metrics:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._client.invoke,
                    FunctionName=self.metrics_lambda_name,
                    Payload=json.dumps(
                        [
                            {"group": group_name, **group.record, **group.custom}
                            for group_name, group in metrics_set.groups.items()
                        ]
                    ),
                ),
            )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.metrics_lambda_name,
                Payload=json.dumps(
                    [
                        {
                            "name": metrics_set.name,
                            "stage": metrics_set.stage,
                            "group": "custom",
                            **{
                                metric.metric_shortname: metric.metric_value
                                for metric in metrics_set.custom_metrics.values()
                            },
                        }
                        for metrics_set in metrics_sets
                    ]
                ),
            ),
        )

    async def submit_errors(self, metrics: List[MetricsSet]):
        for metrics_set in metrics:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._client.invoke,
                    FunctionName=self.error_metrics_lambda_name,
                    Payload=json.dumps(
                        [
                            {
                                "name": metrics_set.name,
                                "stage": metrics_set.stage,
                                **error,
                            }
                            for error in metrics_set.errors
                        ]
                    ),
                ),
            )

    async def close(self):
        self._executor.shutdown(cancel_futures=True)
