import asyncio
import datetime
import functools
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import psutil

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.reporting.metric import MetricsSet

from .cloudwatch_config import CloudwatchConfig

try:
    import boto3

    has_connector = True

except Exception:
    boto3 = None
    has_connector = False


class Cloudwatch:
    def __init__(self, config: CloudwatchConfig) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name
        self.iam_role_arn = config.iam_role_arn
        self.schedule_rate = config.schedule_rate

        self.cloudwatch_targets = config.cloudwatch_targets
        self.aws_resource_arns = config.aws_resource_arns
        self.submit_timeout = config.submit_timeout

        self.events_rule_name = config.events_rule
        self.metrics_rule_name = config.metrics_rule
        self.streams_rule_name = config.streams_rule

        self.shared_metrics_rule_name = f"{config.metrics_rule}_shared"
        self.errors_rule_name = f"{config.metrics_rule}_errors"

        self.experiments_rule_name = config.experiments_rule
        self.variants_rule_name = f"{config.experiments_rule}_variants"
        self.mutations_rule_name = f"{config.experiments_rule}_mutations"

        self.session_system_metrics_rule_name = f"{config.system_metrics_rule}_session"
        self.stage_system_metrics_rule_name = f"{config.system_metrics_rule}_stage"

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

        self.experiments_rule = None

        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Creating AWS Cloudwatch client for region - {self.region_name}"
        )

        self.client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                boto3.client,
                "events",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            ),
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Created AWS Cloudwatch client for region - {self.region_name}"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to Cloudwatch rule - {self.metrics_rule_name}"
        )

        metrics = []
        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for group_name, group in metrics_set.groups.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Metrics Group - {group_name}:{group.metrics_group_id}"
                )

                metrics.append(
                    {
                        "Time": datetime.datetime.now(),
                        "Detail": json.dumps({**group.record, "group": group_name}),
                        "DetailType": self.metrics_rule_name,
                        "Resources": self.aws_resource_arns,
                        "Source": self.metrics_rule_name,
                    }
                )

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(self.client.put_events, Entries=metrics),
            ),
            timeout=self.submit_timeout,
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics to Cloudwatch rule - {self.metrics_rule_name}"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Custom Metrics to Cloudwatch rule - {self.metrics_rule_name}"
        )

        custom_metrics = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Group - Custom"
            )

            custom_metrics.append(
                {
                    "Time": datetime.datetime.now(),
                    "Detail": json.dumps(
                        {
                            "name": metrics_set.name,
                            "stage": metrics_set.stage,
                            "group": "custom",
                            **{
                                custom_metric_name: custom_metric.metric_value
                                for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                            },
                        }
                    ),
                    "DetailType": self.metrics_rule_name,
                    "Resources": self.aws_resource_arns,
                    "Source": self.metrics_rule_name,
                }
            )

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(self.client.put_events, Entries=custom_metrics),
            ),
            timeout=self.submit_timeout,
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Custom Metrics to Cloudwatch rule - {self.metrics_rule_name}"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Error Metrics to Cloudwatch rule - {self.errors_rule_name}"
        )

        cloudwatch_errors = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for error in metrics_set.errors:
                cloudwatch_errors.append(
                    {
                        "Time": datetime.datetime.now(),
                        "Detail": json.dumps(error),
                        "DetailType": self.errors_rule_name,
                        "Resources": self.aws_resource_arns,
                        "Source": self.errors_rule_name,
                    }
                )

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(self.client.put_events, Entries=cloudwatch_errors),
            ),
            timeout=self.submit_timeout,
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to Cloudwatch rule - {self.errors_rule_name}"
        )

    async def close(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
