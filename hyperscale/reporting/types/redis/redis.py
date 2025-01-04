import json
import uuid
from typing import Dict, List

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.reporting.metric import MetricsSet

from .redis_config import RedisConfig

try:
    import aioredis

    has_connector = True

except Exception:
    aioredis = object
    has_connector = True


class Redis:
    def __init__(self, config: RedisConfig) -> None:
        self.host = config.host
        self.base = "rediss" if config.secure else "redis"
        self.username = config.username
        self.password = config.password
        self.database = config.database
        self.events_channel = config.events_channel
        self.metrics_channel = config.metrics_channel
        self.streams_channel = config.streams_channel

        self.experiments_channel = config.experiments_channel
        self.variants_channel = f"{config.experiments_channel}_variants"
        self.mutations_channel = f"{config.experiments_channel}_mutations"

        self.shared_metrics_channel = f"{self.metrics_channel}_metrics"
        self.errors_channel = f"{self.metrics_channel}_errors"
        self.custom_metrics_channel = f"{self.metrics_channel}_custom"
        self.session_system_metrics_channel = f"{config.system_metrics_channel}_session"
        self.stage_system_metrics_channel = f"{config.system_metrics_channel}_stage"

        self.channel_type = config.channel_type
        self.connection = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

    async def connect(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting to Redis instance at - {self.base}://{self.host} - Database: {self.database}"
        )
        self.connection = await aioredis.from_url(
            f"{self.base}://{self.host}",
            username=self.username,
            password=self.password,
            db=self.database,
        )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to Redis instance at - {self.base}://{self.host} - Database: {self.database}"
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            if self.channel_type == "channel":
                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitting Shared Metrics to Channel - {self.shared_metrics_channel}"
                )
                await self.connection.publish(
                    self.shared_metrics_channel,
                    json.dumps(
                        {
                            "name": metrics_set.name,
                            "stage": metrics_set.stage,
                            "group": "common",
                            **metrics_set.common_stats,
                        }
                    ),
                )

                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitted Shared Metrics to Channel - {self.shared_metrics_channel}"
                )

            else:
                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitting Shared Metrics to Redis Set - {self.shared_metrics_channel}"
                )
                await self.connection.sadd(
                    self.shared_metrics_channel,
                    json.dumps(
                        {
                            "name": metrics_set.name,
                            "stage": metrics_set.stage,
                            **metrics_set.common_stats,
                        }
                    ),
                )

                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitted Shared Metrics to Redis Set - {self.shared_metrics_channel}"
                )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for group_name, group in metrics_set.groups.items():
                if self.channel_type == "channel":
                    await self.logger.filesystem.aio["hyperscale.reporting"].info(
                        f"{self.metadata_string} - Submitting Metrics to Channel - {self.metrics_channel} - Group: {group_name}"
                    )
                    await self.connection.publish(
                        self.metrics_channel,
                        json.dumps({**group.record, "group": group_name}),
                    )

                    await self.logger.filesystem.aio["hyperscale.reporting"].info(
                        f"{self.metadata_string} - Submitted Metrics to Channel - {self.metrics_channel} - Group: {group_name}"
                    )

                else:
                    await self.logger.filesystem.aio["hyperscale.reporting"].info(
                        f"{self.metadata_string} - Submitting Metrics to Redis Set - {self.metrics_channel} - Group: {group_name}"
                    )
                    await self.connection.sadd(
                        self.metrics_channel,
                        json.dumps({**group.record, "group": group_name}),
                    )

                    await self.logger.filesystem.aio["hyperscale.reporting"].info(
                        f"{self.metadata_string} - Submitted Metrics to Redis Set - {self.metrics_channel} - Group: {group_name}"
                    )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            if self.channel_type == "channel":
                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitting Custom Metrics to Channel - {self.custom_metrics_channel} - Group: Custom"
                )
                await self.connection.publish(
                    self.custom_metrics_channel,
                    json.dumps(
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
                )

                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitted Custom Metrics to Channel - {self.custom_metrics_channel} - Group: Custom"
                )

            else:
                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitting Custom Metrics to Redis Set - {self.custom_metrics_channel} - Group: Custom"
                )
                await self.connection.sadd(
                    self.custom_metrics_channel,
                    json.dumps(
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
                )

                await self.logger.filesystem.aio["hyperscale.reporting"].info(
                    f"{self.metadata_string} - Submitted Custom Metrics to Redis Set - {self.custom_metrics_channel} - Group: Custom"
                )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for error in metrics_set.errors:
                if self.channel_type == "channel":
                    await self.logger.filesystem.aio["hyperscale.reporting"].info(
                        f"{self.metadata_string} - Submitting Error Metrics to Channel - {self.errors_channel}"
                    )
                    await self.connection.publish(
                        self.errors_channel,
                        json.dumps(
                            {
                                "metrics_name": metrics_set.name,
                                "metrics_stage": metrics_set.stage,
                                "errors_message": error.get("message"),
                                "errors_count": error.get("count"),
                            }
                        ),
                    )

                    await self.logger.filesystem.aio["hyperscale.reporting"].info(
                        f"{self.metadata_string} - Submitted Error Metrics to Channel - {self.errors_channel}"
                    )

                else:
                    await self.logger.filesystem.aio["hyperscale.reporting"].info(
                        f"{self.metadata_string} - Submitting Error Metrics to Reids Set - {self.errors_channel}"
                    )
                    await self.connection.sadd(
                        self.errors_channel,
                        json.dumps(
                            {
                                "metrics_name": metrics_set.name,
                                "metrics_stage": metrics_set.stage,
                                "errors_message": error.get("message"),
                                "errors_count": error.get("count"),
                            }
                        ),
                    )

                    await self.logger.filesystem.aio["hyperscale.reporting"].info(
                        f"{self.metadata_string} - Submitted Error Metrics to Reids Set - {self.errors_channel}"
                    )

    async def close(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closing connectiion to Redis at - {self.base}://{self.host}"
        )

        await self.connection.close()

        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Session Closed - {self.session_uuid}"
        )
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Closed connectiion to Redis at - {self.base}://{self.host}"
        )
