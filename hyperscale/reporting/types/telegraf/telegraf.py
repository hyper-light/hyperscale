import uuid
from typing import Dict, List

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.reporting.metric import MetricsSet

try:
    from aio_statsd import TelegrafClient

    from hyperscale.reporting.types.statsd import StatsD

    from .telegraf_config import TelegrafConfig

    has_connector = True

except Exception:
    from hyperscale.reporting.types.empty import Empty as StatsD

    class TelegrafConfig:
        pass

    has_connector = False


class Telegraf(StatsD):
    def __init__(self, config: TelegrafConfig) -> None:
        self.host = config.host
        self.port = config.port

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.connection = TelegrafClient(host=self.host, port=self.port)

        self.statsd_type = "Telegraf"

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            self.connection.send_telegraf(
                f"{metrics_set.name}_common",
                {
                    "name": metrics_set.name,
                    "stage": metrics_set.stage,
                    "group": "common",
                    **metrics_set.common_stats,
                },
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Shared Metrics to {self.statsd_type}"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for group_name, group in metrics_set.groups.items():
                self.connection.send_telegraf(
                    f"{metrics_set.name}_{group_name}",
                    {**group.record, "group": group_name},
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Metrics to {self.statsd_type}"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Custom Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            self.connection.send_telegraf(
                f"{metrics_set.name}_custom",
                {
                    "name": metrics_set.name,
                    "stage": metrics_set.stage,
                    "group": "custom",
                    **{
                        custom_metric_name: custom_metric.metric_value
                        for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                    },
                },
            )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Custom Metrics to {self.statsd_type}"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Error Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for error in metrics_set.errors:
                self.connection.send_telegraf(
                    f"{metrics_set.name}_errors",
                    {
                        "name": metrics_set.name,
                        "stage": metrics_set.stage,
                        "error_message": error.get("message"),
                        "error_count": error.get("count"),
                    },
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to {self.statsd_type}"
        )
