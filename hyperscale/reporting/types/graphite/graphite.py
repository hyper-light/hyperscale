import re
import uuid
from typing import Dict, List

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.reporting.metric import MetricsSet

try:
    from aio_statsd import GraphiteClient

    from hyperscale.reporting.types.statsd import StatsD

    from .graphite_config import GraphiteConfig

    has_connector = True

except Exception:
    from hyperscale.reporting.types.empty import Empty as StatsD

    has_connector = False

    class GraphiteConfig:
        pass


    class GraphieClient:
        pass


class Graphite(StatsD):
    def __init__(self, config: GraphiteConfig) -> None:
        super().__init__(config)

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.connection = GraphiteClient(host=self.host, port=self.port)

        self.statsd_type = "Graphite"

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for field, value in metrics_set.common_stats.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:common:{field}"
                )

                self.connection.send_graphite(
                    f"{metrics_set.name}_common_{field}", value
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
                for metric_field, metric_value in group.stats.items():
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{metric_field}"
                    )

                    self.connection.send_graphite(
                        f"{metrics_set.name}_{group_name}_{metric_field}", metric_value
                    )

                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitted Metric - {metrics_set.name}:{group_name}:{metric_field}"
                    )

                for metric_field, metric_value in group.custom.items():
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{metric_field}"
                    )

                    self.connection.send_graphite(
                        f"{metrics_set.name}_{group_name}_{metric_field}", metric_value
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

            for custom_metric_name, custom_metric in metrics_set.custom_metrics.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Metric - {metrics_set.name}:custom:{custom_metric_name}"
                )

                self.connection.send_graphite(
                    f"{metrics_set.name}_custom_{custom_metric_name}",
                    custom_metric.metric_value,
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
                error_message = re.sub(
                    "[^0-9a-zA-Z]+", "_", error.get("message").lower()
                )

                self.connection.send_graphite(
                    f"{metrics_set.name}_error_{error_message}", error.get("count")
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to {self.statsd_type}"
        )
