import uuid
from typing import List

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.reporting.metric import MetricsSet, MetricType

try:
    from aio_statsd import DogStatsdClient

    from hyperscale.reporting.types.statsd import StatsD

    from .dogstatsd_config import DogStatsDConfig

    has_connector = True

except Exception:
    from hyperscale.reporting.types.empty import Empty as StatsD

    has_connector = False

    class DogStatsDConfig:
        pass

    class DogStatsdClient:
        pass


class DogStatsD(StatsD):
    def __init__(self, config: DogStatsDConfig) -> None:
        super(DogStatsD, self).__init__(config)

        self.host = config.host
        self.port = config.port

        self.connection = DogStatsdClient(host=self.host, port=self.port)

        self.types_map = {
            "total": "increment",
            "succeeded": "increment",
            "failed": "increment",
            "actions_per_second": "histogram",
            "median": "gauge",
            "mean": "gauge",
            "variance": "gauge",
            "stdev": "gauge",
            "minimum": "gauge",
            "maximum": "gauge",
            "quantiles": "gauge",
        }

        self.stat_type_map = {
            MetricType.COUNT: "count",
            MetricType.DISTRIBUTION: "histogram",
            MetricType.RATE: "gauge",
            MetricType.SAMPLE: "gauge",
        }

        self._update_map = {
            "count": lambda: NotImplementedError("DogStatsD does not support counts."),
            "gauge": self.connection.gauge,
            "sets": lambda: NotImplementedError("DogStatsD does not support sets."),
            "increment": self.connection.increment,
            "histogram": self.connection.histogram,
            "distribution": self.connection.distribution,
            "timer": self.connection.timer,
        }

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.statsd_type = "StatsD"

    async def submit_metrics(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Custom Metrics to {self.statsd_type}"
        )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for custom_metric_name, custom_metric in metrics_set.custom_metrics.items():
                metric_type = self.stat_type_map.get(custom_metric.metric_type, "gauge")

                update_function = self._update_map.get(metric_type)
                update_function(
                    f"{metrics_set.name}_{custom_metric_name}",
                    custom_metric.metric_value,
                )

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Custom Metrics to {self.statsd_type}"
        )
