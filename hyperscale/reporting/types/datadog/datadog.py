import uuid
from typing import Dict

from numpy import float32, float64, int16, int32, int64


from hyperscale.reporting.metric import MetricsSet, MetricType

from .datadog_config import DatadogConfig

try:
    # Datadog uses aiosonic
    from aiosonic import HTTPClient, TCPConnector, Timeouts
    from datadog_api_client import AsyncApiClient, Configuration
    from datadog_api_client.v1.api.events_api import EventsApi
    from datadog_api_client.v2.api.metrics_api import MetricPayload, MetricsApi
    from datadog_api_client.v2.model.metric_point import MetricPoint
    from datadog_api_client.v2.model.metric_series import MetricSeries

    has_connector = True

except Exception:
    has_connector = False
    datadog = object

    class HTTPClient:
        pass

    class TCPConnector:
        pass

    class Timeouts:
        pass

    class AsyncApiClient:
        pass

    class Configuration:
        pass

    class EventsApi:
        pass

    class MetricPayload:
        pass

    class MetricsApi:
        pass

    class MetricPoint:
        pass

    class MetricSeries:
        pass

from datetime import datetime
from typing import List


class Datadog:
    def __init__(self, config: DatadogConfig) -> None:
        self.datadog_api_key = config.api_key
        self.datadog_app_key = config.app_key
        self.event_alert_type = config.event_alert_type or "info"
        self.device_name = config.device_name or "hyperscale"
        self.priority = config.priority
        self.custom_fields = config.custom_fields or {}

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.types_map = {
            "total": "count",
            "succeeded": "count",
            "failed": "count",
            "actions_per_second": "gauge",
            "median": "gauge",
            "mean": "gauge",
            "variance": "gauge",
            "stdev": "gauge",
            "minimum": "gauge",
            "maximum": "gauge",
            **self.custom_fields,
        }

        self._datadog_api_map = {
            "count": 1,
            "rate": 2,
            "gauge": 3,
            "histogram": 4,
        }

        self._config = None
        self._client = None
        self.events_api = None
        self.metrics_api = None
        self.metric_types_map = {
            MetricType.COUNT: "count",
            MetricType.RATE: "rate",
            MetricType.DISTRIBUTION: "histogram",
            MetricType.SAMPLE: "gauge",
        }

    async def connect(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connecting to Datadogg API"
        )

        self._config = Configuration()
        self._config.api_key["apiKeyAuth"] = self.datadog_api_key
        self._config.api_key["appKeyAuth"] = self.datadog_app_key

        self._client = AsyncApiClient(self._config)

        # Datadog's implementation of aiosonic's HTTPClient lacks a lot
        # of configurability, incuding actually being able to set request timeouts
        # so we substitute our own implementation.

        tcp_connection = TCPConnector(timeouts=Timeouts(sock_connect=30))
        self._client.rest_client._client = HTTPClient(tcp_connection)

        self.events_api = EventsApi(self._client)
        self.metrics_api = MetricsApi(self._client)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Connected to Datadogg API"
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to Datadog API"
        )

        metrics_series = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            tags = [f"{tag.name}:{tag.value}" for tag in metrics_set.tags]

            for field, value in metrics_set.common_stats.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Creating Shared Metric - {metrics_set.name}:common:{field}"
                )

                metric_type = self.types_map.get(field)
                datadog_metric_type = self._datadog_api_map.get(metric_type)

                if isinstance(value, (int, int16, int32, int64)):
                    value = int(value)

                elif isinstance(value, (float, float32, float64)):
                    value = float(value)

                series = MetricSeries(
                    f"{metrics_set.name}_{field}",
                    [
                        MetricPoint(
                            timestamp=int(datetime.now().timestamp()), value=value
                        )
                    ],
                    type=datadog_metric_type,
                    tags=[*tags, f"metric_stage:{metrics_set.stage}", "group:common"],
                )

                metrics_series.append(series)

        await self.metrics_api.submit_metrics(MetricPayload(metrics_series))

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Shared Metrics to Datadog API"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to Datadog API"
        )

        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            tags = [f"{tag.name}:{tag.value}" for tag in metrics_set.tags]

            metrics_series = []
            for group_name, group in metrics_set.groups.items():
                for field, value in group.stats.items():
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Creating Metric - {metrics_set.name}:{group_name}:{field}"
                    )

                    metric_type = self.types_map.get(field)
                    datadog_metric_type = self._datadog_api_map.get(metric_type)

                    if isinstance(value, (int, int16, int32, int64)):
                        value = int(value)

                    elif isinstance(value, (float, float32, float64)):
                        value = float(value)

                    series = MetricSeries(
                        f"{metrics_set.name}_{field}",
                        [
                            MetricPoint(
                                timestamp=int(datetime.now().timestamp()),
                                value=float(value),
                            )
                        ],
                        type=datadog_metric_type,
                        tags=[
                            *tags,
                            f"metric_stage:{metrics_set.stage}",
                            f"group:{group_name}",
                        ],
                    )

                    metrics_series.append(series)

                for quantile_name, quantile_value in group.quantiles.items():
                    await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                        f"{self.metadata_string} - Creating Metric Quantile - {metrics_set.name}:{group_name}:{quantile_name}th Quantile"
                    )

                    if isinstance(quantile_value, (int, int16, int32, int64)):
                        quantile_value = int(quantile_value)

                    elif isinstance(quantile_value, (float, float32, float64)):
                        quantile_value = float(quantile_value)

                    datadog_metric_type = self._datadog_api_map.get("gauge")
                    series = MetricSeries(
                        f"{metrics_set.name}_{quantile_name}",
                        [
                            MetricPoint(
                                timestamp=int(datetime.now().timestamp()),
                                value=float(quantile_value),
                            )
                        ],
                        type=datadog_metric_type,
                        tags=[
                            *tags,
                            f"metric_stage:{metrics_set.stage}",
                            f"group:{group_name}",
                        ],
                    )

                    metrics_series.append(series)

            await self.metrics_api.submit_metrics(MetricPayload(metrics_series))

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Metrics to Datadog API"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Custom Metrics to Datadog API"
        )

        metrics_series = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            tags = [f"{tag.name}:{tag.value}" for tag in metrics_set.tags]

            for custom_metric_name, custom_metric in metrics_set.custom_metrics.items():
                metric_type = self.metric_types_map.get(
                    custom_metric.metric_type, "gauge"
                )

                datadog_metric_type = self._datadog_api_map.get(metric_type)

                series = MetricSeries(
                    f"{metrics_set.name}_{custom_metric_name}",
                    [
                        MetricPoint(
                            timestamp=int(datetime.now().timestamp()),
                            value=custom_metric.metric_name,
                        )
                    ],
                    type=datadog_metric_type,
                    tags=[*tags, f"metric_stage:{metrics_set.stage}", "group:custom"],
                )

                metrics_series.append(series)

        await self.metrics_api.submit_metrics(MetricPayload(metrics_series))

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Custom Metrics to Datadog API"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Error Metrics to Datadog API"
        )

        error_series = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            tags = [f"{tag.name}:{tag.value}" for tag in metrics_set.tags]

            for error in metrics_set.errors:
                error_message = error.get("error_message")

                series = MetricSeries(
                    f"{metrics_set.name}_errors",
                    [
                        MetricPoint(
                            timestamp=int(datetime.now().timestamp()),
                            value=int(error.get("count")),
                        )
                    ],
                    type=self._datadog_api_map.get("count"),
                    tags=[
                        *tags,
                        f"metric_stage:{metrics_set.stage}",
                        f"error_message:{error_message}",
                    ],
                )

                error_series.append(series)

        await self.metrics_api.submit_metrics(MetricPayload(error_series))

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Error Metrics to Datadog API"
        )

    async def close(self):
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
