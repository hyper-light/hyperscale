import uuid
from collections import defaultdict
from typing import Dict

from hyperscale.reporting.common.results_types import MetricType
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)


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
from typing import List, Literal

DatadogMetricType = Literal["count", "gauge", "distribution", "rate"]


class Datadog:
    def __init__(self, config: DatadogConfig) -> None:
        self.datadog_api_key = config.api_key
        self.datadog_app_key = config.app_key
        self.device_name = config.device_name or "hyperscale"
        self.priority = config.priority

        self._types_map: Dict[MetricType, DatadogMetricType] = {
            "COUNT": "count",
            "DISTRIBUTION": "distribution",
            "RATE": "rate",
            "SAMPLE": "gauge",
            "TIMING": "gauge",
        }

        self._datadog_api_map = {
            "distribution": 0,
            "count": 1,
            "rate": 2,
            "gauge": 3,
        }

        self._config = None
        self._client = None
        self.metrics_api = None

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.Datadog
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self._config = Configuration()
        self._config.api_key["apiKeyAuth"] = self.datadog_api_key
        self._config.api_key["appKeyAuth"] = self.datadog_app_key

        self._client = AsyncApiClient(self._config)

        # Datadog's implementation of aiosonic's HTTPClient lacks a lot
        # of configurability, incuding actually being able to set request timeouts
        # so we substitute our own implementation.

        tcp_connection = TCPConnector(timeouts=Timeouts(sock_connect=30))
        self._client.rest_client._client = HTTPClient(tcp_connection)

        self.metrics_api = MetricsApi(self._client)

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        results: Dict[
            str,
            Dict[
                Literal["values", "tags", "metric_type"],
                List[str] | List[int | float] | str,
            ],
        ] = defaultdict(lambda: defaultdict(list))
        for result in workflow_results:
            metric_name = result.get("metric_name")
            metric_workflow = result.get("metric_workflow")

            metric_name = f"{metric_workflow}_{metric_name}"
            metric_group = result.get("metric_group")
            metric_value = result.get("metric_value")

            results[metric_name]["values"].append(metric_value)

            if results[metric_name].get("tags") is None:
                results[metric_name]["tags"] = [
                    f"metric_group:{metric_group}",
                ]

            if results[metric_name].get("metric_type") is None:
                metric_type = result.get("metric_type")
                datadog_type = self._datadog_api_map.get(
                    self._types_map.get(
                        metric_type,
                        "gauge",
                    ),
                    0,
                )

                results[metric_name]["metric_type"] = datadog_type

        series: List[MetricSeries] = []
        for series_name, series_data in results.items():
            series = MetricSeries(
                series_name,
                [
                    MetricPoint(timestamp=int(datetime.now().timestamp()), value=value)
                    for value in series_data.get("values")
                ],
                type=series_data.get("metric_type"),
                tags=series_data.get("tags"),
            )

            series.append(series)

        await self.metrics_api.submit_metrics(MetricPayload(series))

    async def submit_step_results(self, step_results: StepMetricSet):
        results: Dict[
            str,
            Dict[
                Literal["values", "tags", "metric_type"],
                List[str] | List[int | float] | str,
            ],
        ] = defaultdict(lambda: defaultdict(list))
        for result in step_results:
            metric_name = result.get("metric_name")
            metric_workflow = result.get("metric_workflow")
            metric_step = result.get("metric_step")

            metric_name = f"{metric_workflow}_{metric_step}_{metric_name}"
            metric_group = result.get("metric_group")
            metric_value = result.get("metric_value")

            results[metric_name]["values"].append(metric_value)

            if results[metric_name].get("tags") is None:
                results[metric_name]["tags"] = [
                    f"metric_group:{metric_group}",
                ]

            if results[metric_name].get("metric_type") is None:
                metric_type = result.get("metric_type")
                datadog_type = self._datadog_api_map.get(
                    self._types_map.get(
                        metric_type,
                        "gauge",
                    ),
                    0,
                )

                results[metric_name]["metric_type"] = datadog_type

        series: List[MetricSeries] = []
        for series_name, series_data in results.items():
            series = MetricSeries(
                series_name,
                [
                    MetricPoint(timestamp=int(datetime.now().timestamp()), value=value)
                    for value in series_data.get("values")
                ],
                type=series_data.get("metric_type"),
                tags=series_data.get("tags"),
            )

            series.append(series)

        await self.metrics_api.submit_metrics(MetricPayload(series))

    async def close(self):
        pass
