import uuid
from typing import Literal, Dict, Callable

from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)
from hyperscale.reporting.common.results_types import MetricType

from .netdata_config import NetdataConfig

try:
    from aio_statsd import StatsdClient

    has_connector = True

except Exception:
    has_connector = False

    class StatsdClient:
        pass


NetdataMetricType = Literal[
    "increment",
    "gauge",
    "timing",
    "count",
    "sets",
]


class Netdata:
    def __init__(self, config: NetdataConfig) -> None:
        self.host = config.host
        self.port = config.port

        self.connection = StatsdClient(host=self.host, port=self.port)

        self._types_map: Dict[
            MetricType,
            NetdataMetricType,
        ] = {
            "COUNT": "count",
            "DISTRIBUTION": "gauge",
            "RATE": "gauge",
            "TIMING": "timer",
            "SAMPLE": "gauge",
        }

        self._update_map: Dict[
            NetdataMetricType, Callable[[str, int | float], None]
        ] = {
            "count": self.connection.counter,
            "gauge": self.connection.gauge,
            "increment": self.connection.increment,
            "sets": self.connection.sets,
            "histogram": lambda: NotImplementedError(
                "StatsD does not support histograms."
            ),
            "distribution": lambda: NotImplementedError(
                "StatsD does not support distributions."
            ),
            "timer": self.connection.timer,
        }

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.Netdata
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

        self.statsd_type = "Netdata"

    async def connect(self):
        await self.connection.connect()

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        for result in workflow_results:
            metric_name = result.get("metric_name")
            metric_workflow = result.get("metric_workflow")
            metric_value = result.get("metric_value")

            metric_type = result.get("metric_type")
            statsd_type = self._types_map.get(metric_type)

            self._update_map.get(statsd_type)(
                f"{metric_workflow}_{metric_name}",
                metric_value,
            )

    async def submit_step_results(self, step_results: StepMetricSet):
        for result in step_results:
            metric_name = result.get("metric_name")
            metric_workflow = result.get("metric_workflow")
            metric_step = result.get("metric_step")
            metric_value = result.get("metric_value")

            metric_type = result.get("metric_type")
            statsd_type = self._types_map.get(metric_type)

            self._update_map.get(statsd_type)(
                f"{metric_workflow}_{metric_step}_{metric_name}",
                metric_value,
            )

    async def close(self):
        await self.connection.close()
