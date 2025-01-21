import uuid

from hyperscale.reporting.common.results_types import MetricType
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)
from typing import Literal, Dict, Callable

try:
    from aio_statsd import GraphiteClient
    from .graphite_config import GraphiteConfig

    has_connector = True

except Exception:
    has_connector = False

    class GraphiteConfig:
        host: str = ""
        port: int = 0

    class GraphiteClient:
        pass


GraphiteMetricType = Literal[
    "increment",
    "gauge",
    "timing",
    "count",
    "sets",
]


class Graphite:
    def __init__(self, config: GraphiteConfig) -> None:
        self.host = config.host
        self.port = config.port

        self.connection = GraphiteClient(host=config.host, port=config.port)

        self._types_map: Dict[
            MetricType,
            GraphiteMetricType,
        ] = {
            "COUNT": "count",
            "DISTRIBUTION": "gauge",
            "RATE": "gauge",
            "TIMING": "timer",
            "SAMPLE": "gauge",
        }

        self._update_map: Dict[
            GraphiteMetricType, Callable[[str, int | float], None]
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
        self.reporter_type = ReporterTypes.Graphite
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

        self.statsd_type = "Graphite"

    async def connect(self):
        await self.connection.connect()

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        for result in workflow_results:
            metric_name = result.get("metric_name")
            metric_workflow = result.get("metric_workflow")
            metric_value = result.get("metric_value")

            self.connection.send_graphite(
                f"{metric_workflow}_{metric_name}",
                metric_value,
            )

    async def submit_step_results(self, step_results: StepMetricSet):
        for result in step_results:
            metric_name = result.get("metric_name")
            metric_workflow = result.get("metric_workflow")
            metric_step = result.get("metric_step")
            metric_value = result.get("metric_value")

            self.connection.send_graphite(
                f"{metric_workflow}_{metric_step}_{metric_name}",
                metric_value,
            )

    async def close(self):
        await self.connection.close()
