import uuid
from typing import Dict, Literal, Callable


from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)
from hyperscale.reporting.common.results_types import MetricType


try:
    from aio_statsd import TelegrafStatsdClient
    from .teleraf_statsd_config import TelegrafStatsDConfig

    has_connector = True

except Exception:

    class TelegrafStatsDConfig:
        host: str = ""
        port: int = 0

    class TelegrafStatsdClient:
        pass

    has_connector = False


TelegrafStatsDMetricType = Literal[
    "increment",
    "histogram",
    "gauge",
    "timer",
    "distribution",
]


class TelegrafStatsD:
    def __init__(self, config: TelegrafStatsDConfig) -> None:
        self.connection = TelegrafStatsdClient(host=config.host, port=config.port)

        self._types_map: Dict[
            MetricType,
            TelegrafStatsD,
        ] = {
            "COUNT": "increment",
            "DISTRIBUTION": "distribution",
            "RATE": "gauge",
            "TIMING": "timer",
            "SAMPLE": "gauge",
        }

        self._update_map: Dict[
            TelegrafStatsDMetricType,
            Callable[
                [str, int],
                None,
            ],
        ] = {
            "count": lambda: NotImplementedError(
                "TelegrafStatsD does not support counts."
            ),
            "gauge": self.connection.gauge,
            "sets": lambda: NotImplementedError(
                "TelegrafStatsD does not support sets."
            ),
            "increment": self.connection.increment,
            "histogram": self.connection.histogram,
            "distribution": self.connection.distribution,
            "timer": self.connection.timer,
        }

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.TelegrafStatsD
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

        self.statsd_type = "TelegrafStatsD"

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
