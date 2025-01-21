import uuid

from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)
from .telegraf_config import TelegrafConfig

try:
    from aio_statsd import TelegrafClient

    from hyperscale.reporting.types.statsd import StatsD

    has_connector = True

except Exception:

    class TelegrafClient:
        pass

    has_connector = False


class Telegraf:
    def __init__(self, config: TelegrafConfig) -> None:
        self.host = config.host
        self.port = config.port

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.Telegraf
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

        self.connection = TelegrafClient(host=self.host, port=self.port)

        self.statsd_type = "Telegraf"

    async def connect(self):
        pass

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        for result in workflow_results:
            metric_workflow = result.get("metric_workflow")
            metric_name = result.get("metric_name")

            self.connection.send_telegraf(
                f"{metric_workflow}_{metric_name}",
                result,
            )

    async def submit_step_results(self, step_results: StepMetricSet):
        for result in step_results:
            metric_workflow = result.get("metric_workflow")
            metric_step = result.get("metric_step")
            metric_name = result.get("metric_name")

            self.connection.send_telegraf(
                f"{metric_workflow}_{metric_step}_{metric_name}",
                result,
            )

    async def close(self):
        pass
