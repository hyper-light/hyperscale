import uuid

from hyperscale.reporting.types.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet
)


try:
    from aio_statsd import GraphiteClient

    from hyperscale.reporting.types.statsd import StatsD

    from .graphite_config import GraphiteConfig

    has_connector = True

except Exception:
    from hyperscale.reporting.types.empty import Empty as StatsD

    has_connector = False

    class GraphiteConfig:
        host: str = ""
        port: int = 0

    class GraphiteClient:
        pass


class Graphite(StatsD):
    def __init__(self, config: GraphiteConfig) -> None:
        super().__init__(config)

        self.session_uuid = str(uuid.uuid4())

        self.connection = GraphiteClient(host=config.host, port=config.port)

        self.reporter_type = ReporterTypes.Graphite
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

        self.statsd_type = "Graphite"

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        for result in workflow_results:

            metric_name = result.get('metric_name')
            metric_workflow = result.get('metric_workflow')
            metric_value = result.get('metric_value')

            self.connection.send_graphite(
                f'{metric_workflow}_{metric_name}',
                metric_value,
            )


    async def submit_step_results(self, step_results: StepMetricSet):
        for result in step_results:

            metric_name = result.get('metric_name')
            metric_workflow = result.get('metric_workflow')
            metric_step = result.get('metric_step')
            metric_value = result.get('metric_value')

            self.connection.send_graphite(
                f'{metric_workflow}_{metric_step}_{metric_name}',
                metric_value,
            )
