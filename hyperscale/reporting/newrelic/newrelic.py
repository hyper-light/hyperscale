import asyncio
import functools
import uuid

from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)
from .newrelic_config import NewRelicConfig

try:
    has_connector = True

except Exception:
    has_connector = False

    class newrelic:
        agent = object


class NewRelic:
    def __init__(self, config: NewRelicConfig) -> None:
        self.config_path = config.config_path
        self.environment = config.environment
        self.registration_timeout = config.registration_timeout
        self.shutdown_timeout = config.shutdown_timeout or 60
        self.newrelic_application_name = config.newrelic_application_name

        self.client = None
        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.NewRelic
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        await self._loop.run_in_executor(
            None,
            functools.partial(
                newrelic.agent.initialize,
                config_file=self.config_path,
                environment=self.environment,
            ),
        )

        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                newrelic.agent.register_application,
                name=self.newrelic_application_name,
                timeout=self.registration_timeout,
            ),
        )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        for result in workflow_results:
            metric_workflow = result.get("metric_workflow")
            metric_name = result.get("metric_name")

            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.record_custom_metric,
                    f"{metric_workflow}_{metric_name}",
                    result.get("metric_value"),
                ),
            )

    async def submit_step_results(self, step_results: StepMetricSet):
        for result in step_results:
            metric_workflow = result.get("metric_workflow")
            metric_step = result.get("metric_step")
            metric_name = result.get("metric_name")

            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.record_custom_metric,
                    f"{metric_workflow}_{metric_step}_{metric_name}",
                    result.get("metric_value"),
                ),
            )

    async def close(self):
        await self._loop.run_in_executor(None, self.client.shutdown)
