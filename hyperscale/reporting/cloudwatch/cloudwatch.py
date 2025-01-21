import asyncio
import datetime
import functools
import orjson
import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .cloudwatch_config import CloudwatchConfig

try:
    import boto3

    has_connector = True

except Exception:
    boto3 = None
    has_connector = False


class Cloudwatch:
    def __init__(self, config: CloudwatchConfig) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name
        self.iam_role_arn = config.iam_role_arn
        self.schedule_rate = config.schedule_rate

        self.cloudwatch_targets = config.cloudwatch_targets
        self.aws_resource_arns = config.aws_resource_arns
        self.submit_timeout = config.submit_timeout

        self._workflow_results_rule_name = config.workflow_results_rule_name
        self._step_results_rule_name = config.step_results_rule_name

        self.session_uuid = str(uuid.uuid4())

        self.client = None
        self._loop = asyncio.get_event_loop()
        self.reporter_type = ReporterTypes.Cloudwatch
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                boto3.client,
                "events",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            ),
        )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        await asyncio.wait_for(
            self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.put_events,
                    Entries=[
                        {
                            "Time": datetime.datetime.now(),
                            "Detail": orjson.dumps(result).decode(),
                            "DetailType": self._workflow_results_rule_name,
                            "Resources": self.aws_resource_arns,
                            "Source": self._workflow_results_rule_name,
                        }
                        for result in workflow_results
                    ],
                ),
            ),
            timeout=self.submit_timeout,
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        await asyncio.wait_for(
            self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.put_events,
                    Entries=[
                        {
                            "Time": datetime.datetime.now(),
                            "Detail": orjson.dumps(result).decode(),
                            "DetailType": self._step_results_rule_name,
                            "Resources": self.aws_resource_arns,
                            "Source": self._step_results_rule_name,
                        }
                        for result in step_results
                    ],
                ),
            ),
            timeout=self.submit_timeout,
        )

    async def close(self):
        pass
