import asyncio
import functools
import orjson
import uuid

from hyperscale.reporting.common import (
    ReporterTypes,
    StepMetricSet,
    WorkflowMetricSet,
)

from .aws_lambda_config import AWSLambdaConfig

try:
    import boto3

    has_connector = True
except Exception:
    boto3 = object
    has_connector = False


class AWSLambda:
    def __init__(self, config: AWSLambdaConfig) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name

        self.workflow_results_lambda_name = config.workflow_results_lambda_name
        self.step_results_lambda_name = config.step_results_lambda_name

        self._client = None
        self._loop = asyncio.get_event_loop()
        self.session_uuid = str(uuid.uuid4())

        self.reporter_type = ReporterTypes.AWSLambda
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self._client = await self._loop.run_in_executor(
            None,
            functools.partial(
                boto3.client,
                "lambda",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            ),
        )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        await self._loop.run_in_executor(
            None,
            functools.partial(
                self._client.invoke,
                FunctionName=self.workflow_results_lambda_name,
                Payload=orjson.dumps(workflow_results).decode(),
            ),
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        await self._loop.run_in_executor(
            None,
            functools.partial(
                self._client.invoke,
                FunctionName=self.step_results_lambda_name,
                Payload=orjson.dumps(step_results).decode(),
            ),
        )

    async def close(self):
        None.shutdown(cancel_futures=True)
