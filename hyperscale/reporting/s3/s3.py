import asyncio
import functools
import json
import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .s3_config import S3Config

try:
    import boto3

    has_connector = True

except Exception:
    boto3 = object
    has_connector = False


class S3:
    def __init__(self, config: S3Config) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name

        self._workflow_results_bucket_name = config.workflow_results_bucket_name
        self._step_results_bucket_name = config.step_results_bucket_name

        self.client = None

        self._loop = asyncio.get_event_loop()
        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.S3
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                boto3.client,
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            ),
        )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        try:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=self._workflow_results_bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region_name},
                ),
            )

        except Exception:
            pass

        metric_workflow = workflow_results[0].get("metric_workflow")
        metric_name = workflow_results[0].get("metric_name")

        workflow_results_key = f"{metric_workflow}_{metric_name}_{self.session_uuid}"

        await asyncio.gather(
            *[
                self._loop.run_in_executor(
                    None,
                    functools.partial(
                        self.client.put_object,
                        Bucket=self._workflow_results_bucket_name,
                        Key=workflow_results_key,
                        Body=json.dumps(workflow_results),
                    ),
                )
            ]
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        try:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=self._step_results_bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region_name},
                ),
            )

        except Exception:
            pass

        metric_workflow = step_results[0].get("metric_workflow")
        metric_step = step_results[0].get("metric_step")
        metric_name = step_results[0].get("metric_name")

        step_results_key = (
            f"{metric_workflow}_{metric_step}_{metric_name}_{self.session_uuid}"
        )

        await asyncio.gather(
            *[
                self._loop.run_in_executor(
                    None,
                    functools.partial(
                        self.client.put_object,
                        Bucket=self._step_results_bucket_name,
                        Key=step_results_key,
                        Body=json.dumps(step_results),
                    ),
                )
            ]
        )

    async def close(self):
        pass
