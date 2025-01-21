import asyncio
import json
import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)


from .google_cloud_storage_config import GoogleCloudStorageConfig


try:
    from google.cloud import storage

    has_connector = True

except Exception:
    has_connector = False
    storage = object


class GoogleCloudStorage:
    def __init__(self, config: GoogleCloudStorageConfig) -> None:
        self.service_account_json_path = config.service_account_json_path

        self.bucket_namespace = config.bucket_namespace
        self._workflow_results_bucket_name = config.workflow_results_bucket_name
        self._step_results_bucket_name = config.step_results_bucket_name

        self.credentials = None
        self.client = None

        self._workflow_results_bucket = None
        self._step_results_bucket = None

        self.session_uuid = str(uuid.uuid4())
        self._loop = asyncio.get_event_loop()
        self.reporter_type = ReporterTypes.GCS
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self.client = await self._loop.run_in_executor(
            None,
            storage.Client.from_service_account_json,
            self.service_account_json_path,
        )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        try:
            self._workflow_results_bucket = await self._loop.run_in_executor(
                None,
                self.client.get_bucket,
                f"{self.bucket_namespace}_{self._workflow_results_bucket_name}",
            )

        except Exception:
            self._workflow_results_bucket = await self._loop.run_in_executor(
                None,
                self.client.create_bucket,
                f"{self.bucket_namespace}_{self._workflow_results_bucket_name}",
            )

        metric_workflow = workflow_results[0].get("metric_workflow")

        blob = await self._loop.run_in_executor(
            None,
            self._workflow_results_bucket.blob,
            f"{metric_workflow}_{self.session_uuid}",
        )

        await self._loop.run_in_executor(
            None,
            blob.upload_from_string,
            json.dumps(workflow_results),
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        try:
            self._step_results_bucket = await self._loop.run_in_executor(
                None,
                self.client.get_bucket,
                f"{self.bucket_namespace}_{self._step_results_bucket_name}",
            )

        except Exception:
            self._step_results_bucket = await self._loop.run_in_executor(
                None,
                self.client.create_bucket,
                f"{self.bucket_namespace}_{self._step_results_bucket_name}",
            )

        metric_workflow = step_results[0].get("metric_workflow")
        metric_step = step_results[0].get("metric_step")

        blob = await self._loop.run_in_executor(
            None,
            self._step_results_bucket.blob,
            f"{metric_workflow}_{metric_step}_{self.session_uuid}",
        )

        await self._loop.run_in_executor(
            None,
            blob.upload_from_string,
            json.dumps(step_results),
        )

    async def close(self):
        await self._loop.run_in_executor(None, self.client.close)
