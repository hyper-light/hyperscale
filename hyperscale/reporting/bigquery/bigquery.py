import asyncio
import functools
import uuid


from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .bigquery_config import BigQueryConfig

try:
    from google.cloud import bigquery

    has_connector = True

except Exception:
    bigquery = None
    has_connector = False


class BigQuery:
    def __init__(self, config: BigQueryConfig) -> None:
        self.service_account_json_path = config.service_account_json_path
        self.project_name = config.project_name
        self.dataset_name = config.dataset_name
        self.dataset_location = config.dataset_location
        self.retry_timeout = config.retry_timeout

        self.workflow_results_table_name = config.workflow_results_table_name
        self.step_results_table_name = config.step_results_table_name

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None

        self.credentials = None
        self.client = None

        self._workflow_results_table = None
        self._step_results_table = None

        self._loop = asyncio.get_event_loop()
        self.reporter_type = ReporterTypes.BigQuery
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        self.client = bigquery.Client.from_service_account_json(
            self.service_account_json_path
        )

        dataset = bigquery.Dataset(f"{self.project_name}_{self.dataset_name}")
        dataset.location = self.dataset_location
        dataset = self.client.create_dataset(
            dataset,
            exists_ok=True,
            timeout=self.retry_timeout,
        )

    async def submit_workflow_results(self, workflow_results: WorkflowMetricSet):
        if self._workflow_results_table is None:
            table_schema = [
                bigquery.SchemaField("metric_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("metric_workflow", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("metric_group", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("metric_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("metric_name", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("metric_value", "INTEGER", mode="REQUIRED"),
            ]

            table_reference = bigquery.TableReference(
                bigquery.DatasetReference(self.project_name, self.dataset_name),
                self.workflow_results_table_name,
            )

            workflow_results_table = bigquery.Table(
                table_reference, schema=table_schema
            )

            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.create_table,
                    workflow_results_table,
                    exists_ok=True,
                ),
            )

            self._workflow_results_table = workflow_results_table

        await self._loop.run_in_executor(
            None,
            functools.partial(
                self.client.insert_rows_json,
                self._workflow_results_table,
                workflow_results,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(self.retry_timeout),
            ),
        )

    async def submit_step_results(self, step_results: StepMetricSet):
        if self._step_results_table is None:
            table_schema = [
                bigquery.SchemaField("metric_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("metric_workflow", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("metric_step", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("metric_group", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("metric_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("metric_name", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("metric_value", "INTEGER", mode="REQUIRED"),
            ]

            table_reference = bigquery.TableReference(
                bigquery.DatasetReference(self.project_name, self.dataset_name),
                self.step_results_table_name,
            )

            step_results_table = bigquery.Table(table_reference, schema=table_schema)

            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.create_table,
                    step_results_table,
                    exists_ok=True,
                ),
            )

            self._step_results_table = step_results_table

        await self._loop.run_in_executor(
            None,
            functools.partial(
                self.client.insert_rows_json,
                self._step_results_table,
                step_results,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(self.retry_timeout),
            ),
        )

    async def close(self):
        await self._loop.run_in_executor(None, self.client.close)
